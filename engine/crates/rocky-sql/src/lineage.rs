use std::collections::{HashMap, HashSet};
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
    /// Table names read INSIDE a derived table or a `WITH` body, lower-cased,
    /// with `WITH`-bound names already removed (#1867).
    ///
    /// [`Self::source_tables`] holds only the top-level `FROM`/`JOIN`
    /// relations, so a read of a model from inside `(SELECT … FROM m)` or
    /// `WITH x AS (SELECT … FROM m)` was invisible to every consumer and no
    /// scheduler ordered the reader after `m`. This carries those reads.
    ///
    /// Deliberately a separate field rather than more `source_tables` entries:
    /// those feed alias resolution and `SELECT *` expansion, which are about
    /// what the query's own `FROM` clause names. A nested read is a dependency,
    /// not a relation the outer query can select from.
    ///
    /// **Best-effort, not complete.** It covers derived tables and `WITH`
    /// bodies. It does not cover a sub-query in `WHERE`, `HAVING`, `GROUP BY`,
    /// a qualifier or a function argument. `crate::lineage_complete` is the
    /// only correct answer to "is this set exhaustive", and it stays exactly as
    /// strict — do not read a non-empty value here as completeness.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nested_sources: Vec<String>,
}

/// What a name in a `FROM`/`JOIN` position actually refers to.
///
/// A `WITH` clause binds names that look exactly like table reads, so the name
/// alone cannot tell the two apart. A consumer deriving dependencies must not
/// treat a CTE as a read: a CTE named after a model would invent an edge to
/// it, and two models with mutual local CTE names would close a cycle that
/// does not exist (#1892).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum TableBinding {
    /// A real read of a table, view or other physical relation.
    #[default]
    Physical,
    /// A reference to a name bound by an enclosing `WITH` clause. Local to the
    /// query — it names no object outside it.
    Cte,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableReference {
    /// Full table name (e.g., "catalog.schema.table").
    pub name: String,
    /// Alias if any.
    pub alias: Option<String>,
    /// Whether this name is a real relation or a `WITH`-bound CTE (#1892).
    ///
    /// `#[serde(default)]` so a `LineageResult` cached by an older build
    /// deserializes as [`TableBinding::Physical`] — the value every reference
    /// carried before CTE scopes were tracked, so an old cache keeps its old
    /// meaning rather than failing to load.
    #[serde(default)]
    pub binding: TableBinding,
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
/// The `(subquery)` marker never appears: a derived table contributes the
/// names read INSIDE it (`LineageResult::nested_sources`, #1867) rather than a
/// placeholder the caller has to filter.
///
/// Names bound by a `WITH` clause are dropped: a CTE is local to the query and
/// names no object a consumer could depend on or schedule against (#1892).
pub fn referenced_tables(sql: &str) -> Result<Vec<String>, String> {
    let result = extract_lineage(sql)?;
    let mut names: Vec<String> = result
        .source_tables
        .iter()
        .filter(|t| t.binding == TableBinding::Physical)
        .map(|t| t.name.to_lowercase())
        .filter(|n| n != "(subquery)")
        .chain(result.nested_sources.iter().cloned())
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
        Statement::Query(query) => extract_query_lineage(query, &CteScope::new()),
        _ => Err("lineage extraction only supports SELECT statements".to_string()),
    }
}

/// The CTE names visible at a point in the walk, folded to lower case.
///
/// SQL scoping nests inward: a name bound by an outer query's `WITH` is
/// visible inside that query's subqueries, so the set is passed down. It never
/// travels back up — a CTE bound inside a subquery is invisible outside it.
type CteScope = HashSet<String>;

/// Every name `query`'s own `WITH` clause binds, added to those already
/// visible. This is the set the query's MAIN BODY sees: all of them.
///
/// The bodies need a narrower set — see [`walk_cte_bodies`].
fn bind_cte_names(query: &Query, outer: &CteScope) -> CteScope {
    let mut scope = outer.clone();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            scope.insert(cte.alias.name.value.to_lowercase());
        }
    }
    scope
}

/// The real table names read inside `query`'s own `WITH` bodies, in clause
/// order, with `WITH`-bound names removed.
///
/// **Binding is incremental, and that is the whole point.** Inside CTE *i*,
/// only CTEs 1..*i*-1 are visible — plus *i* itself when the clause is
/// `RECURSIVE`, because a recursive CTE names itself. Binding the whole clause
/// up front would read a real table in an earlier body as a reference to a
/// later CTE and silently drop its edge:
///
/// ```sql
/// WITH a AS (SELECT * FROM orders),   -- a real read of model `orders`
///      orders AS (SELECT 1)            -- the CTE, defined AFTER
/// SELECT * FROM a
/// ```
fn walk_cte_bodies(query: &Query, outer: &CteScope) -> Vec<String> {
    let Some(with) = &query.with else {
        return Vec::new();
    };
    let mut visible = outer.clone();
    let mut found = Vec::new();
    for cte in &with.cte_tables {
        let own_name = cte.alias.name.value.to_lowercase();
        let mut body_scope = visible.clone();
        if with.recursive {
            body_scope.insert(own_name.clone());
        }
        if let Ok(inner) = extract_query_lineage(&cte.query, &body_scope) {
            collect_nested(&inner, &mut found);
        }
        visible.insert(own_name);
    }
    found
}

/// Fold one inner query's reads into a nested-source list.
///
/// Takes NAMES only. An inner query's `has_star` and `unresolved_projections`
/// describe the inner projection, and merging them into the outer result would
/// claim the outer model's own column set is unresolved when it is not.
fn collect_nested(inner: &LineageResult, out: &mut Vec<String>) {
    for t in &inner.source_tables {
        if t.binding == TableBinding::Physical && t.name != "(subquery)" {
            out.push(t.name.to_lowercase());
        }
    }
    out.extend(inner.nested_sources.iter().cloned());
}

fn extract_query_lineage(query: &Query, outer_ctes: &CteScope) -> Result<LineageResult, String> {
    let ctes = bind_cte_names(query, outer_ctes);
    let mut nested_sources = walk_cte_bodies(query, outer_ctes);
    match query.body.as_ref() {
        SetExpr::Select(select) => {
            // A derived table's own reads come back alongside the relations.
            // The `(subquery)` entry stays in `source_tables` — alias
            // resolution and star expansion still need it — but it names no
            // object, so the names inside it are what a consumer depends on.
            let (source_tables, derived_reads) = extract_tables(&select.from, &ctes);
            nested_sources.extend(derived_reads);
            nested_sources.sort();
            nested_sources.dedup();
            let alias_map = build_alias_map(&source_tables);
            let (columns, has_star, unresolved_projections) =
                extract_select_columns(&select.projection, &alias_map, &source_tables);

            Ok(LineageResult {
                source_tables,
                columns,
                has_star,
                unresolved_projections,
                nested_sources,
            })
        }
        SetExpr::Query(inner) => {
            let mut result = extract_query_lineage(inner, &ctes)?;
            nested_sources.append(&mut result.nested_sources);
            nested_sources.sort();
            nested_sources.dedup();
            result.nested_sources = nested_sources;
            Ok(result)
        }
        _ => Err("unsupported query type for lineage".to_string()),
    }
}

/// The relations named in `from`, plus the table names read INSIDE any derived
/// table there (#1867). The two are returned separately because they answer
/// different questions: the first is what the query can select from, the second
/// is what it depends on.
fn extract_tables(from: &[TableWithJoins], ctes: &CteScope) -> (Vec<TableReference>, Vec<String>) {
    let mut tables = Vec::new();
    let mut nested = Vec::new();

    for table_with_joins in from {
        extract_table_factor(&table_with_joins.relation, ctes, &mut tables, &mut nested);
        for join in &table_with_joins.joins {
            extract_table_factor(&join.relation, ctes, &mut tables, &mut nested);
        }
    }

    (tables, nested)
}

fn extract_table_factor(
    factor: &TableFactor,
    ctes: &CteScope,
    tables: &mut Vec<TableReference>,
    nested: &mut Vec<String>,
) {
    match factor {
        TableFactor::Table { name, alias, .. } => {
            let name = name.to_string();
            // A CTE reference is always a single unqualified name, so a
            // multi-part read can never be one — checking the whole spelling
            // is what keeps `v2.orders` from being shadowed by a CTE `orders`.
            let binding = if ctes.contains(&name.to_lowercase()) {
                TableBinding::Cte
            } else {
                TableBinding::Physical
            };
            tables.push(TableReference {
                name,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
                binding,
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
            let inner = extract_query_lineage(subquery, ctes).ok();
            // The inner query's own reads are this model's dependencies. Taken
            // as NAMES only — the inner `has_star` describes the inner
            // projection, not the outer model's column set (#1867).
            if let Some(inner) = inner.as_ref() {
                collect_nested(inner, nested);
            }
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
                binding: TableBinding::Physical,
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

    /// #1892: a `WITH`-bound name looks exactly like a table read, and a
    /// consumer that cannot tell them apart derives an edge to a model that
    /// the query never reads.
    #[test]
    fn a_cte_reference_is_not_a_table_read() {
        let result =
            extract_lineage("WITH orders AS (SELECT 2 AS id) SELECT id FROM orders").unwrap();

        assert_eq!(result.source_tables.len(), 1);
        assert_eq!(result.source_tables[0].name, "orders");
        assert_eq!(result.source_tables[0].binding, TableBinding::Cte);

        assert!(
            referenced_tables("WITH orders AS (SELECT 2 AS id) SELECT id FROM orders")
                .unwrap()
                .is_empty(),
            "a CTE names no object outside the query, so nothing can depend on it"
        );
    }

    /// The other half of the same rule, and the one that keeps the fix from
    /// suppressing real edges: the SAME spelling with no `WITH` clause is an
    /// ordinary read and must stay one.
    #[test]
    fn the_same_name_without_a_with_clause_is_still_a_table_read() {
        let result = extract_lineage("SELECT id FROM orders").unwrap();

        assert_eq!(result.source_tables[0].binding, TableBinding::Physical);
        assert_eq!(
            referenced_tables("SELECT id FROM orders").unwrap(),
            vec!["orders".to_string()]
        );
    }

    /// A query may bind one CTE and read a real table in the same `FROM`.
    /// Only the bound name is shadowed.
    #[test]
    fn only_the_bound_name_is_shadowed() {
        let result =
            extract_lineage("WITH c AS (SELECT 1 AS id) SELECT c.id FROM c JOIN orders USING (id)")
                .unwrap();

        let by_name: HashMap<&str, TableBinding> = result
            .source_tables
            .iter()
            .map(|t| (t.name.as_str(), t.binding))
            .collect();
        assert_eq!(by_name["c"], TableBinding::Cte);
        assert_eq!(by_name["orders"], TableBinding::Physical);
    }

    /// CTE names fold case, as SQL identifiers do. Without the fold, a project
    /// writing `WITH Orders AS (…) … FROM orders` would keep the false edge.
    #[test]
    fn a_cte_name_shadows_across_case() {
        let result =
            extract_lineage("WITH Orders AS (SELECT 1 AS id) SELECT id FROM ORDERS").unwrap();

        assert_eq!(result.source_tables[0].binding, TableBinding::Cte);
    }

    /// Scope nests inward: a name bound by the outer query is visible inside
    /// that query's derived tables, so the inner read is shadowed too.
    #[test]
    fn an_outer_cte_shadows_inside_a_derived_table() {
        let result = extract_lineage(
            "WITH orders AS (SELECT 1 AS id) \
             SELECT id FROM (SELECT id FROM orders) AS s",
        )
        .unwrap();

        assert!(
            referenced_tables(
                "WITH orders AS (SELECT 1 AS id) SELECT id FROM (SELECT id FROM orders) AS s"
            )
            .unwrap()
            .is_empty(),
            "the inner read is the outer CTE, not a table: {:?}",
            result.source_tables
        );
    }

    /// A CTE reference is always a single unqualified name, so a qualified
    /// read must not be shadowed by a CTE that shares its last part. Checking
    /// the whole spelling rather than the stem is what buys this.
    #[test]
    fn a_qualified_read_is_not_shadowed_by_a_same_stem_cte() {
        let result =
            extract_lineage("WITH orders AS (SELECT 1 AS id) SELECT id FROM v2.orders").unwrap();

        assert_eq!(result.source_tables[0].name, "v2.orders");
        assert_eq!(result.source_tables[0].binding, TableBinding::Physical);
    }

    /// The CTE name lives on the alias, and an explicit column list puts a
    /// second thing there. The name must still bind, or the shadow silently
    /// would not apply to this spelling.
    ///
    /// The other spelling worth naming is `WITH x AS MATERIALIZED (…)`, which
    /// `DatabricksDialect` does not parse at all — so a model using it already
    /// fails lineage extraction outright, long before binding is asked about.
    /// That is pre-existing and unrelated to CTE scopes.
    #[test]
    fn an_explicit_column_list_still_binds_the_name() {
        let result =
            extract_lineage("WITH orders (id) AS (SELECT 1 AS id) SELECT id FROM orders").unwrap();

        assert_eq!(
            result.source_tables[0].binding,
            TableBinding::Cte,
            "the name is bound whatever else the clause carries"
        );
    }

    /// A `WITH RECURSIVE` body names itself. The main body's read is still the
    /// CTE, not a table.
    #[test]
    fn a_recursive_cte_binds_its_own_name() {
        let result = extract_lineage(
            "WITH RECURSIVE orders AS (SELECT 1 AS id UNION ALL SELECT id + 1 FROM orders \
             WHERE id < 3) SELECT id FROM orders",
        )
        .unwrap();

        assert_eq!(result.source_tables[0].binding, TableBinding::Cte);
    }

    /// #1867: a read inside a derived table is a dependency. It used to reach
    /// no consumer at all — `source_tables` held the literal `(subquery)` and
    /// nothing looked inside it — so no scheduler ordered the reader after its
    /// producer.
    #[test]
    fn a_subquery_read_surfaces_as_a_dependency() {
        let sql = "SELECT id FROM (SELECT id FROM orders) AS s";
        let result = extract_lineage(sql).unwrap();

        assert_eq!(result.source_tables[0].name, "(subquery)");
        assert_eq!(result.nested_sources, vec!["orders".to_string()]);
        assert_eq!(
            referenced_tables(sql).unwrap(),
            vec!["orders".to_string()],
            "the marker is gone and the real read is there"
        );
    }

    /// The same for a `WITH` body. The CTE's own name stays out — it names no
    /// object outside the query (#1892).
    #[test]
    fn a_cte_body_read_surfaces_but_the_cte_name_does_not() {
        let sql = "WITH c AS (SELECT id FROM orders) SELECT id FROM c";

        assert_eq!(referenced_tables(sql).unwrap(), vec!["orders".to_string()]);
    }

    /// The rule the incremental binding exists for. `orders` inside `a`'s body
    /// is a REAL table: a non-recursive CTE only sees the CTEs declared before
    /// it, so the `orders` CTE declared afterwards does not shadow it.
    ///
    /// Binding the whole clause up front would classify that read as a CTE
    /// reference and drop the edge — silently, and only for this ordering.
    #[test]
    fn a_cte_declared_after_a_body_does_not_shadow_that_bodys_read() {
        let sql = "WITH a AS (SELECT id FROM orders), \
                   orders AS (SELECT 1 AS id) \
                   SELECT id FROM a";

        assert_eq!(
            referenced_tables(sql).unwrap(),
            vec!["orders".to_string()],
            "the read in `a` predates the `orders` CTE, so it is the table"
        );
    }

    /// And the other ordering, which must give the opposite answer: declared
    /// FIRST, the CTE does shadow the read.
    #[test]
    fn a_cte_declared_before_a_body_does_shadow_that_bodys_read() {
        let sql = "WITH orders AS (SELECT 1 AS id), \
                   a AS (SELECT id FROM orders) \
                   SELECT id FROM a";

        assert!(
            referenced_tables(sql).unwrap().is_empty(),
            "the read in `a` is the CTE above it, not a table"
        );
    }

    /// A `WITH RECURSIVE` body names itself, so its self-read is not a table.
    #[test]
    fn a_recursive_body_does_not_read_itself_as_a_table() {
        let sql = "WITH RECURSIVE walk AS (SELECT id FROM walk) SELECT id FROM walk";

        assert!(
            referenced_tables(sql).unwrap().is_empty(),
            "the recursive CTE is not a table"
        );
    }

    /// A limitation, pinned so it is a known gap rather than a surprise: a CTE
    /// body that is a SET OPERATION contributes nothing.
    ///
    /// `extract_query_lineage` handles `SetExpr::Select` and `SetExpr::Query`
    /// and returns `Err` for `SetExpr::SetOperation`, so `walk_cte_bodies`
    /// skips such a body entirely. `UNION ALL` inside a recursive CTE is the
    /// ordinary spelling of one, so #1867's fix does not reach it.
    ///
    /// This test exists because the recursive test above was originally
    /// written with a `UNION ALL` body and PASSED VACUOUSLY — the CTE name was
    /// absent because nothing was walked, not because self-binding worked.
    #[test]
    fn a_set_operation_cte_body_contributes_no_reads() {
        let sql = "WITH walk AS ( \
                     SELECT id FROM seed \
                     UNION ALL \
                     SELECT id FROM other \
                   ) SELECT id FROM walk";

        assert!(
            referenced_tables(sql).unwrap().is_empty(),
            "known gap: a set-operation body is not walked, so `seed` and \
             `other` derive no edge"
        );
    }

    /// Reads two levels down still surface — the walk recurses rather than
    /// looking one level.
    #[test]
    fn a_read_two_levels_down_still_surfaces() {
        let sql = "SELECT id FROM (SELECT id FROM (SELECT id FROM orders) AS inner_q) AS outer_q";

        assert_eq!(referenced_tables(sql).unwrap(), vec!["orders".to_string()]);
    }

    /// The inner query's OWN projection facts must not become the outer
    /// model's. An inner `SELECT *` says nothing about whether the outer
    /// model's column set is known, and merging it would make every model with
    /// a `SELECT *` subquery look unresolved.
    #[test]
    fn an_inner_star_does_not_make_the_outer_projection_unresolved() {
        let result = extract_lineage("SELECT id FROM (SELECT * FROM orders) AS s").unwrap();

        assert!(!result.has_star, "the OUTER projection names its column");
        assert_eq!(result.unresolved_projections, 0);
        assert_eq!(result.nested_sources, vec!["orders".to_string()]);
    }
}
