use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use std::ops::ControlFlow;

use sqlparser::ast::{
    CastKind, Distinct, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    JoinConstraint, JoinOperator, NamedWindowDefinition, NamedWindowExpr, OrderByKind, Query,
    Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value, Visit, Visitor,
    WindowSpec, WindowType,
};
use sqlparser::parser::Parser;

use crate::dialect::DatabricksDialect;

/// How a column value is transformed from source to target.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransformKind {
    /// Direct column reference (no transformation).
    Direct,
    /// Plain type cast (`CAST(...)` or `expr :: type`). The edge itself does
    /// not know the source or target types, so it carries the input's
    /// nullability; the compiler widens it when the cast can fail (#2299).
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
    /// `source_table` is a physical table name that CTE resolution already
    /// produced, not a name or alias of the enclosing query's `FROM` clause.
    /// A consumer must not resolve it through that query's alias map: an
    /// outer alias spelled like the table would capture it (#2307).
    #[serde(default, skip_serializing_if = "is_false")]
    pub physical: bool,
    /// Another source of the output the previous entry already names, not a
    /// new output column. An output built from several columns (`a.x * b.y`)
    /// has one entry per source column; the first has this `false`, the rest
    /// `true` (#2318).
    #[serde(default, skip_serializing_if = "is_false")]
    pub extra_source: bool,
}

fn is_false(value: &bool) -> bool {
    !*value
}

/// Why a column influences which rows (or groups) a model produces.
///
/// This is the second lineage edge kind. A [`ColumnLineage`] entry is a
/// **value derivation**: the source column feeds an output value. A
/// [`RowSelectionLineage`] entry is a **row selection**: the source column
/// decides which rows or groups exist, without feeding any output value
/// directly. Changing or dropping either kind of column changes the model's
/// output, which is why impact analysis needs both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowSelectionKind {
    /// A key in a `JOIN ... ON` / `USING` / ASOF match condition.
    JoinKey,
    /// A `WHERE` (or `PREWHERE`) predicate.
    Filter,
    /// A `GROUP BY` key.
    GroupBy,
    /// A `HAVING` predicate.
    Having,
    /// A `QUALIFY` predicate.
    Qualify,
    /// A window function's `PARTITION BY` key.
    WindowPartition,
    /// A window function's `ORDER BY` key.
    WindowOrder,
    /// A `DISTINCT ON (...)` key, which picks one row per key value, or an
    /// `ORDER BY` key of a `DISTINCT ON` query, which picks which row that is.
    DistinctOn,
    /// An `ORDER BY` key of a query that also has `LIMIT`, `FETCH` or `TOP`:
    /// the order decides which rows survive the cut.
    OrderLimit,
}

impl fmt::Display for RowSelectionKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            RowSelectionKind::JoinKey => "join_key",
            RowSelectionKind::Filter => "filter",
            RowSelectionKind::GroupBy => "group_by",
            RowSelectionKind::Having => "having",
            RowSelectionKind::Qualify => "qualify",
            RowSelectionKind::WindowPartition => "window_partition",
            RowSelectionKind::WindowOrder => "window_order",
            RowSelectionKind::DistinctOn => "distinct_on",
            RowSelectionKind::OrderLimit => "order_limit",
        };
        f.write_str(label)
    }
}

/// A row-selection edge: `source_table.source_column` decides which rows or
/// groups the query produces. See [`RowSelectionKind`].
///
/// Extracted from the top-level `SELECT` only. Not covered (no edge is
/// recorded): predicates inside a derived table, a `WITH` body or a subquery
/// expression (`IN (SELECT …)`, `EXISTS`), correlated references, `GROUP BY
/// ALL`, `NATURAL` joins (the shared columns are known only to the warehouse
/// catalog) and set operations. An unqualified column in a multi-table query keeps
/// `source_table = None` because its table cannot be determined statically.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowSelectionLineage {
    /// Source table (resolved through aliases when the alias is known).
    pub source_table: Option<String>,
    /// Source column name.
    pub source_column: String,
    /// Which clause the column appears in.
    pub kind: RowSelectionKind,
    /// The single output column this edge affects, for window keys. `None`
    /// means the edge affects every output row of the query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_column: Option<String>,
    /// See [`ColumnLineage::physical`].
    #[serde(default, skip_serializing_if = "is_false")]
    pub physical: bool,
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
    /// Row-selection edges: columns used as join keys, filters, group keys or
    /// window keys. Additive to [`Self::columns`], which stays value-only.
    /// See [`RowSelectionLineage`] for coverage.
    ///
    /// `#[serde(default)]` keeps a cache written by an older build loadable.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_selection: Vec<RowSelectionLineage>,
    /// Where each entry of [`Self::columns`] really comes from, by position:
    /// a physical table and column, or `None` when unknown. Built during
    /// extraction to resolve reads through a CTE (#2307); not serialized.
    #[serde(skip)]
    column_origins: Vec<Option<ColumnOrigin>>,
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
    ///
    /// For a [`TableBinding::Cte`] reference whose body is an unresolved
    /// `SELECT *`, this lists the physical tables the star passes through,
    /// resolved through any chain of CTEs (#2307).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub derived_sources: Vec<String>,
    /// For a [`TableBinding::Cte`] reference: the CTE body's output columns,
    /// each named by `target_column` and traced to the physical table and
    /// column it comes from (`source_table = None` when unknown). A CTE body
    /// that is an unresolved `SELECT *` lists its physical tables in
    /// [`Self::derived_sources`] instead (#2307).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cte_columns: Vec<ColumnLineage>,
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

/// The CTE names visible at a point in the walk, folded to lower case, each
/// with what its body resolves to.
///
/// SQL scoping nests inward: a name bound by an outer query's `WITH` is
/// visible inside that query's subqueries, so the map is passed down. It never
/// travels back up — a CTE bound inside a subquery is invisible outside it.
///
/// The value is `None` when the body cannot be read: a recursive CTE's
/// reference to itself, or a body the extractor refuses.
type CteScope = HashMap<String, Option<Arc<CteBody>>>;

/// Where one output column really comes from: a physical table, a column of
/// it, and how the value is transformed on the way.
type ColumnOrigin = (String, String, TransformKind);

/// What a `WITH` body produces, resolved down to physical tables (#2307).
///
/// A reference to a CTE takes its columns and their lineage from here, the
/// way a derived table takes them from its subquery. A body that reads an
/// earlier CTE was resolved when that body was walked, so a chain of CTEs
/// traces to the physical tables at its end.
#[derive(Debug, Default)]
struct CteBody {
    /// Output columns by name, with every origin that is known. A column built
    /// from several columns (`a.x * b.y`) has several; one with none known has
    /// an empty list.
    columns: Vec<(String, Vec<ColumnOrigin>)>,
    /// Physical tables an unresolved `SELECT *` in the body reads. Empty when
    /// the body has no star.
    star_sources: Vec<String>,
}

impl CteBody {
    fn from_result(result: &LineageResult, ctes: &CteScope) -> Self {
        let mut body = CteBody::default();
        for (i, col) in result.columns.iter().enumerate() {
            let origin = result.column_origins.get(i).cloned().flatten();
            // One entry per source column of an expression (#2318): the later
            // ones add origins to the column the first one named.
            if col.extra_source
                && let Some((_, origins)) = body.columns.last_mut()
            {
                origins.extend(origin);
            } else {
                body.columns
                    .push((col.target_column.clone(), origin.into_iter().collect()));
            }
        }
        if result.has_star {
            for t in &result.source_tables {
                match t.binding {
                    TableBinding::Cte => {
                        let Some(Some(inner)) = ctes.get(&t.name.to_lowercase()) else {
                            continue;
                        };
                        for (name, origin) in &inner.columns {
                            if !body.has_column(name) {
                                body.columns.push((name.clone(), origin.clone()));
                            }
                        }
                        body.star_sources.extend(inner.star_sources.iter().cloned());
                    }
                    TableBinding::Physical if t.name == "(subquery)" => {
                        for name in t.derived_columns.iter().flatten() {
                            if !body.has_column(name) {
                                body.columns.push((name.clone(), Vec::new()));
                            }
                        }
                        body.star_sources.extend(t.derived_sources.iter().cloned());
                    }
                    TableBinding::Physical => body.star_sources.push(t.name.clone()),
                }
            }
            body.star_sources.dedup();
        }
        body
    }

    fn has_column(&self, name: &str) -> bool {
        self.columns
            .iter()
            .any(|(n, _)| n.eq_ignore_ascii_case(name))
    }

    /// The origin of `column` read from this CTE. A column the body does not
    /// list passes through its `SELECT *` only when exactly one table feeds
    /// the star; with several, the owner is unknown.
    fn origins_of(&self, column: &str) -> Vec<ColumnOrigin> {
        if let Some((_, origins)) = self
            .columns
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(column))
        {
            return origins.clone();
        }
        match self.star_sources.as_slice() {
            [only] => vec![(only.clone(), column.to_string(), TransformKind::Direct)],
            _ => Vec::new(),
        }
    }
}

/// The transform of an outer read over an inner one. A plain read keeps what
/// the inner column did; a fallible inner cast stays fallible under a cast.
fn compose_transform(outer: &TransformKind, inner: &TransformKind) -> TransformKind {
    match (outer, inner) {
        (TransformKind::Direct, inner) => inner.clone(),
        (TransformKind::Cast, TransformKind::TryCast) => TransformKind::TryCast,
        (outer, _) => outer.clone(),
    }
}

/// The transform of an outer read over a CTE column built from several
/// columns. The inner value is an `Expression`, so a cast or a function over it
/// has no one column's type either; `COUNT` and a fallible cast keep their
/// own nullability rules.
fn compose_multi_origin_transform(outer: &TransformKind) -> TransformKind {
    match outer {
        TransformKind::Direct => TransformKind::Expression,
        TransformKind::TryCast => TransformKind::TryCast,
        TransformKind::Aggregation(f) if f == "COUNT" => outer.clone(),
        TransformKind::Cast | TransformKind::Aggregation(_) | TransformKind::Expression => {
            TransformKind::Expression
        }
    }
}

/// The relation a column reference names: by alias first, then by name.
fn find_relation<'a>(
    table: &str,
    source_tables: &'a [TableReference],
) -> Option<&'a TableReference> {
    source_tables
        .iter()
        .find(|t| {
            t.alias
                .as_deref()
                .is_some_and(|a| a.eq_ignore_ascii_case(table))
        })
        .or_else(|| {
            source_tables
                .iter()
                .find(|t| t.name.eq_ignore_ascii_case(table))
        })
}

/// How a read of `table.column` resolves when `table` may be a CTE.
enum CteRead {
    /// `table` is not a CTE: the read stays as it is.
    NotCte,
    /// The read goes through a CTE to these physical origins: one for a
    /// column of the body, several for a column built from several columns.
    Origins(Vec<ColumnOrigin>),
    /// The read goes through a CTE whose column origin is unknown.
    Unknown,
}

fn read_through_cte(
    table: &str,
    column: &str,
    source_tables: &[TableReference],
    ctes: &CteScope,
) -> CteRead {
    let Some(relation) = find_relation(table, source_tables) else {
        return CteRead::NotCte;
    };
    match relation.binding {
        TableBinding::Physical => CteRead::NotCte,
        TableBinding::Cte => match ctes.get(&relation.name.to_lowercase()) {
            Some(Some(body)) => match body.origins_of(column) {
                origins if origins.is_empty() => CteRead::Unknown,
                origins => CteRead::Origins(origins),
            },
            Some(None) | None => CteRead::Unknown,
        },
    }
}

/// Rewrite a row-selection edge that reads through a CTE so it names the
/// CTE body's physical origin, or no table when that is unknown. A CTE column
/// built from several columns yields one edge per origin.
fn resolve_row_selection(
    rs: RowSelectionLineage,
    source_tables: &[TableReference],
    ctes: &CteScope,
) -> Vec<RowSelectionLineage> {
    let Some(table) = rs.source_table.clone() else {
        return vec![rs];
    };
    match read_through_cte(&table, &rs.source_column, source_tables, ctes) {
        CteRead::NotCte => vec![rs],
        CteRead::Origins(origins) => origins
            .into_iter()
            .map(|(t, c, _)| RowSelectionLineage {
                source_table: Some(t),
                source_column: c,
                physical: true,
                ..rs.clone()
            })
            .collect(),
        CteRead::Unknown => vec![RowSelectionLineage {
            source_table: None,
            ..rs
        }],
    }
}

/// Rewrite every column and row-selection entry of one `SELECT` that reads
/// through a CTE so it names the CTE body's physical origin, or no table when
/// that is unknown (#2307). A CTE named like a source shadows it, so an entry
/// must never keep the CTE's name: a consumer would bind it to the same-named
/// object and take that object's types.
///
/// Also records each column's origin for [`CteBody::from_result`]. Runs once
/// per `SELECT`: a rewritten entry names a physical table that a CTE in the
/// same scope may shadow, so resolving it again would be wrong.
fn resolve_through_ctes(result: &mut LineageResult, ctes: &CteScope) {
    let mut columns = Vec::with_capacity(result.columns.len());
    let mut origins = Vec::with_capacity(result.columns.len());
    for mut col in std::mem::take(&mut result.columns) {
        let Some(table) = col.source_table.clone() else {
            columns.push(col);
            origins.push(None);
            continue;
        };
        match read_through_cte(&table, &col.source_column, &result.source_tables, ctes) {
            CteRead::NotCte => {
                let origin = match find_relation(&table, &result.source_tables) {
                    Some(t) if t.name == "(subquery)" => None,
                    Some(t) => Some((
                        t.name.clone(),
                        col.source_column.clone(),
                        col.transform.clone(),
                    )),
                    None => Some((table, col.source_column.clone(), col.transform.clone())),
                };
                columns.push(col);
                origins.push(origin);
            }
            CteRead::Origins(inner) => {
                // A column built from several columns is an `Expression` edge
                // per origin, whatever wraps it here.
                let many = inner.len() > 1;
                for (i, (t, c, inner_kind)) in inner.into_iter().enumerate() {
                    let mut entry = col.clone();
                    entry.extra_source |= i > 0;
                    entry.transform = if many || inner_kind == TransformKind::Expression {
                        compose_multi_origin_transform(&col.transform)
                    } else {
                        compose_transform(&col.transform, &inner_kind)
                    };
                    entry.source_table = Some(t.clone());
                    entry.source_column = c.clone();
                    entry.physical = true;
                    origins.push(Some((t, c, entry.transform.clone())));
                    columns.push(entry);
                }
            }
            CteRead::Unknown => {
                col.source_table = None;
                columns.push(col);
                origins.push(None);
            }
        }
    }
    result.columns = columns;
    result.column_origins = origins;
    let source_tables = &result.source_tables;
    let mut seen = HashSet::new();
    result.row_selection = std::mem::take(&mut result.row_selection)
        .into_iter()
        .flat_map(|rs| resolve_row_selection(rs, source_tables, ctes))
        .filter(|rs| seen.insert(rs.clone()))
        .collect();
}

/// The real table names read inside `query`'s own `WITH` bodies, in clause
/// order, with `WITH`-bound names removed; and the scope `query`'s main body
/// sees: the outer CTEs plus every CTE this clause binds, with its body.
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
fn walk_cte_bodies(query: &Query, outer: &CteScope) -> (Vec<String>, CteScope) {
    let Some(with) = &query.with else {
        return (Vec::new(), outer.clone());
    };
    let mut visible = outer.clone();
    let mut found = Vec::new();
    for cte in &with.cte_tables {
        let own_name = cte.alias.name.value.to_lowercase();
        let mut body_scope = visible.clone();
        if with.recursive {
            body_scope.insert(own_name.clone(), None);
        }
        let body = match extract_query_lineage(&cte.query, &body_scope) {
            Ok(inner) => {
                collect_nested(&inner, &mut found);
                Some(Arc::new(CteBody::from_result(&inner, &body_scope)))
            }
            Err(_) => None,
        };
        visible.insert(own_name, body);
    }
    (found, visible)
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
    let (nested_sources, ctes) = walk_cte_bodies(query, outer_ctes);
    let mut result = extract_set_expr_lineage(query.body.as_ref(), &ctes, nested_sources)?;
    if let SetExpr::Select(select) = query.body.as_ref() {
        let edges = extract_order_limit(query, select, &result.source_tables);
        for edge in edges {
            for edge in resolve_row_selection(edge, &result.source_tables, &ctes) {
                if !result.row_selection.contains(&edge) {
                    result.row_selection.push(edge);
                }
            }
        }
    }
    Ok(result)
}

/// `ORDER BY` keys of a query that also cuts its rows (`LIMIT`, `FETCH`,
/// `TOP`, kind `order_limit`) or uses `DISTINCT ON (...)` (kind
/// `distinct_on`: the order picks the row kept per key). Without either, the
/// order only arranges rows, so no edge is made.
fn extract_order_limit(
    query: &Query,
    select: &Select,
    source_tables: &[TableReference],
) -> Vec<RowSelectionLineage> {
    let cuts = query.limit_clause.is_some() || query.fetch.is_some() || select.top.is_some();
    // `DISTINCT ON (...)` keeps the first row of each key group in `ORDER BY`
    // order, so the order keys also decide which row survives.
    let distinct_on = matches!(select.distinct, Some(Distinct::On(_)));
    let Some(order_by) = &query.order_by else {
        return Vec::new();
    };
    let OrderByKind::Expressions(keys) = &order_by.kind else {
        return Vec::new();
    };
    let mut kinds: Vec<RowSelectionKind> = Vec::new();
    if distinct_on {
        kinds.push(RowSelectionKind::DistinctOn);
    }
    if cuts {
        kinds.push(RowSelectionKind::OrderLimit);
    }
    if kinds.is_empty() {
        return Vec::new();
    }
    let alias_map = build_alias_map(source_tables);
    let projection = projection_exprs(&select.projection);
    let mut out: Vec<RowSelectionLineage> = Vec::new();
    for key in keys {
        for (qualifier, column) in refs_with_projection_substitution(&key.expr, &projection, true) {
            let source_table = resolve_ref_table(qualifier.as_deref(), &alias_map, source_tables);
            for kind in &kinds {
                let edge = RowSelectionLineage {
                    source_table: source_table.clone(),
                    source_column: column.clone(),
                    kind: *kind,
                    target_column: None,
                    physical: false,
                };
                if !out.contains(&edge) {
                    out.push(edge);
                }
            }
        }
    }
    out
}

/// Lineage of one query body, with `nested_sources` already gathered from the
/// enclosing query's `WITH` bodies.
///
/// A set operation (`UNION [ALL]`, `INTERSECT`, `EXCEPT`) takes its output
/// column names, and so its column entries, from the LEFT branch, as SQL
/// does. The right branch's relations are dependencies, not relations the
/// output projects from, so they land in `nested_sources` (never in
/// `source_tables`, which drives alias resolution and `SELECT *` expansion).
/// Column edges from the right branch are not recorded: lineage under-reports
/// there rather than refusing the whole model.
fn extract_set_expr_lineage(
    body: &SetExpr,
    ctes: &CteScope,
    mut nested_sources: Vec<String>,
) -> Result<LineageResult, String> {
    match body {
        SetExpr::Select(select) => {
            // A derived table's own reads come back alongside the relations.
            // The `(subquery)` entry stays in `source_tables` — alias
            // resolution and star expansion still need it — but it names no
            // object, so the names inside it are what a consumer depends on.
            let (source_tables, derived_reads) = extract_tables(&select.from, ctes);
            nested_sources.extend(derived_reads);
            nested_sources.sort();
            nested_sources.dedup();
            let alias_map = build_alias_map(&source_tables);
            let (columns, has_star, unresolved_projections) =
                extract_select_columns(&select.projection, &alias_map, &source_tables);
            let row_selection = extract_row_selection(select, &alias_map, &source_tables);

            let mut result = LineageResult {
                source_tables,
                columns,
                has_star,
                unresolved_projections,
                nested_sources,
                row_selection,
                column_origins: Vec::new(),
            };
            resolve_through_ctes(&mut result, ctes);
            Ok(result)
        }
        SetExpr::Query(inner) => {
            let mut result = extract_query_lineage(inner, ctes)?;
            nested_sources.append(&mut result.nested_sources);
            nested_sources.sort();
            nested_sources.dedup();
            result.nested_sources = nested_sources;
            Ok(result)
        }
        SetExpr::SetOperation { left, right, .. } => {
            let mut result = extract_set_expr_lineage(left, ctes, nested_sources)?;
            // A right branch the extractor cannot read (`VALUES`, `TABLE t`)
            // contributes no reads rather than refusing the whole model: the
            // output columns come from the left branch either way.
            if let Ok(right) = extract_set_expr_lineage(right, ctes, Vec::new()) {
                collect_nested(&right, &mut result.nested_sources);
                result.row_selection.extend(right.row_selection);
            }
            result.nested_sources.sort();
            result.nested_sources.dedup();
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
            let cte = ctes.get(&name.to_lowercase());
            let binding = if cte.is_some() {
                TableBinding::Cte
            } else {
                TableBinding::Physical
            };
            let body = cte.cloned().flatten();
            let cte_columns = body
                .as_ref()
                .map(|body| {
                    body.columns
                        .iter()
                        .flat_map(|(target, origins)| {
                            // One entry per origin: a column built from
                            // several columns has several (#2318).
                            let mut entries: Vec<ColumnLineage> = origins
                                .iter()
                                .enumerate()
                                .map(|(i, (table, column, transform))| ColumnLineage {
                                    source_table: Some(table.clone()),
                                    source_column: column.clone(),
                                    target_column: target.clone(),
                                    transform: transform.clone(),
                                    physical: false,
                                    extra_source: i > 0,
                                })
                                .collect();
                            if entries.is_empty() {
                                entries.push(ColumnLineage {
                                    source_table: None,
                                    source_column: target.clone(),
                                    target_column: target.clone(),
                                    transform: TransformKind::Direct,
                                    physical: false,
                                    extra_source: false,
                                });
                            }
                            entries
                        })
                        .collect()
                })
                .unwrap_or_default();
            let derived_sources = body
                .map(|body| body.star_sources.clone())
                .unwrap_or_default();
            tables.push(TableReference {
                name,
                alias: alias.as_ref().map(|a| a.name.value.clone()),
                binding,
                derived_columns: None,
                derived_sources,
                cte_columns,
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
                cte_columns: Vec::new(),
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
        physical: false,
        extra_source: false,
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
                //
                // An expression over several columns (`SELECT a + b`) has no
                // single name to give them, so it adds no entry: naming each
                // one after its source would invent output columns.
                //
                // A parenthesised item is not traced either: it stays as it was
                // before #2318 (unresolved, no entry), so a model's column set
                // does not change under it.
                // An `Expression` edge (`amount * 100`) is skipped too: the
                // source column's name is not the output's, and pushing it
                // would invent or duplicate an output column.
                if !matches!(expr, Expr::Nested(_))
                    && let [lineage] =
                        extract_expr_lineage(expr, alias_map, source_tables).as_slice()
                    && lineage.transform != TransformKind::Expression
                {
                    columns.push(lineage.clone());
                }
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let sources = extract_expr_lineage(expr, alias_map, source_tables);
                if sources.is_empty() {
                    // A named projection with no upstream column — `COUNT(*)`
                    // or a literal — is still a real output column. Emit a
                    // source-less entry so it's not dropped from the model's
                    // schema (the column set drives `rocky profile` / the
                    // Inspector Columns tab and the typed schema); it simply
                    // has no column-level lineage edge.
                    columns.push(source_less_column(&alias.value));
                }
                // One entry per source column: an output built from several
                // columns has several value-lineage edges with one target.
                for (i, mut lineage) in sources.into_iter().enumerate() {
                    lineage.target_column = alias.value.clone();
                    lineage.extra_source = i > 0;
                    columns.push(lineage);
                }
            }
            // Spark SQL `SELECT expr AS (a, b, c)` — multi-alias binding.
            // Emit one lineage entry per alias and source, cloning the upstream lineage.
            SelectItem::ExprWithAliases { expr, aliases } => {
                let sources = extract_expr_lineage(expr, alias_map, source_tables);
                for alias in aliases {
                    if sources.is_empty() {
                        columns.push(source_less_column(&alias.value));
                    }
                    for (i, base) in sources.iter().enumerate() {
                        let mut lineage = base.clone();
                        lineage.target_column = alias.value.clone();
                        lineage.extra_source = i > 0;
                        columns.push(lineage);
                    }
                }
            }
        }
    }

    (columns, has_star, unresolved_projections)
}

/// The value sources of one expression: an edge for every column it reads,
/// outside nested queries (#2318).
///
/// Edge kinds, outermost wins:
/// - a bare column (optionally parenthesised) is `Direct`;
/// - `CAST` / `TRY_CAST` compose with the inner edge ([`compose_cast_transform`]);
/// - a function over exactly one column is `Aggregation(name)`, composed with
///   what the argument did ([`compose_function_transform`]);
/// - everything else (a binary or unary operator, `CASE`, `BETWEEN`, `IN`, a
///   function over several columns) is `Expression`, one edge per column.
///
/// The fallback reads columns through [`ColumnRefCollector`], the same visitor
/// the row-selection code uses. That visitor covers every `Expr` variant, so
/// a new operator cannot be skipped the way a hand-written match skipped
/// `BinaryOp`.
fn extract_expr_lineage(
    expr: &Expr,
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> Vec<ColumnLineage> {
    match expr {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => column_refs(expr)
            .into_iter()
            .take(1)
            .map(|(qualifier, column)| {
                column_lineage(
                    qualifier,
                    column,
                    TransformKind::Direct,
                    alias_map,
                    source_tables,
                )
            })
            .collect(),
        Expr::Nested(inner) => extract_expr_lineage(inner, alias_map, source_tables),
        Expr::Cast { expr, kind, .. } => {
            let mut sources = extract_expr_lineage(expr, alias_map, source_tables);
            for lineage in &mut sources {
                lineage.transform = compose_cast_transform(kind, &lineage.transform);
            }
            sources
        }
        Expr::Function(func) => {
            let func_name = func.name.to_string().to_uppercase();
            let mut sources = Vec::new();
            if let FunctionArguments::List(arg_list) = &func.args {
                for arg in &arg_list.args {
                    let (FunctionArg::Unnamed(FunctionArgExpr::Expr(inner_expr))
                    | FunctionArg::Named {
                        arg: FunctionArgExpr::Expr(inner_expr),
                        ..
                    }
                    | FunctionArg::ExprNamed {
                        arg: FunctionArgExpr::Expr(inner_expr),
                        ..
                    }) = arg
                    else {
                        continue;
                    };
                    sources.extend(extract_expr_lineage(inner_expr, alias_map, source_tables));
                }
            }
            let mut sources = dedup_sources(sources);
            match sources.as_mut_slice() {
                [only] => only.transform = compose_function_transform(&func_name, &only.transform),
                many => {
                    for lineage in many {
                        lineage.transform = compose_multi_source_function(&func_name);
                    }
                }
            }
            sources
        }
        _ => dedup_sources(
            column_refs(expr)
                .into_iter()
                .map(|(qualifier, column)| {
                    column_lineage(
                        qualifier,
                        column,
                        TransformKind::Expression,
                        alias_map,
                        source_tables,
                    )
                })
                .collect(),
        ),
    }
}

/// One value-lineage edge for a column reference, resolving its qualifier
/// through `alias_map`. An unqualified column resolves to the only source
/// table, and to none when several are in scope.
fn column_lineage(
    qualifier: Option<String>,
    column: String,
    transform: TransformKind,
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> ColumnLineage {
    let source_table = match qualifier {
        Some(table) => Some(alias_map.get(&table).cloned().unwrap_or(table)),
        None if source_tables.len() == 1 => Some(
            source_tables[0]
                .alias
                .clone()
                .unwrap_or(source_tables[0].name.clone()),
        ),
        None => None,
    };
    ColumnLineage {
        source_table,
        source_column: column.clone(),
        target_column: column,
        transform,
        physical: false,
        extra_source: false,
    }
}

/// Drops repeated reads of one column (`x * x`), keeping the first.
fn dedup_sources(sources: Vec<ColumnLineage>) -> Vec<ColumnLineage> {
    let mut seen = HashSet::new();
    sources
        .into_iter()
        .filter(|s| seen.insert((s.source_table.clone(), s.source_column.to_lowercase())))
        .collect()
}

/// The edge kind of a cast over an edge of kind `inner`.
fn compose_cast_transform(kind: &CastKind, inner: &TransformKind) -> TransformKind {
    // A fallible cast (`TRY_CAST` / `SAFE_CAST`) yields NULL on a failed
    // conversion, so its output is nullable even over a non-null input.
    // Track it distinctly so typecheck doesn't carry the input's
    // non-null bit through (#1148).
    //
    // The outer cast composes with the inner edge kind; it never
    // overwrites what the inner expression did (#2295).
    match (kind, inner) {
        // Fallible outer cast: nullable output regardless of the inner.
        (CastKind::TryCast | CastKind::SafeCast, _) => TransformKind::TryCast,
        // Infallible outer cast: fallibility is sticky; an inner edge
        // already classified `TryCast` (e.g.
        // `CAST(TRY_CAST(x AS INT) AS BIGINT)`) still returns NULL when
        // the inner conversion fails, so the output stays nullable.
        (CastKind::Cast | CastKind::DoubleColon, TransformKind::TryCast) => TransformKind::TryCast,
        // An infallible cast over a bare column, or over a cast chain
        // with no fallible link, keeps the column's nullability. That
        // is what `Cast` promises. The outer cast's target type is
        // recovered in typecheck Step 2.
        (CastKind::Cast | CastKind::DoubleColon, TransformKind::Direct | TransformKind::Cast) => {
            TransformKind::Cast
        }
        // An infallible cast over a computed value (`CAST(MAX(x) AS
        // BIGINT)`, `CAST(NULLIF(x, 0) AS INT)`, `CAST(a * b AS INT)`) can be
        // NULL when the column is not. It takes the inner expression's
        // nullability, which the edge cannot carry, so it is a general
        // expression.
        (
            CastKind::Cast | CastKind::DoubleColon,
            TransformKind::Aggregation(_) | TransformKind::Expression,
        ) => TransformKind::Expression,
    }
}

/// The edge kind of each column of a call to `func_name` over several columns.
/// The result has no single input column's type, so it is an `Expression`;
/// `COUNT` is the exception, a non-null integer whatever it counts.
fn compose_multi_source_function(func_name: &str) -> TransformKind {
    if func_name == "COUNT" {
        TransformKind::Aggregation(func_name.to_string())
    } else {
        TransformKind::Expression
    }
}

/// The edge kind of a call to `func_name` whose traced argument has edge kind
/// `inner`.
///
/// `Aggregation(f)` means "`f` applied directly to a bare column": typecheck
/// derives the result type from that column's type. A call over a computed
/// argument — `MAX(LENGTH(n))`, `SUM(CAST(y AS DOUBLE))` — does not have the
/// column's type, so it is a general `Expression` (#2295). `COUNT` is the
/// exception: it is a non-null integer whatever its argument is.
fn compose_function_transform(func_name: &str, inner: &TransformKind) -> TransformKind {
    match inner {
        TransformKind::Direct => TransformKind::Aggregation(func_name.to_string()),
        TransformKind::Cast
        | TransformKind::TryCast
        | TransformKind::Aggregation(_)
        | TransformKind::Expression => {
            if func_name == "COUNT" {
                TransformKind::Aggregation(func_name.to_string())
            } else {
                TransformKind::Expression
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Row-selection lineage
// ---------------------------------------------------------------------------

/// A column reference found in an expression: `(qualifier, column)`, where the
/// qualifier is lower-cased.
type ColumnRef = (Option<String>, String);

/// Collects column references from an expression tree without descending into
/// nested queries. A subquery's columns resolve against its own `FROM`, so
/// attributing them to the outer query would invent edges.
#[derive(Default)]
struct ColumnRefCollector {
    query_depth: usize,
    /// Collect references inside nested queries too.
    include_nested: bool,
    refs: Vec<ColumnRef>,
}

impl Visitor for ColumnRefCollector {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0 || self.include_nested {
            match expr {
                Expr::Identifier(ident) => self.refs.push((None, ident.value.clone())),
                Expr::CompoundIdentifier(parts) if parts.len() >= 2 => self.refs.push((
                    Some(parts[parts.len() - 2].value.to_lowercase()),
                    parts[parts.len() - 1].value.clone(),
                )),
                _ => {}
            }
        }
        ControlFlow::Continue(())
    }
}

/// Every column reference in `node`, outside nested queries.
fn column_refs<V: Visit>(node: &V) -> Vec<ColumnRef> {
    let mut collector = ColumnRefCollector::default();
    let _ = node.visit(&mut collector);
    collector.refs
}

/// One column reference anywhere in a statement, as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnReference {
    /// The table qualifier (`t` in `t.col`), lower-cased. `None` when the
    /// column is unqualified.
    pub qualifier: Option<String>,
    /// The column name as written.
    pub column: String,
}

/// Every column reference in `sql`, **including** references inside
/// subqueries, `WITH` bodies and derived tables. No scope resolution is done:
/// this answers "does the SQL mention this column name anywhere", which is
/// the conservative question an impact check needs when lineage could not
/// attribute a reference. Aliases (`expr AS name`) are not references.
pub fn all_column_references(sql: &str) -> Result<Vec<ColumnReference>, String> {
    let dialect = DatabricksDialect;
    let statements = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for stmt in &statements {
        let mut collector = ColumnRefCollector {
            include_nested: true,
            ..Default::default()
        };
        let _ = stmt.visit(&mut collector);
        out.extend(
            collector
                .refs
                .into_iter()
                .map(|(qualifier, column)| ColumnReference { qualifier, column }),
        );
    }
    Ok(out)
}

/// Collects the window specs of window-function calls, outside nested queries.
#[derive(Default)]
struct WindowCollector {
    query_depth: usize,
    windows: Vec<WindowType>,
}

impl Visitor for WindowCollector {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.query_depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if self.query_depth == 0
            && let Expr::Function(func) = expr
            && let Some(over) = &func.over
        {
            self.windows.push(over.clone());
        }
        ControlFlow::Continue(())
    }
}

/// Resolve a window reference to its inline spec. A `WINDOW w AS (…)` name is
/// followed one level; a chain of named references is not resolved.
fn resolve_window<'a>(
    window: &'a WindowType,
    named: &'a [NamedWindowDefinition],
) -> Option<&'a WindowSpec> {
    match window {
        WindowType::WindowSpec(spec) => Some(spec),
        WindowType::NamedWindow(name) => named.iter().find_map(|def| {
            if def.0.value.eq_ignore_ascii_case(&name.value)
                && let NamedWindowExpr::WindowSpec(spec) = &def.1
            {
                Some(spec)
            } else {
                None
            }
        }),
    }
}

/// The output name and expression of each projection item that has one.
fn projection_exprs(projection: &[SelectItem]) -> Vec<(Option<String>, &Expr)> {
    projection
        .iter()
        .filter_map(|item| match item {
            SelectItem::UnnamedExpr(expr) => {
                let name = match expr {
                    Expr::Identifier(ident) => Some(ident.value.clone()),
                    Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.clone()),
                    _ => None,
                };
                Some((name, expr))
            }
            SelectItem::ExprWithAlias { expr, alias } => Some((Some(alias.value.clone()), expr)),
            _ => None,
        })
        .collect()
}

/// Column references of a `GROUP BY` / `HAVING` / `QUALIFY` expression, with
/// projection aliases and ordinals substituted by the projected expression.
///
/// DuckDB, Snowflake and Databricks accept `GROUP BY 1` and `GROUP BY alias`.
/// Recording `alias` as a source column would point at a column that does not
/// exist upstream, so an unqualified name matching a renaming projection alias
/// is replaced by the references of the aliased expression.
fn refs_with_projection_substitution(
    expr: &Expr,
    projection: &[(Option<String>, &Expr)],
    allow_ordinal: bool,
) -> Vec<ColumnRef> {
    if allow_ordinal
        && let Expr::Value(v) = expr
        && let Value::Number(n, _) = &v.value
        && let Ok(idx) = n.parse::<usize>()
        && idx >= 1
        && let Some((_, projected)) = projection.get(idx - 1)
    {
        return column_refs(*projected);
    }
    let mut out = Vec::new();
    for (qualifier, column) in column_refs(expr) {
        if qualifier.is_none()
            && let Some((_, projected)) = projection.iter().find(|(name, projected)| {
                name.as_deref()
                    .is_some_and(|n| n.eq_ignore_ascii_case(&column))
                    // A plain `SELECT col` projects the column under its own
                    // name; substituting it would be a no-op.
                    && !matches!(projected, Expr::Identifier(i) if i.value.eq_ignore_ascii_case(&column))
            })
        {
            out.extend(column_refs(*projected));
            continue;
        }
        out.push((qualifier, column));
    }
    out
}

/// The name a `FROM`/`JOIN` relation is known by (alias first), or `None` for
/// relations that name no columns (`UNNEST`, table functions, …).
fn relation_name(factor: &TableFactor) -> Option<String> {
    match factor {
        TableFactor::Table { name, alias, .. } => Some(
            alias
                .as_ref()
                .map_or_else(|| name.to_string(), |a| a.name.value.clone()),
        ),
        TableFactor::Derived { alias: Some(a), .. } => Some(a.name.value.clone()),
        _ => None,
    }
}

/// The constraint and optional ASOF match condition of a join operator.
fn join_parts(op: &JoinOperator) -> (Option<&JoinConstraint>, Option<&Expr>) {
    match op {
        JoinOperator::Join(c)
        | JoinOperator::Inner(c)
        | JoinOperator::Left(c)
        | JoinOperator::LeftOuter(c)
        | JoinOperator::Right(c)
        | JoinOperator::RightOuter(c)
        | JoinOperator::FullOuter(c)
        | JoinOperator::CrossJoin(c)
        | JoinOperator::Semi(c)
        | JoinOperator::LeftSemi(c)
        | JoinOperator::RightSemi(c)
        | JoinOperator::Anti(c)
        | JoinOperator::LeftAnti(c)
        | JoinOperator::RightAnti(c)
        | JoinOperator::StraightJoin(c) => (Some(c), None),
        JoinOperator::AsOf {
            match_condition,
            constraint,
        } => (Some(constraint), Some(match_condition)),
        JoinOperator::CrossApply
        | JoinOperator::OuterApply
        | JoinOperator::ArrayJoin
        | JoinOperator::LeftArrayJoin
        | JoinOperator::InnerArrayJoin => (None, None),
    }
}

/// Resolve a column reference's table the same way value lineage does: a
/// qualifier goes through the alias map; an unqualified name binds to the only
/// relation when there is exactly one, and stays unknown otherwise.
fn resolve_ref_table(
    qualifier: Option<&str>,
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> Option<String> {
    match qualifier {
        Some(q) => Some(alias_map.get(q).cloned().unwrap_or_else(|| q.to_string())),
        None if source_tables.len() == 1 => Some(
            source_tables[0]
                .alias
                .clone()
                .unwrap_or_else(|| source_tables[0].name.clone()),
        ),
        None => None,
    }
}

/// Row-selection edges of one `SELECT`. See [`RowSelectionLineage`].
fn extract_row_selection(
    select: &Select,
    alias_map: &HashMap<String, String>,
    source_tables: &[TableReference],
) -> Vec<RowSelectionLineage> {
    let mut out: Vec<RowSelectionLineage> = Vec::new();
    let mut push = |refs: Vec<ColumnRef>, kind: RowSelectionKind, target: Option<&str>| {
        for (qualifier, column) in refs {
            let edge = RowSelectionLineage {
                source_table: resolve_ref_table(qualifier.as_deref(), alias_map, source_tables),
                source_column: column,
                kind,
                target_column: target.map(str::to_string),
                physical: false,
            };
            if !out.contains(&edge) {
                out.push(edge);
            }
        }
    };

    // JOIN keys.
    for twj in &select.from {
        let mut preceding: Vec<Option<String>> = vec![relation_name(&twj.relation)];
        for join in &twj.joins {
            let right = relation_name(&join.relation);
            let (constraint, match_condition) = join_parts(&join.join_operator);
            if let Some(cond) = match_condition {
                push(column_refs(cond), RowSelectionKind::JoinKey, None);
            }
            match constraint {
                Some(JoinConstraint::On(expr)) => {
                    push(column_refs(expr), RowSelectionKind::JoinKey, None);
                }
                Some(JoinConstraint::Using(names)) => {
                    // `USING (k)` reads `k` from the right relation and from
                    // the left side. The left side is unambiguous only when a
                    // single relation precedes this join.
                    let left = match preceding.as_slice() {
                        [Some(only)] => Some(only.clone()),
                        _ => None,
                    };
                    for name in names {
                        let Some(column) = name.0.last().and_then(|p| p.as_ident()) else {
                            continue;
                        };
                        for side in [right.as_ref(), left.as_ref()].into_iter().flatten() {
                            push(
                                vec![(Some(side.to_lowercase()), column.value.clone())],
                                RowSelectionKind::JoinKey,
                                None,
                            );
                        }
                    }
                }
                Some(JoinConstraint::Natural | JoinConstraint::None) | None => {}
            }
            preceding.push(right);
        }
    }

    // DISTINCT ON (...) keys.
    if let Some(Distinct::On(keys)) = &select.distinct {
        let projection = projection_exprs(&select.projection);
        for key in keys {
            push(
                refs_with_projection_substitution(key, &projection, true),
                RowSelectionKind::DistinctOn,
                None,
            );
        }
    }

    // WHERE / PREWHERE.
    for expr in [&select.prewhere, &select.selection].into_iter().flatten() {
        push(column_refs(expr), RowSelectionKind::Filter, None);
    }

    let projection = projection_exprs(&select.projection);

    // GROUP BY (ordinals and projection aliases resolve to the projected
    // expression). `GROUP BY ALL` is not resolved.
    if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
        for expr in exprs {
            push(
                refs_with_projection_substitution(expr, &projection, true),
                RowSelectionKind::GroupBy,
                None,
            );
        }
    }
    if let Some(expr) = &select.having {
        push(
            refs_with_projection_substitution(expr, &projection, false),
            RowSelectionKind::Having,
            None,
        );
    }
    if let Some(expr) = &select.qualify {
        push(
            refs_with_projection_substitution(expr, &projection, false),
            RowSelectionKind::Qualify,
            None,
        );
    }

    // Window keys, attributed to the output column the window feeds.
    for (name, expr) in &projection {
        let Some(name) = name.as_deref() else {
            continue;
        };
        let mut collector = WindowCollector::default();
        let _ = expr.visit(&mut collector);
        for window in &collector.windows {
            let Some(spec) = resolve_window(window, &select.named_window) else {
                continue;
            };
            for key in &spec.partition_by {
                push(
                    column_refs(key),
                    RowSelectionKind::WindowPartition,
                    Some(name),
                );
            }
            for key in &spec.order_by {
                push(
                    column_refs(&key.expr),
                    RowSelectionKind::WindowOrder,
                    Some(name),
                );
            }
        }
    }

    out
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
        // `(id)` is `Expr::Nested`, which an unnamed item does not trace: it
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

    /// A nested call or cast composes its edge kind with the inner one instead
    /// of overwriting it (#2295). Before, every row below was labelled with
    /// the outer kind alone: `Cast` or `Aggregation(outer)`.
    #[test]
    fn nested_calls_compose_edge_kinds() {
        let sql = "SELECT CAST(NULLIF(x, 0) AS INT) AS a, CAST(MAX(x) AS BIGINT) AS b, \
                   MAX(LENGTH(n)) AS c, SUM(CAST(y AS DOUBLE)) AS d, \
                   COUNT(UPPER(n)) AS e, TRY_CAST(MAX(x) AS INT) AS f, \
                   MAX(x) AS g, CAST(x AS BIGINT) AS h FROM cat.sch.t";
        let result = extract_lineage(sql).unwrap();
        let kinds: Vec<_> = result
            .columns
            .iter()
            .map(|c| (c.target_column.as_str(), c.transform.clone()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                ("a", TransformKind::Expression),
                ("b", TransformKind::Expression),
                ("c", TransformKind::Expression),
                ("d", TransformKind::Expression),
                ("e", TransformKind::Aggregation("COUNT".to_string())),
                ("f", TransformKind::TryCast),
                ("g", TransformKind::Aggregation("MAX".to_string())),
                ("h", TransformKind::Cast),
            ]
        );
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
             1 AS one, total_revenue / order_count AS avg_order_value \
             FROM cat.sch.customer_orders GROUP BY customer_id",
        )
        .unwrap();
        let by_name: std::collections::HashMap<_, _> = result
            .columns
            .iter()
            .map(|c| (c.target_column.as_str(), c))
            .collect();
        // COUNT(*) and the literal both lack a source column, but are present
        // in the column set.
        let order_count = by_name.get("order_count").expect("order_count kept");
        assert_eq!(order_count.source_table, None);
        let one = by_name.get("one").expect("one kept");
        assert_eq!(one.source_table, None);
        // The division reads two columns: one edge each (#2318). Lineage does
        // not substitute a sibling alias, so `order_count` is attributed to the
        // table, the same as a bare `order_count AS x` always was.
        let avg: Vec<_> = result
            .columns
            .iter()
            .filter(|c| c.target_column == "avg_order_value")
            .map(|c| (c.source_column.as_str(), c.extra_source))
            .collect();
        assert_eq!(avg, vec![("total_revenue", false), ("order_count", true)]);
        // The sourced column still resolves normally.
        assert!(by_name.contains_key("customer_id"));
    }

    /// #1892: a `WITH`-bound name looks exactly like a table read, and a
    /// consumer that cannot tell them apart derives an edge to a model that
    /// the query never reads.
    fn origin(sql: &str) -> Vec<(Option<String>, String, String)> {
        extract_lineage(sql)
            .unwrap()
            .columns
            .into_iter()
            .map(|c| (c.source_table, c.source_column, c.target_column))
            .collect()
    }

    /// #2307: a column read through a CTE names the CTE body's physical
    /// origin, never the CTE's own name (which a same-named source would
    /// capture).
    #[test]
    fn a_cte_column_traces_to_its_bodys_origin() {
        let some = |t: &str, c: &str, x: &str| (Some(t.to_string()), c.to_string(), x.to_string());
        // Shadowed name.
        assert_eq!(
            origin("WITH t AS (SELECT n AS x FROM u) SELECT x FROM t"),
            vec![some("u", "n", "x")]
        );
        assert_eq!(
            origin("WITH t AS (SELECT n AS x FROM u) SELECT a.x FROM t AS a"),
            vec![some("u", "n", "x")]
        );
        // Import CTE: the star passes the same-named physical table through.
        assert_eq!(
            origin("WITH orders AS (SELECT * FROM orders) SELECT id FROM orders"),
            vec![some("orders", "id", "id")]
        );
        // A chain of CTEs, by column and by star.
        assert_eq!(
            origin(
                "WITH a AS (SELECT n AS x FROM u AS uu), b AS (SELECT x AS y FROM a) SELECT y FROM b"
            ),
            vec![some("u", "n", "y")]
        );
        assert_eq!(
            origin("WITH a AS (SELECT * FROM raw.o), b AS (SELECT * FROM a) SELECT id FROM b"),
            vec![some("raw.o", "id", "id")]
        );
        // A star over two tables cannot tell which one owns the column.
        assert_eq!(
            origin("WITH c AS (SELECT * FROM p, q) SELECT id FROM c"),
            vec![(None, "id".to_string(), "id".to_string())]
        );
        // A transform in the body carries through a plain outer read.
        let r =
            extract_lineage("WITH c AS (SELECT TRY_CAST(n AS INT) AS x FROM u) SELECT x FROM c")
                .unwrap();
        assert_eq!(r.columns[0].transform, TransformKind::TryCast);
    }

    /// #2307: a CTE reference carries the body's traced columns, and an import
    /// CTE's star lists the physical table it passes through.
    #[test]
    fn a_cte_reference_carries_its_bodys_columns() {
        let r = extract_lineage("WITH c AS (SELECT n AS x FROM u) SELECT * FROM c").unwrap();
        let c = &r.source_tables[0].cte_columns;
        assert_eq!(c.len(), 1);
        assert_eq!(c[0].source_table.as_deref(), Some("u"));
        assert_eq!(c[0].source_column, "n");
        assert_eq!(c[0].target_column, "x");
        let r =
            extract_lineage("WITH orders AS (SELECT * FROM orders) SELECT * FROM orders").unwrap();
        assert_eq!(r.source_tables[0].binding, TableBinding::Cte);
        assert_eq!(
            r.source_tables[0].derived_sources,
            vec!["orders".to_string()]
        );
    }

    /// #2307: a row-selection edge through a CTE names the body's origin.
    #[test]
    fn a_row_selection_edge_through_a_cte_names_the_bodys_origin() {
        let r = extract_lineage("WITH c AS (SELECT n AS x FROM u) SELECT x FROM c WHERE x > 1")
            .unwrap();
        let filter = r
            .row_selection
            .iter()
            .find(|rs| rs.kind == RowSelectionKind::Filter)
            .unwrap();
        assert_eq!(filter.source_table.as_deref(), Some("u"));
        assert_eq!(filter.source_column, "n");
    }

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

    /// A CTE body that is a SET OPERATION is walked: both branches' reads
    /// surface. This was a pinned gap — `UNION ALL` inside a CTE is the
    /// ordinary spelling, and the body used to contribute nothing.
    #[test]
    fn a_set_operation_cte_body_contributes_both_branches() {
        let sql = "WITH walk AS ( \
                     SELECT id FROM seed \
                     UNION ALL \
                     SELECT id FROM other \
                   ) SELECT id FROM walk";

        assert_eq!(
            referenced_tables(sql).unwrap(),
            vec!["other".to_string(), "seed".to_string()]
        );
    }

    /// A recursive CTE's self-reference in the right branch is a CTE binding,
    /// not a read.
    #[test]
    fn a_recursive_union_cte_does_not_read_itself() {
        let sql = "WITH RECURSIVE walk AS ( \
                     SELECT id FROM seed \
                     UNION ALL \
                     SELECT id FROM walk \
                   ) SELECT id FROM walk";

        assert_eq!(referenced_tables(sql).unwrap(), vec!["seed".to_string()]);
    }

    /// A top-level set operation used to fail lineage outright ("unsupported
    /// query type"), which failed dependency resolution for the whole project.
    /// Output columns come from the left branch; the right branch's reads are
    /// dependencies.
    #[test]
    fn a_top_level_union_takes_columns_from_the_left_and_reads_from_both() {
        let result = extract_lineage(
            "SELECT a.id, a.amount FROM left_t a \
             UNION ALL SELECT b.id, b.total FROM right_t b \
             UNION ALL SELECT c.id, c.total FROM third_t c",
        )
        .unwrap();
        let targets: Vec<&str> = result
            .columns
            .iter()
            .map(|c| c.target_column.as_str())
            .collect();
        assert_eq!(targets, vec!["id", "amount"]);
        assert_eq!(
            result.source_tables.len(),
            1,
            "only the left branch projects"
        );
        assert_eq!(result.source_tables[0].name, "left_t");
        assert_eq!(
            result.nested_sources,
            vec!["right_t".to_string(), "third_t".to_string()]
        );
        assert!(!result.has_star);
    }

    #[test]
    fn a_union_with_a_values_branch_still_extracts() {
        let result = extract_lineage("SELECT id, name FROM t UNION ALL VALUES (1, 'x')").unwrap();
        assert_eq!(result.source_tables[0].name, "t");
        assert_eq!(result.columns.len(), 2);
    }

    #[test]
    fn a_union_under_with_resolves_cte_reads_in_both_branches() {
        let sql = "WITH x AS (SELECT id FROM base) \
                   SELECT * FROM x UNION ALL SELECT id FROM extra";

        assert_eq!(
            referenced_tables(sql).unwrap(),
            vec!["base".to_string(), "extra".to_string()]
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

    // --- Row-selection lineage --------------------------------------------

    fn row_sel(sql: &str) -> Vec<(Option<String>, String, RowSelectionKind, Option<String>)> {
        extract_lineage(sql)
            .unwrap()
            .row_selection
            .into_iter()
            .map(|r| (r.source_table, r.source_column, r.kind, r.target_column))
            .collect()
    }

    fn has_edge(
        edges: &[(Option<String>, String, RowSelectionKind, Option<String>)],
        table: Option<&str>,
        column: &str,
        kind: RowSelectionKind,
        target: Option<&str>,
    ) -> bool {
        edges.iter().any(|(t, c, k, tc)| {
            t.as_deref() == table && c == column && *k == kind && tc.as_deref() == target
        })
    }

    #[test]
    fn distinct_on_keys_are_row_selection_edges() {
        let edges = row_sel(
            "SELECT DISTINCT ON (customer_id) customer_id, amount FROM raw.orders \
             ORDER BY customer_id, created_at DESC",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::DistinctOn,
            None
        ));
        // The ORDER BY keys pick which row of each customer_id group survives,
        // so they are DISTINCT ON edges too.
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "created_at",
            RowSelectionKind::DistinctOn,
            None
        ));
        // No LIMIT: no order_limit edge.
        assert!(!edges.iter().any(|e| e.2 == RowSelectionKind::OrderLimit));
    }

    #[test]
    fn order_by_with_limit_keys_are_row_selection_edges() {
        let edges = row_sel(
            "SELECT o.order_id, o.amount AS amt FROM raw.orders o \
             ORDER BY amt DESC, o.created_at LIMIT 10",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "amount",
            RowSelectionKind::OrderLimit,
            None
        ));
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "created_at",
            RowSelectionKind::OrderLimit,
            None
        ));
    }

    #[test]
    fn order_by_without_limit_is_not_a_row_selection() {
        let edges = row_sel("SELECT order_id FROM raw.orders ORDER BY created_at");
        assert!(edges.is_empty(), "{edges:?}");
    }

    #[test]
    fn order_by_with_fetch_first_is_a_row_selection() {
        let edges =
            row_sel("SELECT order_id FROM raw.orders ORDER BY created_at FETCH FIRST 3 ROWS ONLY");
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "created_at",
            RowSelectionKind::OrderLimit,
            None
        ));
    }

    /// `NATURAL JOIN` keys are the columns both sides share. Only the warehouse
    /// catalog knows them, so the extractor cannot name them from SQL text.
    /// Pinned so a future schema-aware pass notices this test.
    #[test]
    fn natural_join_keys_are_not_resolvable_from_sql_text() {
        let edges = row_sel("SELECT a.x FROM raw.a a NATURAL JOIN raw.b b");
        assert!(
            !edges.iter().any(|e| e.2 == RowSelectionKind::JoinKey),
            "{edges:?}"
        );
    }

    #[test]
    fn join_keys_are_row_selection_edges() {
        let edges = row_sel(
            "SELECT o.order_id, c.customer_name FROM raw.orders o \
             JOIN raw.customers c ON o.customer_id = c.customer_id",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::JoinKey,
            None
        ));
        assert!(has_edge(
            &edges,
            Some("raw.customers"),
            "customer_id",
            RowSelectionKind::JoinKey,
            None
        ));
        assert_eq!(edges.len(), 2, "{edges:?}");
    }

    #[test]
    fn join_using_reads_both_sides() {
        let edges = row_sel("SELECT a.x FROM a JOIN b USING (k)");
        assert!(has_edge(
            &edges,
            Some("a"),
            "k",
            RowSelectionKind::JoinKey,
            None
        ));
        assert!(has_edge(
            &edges,
            Some("b"),
            "k",
            RowSelectionKind::JoinKey,
            None
        ));
    }

    #[test]
    fn filters_group_keys_and_having_are_row_selection_edges() {
        let edges = row_sel(
            "SELECT customer_id, SUM(amount) AS total FROM raw.orders \
             WHERE status = 'paid' GROUP BY customer_id HAVING SUM(amount) > 10",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "status",
            RowSelectionKind::Filter,
            None
        ));
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::GroupBy,
            None
        ));
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "amount",
            RowSelectionKind::Having,
            None
        ));
    }

    #[test]
    fn value_lineage_is_unchanged_by_row_selection() {
        // Backward compatibility: `columns` stays value-only. The filter
        // column `status` must not appear as a value edge.
        let result = extract_lineage(
            "SELECT customer_id, SUM(amount) AS total FROM raw.orders \
             WHERE status = 'paid' GROUP BY customer_id",
        )
        .unwrap();
        let sources: Vec<_> = result
            .columns
            .iter()
            .map(|c| c.source_column.as_str())
            .collect();
        assert_eq!(sources, vec!["customer_id", "amount"]);
    }

    #[test]
    fn group_by_ordinal_and_alias_resolve_to_the_projected_column() {
        let edges = row_sel(
            "SELECT date_trunc('month', order_date) AS m, COUNT(*) AS n FROM raw.orders GROUP BY 1",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "order_date",
            RowSelectionKind::GroupBy,
            None
        ));
        let edges = row_sel(
            "SELECT date_trunc('month', order_date) AS m, COUNT(*) AS n FROM raw.orders GROUP BY m",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "order_date",
            RowSelectionKind::GroupBy,
            None
        ));
        assert!(!edges.iter().any(|(_, c, _, _)| c == "m"), "{edges:?}");
    }

    #[test]
    fn window_keys_attach_to_their_output_column() {
        let edges = row_sel(
            "SELECT order_id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date) AS rn \
             FROM raw.orders QUALIFY rn = 1",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::WindowPartition,
            Some("rn")
        ));
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "order_date",
            RowSelectionKind::WindowOrder,
            Some("rn")
        ));
        // QUALIFY rn → the window's own keys (alias substitution).
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::Qualify,
            None
        ));
    }

    #[test]
    fn named_window_resolves_one_level() {
        let edges = row_sel(
            "SELECT SUM(amount) OVER w AS running FROM raw.orders WINDOW w AS (PARTITION BY customer_id)",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::WindowPartition,
            Some("running")
        ));
    }

    #[test]
    fn subquery_predicates_are_not_attributed_to_the_outer_query() {
        let edges = row_sel(
            "SELECT order_id FROM raw.orders WHERE customer_id IN (SELECT customer_id FROM raw.vip WHERE tier = 'gold')",
        );
        assert!(has_edge(
            &edges,
            Some("raw.orders"),
            "customer_id",
            RowSelectionKind::Filter,
            None
        ));
        assert!(!edges.iter().any(|(_, c, _, _)| c == "tier"), "{edges:?}");
    }

    #[test]
    fn unqualified_reference_in_a_join_has_no_table() {
        let edges = row_sel("SELECT a.x FROM a JOIN b ON a.k = b.k WHERE flag");
        assert!(has_edge(
            &edges,
            None,
            "flag",
            RowSelectionKind::Filter,
            None
        ));
    }

    #[test]
    fn all_column_references_includes_nested_queries_but_not_aliases() {
        let refs = all_column_references(
            "WITH s AS (SELECT amount FROM stg) SELECT o.order_id, x AS amount_alias FROM s o \
             WHERE o.id IN (SELECT v.id FROM vip v)",
        )
        .unwrap();
        let has = |q: Option<&str>, c: &str| {
            refs.iter()
                .any(|r| r.qualifier.as_deref() == q && r.column == c)
        };
        assert!(has(None, "amount"));
        assert!(has(Some("o"), "order_id"));
        assert!(has(Some("v"), "id"));
        assert!(has(None, "x"));
        assert!(!refs.iter().any(|r| r.column == "amount_alias"));
    }

    #[test]
    fn plain_select_has_no_row_selection_and_serializes_without_the_field() {
        let result = extract_lineage("SELECT order_id FROM raw.orders").unwrap();
        assert!(result.row_selection.is_empty());
        let json = serde_json::to_string(&result).unwrap();
        assert!(!json.contains("row_selection"));
    }

    /// #2318: every column an expression reads is a value source, whatever
    /// operator combines them. Returns `(source_table, source_column,
    /// transform)` for each edge of the `amount` output, sorted.
    fn amount_sources(sql: &str) -> Vec<(Option<String>, String, TransformKind)> {
        let mut edges: Vec<_> = extract_lineage(sql)
            .unwrap()
            .columns
            .into_iter()
            .filter(|c| c.target_column == "amount")
            .map(|c| (c.source_table, c.source_column, c.transform))
            .collect();
        edges.sort_by(|a, b| (&a.0, &a.1).cmp(&(&b.0, &b.1)));
        edges
    }

    fn expr_edge(t: &str, c: &str) -> (Option<String>, String, TransformKind) {
        (
            Some(t.to_string()),
            c.to_string(),
            TransformKind::Expression,
        )
    }

    #[test]
    fn a_binary_operator_traces_both_operands() {
        assert_eq!(
            amount_sources("SELECT x + 1 AS amount FROM a"),
            vec![expr_edge("a", "x")]
        );
        assert_eq!(
            amount_sources("SELECT a.x * b.y AS amount FROM a JOIN b ON a.id = b.id"),
            vec![expr_edge("a", "x"), expr_edge("b", "y")]
        );
    }

    #[test]
    fn nested_arithmetic_across_two_tables_traces_every_column() {
        assert_eq!(
            amount_sources(
                "SELECT CAST((a.x + 2) * (b.y - a.z) AS DECIMAL(18, 2)) AS amount \
                 FROM a JOIN b ON a.id = b.id"
            ),
            vec![
                expr_edge("a", "x"),
                expr_edge("a", "z"),
                expr_edge("b", "y")
            ]
        );
    }

    #[test]
    fn case_when_traces_conditions_and_branches() {
        assert_eq!(
            amount_sources(
                "SELECT CASE WHEN a.flag THEN a.x ELSE b.y END AS amount \
                 FROM a JOIN b ON a.id = b.id"
            ),
            vec![
                expr_edge("a", "flag"),
                expr_edge("a", "x"),
                expr_edge("b", "y")
            ]
        );
    }

    #[test]
    fn unary_minus_traces_its_operand() {
        assert_eq!(
            amount_sources("SELECT -x AS amount FROM a"),
            vec![expr_edge("a", "x")]
        );
    }

    #[test]
    fn between_traces_all_three_operands() {
        assert_eq!(
            amount_sources(
                "SELECT a.x BETWEEN a.lo AND b.hi AS amount FROM a JOIN b ON a.id = b.id"
            ),
            vec![
                expr_edge("a", "lo"),
                expr_edge("a", "x"),
                expr_edge("b", "hi")
            ]
        );
    }

    #[test]
    fn in_list_and_multi_column_function_trace_every_column() {
        assert_eq!(
            amount_sources("SELECT x IN (y, 3) AS amount FROM a"),
            vec![expr_edge("a", "x"), expr_edge("a", "y")]
        );
        assert_eq!(
            amount_sources("SELECT COALESCE(a.x, b.y) AS amount FROM a JOIN b ON a.id = b.id"),
            vec![expr_edge("a", "x"), expr_edge("b", "y")]
        );
        // One column under a function keeps the function's edge kind.
        assert_eq!(
            amount_sources("SELECT UPPER(x) AS amount FROM a"),
            vec![(
                Some("a".to_string()),
                "x".to_string(),
                TransformKind::Aggregation("UPPER".to_string())
            )]
        );
    }

    #[test]
    fn a_function_over_arithmetic_is_an_expression() {
        assert_eq!(
            amount_sources("SELECT SUM(a.x * b.y) AS amount FROM a JOIN b ON a.id = b.id"),
            vec![expr_edge("a", "x"), expr_edge("b", "y")]
        );
        assert_eq!(
            amount_sources("SELECT SUM(x + 1) AS amount FROM a"),
            vec![expr_edge("a", "x")]
        );
    }

    #[test]
    fn a_parenthesised_column_is_a_direct_edge() {
        assert_eq!(
            amount_sources("SELECT (x) AS amount FROM a"),
            vec![(
                Some("a".to_string()),
                "x".to_string(),
                TransformKind::Direct
            )]
        );
    }

    #[test]
    fn an_unnamed_multi_column_expression_invents_no_output_column() {
        let result = extract_lineage("SELECT a + b FROM t").unwrap();
        assert!(result.columns.is_empty());
        assert_eq!(result.unresolved_projections, 1);
    }

    #[test]
    fn a_cte_column_built_from_two_columns_traces_to_both_origins() {
        let mut got = origin(
            "WITH c AS (SELECT a.x * b.y AS amount FROM a JOIN b ON a.id = b.id) \
             SELECT amount FROM c",
        );
        got.sort();
        assert_eq!(
            got,
            vec![
                (Some("a".into()), "x".into(), "amount".into()),
                (Some("b".into()), "y".into(), "amount".into()),
            ]
        );
    }

    /// Transform of each edge into `amount`, in source order.
    fn amount_kinds(sql: &str) -> Vec<TransformKind> {
        extract_lineage(sql)
            .unwrap()
            .columns
            .into_iter()
            .filter(|c| c.target_column == "amount" || c.target_column == "m")
            .map(|c| c.transform)
            .collect()
    }

    #[test]
    fn an_outer_function_or_cast_over_a_cte_expression_is_an_expression() {
        let cte = "WITH c AS (SELECT x * 1.5 AS amt FROM t) ";
        let kinds = |outer: &str| amount_kinds(&format!("{cte}{outer} FROM c"));
        assert_eq!(
            kinds("SELECT MAX(amt) AS m"),
            vec![TransformKind::Expression]
        );
        assert_eq!(
            kinds("SELECT SUM(amt) AS m"),
            vec![TransformKind::Expression]
        );
        assert_eq!(
            kinds("SELECT CAST(amt AS INT) AS m"),
            vec![TransformKind::Expression]
        );
        assert_eq!(kinds("SELECT amt AS m"), vec![TransformKind::Expression]);
        assert_eq!(
            kinds("SELECT COUNT(amt) AS m"),
            vec![TransformKind::Aggregation("COUNT".into())]
        );
        assert_eq!(
            kinds("SELECT TRY_CAST(amt AS INT) AS m"),
            vec![TransformKind::TryCast]
        );
        // Two origins: the same rules.
        let two = "WITH c AS (SELECT a.x * b.y AS amt FROM a JOIN b ON a.id = b.id) \
                   SELECT SUM(amt) AS m FROM c";
        assert_eq!(
            amount_kinds(two),
            vec![TransformKind::Expression, TransformKind::Expression]
        );
        // A bare column through a CTE stays what the body did.
        assert_eq!(
            amount_kinds("WITH c AS (SELECT x AS amount FROM t) SELECT MAX(amount) AS m FROM c"),
            vec![TransformKind::Aggregation("MAX".into())]
        );
    }

    #[test]
    fn an_unnamed_expression_adds_no_output_column() {
        let names = |sql: &str| -> Vec<String> {
            extract_lineage(sql)
                .unwrap()
                .columns
                .into_iter()
                .map(|c| c.target_column)
                .collect()
        };
        assert_eq!(names("SELECT id, amount * 100 FROM orders"), vec!["id"]);
        assert_eq!(names("SELECT x, x + 1 FROM orders"), vec!["x"]);
    }
}
