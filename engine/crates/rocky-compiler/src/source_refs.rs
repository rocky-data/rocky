//! Missing-column checks against external source schemas (E041 / W041), and
//! missing-table checks against the schemas they live in (E045 / W045, see
//! [`check_source_table_refs`]).
//!
//! The type checker treats a reference it cannot resolve as
//! [`crate::types::RockyType::Unknown`]. For an in-project upstream, E039
//! (see `typecheck.rs`) already refuses a projection that names a column the
//! upstream provably does not output. This module is the counterpart for
//! **external sources** — the `FROM raw.orders` tables Rocky knows only
//! through [`crate::compile::CompilerConfig::source_schemas`].
//!
//! Absence is only a fact when two things hold:
//!
//! 1. **The schema is trustworthy.** Every source schema carries a
//!    [`SourceSchemaOrigin`]: introspected live in this invocation, read from
//!    the schema cache (with its timestamp), or derived from a seed file. A
//!    source with no recorded origin is never checked. Live and
//!    trusted-fresh cache schemas yield the `E041` error; seed and aged cache
//!    schemas yield the `W041` warning, because the warehouse may already
//!    carry the column. [`SourceProvenance::strict`] escalates every `W041`
//!    to `E041`.
//! 2. **The reference binds unambiguously.** A small binder walks each
//!    `SELECT` scope. A name is reported only when every relation it could
//!    resolve against — in its own scope and every enclosing scope, for
//!    correlated and implicitly-lateral subqueries — is a known source, and
//!    none of them, no `SELECT` alias, and no relation binding matches it. A
//!    CTE, derived table, in-project model, table function or any other
//!    relation whose columns Rocky cannot enumerate makes the name
//!    unprovable, and it stays `Unknown`. A qualified `a.b` is reported only
//!    when `a` names exactly one source binding and cannot also be a column
//!    (a struct field access such as `payload.field`).
//!
//! Quoted names are never reported: this dialect parses `"shipped"` as an
//! identifier, but BigQuery (and Databricks by default) read it as a string.
//! Neither are `_`-prefixed names, the warehouses' metadata-column convention.
//!
//! Anything the binder does not model — set-operation `ORDER BY`, window
//! specs, lambdas, `LATERAL VIEW`, 3-part references, function arguments that
//! may be keywords (`DATEADD(day, …)`) — is skipped, never guessed.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sqlparser::ast::{
    self, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, Ident,
    JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E041, E045, SourceSpan, W041, W045};
use crate::types::TypedColumn;

/// Where a source schema handed to the compiler came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceSchemaOrigin {
    /// Introspected from the warehouse during this invocation.
    Live,
    /// Read from the persisted schema cache.
    Cache {
        /// When the entry was written.
        cached_at: DateTime<Utc>,
        /// Whether the entry is younger than
        /// `[cache.schemas] trusted_max_age_seconds`. The loader decides this
        /// against its own clock so the check itself stays deterministic.
        trusted: bool,
    },
    /// Derived from a seed file (`rocky compile --with-seed`).
    Seed,
}

impl SourceSchemaOrigin {
    fn describe(&self) -> String {
        match self {
            Self::Live => "schema introspected from the warehouse during this run".to_string(),
            Self::Cache { cached_at, .. } => format!(
                "schema from the schema cache, written {}",
                cached_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
            ),
            Self::Seed => "schema from the seed file".to_string(),
        }
    }

    fn is_current(&self) -> bool {
        match self {
            Self::Live => true,
            Self::Cache { trusted, .. } => *trusted,
            Self::Seed => false,
        }
    }
}

/// Provenance of the source schemas in a compile, keyed like
/// [`crate::compile::CompilerConfig::source_schemas`].
///
/// The default — no origins, not strict — disables the check entirely, which
/// is what every caller that does not know where its schemas came from gets.
#[derive(Debug, Clone, Default)]
pub struct SourceProvenance {
    /// Origin of each source schema. A source schema with no entry here is
    /// never used to prove a column absent.
    pub origins: HashMap<String, SourceSchemaOrigin>,
    /// Escalate findings against seed and untrusted cache schemas from `W041`
    /// to `E041` (`--strict-sources` / `[cache.schemas] strict_sources`).
    pub strict: bool,
}

impl SourceProvenance {
    /// Record the same origin for every key.
    pub fn uniform<'a>(
        keys: impl IntoIterator<Item = &'a String>,
        origin: &SourceSchemaOrigin,
    ) -> Self {
        Self {
            origins: keys.into_iter().map(|key| (key.clone(), *origin)).collect(),
            strict: false,
        }
    }

    /// Builder: set [`Self::strict`].
    #[must_use]
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }
}

/// A source schema eligible to prove absence.
struct KnownSource<'a> {
    key: &'a str,
    columns: HashSet<String>,
    display_columns: Vec<&'a str>,
    origin: &'a SourceSchemaOrigin,
}

impl KnownSource<'_> {
    fn has(&self, lower: &str) -> bool {
        self.columns.contains(lower)
    }
}

#[derive(Clone)]
enum RelKind<'s, 'a> {
    Source(&'s KnownSource<'a>),
    /// A relation whose column set Rocky cannot enumerate.
    Opaque,
}

#[derive(Clone)]
struct Rel<'s, 'a> {
    /// Lower-cased name the relation is addressable by (alias, or the last
    /// part of a table name). `None` when the relation has no usable name.
    binding: Option<String>,
    kind: RelKind<'s, 'a>,
}

#[derive(Clone, Default)]
struct Scope<'s, 'a> {
    rels: Vec<Rel<'s, 'a>>,
    /// Lower-cased `SELECT` aliases. Treated as visible everywhere in the
    /// scope: DuckDB resolves lateral aliases in the projection, `WHERE`,
    /// `GROUP BY` and `HAVING`.
    aliases: HashSet<String>,
}

enum Resolution<'s, 'a> {
    Resolved,
    /// Cannot tell — stay `Unknown`.
    Unprovable,
    Missing(Vec<&'s KnownSource<'a>>),
}

struct Finding<'s, 'a> {
    column: String,
    sources: Vec<&'s KnownSource<'a>>,
    line: u64,
    col: u64,
}

struct Binder<'s, 'a> {
    sources: &'s HashMap<String, KnownSource<'a>>,
    findings: Vec<Finding<'s, 'a>>,
}

/// Check every model for direct references to columns absent from a source
/// schema with known provenance. See the module docs for when it fires.
pub fn check_source_column_refs(
    models: &[rocky_core::models::Model],
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    provenance: &SourceProvenance,
) -> Vec<Diagnostic> {
    if provenance.origins.is_empty() {
        return Vec::new();
    }
    let sources = known_sources(source_schemas, provenance);
    if sources.is_empty() {
        return Vec::new();
    }
    let mut diagnostics = Vec::new();
    for model in models {
        diagnostics.extend(check_model(model, &sources, provenance.strict));
    }
    diagnostics
}

/// Index source schemas by lower-cased key. Keys that collide
/// case-insensitively are dropped: Rocky cannot tell which one a reference
/// reads.
fn known_sources<'a>(
    source_schemas: &'a HashMap<String, Vec<TypedColumn>>,
    provenance: &'a SourceProvenance,
) -> HashMap<String, KnownSource<'a>> {
    let mut out: HashMap<String, KnownSource<'a>> = HashMap::new();
    let mut collided: HashSet<String> = HashSet::new();
    for (key, columns) in source_schemas {
        let Some(origin) = provenance.origins.get(key) else {
            continue;
        };
        // An empty column list is not a schema; it proves nothing.
        if columns.is_empty() {
            continue;
        }
        let lower = key.to_lowercase();
        if out.contains_key(&lower) {
            collided.insert(lower);
            continue;
        }
        out.insert(
            lower,
            KnownSource {
                key,
                columns: columns.iter().map(|c| c.name.to_lowercase()).collect(),
                display_columns: columns.iter().map(|c| c.name.as_str()).collect(),
                origin,
            },
        );
    }
    for key in collided {
        out.remove(&key);
    }
    out
}

fn check_model(
    model: &rocky_core::models::Model,
    sources: &HashMap<String, KnownSource<'_>>,
    strict: bool,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
    else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut binder = Binder {
        sources,
        findings: Vec::new(),
    };
    binder.check_query(query, &[], &HashSet::new());

    let model_name = model.config.name.as_str();
    let file = model.file_path.display().to_string();
    let sql_offset = sql_position_in_file(model);

    let mut seen: HashSet<(String, Vec<&str>)> = HashSet::new();
    let mut diagnostics = Vec::new();
    for finding in binder.findings {
        let mut keys: Vec<&str> = finding.sources.iter().map(|s| s.key).collect();
        keys.sort_unstable();
        keys.dedup();
        if !seen.insert((finding.column.to_lowercase(), keys)) {
            continue;
        }
        let mut diagnostic = build_diagnostic(model_name, &finding, strict);
        let (line, col) = match sql_offset {
            Some((line_offset, col_offset)) if finding.line > 0 => {
                let line = finding.line as usize;
                let col = finding.col.max(1) as usize;
                if line == 1 {
                    (line + line_offset, col + col_offset)
                } else {
                    (line + line_offset, col)
                }
            }
            _ => (1, 1),
        };
        diagnostic = diagnostic.with_span(SourceSpan {
            file: file.clone(),
            line,
            col,
        });
        diagnostics.push(diagnostic);
    }
    diagnostics
}

/// Where the model SQL the binder parsed starts in its `.sql` file, as
/// `(lines before, columns before on that line)`.
///
/// `None` when the parsed text is not a verbatim slice of the file: a `.rocky`
/// model's lowered SQL, or SQL rewritten by `@var()` substitution. Frontmatter
/// (`---toml`) before the body is accounted for by the offset.
fn sql_position_in_file(model: &rocky_core::models::Model) -> Option<(usize, usize)> {
    if !model
        .file_path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"))
    {
        return None;
    }
    let text = std::fs::read_to_string(&model.file_path).ok()?;
    let start = text.find(model.sql.as_str())?;
    let before = &text[..start];
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    Some((
        before.matches('\n').count(),
        before[line_start..].chars().count(),
    ))
}

fn build_diagnostic(model_name: &str, finding: &Finding<'_, '_>, strict: bool) -> Diagnostic {
    let column = finding.column.as_str();
    let mut sources = finding.sources.clone();
    sources.sort_by_key(|s| s.key);
    sources.dedup_by_key(|s| s.key);
    let current = sources.iter().all(|s| s.origin.is_current());
    let is_error = current || strict;

    let where_ = match sources.as_slice() {
        [single] => format!("source '{}' ({})", single.key, single.origin.describe()),
        many => format!(
            "any source in scope: {}",
            many.iter()
                .map(|s| format!("'{}' ({})", s.key, s.origin.describe()))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    };

    let mut suggestion = close_names_hint(column, &sources);
    if is_error {
        let message = if current {
            format!("column '{column}' does not exist in {where_}")
        } else {
            format!(
                "column '{column}' does not exist in {where_}; strict sources are on, so a \
                 possibly out-of-date schema is treated as authoritative"
            )
        };
        if !current {
            suggestion.push_str(
                ". If the warehouse has this column, refresh the schema: fix the seed file, or \
                 re-warm the cache with `rocky discover --with-schemas`",
            );
        }
        Diagnostic::error(E041, model_name, message).with_suggestion(suggestion)
    } else {
        suggestion.push_str(
            ". If the warehouse has this column, refresh the schema: fix the seed file, or \
             re-warm the cache with `rocky discover --with-schemas`. To make this an error, \
             pass `--strict-sources` or set `[cache.schemas] strict_sources = true`",
        );
        Diagnostic::warning(
            W041,
            model_name,
            format!("column '{column}' was not found in {where_}; the schema may be out of date"),
        )
        .with_suggestion(suggestion)
    }
}

/// "did you mean …" from edit distance, else the available column list.
fn close_names_hint(column: &str, sources: &[&KnownSource<'_>]) -> String {
    let wanted = column.to_lowercase();
    let threshold = (wanted.chars().count() / 3).max(1);
    let mut scored: Vec<(usize, &str)> = sources
        .iter()
        .flat_map(|s| s.display_columns.iter().copied())
        .map(|name| (strsim::levenshtein(&wanted, &name.to_lowercase()), name))
        .filter(|(distance, _)| *distance <= threshold)
        .collect();
    scored.sort_unstable();
    scored.dedup_by(|a, b| a.1.eq_ignore_ascii_case(b.1));
    if !scored.is_empty() {
        let names: Vec<String> = scored
            .iter()
            .take(3)
            .map(|(_, name)| format!("'{name}'"))
            .collect();
        return format!("did you mean {}?", names.join(" or "));
    }
    const MAX_LISTED: usize = 12;
    let mut all: Vec<&str> = sources
        .iter()
        .flat_map(|s| s.display_columns.iter().copied())
        .collect();
    let total = all.len();
    all.truncate(MAX_LISTED);
    let mut listed = all.join(", ");
    if total > MAX_LISTED {
        listed.push_str(&format!(", … ({} more)", total - MAX_LISTED));
    }
    format!("available columns: {listed}")
}

/// Names the warehouse resolves without any relation: niladic keywords and
/// pseudo-columns.
fn is_relation_free_name(lower: &str) -> bool {
    matches!(
        lower,
        "current_date"
            | "current_time"
            | "current_timestamp"
            | "current_user"
            | "current_role"
            | "current_catalog"
            | "current_schema"
            | "current_database"
            | "session_user"
            | "user"
            | "localtime"
            | "localtimestamp"
            | "sysdate"
            | "systimestamp"
            | "current_datetime"
            | "current_timezone"
            | "null"
            | "true"
            | "false"
    ) || crate::typecheck::is_warehouse_pseudo_column(lower)
        // Warehouse metadata columns are conventionally underscore-prefixed
        // and absent from introspected schemas (BigQuery `_FILE_NAME`,
        // `_TABLE_SUFFIX`; Databricks change-feed `_change_type`, …).
        || lower.starts_with('_')
}

/// Functions whose bare-identifier arguments are always column values.
///
/// Elsewhere a bare identifier argument can be a keyword the parser leaves as
/// an identifier (`DATEADD(day, 1, ts)`, `DATE_TRUNC(month, ts)` on
/// Snowflake), so it is not checked.
fn args_are_values(function: &str) -> bool {
    matches!(
        function,
        "count"
            | "sum"
            | "avg"
            | "min"
            | "max"
            | "any_value"
            | "first"
            | "last"
            | "median"
            | "stddev"
            | "stddev_pop"
            | "stddev_samp"
            | "variance"
            | "var_pop"
            | "var_samp"
            | "count_if"
            | "bool_and"
            | "bool_or"
            | "array_agg"
            | "list"
            | "string_agg"
            | "listagg"
            | "approx_count_distinct"
            | "coalesce"
            | "nullif"
            | "ifnull"
            | "nvl"
            | "greatest"
            | "least"
            | "abs"
            | "round"
            | "floor"
            | "ceil"
            | "ceiling"
            | "upper"
            | "lower"
            | "trim"
            | "ltrim"
            | "rtrim"
            | "length"
            | "concat"
            | "md5"
            | "sha256"
            | "hash"
    )
}

/// Whether `expr` contains a lambda. A lambda's parameters are not columns,
/// and `(v, i) -> v > i` parses as `((v, i) -> v) > i`, so an argument that
/// contains one anywhere is skipped whole.
fn contains_lambda(expr: &Expr) -> bool {
    use std::ops::ControlFlow;
    ast::visit_expressions(expr, |e| match e {
        Expr::Lambda(_)
        | Expr::BinaryOp {
            op: ast::BinaryOperator::Arrow,
            ..
        } => ControlFlow::Break(()),
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn is_value_operator(op: &ast::BinaryOperator) -> bool {
    use ast::BinaryOperator as Op;
    matches!(
        op,
        Op::Plus
            | Op::Minus
            | Op::Multiply
            | Op::Divide
            | Op::Modulo
            | Op::StringConcat
            | Op::Gt
            | Op::Lt
            | Op::GtEq
            | Op::LtEq
            | Op::Spaceship
            | Op::Eq
            | Op::NotEq
            | Op::And
            | Op::Or
            | Op::Xor
            | Op::BitwiseOr
            | Op::BitwiseAnd
            | Op::BitwiseXor
            | Op::DuckIntegerDivide
            | Op::MyIntegerDivide
    )
}

impl<'s, 'a> Binder<'s, 'a> {
    fn check_query(&mut self, query: &Query, chain: &[Scope<'s, 'a>], ctes: &HashSet<String>) {
        if !query.pipe_operators.is_empty() {
            return;
        }
        let mut visible_ctes = ctes.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                visible_ctes.insert(cte.alias.name.value.to_lowercase());
            }
            for cte in &with.cte_tables {
                self.check_query(&cte.query, chain, &visible_ctes);
            }
        }
        // `ORDER BY` / `LIMIT` resolve against output names (and, for a set
        // operation, only those), so they are not checked.
        self.check_set_expr(&query.body, chain, &visible_ctes);
    }

    fn check_set_expr(&mut self, body: &SetExpr, chain: &[Scope<'s, 'a>], ctes: &HashSet<String>) {
        match body {
            SetExpr::Select(select) => self.check_select(select, chain, ctes),
            SetExpr::Query(query) => self.check_query(query, chain, ctes),
            SetExpr::SetOperation { left, right, .. } => {
                self.check_set_expr(left, chain, ctes);
                self.check_set_expr(right, chain, ctes);
            }
            _ => {}
        }
    }

    fn check_select(&mut self, select: &Select, chain: &[Scope<'s, 'a>], ctes: &HashSet<String>) {
        if !select.lateral_views.is_empty()
            || !select.connect_by.is_empty()
            || select.value_table_mode.is_some()
            || select.into.is_some()
            || select.flavor != ast::SelectFlavor::Standard
        {
            return;
        }

        let mut scope = Scope::default();
        let mut join_conditions: Vec<&Expr> = Vec::new();
        for table in &select.from {
            self.add_table_with_joins(table, &mut scope, chain, ctes, &mut join_conditions);
        }
        for item in &select.projection {
            match item {
                SelectItem::ExprWithAlias { alias, .. } => {
                    scope.aliases.insert(alias.value.to_lowercase());
                }
                SelectItem::ExprWithAliases { aliases, .. } => {
                    scope
                        .aliases
                        .extend(aliases.iter().map(|alias| alias.value.to_lowercase()));
                }
                // DuckDB's prefix alias `total: amount` parses as JSON access
                // on `total`; treat `total` as the alias it is.
                SelectItem::UnnamedExpr(Expr::JsonAccess { value, .. }) => {
                    if let Expr::Identifier(alias) = value.as_ref() {
                        scope.aliases.insert(alias.value.to_lowercase());
                    }
                }
                _ => {}
            }
        }

        let mut full = chain.to_vec();
        full.push(scope);

        for condition in join_conditions {
            self.check_expr(condition, &full, ctes);
        }
        for item in &select.projection {
            match item {
                // `r'\d+'` / `E'\t'`: this dialect reads the string prefix as
                // an identifier and the literal as its alias.
                SelectItem::ExprWithAlias { alias, .. } if alias.quote_style == Some('\'') => {}
                SelectItem::UnnamedExpr(expr)
                | SelectItem::ExprWithAlias { expr, .. }
                | SelectItem::ExprWithAliases { expr, .. } => self.check_expr(expr, &full, ctes),
                _ => {}
            }
        }
        if let Some(selection) = &select.selection {
            self.check_expr(selection, &full, ctes);
        }
        if let GroupByExpr::Expressions(exprs, _) = &select.group_by {
            for expr in exprs {
                self.check_expr(expr, &full, ctes);
            }
        }
        if let Some(having) = &select.having {
            self.check_expr(having, &full, ctes);
        }
        if let Some(qualify) = &select.qualify {
            self.check_expr(qualify, &full, ctes);
        }
    }

    fn add_table_with_joins<'q>(
        &mut self,
        table: &'q TableWithJoins,
        scope: &mut Scope<'s, 'a>,
        chain: &[Scope<'s, 'a>],
        ctes: &HashSet<String>,
        join_conditions: &mut Vec<&'q Expr>,
    ) {
        self.add_factor(&table.relation, scope, chain, ctes, join_conditions);
        for join in &table.joins {
            self.add_factor(&join.relation, scope, chain, ctes, join_conditions);
            let constraint = match &join.join_operator {
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
                | JoinOperator::StraightJoin(c) => Some(c),
                _ => None,
            };
            if let Some(JoinConstraint::On(expr)) = constraint {
                join_conditions.push(expr);
            }
        }
    }

    fn add_factor<'q>(
        &mut self,
        factor: &'q TableFactor,
        scope: &mut Scope<'s, 'a>,
        chain: &[Scope<'s, 'a>],
        ctes: &HashSet<String>,
        join_conditions: &mut Vec<&'q Expr>,
    ) {
        match factor {
            TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
                version,
                with_ordinality,
                partitions,
                json_path,
                sample,
                index_hints,
            } => {
                let parts: Option<Vec<&str>> = name
                    .0
                    .iter()
                    .map(|part| part.as_ident().map(|ident| ident.value.as_str()))
                    .collect();
                let binding = match (alias, &parts) {
                    (Some(alias), _) => Some(alias.name.value.to_lowercase()),
                    (None, Some(parts)) => parts.last().map(|last| last.to_lowercase()),
                    (None, None) => None,
                };
                let plain = args.is_none()
                    && with_hints.is_empty()
                    && version.is_none()
                    && !*with_ordinality
                    && partitions.is_empty()
                    && json_path.is_none()
                    && sample.is_none()
                    && index_hints.is_empty()
                    && alias.as_ref().is_none_or(|alias| alias.columns.is_empty());
                let source = parts
                    .filter(|parts| plain && parts.len() >= 2)
                    .map(|parts| parts.join(".").to_lowercase())
                    // A CTE only shadows a single-part name today, but a
                    // quoted dotted CTE name must still win over a source.
                    .filter(|dotted| !ctes.contains(dotted))
                    .and_then(|dotted| self.sources.get(&dotted));
                scope.rels.push(Rel {
                    binding,
                    kind: source.map_or(RelKind::Opaque, RelKind::Source),
                });
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                // DuckDB plans a FROM-clause subquery as an implicit lateral
                // join, so it may read the relations listed before it.
                let mut inner_chain = chain.to_vec();
                inner_chain.push(Scope {
                    rels: scope.rels.clone(),
                    aliases: HashSet::new(),
                });
                self.check_query(subquery, &inner_chain, ctes);
                scope.rels.push(Rel {
                    binding: alias.as_ref().map(|a| a.name.value.to_lowercase()),
                    kind: RelKind::Opaque,
                });
            }
            TableFactor::NestedJoin {
                table_with_joins,
                alias: None,
            } => {
                self.add_table_with_joins(table_with_joins, scope, chain, ctes, join_conditions);
            }
            TableFactor::NestedJoin {
                alias: Some(alias), ..
            } => scope.rels.push(Rel {
                binding: Some(alias.name.value.to_lowercase()),
                kind: RelKind::Opaque,
            }),
            _ => scope.rels.push(Rel {
                binding: None,
                kind: RelKind::Opaque,
            }),
        }
    }

    fn check_expr(&mut self, expr: &Expr, chain: &[Scope<'s, 'a>], ctes: &HashSet<String>) {
        match expr {
            Expr::Identifier(ident) => self.check_unqualified(ident, chain),
            Expr::CompoundIdentifier(parts) => {
                if let [qualifier, column] = parts.as_slice() {
                    self.check_qualified(qualifier, column, chain);
                }
            }
            // Only ordinary operators: `x -> x + 1` (a lambda) and the JSON
            // arrows parse as `BinaryOp` too, and their left side is not a
            // column reference.
            Expr::BinaryOp { left, op, right } if is_value_operator(op) => {
                self.check_expr(left, chain, ctes);
                self.check_expr(right, chain, ctes);
            }
            Expr::IsDistinctFrom(left, right) | Expr::IsNotDistinctFrom(left, right) => {
                self.check_expr(left, chain, ctes);
                self.check_expr(right, chain, ctes);
            }
            Expr::UnaryOp { expr, .. }
            | Expr::Nested(expr)
            | Expr::IsNull(expr)
            | Expr::IsNotNull(expr)
            | Expr::IsTrue(expr)
            | Expr::IsNotTrue(expr)
            | Expr::IsFalse(expr)
            | Expr::IsNotFalse(expr)
            | Expr::Cast { expr, .. }
            | Expr::Extract { expr, .. } => self.check_expr(expr, chain, ctes),
            Expr::InList { expr, list, .. } => {
                self.check_expr(expr, chain, ctes);
                for item in list {
                    self.check_expr(item, chain, ctes);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.check_expr(expr, chain, ctes);
                self.check_expr(low, chain, ctes);
                self.check_expr(high, chain, ctes);
            }
            Expr::Like { expr, pattern, .. } | Expr::ILike { expr, pattern, .. } => {
                self.check_expr(expr, chain, ctes);
                self.check_expr(pattern, chain, ctes);
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    self.check_expr(operand, chain, ctes);
                }
                for when in conditions {
                    self.check_expr(&when.condition, chain, ctes);
                    self.check_expr(&when.result, chain, ctes);
                }
                if let Some(else_result) = else_result {
                    self.check_expr(else_result, chain, ctes);
                }
            }
            Expr::Tuple(items) => {
                for item in items {
                    self.check_expr(item, chain, ctes);
                }
            }
            Expr::Function(function) => self.check_function(function, chain, ctes),
            Expr::Subquery(query)
            | Expr::Exists {
                subquery: query, ..
            } => {
                self.check_query(query, chain, ctes);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.check_expr(expr, chain, ctes);
                self.check_query(subquery, chain, ctes);
            }
            // Lambdas bind their own parameters; struct, map, JSON and
            // subscript access, window specs and the rest are not modelled.
            _ => {}
        }
    }

    fn check_function(
        &mut self,
        function: &ast::Function,
        chain: &[Scope<'s, 'a>],
        ctes: &HashSet<String>,
    ) {
        let name = function
            .name
            .0
            .last()
            .and_then(|part| part.as_ident())
            .map(|ident| ident.value.to_lowercase())
            .unwrap_or_default();
        let bare_args_are_values = function.name.0.len() == 1 && args_are_values(&name);
        match &function.args {
            FunctionArguments::List(list) => {
                for arg in &list.args {
                    let arg_expr = match arg {
                        FunctionArg::Unnamed(arg)
                        | FunctionArg::Named { arg, .. }
                        | FunctionArg::ExprNamed { arg, .. } => arg,
                    };
                    let FunctionArgExpr::Expr(expr) = arg_expr else {
                        continue;
                    };
                    if (matches!(expr, Expr::Identifier(_)) && !bare_args_are_values)
                        || contains_lambda(expr)
                    {
                        continue;
                    }
                    self.check_expr(expr, chain, ctes);
                }
            }
            FunctionArguments::Subquery(query) => self.check_query(query, chain, ctes),
            FunctionArguments::None => {}
        }
        if let Some(filter) = &function.filter {
            self.check_expr(filter, chain, ctes);
        }
    }

    fn check_unqualified(&mut self, ident: &Ident, chain: &[Scope<'s, 'a>]) {
        // A quoted name is not provably a column: BigQuery, and Databricks by
        // default, read `"shipped"` as a string literal, while this dialect
        // parses it as an identifier.
        if ident.quote_style.is_some() {
            return;
        }
        if let Resolution::Missing(sources) = resolve_unqualified(chain, &ident.value) {
            self.record(ident, &ident.value, sources);
        }
    }

    fn check_qualified(&mut self, qualifier: &Ident, column: &Ident, chain: &[Scope<'s, 'a>]) {
        if qualifier.quote_style.is_some() || column.quote_style.is_some() {
            return;
        }
        if let Resolution::Missing(sources) =
            resolve_qualified(chain, &qualifier.value, &column.value)
        {
            self.record(column, &column.value, sources);
        }
    }

    fn record(&mut self, at: &Ident, column: &str, sources: Vec<&'s KnownSource<'a>>) {
        self.findings.push(Finding {
            column: column.to_string(),
            sources,
            line: at.span.start.line,
            col: at.span.start.column,
        });
    }
}

fn resolve_unqualified<'s, 'a>(chain: &[Scope<'s, 'a>], name: &str) -> Resolution<'s, 'a> {
    let lower = name.to_lowercase();
    if is_relation_free_name(&lower) {
        return Resolution::Resolved;
    }
    let mut searched = Vec::new();
    for scope in chain.iter().rev() {
        // A lateral alias, or a whole-row reference to a relation binding.
        if scope.aliases.contains(&lower)
            || scope
                .rels
                .iter()
                .any(|rel| rel.binding.as_deref() == Some(lower.as_str()))
        {
            return Resolution::Resolved;
        }
        for rel in &scope.rels {
            match rel.kind {
                RelKind::Opaque => return Resolution::Unprovable,
                RelKind::Source(source) if source.has(&lower) => return Resolution::Resolved,
                RelKind::Source(source) => searched.push(source),
            }
        }
    }
    if searched.is_empty() {
        Resolution::Unprovable
    } else {
        Resolution::Missing(searched)
    }
}

fn resolve_qualified<'s, 'a>(
    chain: &[Scope<'s, 'a>],
    qualifier: &str,
    column: &str,
) -> Resolution<'s, 'a> {
    let qualifier = qualifier.to_lowercase();
    let column = column.to_lowercase();
    if is_relation_free_name(&qualifier) || is_relation_free_name(&column) {
        return Resolution::Resolved;
    }
    // `a.b` may be a field access on a column or alias named `a`. If any
    // relation in reach could carry such a column, the reading is ambiguous.
    for scope in chain {
        if scope.aliases.contains(&qualifier) {
            return Resolution::Unprovable;
        }
        for rel in &scope.rels {
            match rel.kind {
                RelKind::Opaque => return Resolution::Unprovable,
                RelKind::Source(source) if source.has(&qualifier) => {
                    return Resolution::Unprovable;
                }
                RelKind::Source(_) => {}
            }
        }
    }
    for scope in chain.iter().rev() {
        let mut bound = scope
            .rels
            .iter()
            .filter(|rel| rel.binding.as_deref() == Some(qualifier.as_str()));
        let Some(rel) = bound.next() else {
            continue;
        };
        if bound.next().is_some() {
            return Resolution::Unprovable;
        }
        return match rel.kind {
            RelKind::Opaque => Resolution::Unprovable,
            RelKind::Source(source) if source.has(&column) => Resolution::Resolved,
            RelKind::Source(source) => Resolution::Missing(vec![source]),
        };
    }
    Resolution::Unprovable
}

// ---------------------------------------------------------------------------
// Missing source tables (E045 / W045)
// ---------------------------------------------------------------------------

/// The tables Rocky knows in one external schema, from source schemas with a
/// recorded origin.
struct KnownSchema<'a> {
    /// Lower-cased table names.
    tables: HashSet<String>,
    /// Table names as the source schemas spell them, sorted.
    display_tables: Vec<&'a str>,
    /// Every origin the schema's tables came from.
    origins: Vec<&'a SourceSchemaOrigin>,
}

impl KnownSchema<'_> {
    /// Whether the table list is the schema's whole content: every table was
    /// introspected from the warehouse in this invocation. A seed file or the
    /// schema cache lists the tables it was given or still holds, which may be
    /// fewer than the warehouse has.
    fn is_complete(&self) -> bool {
        self.origins
            .iter()
            .all(|origin| matches!(origin, SourceSchemaOrigin::Live))
    }
}

/// Check every model for two-part reads (`schema.table`) of a table that is
/// absent from a schema Rocky knows.
///
/// A schema is known when at least one source schema with a recorded origin
/// lives in it. The check never fires for a schema Rocky knows nothing about,
/// for a schema that a model of this project writes to (the project adds
/// tables Rocky has no source schema for), for a table a model writes, or for
/// a one-part or three-part name. A `WITH`-bound name is never a read.
///
/// Severity follows completeness: `E045` when every table of the schema was
/// introspected live, `W045` when the list came from a seed file or the schema
/// cache, which can miss tables the warehouse has. Strict sources
/// ([`SourceProvenance::strict`]) escalate `W045` to `E045`.
pub fn check_source_table_refs(
    models: &[rocky_core::models::Model],
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    provenance: &SourceProvenance,
) -> Vec<Diagnostic> {
    if provenance.origins.is_empty() {
        return Vec::new();
    }
    let mut schemas: HashMap<String, KnownSchema<'_>> = HashMap::new();
    for key in source_schemas.keys() {
        let Some(origin) = provenance.origins.get(key) else {
            continue;
        };
        let Some((schema, table)) = key.split_once('.') else {
            continue;
        };
        if schema.is_empty() || table.is_empty() || table.contains('.') {
            continue;
        }
        let entry = schemas
            .entry(schema.to_lowercase())
            .or_insert_with(|| KnownSchema {
                tables: HashSet::new(),
                display_tables: Vec::new(),
                origins: Vec::new(),
            });
        entry.tables.insert(table.to_lowercase());
        entry.display_tables.push(table);
        entry.origins.push(origin);
    }
    if schemas.is_empty() {
        return Vec::new();
    }
    for schema in schemas.values_mut() {
        schema.display_tables.sort_unstable();
    }

    let written_schemas: HashSet<String> = models
        .iter()
        .map(|m| m.config.target.schema.to_lowercase())
        .collect();
    let written_tables: HashSet<String> = models
        .iter()
        .map(|m| {
            format!(
                "{}.{}",
                m.config.target.schema.to_lowercase(),
                m.config.target.table.to_lowercase()
            )
        })
        .collect();

    let mut diagnostics = Vec::new();
    for model in models {
        let Ok(reads) = rocky_sql::lineage::referenced_tables(&model.sql) else {
            continue;
        };
        for read in reads {
            let parts: Vec<&str> = read.split('.').collect();
            let [schema_name, table_name] = parts.as_slice() else {
                continue;
            };
            if written_schemas.contains(*schema_name) || written_tables.contains(&read) {
                continue;
            }
            let Some(schema) = schemas.get(*schema_name) else {
                continue;
            };
            if schema.tables.contains(*table_name) {
                continue;
            }
            let mut diagnostic =
                missing_table_diagnostic(&model.config.name, &read, schema, provenance.strict);
            if let Some(span) = read_span(model, &read) {
                diagnostic = diagnostic.with_span(span);
            }
            diagnostics.push(diagnostic);
        }
    }
    diagnostics
}

fn missing_table_diagnostic(
    model_name: &str,
    read: &str,
    schema: &KnownSchema<'_>,
    strict: bool,
) -> Diagnostic {
    let (schema_name, table_name) = read.split_once('.').unwrap_or((read, read));
    let threshold = (table_name.chars().count() / 3).max(1);
    let mut close: Vec<(usize, &str)> = schema
        .display_tables
        .iter()
        .map(|t| (strsim::levenshtein(table_name, &t.to_lowercase()), *t))
        .filter(|(distance, _)| *distance <= threshold)
        .collect();
    close.sort_unstable();
    let mut suggestion = if let Some((_, best)) = close.first() {
        format!("did you mean '{schema_name}.{best}'?")
    } else {
        const MAX_LISTED: usize = 12;
        let mut listed: Vec<&str> = schema.display_tables.clone();
        let total = listed.len();
        listed.truncate(MAX_LISTED);
        let mut text = format!("known tables in '{schema_name}': {}", listed.join(", "));
        if total > MAX_LISTED {
            text.push_str(&format!(", … ({} more)", total - MAX_LISTED));
        }
        text
    };
    if schema.is_complete() {
        return Diagnostic::error(
            E045,
            model_name,
            format!(
                "table '{read}' does not exist: the warehouse schema '{schema_name}' has no \
                 table '{table_name}'"
            ),
        )
        .with_suggestion(suggestion);
    }
    suggestion.push_str(
        ". If the warehouse has this table, refresh the schema: add it to the seed file, or \
         re-warm the cache with `rocky discover --with-schemas`",
    );
    if strict {
        Diagnostic::error(
            E045,
            model_name,
            format!(
                "table '{read}' is not among the known tables of schema '{schema_name}'; strict \
                 sources are on, so a possibly incomplete table list is treated as authoritative"
            ),
        )
        .with_suggestion(suggestion)
    } else {
        suggestion.push_str(
            ". To make this an error, pass `--strict-sources` or set \
             `[cache.schemas] strict_sources = true`",
        );
        Diagnostic::warning(
            W045,
            model_name,
            format!(
                "table '{read}' was not found among the known tables of schema '{schema_name}' \
                 (from the seed file or the schema cache); the table list may be incomplete"
            ),
        )
        .with_suggestion(suggestion)
    }
}

/// Where `read` (lower-cased `schema.table`) first appears in the model's
/// `.sql` file, when the parsed SQL is a verbatim slice of it.
fn read_span(model: &rocky_core::models::Model, read: &str) -> Option<SourceSpan> {
    let (line_offset, col_offset) = sql_position_in_file(model)?;
    let at = model.sql.to_lowercase().find(read)?;
    let before = model.sql.get(..at)?;
    let line = before.matches('\n').count() + 1;
    let line_start = before.rfind('\n').map_or(0, |i| i + 1);
    let col = before[line_start..].chars().count() + 1;
    let (line, col) = if line == 1 {
        (line + line_offset, col + col_offset)
    } else {
        (line + line_offset, col)
    };
    Some(SourceSpan {
        file: model.file_path.display().to_string(),
        line,
        col,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::Severity;
    use crate::types::RockyType;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};

    fn col(name: &str) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: RockyType::Unknown,
            nullable: true,
        }
    }

    fn schemas() -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([
            (
                "raw.orders".to_string(),
                ["order_id", "customer_id", "amount", "status", "order_date"]
                    .into_iter()
                    .map(col)
                    .collect(),
            ),
            (
                "raw.customers".to_string(),
                ["customer_id", "customer_name", "email", "payload"]
                    .into_iter()
                    .map(col)
                    .collect(),
            ),
        ])
    }

    fn model(name: &str, sql: &str) -> Model {
        Model {
            drop_existing_kind: None,
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "c".to_string(),
                    schema: "s".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql").into(),
            contract_path: None,
        }
    }

    fn check_with(origin: SourceSchemaOrigin, strict: bool, sql: &str) -> Vec<Diagnostic> {
        let schemas = schemas();
        let provenance = SourceProvenance::uniform(schemas.keys(), &origin).with_strict(strict);
        check_source_column_refs(&[model("m", sql)], &schemas, &provenance)
    }

    fn check(sql: &str) -> Vec<Diagnostic> {
        check_with(SourceSchemaOrigin::Live, false, sql)
    }

    fn codes(diagnostics: &[Diagnostic]) -> Vec<&str> {
        diagnostics.iter().map(|d| d.code.as_ref()).collect()
    }

    const D1: &str = "SELECT order_id, customer_id, order_total FROM raw.orders";

    #[test]
    fn d1_live_schema_is_e041_naming_column_and_source() {
        let diagnostics = check(D1);
        assert_eq!(codes(&diagnostics), ["E041"], "{diagnostics:?}");
        let d = &diagnostics[0];
        assert_eq!(d.severity, Severity::Error);
        assert!(d.message.contains("'order_total'"), "{}", d.message);
        assert!(d.message.contains("'raw.orders'"), "{}", d.message);
        // No file on disk: the span falls back to the file's first line.
        let span = d.span.as_ref().unwrap();
        assert_eq!((span.line, span.col), (1, 1));
    }

    #[test]
    fn span_points_at_the_reference_in_the_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("m.sql");
        let sql = "SELECT order_id,\n  o.order_total\nFROM raw.orders o";
        std::fs::write(&path, format!("---toml\nname = \"m\"\n---\n{sql}\n")).unwrap();
        let mut m = model("m", sql);
        m.file_path = path;
        let schemas = schemas();
        let provenance = SourceProvenance::uniform(schemas.keys(), &SourceSchemaOrigin::Live);
        let diagnostics = check_source_column_refs(&[m], &schemas, &provenance);
        assert_eq!(codes(&diagnostics), ["E041"]);
        let span = diagnostics[0].span.as_ref().unwrap();
        // Three frontmatter lines, then the column part of `o.order_total`.
        assert_eq!((span.line, span.col), (5, 5));
    }

    #[test]
    fn d1_seed_schema_is_w041_with_refresh_guidance() {
        let diagnostics = check_with(SourceSchemaOrigin::Seed, false, D1);
        assert_eq!(codes(&diagnostics), ["W041"], "{diagnostics:?}");
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        let suggestion = diagnostics[0].suggestion.as_deref().unwrap();
        assert!(suggestion.contains("refresh the schema"), "{suggestion}");
        assert!(suggestion.contains("--strict-sources"), "{suggestion}");
    }

    #[test]
    fn d1_seed_schema_under_strict_is_e041() {
        let diagnostics = check_with(SourceSchemaOrigin::Seed, true, D1);
        assert_eq!(codes(&diagnostics), ["E041"], "{diagnostics:?}");
        assert!(diagnostics[0].message.contains("strict sources"));
    }

    #[test]
    fn cache_origin_severity_follows_trust() {
        let cached_at = Utc::now();
        let trusted = SourceSchemaOrigin::Cache {
            cached_at,
            trusted: true,
        };
        let aged = SourceSchemaOrigin::Cache {
            cached_at,
            trusted: false,
        };
        assert_eq!(codes(&check_with(trusted, false, D1)), ["E041"]);
        assert_eq!(codes(&check_with(aged, false, D1)), ["W041"]);
        assert_eq!(codes(&check_with(aged, true, D1)), ["E041"]);
    }

    #[test]
    fn unknown_provenance_never_fires() {
        let schemas = schemas();
        let diagnostics = check_source_column_refs(
            &[model("m", D1)],
            &schemas,
            &SourceProvenance::default().with_strict(true),
        );
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }

    #[test]
    fn close_names_are_suggested() {
        let diagnostics = check("SELECT ammount FROM raw.orders");
        assert_eq!(codes(&diagnostics), ["E041"]);
        let suggestion = diagnostics[0].suggestion.as_deref().unwrap();
        assert!(suggestion.contains("did you mean 'amount'"), "{suggestion}");
    }

    #[test]
    fn missing_column_with_no_close_name_lists_available_columns() {
        let diagnostics = check(D1);
        let suggestion = diagnostics[0].suggestion.as_deref().unwrap();
        assert!(
            suggestion.contains("available columns: order_id"),
            "{suggestion}"
        );
    }

    #[test]
    fn g1_s4_stale_seed_lacking_a_column_warns_only() {
        let mut schemas = schemas();
        schemas
            .get_mut("raw.orders")
            .unwrap()
            .retain(|c| c.name != "amount");
        let provenance = SourceProvenance::uniform(schemas.keys(), &SourceSchemaOrigin::Seed);
        let sql = "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders";
        let diagnostics = check_source_column_refs(&[model("m", sql)], &schemas, &provenance);
        assert_eq!(codes(&diagnostics), ["W041"], "{diagnostics:?}");
    }

    #[test]
    fn valid_controls_stay_clean() {
        for sql in [
            // C2-shaped, against sources only.
            "SELECT c.customer_name, SUM(o.amount) AS total FROM raw.orders o \
             JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
            // V1: lateral column alias.
            "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
            // V2: no relation at all.
            "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value",
            // V3: derived table.
            "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
            // V4: function outside inference.
            "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
            // G1-S2: CTE shadowing a model name.
            "WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) \
             SELECT stg_orders.amount FROM stg_orders",
            // G1-S3 consumer: an in-project model is opaque here.
            "SELECT s.amount FROM stg_orders AS s",
            "SELECT * FROM raw.orders",
            "SELECT o.* FROM raw.orders o",
            // Whole-row reference to a relation binding.
            "SELECT o FROM raw.orders o",
            "SELECT orders.amount FROM raw.orders",
            "SELECT customer_id FROM raw.orders JOIN raw.customers USING (customer_id)",
            // Lateral alias in WHERE / GROUP BY.
            "SELECT amount * 2 AS dbl FROM raw.orders WHERE dbl > 5 GROUP BY dbl",
            // Correlated subquery reading an outer column.
            "SELECT order_id FROM raw.orders o WHERE EXISTS \
             (SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id AND amount > 0)",
            "SELECT (SELECT MAX(amount) FROM raw.customers c WHERE c.customer_id = o.customer_id) \
             AS m FROM raw.orders o",
            // Implicit lateral derived table.
            "SELECT d.x FROM raw.orders o, (SELECT amount * 2 AS x) d",
            // Struct field access on a source column.
            "SELECT payload.field FROM raw.customers",
            "SELECT c.payload.field FROM raw.customers c",
            // An opaque relation makes unqualified names unprovable.
            "SELECT order_total FROM raw.orders JOIN stg_x USING (order_id)",
            "SELECT order_total FROM raw.orders, read_parquet('x.parquet')",
            // Keyword-like arguments are not column references.
            "SELECT DATEADD(day, 1, order_date) AS d FROM raw.orders",
            "SELECT DATE_TRUNC(month, order_date) AS d FROM raw.orders",
            "SELECT current_date AS d, rowid FROM raw.orders",
            // Lambda parameters, and JSON arrows.
            "SELECT transform(array(amount), x -> x + 1) AS a FROM raw.orders",
            "SELECT list_transform([amount], x -> x * 2) AS a FROM raw.orders",
            "SELECT filter(array(amount), (v, i) -> v > i) AS a FROM raw.orders",
            "SELECT payload -> 'k' AS a FROM raw.customers",
            // Window spec and set-operation ORDER BY are not checked.
            "SELECT order_id FROM raw.orders UNION ALL SELECT customer_id FROM raw.customers \
             ORDER BY order_id",
            // Unknown 3-part relation: no provenance, stays Unknown.
            "SELECT nope FROM cat.raw.orders",
            "SELECT nope FROM other.table_x",
            // Ambiguous self-join binding.
            "SELECT orders.nope FROM raw.orders JOIN raw.orders ON true",
            // Correlated reads resolve outward, by binding or bare name.
            "SELECT o.order_id FROM raw.orders o WHERE o.status IN \
             (SELECT status FROM raw.customers)",
            "SELECT * FROM raw.orders WHERE EXISTS \
             (SELECT 1 FROM raw.orders o2 WHERE orders.status = o2.status)",
            // An opaque relation in a subquery makes its bare names unprovable.
            "SELECT order_id FROM raw.orders WHERE EXISTS \
             (SELECT 1 FROM stg_x WHERE stg_x.id = order_total)",
            // Renamed columns, table functions, VALUES, time travel.
            "SELECT a FROM raw.orders AS o(a, b, c, d, e)",
            "SELECT a FROM raw.orders, UNNEST([1, 2]) AS t(a)",
            "SELECT v.a FROM (VALUES (1)) v(a)",
            // Pseudo-columns and metadata structs.
            "SELECT _metadata.file_path FROM raw.orders",
            // Duplicate binding: ambiguous.
            "SELECT x.order_id FROM raw.orders x JOIN raw.customers x ON true",
            // A struct column named like the binding.
            "WITH orders AS (SELECT 1 AS z) SELECT orders.z FROM raw.orders, orders",
            // Window, ordinal and grouping forms.
            "SELECT order_id FROM raw.orders QUALIFY ROW_NUMBER() OVER \
             (PARTITION BY customer_id ORDER BY order_date) = 1",
            "SELECT customer_id, SUM(amount) FROM raw.orders GROUP BY 1",
            "SELECT EXTRACT(year FROM order_date) AS y FROM raw.orders",
            "SELECT LAG(amount, 1) OVER (ORDER BY order_date) AS prev FROM raw.orders",
            "SELECT DATEDIFF(day, order_date, order_date) > 1 AS late FROM raw.orders",
            "SELECT ORDER_ID, Amount FROM RAW.ORDERS",
            // Double-quoted strings (BigQuery, Databricks default).
            "SELECT order_id, COALESCE(status, \"unknown\") AS s FROM raw.orders \
             WHERE status = \"shipped\"",
            "SELECT CONCAT(customer_name, \" <\", email, \">\") AS who FROM raw.customers",
            "SELECT o.\"order_total\" FROM raw.orders o",
            // Parenthesis-free niladic functions and metadata columns.
            "SELECT order_id, CURRENT_DATETIME AS loaded_at FROM raw.orders",
            "SELECT order_id, _FILE_NAME AS src_file, o._TABLE_SUFFIX FROM raw.orders o",
            // DuckDB prefix aliases and prefixed string literals.
            "SELECT total: amount, total + 1 AS t2 FROM raw.orders",
            "SELECT total: amount FROM raw.orders WHERE total > 10",
            "SELECT order_id, r'\\d+' FROM raw.orders",
            "SELECT order_id, E'\\t' FROM raw.orders",
            // Unparseable SQL.
            "SELECT FROM WHERE",
        ] {
            let diagnostics = check(sql);
            assert!(
                diagnostics.is_empty(),
                "false positive for {sql}: {diagnostics:?}"
            );
        }
    }

    #[test]
    fn scoped_defects_are_found() {
        for (sql, column) in [
            ("SELECT o.order_total FROM raw.orders o", "order_total"),
            ("SELECT orders.order_total FROM raw.orders", "order_total"),
            (
                "SELECT order_id FROM raw.orders WHERE order_total > 0",
                "order_total",
            ),
            (
                "SELECT o.order_id FROM raw.orders o JOIN raw.customers c \
                 ON o.customer_id = c.customer_key",
                "customer_key",
            ),
            (
                "WITH x AS (SELECT order_total FROM raw.orders) SELECT * FROM x",
                "order_total",
            ),
            (
                "SELECT scoped.order_id FROM (SELECT order_total FROM raw.orders) AS scoped",
                "order_total",
            ),
            (
                "SELECT SUM(order_total) AS t FROM raw.orders",
                "order_total",
            ),
            (
                "SELECT order_id FROM raw.orders UNION ALL SELECT bogus FROM raw.customers",
                "bogus",
            ),
        ] {
            let diagnostics = check(sql);
            assert_eq!(codes(&diagnostics), ["E041"], "{sql}: {diagnostics:?}");
            assert!(
                diagnostics[0].message.contains(&format!("'{column}'")),
                "{sql}: {}",
                diagnostics[0].message
            );
        }
    }

    #[test]
    fn unqualified_miss_across_two_sources_names_both() {
        let diagnostics = check(
            "SELECT nope FROM raw.orders o JOIN raw.customers c ON o.customer_id = c.customer_id",
        );
        assert_eq!(codes(&diagnostics), ["E041"]);
        assert!(diagnostics[0].message.contains("'raw.customers'"));
        assert!(diagnostics[0].message.contains("'raw.orders'"));
    }

    #[test]
    fn repeated_reference_reports_once() {
        let diagnostics =
            check("SELECT order_total, order_total + 1 AS x FROM raw.orders WHERE order_total > 0");
        assert_eq!(codes(&diagnostics), ["E041"]);
    }

    #[test]
    fn mixed_origins_warn_unless_every_source_is_current() {
        let schemas = schemas();
        let mut provenance = SourceProvenance::default();
        provenance
            .origins
            .insert("raw.orders".into(), SourceSchemaOrigin::Live);
        provenance
            .origins
            .insert("raw.customers".into(), SourceSchemaOrigin::Seed);
        let sql = "SELECT nope FROM raw.orders o JOIN raw.customers c ON true";
        let diagnostics = check_source_column_refs(&[model("m", sql)], &schemas, &provenance);
        assert_eq!(codes(&diagnostics), ["W041"]);
    }

    fn check_tables(origin: SourceSchemaOrigin, strict: bool, models: &[Model]) -> Vec<Diagnostic> {
        let schemas = schemas();
        let provenance = SourceProvenance::uniform(schemas.keys(), &origin).with_strict(strict);
        check_source_table_refs(models, &schemas, &provenance)
    }

    #[test]
    fn a_missing_table_in_a_seeded_schema_warns_and_strict_refuses() {
        let m = [model("stg", "SELECT order_id FROM raw.orderz")];
        let diagnostics = check_tables(SourceSchemaOrigin::Seed, false, &m);
        assert_eq!(codes(&diagnostics), ["W045"], "{diagnostics:?}");
        assert_eq!(diagnostics[0].severity, Severity::Warning);
        assert!(diagnostics[0].message.contains("raw.orderz"));
        let suggestion = diagnostics[0].suggestion.as_deref().unwrap();
        assert!(
            suggestion.contains("did you mean 'raw.orders'?"),
            "{suggestion}"
        );
        assert!(suggestion.contains("--strict-sources"), "{suggestion}");

        let strict = check_tables(SourceSchemaOrigin::Seed, true, &m);
        assert_eq!(codes(&strict), ["E045"], "{strict:?}");
        assert_eq!(strict[0].severity, Severity::Error);
    }

    #[test]
    fn a_missing_table_in_a_live_schema_is_an_error() {
        let m = [model("stg", "SELECT 1 AS x FROM raw.payments")];
        let diagnostics = check_tables(SourceSchemaOrigin::Live, false, &m);
        assert_eq!(codes(&diagnostics), ["E045"], "{diagnostics:?}");
        let suggestion = diagnostics[0].suggestion.as_deref().unwrap();
        assert!(suggestion.contains("customers, orders"), "{suggestion}");
    }

    #[test]
    fn a_missing_table_in_a_cached_schema_warns_even_when_trusted() {
        let m = [model("stg", "SELECT 1 AS x FROM raw.orderz")];
        let trusted = SourceSchemaOrigin::Cache {
            cached_at: Utc::now(),
            trusted: true,
        };
        assert_eq!(codes(&check_tables(trusted, false, &m)), ["W045"]);
        assert_eq!(codes(&check_tables(trusted, true, &m)), ["E045"]);
    }

    #[test]
    fn a_missing_table_is_found_in_joins_ctes_and_subqueries() {
        for sql in [
            "SELECT o.order_id FROM raw.orders o JOIN raw.refunds r ON r.order_id = o.order_id",
            "WITH x AS (SELECT * FROM raw.refunds) SELECT * FROM x",
            "SELECT order_id FROM raw.orders WHERE order_id IN (SELECT order_id FROM raw.refunds)",
            "SELECT * FROM (SELECT order_id FROM raw.refunds) AS s",
        ] {
            let diagnostics = check_tables(SourceSchemaOrigin::Live, false, &[model("m", sql)]);
            assert_eq!(codes(&diagnostics), ["E045"], "{sql}: {diagnostics:?}");
            assert!(diagnostics[0].message.contains("raw.refunds"), "{sql}");
        }
    }

    #[test]
    fn table_reads_rocky_cannot_judge_stay_silent() {
        let mut writer = model("writer", "SELECT 1 AS x");
        writer.config.target.schema = "mart".to_string();
        let mut raw_writer = model("raw_writer", "SELECT 1 AS x");
        raw_writer.config.target.schema = "raw".to_string();
        raw_writer.config.target.table = "staged".to_string();
        let cases: Vec<Vec<Model>> = vec![
            // A known table, in any case.
            vec![model("m", "SELECT order_id FROM RAW.Orders")],
            // A schema Rocky knows nothing about.
            vec![model("m", "SELECT * FROM other.anything")],
            // A one-part read: a model, a CTE or a search-path table.
            vec![model("m", "SELECT * FROM orderz")],
            // A three-part read: its catalog may hold another `raw`.
            vec![model("m", "SELECT * FROM cat.raw.orderz")],
            // A schema a project model writes to: the project adds tables.
            vec![writer.clone(), model("m", "SELECT * FROM mart.new_table")],
            vec![raw_writer, model("m", "SELECT * FROM raw.whatever")],
            // SQL that does not parse is another check's to report.
            vec![model("m", "SELEC broken FROM raw.orderz")],
        ];
        for models in &cases {
            for origin in [SourceSchemaOrigin::Live, SourceSchemaOrigin::Seed] {
                let diagnostics = check_tables(origin, true, models);
                assert!(
                    diagnostics.is_empty(),
                    "{:?}: {diagnostics:?}",
                    models.last().map(|m| &m.sql)
                );
            }
        }
    }

    #[test]
    fn unknown_provenance_never_reports_a_missing_table() {
        let schemas = schemas();
        let m = [model("m", "SELECT * FROM raw.orderz")];
        let diagnostics = check_source_table_refs(&m, &schemas, &SourceProvenance::default());
        assert!(diagnostics.is_empty(), "{diagnostics:?}");
    }
}
