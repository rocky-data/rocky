//! Calls to functions the target warehouse does not have (E057 / W057).
//!
//! `SUMM(amount)` parses and type-checks as an unknown function returning
//! `Unknown`, so it used to compile clean and fail at run time. This pass
//! reports a call whose name is neither a function of the target dialect nor
//! a project function (`functions/`).
//!
//! # Tiers
//!
//! A false refusal of a real warehouse function is worse than silence, so
//! each dialect's list carries a [`Tier`]:
//!
//! | Dialect | List | Verified | Code |
//! |---|---|---|---|
//! | DuckDB | `data/duckdb_functions.txt` | against a live DuckDB (`duckdb_functions()`) | `E057` |
//! | PostgreSQL | PostgreSQL 17 `pg_proc` | against a live PostgreSQL 17.11 (`pg_proc`); extensions add functions | `W057` |
//! | Snowflake | vendor reference | built from docs, not run | `W057` |
//! | Databricks | vendor reference + Spark | built from docs, not run (a superset of the live Spark list) | `W057` |
//! | Spark | Spark 4.0.1 `SHOW FUNCTIONS` | against a live Spark 4.0.1 with Delta; UDFs and session extensions add functions | `W057` |
//! | BigQuery | vendor reference | built from docs, not run | `W057` |
//! | Trino | Trino 483 `SHOW FUNCTIONS` | against a live Trino 483; connectors add functions | `W057` |
//! | Redshift | vendor reference + PostgreSQL | built from docs, not run | `W057` |
//! | SQL Server, ClickHouse | none | | silent |
//!
//! Only DuckDB reports `E057`: its list was checked against a running engine
//! and includes the functions its extensions load on first use, and a macro
//! created outside Rocky is called schema-qualified. A list built from
//! documentation can lag a warehouse release. A live-checked list is still
//! not closed where the warehouse lets a user add plain-named functions:
//! PostgreSQL extensions (PostGIS, pgcrypto), Trino connectors, and Spark
//! UDFs, `CREATE FUNCTION` and session extensions (Sedona). A list is also
//! pinned to one engine version. Those dialects warn (`W057`); `rocky compile --deny-warnings
//! W057` turns the warning into an error. SQL Server and ClickHouse have no
//! list yet and stay silent. The files under `data/functions/` name their
//! source and build date in a header comment. Moving a dialect to `E057` is
//! one line in [`FunctionDialect::tier`], once its list is verified live.
//!
//! # Never reported
//!
//! - A schema-qualified call (`main.my_macro(x)`, `NET.HOST(x)`): it names
//!   its own catalog entry, which may be a macro, a namespaced built-in or a
//!   function created outside Rocky. This is also the escape hatch.
//! - A quoted name (`"my fn"(x)`).
//! - A name the project declares in `functions/`, valid or not (E051 covers
//!   an invalid one).
//! - SQL forms the parser or the warehouse rewrites before catalog lookup
//!   (`COALESCE`, `IF`, `IFNULL`, `GROUPING`, `COLUMNS`, `TRY`, and DuckDB's
//!   `DATE(x)`, a cast, ...).
//! - A model whose SQL does not parse.
//! - A dialect the model's SQL was not written for, when
//!   `[portability] target_dialect` names another warehouse (P001 covers
//!   portability).

use std::collections::HashSet;
use std::sync::LazyLock;

use sqlparser::ast::{self, Expr, Spanned, Statement, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E057, E068, SourceSpan, W057, W068};
use crate::operand_check::{OperandDialect, OperandTarget};
use crate::udf::FunctionRegistry;

/// How sure Rocky is that a dialect's function list is complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tier {
    /// Checked against a live engine, with the functions its extensions load on
    /// first use; a macro made outside Rocky is called schema-qualified. An
    /// unknown call is an error (`E057`). Only DuckDB.
    Verified,
    /// Built from the vendor's reference, or open to extensions and connectors
    /// that add functions: an unknown call is a warning (`W057`).
    Documented,
}

/// A warehouse that has a function list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FunctionDialect {
    DuckDb,
    Postgres,
    Snowflake,
    Databricks,
    Spark,
    BigQuery,
    Trino,
    Redshift,
}

fn load(text: &'static str) -> HashSet<&'static str> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
}

macro_rules! function_list {
    ($name:ident, $file:literal) => {
        static $name: LazyLock<HashSet<&'static str>> = LazyLock::new(|| load(include_str!($file)));
    };
}

function_list!(DUCKDB_FUNCTIONS, "data/duckdb_functions.txt");
function_list!(POSTGRES_FUNCTIONS, "data/functions/postgres_functions.txt");
function_list!(
    SNOWFLAKE_FUNCTIONS,
    "data/functions/snowflake_functions.txt"
);
function_list!(
    DATABRICKS_FUNCTIONS,
    "data/functions/databricks_functions.txt"
);
function_list!(SPARK_FUNCTIONS, "data/functions/spark_functions.txt");
function_list!(BIGQUERY_FUNCTIONS, "data/functions/bigquery_functions.txt");
function_list!(TRINO_FUNCTIONS, "data/functions/trino_functions.txt");
function_list!(REDSHIFT_FUNCTIONS, "data/functions/redshift_functions.txt");

impl FunctionDialect {
    /// The dialect for an operand-check dialect, when Rocky holds a list.
    fn from_operand(dialect: OperandDialect) -> Option<Self> {
        match dialect {
            OperandDialect::DuckDb => Some(Self::DuckDb),
            OperandDialect::Postgres => Some(Self::Postgres),
            OperandDialect::Snowflake => Some(Self::Snowflake),
            OperandDialect::Databricks => Some(Self::Databricks),
            OperandDialect::BigQuery => Some(Self::BigQuery),
            OperandDialect::Trino => Some(Self::Trino),
            OperandDialect::Redshift => Some(Self::Redshift),
            OperandDialect::SqlServer => None,
        }
    }

    /// The dialect for an adapter type the operand checks have no table for.
    fn from_unruled_adapter(adapter_type: &str) -> Option<Self> {
        adapter_type
            .eq_ignore_ascii_case("spark")
            .then_some(Self::Spark)
    }

    /// The operand-check dialect this one is written in, for the
    /// `[portability] target_dialect` comparison.
    fn operand(self) -> Option<OperandDialect> {
        match self {
            Self::DuckDb => Some(OperandDialect::DuckDb),
            Self::Postgres => Some(OperandDialect::Postgres),
            Self::Snowflake => Some(OperandDialect::Snowflake),
            Self::Databricks => Some(OperandDialect::Databricks),
            Self::BigQuery => Some(OperandDialect::BigQuery),
            Self::Trino => Some(OperandDialect::Trino),
            Self::Redshift => Some(OperandDialect::Redshift),
            Self::Spark => None,
        }
    }

    fn display(self) -> &'static str {
        match self {
            Self::DuckDb => "DuckDB",
            Self::Postgres => "PostgreSQL",
            Self::Snowflake => "Snowflake",
            Self::Databricks => "Databricks",
            Self::Spark => "Spark",
            Self::BigQuery => "BigQuery",
            Self::Trino => "Trino",
            Self::Redshift => "Redshift",
        }
    }

    /// Whether this dialect's list was checked against a live engine.
    pub fn tier(self) -> Tier {
        match self {
            Self::DuckDb => Tier::Verified,
            // PostgreSQL, Trino and Spark were checked live, but extensions,
            // connectors and UDFs add plain-named functions the list cannot
            // hold, and each list is pinned to one engine version.
            Self::Postgres
            | Self::Trino
            | Self::Spark
            | Self::Snowflake
            | Self::Databricks
            | Self::BigQuery
            | Self::Redshift => Tier::Documented,
        }
    }

    fn list(self) -> &'static HashSet<&'static str> {
        match self {
            Self::DuckDb => &DUCKDB_FUNCTIONS,
            Self::Postgres => &POSTGRES_FUNCTIONS,
            Self::Snowflake => &SNOWFLAKE_FUNCTIONS,
            Self::Databricks => &DATABRICKS_FUNCTIONS,
            Self::Spark => &SPARK_FUNCTIONS,
            Self::BigQuery => &BIGQUERY_FUNCTIONS,
            Self::Trino => &TRINO_FUNCTIONS,
            Self::Redshift => &REDSHIFT_FUNCTIONS,
        }
    }

    fn knows(self, lower_name: &str) -> bool {
        self.list().contains(lower_name)
            || SPECIAL_FORMS.contains(&lower_name)
            || self.own_forms().contains(&lower_name)
    }

    /// Call-position forms this dialect's grammar accepts that are not in its
    /// catalog list and that other dialects reject.
    fn own_forms(self) -> &'static [&'static str] {
        match self {
            Self::DuckDb => DUCKDB_ONLY_FORMS,
            Self::Spark | Self::Databricks => SPARK_GRAMMAR_FORMS,
            Self::Snowflake => SNOWFLAKE_GRAMMAR_FORMS,
            Self::Trino => &["table"],
            Self::BigQuery => &["struct"],
            Self::Postgres | Self::Redshift => &[],
        }
    }
}

/// Names accepted in call position that are not catalog functions on any
/// dialect: the parser or the warehouse rewrites them (`COALESCE`, `IF`,
/// `IFNULL` become `CASE` / `COALESCE`), or they are special syntax. Verified
/// against DuckDB 1.5; the same forms exist in the other dialects' grammars.
const SPECIAL_FORMS: &[&str] = &[
    "coalesce",
    "if",
    "ifnull",
    "nullif",
    "greatest",
    "least",
    "grouping",
    "grouping_id",
    "row",
    "map",
    "array",
    "list",
    "exists",
    "cast",
    "try_cast",
    "extract",
    "position",
    "overlay",
    // sqlparser reads `SUBSTR(...)` and `SUBSTRING(...)` as its own
    // `Substring` expression, not as a function call.
    "substr",
    "substring",
    "trim",
    "ceil",
    "floor",
    "current_date",
    "current_time",
    "current_timestamp",
    "localtime",
    "localtimestamp",
    "current_user",
    "current_role",
    "current_catalog",
    "current_schema",
    "session_user",
    "user",
];

/// Spark grammar functions that `SHOW FUNCTIONS` omits: the parser handles
/// `TIMESTAMPADD(unit, n, ts)` and `TIMESTAMPDIFF(unit, a, b)` itself, and
/// `IDENTIFIER('name')` and `TABLE(...)` are clauses. Shared by Spark and
/// Databricks.
const SPARK_GRAMMAR_FORMS: &[&str] = &["timestampadd", "timestampdiff", "identifier", "table"];

/// Snowflake `IDENTIFIER('name')` and `TABLE(...)` clauses.
const SNOWFLAKE_GRAMMAR_FORMS: &[&str] = &["identifier", "table"];

/// Special forms only DuckDB has. DuckDB's parser also turns `date(x)` into
/// `CAST(x AS DATE)`, so `duckdb_functions()` never lists `date`; every other
/// dialect's list already holds it. The ordered-set forms
/// `percentile_cont(p) WITHIN GROUP (ORDER BY x)` and `percentile_disc(p) ...`
/// are handled separately (see `DUCKDB_WITHIN_GROUP_FORMS`; `mode` is listed).
const DUCKDB_ONLY_FORMS: &[&str] = &["columns", "unpack", "try", "date"];

/// Ordered-set aggregates DuckDB rewrites at parse time, and only in the
/// `WITHIN GROUP (ORDER BY ...)` form. The plain call `percentile_cont(x, 0.5)`
/// is an unknown function there, so these names are exempt only when the
/// parsed call carries a non-empty `within_group`.
const DUCKDB_WITHIN_GROUP_FORMS: &[&str] = &["percentile_cont", "percentile_disc"];

/// Report calls to functions the target dialect does not have.
///
/// `target_for` maps a model name to the warehouses it runs on, as for the
/// operand checks. A model that runs on several is checked against each. A
/// dialect without a list is skipped. `written_for` is the dialect
/// `[portability] target_dialect` says the SQL is written in: a model is not
/// checked against any other dialect.
pub fn check_unknown_functions(
    models: &[rocky_core::models::Model],
    registry: &FunctionRegistry,
    target_for: &dyn Fn(&str) -> OperandTarget,
    written_for: Option<OperandDialect>,
    seed_functions: &HashSet<String>,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for model in models {
        let mut dialects: Vec<FunctionDialect> = Vec::new();
        if let OperandTarget::Targets {
            dialects: ruled,
            unruled,
        } = target_for(&model.config.name)
        {
            let from_ruled = ruled
                .iter()
                .filter_map(|d| FunctionDialect::from_operand(*d));
            let from_unruled = unruled
                .iter()
                .filter_map(|a| FunctionDialect::from_unruled_adapter(a));
            for d in from_ruled.chain(from_unruled) {
                if !dialects.contains(&d) {
                    dialects.push(d);
                }
            }
        }
        dialects.retain(|d| written_for.is_none_or(|w| d.operand() == Some(w)));
        if dialects.is_empty() {
            continue;
        }
        diagnostics.extend(check_model(model, registry, &dialects, seed_functions));
    }
    diagnostics
}

/// The function and macro names a project's `data/seed.sql` creates, in
/// lower case, for [`check_unknown_functions`].
///
/// A text scan, not a parse, so it errs toward allowing: it also reads
/// `CREATE MACRO` inside comments and string literals. A missed name is a
/// refusal the seed would have prevented, so the scan takes every match.
/// Comments may sit between the keywords. The scan runs whether or not the
/// seed does, so every entry point sees the same names. The last part of a
/// qualified name is kept (`main.my_fn` is `my_fn`), since a call that is
/// unqualified in the model still reaches it.
pub fn seed_created_functions(seed_sql: &str) -> HashSet<String> {
    static CREATE_FUNCTION: LazyLock<regex::Regex> = LazyLock::new(|| {
        // `SEP` is the whitespace or comment that may sit between two keywords.
        const SEP: &str = r"(?:\s|--[^\n]*|/\*[\s\S]*?\*/)+";
        const IDENT: &str = r#""[^"]+"|[A-Za-z_][A-Za-z0-9_$]*"#;
        let pattern = format!(
            r"(?i)\bcreate{SEP}(?:or{SEP}replace{SEP})?(?:(?:temp|temporary){SEP})?(?:macro|function){SEP}(?:if{SEP}not{SEP}exists{SEP})?((?:{IDENT})(?:\s*\.\s*(?:{IDENT}))*)"
        );
        regex::Regex::new(&pattern).expect("seed function pattern is valid")
    });
    CREATE_FUNCTION
        .captures_iter(seed_sql)
        .filter_map(|caps| caps.get(1))
        .flat_map(|name| {
            name.as_str()
                .split('.')
                .map(|part| part.trim_matches('"').to_ascii_lowercase())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn check_model(
    model: &rocky_core::models::Model,
    registry: &FunctionRegistry,
    dialects: &[FunctionDialect],
    seed_functions: &HashSet<String>,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
    else {
        return Vec::new();
    };
    let [Statement::Query(_)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut visitor = Calls {
        registry,
        found: Vec::new(),
        chained: HashSet::new(),
        top_level: top_level_calls(&statements),
    };
    let _ = statements.visit(&mut visitor);

    // Line numbers are only meaningful when the SQL is the file's own text;
    // a `.rocky` model reaches here as lowered SQL.
    let precise_spans = model
        .file_path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"));
    let file = model.file_path.display().to_string();
    let mut diagnostics = Vec::new();
    for &dialect in dialects {
        let mut seen = HashSet::new();
        for found in &visitor.found {
            let Found {
                name,
                line,
                col,
                within_group,
                ..
            } = found;
            let lower = name.to_ascii_lowercase();
            if dialect == FunctionDialect::DuckDb
                && *within_group
                && DUCKDB_WITHIN_GROUP_FORMS.contains(&lower.as_str())
            {
                continue;
            }
            // A macro the project's seed creates runs on DuckDB; it is not a
            // call to an unknown function (the seed runs or not).
            if dialect == FunctionDialect::DuckDb && seed_functions.contains(&lower) {
                continue;
            }
            if dialect.knows(&lower) || !seen.insert(lower) {
                continue;
            }
            let (line, col) = if precise_spans && *line > 0 {
                (*line, *col)
            } else {
                (1, 1)
            };
            let (code, constructor, phrase): (_, fn(&str, &str, String) -> Diagnostic, _) =
                match dialect.tier() {
                    Tier::Verified => (E057, Diagnostic::error, "does not exist in"),
                    Tier::Documented => (
                        W057,
                        Diagnostic::warning,
                        "is not in Rocky's function list for",
                    ),
                };
            diagnostics.push(
                constructor(
                    code,
                    &model.config.name,
                    format!(
                        "function `{name}` {phrase} {} and is not a project function in \
                         `functions/`",
                        dialect.display()
                    ),
                )
                .with_suggestion(suggestion(name, dialect))
                .with_span(SourceSpan {
                    file: file.clone(),
                    line,
                    col,
                }),
            );
        }
    }
    if dialects.contains(&FunctionDialect::DuckDb) {
        diagnostics.extend(arity_diagnostics(
            &model.config.name,
            &visitor.found,
            seed_functions,
            &file,
            precise_spans,
        ));
    }
    diagnostics
}

/// E068 / W068: a DuckDB built-in called with an argument count DuckDB
/// refuses. Only a count the SQL states is judged ([`Found::arity`]). A
/// name the seed, a parser rewrite or a special form owns is skipped. An
/// error only where DuckDB always binds the call ([`Found::always_bound`]).
///
/// `precise_spans` is false for SQL that is not the file's own text (a
/// `.rocky` model), so the span is `1:1` as for E057.
fn arity_diagnostics(
    model_name: &str,
    found: &[Found],
    seed_functions: &HashSet<String>,
    file: &str,
    precise_spans: bool,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for call in found {
        let Some(arity) = call.arity else {
            continue;
        };
        if call.within_group {
            continue;
        }
        let lower = call.name.to_ascii_lowercase();
        if SPECIAL_FORMS.contains(&lower.as_str())
            || DUCKDB_ONLY_FORMS.contains(&lower.as_str())
            || seed_functions.contains(&lower)
        {
            continue;
        }
        let Some(counts) = DUCKDB_ARITIES.get(lower.as_str()) else {
            continue;
        };
        if counts.iter().any(|count| count.accepts(arity)) {
            continue;
        }
        let (code, constructor): (_, fn(&str, &str, String) -> Diagnostic) = if call.always_bound {
            (E068, Diagnostic::error)
        } else {
            (W068, Diagnostic::warning)
        };
        let (line, col) = if precise_spans && call.line > 0 {
            (call.line, call.col)
        } else {
            (1, 1)
        };
        let diagnostic = constructor(
            code,
            model_name,
            format!(
                "DuckDB refuses `{}` with {arity} argument{}: it takes {}",
                call.name,
                if arity == 1 { "" } else { "s" },
                describe_counts(counts),
            ),
        )
        .with_suggestion(format!(
            "check the argument list against DuckDB's signatures for `{}`",
            call.name
        ))
        .with_span(SourceSpan {
            file: file.to_string(),
            line,
            col,
        });
        diagnostics.push(diagnostic);
    }
    diagnostics
}

/// "did you mean …" from edit distance, plus the escape hatch.
fn suggestion(name: &str, dialect: FunctionDialect) -> String {
    let wanted = name.to_ascii_lowercase();
    let threshold = (wanted.chars().count() / 3).max(1);
    let mut close: Vec<(usize, &str)> = dialect
        .list()
        .iter()
        .copied()
        .chain(SPECIAL_FORMS.iter().copied())
        .map(|known| (strsim::levenshtein(&wanted, known), known))
        .filter(|(distance, _)| *distance <= threshold)
        .collect();
    close.sort_unstable();
    close.dedup_by(|a, b| a.1 == b.1);
    let hatch = match dialect.tier() {
        Tier::Verified => {
            "If it is a macro or an extension function loaded outside Rocky, call it \
             schema-qualified (`main.<name>(...)`), which Rocky does not check"
        }
        Tier::Documented => {
            "If the warehouse has it (a newer release, an extension, or a function created \
             outside Rocky), call it schema-qualified, which Rocky does not check, or declare it \
             in `functions/`"
        }
    };
    if close.is_empty() {
        format!("check the spelling. {hatch}")
    } else {
        let names: Vec<String> = close
            .iter()
            .take(3)
            .map(|(_, known)| format!("`{known}`"))
            .collect();
        format!("did you mean {}? {hatch}", names.join(" or "))
    }
}

/// One argument count a DuckDB function accepts: exactly `N`, or `N` and
/// more (a variadic function).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArgCount {
    Exact(usize),
    AtLeast(usize),
}

impl ArgCount {
    fn accepts(self, arity: usize) -> bool {
        match self {
            Self::Exact(n) => n == arity,
            Self::AtLeast(n) => arity >= n,
        }
    }
}

/// DuckDB's argument counts per function name, from `data/duckdb_arities.txt`
/// (see the header there and the `duckdb_arity_table` test). A name not in
/// the table is not judged.
static DUCKDB_ARITIES: LazyLock<std::collections::HashMap<&'static str, Vec<ArgCount>>> =
    LazyLock::new(|| parse_arities(include_str!("data/duckdb_arities.txt")));

/// Parse `name: 1 2 3+` lines. A malformed line is dropped, which leaves the
/// name unjudged rather than judged by a wrong count.
fn parse_arities(text: &'static str) -> std::collections::HashMap<&'static str, Vec<ArgCount>> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .filter_map(|line| {
            let (name, counts) = line.split_once(':')?;
            let counts = counts
                .split_whitespace()
                .map(|token| match token.strip_suffix('+') {
                    Some(n) => n.parse().ok().map(ArgCount::AtLeast),
                    None => token.parse().ok().map(ArgCount::Exact),
                })
                .collect::<Option<Vec<_>>>()?;
            (!counts.is_empty()).then_some((name.trim(), counts))
        })
        .collect()
}

/// "1 or 2", "1, 2 or at least 3" for the counts a function takes.
fn describe_counts(counts: &[ArgCount]) -> String {
    let parts: Vec<String> = counts
        .iter()
        .map(|count| match count {
            ArgCount::Exact(n) => n.to_string(),
            ArgCount::AtLeast(n) => format!("at least {n}"),
        })
        .collect();
    match parts.as_slice() {
        [] => String::new(),
        [only] => only.clone(),
        [init @ .., last] => format!("{} or {last}", init.join(", ")),
    }
}

/// One unqualified call the project does not declare.
struct Found {
    name: String,
    line: usize,
    col: usize,
    within_group: bool,
    /// The number of arguments the SQL states, when it is known. `None` for
    /// `*`, `DISTINCT`, named arguments, lambdas, `COLUMNS(...)`/`UNPACK(...)`
    /// arguments, a dot-call chain or an ordered-set `WITHIN GROUP` call.
    arity: Option<usize>,
    /// Whether DuckDB always binds this call (see [`top_level_calls`]).
    always_bound: bool,
}

struct Calls<'r> {
    registry: &'r FunctionRegistry,
    /// Unqualified, unquoted calls the project does not declare.
    found: Vec<Found>,
    /// Calls that are the root of a dot-call chain (`upper(x).lower()`):
    /// their argument count is not the count the SQL states.
    chained: HashSet<*const Expr>,
    /// Calls DuckDB always binds: see [`top_level_calls`].
    top_level: HashSet<*const Expr>,
}

impl Visitor for Calls<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        // A dot-call chain (`upper(x).lower()`) passes the root and every
        // link to the next call. Their counts are not the ones the SQL states.
        if let Expr::CompoundFieldAccess { root, access_chain } = expr {
            let links = access_chain.iter().filter_map(|access| match access {
                ast::AccessExpr::Dot(link) => Some(link),
                ast::AccessExpr::Subscript(_) => None,
            });
            for link in std::iter::once(root.as_ref()).chain(links) {
                if matches!(link, Expr::Function(_)) {
                    self.chained.insert(link as *const Expr);
                }
            }
        }
        if let Expr::Function(function) = expr
            && let [ast::ObjectNamePart::Identifier(ident)] = function.name.0.as_slice()
            && ident.quote_style.is_none()
            && !self.registry.declares(&ident.value.to_ascii_lowercase())
        {
            let span = expr.span();
            let chained = self.chained.contains(&(expr as *const Expr));
            self.found.push(Found {
                name: ident.value.clone(),
                line: span.start.line as usize,
                col: span.start.column as usize,
                within_group: !function.within_group.is_empty(),
                arity: if chained {
                    None
                } else {
                    stated_arity(function)
                },
                always_bound: self.top_level.contains(&(expr as *const Expr)),
            });
        }
        std::ops::ControlFlow::Continue(())
    }
}

/// The argument count the SQL states for `function`, or `None` when DuckDB
/// may bind a different count (see [`Found::arity`]).
fn stated_arity(function: &ast::Function) -> Option<usize> {
    if !function.within_group.is_empty() {
        return None;
    }
    if !matches!(function.parameters, ast::FunctionArguments::None) {
        return None;
    }
    match &function.args {
        ast::FunctionArguments::None => Some(0),
        ast::FunctionArguments::Subquery(_) => None,
        ast::FunctionArguments::List(list) => {
            if list.duplicate_treatment.is_some() {
                return None;
            }
            let mut count = 0;
            for arg in &list.args {
                let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg else {
                    return None;
                };
                if !is_counted_argument(expr) {
                    return None;
                }
                count += 1;
            }
            Some(count)
        }
    }
}

/// An argument that is one value: not a lambda, and not a `COLUMNS(...)` or
/// `UNPACK(...)` call that expands to several.
fn is_counted_argument(expr: &Expr) -> bool {
    match expr {
        Expr::Lambda(_) => false,
        // `name := value` is a named argument; the Databricks grammar Rocky
        // parses with reads it as an assignment operator, not `FunctionArg::Named`.
        Expr::BinaryOp {
            op: ast::BinaryOperator::Assignment,
            ..
        } => false,
        Expr::Function(function) => {
            let expands = matches!(
                function.name.0.as_slice(),
                [ast::ObjectNamePart::Identifier(ident)]
                    if ident.value.eq_ignore_ascii_case("columns")
                        || ident.value.eq_ignore_ascii_case("unpack")
            );
            !expands
        }
        _ => true,
    }
}

/// The calls DuckDB always binds, for error placement: those in the main
/// `SELECT` list, `WHERE` or `HAVING`, reached without passing through a
/// subquery. A CTE body, a derived table, a subquery, an `ORDER BY`, a named
/// `WINDOW` or a set-operation branch is not listed, since DuckDB may skip
/// binding it.
fn top_level_calls(statements: &[Statement]) -> HashSet<*const Expr> {
    struct Depth {
        query_depth: usize,
        calls: HashSet<*const Expr>,
    }
    impl Visitor for Depth {
        type Break = ();

        fn pre_visit_query(&mut self, _query: &ast::Query) -> std::ops::ControlFlow<()> {
            self.query_depth += 1;
            std::ops::ControlFlow::Continue(())
        }

        fn post_visit_query(&mut self, _query: &ast::Query) -> std::ops::ControlFlow<()> {
            self.query_depth = self.query_depth.saturating_sub(1);
            std::ops::ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
            if self.query_depth == 0 && matches!(expr, Expr::Function(_)) {
                self.calls.insert(expr as *const Expr);
            }
            std::ops::ControlFlow::Continue(())
        }
    }

    let mut roots: Vec<&Expr> = Vec::new();
    for statement in statements {
        let Statement::Query(query) = statement else {
            continue;
        };
        let ast::SetExpr::Select(select) = query.body.as_ref() else {
            continue;
        };
        for item in &select.projection {
            match item {
                ast::SelectItem::UnnamedExpr(expr)
                | ast::SelectItem::ExprWithAlias { expr, .. } => roots.push(expr),
                _ => {}
            }
        }
        roots.extend(select.selection.iter());
        roots.extend(select.having.iter());
    }
    let mut depth = Depth {
        query_depth: 0,
        calls: HashSet::new(),
    };
    for root in roots {
        let _ = root.visit(&mut depth);
    }
    depth.calls
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every plain-named function the bundled DuckDB lists is in Rocky's
    /// DuckDB list. DuckDB misses are `E057` errors, so a stale list refuses
    /// valid SQL; the list was last regenerated for DuckDB 2.0, which added
    /// functions such as `if`, `overlay` and `json_set`. Only the extensions
    /// the bundled build links are listed here; the list also carries
    /// autoloadable ones, which this cannot see offline.
    #[test]
    fn the_duckdb_list_covers_the_bundled_duckdb() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut stmt = conn
            .prepare("SELECT DISTINCT lower(function_name) FROM duckdb_functions()")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(
            names.len() > 500,
            "duckdb_functions() listed {}",
            names.len()
        );
        let missing: Vec<&str> = names
            .iter()
            .map(String::as_str)
            .filter(|n| {
                n.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            })
            .filter(|n| !FunctionDialect::DuckDb.knows(n))
            .collect();
        assert!(
            missing.is_empty(),
            "regenerate data/duckdb_functions.txt; missing: {missing:?}"
        );
    }
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};
    use std::path::PathBuf;

    fn model(name: &str, sql: &str) -> Model {
        Model {
            drop_existing_kind: None,
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "silver".to_string(),
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
            file_path: PathBuf::from(format!("models/{name}.sql")),
            contract_path: None,
        }
    }

    fn run(sql: &str, target: &OperandTarget, registry: &FunctionRegistry) -> Vec<Diagnostic> {
        check_unknown_functions(
            &[model("m", sql)],
            registry,
            &|_| target.clone(),
            None,
            &HashSet::new(),
        )
    }

    fn registry_with(name: &str) -> FunctionRegistry {
        use rocky_core::functions::{
            FunctionArgument, FunctionConfig, FunctionDef, FunctionTarget,
        };
        let def = FunctionDef {
            name: name.to_string(),
            config: FunctionConfig {
                name: None,
                description: None,
                language: "sql".to_string(),
                returns: "VARCHAR".to_string(),
                arguments: vec![FunctionArgument {
                    name: "value".to_string(),
                    data_type: "DOUBLE".to_string(),
                }],
                deterministic: None,
                target: FunctionTarget::default(),
            },
            body: Some("CAST(value AS VARCHAR)".to_string()),
            file_path: PathBuf::from(format!("functions/{name}.toml")),
        };
        let (registry, diags) =
            crate::udf::build_registry(rocky_core::functions::LoadedFunctions {
                functions: vec![def],
                errors: Vec::new(),
            });
        assert!(diags.is_empty(), "{diags:?}");
        registry
    }

    fn duckdb() -> OperandTarget {
        Some(OperandDialect::DuckDb).into()
    }

    #[test]
    fn a_misspelled_aggregate_is_refused_on_duckdb() {
        let diags = run(
            "SELECT customer_id,\n    SUMM(amount) AS lifetime_value\nFROM fct_orders GROUP BY customer_id",
            &duckdb(),
            &FunctionRegistry::default(),
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(&*diags[0].code, "E057");
        assert!(diags[0].is_error());
        assert!(diags[0].message.contains("`SUMM`"), "{diags:?}");
        let suggestion = diags[0].suggestion.as_deref().unwrap();
        assert!(suggestion.contains("`sum`"), "{suggestion}");
        let span = diags[0].span.as_ref().unwrap();
        assert_eq!((span.line, span.col), (2, 5), "points at the call");
    }

    #[test]
    fn unknown_calls_are_found_in_every_clause() {
        for sql in [
            "SELECT a FROM t WHERE no_such_fn(a) > 1",
            "SELECT a FROM t JOIN u ON no_such_fn(t.a) = u.a",
            "SELECT a FROM t GROUP BY no_such_fn(a)",
            "SELECT a FROM t ORDER BY no_such_fn(a)",
            "WITH c AS (SELECT no_such_fn(a) AS b FROM t) SELECT b FROM c",
            "SELECT (SELECT no_such_fn(1)) AS x",
            "SELECT upper(no_such_fn(a)) AS x FROM t",
        ] {
            let diags = run(sql, &duckdb(), &FunctionRegistry::default());
            assert_eq!(diags.len(), 1, "`{sql}`: {diags:?}");
        }
    }

    fn spark() -> OperandTarget {
        OperandTarget::Targets {
            dialects: Vec::new(),
            unruled: vec!["spark".to_string()],
        }
    }

    /// Every dialect with a list: its target, the tier of its diagnostic, a
    /// real function of that warehouse (a control) and the code.
    fn dialect_cases() -> Vec<(&'static str, OperandTarget, &'static str, &'static str)> {
        vec![
            ("DuckDB", duckdb(), "E057", "list_transform(a, x -> x)"),
            (
                "PostgreSQL",
                Some(OperandDialect::Postgres).into(),
                "W057",
                "date_trunc('day', d)",
            ),
            (
                "Snowflake",
                Some(OperandDialect::Snowflake).into(),
                "W057",
                "dateadd(day, 1, d)",
            ),
            (
                "Databricks",
                Some(OperandDialect::Databricks).into(),
                "W057",
                "try_divide(a, b)",
            ),
            ("Spark", spark(), "W057", "array_distinct(a)"),
            (
                "BigQuery",
                Some(OperandDialect::BigQuery).into(),
                "W057",
                "timestamp_diff(a, b, DAY)",
            ),
            (
                "Trino",
                Some(OperandDialect::Trino).into(),
                "W057",
                "approx_distinct(a)",
            ),
            (
                "Redshift",
                Some(OperandDialect::Redshift).into(),
                "W057",
                "listagg(s, ',')",
            ),
        ]
    }

    #[test]
    fn every_dialect_refuses_a_misspelled_function_at_its_tier() {
        let registry = FunctionRegistry::default();
        for (name, target, code, _) in dialect_cases() {
            let diags = run("SELECT SUMM(amount) AS s FROM t", &target, &registry);
            assert_eq!(diags.len(), 1, "{name}: {diags:?}");
            assert_eq!(&*diags[0].code, code, "{name}");
            assert_eq!(diags[0].is_error(), code == "E057", "{name}");
            assert!(diags[0].message.contains(name), "{name}: {diags:?}");
            assert!(diags[0].message.contains("`SUMM`"), "{name}");
            let suggestion = diags[0].suggestion.as_deref().unwrap();
            assert!(suggestion.contains("`sum`"), "{name}: {suggestion}");
        }
    }

    #[test]
    fn every_dialect_accepts_its_own_functions() {
        let registry = FunctionRegistry::default();
        for (name, target, _, call) in dialect_cases() {
            let sql = format!(
                "SELECT {call} AS c, SUM(a) AS s, COUNT(*) AS n, MAX(a) AS m, \
                 COALESCE(a, 0) AS k, row_number() OVER (ORDER BY a) AS rn FROM t"
            );
            let diags = run(&sql, &target, &registry);
            assert!(diags.is_empty(), "{name}: {diags:?}");
        }
    }

    #[test]
    fn a_project_function_is_not_flagged_on_any_dialect() {
        let registry = registry_with("ltv_band");
        for (name, target, _, _) in dialect_cases() {
            let diags = run("SELECT ltv_band(a) AS b FROM t", &target, &registry);
            assert!(diags.is_empty(), "{name}: {diags:?}");
            let diags = run("SELECT other_band(a) AS b FROM t", &target, &registry);
            assert_eq!(diags.len(), 1, "{name}: {diags:?}");
        }
    }

    #[test]
    fn qualified_and_quoted_calls_are_not_checked_on_any_dialect() {
        let registry = FunctionRegistry::default();
        for (name, target, _, _) in dialect_cases() {
            let diags = run(
                "SELECT main.my_fn(a) AS x, NET.HOST(a) AS y, \"Weird Fn\"(a) AS z FROM t",
                &target,
                &registry,
            );
            assert!(diags.is_empty(), "{name}: {diags:?}");
        }
    }

    #[test]
    fn dialects_without_a_list_and_unconfigured_projects_stay_silent() {
        let sql = "SELECT SUMM(amount) AS s FROM t";
        let registry = FunctionRegistry::default();
        assert!(run(sql, &OperandTarget::Unconfigured, &registry).is_empty());
        assert!(run(sql, &Some(OperandDialect::SqlServer).into(), &registry).is_empty());
        let clickhouse = OperandTarget::Targets {
            dialects: Vec::new(),
            unruled: vec!["clickhouse".to_string()],
        };
        assert!(run(sql, &clickhouse, &registry).is_empty());
    }

    #[test]
    fn a_model_on_several_warehouses_is_checked_on_each() {
        let both = OperandTarget::Targets {
            dialects: vec![OperandDialect::Snowflake, OperandDialect::DuckDb],
            unruled: Vec::new(),
        };
        let diags = run(
            "SELECT SUMM(amount) AS s FROM t",
            &both,
            &FunctionRegistry::default(),
        );
        let mut codes: Vec<&str> = diags.iter().map(|d| &*d.code).collect();
        codes.sort_unstable();
        assert_eq!(codes, ["E057", "W057"]);
    }

    #[test]
    fn sql_written_for_another_warehouse_is_not_judged_by_this_one() {
        let sql = "SELECT SUMM(amount) AS s FROM t";
        let registry = FunctionRegistry::default();
        let on_duckdb = duckdb();
        let models = [model("m", sql)];
        let check = |written_for| {
            check_unknown_functions(
                &models,
                &registry,
                &|_| on_duckdb.clone(),
                written_for,
                &HashSet::new(),
            )
        };
        assert_eq!(check(None).len(), 1);
        assert_eq!(check(Some(OperandDialect::DuckDb)).len(), 1);
        assert!(check(Some(OperandDialect::Snowflake)).is_empty());
    }

    #[test]
    fn only_a_list_checked_against_a_live_engine_reports_an_error() {
        for (name, target, code, _) in dialect_cases() {
            let diags = run(
                "SELECT no_such_fn(a) AS x FROM t",
                &target,
                &FunctionRegistry::default(),
            );
            assert_eq!(diags.len(), 1, "{name}");
            assert_eq!(diags[0].is_error(), code == "E057", "{name}");
        }
        for dialect in [
            FunctionDialect::Postgres,
            FunctionDialect::Trino,
            FunctionDialect::Spark,
            FunctionDialect::Snowflake,
            FunctionDialect::Databricks,
            FunctionDialect::BigQuery,
            FunctionDialect::Redshift,
        ] {
            assert_eq!(dialect.tier(), Tier::Documented, "{dialect:?}");
        }
        assert_eq!(FunctionDialect::DuckDb.tier(), Tier::Verified);
    }

    #[test]
    fn valid_calls_stay_clean() {
        let sql = "SELECT
            customer_id,
            SUM(amount) AS a, COUNT(*) AS b, AVG(amount) AS c, MIN(d) AS e, MAX(d) AS f,
            COUNT(DISTINCT s) AS g, COALESCE(x, 0) AS h, IFNULL(x, 0) AS i, IF(x > 1, 1, 2) AS j,
            NULLIF(x, 0) AS k, CAST(x AS INT) AS l, TRY_CAST(x AS INT) AS m,
            date_trunc('month', d) AS n, strftime(d, '%Y') AS o, CURRENT_DATE AS p,
            current_timestamp AS q, now() AS r, row_number() OVER (ORDER BY d) AS rn,
            list_transform([1, 2], v -> v + 1) AS t, regexp_matches(s, 'a') AS u,
            st_area(geom) AS v, json_extract(j, '$.a') AS w, epoch_ms(d) AS x2,
            GREATEST(x, 1) AS y, len(s) AS z, string_agg(s, ',') AS sa,
            EXTRACT(year FROM d) AS ex, TRIM(s) AS tr, SUBSTRING(s, 1, 2) AS ss,
            main.my_macro(x) AS mm, \"Weird Fn\"(x) AS wf, ltv_band(x) AS lb,
            GROUPING(customer_id) AS gr
        FROM t
        GROUP BY customer_id";
        let registry = registry_with("ltv_band");
        for target in [duckdb(), OperandTarget::Unconfigured] {
            let diags = run(sql, &target, &registry);
            assert!(diags.is_empty(), "{diags:?}");
        }
    }

    #[test]
    fn unparseable_sql_is_skipped() {
        let diags = run("SELECT SUMM( FROM", &duckdb(), &FunctionRegistry::default());
        assert!(diags.is_empty());
    }

    #[test]
    fn the_list_is_loaded() {
        for name in ["sum", "count", "date_trunc", "list_transform", "st_area"] {
            assert!(FunctionDialect::DuckDb.knows(name), "{name}");
        }
        assert!(!FunctionDialect::DuckDb.knows("summ"));
    }

    #[test]
    fn the_live_checked_lists_hold_names_the_catalog_hides() {
        for (dialect, names) in [
            (
                FunctionDialect::Postgres,
                [
                    "xmlelement",
                    "json_value",
                    "_pg_expandarray",
                    "ri_fkey_check_ins",
                ],
            ),
            (
                FunctionDialect::Spark,
                ["table_changes", "<<", "array_distinct", "try_divide"],
            ),
            (
                FunctionDialect::Trino,
                ["version", "format", "dot_product", "approx_distinct"],
            ),
        ] {
            for name in names {
                assert!(dialect.knows(name), "{dialect:?}: {name}");
            }
        }
    }

    #[test]
    fn duckdb_refuses_forms_it_does_not_have() {
        // Live DuckDB 1.5 rejects each of these as an unknown function.
        let registry = FunctionRegistry::default();
        for call in [
            "nvl(a, 0)",
            "nvl2(a, 1, 0)",
            "iff(a, 1, 0)",
            "struct(a)",
            "identifier('t')",
        ] {
            let diags = run(&format!("SELECT {call} AS c FROM t"), &duckdb(), &registry);
            assert_eq!(diags.len(), 1, "{call}: {diags:?}");
            assert_eq!(&*diags[0].code, "E057", "{call}");
        }
    }

    #[test]
    fn duckdb_accepts_the_casts_its_parser_writes_as_calls() {
        // Live DuckDB 1.5.5 runs `date(x)` as `CAST(x AS DATE)`, so
        // `duckdb_functions()` never lists it. It is the common way to take
        // the day of a timestamp.
        let registry = FunctionRegistry::default();
        for call in [
            "date(occurred_at)",
            "DATE(occurred_at)",
            "date('2024-01-01')",
            "interval('1 day')",
        ] {
            let diags = run(&format!("SELECT {call} AS c FROM t"), &duckdb(), &registry);
            assert!(diags.is_empty(), "{call}: {diags:?}");
        }
    }

    #[test]
    fn duckdb_accepts_the_ordered_set_aggregates_it_rewrites() {
        // Live DuckDB 2.0 runs each of these; `duckdb_functions()` lists only
        // the `quantile_*` names they are rewritten to.
        let registry = FunctionRegistry::default();
        for call in [
            "percentile_cont(0.5) WITHIN GROUP (ORDER BY a)",
            "percentile_disc(0.5) WITHIN GROUP (ORDER BY a)",
            "mode() WITHIN GROUP (ORDER BY a)",
        ] {
            let diags = run(&format!("SELECT {call} AS c FROM t"), &duckdb(), &registry);
            assert!(diags.is_empty(), "{call}: {diags:?}");
        }
    }

    #[test]
    fn only_duckdb_exempts_the_within_group_percentiles() {
        // The exemption is DuckDB's rewrite; Trino's list does not hold them.
        let registry = FunctionRegistry::default();
        let diags = run(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY a) AS c FROM t",
            &Some(OperandDialect::Trino).into(),
            &registry,
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(&*diags[0].code, "W057");
    }

    #[test]
    fn duckdb_refuses_the_plain_percentile_call() {
        // Live DuckDB rejects `percentile_cont(a, 0.5)`; only the WITHIN GROUP
        // form is rewritten.
        let registry = FunctionRegistry::default();
        for call in ["percentile_cont(a, 0.5)", "percentile_disc(a, 0.5)"] {
            let diags = run(&format!("SELECT {call} AS c FROM t"), &duckdb(), &registry);
            assert_eq!(diags.len(), 1, "{call}: {diags:?}");
            assert_eq!(&*diags[0].code, "E057", "{call}");
        }
    }

    #[test]
    fn dialects_that_have_a_form_accept_it() {
        let registry = FunctionRegistry::default();
        let on = |dialect: Option<OperandDialect>, call: &str| {
            run(
                &format!("SELECT {call} AS c FROM t"),
                &dialect.into(),
                &registry,
            )
        };
        for (target, call) in [
            (Some(OperandDialect::Snowflake), "nvl(a, 0)"),
            (Some(OperandDialect::Snowflake), "iff(a, 1, 0)"),
            (Some(OperandDialect::Databricks), "nvl2(a, 1, 0)"),
            (Some(OperandDialect::Databricks), "iff(a, 1, 0)"),
            (Some(OperandDialect::Redshift), "nvl(a, 0)"),
            (Some(OperandDialect::BigQuery), "struct(a)"),
        ] {
            assert!(on(target, call).is_empty(), "{target:?}: {call}");
        }
        let spark = run("SELECT nvl(a, 0) AS c FROM t", &spark(), &registry);
        assert!(spark.is_empty(), "{spark:?}");
        // Postgres has neither.
        let diags = on(Some(OperandDialect::Postgres), "nvl(a, 0)");
        assert_eq!(diags.len(), 1, "{diags:?}");
    }

    #[test]
    fn spark_and_databricks_accept_grammar_level_functions() {
        let registry = FunctionRegistry::default();
        let sql = "SELECT timestampadd(DAY, 1, d) AS a, timestampdiff(DAY, d, e) AS b FROM t";
        for target in [spark(), Some(OperandDialect::Databricks).into()] {
            let diags = run(sql, &target, &registry);
            assert!(diags.is_empty(), "{diags:?}");
        }
        // Not a function on DuckDB.
        assert!(!run(sql, &duckdb(), &registry).is_empty());
    }

    #[test]
    fn databricks_holds_every_spark_name() {
        for name in SPARK_FUNCTIONS.iter() {
            assert!(FunctionDialect::Databricks.knows(name), "{name}");
        }
    }

    /// #2691: the scan finds a seed macro however the seed writes it.
    #[test]
    fn seed_scan_finds_macros_in_every_written_form() {
        let seed = "CREATE MACRO a(x) AS x;\n\
            create or replace temp macro b(x) as x;\n\
            CREATE FUNCTION main.c(x) AS x;\n\
            CREATE /* note */ MACRO d(x) AS x;\n\
            CREATE\n-- split\nMACRO e(x) AS x;\n\
            CREATE MACRO IF NOT EXISTS f(x) AS x;\n\
            CREATE MACRO \"G\"(x) AS x;\n\
            CREATE TABLE t AS SELECT 1 AS h;\n";
        let names = seed_created_functions(seed);
        for name in ["a", "b", "c", "d", "e", "f", "g"] {
            assert!(names.contains(name), "{name}: {names:?}");
        }
        assert!(!names.contains("t"), "{names:?}");
    }

    /// #2691, the DuckDB verdict next to Rocky's: the seed runs, the model
    /// binds, and the check no longer refuses the seed's macro.
    #[test]
    fn a_seed_macro_is_not_an_unknown_function_on_duckdb() {
        let seed = "CREATE TABLE orders AS SELECT 'a' AS status;\n\
                    CREATE MACRO my_fn(x) AS upper(x);\n";
        let sql = "SELECT my_fn(status) AS s FROM orders";
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(seed).unwrap();
        conn.prepare(sql).expect("DuckDB binds the model");

        let registry = FunctionRegistry::default();
        let names = seed_created_functions(seed);
        let models = [model("m", sql)];
        let with_seed = check_unknown_functions(&models, &registry, &|_| duckdb(), None, &names);
        assert!(with_seed.is_empty(), "{with_seed:?}");
        // Without the seed's names the same call is still refused.
        let without =
            check_unknown_functions(&models, &registry, &|_| duckdb(), None, &HashSet::new());
        assert_eq!(without.len(), 1, "{without:?}");
    }

    /// The largest argument count the table probes. DuckDB's built-ins take
    /// at most a handful; a variadic one is `N+`, where `N` is the smallest
    /// count of the tail the probe accepts.
    const PROBE_MAX: usize = 10;

    /// The argument counts DuckDB binds for `name`, by probing `SELECT
    /// name(NULL, ...)` with `0..=PROBE_MAX` arguments. A NULL argument
    /// matches any parameter type, so a refusal of the probe is a refusal of
    /// the count. Only DuckDB's "No function matches" refuses a count; any
    /// other bind error means the count is not judged.
    fn probe_counts(conn: &duckdb::Connection, name: &str) -> Vec<ArgCount> {
        let accepted: Vec<bool> = (0..=PROBE_MAX)
            .map(|n| {
                let nulls = vec!["NULL"; n].join(", ");
                match conn.prepare(&format!("SELECT {name}({nulls})")) {
                    Ok(_) => true,
                    Err(err) => !err.to_string().contains("No function matches"),
                }
            })
            .collect();
        // The start of the run of counts accepted up to `PROBE_MAX`, when the
        // function is variadic: every count from there on is accepted.
        let open_from = accepted[PROBE_MAX].then(|| {
            (0..=PROBE_MAX)
                .rev()
                .take_while(|&n| accepted[n])
                .last()
                .unwrap_or(PROBE_MAX)
        });
        let mut counts = Vec::new();
        for (n, &ok) in accepted.iter().enumerate() {
            if Some(n) == open_from {
                counts.push(ArgCount::AtLeast(n));
                break;
            }
            if ok {
                counts.push(ArgCount::Exact(n));
            }
        }
        counts
    }

    /// The table as DuckDB gives it: every plain lower-case function name
    /// `duckdb_functions()` lists that accepts some count, with its counts.
    fn live_arity_table(conn: &duckdb::Connection) -> Vec<(String, Vec<ArgCount>)> {
        // A name an extension other than core_functions provides can have an
        // overload with another count; the bundled build cannot load it offline.
        let extension_owned: HashSet<&str> = EXTENSION_OWNED
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .collect();
        let mut stmt = conn
            .prepare("SELECT DISTINCT lower(function_name) FROM duckdb_functions() ORDER BY 1")
            .unwrap();
        let names: Vec<String> = stmt
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        names
            .into_iter()
            .filter(|n| {
                !n.is_empty()
                    && n.bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
            })
            .filter(|n| !extension_owned.contains(&n.as_str()))
            .filter_map(|name| {
                let counts = probe_counts(conn, &name);
                (!counts.is_empty()).then_some((name, counts))
            })
            .collect()
    }

    fn format_counts(counts: &[ArgCount]) -> String {
        counts
            .iter()
            .map(|c| match c {
                ArgCount::Exact(n) => n.to_string(),
                ArgCount::AtLeast(n) => format!("{n}+"),
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    const ARITY_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/src/data/duckdb_arities.txt");
    const EXTENSION_OWNED: &str = include_str!("data/duckdb_extension_functions.txt");

    /// Regenerate `data/duckdb_arities.txt` from the bundled DuckDB. Run:
    /// `cargo test -p rocky-compiler write_duckdb_arity_table -- --ignored`.
    #[test]
    #[ignore = "writes data/duckdb_arities.txt"]
    fn write_duckdb_arity_table() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut text = String::from(
            "# DuckDB argument counts, per function name: `name: N N+`. `N+` is N or more.\n\
             # Generated from the bundled DuckDB (libduckdb-sys) by probing `SELECT name(NULL, ...)`\n\
             # with 0..=10 arguments; a count DuckDB refuses with \"No function matches\" is absent.\n\
             # Names an extension other than core_functions provides are left out\n\
             # (data/duckdb_extension_functions.txt).\n\
             # Regenerate: cargo test -p rocky-compiler write_duckdb_arity_table -- --ignored\n",
        );
        for (name, counts) in live_arity_table(&conn) {
            text.push_str(&format!("{name}: {}\n", format_counts(&counts)));
        }
        std::fs::write(ARITY_FILE, text).unwrap();
    }

    /// The committed table is the bundled DuckDB's table, name by name.
    #[test]
    fn duckdb_arity_table_matches_the_bundled_duckdb() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let live = live_arity_table(&conn);
        assert!(live.len() > 300, "probed {} names", live.len());
        let committed: Vec<(String, String)> = std::fs::read_to_string(ARITY_FILE)
            .unwrap()
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .map(|l| {
                let (n, c) = l.split_once(": ").unwrap();
                (n.to_string(), c.to_string())
            })
            .collect();
        let expected: Vec<(String, String)> = live
            .iter()
            .map(|(n, c)| (n.clone(), format_counts(c)))
            .collect();
        assert!(
            committed == expected,
            "regenerate data/duckdb_arities.txt: `cargo test -p rocky-compiler \
             write_duckdb_arity_table -- --ignored`"
        );
    }

    /// Every name the table holds parses, so none is dropped silently.
    #[test]
    fn every_arity_line_parses() {
        let lines = std::fs::read_to_string(ARITY_FILE).unwrap();
        let data_lines = lines
            .lines()
            .filter(|l| !l.is_empty() && !l.starts_with('#'))
            .count();
        assert_eq!(DUCKDB_ARITIES.len(), data_lines);
    }

    /// The DuckDB verdict for one SQL text: does it bind?
    fn duckdb_binds(conn: &duckdb::Connection, sql: &str) -> bool {
        conn.prepare(sql).is_ok()
    }

    /// The E068 / W068 codes Rocky gives `sql` on DuckDB.
    fn arity_codes(sql: &str) -> Vec<String> {
        let registry = FunctionRegistry::default();
        run(sql, &duckdb(), &registry)
            .iter()
            .filter(|d| matches!(d.code.to_string().as_str(), "E068" | "W068"))
            .map(|d| d.code.to_string())
            .collect()
    }

    /// E068 is an error where DuckDB always binds the call, the main
    /// `SELECT` list, `WHERE` or `HAVING`, and the verdict matches DuckDB.
    #[test]
    fn a_wrong_count_in_the_main_select_is_an_error() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for sql in [
            "SELECT lower('a', 'b') AS x",
            "SELECT 1 AS n WHERE lower('a', 'b') = 'a'",
        ] {
            assert!(!duckdb_binds(&conn, sql), "DuckDB binds {sql}");
            let registry = FunctionRegistry::default();
            let diags = run(sql, &duckdb(), &registry);
            assert_eq!(
                diags.iter().map(|d| d.code.to_string()).collect::<Vec<_>>(),
                ["E068"],
                "{sql}: {diags:?}"
            );
            assert!(diags[0].is_error(), "{sql}");
        }
    }

    /// A call DuckDB may skip, a CTE body, a derived table or a subquery, is
    /// the warning W068, never the error.
    #[test]
    fn a_wrong_count_outside_the_main_select_is_a_warning() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for sql in [
            "WITH c AS (SELECT lower('a', 'b') AS x) SELECT 1 AS n FROM c",
            "SELECT x FROM (SELECT lower('a', 'b') AS x)",
            "SELECT (SELECT lower('a', 'b')) AS x",
            "SELECT 1 AS n ORDER BY lower('a', 'b')",
        ] {
            assert!(!duckdb_binds(&conn, sql), "DuckDB binds {sql}");
            let registry = FunctionRegistry::default();
            let diags = run(sql, &duckdb(), &registry);
            assert_eq!(
                diags.iter().map(|d| d.code.to_string()).collect::<Vec<_>>(),
                ["W068"],
                "{sql}: {diags:?}"
            );
            assert!(!diags[0].is_error(), "{sql}");
        }
    }

    /// E068 never refuses SQL DuckDB binds. Each call here is a count DuckDB
    /// accepts, or a form Rocky must not count; the verdict is DuckDB's.
    #[test]
    fn e068_never_refuses_a_call_duckdb_binds() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        for sql in [
            "SELECT lower('a') AS x",
            "SELECT concat('a', 'b', 'c', 'd') AS x",
            "SELECT round(1.5) AS x",
            "SELECT round(1.5, 1) AS x",
            "SELECT count(*) AS x FROM (SELECT 1)",
            "SELECT count(DISTINCT 1) AS x FROM (SELECT 1)",
            "SELECT list_transform([1], x -> x + 1) AS x",
            "SELECT struct_pack(a := 1) AS x",
            "SELECT upper('a').lower() AS x",
            "SELECT substring('abc', 1, 2) AS x",
            "SELECT regexp_extract('abc', 'b') AS x",
            "SELECT coalesce(1) AS x",
        ] {
            let binds = duckdb_binds(&conn, sql);
            let codes = arity_codes(sql);
            if binds {
                assert!(codes.is_empty(), "DuckDB binds {sql}; Rocky gave {codes:?}");
            }
        }
    }

    /// Across the whole table, Rocky's E068 on a main-select call with `n`
    /// NULL arguments is exactly DuckDB's "No function matches" refusal of
    /// the same call. Names Rocky never judges (special forms, parser
    /// rewrites) are excluded from the comparison, not asserted.
    #[test]
    fn e068_agrees_with_duckdb_on_every_table_name() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut disagreements = Vec::new();
        let mut checked = 0usize;
        for name in DUCKDB_ARITIES.keys() {
            if SPECIAL_FORMS.contains(name)
                || DUCKDB_ONLY_FORMS.contains(name)
                || DUCKDB_WITHIN_GROUP_FORMS.contains(name)
            {
                continue;
            }
            for n in 0..=3usize {
                let nulls = vec!["NULL"; n].join(", ");
                let sql = format!("SELECT {name}({nulls}) AS x");
                let duck_refuses = conn
                    .prepare(&format!("SELECT {name}({nulls})"))
                    .err()
                    .is_some_and(|e| e.to_string().contains("No function matches"));
                let rocky_refuses = arity_codes(&sql).iter().any(|c| c == "E068");
                checked += 1;
                if duck_refuses != rocky_refuses {
                    disagreements.push(format!(
                        "{sql}: duckdb={duck_refuses} rocky={rocky_refuses}"
                    ));
                }
            }
        }
        assert!(checked > 1000, "checked {checked}");
        assert!(disagreements.is_empty(), "{disagreements:#?}");
    }

    /// A name an extension other than core_functions provides is not tabled,
    /// since the extension can add an overload with another count; a core name
    /// is tabled.
    #[test]
    fn extension_owned_names_are_not_judged_by_count() {
        assert!(!DUCKDB_ARITIES.contains_key("json_extract"));
        assert!(!DUCKDB_ARITIES.contains_key("timezone"));
        assert!(DUCKDB_ARITIES.contains_key("lower"));
        let registry = FunctionRegistry::default();
        // `json_extract` takes two arguments in its extension; no count is judged.
        let sql = "SELECT json_extract('{}', '$.a', 1) AS x";
        assert!(
            arity_codes(sql).is_empty(),
            "{:?}",
            run(sql, &duckdb(), &registry)
        );
    }
}
