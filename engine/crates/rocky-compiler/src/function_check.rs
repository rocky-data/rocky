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
//! - On DuckDB, a name the project's `data/seed.sql` creates as a function or
//!   macro (#2691): the seed runs in DuckDB, so the name exists there. The
//!   caller passes the names ([`check_unknown_functions_with`]), from the
//!   DuckDB that ran the seed and from [`seed_function_names_in_text`].
//!
//! # Argument counts (`E068` / `W068`)
//!
//! On DuckDB, a call to a built-in with a number of arguments no overload
//! of it takes is refused at bind time (`lower(name, 'x')`, #2690). The
//! counts come from [`crate::duckdb_signatures`], generated from the bundled
//! engine. The finding is the error `E068` only for a call DuckDB is shown
//! to bind, by the rule `E071` uses ([`crate::set_operation::bound_calls`]):
//! the select list, `WHERE`, `HAVING` or a join's `ON` of a bound query,
//! reached only through forms a macro cannot shadow. Anywhere else, a CTE
//! body, a named `WINDOW`, `ORDER BY`, an argument of another call, it is
//! the warning `W068`. A call whose argument count the SQL does not state
//! ([`plain_argument_count`]) and a link of a dot-call chain are not
//! counted.

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

/// Whether `lower_name` is a call-position form the parser or the warehouse
/// rewrites ([`SPECIAL_FORMS`]) or one only DuckDB's grammar has.
#[cfg(test)]
pub(crate) fn is_special_form(lower_name: &str) -> bool {
    SPECIAL_FORMS.contains(&lower_name) || DUCKDB_ONLY_FORMS.contains(&lower_name)
}

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
) -> Vec<Diagnostic> {
    check_unknown_functions_with(models, registry, &HashSet::new(), target_for, written_for)
}

/// The key a seed name and a call name are compared by: Unicode lower case,
/// so a name that differs only in case matches (#2691). Folding more names
/// together only turns the check off for more of them.
pub fn seed_name_key(name: &str) -> String {
    name.to_lowercase()
}

/// The names (as [`seed_name_key`]) a seed file's text may create as
/// functions or macros: every `CREATE [OR REPLACE] [TEMP | TEMPORARY] MACRO |
/// FUNCTION [IF NOT EXISTS] [schema.]name`, case-insensitive, with Unicode
/// letters in names.
///
/// The text is read three ways and the names are the union: raw, with its
/// comments removed reading only `'...'` strings and `"..."` names
/// ([`Lexing::Plain`]), and with its comments removed reading every DuckDB
/// literal form, `E'...'` escape strings and `$$...$$` / `$tag$...$tag$`
/// strings too ([`Lexing::DuckDb`]). A reading that takes a literal for
/// something else can only lose a name, and another reading finds it: the
/// raw one has no literals to misread, and each comment-free one finds a
/// name with comments between the words (#2691). A name found inside a
/// comment or literal only turns a check off for that name, while a real one
/// the scan dropped would be refused, so the scan errs toward finding more.
/// Each reading takes time linear in the text.
pub fn seed_function_names_in_text(seed_sql: &str) -> HashSet<String> {
    static CREATE: LazyLock<regex::Regex> = LazyLock::new(|| {
        let name = r#"(?:"(?:[^"]|"")+"|[\p{L}_][\p{L}\p{N}_$]*)"#;
        regex::Regex::new(&format!(
            concat!(
                r"(?i)\bcreate\s+(?:or\s+replace\s+)?(?:temp(?:orary)?\s+)?",
                r"(?:macro|function)\s+(?:if\s+not\s+exists\s+)?",
                r"((?:{name}\s*\.\s*)*{name})",
            ),
            name = name,
        ))
        .expect("valid regex")
    });
    let plain = without_comments(seed_sql, Lexing::Plain);
    let duckdb = without_comments(seed_sql, Lexing::DuckDb);
    [seed_sql, plain.as_str(), duckdb.as_str()]
        .into_iter()
        .flat_map(|text| CREATE.captures_iter(text))
        .filter_map(|c| c.get(1))
        .filter_map(|m| last_name_part(m.as_str()))
        .collect()
}

/// Which literal forms [`without_comments`] reads, so that a comment marker
/// inside a literal is not taken for a comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Lexing {
    /// `'...'` strings and `"..."` names, with `''` and `""` as escapes.
    Plain,
    /// Also DuckDB's `E'...'` strings, where a backslash escapes the next
    /// character, and dollar-quoted `$$...$$` and `$tag$...$tag$` strings.
    DuckDb,
}

/// Whether `c` can be part of an unquoted DuckDB name.
fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// `sql` with each `--` and (nested) `/* */` comment replaced by white space.
/// The literals `lexing` names are kept whole. One pass, so linear in the
/// text.
fn without_comments(sql: &str, lexing: Lexing) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let after_name = i > 0 && is_name_char(chars[i - 1]);
        match c {
            'e' | 'E'
                if lexing == Lexing::DuckDb && !after_name && chars.get(i + 1) == Some(&'\'') =>
            {
                // `E'...'`: a backslash escapes the next character, and `''`
                // is a quote.
                out.push(c);
                out.push('\'');
                i += 2;
                while i < chars.len() {
                    let d = chars[i];
                    out.push(d);
                    i += 1;
                    if d == '\\' {
                        if let Some(&next) = chars.get(i) {
                            out.push(next);
                            i += 1;
                        }
                    } else if d == '\'' {
                        if chars.get(i) == Some(&'\'') {
                            out.push('\'');
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '$' if lexing == Lexing::DuckDb && !after_name => {
                match dollar_tag(&chars, i) {
                    Some(tag_len) => {
                        // `$tag$ ... $tag$`: nothing inside is special.
                        let tag = &chars[i..i + tag_len];
                        out.extend(tag);
                        i += tag_len;
                        let end = (i..chars.len()).find(|&j| chars[j..].starts_with(tag));
                        match end {
                            Some(j) => {
                                out.extend(&chars[i..j + tag_len]);
                                i = j + tag_len;
                            }
                            None => {
                                out.extend(&chars[i..]);
                                i = chars.len();
                            }
                        }
                    }
                    None => {
                        out.push(c);
                        i += 1;
                    }
                }
            }
            '\'' | '"' => {
                out.push(c);
                i += 1;
                while i < chars.len() {
                    let d = chars[i];
                    out.push(d);
                    i += 1;
                    if d == c {
                        if chars.get(i) == Some(&c) {
                            out.push(c);
                            i += 1;
                        } else {
                            break;
                        }
                    }
                }
            }
            '-' if chars.get(i + 1) == Some(&'-') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                out.push('\n');
            }
            '/' if chars.get(i + 1) == Some(&'*') => {
                i += 2;
                let mut depth = 1usize;
                while depth > 0 && i < chars.len() {
                    if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                        depth -= 1;
                        i += 2;
                    } else if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                        depth += 1;
                        i += 2;
                    } else {
                        i += 1;
                    }
                }
                out.push(' ');
            }
            _ => {
                out.push(c);
                i += 1;
            }
        }
    }
    out
}

/// The length of the dollar-quote opener at `chars[at]` (`$$` or `$tag$`,
/// where `tag` is a name that does not start with a digit), if there is one.
fn dollar_tag(chars: &[char], at: usize) -> Option<usize> {
    let mut j = at + 1;
    while let Some(&c) = chars.get(j) {
        if c == '$' {
            return Some(j - at + 1);
        }
        let first = j == at + 1;
        if !(c.is_alphabetic() || c == '_' || (!first && c.is_numeric())) {
            return None;
        }
        j += 1;
    }
    None
}

/// The last part of a possibly qualified, possibly quoted name, as
/// [`seed_name_key`], unquoted.
fn last_name_part(qualified: &str) -> Option<String> {
    static LAST: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r#"(?i)(?:"((?:[^"]|"")+)"|([\p{L}_][\p{L}\p{N}_$]*))\s*$"#)
            .expect("valid regex")
    });
    let captures = LAST.captures(qualified)?;
    let name = match (captures.get(1), captures.get(2)) {
        (Some(quoted), _) => quoted.as_str().replace("\"\"", "\""),
        (None, Some(bare)) => bare.as_str().to_string(),
        (None, None) => return None,
    };
    Some(seed_name_key(&name))
}

/// [`check_unknown_functions`], skipping DuckDB's verdict (`E057`, `E068`,
/// `W068`) on calls to `seed_names`: the names, as [`seed_name_key`], of the
/// functions and macros the project's seed file creates. The seed runs only
/// in DuckDB, so another warehouse still judges the call.
pub fn check_unknown_functions_with(
    models: &[rocky_core::models::Model],
    registry: &FunctionRegistry,
    seed_names: &HashSet<String>,
    target_for: &dyn Fn(&str) -> OperandTarget,
    written_for: Option<OperandDialect>,
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
        diagnostics.extend(check_model(model, registry, seed_names, &dialects));
    }
    diagnostics
}

fn check_model(
    model: &rocky_core::models::Model,
    registry: &FunctionRegistry,
    seed_names: &HashSet<String>,
    dialects: &[FunctionDialect],
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
    else {
        return Vec::new();
    };
    let [Statement::Query(query)] = statements.as_slice() else {
        return Vec::new();
    };
    // Pointers into this parse: the visitor below walks the same tree. An
    // ephemeral model's SQL only reaches DuckDB as a CTE inside each model
    // that reads it, so none of its calls is bound: a CTE nothing reads is
    // never bound, and every CTE body is a warning place.
    // Same test as `crate::ephemeral::apply_ephemerals`, which inlines them.
    let bound = if matches!(
        model.config.strategy,
        rocky_core::models::StrategyConfig::Ephemeral
    ) {
        HashSet::new()
    } else {
        crate::set_operation::bound_calls(query)
    };
    let mut visitor = Calls {
        registry,
        bound,
        chained: HashSet::new(),
        found: Vec::new(),
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
    let at = |line: usize, col: usize| {
        if precise_spans && line > 0 {
            (line, col)
        } else {
            (1, 1)
        }
    };
    for &dialect in dialects {
        let mut seen = HashSet::new();
        for call in &visitor.found {
            let name = &call.name;
            let lower = name.to_ascii_lowercase();
            if dialect == FunctionDialect::DuckDb && seed_names.contains(&seed_name_key(name)) {
                continue;
            }
            if dialect == FunctionDialect::DuckDb
                && call.within_group
                && DUCKDB_WITHIN_GROUP_FORMS.contains(&lower.as_str())
            {
                continue;
            }
            if dialect.knows(&lower) {
                if dialect == FunctionDialect::DuckDb
                    && let Some(count) = call.count
                    && crate::duckdb_signatures::refuses_count(&lower, count)
                {
                    let (line, col) = at(call.line, call.col);
                    diagnostics.push(count_diagnostic(
                        model, name, &lower, count, call.bound, &file, line, col,
                    ));
                }
                continue;
            }
            if !seen.insert(lower) {
                continue;
            }
            let (line, col) = at(call.line, call.col);
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
    diagnostics
}

/// E068 / W068: DuckDB has no overload of `name` that takes `count`
/// arguments. `bound` says whether DuckDB is shown to bind the call
/// ([`crate::set_operation::bound_calls`]): an error then, else a warning.
#[allow(clippy::too_many_arguments)]
fn count_diagnostic(
    model: &rocky_core::models::Model,
    name: &str,
    lower: &str,
    count: usize,
    bound: bool,
    file: &str,
    line: usize,
    col: usize,
) -> Diagnostic {
    let (accepted, bounded) = crate::duckdb_signatures::accepted_counts(lower);
    let accepted: Vec<String> = accepted.iter().map(ToString::to_string).collect();
    let more = if bounded { "" } else { ", or more" };
    let takes = if accepted.is_empty() {
        String::new()
    } else {
        format!(" It takes {} argument(s){more}.", accepted.join(", "))
    };
    let plural = if count == 1 { "" } else { "s" };
    let message = format!("DuckDB has no `{name}` that takes {count} argument{plural}");
    let diagnostic = if bound {
        Diagnostic::error(
            E068,
            &model.config.name,
            format!("{message}, so DuckDB refuses the query before it reads any data.{takes}"),
        )
    } else {
        Diagnostic::warning(
            W068,
            &model.config.name,
            format!(
                "{message}. The call is in a place DuckDB may skip, such as a CTE, a named \
                 window nothing uses, or an argument a macro drops, so this is a warning.{takes}"
            ),
        )
    };
    diagnostic
        .with_suggestion(format!(
            "pass the number of arguments `{name}` takes. If `{name}` is a macro created \
             outside Rocky, call it schema-qualified (`main.{name}(...)`), which Rocky does not \
             check"
        ))
        .with_span(SourceSpan {
            file: file.to_string(),
            line,
            col,
        })
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

/// An unqualified, unquoted call the project does not declare.
struct Call {
    /// The name as written.
    name: String,
    line: usize,
    col: usize,
    /// The call has `WITHIN GROUP (...)`.
    within_group: bool,
    /// The argument count, when the SQL states it ([`plain_argument_count`])
    /// and the call is not a link of a dot-call chain.
    count: Option<usize>,
    /// DuckDB binds the call whenever it binds the model's query
    /// ([`crate::set_operation::bound_calls`]).
    bound: bool,
}

struct Calls<'r> {
    registry: &'r FunctionRegistry,
    /// The calls DuckDB is shown to bind.
    bound: HashSet<*const ast::Function>,
    /// Calls that are a link of a dot-call chain (`(x).lower()`).
    chained: HashSet<*const ast::Function>,
    found: Vec<Call>,
}

/// The argument count of a call whose arguments are a plain list of
/// expressions, the shape the DuckDB count data was probed with. `None` for
/// a call with no parentheses, a subquery argument, a named or `*`
/// argument, a lambda, `DISTINCT` / `ALL`, a clause inside the parentheses
/// (`ORDER BY`, `SEPARATOR`, ...), `WITHIN GROUP`, `FILTER` or `OVER`, an
/// `name := value` argument, and an argument that expands to several
/// (`UNPACK(COLUMNS(...))`).
pub(crate) fn plain_argument_count(function: &ast::Function) -> Option<usize> {
    let ast::FunctionArguments::List(list) = &function.args else {
        return None;
    };
    if list.duplicate_treatment.is_some()
        || !list.clauses.is_empty()
        || !function.within_group.is_empty()
        || function.filter.is_some()
        || function.over.is_some()
        || !matches!(function.parameters, ast::FunctionArguments::None)
    {
        return None;
    }
    // `name := value` parses as an expression, not a named argument, and an
    // `UNPACK(COLUMNS(...))` argument expands to several.
    list.args
        .iter()
        .all(|arg| {
            matches!(
                arg,
                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    if !matches!(
                        e,
                        Expr::Lambda(_)
                            | Expr::BinaryOp {
                                op: ast::BinaryOperator::Assignment,
                                ..
                            }
                    ) && !crate::set_operation::expands_to_columns(e)
            )
        })
        .then_some(list.args.len())
}

impl Visitor for Calls<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        // `x.lower()` with `x` not a bare name parses as a field access whose
        // chain holds the call. Its receiver is the call's first argument,
        // which the call's own list does not hold, so it is not counted.
        if let Expr::CompoundFieldAccess { access_chain, .. } = expr {
            for access in access_chain {
                if let ast::AccessExpr::Dot(Expr::Function(function)) = access {
                    self.chained.insert(std::ptr::from_ref(function));
                }
            }
        }
        if let Expr::Function(function) = expr
            && let [ast::ObjectNamePart::Identifier(ident)] = function.name.0.as_slice()
            && ident.quote_style.is_none()
            && !self.registry.declares(&ident.value.to_ascii_lowercase())
        {
            let span = expr.span();
            let this = std::ptr::from_ref(function);
            self.found.push(Call {
                name: ident.value.clone(),
                line: span.start.line as usize,
                col: span.start.column as usize,
                within_group: !function.within_group.is_empty(),
                count: (!self.chained.contains(&this))
                    .then(|| plain_argument_count(function))
                    .flatten(),
                bound: self.bound.contains(&this),
            });
        }
        std::ops::ControlFlow::Continue(())
    }
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
    /// The list follows the 2.0.1 autoload table, both ways: names 2.0.1 added
    /// are accepted and names 2.0.1 removed are refused.
    #[test]
    fn the_duckdb_list_tracks_the_autoload_table_of_2_0_1() {
        for name in [
            "iceberg_partitions",
            "s3_buckets",
            "parquet_page_index",
            "st_asgrid",
            "__aws__cloudformation__stack__create",
        ] {
            assert!(FunctionDialect::DuckDb.knows(name), "{name} must be known");
        }
        for name in [
            "__aws__cloudformation__quack_on_ec2__create",
            "__aws__cloudformation__quack_on_ec2__destroy",
            "__aws__cloudformation__quack_on_ec2__list",
            "__aws__cloudformation__quack_on_ec2__status",
            "odbc_bind_params",
            "odbc_create_params",
        ] {
            assert!(
                !FunctionDialect::DuckDb.knows(name),
                "{name} must be refused"
            );
        }
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
        check_unknown_functions(&[model("m", sql)], registry, &|_| target.clone(), None)
    }

    fn count_codes(sql: &str, target: &OperandTarget) -> Vec<String> {
        run(sql, target, &FunctionRegistry::default())
            .into_iter()
            .map(|d| d.code.to_string())
            .collect()
    }

    /// The bundled DuckDB, with the tables the E068 tests read.
    fn count_db() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (name VARCHAR, s VARCHAR, a VARCHAR, b VARCHAR, status VARCHAR, \
             price DOUBLE, amount DOUBLE, qty INTEGER, created_at TIMESTAMP, tags VARCHAR[], \
             payload STRUCT(k INTEGER)); \
             CREATE TABLE u (s VARCHAR, a VARCHAR);",
        )
        .unwrap();
        conn
    }

    /// DuckDB refuses `sql` because no overload of a call matches.
    fn duckdb_refuses_the_count(conn: &duckdb::Connection, sql: &str) {
        let err = conn.prepare(sql).err().map(|e| e.to_string());
        assert!(
            err.as_deref().is_some_and(
                |e| e.contains("No function matches the given name and argument types")
            ),
            "DuckDB does not refuse {sql} for an argument count: {err:?}"
        );
    }

    fn duckdb_binds(conn: &duckdb::Connection, sql: &str) {
        conn.prepare(sql)
            .unwrap_or_else(|e| panic!("DuckDB refuses {sql}: {e}"));
    }

    /// E068 (#2690): a DuckDB built-in called with a number of arguments it
    /// does not take, in a place DuckDB binds. DuckDB refuses each one.
    #[test]
    fn a_wrong_argument_count_is_refused_on_duckdb() {
        let duckdb = duckdb();
        let conn = count_db();
        for sql in [
            "select lower(name, 'x') from t",
            "select lower() from t",
            "select round(price, 1, 2) from t",
            "select md5(a, b) from t",
            "select lower(name, 'x') = 'a' as l from t",
            "select a from t where lower(a, b) = 'x'",
            "select a from t where not (lower(a, b) = 'x')",
            "select a from t group by a having lower(a, 'x') = 'x'",
            "select t.a from t join u on lower(t.a, u.a) = u.s",
            "select * from (select lower(s, 'x') as l from t) d",
            "select a from t where a in (select lower(s, 'x') from u)",
            "select a from t union all select lower(s, 'x') from u",
            "select cast(lower(s, 'x') as varchar) from t",
            "select case when s = 'a' then lower(s, 'x') end from t",
            "select a from t where lower(s, 'x') between 'a' and 'b'",
            "select a from t where lower(s, 'x') is distinct from 'a'",
            "select a from t where lower(s, 'x') is not distinct from 'a'",
            "select a from t where a in ('a', lower(s, 'x'))",
            "select a from t where a = 'x' or lower(s, 'x') = 'b'",
            "select a from t where exists (select 1 from u where lower(u.s, 'x') = 'a')",
            // Bound even where it never runs.
            "select case when false then (select lower(s, 'x') from t) else 'b' end from t",
            "select 1 from t where false and exists (select lower(s, 'x') from t)",
            "select * from (select lower(s, 'x') from t) d where false",
            "select case when false then lower(s, 'x') end from t",
        ] {
            assert_eq!(count_codes(sql, &duckdb), ["E068"], "{sql}");
            duckdb_refuses_the_count(&conn, sql);
        }
        let diags = run(
            "SELECT id,\n    lower(name, 'x') AS n\nFROM t",
            &duckdb,
            &FunctionRegistry::default(),
        );
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(diags[0].is_error());
        assert!(
            diags[0].message.contains("`lower` that takes 2 arguments")
                && diags[0].message.contains("It takes 1 argument(s)."),
            "{}",
            diags[0].message
        );
        let span = diags[0].span.as_ref().unwrap();
        assert_eq!((span.line, span.col), (2, 5), "points at the call");
    }

    /// Calls with a count DuckDB takes are silent, and DuckDB binds them.
    #[test]
    fn a_count_duckdb_takes_is_not_reported() {
        let duckdb = duckdb();
        let conn = count_db();
        for sql in [
            "select lower(name) from t",
            "select round(price) from t",
            "select round(price, 2) from t",
            "select concat(a, b, s, name, status, a, b) from t",
            "select count(*) from t",
            "select string_agg(a, ',' order by a) from t",
            "select coalesce(a, b, s) from t",
            "select upper(lower(a)) from t",
            // Name-bound calls and expanded arguments are not counted.
            "select struct_pack(a := qty) from t",
            "select struct_pack(qty, status) from t",
            "select struct_insert(payload, extra := qty) from t",
            "select union_value(k := qty) from t",
            "select lower(columns('^(name|status)$')) from t",
        ] {
            assert!(count_codes(sql, &duckdb).is_empty(), "{sql}");
            duckdb_binds(&conn, sql);
        }
        // Not checked at all: a schema-qualified or quoted name, and a call
        // whose count the SQL does not state.
        for sql in [
            "select main.lower(a, b) from t",
            "select \"lower\"(a, b) from t",
            "select count(distinct a, b) from t",
            "select lower(a, b) filter (where a = 'x') from t",
            "select date_diff(unpack(columns(['a', 'b', 's']))) from t",
        ] {
            assert!(count_codes(sql, &duckdb).is_empty(), "{sql}");
        }
        // Only DuckDB's counts are known.
        let sql = "select lower(name, 'x') from t";
        assert!(count_codes(sql, &Some(OperandDialect::Snowflake).into()).is_empty());
        assert!(count_codes(sql, &OperandTarget::Unconfigured).is_empty());
        // A model that also runs on DuckDB is refused.
        let both = OperandTarget::Targets {
            dialects: vec![OperandDialect::Snowflake, OperandDialect::DuckDb],
            unruled: Vec::new(),
        };
        assert_eq!(count_codes(sql, &both), ["E068"]);
    }

    /// A wrong count in a place DuckDB may skip is the warning `W068`, by
    /// the rule `E071` uses (#2690): DuckDB binds every query here, so an
    /// error would refuse valid SQL. The built-in macro `pg_table_is_visible`
    /// drops its argument; a named window nothing uses and a CTE nothing
    /// reads are never bound.
    #[test]
    fn a_wrong_count_duckdb_may_skip_is_a_warning() {
        let duckdb = duckdb();
        let conn = count_db();
        for sql in [
            "select pg_table_is_visible(lower(s, 'x')) from t",
            "select pg_has_role(lower(s, 'x'), 'a', 'b') from t",
            "select 1 from t window w as (order by lower(s, 'x'))",
            "with x as (select lower(s, 'x') as l from t) select qty from t",
            "with x as (select lower(s, 'x') as l from t), \
             y as (select l from x) select qty from t",
            "select * from (with x as (select lower(s, 'x') l from t) select 1) d",
            "with x as (select lower(s, 'x') as l from t) \
             select * from (with x as (select 1 a) select * from x) d",
        ] {
            assert_eq!(count_codes(sql, &duckdb), ["W068"], "{sql}");
            duckdb_binds(&conn, sql);
        }
        // Also a warning where DuckDB does refuse: Rocky does not prove the
        // place is bound, so it does not fail the compile.
        for sql in [
            "with x as (select lower(s, 'x') as l from t) select l from x",
            "select upper(lower(s, 'x')) from t",
            "select s from t order by lower(s, 'x')",
            "select s from t group by lower(s, 'x')",
            "select s from t qualify lower(s, 'x') = 'a'",
            "select s || lower(s, 'x') from t",
            "select row_number() over (order by lower(s, 'x')) from t",
            "select list_transform(tags, x -> lower(x, 'y')) from t",
            "select lower(status, 'x').upper() from t",
        ] {
            assert_eq!(count_codes(sql, &duckdb), ["W068"], "{sql}");
            duckdb_refuses_the_count(&conn, sql);
        }
        let diags = run(
            "with x as (select lower(s, 'x') as l from t) select 1",
            &duckdb,
            &FunctionRegistry::default(),
        );
        assert!(!diags[0].is_error(), "{diags:?}");
    }

    /// An ephemeral model's SQL reaches DuckDB only as a CTE inside each
    /// model that reads it, so a wrong count in it is `W068`: DuckDB runs a
    /// consumer that never reads the CTE. A materialized model with the
    /// same SQL gets `E068`.
    #[test]
    fn a_wrong_count_in_an_ephemeral_model_is_a_warning() {
        let sql = "select lower(s, 'x') as l from t";
        let mut ephemeral = model("eph", sql);
        ephemeral.config.strategy = StrategyConfig::Ephemeral;
        let duckdb = duckdb();
        let codes = |m: &Model| -> Vec<String> {
            check_unknown_functions(
                std::slice::from_ref(m),
                &FunctionRegistry::default(),
                &|_| duckdb.clone(),
                None,
            )
            .into_iter()
            .map(|d| d.code.to_string())
            .collect()
        };
        assert_eq!(codes(&ephemeral), ["W068"]);
        assert_eq!(codes(&model("table_model", sql)), ["E068"]);
        // The inlined form a consumer sends, which does not read the CTE.
        let conn = count_db();
        duckdb_binds(&conn, &format!("with eph as ({sql}) select qty from t"));
        duckdb_refuses_the_count(&conn, sql);
    }

    /// In a dot-call chain the receiver is the first argument, which the
    /// call's own list does not hold, so a link is not counted (#2690).
    #[test]
    fn dot_call_chains_are_not_counted() {
        let duckdb = duckdb();
        let conn = count_db();
        for sql in [
            "select status.upper().lower() from t",
            "select (status).lower() from t",
            "select upper(status).lower() from t",
            "select amount.round(1).abs() from t",
            "select (amount + 1).round() from t",
            "select created_at.strftime('%Y').upper() from t",
            "select 'a,b'.split(',').len() from t",
            "select (created_at).year() from t",
            "select (status || 'x').md5() from t",
            "select ('abc').upper() from t",
            "select tags[1].upper() from t",
        ] {
            assert!(count_codes(sql, &duckdb).is_empty(), "{sql}");
            duckdb_binds(&conn, sql);
        }
    }

    /// Macros and functions the project's seed file creates are the
    /// project's: on DuckDB a call to one is judged neither by `E057` nor by
    /// `E068` (#2691). DuckDB, with the seed's macros, binds each call.
    #[test]
    fn a_seed_macro_is_not_judged_on_duckdb() {
        let names: HashSet<String> = ["lower", "my_fn", "café"].map(String::from).into();
        let registry = FunctionRegistry::default();
        let judged = |sql: &str, dialect: OperandDialect| {
            let target = OperandTarget::from(Some(dialect));
            check_unknown_functions_with(
                &[model("m", sql)],
                &registry,
                &names,
                &|_| target.clone(),
                None,
            )
        };
        let conn = count_db();
        conn.execute_batch(
            "CREATE MACRO lower(a, b) AS a || b; CREATE MACRO my_fn(x) AS upper(x); \
             CREATE MACRO café(x) AS x;",
        )
        .unwrap();
        for sql in [
            "select lower(a, b) from t",
            "select my_fn(status) as s from t",
            "select MY_FN(status) as s from t",
            "select café(status) as s from t",
        ] {
            assert!(judged(sql, OperandDialect::DuckDb).is_empty(), "{sql}");
            duckdb_binds(&conn, sql);
        }
        // DuckDB folds only ASCII case, so it refuses `CAFÉ`. Rocky folds
        // Unicode case and skips it: matching more names only turns the
        // check off for more calls.
        assert!(judged("select CAFÉ(status) as s from t", OperandDialect::DuckDb).is_empty());
        // A name the seed does not create is still judged.
        let diags = judged("select upper(a, b) from t", OperandDialect::DuckDb);
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(&*diags[0].code, "E068");
        let diags = judged("select other_fn(a) from t", OperandDialect::DuckDb);
        assert_eq!(&*diags[0].code, "E057", "{diags:?}");
        // The seed runs only in DuckDB: another warehouse still judges it.
        let diags = judged("select my_fn(a) from t", OperandDialect::Snowflake);
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(&*diags[0].code, "W057");
    }

    /// The names DuckDB itself reports for a seed it ran.
    fn duckdb_seed_names(seed: &str) -> HashSet<String> {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(seed)
            .unwrap_or_else(|e| panic!("DuckDB refuses the seed {seed}: {e}"));
        let mut stmt = conn
            .prepare("SELECT function_name FROM duckdb_functions() WHERE NOT internal")
            .unwrap();
        stmt.query_map([], |r| r.get::<_, String>(0))
            .unwrap()
            .map(|n| seed_name_key(&n.unwrap()))
            .collect()
    }

    /// The text scan finds every name DuckDB reports for a seed it runs,
    /// also after a literal that holds a comment marker or a quote and with
    /// comments between the words (#2691): `$$...$$`, `$tag$...$tag$`,
    /// `E'...'` with a backslash escape, and plain strings.
    #[test]
    fn the_seed_text_scan_finds_every_name_duckdb_creates() {
        for seed in [
            // The #2691 gaps: a literal the plain reading misreads, then
            // comments between the keywords.
            "SELECT $$it's$$ AS a; CREATE /* c */ MACRO f1(x) AS x;",
            "SELECT $$ /* $$ AS a; CREATE /* c */ MACRO f1(x) AS x;",
            "SELECT $q$ it's -- $q$ AS a; CREATE -- c\n MACRO f2(x) AS x;",
            "SELECT E'it\\'s' AS a; CREATE /* c */ OR REPLACE MACRO f3(x) AS x;",
            "SELECT e'\\' /*' AS a; CREATE -- c\n TEMP /* d */ MACRO f4(x) AS x;",
            "SELECT 'a--b' AS p, '/*' AS q; CREATE /* c */ FUNCTION f5(x) AS x;",
            "SELECT \"we\"\"ird\" FROM (SELECT 1 AS \"we\"\"ird\"); \
             CREATE /* c */ MACRO f6(x) AS x;",
            // Comment forms DuckDB reads.
            "CREATE /* a /* b */ c */ MACRO g(x) AS x;",
            "CREATE OR REPLACE -- note\n MACRO\n\tabs(a, b) AS a;",
            "CREATE MACRO lower -- see docs.md\n(a, b) AS a;",
            "CREATE MACRO upper /* a /* b */ . c */ (a, b) AS a;",
            // Names.
            "CREATE MACRO café(x) AS x; CREATE MACRO My_Macro(x) AS x;",
            "CREATE SCHEMA util; CREATE FUNCTION util.\"Shout\"(s) AS upper(s); \
             CREATE TEMPORARY FUNCTION shout_temp(s) AS upper(s);",
            "CREATE MACRO IF NOT EXISTS keep(x) AS x; CREATE FUNCTION \"we\"\"ird\"(x) AS x;",
            "CREATE MACRO rows_of() AS TABLE SELECT 1 AS one;",
            "CREATE MACRO a$b(x) AS x;",
        ] {
            let from_duckdb = duckdb_seed_names(seed);
            assert!(!from_duckdb.is_empty(), "{seed}");
            let scanned = seed_function_names_in_text(seed);
            let missed: Vec<&String> = from_duckdb.difference(&scanned).collect();
            assert!(missed.is_empty(), "{seed}: missed {missed:?}");
        }
    }

    /// A name in a comment or a literal is found too: that only turns the
    /// check off for that name. An unclosed comment or literal does not make
    /// the scan slow.
    #[test]
    fn the_seed_text_scan_errs_toward_more_names_in_linear_time() {
        let names = seed_function_names_in_text(
            "-- CREATE MACRO commented(x) AS x\n\
             SELECT 'CREATE MACRO quoted(x) AS x';",
        );
        assert!(
            names.contains("commented") && names.contains("quoted"),
            "{names:?}"
        );
        for seed in [
            "-- CREATE MACRO a /* x\n".repeat(40_000),
            "SELECT $a$ CREATE MACRO a(x) AS x; $".repeat(40_000),
            "SELECT E'\\".repeat(40_000),
            "/* /* /* CREATE MACRO a(x) AS x;".repeat(40_000),
        ] {
            let start = std::time::Instant::now();
            let _ = seed_function_names_in_text(&seed);
            assert!(
                start.elapsed() < std::time::Duration::from_secs(3),
                "{:?}",
                start.elapsed()
            );
        }
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
            check_unknown_functions(&models, &registry, &|_| on_duckdb.clone(), written_for)
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
}
