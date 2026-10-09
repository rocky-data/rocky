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

use crate::diagnostic::{Diagnostic, E057, SourceSpan, W057};
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
/// dialect's list already holds it.
const DUCKDB_ONLY_FORMS: &[&str] = &["columns", "unpack", "try", "date"];

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
        diagnostics.extend(check_model(model, registry, &dialects));
    }
    diagnostics
}

fn check_model(
    model: &rocky_core::models::Model,
    registry: &FunctionRegistry,
    dialects: &[FunctionDialect],
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
        for (name, line, col) in &visitor.found {
            let lower = name.to_ascii_lowercase();
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

struct Calls<'r> {
    registry: &'r FunctionRegistry,
    /// Unqualified, unquoted calls the project does not declare:
    /// `(name as written, line, column)`.
    found: Vec<(String, usize, usize)>,
}

impl Visitor for Calls<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if let Expr::Function(function) = expr
            && let [ast::ObjectNamePart::Identifier(ident)] = function.name.0.as_slice()
            && ident.quote_style.is_none()
            && !self.registry.declares(&ident.value.to_ascii_lowercase())
        {
            let span = expr.span();
            self.found.push((
                ident.value.clone(),
                span.start.line as usize,
                span.start.column as usize,
            ));
        }
        std::ops::ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
