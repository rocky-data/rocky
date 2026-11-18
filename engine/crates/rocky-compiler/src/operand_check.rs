//! Dialect-aware operand checks for aggregates and comparisons (E042 / W042,
//! E043 / W043).
//!
//! Type inference in [`crate::typecheck`] answers "what type does this
//! expression produce?" and never asks whether the warehouse accepts the
//! operands. Two consequences used to compile clean and fail at run time:
//!
//! - `SUM(customer_name)` over a `VARCHAR` column. DuckDB has no `sum(VARCHAR)`
//!   overload and refuses the query at bind time.
//! - `ON o.customer_id = c.customer_name` (`BIGINT` = `VARCHAR`). DuckDB casts
//!   the text side to a number for every row and fails on the first name that
//!   does not parse.
//!
//! This pass walks every `SELECT` in a model with the same relation scope the
//! type checker builds and judges two things against a small per-dialect
//! table:
//!
//! - **Aggregate arguments** ([`E042`] / [`W042`]): numeric aggregates
//!   (`SUM`, `AVG`, the `STDDEV` / `VARIANCE` family), boolean aggregates
//!   (`BOOL_AND` / `BOOL_OR` and their dialect spellings) over text, and
//!   string aggregates (`STRING_AGG` / `LISTAGG`) over numbers.
//! - **Comparison operands** ([`E043`] / [`W043`]): `=`, `<>`, `<`, `>`,
//!   `<=`, `>=`, `<=>`, `IN (...)`, `BETWEEN`, and simple `CASE x WHEN ...`,
//!   wherever they appear (projection, `WHERE`, `HAVING`, `QUALIFY`, join
//!   `ON`).
//!
//! # Severity rule
//!
//! - **Error** when the dialect has no overload / no comparison for the type
//!   pair, so the statement can never run.
//! - **Warning** when the dialect casts implicitly at run time and success
//!   depends on the data. The warning names the risk and the fix. It escalates
//!   to an error with `rocky compile --deny-warnings W042,W043`.
//! - **Clean** otherwise. Every operand whose type Rocky does not know is
//!   clean: `Unknown` is never evidence of a defect.
//!
//! With no resolvable dialect the verdict is the *least* severe across all
//! supported dialects, so an unconfigured project only ever gets warnings.
//!
//! # Deliberately clean (no false refusals)
//!
//! - A string literal that parses as a number, compared with a number
//!   (`10::BIGINT = '10'::VARCHAR`), and any string literal compared with a
//!   date or timestamp (`order_date >= '2024-01-01'`).
//! - `DATE` vs `TIMESTAMP`, numeric vs numeric, and every pair this table
//!   does not name (for example boolean vs anything).
//! - Date arithmetic: `order_date >= CURRENT_DATE - 5` and `last - first > 5`
//!   type their arithmetic side `Unknown`, never a date or a number.
//! - `MIN` / `MAX` / `COUNT` / `ARRAY_AGG` over any type, `COUNT(*)`,
//!   `COUNT(DISTINCT x)`, and an aggregate whose argument is a literal.
//!
//! # Dialect sources
//!
//! Implicit-cast rules per dialect, as documented:
//!
//! - DuckDB — <https://duckdb.org/docs/stable/sql/data_types/typecasting>
//!   (`VARCHAR` is implicitly cast only from literals; functions bind by
//!   overload). Comparison of `BIGINT` with a `VARCHAR` column casts the text
//!   side per row (verified against DuckDB 1.5: `Conversion Error: Could not
//!   convert string 'bob' to INT64`). `sum(VARCHAR)`, `stddev(VARCHAR)`,
//!   `bool_and(VARCHAR)` fail with `Binder Error: No function matches`.
//!   A `DATE` or `TIMESTAMP` against a number has no comparison (verified
//!   against DuckDB 1.5: `order_date > 5` fails with `Binder Error: Cannot
//!   compare values of type DATE and type INTEGER_LITERAL`; `order_date = 5`
//!   binds but fails on the first row with `Conversion Error: Unimplemented
//!   type for cast (INTEGER -> DATE)`).
//! - Snowflake — <https://docs.snowflake.com/en/sql-reference/data-type-conversion>
//!   (`VARCHAR` is implicitly coerced to `NUMBER`, `DATE`, `TIMESTAMP` and
//!   `BOOLEAN`; failure is a run-time conversion error).
//! - Databricks / Spark SQL —
//!   <https://docs.databricks.com/en/sql/language-manual/sql-ref-datatype-rules.html>
//!   (implicit crosscasting of `STRING` to the expected type in function
//!   invocation and comparison; ANSI mode raises on a malformed value).
//! - BigQuery —
//!   <https://cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules>
//!   (no implicit `STRING` → `INT64` / `FLOAT64` / `NUMERIC` coercion outside
//!   literals and parameters; `SUM(STRING)` and `INT64 = STRING` are
//!   signature errors; `DATE` vs `INT64` has no comparison signature).
//! - Trino — <https://trino.io/docs/current/functions/conversion.html>
//!   ("Trino will not convert between character and numeric types"; there
//!   is no implicit cast between a number and `DATE` / `TIMESTAMP` either).
//! - SQL Server —
//!   <https://learn.microsoft.com/sql/t-sql/data-types/data-type-conversion-database-engine>
//!   (character types convert implicitly to the numeric and date/time types;
//!   by data type precedence the character side is converted, per row, and a
//!   malformed value fails with "Conversion failed") and
//!   <https://learn.microsoft.com/sql/t-sql/functions/sum-transact-sql>
//!   (`SUM` / `AVG` take "the exact numeric or approximate numeric data type
//!   category" only: `SUM(varchar)` is "Operand data type varchar is invalid
//!   for sum operator"). `STRING_AGG` converts non-string input to
//!   `NVARCHAR`, so numbers are clean.
//! - PostgreSQL — <https://www.postgresql.org/docs/current/typeconv-overview.html>
//!   (no implicit cast from `text` to a numeric or date/time type; only an
//!   untyped literal is resolved to the other side's type) and
//!   <https://www.postgresql.org/docs/current/functions-aggregate.html>
//!   (`sum` / `avg` / `stddev` / `variance` take numeric and interval inputs,
//!   `bool_and` / `bool_or` take `boolean`, `string_agg` takes `text` or
//!   `bytea`). Verified against PostgreSQL 16: `sum(text)`, `bool_and(text)`
//!   and `string_agg(integer, unknown)` fail with "function … does not
//!   exist"; `integer = text`, `date = text` and `date > integer` fail with
//!   "operator does not exist".
//! - Amazon Redshift —
//!   <https://docs.aws.amazon.com/redshift/latest/dg/r_SUM.html>,
//!   <https://docs.aws.amazon.com/redshift/latest/dg/r_AVG.html>,
//!   <https://docs.aws.amazon.com/redshift/latest/dg/r_STDDEV_functions.html>,
//!   <https://docs.aws.amazon.com/redshift/latest/dg/r_VARIANCE_functions.html>
//!   (numeric argument types only) and
//!   <https://docs.aws.amazon.com/redshift/latest/dg/r_BOOL_AND.html> (Boolean or
//!   integer only), so text into these aggregates is refused. Comparisons
//!   follow <https://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html>
//!   ("Type compatibility and conversion"): a character string is converted
//!   implicitly to a numeric or date/time value when it is a valid literal,
//!   so a text column compared with a number or a date is value-dependent,
//!   not refused. Redshift has no `STRING_AGG`; its `LISTAGG` rule is not
//!   modelled.
//! - ClickHouse has no table here yet. A model that targets only ClickHouse
//!   gets the least severe verdict across the dialects above, and the
//!   message says ClickHouse has no rules, not that no dialect is configured.

use std::collections::HashSet;

use indexmap::IndexMap;
use sqlparser::ast::{self, Expr, SetExpr, Spanned, Statement, TableFactor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E042, E043, E069, E070, SourceSpan, W042, W043};
use crate::duckdb_signatures::{self, ArgClass};
use crate::semantic::SemanticGraph;
use crate::typecheck::{
    InferEnv, RelationRef, TypeScope, has_exact_type, infer_expr_type, infer_query_types,
    rename_relation_columns, select_type_scope,
};
use crate::types::{RockyType, TypedColumn};

/// The warehouse dialects this pass has an implicit-cast table for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperandDialect {
    DuckDb,
    Snowflake,
    Databricks,
    BigQuery,
    Trino,
    SqlServer,
    Postgres,
    Redshift,
}

impl OperandDialect {
    pub(crate) const ALL: [OperandDialect; 8] = [
        OperandDialect::DuckDb,
        OperandDialect::Snowflake,
        OperandDialect::Databricks,
        OperandDialect::BigQuery,
        OperandDialect::Trino,
        OperandDialect::SqlServer,
        OperandDialect::Postgres,
        OperandDialect::Redshift,
    ];

    /// Map a `rocky.toml` adapter `type` to a dialect. Returns `None` for
    /// source-only adapters (`fivetran`, `airbyte`, …) and unknown types.
    pub fn from_adapter_type(adapter_type: &str) -> Option<Self> {
        match adapter_type.to_ascii_lowercase().as_str() {
            "duckdb" => Some(Self::DuckDb),
            "snowflake" => Some(Self::Snowflake),
            "databricks" => Some(Self::Databricks),
            "bigquery" => Some(Self::BigQuery),
            "trino" => Some(Self::Trino),
            "sqlserver" => Some(Self::SqlServer),
            "postgres" | "postgresql" => Some(Self::Postgres),
            "redshift" => Some(Self::Redshift),
            _ => None,
        }
    }

    /// The display name used in diagnostics.
    pub fn name(self) -> &'static str {
        match self {
            Self::DuckDb => "DuckDB",
            Self::Snowflake => "Snowflake",
            Self::Databricks => "Databricks",
            Self::BigQuery => "BigQuery",
            Self::Trino => "Trino",
            Self::SqlServer => "SQL Server",
            Self::Postgres => "PostgreSQL",
            Self::Redshift => "Redshift",
        }
    }
}

impl From<rocky_sql::transpile::Dialect> for OperandDialect {
    fn from(value: rocky_sql::transpile::Dialect) -> Self {
        match value {
            rocky_sql::transpile::Dialect::Databricks => Self::Databricks,
            rocky_sql::transpile::Dialect::Snowflake => Self::Snowflake,
            rocky_sql::transpile::Dialect::BigQuery => Self::BigQuery,
            rocky_sql::transpile::Dialect::DuckDB => Self::DuckDb,
        }
    }
}

/// How a dialect treats one operand combination. Ordered by severity so the
/// dialect-agnostic verdict is the minimum across dialects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Verdict {
    /// Valid as written.
    Clean,
    /// Implicitly cast at run time; fails only on some values.
    ValueDependent,
    /// No overload / comparison exists; the statement never runs.
    Refused,
}

/// Coarse type families the coercion table is written over.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Family {
    Numeric,
    Text,
    Temporal,
    Other,
}

fn family(ty: &RockyType) -> Option<Family> {
    if *ty == RockyType::Unknown {
        None
    } else if ty.is_numeric() {
        Some(Family::Numeric)
    } else if *ty == RockyType::String {
        Some(Family::Text)
    } else if ty.is_temporal() {
        Some(Family::Temporal)
    } else {
        Some(Family::Other)
    }
}

/// Aggregate families with an argument-type rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AggregateKind {
    /// `SUM`, `AVG`, `STDDEV*`, `VAR*`: numeric argument expected.
    Numeric,
    /// `BOOL_AND` / `BOOL_OR` and spellings: boolean argument expected.
    Boolean,
    /// `STRING_AGG` (BigQuery spelling).
    StringAgg,
    /// `LISTAGG` (Trino / Snowflake spelling).
    ListAgg,
}

fn aggregate_kind(name: &str) -> Option<AggregateKind> {
    match name {
        "SUM" | "AVG" | "STDDEV" | "STDDEV_POP" | "STDDEV_SAMP" | "VARIANCE" | "VAR_POP"
        | "VAR_SAMP" => Some(AggregateKind::Numeric),
        "BOOL_AND" | "BOOL_OR" | "LOGICAL_AND" | "LOGICAL_OR" | "BOOLAND_AGG" | "BOOLOR_AGG"
        | "EVERY" => Some(AggregateKind::Boolean),
        "STRING_AGG" => Some(AggregateKind::StringAgg),
        "LISTAGG" => Some(AggregateKind::ListAgg),
        _ => None,
    }
}

/// The aggregate signature table.
fn aggregate_verdict(dialect: OperandDialect, kind: AggregateKind, arg: Family) -> Verdict {
    use OperandDialect::*;
    match (kind, arg) {
        // Text into a numeric or boolean aggregate: DuckDB, BigQuery and
        // Trino have no overload; Snowflake and Databricks cast the text at
        // run time. SQL Server's SUM / AVG refuse a character operand at bind
        // time (its boolean-aggregate names do not exist at all, so a call is
        // refused either way).
        (AggregateKind::Numeric | AggregateKind::Boolean, Family::Text) => match dialect {
            DuckDb | BigQuery | Trino | SqlServer | Postgres | Redshift => Verdict::Refused,
            Snowflake | Databricks => Verdict::ValueDependent,
        },
        // BigQuery `STRING_AGG` takes STRING or BYTES only.
        // PostgreSQL `string_agg` takes `text` or `bytea` and does not cast
        // a number to text implicitly.
        (AggregateKind::StringAgg, Family::Numeric) => match dialect {
            BigQuery | Postgres => Verdict::Refused,
            DuckDb | Snowflake | Databricks | Trino | SqlServer | Redshift => Verdict::Clean,
        },
        // Trino `LISTAGG` takes VARCHAR only, and Trino never converts
        // numbers to text implicitly.
        (AggregateKind::ListAgg, Family::Numeric) => match dialect {
            Trino => Verdict::Refused,
            DuckDb | Snowflake | Databricks | BigQuery | SqlServer | Postgres | Redshift => {
                Verdict::Clean
            }
        },
        _ => Verdict::Clean,
    }
}

/// One side of a comparison, as the table needs to see it.
struct Operand<'e> {
    family: Option<Family>,
    /// The text of a string literal, seen through parentheses and casts to a
    /// text type (`'10'::VARCHAR`).
    literal: Option<&'e str>,
}

/// The comparison table. Symmetric; the caller tries both orders.
fn comparison_verdict(dialect: OperandDialect, a: &Operand<'_>, b: &Operand<'_>) -> Verdict {
    use OperandDialect::*;
    match (a.family, b.family) {
        (Some(Family::Numeric), Some(Family::Text)) => match b.literal {
            // `id = '10'`: every dialect that accepts the pair converts the
            // literal once. Kept clean everywhere (the conservative choice).
            Some(text) if text.trim().parse::<f64>().is_ok() => Verdict::Clean,
            // `id = 'abc'`: the value is known not to convert. Reported, but
            // only as a warning, because literal coercion rules vary.
            Some(_) => Verdict::ValueDependent,
            None => match dialect {
                DuckDb | Snowflake | Databricks | SqlServer | Redshift => Verdict::ValueDependent,
                BigQuery | Trino | Postgres => Verdict::Refused,
            },
        },
        (Some(Family::Temporal), Some(Family::Text)) => match b.literal {
            Some(_) => Verdict::Clean,
            None => match dialect {
                // Trino's rule for date-vs-varchar columns is not stated in
                // its conversion docs; warn rather than refuse.
                DuckDb | Snowflake | Databricks | Trino | SqlServer | Redshift => {
                    Verdict::ValueDependent
                }
                BigQuery | Postgres => Verdict::Refused,
            },
        },
        // `order_date > 5`: a date or timestamp against a number, column or
        // literal. DuckDB, PostgreSQL, BigQuery and Trino have no such
        // comparison. The other dialects are not verified here, so the pair
        // is only a warning there; it is never clean, because no dialect is
        // known to accept it.
        (Some(Family::Temporal), Some(Family::Numeric)) => match dialect {
            DuckDb | Postgres | BigQuery | Trino => Verdict::Refused,
            Snowflake | Databricks | SqlServer | Redshift => Verdict::ValueDependent,
        },
        _ => Verdict::Clean,
    }
}

/// What one model's operands are judged against.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum OperandTarget {
    /// No target is known. The verdict is the least severe one across every
    /// dialect, so an unconfigured project only ever gets warnings.
    #[default]
    Unconfigured,
    /// The model can run on each of `dialects`. The verdict is the most
    /// severe one across them (fail closed): a model that one target refuses
    /// is refused. `unruled` names target adapter types this pass has no
    /// table for (ClickHouse); with no ruled dialect left, the verdict falls
    /// back to the unconfigured one and the message names them.
    Targets {
        dialects: Vec<OperandDialect>,
        unruled: Vec<String>,
    },
}

impl From<Option<OperandDialect>> for OperandTarget {
    fn from(value: Option<OperandDialect>) -> Self {
        match value {
            Some(d) => Self::Targets {
                dialects: vec![d],
                unruled: Vec::new(),
            },
            None => Self::Unconfigured,
        }
    }
}

impl OperandTarget {
    /// The dialects a model runs on, when every one of them is known. Empty
    /// when no target is configured, or when one target is a warehouse this
    /// crate has no rules for (ClickHouse, Spark): a type that depends on the
    /// warehouse is then not known.
    pub fn known_dialects(&self) -> &[OperandDialect] {
        match self {
            Self::Targets { dialects, unruled } if unruled.is_empty() => dialects,
            Self::Targets { .. } | Self::Unconfigured => &[],
        }
    }
}

/// The warehouse each model runs on, for the passes that need it inside the
/// compile: today the type of a `CAST` target whose width differs between
/// warehouses (#2333). The default knows no target for any model.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TargetDialects {
    by_model: std::collections::HashMap<String, OperandTarget>,
    otherwise: OperandTarget,
}

impl TargetDialects {
    /// Every model runs on `target`.
    pub fn uniform(target: OperandTarget) -> Self {
        Self {
            by_model: std::collections::HashMap::new(),
            otherwise: target,
        }
    }

    /// `model` runs on `target`, whatever the default says.
    pub fn set(&mut self, model: impl Into<String>, target: OperandTarget) {
        self.by_model.insert(model.into(), target);
    }

    /// The target of `model`.
    pub fn for_model(&self, model: &str) -> &OperandTarget {
        self.by_model.get(model).unwrap_or(&self.otherwise)
    }
}

/// Resolve the verdict, and the dialect that gives it when the target is
/// known: the most severe verdict across the target dialects, or the least
/// severe one across every dialect when no ruled target is known.
fn resolve(
    target: &OperandTarget,
    f: impl Fn(OperandDialect) -> Verdict,
) -> (Verdict, Option<OperandDialect>) {
    match target {
        OperandTarget::Targets { dialects, .. } if !dialects.is_empty() => dialects
            .iter()
            .map(|d| (f(*d), Some(*d)))
            // `max_by_key` keeps the last maximum; reverse so the first
            // configured dialect with the worst verdict names the message.
            .rev()
            .max_by_key(|(v, _)| *v)
            .unwrap_or((Verdict::Clean, None)),
        _ => (
            OperandDialect::ALL
                .iter()
                .map(|d| f(*d))
                .min()
                .unwrap_or(Verdict::Clean),
            None,
        ),
    }
}

/// The opening of the message when no ruled target dialect is known.
fn no_dialect_clause(target: &OperandTarget) -> String {
    match target {
        OperandTarget::Targets { unruled, .. } if !unruled.is_empty() => format!(
            "Rocky has no operand rules for the target warehouse ({}) yet; across the dialects \
             it does know",
            unruled.join(", ")
        ),
        _ => "With no target dialect configured".to_string(),
    }
}

/// Dialects (from all of them) that give `verdict`, for the unknown-dialect
/// message.
fn dialects_with(f: impl Fn(OperandDialect) -> Verdict, verdict: Verdict) -> String {
    OperandDialect::ALL
        .iter()
        .filter(|d| f(**d) == verdict)
        .map(|d| d.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Check aggregate arguments and comparison operands in every model.
///
/// `typed_models` is the type checker's output (it also carries the source
/// schemas, keyed by their qualified name). `graph` is used only to avoid
/// re-reporting a same-named join key that [`check_join_keys`]'s E001/W001
/// already covers.
///
/// [`check_join_keys`]: crate::typecheck
pub fn check_operand_types(
    models: &[rocky_core::models::Model],
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    dialect: Option<OperandDialect>,
) -> Vec<Diagnostic> {
    let target = OperandTarget::from(dialect);
    check_operand_types_per_model(models, graph, typed_models, &|_| target.clone())
}

/// [`check_operand_types`] with a target per model: `target_for` maps a model
/// name to the dialects that model runs on.
pub fn check_operand_types_per_model(
    models: &[rocky_core::models::Model],
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    target_for: &dyn Fn(&str) -> OperandTarget,
) -> Vec<Diagnostic> {
    check_operand_types_with(models, graph, typed_models, &HashSet::new(), target_for)
}

/// [`check_operand_types_per_model`], with the functions and macros the
/// project's seed file creates: a call to one is not judged against DuckDB's
/// built-ins (E069).
pub fn check_operand_types_with(
    models: &[rocky_core::models::Model],
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    seed_names: &HashSet<String>,
    target_for: &dyn Fn(&str) -> OperandTarget,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let guesses = model_guesses(models, typed_models, target_for);
    for model in models {
        let target = target_for(&model.config.name);
        diagnostics.extend(check_model(
            model,
            graph,
            typed_models,
            &guesses,
            seed_names,
            target,
        ));
    }
    diagnostics
}

fn check_model(
    model: &rocky_core::models::Model,
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    guesses: &std::collections::HashMap<String, HashSet<usize>>,
    seed_names: &HashSet<String>,
    target: OperandTarget,
) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
    else {
        return Vec::new();
    };
    let Some(Statement::Query(query)) = statements.first() else {
        return Vec::new();
    };
    let file = model.file_path.display().to_string();
    // Line numbers are only meaningful when the SQL is the file's own text;
    // a `.rocky` model reaches here as lowered SQL.
    let precise_spans = model
        .file_path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("sql"));
    let mut ctx = Ctx {
        model: &model.config.name,
        file,
        precise_spans,
        target,
        functions: graph.functions(),
        seed_names,
        join_key_columns: join_key_checked_columns(&model.config.name, graph, typed_models),
        having_aliases: HashSet::new(),
        having_opaque: false,
        seen: HashSet::new(),
        out: Vec::new(),
    };
    let lookup = |name: &str| {
        typed_models.get(name).map(|columns| RelationRef {
            columns,
            inexact: guesses.get(name),
        })
    };
    walk_query(query, &lookup, &mut ctx);
    ctx.out
}

/// Column names the existing E001/W001 join-key check already reports for
/// this model: names exposed by two or more upstream relations with different
/// known types. A same-named comparison over one of them is left to that
/// check, so one predicate never yields two diagnostics.
fn join_key_checked_columns(
    model: &str,
    graph: &SemanticGraph,
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
) -> HashSet<String> {
    let mut covered = HashSet::new();
    let Some(schema) = graph.model_schema(model) else {
        return covered;
    };
    if schema.upstream.len() < 2 {
        return covered;
    }
    let mut types: std::collections::HashMap<&str, Vec<&RockyType>> =
        std::collections::HashMap::new();
    for upstream in &schema.upstream {
        for col in typed_models.get(upstream.as_str()).into_iter().flatten() {
            if col.data_type != RockyType::Unknown {
                types
                    .entry(col.name.as_str())
                    .or_default()
                    .push(&col.data_type);
            }
        }
    }
    for (name, tys) in types {
        if tys.len() >= 2 && tys.iter().any(|t| *t != tys[0]) {
            covered.insert(name.to_ascii_lowercase());
        }
    }
    covered
}

struct Ctx<'m> {
    model: &'m str,
    file: String,
    precise_spans: bool,
    target: OperandTarget,
    /// Functions the project declares in `functions/`: a call to one is not
    /// judged against DuckDB's built-ins.
    functions: &'m crate::udf::FunctionRegistry,
    /// Functions and macros the project's seed file creates.
    seed_names: &'m HashSet<String>,
    join_key_columns: HashSet<String>,
    /// Lower-case select-list aliases while `HAVING` is checked, else empty.
    having_aliases: HashSet<String>,
    /// While `HAVING` is checked: the select list can rename columns in a
    /// way the aliases do not show, so no bare name is judged.
    having_opaque: bool,
    /// `(code, line, col, message)` already emitted — CTE bodies are typed
    /// twice by construction, never reported twice.
    seen: HashSet<(String, usize, usize, String)>,
    out: Vec<Diagnostic>,
}

impl Ctx<'_> {
    fn emit(
        &mut self,
        refused: bool,
        warn_code: &str,
        err_code: &str,
        at: &Expr,
        message: String,
        suggestion: String,
    ) {
        let span = at.span();
        let (line, col) = if self.precise_spans && span.start.line > 0 {
            (span.start.line as usize, span.start.column as usize)
        } else {
            (1, 1)
        };
        let code = if refused { err_code } else { warn_code };
        if !self
            .seen
            .insert((code.to_string(), line, col, message.clone()))
        {
            return;
        }
        let diag = if refused {
            Diagnostic::error(code, self.model, message)
        } else {
            Diagnostic::warning(code, self.model, message)
        };
        self.out
            .push(diag.with_suggestion(suggestion).with_span(SourceSpan {
                file: self.file.clone(),
                line,
                col,
            }));
    }
}

fn walk_query<'a>(
    query: &ast::Query,
    lookup: &dyn Fn(&str) -> Option<RelationRef<'a>>,
    ctx: &mut Ctx<'_>,
) {
    // Each CTE keeps which of its columns are a guess, as inference does
    // (#2320), so a guessed column is not judged by E069 / E070.
    let mut ctes: std::collections::HashMap<String, (Vec<TypedColumn>, HashSet<usize>)> =
        std::collections::HashMap::new();
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            let cte_lookup = |name: &str| cte_ref(&ctes, name).or_else(|| lookup(name));
            walk_query(&cte.query, &cte_lookup, ctx);
            let env = InferEnv {
                lookup: &cte_lookup,
                target: &ctx.target,
            };
            let (mut columns, inexact) = match infer_query_types(&cte.query, env) {
                Ok(inferred) => {
                    let inexact = guessed_outputs(&inferred);
                    (inferred.columns, inexact)
                }
                Err(_) => (Vec::new(), HashSet::new()),
            };
            rename_relation_columns(&mut columns, &cte.alias);
            ctes.insert(cte.alias.name.value.clone(), (columns, inexact));
        }
    }
    let lookup = |name: &str| cte_ref(&ctes, name).or_else(|| lookup(name));
    walk_set_expr(&query.body, &lookup, ctx);
}

fn cte_ref<'c>(
    ctes: &'c std::collections::HashMap<String, (Vec<TypedColumn>, HashSet<usize>)>,
    name: &str,
) -> Option<RelationRef<'c>> {
    ctes.get(name).map(|(columns, inexact)| RelationRef {
        columns,
        inexact: Some(inexact),
    })
}

/// The output positions of an inferred query whose type is a guess: not
/// exact per `has_exact_type`, or a guess where a `*` read it from.
fn guessed_outputs(inferred: &crate::typecheck::SelectInference) -> HashSet<usize> {
    (0..inferred.columns.len())
        .filter(|i| {
            !inferred.exact_type_outputs.contains(i) || inferred.relation_inexact.contains(i)
        })
        .collect()
}

/// For each model, the output positions whose type is a guess (#2659 red
/// team): `AVG` over a column of a table with no known schema is typed
/// `Float64` only so a contract on it is checked. The type checker treats a
/// model's columns as exact; E069 / E070 must not. A guess carries through
/// pass-through models, so this repeats until nothing changes. A model whose
/// SQL Rocky cannot infer, or whose inferred columns do not line up with its
/// typed ones, has every column a guess.
fn model_guesses(
    models: &[rocky_core::models::Model],
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    target_for: &dyn Fn(&str) -> OperandTarget,
) -> std::collections::HashMap<String, HashSet<usize>> {
    let mut guesses: std::collections::HashMap<String, HashSet<usize>> =
        std::collections::HashMap::new();
    for _ in 0..=models.len() {
        let mut changed = false;
        for model in models {
            let name = &model.config.name;
            let Some(typed) = typed_models.get(name) else {
                continue;
            };
            let all = || (0..typed.len()).collect::<HashSet<usize>>();
            let inferred = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
                .ok()
                .and_then(|statements| match statements.as_slice() {
                    [Statement::Query(query)] => {
                        let lookup = |n: &str| {
                            typed_models.get(n).map(|columns| RelationRef {
                                columns,
                                inexact: guesses.get(n),
                            })
                        };
                        let target = target_for(name);
                        infer_query_types(
                            query,
                            InferEnv {
                                lookup: &lookup,
                                target: &target,
                            },
                        )
                        .ok()
                    }
                    _ => None,
                });
            let guessed = match inferred {
                Some(inferred) if inferred.columns.len() == typed.len() => {
                    guessed_outputs(&inferred)
                }
                _ => all(),
            };
            if guesses.get(name) != Some(&guessed) {
                guesses.insert(name.clone(), guessed);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    guesses
}

fn walk_set_expr<'a>(
    body: &SetExpr,
    lookup: &dyn Fn(&str) -> Option<RelationRef<'a>>,
    ctx: &mut Ctx<'_>,
) {
    match body {
        SetExpr::Select(select) => walk_select(select, lookup, ctx),
        SetExpr::Query(query) => walk_query(query, lookup, ctx),
        SetExpr::SetOperation { left, right, .. } => {
            walk_set_expr(left, lookup, ctx);
            walk_set_expr(right, lookup, ctx);
        }
        _ => {}
    }
}

fn walk_table_factor<'a>(
    factor: &TableFactor,
    lookup: &dyn Fn(&str) -> Option<RelationRef<'a>>,
    ctx: &mut Ctx<'_>,
) {
    match factor {
        TableFactor::Derived { subquery, .. } => walk_query(subquery, lookup, ctx),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            walk_table_factor(&table_with_joins.relation, lookup, ctx);
            for join in &table_with_joins.joins {
                walk_table_factor(&join.relation, lookup, ctx);
            }
        }
        _ => {}
    }
}

fn join_on_constraints(from: &ast::TableWithJoins, out: &mut Vec<Expr>) {
    use ast::{JoinConstraint, JoinOperator};
    if let TableFactor::NestedJoin {
        table_with_joins, ..
    } = &from.relation
    {
        join_on_constraints(table_with_joins, out);
    }
    for join in &from.joins {
        if let TableFactor::NestedJoin {
            table_with_joins, ..
        } = &join.relation
        {
            join_on_constraints(table_with_joins, out);
        }
        let constraint = match &join.join_operator {
            JoinOperator::Join(c)
            | JoinOperator::Inner(c)
            | JoinOperator::Left(c)
            | JoinOperator::LeftOuter(c)
            | JoinOperator::Right(c)
            | JoinOperator::RightOuter(c)
            | JoinOperator::FullOuter(c)
            | JoinOperator::Semi(c)
            | JoinOperator::LeftSemi(c)
            | JoinOperator::RightSemi(c)
            | JoinOperator::Anti(c)
            | JoinOperator::LeftAnti(c)
            | JoinOperator::RightAnti(c) => c,
            _ => continue,
        };
        if let JoinConstraint::On(expr) = constraint {
            out.push(expr.clone());
        }
    }
}

fn walk_select<'a>(
    select: &ast::Select,
    lookup: &dyn Fn(&str) -> Option<RelationRef<'a>>,
    ctx: &mut Ctx<'_>,
) {
    let mut on_exprs = Vec::new();
    for from in &select.from {
        walk_table_factor(&from.relation, lookup, ctx);
        for join in &from.joins {
            walk_table_factor(&join.relation, lookup, ctx);
        }
        join_on_constraints(from, &mut on_exprs);
    }
    let env = InferEnv {
        lookup,
        target: &ctx.target,
    };
    let (_, scope) = select_type_scope(select, env);

    for item in &select.projection {
        match item {
            ast::SelectItem::UnnamedExpr(expr)
            | ast::SelectItem::ExprWithAlias { expr, .. }
            | ast::SelectItem::ExprWithAliases { expr, .. } => {
                check_expr(expr, &scope, lookup, ctx);
            }
            _ => {}
        }
    }
    for expr in on_exprs
        .iter()
        .chain(select.selection.as_ref())
        .chain(select.qualify.as_ref())
    {
        check_expr(expr, &scope, lookup, ctx);
    }
    if let Some(having) = &select.having {
        // A `* REPLACE (...)`, `* RENAME (...)` or `COLUMNS(...)` item can
        // give a source column's name to another expression, and the names
        // are not spelled out here. Then no bare name in `HAVING` is judged.
        let opaque = select.projection.iter().any(|item| match item {
            ast::SelectItem::Wildcard(opts) | ast::SelectItem::QualifiedWildcard(_, opts) => {
                opts.opt_replace.is_some() || opts.opt_rename.is_some()
            }
            ast::SelectItem::UnnamedExpr(expr) | ast::SelectItem::ExprWithAlias { expr, .. } => {
                crate::set_operation::expands_to_columns(expr)
            }
            ast::SelectItem::ExprWithAliases { .. } => true,
        });
        let outer_opaque = std::mem::replace(&mut ctx.having_opaque, opaque);
        let aliases: HashSet<String> = select
            .projection
            .iter()
            .filter_map(|item| match item {
                ast::SelectItem::ExprWithAlias { alias, .. } => {
                    Some(alias.value.to_ascii_lowercase())
                }
                _ => None,
            })
            .collect();
        let outer = std::mem::replace(&mut ctx.having_aliases, aliases);
        check_expr(having, &scope, lookup, ctx);
        ctx.having_aliases = outer;
        ctx.having_opaque = outer_opaque;
    }
}

/// Recurse through the expression forms that can carry an aggregate or a
/// comparison. A subquery is walked as its own query with its own scope (an
/// outer column it references resolves `Unknown` there, which is clean).
/// Every other form is left alone — a miss, never a false report.
fn check_expr<'a>(
    expr: &Expr,
    scope: &TypeScope,
    lookup: &dyn Fn(&str) -> Option<RelationRef<'a>>,
    ctx: &mut Ctx<'_>,
) {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            if is_comparison(op) {
                check_comparison(expr, left, right, scope, ctx);
            }
            check_expr(left, scope, lookup, ctx);
            check_expr(right, scope, lookup, ctx);
        }
        Expr::Between {
            expr: inner,
            low,
            high,
            ..
        } => {
            check_comparison(expr, inner, low, scope, ctx);
            check_comparison(expr, inner, high, scope, ctx);
            for e in [inner, low, high] {
                check_expr(e, scope, lookup, ctx);
            }
        }
        Expr::InList {
            expr: inner, list, ..
        } => {
            for item in list {
                check_comparison(expr, inner, item, scope, ctx);
            }
            check_expr(inner, scope, lookup, ctx);
            for item in list {
                check_expr(item, scope, lookup, ctx);
            }
        }
        Expr::Case {
            operand,
            conditions,
            else_result,
            ..
        } => {
            for case_when in conditions {
                if let Some(operand) = operand {
                    check_comparison(expr, operand, &case_when.condition, scope, ctx);
                }
                check_expr(&case_when.condition, scope, lookup, ctx);
                check_expr(&case_when.result, scope, lookup, ctx);
            }
            if let Some(operand) = operand {
                check_expr(operand, scope, lookup, ctx);
            }
            if let Some(else_result) = else_result {
                check_expr(else_result, scope, lookup, ctx);
            }
            let branches: Vec<&Expr> = conditions
                .iter()
                .map(|w| &w.result)
                .chain(else_result.as_deref())
                .collect();
            check_branch_types(expr, "CASE", &branches, scope, ctx);
        }
        Expr::Nested(inner)
        | Expr::UnaryOp { expr: inner, .. }
        | Expr::IsNull(inner)
        | Expr::IsNotNull(inner)
        | Expr::IsTrue(inner)
        | Expr::IsFalse(inner)
        | Expr::IsNotTrue(inner)
        | Expr::IsNotFalse(inner)
        | Expr::Cast { expr: inner, .. } => check_expr(inner, scope, lookup, ctx),
        Expr::Function(func) => {
            check_aggregate(expr, func, scope, ctx);
            check_duckdb_call(expr, func, scope, ctx);
            if let [ast::ObjectNamePart::Identifier(ident)] = func.name.0.as_slice()
                && ident.quote_style.is_none()
                && ident.value.eq_ignore_ascii_case("coalesce")
                && let ast::FunctionArguments::List(list) = &func.args
                && crate::function_check::plain_argument_count(func).is_some()
            {
                let branches: Vec<&Expr> = list
                    .args
                    .iter()
                    .filter_map(|a| match a {
                        ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e),
                        _ => None,
                    })
                    .collect();
                check_branch_types(expr, "COALESCE", &branches, scope, ctx);
            }
            if let ast::FunctionArguments::List(list) = &func.args {
                for arg in &list.args {
                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e))
                    | ast::FunctionArg::Named {
                        arg: ast::FunctionArgExpr::Expr(e),
                        ..
                    } = arg
                    {
                        check_expr(e, scope, lookup, ctx);
                    }
                }
            }
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            ..
        } => {
            check_expr(inner, scope, lookup, ctx);
            walk_query(subquery, lookup, ctx);
        }
        Expr::Subquery(query)
        | Expr::Exists {
            subquery: query, ..
        } => walk_query(query, lookup, ctx),
        _ => {}
    }
}

fn is_comparison(op: &ast::BinaryOperator) -> bool {
    matches!(
        op,
        ast::BinaryOperator::Eq
            | ast::BinaryOperator::NotEq
            | ast::BinaryOperator::Lt
            | ast::BinaryOperator::LtEq
            | ast::BinaryOperator::Gt
            | ast::BinaryOperator::GtEq
            | ast::BinaryOperator::Spaceship
    )
}

/// The text of a string literal, seen through parentheses and through
/// infallible casts to a text type.
fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(v) => match &v.value {
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => Some(s),
            _ => None,
        },
        Expr::Nested(inner) => string_literal(inner),
        Expr::Cast {
            expr: inner,
            data_type:
                ast::DataType::Varchar(_)
                | ast::DataType::Char(_)
                | ast::DataType::Text
                | ast::DataType::String(_),
            kind: ast::CastKind::Cast | ast::CastKind::DoubleColon,
            ..
        } => string_literal(inner),
        _ => None,
    }
}

/// The final name of a bare or qualified column reference.
fn column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(ident) => Some(&ident.value),
        Expr::CompoundIdentifier(parts) => parts.last().map(|p| p.value.as_str()),
        Expr::Nested(inner) => column_name(inner),
        _ => None,
    }
}

fn check_comparison(at: &Expr, left: &Expr, right: &Expr, scope: &TypeScope, ctx: &mut Ctx<'_>) {
    let (left_ty, _) = infer_expr_type(left, scope);
    let (right_ty, _) = infer_expr_type(right, scope);
    let l = Operand {
        family: family(&left_ty),
        literal: string_literal(left),
    };
    let r = Operand {
        family: family(&right_ty),
        literal: string_literal(right),
    };
    let judge =
        |d: OperandDialect| comparison_verdict(d, &l, &r).max(comparison_verdict(d, &r, &l));
    let (verdict, dialect) = resolve(&ctx.target, judge);
    if verdict == Verdict::Clean {
        return;
    }
    // `a.k = b.k` with differing types across upstreams is E001/W001's.
    if let (Some(a), Some(b)) = (column_name(left), column_name(right))
        && a.eq_ignore_ascii_case(b)
        && ctx.join_key_columns.contains(&a.to_ascii_lowercase())
    {
        return;
    }

    let pair = format!("`{left}` ({left_ty}) vs `{right}` ({right_ty})");
    let refused = verdict == Verdict::Refused;
    if l.family != Some(Family::Text) && r.family != Some(Family::Text) {
        // A date or timestamp against a number (the only other ruled pair).
        let message = match (dialect, refused) {
            (Some(d), true) => format!(
                "comparison of incompatible types: {pair}. {} cannot compare a date or \
                 timestamp with a number and does not cast either side, so the query never runs",
                d.name()
            ),
            (Some(d), false) => format!(
                "comparison of a date or timestamp with a number: {pair}. Rocky has no rule \
                 for this pair on {}; DuckDB, PostgreSQL, BigQuery and Trino refuse it",
                d.name()
            ),
            (None, _) => format!(
                "comparison of a date or timestamp with a number: {pair}. {}: {} refuse this \
                 outright",
                no_dialect_clause(&ctx.target),
                dialects_with(judge, Verdict::Refused),
            ),
        };
        let suggestion = "compare with a date or timestamp value (e.g. `DATE '2024-01-01'` or \
                          `CURRENT_DATE - INTERVAL 5 DAY`), or check that the intended column \
                          is used"
            .to_string();
        ctx.emit(refused, W043, E043, at, message, suggestion);
        return;
    }

    let (text_side, other_ty) = if l.family == Some(Family::Text) {
        (left, &right_ty)
    } else {
        (right, &left_ty)
    };
    let target = if other_ty.is_temporal() {
        "a date/timestamp"
    } else {
        "a number"
    };
    let message = match (dialect, refused) {
        (Some(d), true) => format!(
            "comparison of incompatible types: {pair}. {} has no comparison between these types \
             and does not cast implicitly, so the query fails before reading any rows",
            d.name()
        ),
        (Some(d), false) => format!(
            "value-dependent implicit cast in comparison: {pair}. {} casts `{text_side}` to \
             {target} for every row, and the query fails at run time on the first value that \
             does not convert",
            d.name()
        ),
        (None, _) => format!(
            "value-dependent implicit cast in comparison: {pair}. {}: {} refuse this \
             outright; {} cast `{text_side}` to {target} at run time and fail on the first value \
             that does not convert",
            no_dialect_clause(&ctx.target),
            dialects_with(judge, Verdict::Refused),
            dialects_with(judge, Verdict::ValueDependent),
        ),
    };
    let suggestion = format!(
        "check the join or filter uses the intended column; if it does, cast explicitly \
         (e.g. `TRY_CAST({text_side} AS {other_ty})`) so the conversion is visible and \
         NULL-safe"
    );
    ctx.emit(refused, W043, E043, at, message, suggestion);
}

/// Whether the model runs on DuckDB: the call checks generated from the
/// bundled DuckDB (E069, E070) fire only then (#2659, #2661).
fn runs_on_duckdb(target: &OperandTarget) -> bool {
    matches!(target, OperandTarget::Targets { dialects, .. } if dialects.contains(&OperandDialect::DuckDb))
}

/// The class of a branch or argument that is a column whose type Rocky read
/// from the SQL, not a guess. A literal, `NULL`, a computed expression and an
/// unresolved column give `None`: DuckDB coerces literals, and a computed
/// type can be a guess.
fn exact_column_class(
    expr: &Expr,
    scope: &TypeScope,
    aliases: &HashSet<String>,
    opaque: bool,
) -> Option<(ArgClass, RockyType)> {
    let mut inner = expr;
    while let Expr::Nested(e) = inner {
        inner = e;
    }
    // In `HAVING`, DuckDB reads a bare name that is a select-list alias as
    // the aliased expression, not the source column.
    if let Expr::Identifier(ident) = inner
        && (opaque || aliases.contains(&ident.value.to_ascii_lowercase()))
    {
        return None;
    }
    if !matches!(inner, Expr::Identifier(_) | Expr::CompoundIdentifier(_))
        || scope.is_unresolved_column(inner)
        || !has_exact_type(inner, scope)
    {
        return None;
    }
    let (ty, _) = infer_expr_type(inner, scope);
    ArgClass::of(&ty).map(|class| (class, ty))
}

/// E069: a column argument of a type no DuckDB overload of the function
/// accepts at that position (#2659). Judged only for plain calls to
/// unqualified built-ins the project does not declare; aggregates
/// [`check_aggregate`] judges (E042) and call counts DuckDB refuses (E068)
/// are left to those checks.
fn check_duckdb_call(at: &Expr, func: &ast::Function, scope: &TypeScope, ctx: &mut Ctx<'_>) {
    if !runs_on_duckdb(&ctx.target) {
        return;
    }
    let [ast::ObjectNamePart::Identifier(ident)] = func.name.0.as_slice() else {
        return;
    };
    if ident.quote_style.is_some() {
        return;
    }
    let lower = ident.value.to_ascii_lowercase();
    if aggregate_kind(&lower.to_ascii_uppercase()).is_some()
        || crate::function_check::is_special_form(&lower)
        || ctx.functions.declares(&lower)
        || ctx.seed_names.contains(&lower)
    {
        return;
    }
    let Some(count) = crate::function_check::plain_argument_count(func) else {
        return;
    };
    if duckdb_signatures::refuses_count(&lower, count) {
        return;
    }
    let ast::FunctionArguments::List(list) = &func.args else {
        return;
    };
    for (position, arg) in list.args.iter().enumerate() {
        let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg)) = arg else {
            continue;
        };
        let Some((class, ty)) =
            exact_column_class(arg, scope, &ctx.having_aliases, ctx.having_opaque)
        else {
            continue;
        };
        if !duckdb_signatures::refuses_argument(&lower, count, position, class) {
            continue;
        }
        let name = &ident.value;
        let accepted: Vec<&str> = duckdb_signatures::accepted_classes(&lower, count, position)
            .iter()
            .map(|c| c.name())
            .collect();
        let accepts = if accepted.is_empty() {
            "no column type Rocky checks".to_string()
        } else {
            accepted.join(", ")
        };
        let message = format!(
            "DuckDB has no `{name}` that takes {} at argument {} of {count}: `{arg}` is {ty}, \
             so DuckDB refuses the query before it reads any data. That argument takes: \
             {accepts}",
            class_phrase(class),
            position + 1,
        );
        let suggestion = format!(
            "pass a column of a type `{name}` takes, or cast it explicitly (e.g. \
             `CAST({arg} AS VARCHAR)`). If `{name}` is a macro created outside Rocky, call it \
             schema-qualified (`main.{name}(...)`), which Rocky does not check"
        );
        ctx.emit(true, E069, E069, at, message, suggestion);
        return;
    }
}

fn class_phrase(class: ArgClass) -> &'static str {
    match class {
        ArgClass::Numeric => "a number",
        ArgClass::Text => "text",
        ArgClass::Boolean => "a boolean",
        ArgClass::Date => "a date",
        ArgClass::Timestamp => "a timestamp",
    }
}

/// The groups of [`ArgClass`] whose members DuckDB can give one common type
/// in `CASE` and `COALESCE`. Two branches from different groups never bind;
/// within a group the result can depend on the branch order (`COALESCE(i,
/// b, d)` binds, `COALESCE(b, d)` does not), so those are not checked.
fn branch_group(class: ArgClass) -> u8 {
    match class {
        ArgClass::Numeric | ArgClass::Boolean => 0,
        ArgClass::Text => 1,
        ArgClass::Date | ArgClass::Timestamp => 2,
    }
}

/// E070: `CASE` / `COALESCE` branches that are columns from groups DuckDB
/// has no common type for (#2661).
fn check_branch_types(
    at: &Expr,
    construct: &str,
    branches: &[&Expr],
    scope: &TypeScope,
    ctx: &mut Ctx<'_>,
) {
    if !runs_on_duckdb(&ctx.target) {
        return;
    }
    // Every branch must be a classed column or `NULL`: any other branch (a
    // literal, a computed value, a JSON column) can change the type DuckDB
    // unifies to, so the list is then not judged.
    let mut typed: Vec<(&Expr, ArgClass, RockyType)> = Vec::new();
    for branch in branches {
        if matches!(branch, Expr::Value(v) if v.value == ast::Value::Null) {
            continue;
        }
        let Some((class, ty)) =
            exact_column_class(branch, scope, &ctx.having_aliases, ctx.having_opaque)
        else {
            return;
        };
        typed.push((branch, class, ty));
    }
    let Some((first, first_class, first_ty)) = typed.first() else {
        return;
    };
    let Some((other, _, other_ty)) = typed
        .iter()
        .find(|(_, c, _)| branch_group(*c) != branch_group(*first_class))
    else {
        return;
    };
    let message = format!(
        "{construct} mixes `{first}` ({first_ty}) and `{other}` ({other_ty}). DuckDB has no \
         common type for them, so it refuses the query before it reads any data"
    );
    let suggestion = format!(
        "cast the branches to one type (e.g. `CAST({other} AS VARCHAR)`), or return the \
         intended column"
    );
    ctx.emit(true, E070, E070, at, message, suggestion);
}

fn check_aggregate(at: &Expr, func: &ast::Function, scope: &TypeScope, ctx: &mut Ctx<'_>) {
    let Some(name) = func
        .name
        .0
        .last()
        .and_then(ast::ObjectNamePart::as_ident)
        .map(|i| i.value.to_ascii_uppercase())
    else {
        return;
    };
    let Some(kind) = aggregate_kind(&name) else {
        return;
    };
    let ast::FunctionArguments::List(list) = &func.args else {
        return;
    };
    let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg))) = list.args.first() else {
        return;
    };
    // A literal argument is coerced once by every dialect; leave it.
    if string_literal(arg).is_some() || matches!(arg, Expr::Value(_)) {
        return;
    }
    let (arg_ty, _) = infer_expr_type(arg, scope);
    let Some(arg_family) = family(&arg_ty) else {
        return;
    };
    let judge = |d: OperandDialect| aggregate_verdict(d, kind, arg_family);
    let (verdict, dialect) = resolve(&ctx.target, judge);
    if verdict == Verdict::Clean {
        return;
    }
    let call = format!("{name}({arg_ty})");
    let refused = verdict == Verdict::Refused;
    let expected = match kind {
        AggregateKind::Numeric => "a number",
        AggregateKind::Boolean => "a boolean",
        AggregateKind::StringAgg | AggregateKind::ListAgg => "text",
    };
    let message = match (dialect, refused) {
        (Some(d), true) => format!(
            "{call} has no overload on {}: argument `{arg}` is {arg_ty}, and {} does not cast \
             it to {expected} implicitly",
            d.name(),
            d.name()
        ),
        (Some(d), false) => format!(
            "{call}: {} implicitly casts argument `{arg}` ({arg_ty}) to {expected} at run time, \
             so the query fails on the first value that does not convert",
            d.name()
        ),
        (None, _) => format!(
            "{call}: argument `{arg}` is {arg_ty}. {}: {} have no such overload; {} cast it \
             to {expected} at run time and fail on the first value that does not convert",
            no_dialect_clause(&ctx.target),
            dialects_with(judge, Verdict::Refused),
            dialects_with(judge, Verdict::ValueDependent),
        ),
    };
    let suggestion = match kind {
        AggregateKind::Numeric | AggregateKind::Boolean => format!(
            "aggregate the intended column, or cast explicitly (e.g. `{name}(TRY_CAST({arg} AS \
             {}))`)",
            if kind == AggregateKind::Numeric {
                "DOUBLE"
            } else {
                "BOOLEAN"
            }
        ),
        AggregateKind::StringAgg | AggregateKind::ListAgg => {
            format!("cast the argument to text (e.g. `{name}(CAST({arg} AS STRING), ...)`)")
        }
    };
    ctx.emit(refused, W042, E042, at, message, suggestion);
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};
    use std::collections::HashMap;
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

    fn col(name: &str, ty: RockyType) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable: true,
        }
    }

    fn sources() -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([
            (
                "raw.orders".to_string(),
                vec![
                    col("order_id", RockyType::Int64),
                    col("customer_id", RockyType::Int64),
                    col("amount", RockyType::Float64),
                    col("status", RockyType::String),
                    col("order_date", RockyType::Date),
                    col(
                        "price",
                        RockyType::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                    ),
                    col("qty", RockyType::Int32),
                    col("is_paid", RockyType::Boolean),
                ],
            ),
            (
                "raw.customers".to_string(),
                vec![
                    col("customer_id", RockyType::Int64),
                    col("customer_name", RockyType::String),
                    col("email", RockyType::String),
                    col("signed_up_at", RockyType::Timestamp),
                ],
            ),
        ])
    }

    /// Compile the project the normal way, then run this pass.
    fn run(models: &[(&str, &str)], dialect: Option<OperandDialect>) -> Vec<Diagnostic> {
        let models: Vec<Model> = models.iter().map(|(n, s)| model(n, s)).collect();
        let config = crate::compile::CompilerConfig {
            source_schemas: sources(),
            ..Default::default()
        };
        let result = crate::compile::compile_preloaded_models(models, &config).expect("compile");
        check_operand_types(
            &result.project.models,
            &result.semantic_graph,
            &result.type_check.typed_models,
            dialect,
        )
    }

    fn codes(diags: &[Diagnostic]) -> Vec<&str> {
        diags.iter().map(|d| &*d.code).collect()
    }

    fn codes_on(sql: &str, dialect: Option<OperandDialect>) -> Vec<String> {
        run(&[("m", sql)], dialect)
            .into_iter()
            .map(|d| d.code.to_string())
            .collect()
    }

    /// E069: a column argument no DuckDB overload takes (#2659).
    #[test]
    fn a_column_argument_duckdb_has_no_overload_for_is_refused() {
        let duckdb = Some(OperandDialect::DuckDb);
        for sql in [
            "SELECT lower(qty) AS x FROM raw.orders",
            "SELECT abs(status) AS x FROM raw.orders",
            "SELECT md5(order_id) AS x FROM raw.orders",
            "SELECT upper(is_paid) AS x FROM raw.orders",
            "SELECT abs(order_date) AS x FROM raw.orders",
            "SELECT date_trunc('day', qty) AS x FROM raw.orders",
            "SELECT length(o.qty) AS x FROM raw.orders o",
            "SELECT o.order_id FROM raw.orders o WHERE lower(o.amount) = 'x'",
        ] {
            assert_eq!(codes_on(sql, duckdb), ["E069"], "{sql}");
        }
        for sql in [
            "SELECT lower(status) AS x FROM raw.orders",
            "SELECT abs(amount) AS x FROM raw.orders",
            "SELECT abs(price) AS x FROM raw.orders",
            "SELECT lower('x') AS x FROM raw.orders",
            "SELECT abs('5') AS x FROM raw.orders",
            "SELECT substr(status, 1) AS x FROM raw.orders",
            "SELECT concat(qty, status) AS x FROM raw.orders",
            "SELECT date_trunc('day', order_date) AS x FROM raw.orders",
            "SELECT main.lower(qty) AS x FROM raw.orders",
            "SELECT lower(CAST(qty AS VARCHAR)) AS x FROM raw.orders",
            "SELECT lower(missing_col) AS x FROM raw.orders",
            "SELECT lower(q) AS x FROM (SELECT qty + 1 AS q FROM raw.orders) s",
            // Red team: in HAVING a bare name can be a select-list alias.
            "SELECT CAST(qty AS VARCHAR) AS qty, count(*) AS n FROM raw.orders GROUP BY ALL \
             HAVING lower(qty) = '2'",
            "SELECT * REPLACE (CAST(qty AS VARCHAR) AS qty) FROM raw.orders GROUP BY ALL \
             HAVING lower(qty) = '2'",
            "SELECT o.* REPLACE (CAST(qty AS VARCHAR) AS qty) FROM raw.orders o GROUP BY ALL \
             HAVING lower(qty) = '2'",
            "SELECT CAST(COLUMNS('^qty$') AS VARCHAR), count(*) AS n FROM raw.orders \
             GROUP BY ALL HAVING lower(qty) = '2'",
        ] {
            assert!(codes_on(sql, duckdb).is_empty(), "{sql}");
        }
        // SUM over text is E042's, once.
        assert_eq!(
            codes_on("SELECT SUM(status) AS s FROM raw.orders", duckdb),
            ["E042"]
        );
        // Only DuckDB's signatures are known.
        for dialect in [Some(OperandDialect::Snowflake), None] {
            assert!(codes_on("SELECT lower(qty) AS x FROM raw.orders", dialect).is_empty());
        }
    }

    /// DuckDB's top-n `max(col, n)` / `min(col, n)` returns a list, so a
    /// column built from one is not a scalar of the argument's type.
    #[test]
    fn a_top_n_min_max_column_is_not_judged_as_a_scalar() {
        let diags = run(
            &[
                (
                    "up",
                    "SELECT max(status, 2) AS m, min(qty, 3) AS n FROM raw.orders",
                ),
                (
                    "down",
                    "SELECT array_length(m) AS a, list_sort(n) AS b FROM up",
                ),
            ],
            Some(OperandDialect::DuckDb),
        );
        assert!(diags.is_empty(), "{diags:?}");
        assert!(
            codes_on(
                "WITH c AS (SELECT max(status, 2) AS m FROM raw.orders) \
                 SELECT array_length(m) AS a FROM c",
                Some(OperandDialect::DuckDb)
            )
            .is_empty()
        );
        assert!(
            codes_on(
                "SELECT len(max(order_date, 2)) AS x FROM raw.orders",
                Some(OperandDialect::DuckDb)
            )
            .is_empty()
        );
    }

    /// `AVG` over a column of a table with no known schema is typed `Float64`
    /// only so a contract on it is checked; DuckDB may return a timestamp. A
    /// column built from that guess is not judged, in a CTE or downstream.
    #[test]
    fn a_guessed_avg_column_is_not_judged() {
        let duckdb = Some(OperandDialect::DuckDb);
        let diags = run(
            &[
                // Downstream first: the guess must reach `d4` whatever the
                // order models are listed in.
                ("d4", "SELECT year(a) AS y FROM mid"),
                ("mid", "SELECT a FROM a_avg"),
                ("a_avg", "SELECT avg(ts) AS a FROM ext.events"),
                ("d1", "SELECT year(a) AS y FROM a_avg"),
                (
                    "d2",
                    "SELECT coalesce(a, o.order_date) AS x FROM a_avg, raw.orders o",
                ),
            ],
            duckdb,
        );
        assert!(diags.is_empty(), "{diags:?}");
        assert!(
            codes_on(
                "WITH c AS (SELECT avg(ts) AS a FROM ext.events) SELECT year(a) AS y FROM c",
                duckdb
            )
            .is_empty()
        );
        // A known AVG is still judged: AVG over a number is a DOUBLE.
        assert_eq!(
            codes_on(
                "WITH c AS (SELECT avg(qty) AS a FROM raw.orders) SELECT lower(a) AS y FROM c",
                duckdb
            ),
            ["E069"]
        );
        let diags = run(
            &[
                ("k_avg", "SELECT avg(qty) AS a FROM raw.orders"),
                ("d3", "SELECT lower(a) AS y FROM k_avg"),
            ],
            duckdb,
        );
        assert_eq!(codes(&diags), vec!["E069"], "{diags:?}");
    }

    /// A seed-file macro reusing a built-in name is the project's to judge.
    #[test]
    fn a_seed_file_macro_argument_is_not_judged() {
        let models = vec![model("m", "SELECT lower(qty) AS x FROM raw.orders")];
        let config = crate::compile::CompilerConfig {
            source_schemas: sources(),
            ..Default::default()
        };
        let result = crate::compile::compile_preloaded_models(models, &config).expect("compile");
        let target = OperandTarget::from(Some(OperandDialect::DuckDb));
        let judge = |seed: &HashSet<String>| {
            check_operand_types_with(
                &result.project.models,
                &result.semantic_graph,
                &result.type_check.typed_models,
                seed,
                &|_| target.clone(),
            )
        };
        assert_eq!(codes(&judge(&HashSet::new())), vec!["E069"]);
        assert!(judge(&HashSet::from(["lower".to_string()])).is_empty());
    }

    /// The column of an upstream model is judged by its type, too.
    #[test]
    fn an_upstream_model_column_argument_is_judged() {
        let diags = run(
            &[
                ("up", "SELECT qty AS n FROM raw.orders"),
                ("down", "SELECT lower(n) AS x FROM up"),
            ],
            Some(OperandDialect::DuckDb),
        );
        assert_eq!(codes(&diags), vec!["E069"], "{diags:?}");
        assert!(diags[0].message.contains("lower"), "{diags:?}");
    }

    /// E070: `CASE` / `COALESCE` branches DuckDB has no common type for
    /// (#2661).
    #[test]
    fn branches_duckdb_cannot_unify_are_refused() {
        let duckdb = Some(OperandDialect::DuckDb);
        for sql in [
            "SELECT coalesce(order_date, status) AS x FROM raw.orders",
            "SELECT coalesce(qty, status) AS x FROM raw.orders",
            "SELECT CASE WHEN is_paid THEN qty ELSE status END AS x FROM raw.orders",
            "SELECT CASE WHEN qty > 1 THEN order_date WHEN is_paid THEN amount END AS x \
             FROM raw.orders",
            "SELECT coalesce(is_paid, order_date) AS x FROM raw.orders",
            "SELECT coalesce(qty, NULL, status) AS x FROM raw.orders",
        ] {
            assert_eq!(codes_on(sql, duckdb), ["E070"], "{sql}");
        }
        for sql in [
            "SELECT coalesce(qty, price, amount) AS x FROM raw.orders",
            "SELECT coalesce(qty, is_paid, price) AS x FROM raw.orders",
            "SELECT coalesce(status, 'x') AS x FROM raw.orders",
            "SELECT coalesce(qty, '1') AS x FROM raw.orders",
            "SELECT coalesce(order_date, '2024-01-01') AS x FROM raw.orders",
            // The issue's repro binds; it fails only when a row reaches the
            // text branch, so it is not refused here.
            "SELECT CASE WHEN qty > 1 THEN 'many' ELSE qty END AS x FROM raw.orders",
            "SELECT CASE WHEN is_paid THEN qty ELSE CAST(status AS INT) END AS x \
             FROM raw.orders",
            // Red team: a branch Rocky does not class can change DuckDB's
            // result type (JSON here), so the list is not judged.
            "SELECT coalesce(qty, CAST(status AS JSON), status) AS x FROM raw.orders",
            "SELECT coalesce(qty, upper(status), status) AS x FROM raw.orders",
            "SELECT coalesce(qty, '1', status) AS x FROM raw.orders",
            "SELECT CAST(order_date AS VARCHAR) AS order_date, max(status) AS status \
             FROM raw.orders GROUP BY ALL HAVING coalesce(order_date, status) = 's'",
        ] {
            assert!(codes_on(sql, duckdb).is_empty(), "{sql}");
        }
        assert!(codes_on("SELECT coalesce(qty, status) AS x FROM raw.orders", None).is_empty());
    }

    /// Every branch list E070's rule refuses, built from the DuckDB types
    /// each class stands for, is refused by the bundled DuckDB, in `COALESCE`
    /// and in `CASE`, in every order and with a `NULL` branch mixed in.
    #[test]
    fn duckdb_refuses_every_branch_mix_e070_reports() {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        let mut members: Vec<(ArgClass, String, &str)> = Vec::new();
        let mut columns = Vec::new();
        for class in ArgClass::ALL {
            for (i, ty) in class.duckdb_types().iter().enumerate() {
                let name = format!("c_{}_{i}", class.name());
                columns.push(format!("{name} {ty}"));
                members.push((class, name, ty));
            }
        }
        conn.execute_batch(&format!(
            "CREATE TABLE t (b BOOLEAN, {})",
            columns.join(", ")
        ))
        .unwrap();
        let refused_by_rule = |list: &[&(ArgClass, String, &str)]| {
            list.iter()
                .any(|m| branch_group(m.0) != branch_group(list[0].0))
        };
        let mut checked = 0;
        let mut lists: Vec<Vec<&(ArgClass, String, &str)>> = Vec::new();
        for a in &members {
            for b in &members {
                lists.push(vec![a, b]);
                for c in &members {
                    lists.push(vec![a, b, c]);
                }
            }
        }
        for list in lists {
            if !refused_by_rule(&list) {
                continue;
            }
            let mut names: Vec<&str> = list.iter().map(|m| m.1.as_str()).collect();
            for with_null in [false, true] {
                if with_null {
                    names.insert(1, "NULL");
                }
                let coalesce = format!("SELECT coalesce({}) FROM t", names.join(", "));
                let (last, init) = names.split_last().unwrap();
                let whens: Vec<String> = init.iter().map(|n| format!("WHEN b THEN {n}")).collect();
                let case = format!("SELECT CASE {} ELSE {last} END FROM t", whens.join(" "));
                for sql in [coalesce, case] {
                    let err = conn.prepare(&sql).err().map(|e| e.to_string());
                    assert!(
                        err.as_deref()
                            .is_some_and(|e| e.contains("Cannot mix values of type")),
                        "DuckDB does not refuse {sql} for its branch types: {err:?}"
                    );
                    checked += 1;
                }
            }
        }
        assert!(checked > 2000, "checked {checked}");
    }

    const D2: &str =
        "SELECT customer_id, SUM(customer_name) AS s FROM raw.customers GROUP BY customer_id";
    const D5: &str = "SELECT o.order_id, c.customer_name FROM raw.orders o \
                      JOIN raw.customers c ON o.customer_id = c.customer_name";

    #[test]
    fn d2_sum_over_varchar_is_refused_on_duckdb() {
        let diags = run(&[("bad_agg", D2)], Some(OperandDialect::DuckDb));
        assert_eq!(codes(&diags), vec!["E042"], "{diags:?}");
        assert!(diags[0].message.contains("SUM(STRING)"), "{diags:?}");
        assert!(diags[0].message.contains("customer_name"), "{diags:?}");
        let span = diags[0].span.as_ref().expect("span");
        assert_eq!((span.line, span.col), (1, 21), "points at the SUM call");
    }

    #[test]
    fn d2_per_dialect_verdicts() {
        for (dialect, code) in [
            (OperandDialect::BigQuery, "E042"),
            (OperandDialect::Trino, "E042"),
            (OperandDialect::SqlServer, "E042"),
            (OperandDialect::Snowflake, "W042"),
            (OperandDialect::Databricks, "W042"),
        ] {
            let diags = run(&[("bad_agg", D2)], Some(dialect));
            assert_eq!(codes(&diags), vec![code], "{dialect:?}: {diags:?}");
        }
        // Unknown dialect: the least severe verdict across dialects.
        let diags = run(&[("bad_agg", D2)], None);
        assert_eq!(codes(&diags), vec!["W042"], "{diags:?}");
        assert!(
            diags[0]
                .message
                .contains("DuckDB, BigQuery, Trino, SQL Server")
        );
    }

    #[test]
    fn d5_varchar_join_key_warns_on_duckdb_and_refuses_on_bigquery() {
        let diags = run(&[("bad_join", D5)], Some(OperandDialect::DuckDb));
        assert_eq!(codes(&diags), vec!["W043"], "{diags:?}");
        assert!(diags[0].message.contains("o.customer_id"), "{diags:?}");
        assert!(diags[0].message.contains("fails at run time"), "{diags:?}");
        assert!(diags[0].suggestion.as_deref().unwrap().contains("TRY_CAST"));

        let diags = run(&[("bad_join", D5)], Some(OperandDialect::BigQuery));
        assert_eq!(codes(&diags), vec!["E043"], "{diags:?}");
        let diags = run(&[("bad_join", D5)], Some(OperandDialect::Trino));
        assert_eq!(codes(&diags), vec!["E043"], "{diags:?}");
        // SQL Server converts the VARCHAR side per row (data type
        // precedence): value-dependent, like Snowflake and Databricks.
        for d in [
            OperandDialect::Snowflake,
            OperandDialect::Databricks,
            OperandDialect::SqlServer,
        ] {
            assert_eq!(codes(&run(&[("bad_join", D5)], Some(d))), vec!["W043"]);
        }
        assert_eq!(codes(&run(&[("bad_join", D5)], None)), vec!["W043"]);
    }

    #[test]
    fn comparisons_are_found_in_every_clause() {
        for sql in [
            "SELECT order_id FROM raw.orders o JOIN raw.customers c ON c.customer_name <> o.customer_id",
            "SELECT order_id FROM raw.orders WHERE customer_id = status",
            "SELECT status, COUNT(*) FROM raw.orders GROUP BY status HAVING MAX(order_id) > MAX(status)",
            "SELECT customer_id IN (status, 'x') AS f FROM raw.orders",
            "SELECT order_id BETWEEN status AND 10 AS f FROM raw.orders",
            "SELECT CASE customer_id WHEN status THEN 1 END AS f FROM raw.orders",
            "SELECT order_id FROM raw.orders WHERE order_id IN (SELECT customer_name FROM raw.customers WHERE customer_id = email)",
            "WITH x AS (SELECT * FROM raw.orders) SELECT order_id FROM x WHERE x.customer_id = x.status",
            "SELECT * FROM (SELECT order_id FROM raw.orders WHERE amount >= status) d",
        ] {
            let diags = run(&[("m", sql)], Some(OperandDialect::BigQuery));
            assert!(
                codes(&diags).contains(&"E043"),
                "missed comparison in `{sql}`: {diags:?}"
            );
        }
    }

    #[test]
    fn date_vs_text_column_follows_the_dialect() {
        let sql =
            "SELECT o.order_id FROM raw.orders o JOIN raw.customers c ON o.order_date = c.email";
        assert_eq!(
            codes(&run(&[("m", sql)], Some(OperandDialect::DuckDb))),
            vec!["W043"]
        );
        assert_eq!(
            codes(&run(&[("m", sql)], Some(OperandDialect::BigQuery))),
            vec!["E043"]
        );
    }

    #[test]
    fn date_vs_number_is_refused_where_no_comparison_exists() {
        for sql in [
            "SELECT order_id FROM raw.orders WHERE order_date > 5",
            "SELECT order_id FROM raw.orders WHERE 5 <= order_date",
            "SELECT order_id FROM raw.orders WHERE order_date = customer_id",
            "SELECT order_id FROM raw.orders WHERE order_date BETWEEN 1 AND 5",
            "SELECT c.customer_id FROM raw.customers c WHERE c.signed_up_at >= 20240101",
        ] {
            for (dialect, code) in [
                (Some(OperandDialect::DuckDb), "E043"),
                (Some(OperandDialect::Postgres), "E043"),
                (Some(OperandDialect::BigQuery), "E043"),
                (Some(OperandDialect::Trino), "E043"),
                (Some(OperandDialect::Snowflake), "W043"),
                (Some(OperandDialect::Databricks), "W043"),
                (Some(OperandDialect::SqlServer), "W043"),
                (Some(OperandDialect::Redshift), "W043"),
                (None, "W043"),
            ] {
                let diags = run(&[("m", sql)], dialect);
                assert!(
                    !diags.is_empty() && codes(&diags).iter().all(|c| *c == code),
                    "{dialect:?} `{sql}`: {diags:?}"
                );
            }
        }
        let diags = run(
            &[("m", "SELECT order_id FROM raw.orders WHERE order_date > 5")],
            Some(OperandDialect::DuckDb),
        );
        assert!(diags[0].message.contains("order_date"), "{diags:?}");
        assert!(diags[0].message.contains("DuckDB"), "{diags:?}");
        assert!(
            !diags[0].suggestion.as_deref().unwrap().contains("TRY_CAST"),
            "a number does not cast to a date: {diags:?}"
        );
    }

    #[test]
    fn text_vs_number_join_through_upstream_models_warns_on_duckdb() {
        // Types flow through two upstream models; the join compares text with
        // a number.
        let project = [
            (
                "stg_customers",
                "SELECT customer_id, email FROM raw.customers",
            ),
            (
                "customer_ltv",
                "SELECT customer_id, COUNT(*) AS order_count FROM raw.orders GROUP BY customer_id",
            ),
            (
                "dim_customers",
                "SELECT c.customer_id, COALESCE(l.order_count, 0) AS order_count \
                 FROM stg_customers AS c LEFT JOIN customer_ltv AS l ON c.email = l.customer_id",
            ),
        ];
        let diags = run(&project, Some(OperandDialect::DuckDb));
        assert_eq!(codes(&diags), vec!["W043"], "{diags:?}");
        assert_eq!(diags[0].model, "dim_customers");
        let fixed = [
            project[0],
            project[1],
            (
                "dim_customers",
                "SELECT c.customer_id FROM stg_customers AS c \
                 LEFT JOIN customer_ltv AS l ON c.customer_id = l.customer_id",
            ),
        ];
        assert!(run(&fixed, Some(OperandDialect::DuckDb)).is_empty());
    }

    #[test]
    fn date_arithmetic_and_date_literals_stay_clean() {
        for sql in [
            "SELECT order_id FROM raw.orders WHERE order_date >= CURRENT_DATE - 5",
            "SELECT order_id FROM raw.orders WHERE order_date - order_date > 5",
            "SELECT order_id FROM raw.orders WHERE order_date + 1 > order_date",
            "SELECT order_id FROM raw.orders WHERE order_date >= DATE '2024-01-01'",
            "SELECT order_id FROM raw.orders WHERE order_date >= '2024-01-01'",
            "SELECT order_id FROM raw.orders WHERE YEAR(order_date) = 2024",
            "SELECT order_id FROM raw.orders WHERE missing_col + 1 > order_date",
            "SELECT order_id FROM raw.orders WHERE order_date IS NOT NULL AND order_id >= '1'",
        ] {
            for dialect in OperandDialect::ALL.map(Some).into_iter().chain([None]) {
                let diags = run(&[("m", sql)], dialect);
                assert!(diags.is_empty(), "{dialect:?} `{sql}`: {diags:?}");
            }
        }
    }

    #[test]
    fn non_numeric_string_literal_against_a_number_warns() {
        let sql = "SELECT order_id FROM raw.orders WHERE customer_id = 'abc'";
        for d in OperandDialect::ALL {
            assert_eq!(codes(&run(&[("m", sql)], Some(d))), vec!["W043"], "{d:?}");
        }
    }

    #[test]
    fn valid_controls_stay_clean_on_every_dialect() {
        let controls: &[&[(&str, &str)]] = &[
            // V2: DuckDB coerces a numeric string literal.
            &[("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value")],
            &[(
                "v2b",
                "SELECT order_id FROM raw.orders WHERE customer_id = '10' OR amount IN ('1.5', ' 2 ')",
            )],
            // C2
            &[
                (
                    "stg_orders",
                    "SELECT order_id, customer_id, amount FROM raw.orders",
                ),
                (
                    "fct_revenue",
                    "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
                     JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
                ),
            ],
            // V1: lateral alias stays Unknown.
            &[(
                "v1",
                "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
            )],
            // V3
            &[(
                "v3",
                "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
            )],
            // V4: unknown function result.
            &[(
                "v4",
                "SELECT sha256(customer_name) AS h FROM raw.customers WHERE sha256(customer_name) = customer_id",
            )],
            // Aggregate overload controls.
            &[(
                "aggs",
                "SELECT SUM(price) AS a, AVG(qty) AS b, MAX(status) AS c, MIN(order_date) AS d, \
                 COUNT(*) AS e, COUNT(DISTINCT status) AS f, SUM(DISTINCT amount) AS g, \
                 STDDEV(amount) AS h, BOOL_AND(is_paid) AS i, STRING_AGG(status, ',') AS j, \
                 ARRAY_AGG(status) AS k, SUM(1) AS l, SUM(amount) OVER (PARTITION BY status) AS m \
                 FROM raw.orders",
            )],
            // Temporal comparisons.
            &[(
                "dates",
                "SELECT o.order_id FROM raw.orders o JOIN raw.customers c \
                 ON o.order_date = c.signed_up_at WHERE o.order_date >= '2024-01-01' \
                 AND c.signed_up_at < '2024-01-01 00:00:00'",
            )],
            // Numeric widening and unknown columns.
            &[(
                "nums",
                "SELECT order_id FROM raw.orders WHERE qty = order_id AND price > amount \
                 AND missing_col = status AND is_paid = TRUE",
            )],
            // Ambiguous bare name resolves Unknown.
            &[(
                "ambiguous",
                "SELECT o.order_id FROM raw.orders o JOIN raw.customers c \
                 ON o.customer_id = c.customer_id WHERE customer_id = 'abc'",
            )],
            // Outer column inside a correlated subquery resolves Unknown.
            &[(
                "correlated",
                "SELECT order_id FROM raw.orders o WHERE EXISTS \
                 (SELECT 1 FROM raw.customers c WHERE c.customer_id = o.status)",
            )],
        ];
        for project in controls {
            for dialect in OperandDialect::ALL.map(Some).into_iter().chain([None]) {
                let diags = run(project, dialect);
                assert!(diags.is_empty(), "{dialect:?} {project:?}: {diags:?}");
            }
        }
    }

    fn run_target(models: &[(&str, &str)], target: &OperandTarget) -> Vec<Diagnostic> {
        let models: Vec<Model> = models.iter().map(|(n, s)| model(n, s)).collect();
        let config = crate::compile::CompilerConfig {
            source_schemas: sources(),
            ..Default::default()
        };
        let result = crate::compile::compile_preloaded_models(models, &config).expect("compile");
        check_operand_types_per_model(
            &result.project.models,
            &result.semantic_graph,
            &result.type_check.typed_models,
            &|_| target.clone(),
        )
    }

    const DATE_VS_TEXT: &str =
        "SELECT o.order_id FROM raw.orders o JOIN raw.customers c ON o.order_date = c.email";
    const STRING_AGG_NUM: &str = "SELECT STRING_AGG(order_id, ',') AS s FROM raw.orders";

    /// PostgreSQL has no implicit cast from `text` (verified on PostgreSQL
    /// 16): `sum(text)`, `bool_and(text)`, `string_agg(integer, …)`,
    /// `integer = text` and `date = text` all fail before reading a row.
    #[test]
    fn postgres_refuses_text_aggregates_and_text_comparisons() {
        let pg = Some(OperandDialect::Postgres);
        for (sql, code) in [
            (D2, "E042"),
            ("SELECT BOOL_AND(status) AS b FROM raw.orders", "E042"),
            ("SELECT AVG(status) AS b FROM raw.orders", "E042"),
            (STRING_AGG_NUM, "E042"),
            (D5, "E043"),
            (DATE_VS_TEXT, "E043"),
        ] {
            let diags = run(&[("m", sql)], pg);
            assert_eq!(codes(&diags), vec![code], "{sql}: {diags:?}");
            assert!(diags[0].message.contains("PostgreSQL"), "{diags:?}");
        }
        // Valid controls: a numeric literal, a numeric aggregate, text into
        // STRING_AGG.
        for sql in [
            "SELECT order_id FROM raw.orders WHERE customer_id = '10'",
            "SELECT SUM(amount) AS s, STRING_AGG(status, ',') AS t FROM raw.orders",
            "SELECT order_id FROM raw.orders WHERE order_date >= '2024-01-01'",
        ] {
            assert!(run(&[("m", sql)], pg).is_empty(), "{sql}");
        }
    }

    /// Redshift's numeric aggregates take numeric arguments only, but it
    /// converts a character string implicitly in a comparison, so a text
    /// column against a number or a date is value-dependent.
    #[test]
    fn redshift_refuses_text_aggregates_and_warns_on_text_comparisons() {
        let rs = Some(OperandDialect::Redshift);
        for (sql, code) in [
            (D2, "E042"),
            ("SELECT STDDEV(status) AS b FROM raw.orders", "E042"),
            (D5, "W043"),
            (DATE_VS_TEXT, "W043"),
        ] {
            let diags = run(&[("m", sql)], rs);
            assert_eq!(codes(&diags), vec![code], "{sql}: {diags:?}");
            assert!(diags[0].message.contains("Redshift"), "{diags:?}");
        }
        assert!(run(&[("m", STRING_AGG_NUM)], rs).is_empty());
        assert!(run(&[("m", "SELECT SUM(amount) AS s FROM raw.orders")], rs).is_empty());
    }

    /// A model that runs on several warehouses is judged against the
    /// strictest one: DuckDB refuses `SUM(VARCHAR)` even though Snowflake
    /// only warns.
    #[test]
    fn several_targets_take_the_most_severe_verdict() {
        let target = OperandTarget::Targets {
            dialects: vec![OperandDialect::Snowflake, OperandDialect::DuckDb],
            unruled: Vec::new(),
        };
        let diags = run_target(&[("m", D2)], &target);
        assert_eq!(codes(&diags), vec!["E042"], "{diags:?}");
        assert!(diags[0].message.contains("DuckDB"), "{diags:?}");
    }

    /// A ClickHouse target has no table yet. The verdict stays the
    /// unconfigured one, and the message says so instead of claiming no
    /// dialect is configured.
    #[test]
    fn unruled_target_says_it_has_no_rules() {
        let target = OperandTarget::Targets {
            dialects: Vec::new(),
            unruled: vec!["clickhouse".to_string()],
        };
        let diags = run_target(&[("m", D2)], &target);
        assert_eq!(codes(&diags), vec!["W042"], "{diags:?}");
        assert!(
            diags[0]
                .message
                .contains("no operand rules for the target warehouse (clickhouse)"),
            "{diags:?}"
        );
        assert!(!diags[0].message.contains("no target dialect configured"));
    }

    #[test]
    fn string_agg_over_numbers_is_refused_only_where_documented() {
        let sql = "SELECT STRING_AGG(order_id, ',') AS s FROM raw.orders";
        assert_eq!(
            codes(&run(&[("m", sql)], Some(OperandDialect::BigQuery))),
            vec!["E042"]
        );
        assert!(run(&[("m", sql)], Some(OperandDialect::DuckDb)).is_empty());
        assert!(run(&[("m", sql)], None).is_empty());
        let sql = "SELECT LISTAGG(order_id, ',') AS s FROM raw.orders";
        assert_eq!(
            codes(&run(&[("m", sql)], Some(OperandDialect::Trino))),
            vec!["E042"]
        );
        assert!(run(&[("m", sql)], Some(OperandDialect::Snowflake)).is_empty());
    }

    #[test]
    fn boolean_and_stddev_aggregates_over_text() {
        for sql in [
            "SELECT BOOL_AND(status) AS b FROM raw.orders",
            "SELECT STDDEV(status) AS b FROM raw.orders",
            "SELECT AVG(status) AS b FROM raw.orders",
        ] {
            assert_eq!(
                codes(&run(&[("m", sql)], Some(OperandDialect::DuckDb))),
                vec!["E042"],
                "{sql}"
            );
            assert_eq!(
                codes(&run(&[("m", sql)], Some(OperandDialect::Snowflake))),
                vec!["W042"],
                "{sql}"
            );
        }
    }

    /// `a.k = b.k` with mismatched types across two upstream models is the
    /// existing join-key check's (E001/W001); this pass must not re-report it.
    #[test]
    fn same_named_join_key_is_left_to_the_join_key_check() {
        let models = [
            (
                "a",
                "SELECT CAST(customer_id AS STRING) AS k FROM raw.orders",
            ),
            ("b", "SELECT customer_id AS k FROM raw.customers"),
            ("j", "SELECT a.k FROM a JOIN b ON a.k = b.k"),
        ];
        let diags = run(&models, Some(OperandDialect::BigQuery));
        assert!(diags.is_empty(), "{diags:?}");
        let compiled = crate::compile::compile_preloaded_models(
            models.iter().map(|(n, s)| model(n, s)).collect(),
            &crate::compile::CompilerConfig {
                source_schemas: sources(),
                ..Default::default()
            },
        )
        .expect("compile");
        assert!(
            compiled
                .diagnostics
                .iter()
                .any(|d| matches!(&*d.code, "E001" | "W001") && d.model == "j"),
            "the join-key check must own this predicate: {:?}",
            compiled.diagnostics
        );
        // A differently named pair over the same models is still ours.
        let models = [
            (
                "a",
                "SELECT CAST(customer_id AS STRING) AS k FROM raw.orders",
            ),
            ("b", "SELECT customer_id AS id FROM raw.customers"),
            ("j", "SELECT a.k FROM a JOIN b ON a.k = b.id"),
        ];
        assert_eq!(
            codes(&run(&models, Some(OperandDialect::BigQuery))),
            vec!["E043"]
        );
    }

    #[test]
    fn adapter_type_maps_to_dialect() {
        assert_eq!(
            OperandDialect::from_adapter_type("DuckDB"),
            Some(OperandDialect::DuckDb)
        );
        assert_eq!(
            OperandDialect::from_adapter_type("trino"),
            Some(OperandDialect::Trino)
        );
        assert_eq!(OperandDialect::from_adapter_type("fivetran"), None);
    }
}
