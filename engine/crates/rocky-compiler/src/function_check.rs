//! Calls to functions the target warehouse does not have (E045).
//!
//! `SUMM(amount)` parses and type-checks as an unknown function returning
//! `Unknown`, so it used to compile clean and fail at run time. This pass
//! reports a call whose name is neither a function of the target dialect nor
//! a project function (`functions/`).
//!
//! # When it fires
//!
//! Only when the model runs on DuckDB (its adapter, `--target-dialect duckdb`
//! or `[portability] target_dialect = "duckdb"`), because only DuckDB has a
//! complete function list here: `data/duckdb_functions.txt`, generated from
//! `duckdb_functions()` plus the functions DuckDB autoloads from its
//! extensions. Every other dialect stays silent, so no real warehouse function
//! is ever refused for lack of a list.
//!
//! # Never reported
//!
//! - A schema-qualified call (`main.my_macro(x)`): it names its own catalog
//!   entry, which may be a macro or an extension function created outside
//!   Rocky. This is also the escape hatch for such functions.
//! - A quoted name (`"my fn"(x)`).
//! - A name the project declares in `functions/`, valid or not (E051 covers
//!   an invalid one).
//! - SQL forms DuckDB rewrites before catalog lookup (`COALESCE`, `IF`,
//!   `IFNULL`, `GROUPING`, `COLUMNS`, `TRY`, ...).
//! - A model whose SQL does not parse.

use std::collections::HashSet;
use std::sync::LazyLock;

use sqlparser::ast::{self, Expr, Spanned, Statement, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E045, SourceSpan};
use crate::operand_check::{OperandDialect, OperandTarget};
use crate::udf::FunctionRegistry;

/// DuckDB's function names, lower-cased.
static DUCKDB_FUNCTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    include_str!("data/duckdb_functions.txt")
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .collect()
});

/// Names DuckDB accepts in call position that are not catalog functions:
/// the parser rewrites them (`COALESCE`, `IF`, `IFNULL` → `CASE` /
/// `COALESCE`), or they are special forms. Verified against DuckDB 1.5.
const DUCKDB_SPECIAL_FORMS: &[&str] = &[
    "coalesce",
    "if",
    "ifnull",
    "nullif",
    "grouping",
    "grouping_id",
    "columns",
    "unpack",
    "try",
    "row",
    "struct",
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

/// Whether DuckDB has a function called `lower_name`.
fn duckdb_knows(lower_name: &str) -> bool {
    DUCKDB_FUNCTIONS.contains(lower_name) || DUCKDB_SPECIAL_FORMS.contains(&lower_name)
}

/// Report calls to functions the target dialect does not have (E045).
///
/// `target_for` maps a model name to the warehouses it runs on, as for the
/// operand checks. Only DuckDB targets are checked; see the module docs.
pub fn check_unknown_functions(
    models: &[rocky_core::models::Model],
    registry: &FunctionRegistry,
    target_for: &dyn Fn(&str) -> OperandTarget,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for model in models {
        let runs_on_duckdb = match target_for(&model.config.name) {
            OperandTarget::Targets { dialects, .. } => dialects.contains(&OperandDialect::DuckDb),
            OperandTarget::Unconfigured => false,
        };
        if runs_on_duckdb {
            diagnostics.extend(check_model(model, registry));
        }
    }
    diagnostics
}

fn check_model(model: &rocky_core::models::Model, registry: &FunctionRegistry) -> Vec<Diagnostic> {
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, &model.sql)
    else {
        return Vec::new();
    };
    let [Statement::Query(_)] = statements.as_slice() else {
        return Vec::new();
    };
    let mut visitor = UnknownCalls {
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
    let mut seen = HashSet::new();
    let mut diagnostics = Vec::new();
    for (name, line, col) in visitor.found {
        if !seen.insert(name.to_ascii_lowercase()) {
            continue;
        }
        let (line, col) = if precise_spans && line > 0 {
            (line, col)
        } else {
            (1, 1)
        };
        diagnostics.push(
            Diagnostic::error(
                E045,
                &model.config.name,
                format!(
                    "function `{name}` does not exist in DuckDB and is not a project function \
                     in `functions/`"
                ),
            )
            .with_suggestion(suggestion(&name))
            .with_span(SourceSpan {
                file: file.clone(),
                line,
                col,
            }),
        );
    }
    diagnostics
}

/// "did you mean …" from edit distance, plus the escape hatch.
fn suggestion(name: &str) -> String {
    let wanted = name.to_ascii_lowercase();
    let threshold = (wanted.chars().count() / 3).max(1);
    let mut close: Vec<(usize, &str)> = DUCKDB_FUNCTIONS
        .iter()
        .copied()
        .chain(DUCKDB_SPECIAL_FORMS.iter().copied())
        .map(|known| (strsim::levenshtein(&wanted, known), known))
        .filter(|(distance, _)| *distance <= threshold)
        .collect();
    close.sort_unstable();
    close.dedup_by(|a, b| a.1 == b.1);
    let hatch = "If it is a macro or an extension function loaded outside Rocky, call it \
                 schema-qualified (`main.<name>(...)`), which Rocky does not check";
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

struct UnknownCalls<'r> {
    registry: &'r FunctionRegistry,
    /// `(name as written, line, column)`.
    found: Vec<(String, usize, usize)>,
}

impl Visitor for UnknownCalls<'_> {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> std::ops::ControlFlow<()> {
        if let Expr::Function(function) = expr
            && let [ast::ObjectNamePart::Identifier(ident)] = function.name.0.as_slice()
            && ident.quote_style.is_none()
        {
            let lower = ident.value.to_ascii_lowercase();
            if !duckdb_knows(&lower) && !self.registry.declares(&lower) {
                let span = expr.span();
                self.found.push((
                    ident.value.clone(),
                    span.start.line as usize,
                    span.start.column as usize,
                ));
            }
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
        check_unknown_functions(&[model("m", sql)], registry, &|_| target.clone())
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
        assert_eq!(&*diags[0].code, "E045");
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

    #[test]
    fn only_duckdb_targets_are_checked() {
        let sql = "SELECT SUMM(amount) AS s FROM t";
        let registry = FunctionRegistry::default();
        assert!(run(sql, &OperandTarget::Unconfigured, &registry).is_empty());
        for d in [
            OperandDialect::Snowflake,
            OperandDialect::Databricks,
            OperandDialect::BigQuery,
            OperandDialect::Postgres,
        ] {
            assert!(run(sql, &Some(d).into(), &registry).is_empty(), "{d:?}");
        }
        // A model that also runs on DuckDB fails there.
        let both = OperandTarget::Targets {
            dialects: vec![OperandDialect::Snowflake, OperandDialect::DuckDb],
            unruled: Vec::new(),
        };
        assert_eq!(run(sql, &both, &registry).len(), 1);
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
            assert!(duckdb_knows(name), "{name}");
        }
        assert!(!duckdb_knows("summ"));
    }
}
