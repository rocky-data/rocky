//! User-defined functions (UDFs) as first-class project resources.
//!
//! A project may ship a `functions/` directory beside its `models/`
//! directory. Each function is a pair, mirroring the model sidecar
//! convention:
//!
//! ```text
//! functions/
//! ├── cents_to_dollars.sql    # the SQL body: one scalar expression
//! └── cents_to_dollars.toml   # signature: name, typed arguments, return type
//! ```
//!
//! ```toml
//! name = "cents_to_dollars"        # optional; defaults to the file stem
//! description = "Integer cents to dollars"
//! language = "sql"                 # default; Python UDFs are refused
//! returns = "DOUBLE"
//! deterministic = true             # optional
//!
//! [[arguments]]
//! name = "cents"
//! type = "BIGINT"
//!
//! [target]                         # optional; where the function is created
//! schema = "analytics"
//! ```
//!
//! Types are written in the target warehouse's own spelling (`BIGINT` on
//! DuckDB, `INT64` on BigQuery, `NUMBER(38,0)` on Snowflake) and spliced into
//! the DDL verbatim after an allowlist check — Rocky does not translate them,
//! exactly as it does not translate model SQL.
//!
//! This module owns loading, validation and per-dialect `CREATE FUNCTION`
//! generation. Type checking and the model → function dependency edges live in
//! `rocky-compiler::udf`, which consumes [`FunctionDef`].

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use rocky_sql::literal::{LiteralEscape, encode_string_literal};
use rocky_sql::validation::{validate_gcp_project_id, validate_identifier, validate_sql_type};

/// The `functions/` directory that belongs to a models directory.
///
/// Same convention as `macros/`: a sibling of the models directory. Returns
/// `None` for an empty `models_dir` (a compile over preloaded models with no
/// directory), so a caller never resolves `../functions` against the process
/// working directory by accident.
#[must_use]
pub fn functions_dir_for(models_dir: &Path) -> Option<PathBuf> {
    if models_dir.as_os_str().is_empty() {
        return None;
    }
    Some(models_dir.join("../functions"))
}

/// One declared argument of a function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionArgument {
    /// Parameter name, referenced bare inside the body.
    pub name: String,
    /// Warehouse type, in the target dialect's spelling.
    #[serde(rename = "type")]
    pub data_type: String,
}

/// Where a function is created. Both parts optional: an unqualified function
/// lands in the session's current schema.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionTarget {
    #[serde(default)]
    pub catalog: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
}

/// The `<name>.toml` sidecar of a function.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionConfig {
    /// Function name. Defaults to the sidecar's file stem.
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    /// Body language. Only `"sql"` is supported; anything else is refused at
    /// compile time with `E051`.
    #[serde(default = "default_language")]
    pub language: String,
    /// Declared return type.
    pub returns: String,
    /// Declared arguments, in call order.
    #[serde(default, alias = "args")]
    pub arguments: Vec<FunctionArgument>,
    /// `true` → same inputs always give the same output. `None` leaves the
    /// warehouse default in place.
    #[serde(default)]
    pub deterministic: Option<bool>,
    #[serde(default)]
    pub target: FunctionTarget,
}

fn default_language() -> String {
    "sql".to_string()
}

/// A loaded function: sidecar plus body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FunctionDef {
    /// Resolved name (`config.name` or the file stem).
    pub name: String,
    pub config: FunctionConfig,
    /// The SQL body (one scalar expression), trimmed of surrounding
    /// whitespace and trailing `;`. `None` when no `<name>.sql` exists.
    pub body: Option<String>,
    /// Path of the `.toml` sidecar.
    pub file_path: PathBuf,
}

/// A function file that could not be loaded at all.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionLoadError {
    /// Best-effort name (the file stem).
    pub name: String,
    pub file_path: PathBuf,
    pub message: String,
}

/// Result of scanning a `functions/` directory.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LoadedFunctions {
    pub functions: Vec<FunctionDef>,
    pub errors: Vec<FunctionLoadError>,
}

/// Load every function under `dir` (recursively, in sorted order).
///
/// An absent directory is an empty project, not an error. Unreadable or
/// malformed files are returned in [`LoadedFunctions::errors`] rather than
/// failing the scan, so the compiler can report each one as a diagnostic.
#[must_use]
pub fn load_functions_from_dir(dir: &Path) -> LoadedFunctions {
    let mut loaded = LoadedFunctions::default();
    if !dir.is_dir() {
        return loaded;
    }
    let (dirs, walk_errors) = crate::model_walk::walk_model_dirs(dir);
    for err in walk_errors {
        loaded.errors.push(FunctionLoadError {
            name: dir.display().to_string(),
            file_path: dir.to_path_buf(),
            message: err.to_string(),
        });
    }
    for sub in dirs {
        let mut tomls: Vec<PathBuf> = match std::fs::read_dir(&sub) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .map(|e| e.path())
                .filter(|p| p.is_file() && p.extension().is_some_and(|e| e == "toml"))
                .collect(),
            Err(e) => {
                loaded.errors.push(FunctionLoadError {
                    name: sub.display().to_string(),
                    file_path: sub.clone(),
                    message: format!("failed to read functions directory: {e}"),
                });
                continue;
            }
        };
        tomls.sort();
        for toml_path in tomls {
            match load_function(&toml_path) {
                Ok(def) => loaded.functions.push(def),
                Err(err) => loaded.errors.push(err),
            }
        }
    }
    loaded
}

fn load_function(toml_path: &Path) -> Result<FunctionDef, FunctionLoadError> {
    let stem = toml_path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let fail = |message: String| FunctionLoadError {
        name: stem.clone(),
        file_path: toml_path.to_path_buf(),
        message,
    };
    let text = std::fs::read_to_string(toml_path)
        .map_err(|e| fail(format!("failed to read {}: {e}", toml_path.display())))?;
    let config: FunctionConfig = toml::from_str(&text).map_err(|e| {
        fail(format!(
            "invalid function sidecar {}: {e}",
            toml_path.display()
        ))
    })?;
    let sql_path = toml_path.with_extension("sql");
    let body = if sql_path.is_file() {
        let raw = std::fs::read_to_string(&sql_path)
            .map_err(|e| fail(format!("failed to read {}: {e}", sql_path.display())))?;
        Some(normalize_body(&raw))
    } else {
        None
    };
    Ok(FunctionDef {
        name: config.name.clone().unwrap_or(stem.clone()),
        config,
        body,
        file_path: toml_path.to_path_buf(),
    })
}

/// Trim whitespace, then any trailing `;` (and the whitespace before it).
fn normalize_body(raw: &str) -> String {
    let mut body = raw.trim();
    while let Some(stripped) = body.strip_suffix(';') {
        body = stripped.trim_end();
    }
    body.to_string()
}

/// Why a function cannot be created.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FunctionError {
    /// The definition itself is invalid (bad identifier, bad type, …).
    #[error("function '{name}': {message}")]
    Invalid { name: String, message: String },
    /// The definition is valid but this warehouse cannot create it.
    #[error("function '{name}': {message}")]
    Unsupported { name: String, message: String },
}

impl FunctionDef {
    /// Every problem that makes this definition uncreatable on any warehouse.
    ///
    /// Empty means the definition is well-formed. Dialect-specific refusals
    /// (Trino, a BigQuery function without a dataset, …) come from
    /// [`create_function_sql`] instead.
    #[must_use]
    pub fn validation_problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        let language = self.config.language.trim().to_ascii_lowercase();
        if language != "sql" {
            problems.push(if language == "python" {
                "Python UDFs are not supported; only `language = \"sql\"` functions can be \
                 declared"
                    .to_string()
            } else {
                format!(
                    "unsupported language `{}`; only `language = \"sql\"` is supported",
                    self.config.language
                )
            });
        }
        if validate_identifier(&self.name).is_err() {
            problems.push(format!(
                "function name `{}` must match [A-Za-z0-9_]+",
                self.name
            ));
        }
        if validate_sql_type(self.config.returns.trim()).is_err() {
            problems.push(format!(
                "return type `{}` is not a valid SQL type name",
                self.config.returns
            ));
        }
        let mut seen = std::collections::HashSet::new();
        for arg in &self.config.arguments {
            if validate_identifier(&arg.name).is_err() {
                problems.push(format!(
                    "argument name `{}` must match [A-Za-z0-9_]+",
                    arg.name
                ));
            }
            if !seen.insert(arg.name.to_ascii_lowercase()) {
                problems.push(format!("argument `{}` is declared twice", arg.name));
            }
            if validate_sql_type(arg.data_type.trim()).is_err() {
                problems.push(format!(
                    "argument `{}` has an invalid SQL type `{}`",
                    arg.name, arg.data_type
                ));
            }
        }
        // A catalog may also be a hyphenated GCP project id (BigQuery);
        // the non-BigQuery DDL arms re-check it as a plain identifier.
        if let Some(catalog) = &self.config.target.catalog
            && validate_identifier(catalog).is_err()
            && validate_gcp_project_id(catalog).is_err()
        {
            problems.push(format!(
                "target.catalog `{catalog}` is not a valid identifier"
            ));
        }
        if let Some(schema) = &self.config.target.schema
            && validate_identifier(schema).is_err()
        {
            problems.push(format!("target.schema `{schema}` must match [A-Za-z0-9_]+"));
        }
        if self.config.target.catalog.is_some() && self.config.target.schema.is_none() {
            problems.push("target.catalog requires target.schema".to_string());
        }
        match &self.body {
            None if language == "sql" => problems.push(format!(
                "missing SQL body: expected {}",
                self.file_path.with_extension("sql").display()
            )),
            Some(body) if body.is_empty() => problems.push("the SQL body is empty".to_string()),
            // A top-level `;` would end the CREATE statement and run the rest
            // as a second one. Only that certain case refuses; constructs the
            // scanner calls ambiguous across dialects are left alone.
            Some(body)
                if matches!(
                    rocky_sql::validation::reject_statement_terminator("function body", body),
                    Err(rocky_sql::validation::ValidationError::StatementTerminator { .. })
                ) =>
            {
                problems.push(
                    "the SQL body contains a `;`; a function body is one expression".to_string(),
                );
            }
            _ => {}
        }
        problems
    }

    /// `name(arg TYPE, ...) RETURNS TYPE` — the signature as declared.
    #[must_use]
    pub fn signature(&self) -> String {
        let args = self
            .config
            .arguments
            .iter()
            .map(|a| format!("{} {}", a.name, a.data_type.trim()))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "{}({args}) RETURNS {}",
            self.name,
            self.config.returns.trim()
        )
    }
}

/// Warehouses Rocky can (or knowingly cannot) create a function on.
///
/// The `match` in [`create_function_sql`] is exhaustive with no `_` arm, so a
/// new variant fails to compile until its DDL is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionDialect {
    DuckDb,
    Snowflake,
    Databricks,
    BigQuery,
    Postgres,
    Redshift,
    Trino,
}

impl FunctionDialect {
    /// Resolve from [`crate::traits::SqlDialect::name`]. `None` for a dialect
    /// with no known function DDL.
    #[must_use]
    pub fn from_dialect_name(name: &str) -> Option<Self> {
        match name {
            "duckdb" => Some(Self::DuckDb),
            "snowflake" => Some(Self::Snowflake),
            "databricks" => Some(Self::Databricks),
            "bigquery" => Some(Self::BigQuery),
            "postgres" => Some(Self::Postgres),
            "redshift" => Some(Self::Redshift),
            "trino" => Some(Self::Trino),
            _ => None,
        }
    }
}

/// Generate the `CREATE OR REPLACE` statement that materializes `def`.
///
/// # Errors
///
/// [`FunctionError::Invalid`] when the definition fails
/// [`FunctionDef::validation_problems`] (re-checked here so nothing
/// unvalidated is ever interpolated), and [`FunctionError::Unsupported`] when
/// the warehouse cannot create it.
pub fn create_function_sql(
    def: &FunctionDef,
    dialect: FunctionDialect,
) -> Result<String, FunctionError> {
    let invalid = |message: String| FunctionError::Invalid {
        name: def.name.clone(),
        message,
    };
    let unsupported = |message: String| FunctionError::Unsupported {
        name: def.name.clone(),
        message,
    };
    let problems = def.validation_problems();
    if !problems.is_empty() {
        return Err(invalid(problems.join("; ")));
    }
    // `validation_problems` guarantees a non-empty body for SQL functions.
    let body = def.body.as_deref().unwrap_or_default();
    let returns = def.config.returns.trim();
    let typed_args = || {
        def.config
            .arguments
            .iter()
            .map(|a| format!("{} {}", a.name, a.data_type.trim()))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let dotted = |quote: fn(&str) -> String| {
        let mut parts = Vec::new();
        if let Some(c) = &def.config.target.catalog {
            parts.push(quote(c));
        }
        if let Some(s) = &def.config.target.schema {
            parts.push(quote(s));
        }
        parts.push(quote(&def.name));
        parts.join(".")
    };
    let comment = |rule: LiteralEscape| {
        def.config
            .description
            .as_deref()
            .filter(|d| !d.trim().is_empty())
            .map(|d| encode_string_literal(rule, d))
    };

    let plain_catalog = || match &def.config.target.catalog {
        Some(c) if validate_identifier(c).is_err() => Err(invalid(format!(
            "target.catalog `{c}` must match [A-Za-z0-9_]+ on this warehouse"
        ))),
        _ => Ok(()),
    };

    match dialect {
        // The body is closed on a new line in the DuckDB and BigQuery arms so a
        // trailing `-- comment` in it cannot swallow the closing paren.
        //
        // DuckDB scalar macro:
        // https://duckdb.org/docs/stable/sql/statements/create_macro
        // Macro parameters are untyped and the macro has no RETURNS clause, so
        // the body is wrapped in a CAST to the declared return type. That makes
        // the type Rocky's compiler propagates the type the warehouse produces.
        FunctionDialect::DuckDb => {
            plain_catalog()?;
            let params = def
                .config
                .arguments
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!(
                "CREATE OR REPLACE MACRO {}({params}) AS CAST(({body}\n) AS {returns})",
                dotted(ToString::to_string)
            ))
        }
        // Snowflake SQL UDF:
        // https://docs.snowflake.com/en/sql-reference/sql/create-function
        // `CREATE [ OR REPLACE ] FUNCTION <name> ( [ <arg> <type> ] [ , ... ] )
        //  RETURNS <type> LANGUAGE SQL [ { VOLATILE | IMMUTABLE } ]
        //  [ COMMENT = '<string>' ] AS '<function_definition>'`
        // The definition is dollar-quoted, so a body containing `$$` cannot be
        // delimited and is refused.
        FunctionDialect::Snowflake => {
            plain_catalog()?;
            if body.contains("$$") {
                return Err(invalid(
                    "the body contains `$$`, which cannot appear inside Snowflake's \
                     dollar-quoted function definition"
                        .to_string(),
                ));
            }
            let mut sql = format!(
                "CREATE OR REPLACE FUNCTION {}({})\n  RETURNS {returns}\n  LANGUAGE SQL",
                dotted(ToString::to_string),
                typed_args()
            );
            match def.config.deterministic {
                Some(true) => sql.push_str("\n  IMMUTABLE"),
                Some(false) => sql.push_str("\n  VOLATILE"),
                None => {}
            }
            if let Some(c) = comment(LiteralEscape::Backslash) {
                sql.push_str(&format!("\n  COMMENT = {c}"));
            }
            sql.push_str(&format!("\n  AS\n$$\n{body}\n$$"));
            Ok(sql)
        }
        // Databricks SQL UDF:
        // https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-sql-function.html
        // `CREATE [OR REPLACE] FUNCTION name ( [ param type [, ...] ] )
        //  RETURNS type [ characteristic [...] ] RETURN expression`, where a
        // characteristic is `LANGUAGE SQL | [NOT] DETERMINISTIC | COMMENT '...'`.
        FunctionDialect::Databricks => {
            plain_catalog()?;
            let mut sql = format!(
                "CREATE OR REPLACE FUNCTION {}({})\n  RETURNS {returns}\n  LANGUAGE SQL",
                dotted(ToString::to_string),
                typed_args()
            );
            match def.config.deterministic {
                Some(true) => sql.push_str("\n  DETERMINISTIC"),
                Some(false) => sql.push_str("\n  NOT DETERMINISTIC"),
                None => {}
            }
            if let Some(c) = comment(LiteralEscape::Backslash) {
                sql.push_str(&format!("\n  COMMENT {c}"));
            }
            sql.push_str(&format!("\n  RETURN {body}"));
            Ok(sql)
        }
        // BigQuery SQL UDF:
        // https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#create_function_statement
        // `CREATE [OR REPLACE] FUNCTION [[project.]dataset.]name ([param type[, ...]])
        //  [RETURNS type] AS (sql_expression) [OPTIONS (description = '...')]`.
        // A persistent function needs a dataset. SQL UDFs take no determinism
        // clause (only JavaScript UDFs do), so `deterministic` is not emitted.
        FunctionDialect::BigQuery => {
            if def.config.target.schema.is_none() {
                return Err(unsupported(
                    "BigQuery persistent functions need a dataset: set `[target] schema` \
                     (and optionally `catalog` for the project)"
                        .to_string(),
                ));
            }
            let mut sql = format!(
                "CREATE OR REPLACE FUNCTION {}({})\nRETURNS {returns}\nAS ({body}\n)",
                dotted(|s| format!("`{s}`")),
                typed_args()
            );
            if let Some(c) = comment(LiteralEscape::Backslash) {
                sql.push_str(&format!("\nOPTIONS (description = {c})"));
            }
            Ok(sql)
        }
        // PostgreSQL SQL-language function:
        // https://www.postgresql.org/docs/current/sql-createfunction.html
        // `CREATE [ OR REPLACE ] FUNCTION name ( [ argname argtype [, ...] ] )
        //  RETURNS rettype LANGUAGE sql { IMMUTABLE | STABLE | VOLATILE }
        //  AS 'definition'`. Arguments are referenced by name in the body
        // (https://www.postgresql.org/docs/current/xfunc-sql.html, since 9.2).
        // The definition is dollar-quoted with a Rocky-specific tag, so only a
        // body containing that tag is refused. Without `deterministic` the
        // warehouse default (VOLATILE) stands. A description would need a
        // separate `COMMENT ON FUNCTION` statement, so it is not emitted.
        FunctionDialect::Postgres => {
            plain_catalog()?;
            const TAG: &str = "$rocky$";
            if body.contains(TAG) {
                return Err(invalid(format!(
                    "the body contains `{TAG}`, which cannot appear inside the \
                     dollar-quoted function definition"
                )));
            }
            let mut sql = format!(
                "CREATE OR REPLACE FUNCTION {}({})\n  RETURNS {returns}\n  LANGUAGE sql",
                dotted(ToString::to_string),
                typed_args()
            );
            match def.config.deterministic {
                Some(true) => sql.push_str("\n  IMMUTABLE"),
                Some(false) => sql.push_str("\n  VOLATILE"),
                None => {}
            }
            sql.push_str(&format!("\n  AS {TAG}\nSELECT {body}\n{TAG}"));
            Ok(sql)
        }
        // Redshift scalar SQL UDF:
        // https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_FUNCTION.html
        // `CREATE [ OR REPLACE ] FUNCTION f_function_name ( [sql_arg_data_type [, ...]] )
        //  RETURNS data_type { VOLATILE | STABLE | IMMUTABLE }
        //  AS $$ SELECT_clause $$ LANGUAGE sql`.
        // Arguments are unnamed and the body references them as `$1`, `$2`, …
        // (https://docs.aws.amazon.com/redshift/latest/dg/udf-creating-a-scalar-sql-udf.html),
        // so named references are rewritten through the parsed expression.
        // The volatility clause is required; without `deterministic` Rocky
        // emits VOLATILE, the clause that promises nothing.
        FunctionDialect::Redshift => {
            plain_catalog()?;
            if def.config.target.catalog.is_some() {
                return Err(unsupported(
                    "Redshift functions are created in the connected database; drop \
                     `[target] catalog` and keep `schema`"
                        .to_string(),
                ));
            }
            let params: Vec<&str> = def
                .config
                .arguments
                .iter()
                .map(|a| a.name.as_str())
                .collect();
            let positional = rocky_sql::udf_body::positional_params(body, &params)
                .map_err(|e| invalid(format!("cannot render the body for Redshift: {e}")))?;
            if positional.contains("$$") {
                return Err(invalid(
                    "the body contains `$$`, which cannot appear inside Redshift's \
                     dollar-quoted function definition"
                        .to_string(),
                ));
            }
            let types = def
                .config
                .arguments
                .iter()
                .map(|a| a.data_type.trim())
                .collect::<Vec<_>>()
                .join(", ");
            let volatility = match def.config.deterministic {
                Some(true) => "IMMUTABLE",
                Some(false) | None => "VOLATILE",
            };
            Ok(format!(
                "CREATE OR REPLACE FUNCTION {}({types})\n  RETURNS {returns}\n  {volatility}\n  \
                 AS $$\nSELECT {positional}\n$$ LANGUAGE sql",
                dotted(ToString::to_string)
            ))
        }
        // Trino stores SQL routines only in connectors that implement routine
        // storage (https://trino.io/docs/current/udf/sql.html), and Rocky's
        // Trino adapter does not manage that. Refuse rather than emit DDL the
        // catalog may reject.
        FunctionDialect::Trino => Err(unsupported(
            "the Trino adapter does not support creating persistent user-defined \
             functions; inline the expression or create the routine outside Rocky"
                .to_string(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn def(name: &str, args: &[(&str, &str)], returns: &str, body: &str) -> FunctionDef {
        FunctionDef {
            name: name.to_string(),
            config: FunctionConfig {
                name: None,
                description: None,
                language: "sql".to_string(),
                returns: returns.to_string(),
                arguments: args
                    .iter()
                    .map(|(n, t)| FunctionArgument {
                        name: n.to_string(),
                        data_type: t.to_string(),
                    })
                    .collect(),
                deterministic: None,
                target: FunctionTarget::default(),
            },
            body: Some(body.to_string()),
            file_path: PathBuf::from(format!("functions/{name}.toml")),
        }
    }

    fn cents() -> FunctionDef {
        def(
            "cents_to_dollars",
            &[("cents", "BIGINT")],
            "DOUBLE",
            "cents / 100.0",
        )
    }

    #[test]
    fn duckdb_macro_casts_to_the_declared_return_type() {
        assert_eq!(
            create_function_sql(&cents(), FunctionDialect::DuckDb).unwrap(),
            "CREATE OR REPLACE MACRO cents_to_dollars(cents) AS CAST((cents / 100.0\n) AS DOUBLE)"
        );
    }

    #[test]
    fn duckdb_macro_with_schema_and_two_args() {
        let mut d = def(
            "safe_div",
            &[("a", "DOUBLE"), ("b", "DOUBLE")],
            "DOUBLE",
            "CASE WHEN b = 0 THEN NULL ELSE a / b END",
        );
        d.config.target.schema = Some("util".to_string());
        assert_eq!(
            create_function_sql(&d, FunctionDialect::DuckDb).unwrap(),
            "CREATE OR REPLACE MACRO util.safe_div(a, b) AS \
             CAST((CASE WHEN b = 0 THEN NULL ELSE a / b END\n) AS DOUBLE)"
        );
    }

    #[test]
    fn snowflake_function_syntax() {
        let mut d = def(
            "cents_to_dollars",
            &[("cents", "NUMBER(38,0)")],
            "FLOAT",
            "cents / 100.0",
        );
        d.config.target = FunctionTarget {
            catalog: Some("analytics".to_string()),
            schema: Some("util".to_string()),
        };
        d.config.deterministic = Some(true);
        d.config.description = Some("Cents to dollars, it's simple".to_string());
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Snowflake).unwrap(),
            "CREATE OR REPLACE FUNCTION analytics.util.cents_to_dollars(cents NUMBER(38,0))\n  \
             RETURNS FLOAT\n  LANGUAGE SQL\n  IMMUTABLE\n  \
             COMMENT = 'Cents to dollars, it\\'s simple'\n  AS\n$$\ncents / 100.0\n$$"
        );
        d.config.deterministic = Some(false);
        d.config.description = None;
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Snowflake).unwrap(),
            "CREATE OR REPLACE FUNCTION analytics.util.cents_to_dollars(cents NUMBER(38,0))\n  \
             RETURNS FLOAT\n  LANGUAGE SQL\n  VOLATILE\n  AS\n$$\ncents / 100.0\n$$"
        );
    }

    #[test]
    fn snowflake_refuses_a_body_with_dollar_quotes() {
        let d = def("f", &[], "VARCHAR", "'$$'");
        assert!(matches!(
            create_function_sql(&d, FunctionDialect::Snowflake),
            Err(FunctionError::Invalid { .. })
        ));
    }

    #[test]
    fn databricks_function_syntax() {
        let mut d = cents();
        d.config.target = FunctionTarget {
            catalog: Some("main".to_string()),
            schema: Some("util".to_string()),
        };
        d.config.deterministic = Some(true);
        d.config.description = Some("Cents to dollars".to_string());
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Databricks).unwrap(),
            "CREATE OR REPLACE FUNCTION main.util.cents_to_dollars(cents BIGINT)\n  \
             RETURNS DOUBLE\n  LANGUAGE SQL\n  DETERMINISTIC\n  \
             COMMENT 'Cents to dollars'\n  RETURN cents / 100.0"
        );
        d.config.deterministic = Some(false);
        d.config.description = None;
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Databricks).unwrap(),
            "CREATE OR REPLACE FUNCTION main.util.cents_to_dollars(cents BIGINT)\n  \
             RETURNS DOUBLE\n  LANGUAGE SQL\n  NOT DETERMINISTIC\n  RETURN cents / 100.0"
        );
    }

    #[test]
    fn bigquery_function_syntax() {
        let mut d = def(
            "cents_to_dollars",
            &[("cents", "INT64")],
            "FLOAT64",
            "cents / 100.0",
        );
        d.config.target = FunctionTarget {
            catalog: Some("my-project".to_string()),
            schema: Some("util".to_string()),
        };
        d.config.deterministic = Some(true);
        d.config.description = Some("Cents to dollars".to_string());
        assert_eq!(
            create_function_sql(&d, FunctionDialect::BigQuery).unwrap(),
            "CREATE OR REPLACE FUNCTION `my-project`.`util`.`cents_to_dollars`(cents INT64)\n\
             RETURNS FLOAT64\nAS (cents / 100.0\n)\nOPTIONS (description = 'Cents to dollars')"
        );
    }

    #[test]
    fn bigquery_requires_a_dataset() {
        let d = def("f", &[("x", "INT64")], "INT64", "x");
        assert!(matches!(
            create_function_sql(&d, FunctionDialect::BigQuery),
            Err(FunctionError::Unsupported { .. })
        ));
    }

    #[test]
    fn postgres_function_syntax() {
        let mut d = def(
            "safe_div",
            &[("a", "NUMERIC"), ("b", "NUMERIC")],
            "NUMERIC",
            "CASE WHEN b = 0 THEN NULL ELSE a / b END",
        );
        d.config.target.schema = Some("util".to_string());
        d.config.deterministic = Some(true);
        d.config.description = Some("not emitted".to_string());
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Postgres).unwrap(),
            "CREATE OR REPLACE FUNCTION util.safe_div(a NUMERIC, b NUMERIC)\n  \
             RETURNS NUMERIC\n  LANGUAGE sql\n  IMMUTABLE\n  \
             AS $rocky$\nSELECT CASE WHEN b = 0 THEN NULL ELSE a / b END\n$rocky$"
        );
        d.config.deterministic = Some(false);
        assert!(
            create_function_sql(&d, FunctionDialect::Postgres)
                .unwrap()
                .contains("\n  VOLATILE\n")
        );
        d.config.deterministic = None;
        let sql = create_function_sql(&d, FunctionDialect::Postgres).unwrap();
        assert!(
            !sql.contains("VOLATILE") && !sql.contains("IMMUTABLE"),
            "{sql}"
        );
    }

    #[test]
    fn postgres_dollar_quotes_without_the_rocky_tag_are_fine() {
        let d = def("f", &[], "TEXT", "'$$' || '$tag$'");
        assert!(create_function_sql(&d, FunctionDialect::Postgres).is_ok());
        let tagged = def("g", &[], "TEXT", "'$rocky$'");
        assert!(matches!(
            create_function_sql(&tagged, FunctionDialect::Postgres),
            Err(FunctionError::Invalid { .. })
        ));
    }

    #[test]
    fn postgres_trailing_comment_cannot_swallow_the_closing_tag() {
        let d = def("h", &[("x", "BIGINT")], "BIGINT", "x -- note");
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Postgres).unwrap(),
            "CREATE OR REPLACE FUNCTION h(x BIGINT)\n  RETURNS BIGINT\n  LANGUAGE sql\n  \
             AS $rocky$\nSELECT x -- note\n$rocky$"
        );
    }

    #[test]
    fn redshift_function_uses_positional_arguments() {
        let mut d = def(
            "f_safe_div",
            &[("a", "FLOAT8"), ("b", "FLOAT8")],
            "FLOAT8",
            "CASE WHEN b = 0 THEN NULL ELSE a / b END",
        );
        d.config.target.schema = Some("util".to_string());
        d.config.deterministic = Some(true);
        assert_eq!(
            create_function_sql(&d, FunctionDialect::Redshift).unwrap(),
            "CREATE OR REPLACE FUNCTION util.f_safe_div(FLOAT8, FLOAT8)\n  \
             RETURNS FLOAT8\n  IMMUTABLE\n  \
             AS $$\nSELECT CASE WHEN $2 = 0 THEN NULL ELSE $1 / $2 END\n$$ LANGUAGE sql"
        );
        d.config.deterministic = None;
        assert!(
            create_function_sql(&d, FunctionDialect::Redshift)
                .unwrap()
                .contains("\n  VOLATILE\n")
        );
    }

    #[test]
    fn redshift_leaves_literals_alone_and_refuses_what_it_cannot_render() {
        let d = def("f", &[("x", "VARCHAR")], "VARCHAR", "x || 'x'");
        assert!(
            create_function_sql(&d, FunctionDialect::Redshift)
                .unwrap()
                .contains("SELECT $1 || 'x'\n")
        );
        let dollars = def("g", &[], "VARCHAR", "'$$'");
        assert!(matches!(
            create_function_sql(&dollars, FunctionDialect::Redshift),
            Err(FunctionError::Invalid { .. })
        ));
        let qualified = def("h", &[("p", "SUPER")], "VARCHAR", "p.field");
        assert!(matches!(
            create_function_sql(&qualified, FunctionDialect::Redshift),
            Err(FunctionError::Invalid { .. })
        ));
        let mut cataloged = def("k", &[], "INT", "1");
        cataloged.config.target = FunctionTarget {
            catalog: Some("dev".to_string()),
            schema: Some("util".to_string()),
        };
        assert!(matches!(
            create_function_sql(&cataloged, FunctionDialect::Redshift),
            Err(FunctionError::Unsupported { .. })
        ));
    }

    #[test]
    fn trino_is_refused() {
        let err = create_function_sql(&cents(), FunctionDialect::Trino).unwrap_err();
        assert!(matches!(err, FunctionError::Unsupported { .. }));
        assert!(err.to_string().contains("Trino"));
    }

    #[test]
    fn invalid_definitions_never_reach_the_ddl() {
        let mut bad_type = cents();
        bad_type.config.returns = "DOUBLE); DROP TABLE x; --".to_string();
        let mut bad_name = cents();
        bad_name.name = "drop table".to_string();
        let mut python = cents();
        python.config.language = "python".to_string();
        for d in [bad_type, bad_name, python] {
            for dialect in [
                FunctionDialect::DuckDb,
                FunctionDialect::Snowflake,
                FunctionDialect::Databricks,
                FunctionDialect::BigQuery,
                FunctionDialect::Postgres,
                FunctionDialect::Redshift,
            ] {
                assert!(matches!(
                    create_function_sql(&d, dialect),
                    Err(FunctionError::Invalid { .. })
                ));
            }
        }
    }

    #[test]
    fn a_statement_terminator_in_the_body_is_refused() {
        let d = def("f", &[], "BIGINT", "1); DROP TABLE t; SELECT (1");
        assert!(
            d.validation_problems()
                .iter()
                .any(|p| p.contains("contains a `;`"))
        );
        // A `;` inside a string literal is data, not a terminator.
        let ok = def("g", &[], "VARCHAR", "'a;b'");
        assert!(ok.validation_problems().is_empty());
        // A trailing comment cannot comment out the closing paren.
        let commented = def("h", &[("x", "BIGINT")], "BIGINT", "x -- note");
        assert_eq!(
            create_function_sql(&commented, FunctionDialect::DuckDb).unwrap(),
            "CREATE OR REPLACE MACRO h(x) AS CAST((x -- note\n) AS BIGINT)"
        );
    }

    #[test]
    fn python_language_is_named_in_the_problem() {
        let mut d = cents();
        d.config.language = "python".to_string();
        let problems = d.validation_problems();
        assert_eq!(problems.len(), 1, "{problems:?}");
        assert!(problems[0].contains("Python UDFs are not supported"));
    }

    #[test]
    fn loads_a_function_pair_and_normalizes_the_body() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(
            dir.join("cents_to_dollars.toml"),
            "returns = \"DOUBLE\"\ndeterministic = true\n\n[[arguments]]\nname = \"cents\"\n\
             type = \"BIGINT\"\n",
        )
        .unwrap();
        std::fs::write(dir.join("cents_to_dollars.sql"), "\n  cents / 100.0 ;\n").unwrap();
        std::fs::write(dir.join("broken.toml"), "returns = 1\n").unwrap();
        let loaded = load_functions_from_dir(dir);
        assert_eq!(loaded.functions.len(), 1);
        let f = &loaded.functions[0];
        assert_eq!(f.name, "cents_to_dollars");
        assert_eq!(f.body.as_deref(), Some("cents / 100.0"));
        assert_eq!(f.config.deterministic, Some(true));
        assert_eq!(
            f.signature(),
            "cents_to_dollars(cents BIGINT) RETURNS DOUBLE"
        );
        assert!(f.validation_problems().is_empty());
        assert_eq!(loaded.errors.len(), 1);
        assert_eq!(loaded.errors[0].name, "broken");
    }

    #[test]
    fn absent_directory_is_empty_and_empty_models_dir_has_none() {
        let loaded = load_functions_from_dir(Path::new("/definitely/not/here"));
        assert!(loaded.functions.is_empty() && loaded.errors.is_empty());
        assert!(functions_dir_for(Path::new("")).is_none());
        assert_eq!(
            functions_dir_for(Path::new("proj/models")).unwrap(),
            PathBuf::from("proj/models/../functions")
        );
    }

    #[test]
    fn dialect_names_resolve() {
        assert_eq!(
            FunctionDialect::from_dialect_name("duckdb"),
            Some(FunctionDialect::DuckDb)
        );
        assert_eq!(
            FunctionDialect::from_dialect_name("postgres"),
            Some(FunctionDialect::Postgres)
        );
        assert_eq!(
            FunctionDialect::from_dialect_name("redshift"),
            Some(FunctionDialect::Redshift)
        );
        assert_eq!(FunctionDialect::from_dialect_name("unknown"), None);
    }
}
