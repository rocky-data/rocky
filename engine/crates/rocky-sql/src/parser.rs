use sqlparser::ast::Statement;
use sqlparser::parser::Parser;
use thiserror::Error;

use crate::dialect::DatabricksDialect;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("SQL parse error: {0}")]
    SqlParser(#[from] sqlparser::parser::ParserError),

    #[error("expected exactly one statement, got {0}")]
    MultipleStatements(usize),

    #[error("empty SQL input")]
    EmptyInput,
}

/// A SQL parse error enriched with the original source text and file path,
/// ready for miette rendering with source spans.
#[derive(Debug, Error, miette::Diagnostic)]
#[error("{message}")]
pub struct RichParseError {
    /// Human-readable summary.
    pub message: String,

    /// The SQL source text that failed to parse.
    #[source_code]
    pub src: miette::NamedSource<String>,

    /// Span pointing at the error location.
    #[label("here")]
    pub span: Option<miette::SourceSpan>,

    /// Actionable suggestion.
    #[help]
    pub help: Option<String>,
}

impl ParseError {
    /// Convert into a miette-compatible diagnostic that carries the
    /// original source text and (where possible) the error position.
    pub fn into_rich(self, source: &str, file: &str) -> RichParseError {
        let named = miette::NamedSource::new(file, source.to_string());

        match &self {
            ParseError::SqlParser(e) => {
                // sqlparser error messages often look like:
                //   "Expected: ..., found: ... at Line: N, Column M"
                let (span, help) = parse_sqlparser_location(e, source);
                RichParseError {
                    message: format!("{e}"),
                    src: named,
                    span,
                    help,
                }
            }
            ParseError::MultipleStatements(n) => RichParseError {
                message: format!("expected exactly one statement, got {n}"),
                src: named,
                span: None,
                help: Some("Rocky model files must contain a single SELECT statement".to_string()),
            },
            ParseError::EmptyInput => RichParseError {
                message: "empty SQL input".to_string(),
                src: named,
                span: None,
                help: Some("Add a SELECT statement to the model file".to_string()),
            },
        }
    }
}

/// Errors from [`isolate_cte`].
#[derive(Debug, Error)]
pub enum CteError {
    /// The SQL didn't parse as a single query statement.
    #[error(transparent)]
    Parse(#[from] ParseError),

    /// `cte_name` was not a valid SQL identifier.
    #[error("invalid CTE name: {0}")]
    InvalidName(String),

    /// The query has no `WITH` clause.
    #[error("query has no CTEs (no WITH clause)")]
    NoCtes,

    /// No CTE in the query matched `cte_name`.
    #[error("no CTE named '{0}' in query")]
    NotFound(String),
}

/// Rewrite a query so a single named CTE can be executed in isolation.
///
/// Given `WITH a AS (...), b AS (...) SELECT ...`, returns
/// `WITH a AS (...), b AS (...)\nSELECT * FROM <cte_name>` — the full `WITH`
/// clause is preserved (unreferenced sibling CTEs are harmless) and only the
/// trailing query is replaced. Used by `rocky preview rows --cte` to sample a
/// single CTE's output.
///
/// The CTE reference is emitted **unquoted**, which is safe because
/// `cte_name` is validated against `^[A-Za-z0-9_]+$` first. Quoted /
/// case-sensitive CTE names are matched on their bare value and are not yet
/// re-quoted on output (a known limitation).
///
/// # Errors
///
/// Returns [`CteError::InvalidName`] when `cte_name` isn't a valid identifier,
/// [`CteError::Parse`] when `sql` isn't a single query, [`CteError::NoCtes`]
/// when there's no `WITH` clause, and [`CteError::NotFound`] when no CTE
/// matches `cte_name`.
pub fn isolate_cte(sql: &str, cte_name: &str) -> Result<String, CteError> {
    crate::validation::validate_identifier(cte_name)
        .map_err(|_| CteError::InvalidName(cte_name.to_string()))?;

    let Statement::Query(query) = parse_single_statement(sql)? else {
        return Err(CteError::NoCtes);
    };

    let Some(with) = &query.with else {
        return Err(CteError::NoCtes);
    };

    if !with
        .cte_tables
        .iter()
        .any(|cte| cte.alias.name.value == cte_name)
    {
        return Err(CteError::NotFound(cte_name.to_string()));
    }

    Ok(format!("{with}\nSELECT * FROM {cte_name}"))
}

/// Extract line:col from a sqlparser error message and convert to a byte offset.
///
/// sqlparser formats errors as "... at Line: N, Column M" where N and M are
/// 1-based. We convert to a byte offset into the source string so miette can
/// underline the exact position.
fn parse_sqlparser_location(
    err: &sqlparser::parser::ParserError,
    source: &str,
) -> (Option<miette::SourceSpan>, Option<String>) {
    let msg = err.to_string();

    // Pattern: "at Line: N, Column M" (sqlparser 0.40+)
    if let Some(pos) = msg.find("at Line: ") {
        let tail = &msg[pos + 9..]; // skip "at Line: "
        if let Some(comma) = tail.find(", Column ") {
            let line_str = &tail[..comma];
            let col_str = tail[comma + 9..].trim_end_matches(|c: char| !c.is_ascii_digit());
            if let (Ok(line), Ok(col)) = (line_str.parse::<usize>(), col_str.parse::<usize>())
                && let Some(offset) = line_col_to_offset(source, line, col)
            {
                // Span length: highlight up to end of the token or line.
                let rest = &source[offset..];
                let len = rest
                    .find(|c: char| c.is_whitespace())
                    .unwrap_or(rest.len())
                    .max(1);
                return (
                    Some(miette::SourceSpan::new(offset.into(), len)),
                    Some("Check SQL syntax near this position".to_string()),
                );
            }
        }
    }

    (None, None)
}

/// Convert 1-based line and column to a byte offset.
fn line_col_to_offset(source: &str, line: usize, col: usize) -> Option<usize> {
    let mut current_line = 1;
    let mut line_start = 0;
    for (i, ch) in source.char_indices() {
        if current_line == line {
            let offset = line_start + col.saturating_sub(1);
            return if offset <= source.len() {
                Some(offset)
            } else {
                None
            };
        }
        if ch == '\n' {
            current_line += 1;
            line_start = i + 1;
        }
    }
    // Last line (no trailing newline)
    if current_line == line {
        let offset = line_start + col.saturating_sub(1);
        return if offset <= source.len() {
            Some(offset)
        } else {
            None
        };
    }
    None
}

/// Parses a SQL string using the Databricks dialect.
pub fn parse_sql(sql: &str) -> Result<Vec<Statement>, ParseError> {
    let dialect = DatabricksDialect;
    let statements = Parser::parse_sql(&dialect, sql)?;
    if statements.is_empty() {
        return Err(ParseError::EmptyInput);
    }
    Ok(statements)
}

/// Parses a SQL string expecting exactly one statement.
pub fn parse_single_statement(sql: &str) -> Result<Statement, ParseError> {
    let mut statements = parse_sql(sql)?;
    if statements.len() != 1 {
        return Err(ParseError::MultipleStatements(statements.len()));
    }
    // Length is guaranteed to be 1 by the check above; `swap_remove(0)` gives
    // us the Statement without an `unwrap()` on the iterator.
    Ok(statements.swap_remove(0))
}

/// Returns `true` iff `sql` parses as exactly one `SELECT`/query statement.
///
/// Used to reject non-`SELECT` model bodies (DDL, multi-statement, INSERT)
/// before they're executed as a preview. Keeps the `sqlparser` AST dependency
/// inside `rocky-sql`.
pub fn is_single_select(sql: &str) -> bool {
    matches!(parse_single_statement(sql), Ok(Statement::Query(_)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_select() {
        let sql = "SELECT * FROM my_catalog.my_schema.my_table WHERE id > 10";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_single() {
        let sql = "SELECT 1";
        let stmt = parse_single_statement(sql).unwrap();
        assert!(matches!(stmt, Statement::Query(_)));
    }

    #[test]
    fn test_parse_multiple_rejects() {
        let sql = "SELECT 1; SELECT 2";
        let result = parse_single_statement(sql);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_create_table() {
        let sql = "CREATE TABLE IF NOT EXISTS my_catalog.my_schema.my_table (id INT, name STRING)";
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_incremental_select() {
        let sql = r#"
            SELECT *, CAST(NULL AS STRING) AS _loaded_by
            FROM source_catalog.source_schema.my_table
            WHERE _fivetran_synced > (
                SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
                FROM target_catalog.target_schema.my_table
            )
        "#;
        let stmts = parse_sql(sql).unwrap();
        assert_eq!(stmts.len(), 1);
    }

    #[test]
    fn test_parse_empty() {
        let result = parse_sql("");
        assert!(result.is_err());
    }

    #[test]
    fn test_rich_error_carries_source() {
        let sql = "SELCT * FROM foo";
        let err = parse_sql(sql).unwrap_err();
        let rich = err.into_rich(sql, "models/broken.sql");
        // Should have a message and source
        assert!(!rich.message.is_empty());
    }

    #[test]
    fn test_line_col_to_offset_basic() {
        let src = "line1\nline2\nline3";
        assert_eq!(line_col_to_offset(src, 1, 1), Some(0));
        assert_eq!(line_col_to_offset(src, 2, 1), Some(6));
        assert_eq!(line_col_to_offset(src, 3, 1), Some(12));
    }

    #[test]
    fn test_multiple_statements_rich() {
        let sql = "SELECT 1; SELECT 2";
        let err = parse_single_statement(sql).unwrap_err();
        let rich = err.into_rich(sql, "models/multi.sql");
        assert!(rich.help.is_some());
    }

    #[test]
    fn test_isolate_cte_basic() {
        let sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM b";
        let isolated = isolate_cte(sql, "a").unwrap();
        assert!(isolated.to_uppercase().contains("WITH"));
        assert!(isolated.contains("SELECT * FROM a"));
        // Both CTE definitions are preserved; only the trailing query changes.
        assert!(isolated.contains("AS (SELECT 1 AS x)") || isolated.contains("AS (SELECT 1 AS X)"));
        // The rewritten query must itself parse.
        parse_single_statement(&isolated).expect("isolated CTE query should parse");
    }

    #[test]
    fn test_isolate_cte_second() {
        let sql = "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS y) SELECT * FROM a";
        let isolated = isolate_cte(sql, "b").unwrap();
        assert!(isolated.trim_end().ends_with("SELECT * FROM b"));
        parse_single_statement(&isolated).unwrap();
    }

    #[test]
    fn test_isolate_cte_unknown_name() {
        let sql = "WITH a AS (SELECT 1 AS x) SELECT * FROM a";
        assert!(matches!(
            isolate_cte(sql, "missing"),
            Err(CteError::NotFound(_))
        ));
    }

    #[test]
    fn test_isolate_cte_no_with_clause() {
        let sql = "SELECT * FROM some_table";
        assert!(matches!(isolate_cte(sql, "a"), Err(CteError::NoCtes)));
    }

    #[test]
    fn test_isolate_cte_invalid_name_rejected() {
        let sql = "WITH a AS (SELECT 1 AS x) SELECT * FROM a";
        // Identifier validation runs before parsing, so an injection attempt
        // never reaches the SQL.
        assert!(matches!(
            isolate_cte(sql, "a; DROP TABLE users"),
            Err(CteError::InvalidName(_))
        ));
    }

    #[test]
    fn test_isolate_cte_rejects_multi_statement() {
        let sql = "WITH a AS (SELECT 1) SELECT * FROM a; SELECT 2";
        assert!(matches!(isolate_cte(sql, "a"), Err(CteError::Parse(_))));
    }

    #[test]
    fn test_is_single_select() {
        assert!(is_single_select("SELECT 1"));
        assert!(is_single_select(
            "WITH a AS (SELECT 1 AS x) SELECT * FROM a"
        ));
        assert!(!is_single_select("CREATE TABLE t (id INT)"));
        assert!(!is_single_select("SELECT 1; SELECT 2"));
        assert!(!is_single_select(""));
    }
}
