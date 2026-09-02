use regex::Regex;
use std::sync::LazyLock;
use thiserror::Error;

/// Pattern for valid SQL identifiers (catalogs, schemas, tables, clients, etc.)
static SQL_IDENTIFIER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_]+$").unwrap());

/// Pattern for valid principal names (for GRANT/REVOKE statements)
static PRINCIPAL_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_ \-\.@]+$").unwrap());

/// Pattern for valid GCP project IDs.
///
/// GCP requires project IDs to be 6–30 chars, lowercase alphanumeric +
/// hyphens, starting with a letter and not ending in a hyphen. The
/// hyphen is what makes the stricter [`SQL_IDENTIFIER_RE`] reject them
/// (e.g. `my-gcp-project-123`).
static GCP_PROJECT_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$").unwrap());

/// Errors from SQL identifier and principal name validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid SQL identifier '{value}': must match [a-zA-Z0-9_]+")]
    InvalidIdentifier { value: String },

    #[error("invalid principal name '{value}': must match [a-zA-Z0-9_ \\-\\.@]+")]
    InvalidPrincipal { value: String },

    #[error(
        "invalid GCP project ID '{value}': must be 6-30 chars, start with a letter, \
         contain only lowercase letters / digits / hyphens, and not end with a hyphen"
    )]
    InvalidGcpProjectId { value: String },

    #[error("SQL identifier cannot be empty")]
    EmptyIdentifier,

    #[error("principal name cannot be empty")]
    EmptyPrincipal,

    #[error(
        "{context}: SQL fragment contains a statement terminator ';' outside a string literal, \
         a quoted identifier or a comment. A fragment is one expression, not a statement — \
         remove the ';' (including a trailing one)"
    )]
    StatementTerminator { context: String },

    #[error(
        "{context}: SQL fragment ends inside an unterminated {region}. Close it — an unbalanced \
         quote or comment changes how the rest of the generated statement is read"
    )]
    UnterminatedSqlRegion {
        context: String,
        region: &'static str,
    },
}

/// Validates a SQL identifier (catalog, schema, table, column names).
///
/// Rejects anything that doesn't match `^[a-zA-Z0-9_]+$`.
/// Never use `format!()` to build SQL with untrusted input — validate first.
pub fn validate_identifier(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyIdentifier);
    }
    if !SQL_IDENTIFIER_RE.is_match(value) {
        return Err(ValidationError::InvalidIdentifier {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Validates a GCP project ID for safe interpolation into BigQuery SQL.
///
/// GCP project IDs allow hyphens (`my-project-id-123`), which the
/// stricter [`validate_identifier`] would reject. Use this for the
/// catalog/project component on the BigQuery adapter; keep
/// [`validate_identifier`] for dataset and table names, which still
/// must match `[a-zA-Z0-9_]+`.
pub fn validate_gcp_project_id(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyIdentifier);
    }
    if !GCP_PROJECT_ID_RE.is_match(value) {
        return Err(ValidationError::InvalidGcpProjectId {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Validates a principal name for use in GRANT/REVOKE statements.
///
/// Allows alphanumeric, underscores, spaces, hyphens, dots, and @.
/// Principal names should always be wrapped in backticks in SQL.
pub fn validate_principal(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyPrincipal);
    }
    if !PRINCIPAL_NAME_RE.is_match(value) {
        return Err(ValidationError::InvalidPrincipal {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Refuses a user-supplied SQL *fragment* that could end the generated
/// statement and start a new one.
///
/// A fragment is a piece of SQL an author writes in config (a declarative
/// check's `expression`, `filter` or `key_expr`) that Rocky splices into a
/// statement it builds. The fragment is always spliced inside parentheses, so
/// it is an *expression*, never a statement. This function enforces that:
///
/// - a `;` outside a string literal, a quoted identifier or a comment is
///   [`ValidationError::StatementTerminator`];
/// - a fragment that ends inside an unclosed `'…'`, `"…"` or `/* … */` is
///   [`ValidationError::UnterminatedSqlRegion`].
///
/// `context` names the field and the table under check; it is quoted verbatim
/// in the error so the author knows which line to fix.
///
/// # Why both rules are needed
///
/// Statement injection needs a `;` in the fragment — rule one refuses that.
/// Rule two closes the way a fragment can *manufacture* one. Some templates
/// splice the same fragment **twice** into one statement
/// (`generate_test_sql_inner`'s `unique_expr` arm repeats `key_expr` in the
/// `SELECT` and the `GROUP BY`; `quarantine::build_quarantine_ctas` repeats a
/// predicate in the error-label column and the `WHERE`). A fragment with an
/// odd number of quotes pairs its quotes *across* the two copies: template
/// text becomes string content, and the tail of the second copy becomes live
/// SQL — including a `;` this scanner had classified as "inside a string".
/// Requiring balanced quotes makes the scan's view of the fragment the same as
/// the warehouse's view, for any number of copies.
///
/// The invariant is therefore: **balanced quotes plus no top-level `;` means no
/// statement injection, however many times the fragment is embedded.**
///
/// # Scanner shape and its deliberate deviations
///
/// Same shape as the live-verified `count_statements` in
/// `rocky-snowflake/src/connector.rs`, with the opposite bias. That one
/// *counts* statements for one dialect, so it models Snowflake's lexer exactly.
/// This one is a *security* check across every dialect Rocky targets
/// (Databricks, Snowflake, BigQuery, DuckDB, Trino), so every ambiguity resolves
/// toward refusing:
///
/// - **Backslash escapes are not honoured.** Spark and Snowflake read `\'` as an
///   escaped quote; standard SQL (DuckDB, BigQuery, Trino) reads it as a quote
///   that closes the literal. Honouring it would *extend* the literal and could
///   swallow a real terminator — fail-open. Ignoring it *shortens* the literal
///   and surfaces more top-level `;` — fail-closed.
/// - **`$$…$$`, backticks and `//` are not skip regions.** Dollar quoting and
///   `//` comments are Snowflake-only; backticks are not Snowflake. Skipping a
///   region a dialect does not recognise hides a `;` that dialect would execute.
///   A `;` inside one of those is refused instead. Refusing is loud and the
///   author can rewrite the literal; accepting destroys data.
/// - **Block comments do not nest** (they end at the first `*/`, as Snowflake's
///   lexer does). Under nesting, `/* a /* b */ ; */` hides the `;`; flat, the
///   `;` is top-level and refused. Flat is the fail-closed reading.
///
/// Only `''` / `""` doubling escapes a quote — every dialect Rocky targets
/// agrees on that.
///
/// # What this does not cover
///
/// It bounds the fragment to a single statement. It does **not** bound what the
/// expression may *read*: a correlated subquery, or a DuckDB `read_csv` /
/// `read_text` call, is still evaluated with the project's warehouse
/// credentials.
///
/// # Errors
///
/// Returns [`ValidationError::StatementTerminator`] or
/// [`ValidationError::UnterminatedSqlRegion`] as described above.
pub fn reject_statement_terminator(context: &str, sql: &str) -> Result<(), ValidationError> {
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            ';' => {
                return Err(ValidationError::StatementTerminator {
                    context: context.to_string(),
                });
            }
            '\'' | '"' => {
                let quote = c;
                let mut closed = false;
                while let Some(q) = chars.next() {
                    if q != quote {
                        continue;
                    }
                    // `''` / `""` doubles the quote and stays inside.
                    if chars.peek() == Some(&quote) {
                        chars.next();
                    } else {
                        closed = true;
                        break;
                    }
                }
                if !closed {
                    return Err(ValidationError::UnterminatedSqlRegion {
                        context: context.to_string(),
                        region: if quote == '\'' {
                            "string literal (')"
                        } else {
                            "quoted identifier or string (\")"
                        },
                    });
                }
            }
            '-' if chars.peek() == Some(&'-') => {
                // `-- …` runs to end of line. Running off the end of the
                // fragment is allowed: it comments out the remainder of the
                // generated statement, which is a syntax error, not a way to
                // smuggle a terminator past this scan.
                for nc in chars.by_ref() {
                    if nc == '\n' {
                        break;
                    }
                }
            }
            '/' if chars.peek() == Some(&'*') => {
                chars.next();
                let mut prev = ' ';
                let mut closed = false;
                for nc in chars.by_ref() {
                    if prev == '*' && nc == '/' {
                        closed = true;
                        break;
                    }
                    prev = nc;
                }
                if !closed {
                    return Err(ValidationError::UnterminatedSqlRegion {
                        context: context.to_string(),
                        region: "block comment (/* */)",
                    });
                }
            }
            _ => {}
        }
    }
    Ok(())
}

/// Formats a validated identifier for use in SQL (no quoting needed for valid identifiers).
pub fn format_table_ref(
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<String, ValidationError> {
    validate_identifier(catalog)?;
    validate_identifier(schema)?;
    validate_identifier(table)?;
    Ok(format!("{catalog}.{schema}.{table}"))
}

/// Formats a principal name wrapped in backticks for SQL GRANT/REVOKE.
pub fn format_principal(name: &str) -> Result<String, ValidationError> {
    validate_principal(name)?;
    Ok(format!("`{name}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("CamelCase").is_ok());
        assert!(validate_identifier("table123").is_ok());
        assert!(validate_identifier("_leading_underscore").is_ok());
        assert!(validate_identifier("ALL_CAPS_123").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("has space").is_err());
        assert!(validate_identifier("has-dash").is_err());
        assert!(validate_identifier("has.dot").is_err());
        assert!(validate_identifier("has;semicolon").is_err());
        assert!(validate_identifier("DROP TABLE users--").is_err());
        assert!(validate_identifier("'; DROP TABLE users; --").is_err());
        assert!(validate_identifier("table\nname").is_err());
    }

    #[test]
    fn test_valid_principals() {
        assert!(validate_principal("user@domain.com").is_ok());
        assert!(validate_principal("my-service-principal").is_ok());
        assert!(validate_principal("Data Engineers").is_ok());
        assert!(validate_principal("user_name").is_ok());
        assert!(validate_principal("group.name").is_ok());
    }

    #[test]
    fn test_invalid_principals() {
        assert!(validate_principal("").is_err());
        assert!(validate_principal("user;DROP TABLE").is_err());
        assert!(validate_principal("user`backtick").is_err());
        assert!(validate_principal("user'quote").is_err());
        assert!(validate_principal("user\nnewline").is_err());
    }

    #[test]
    fn test_format_table_ref() {
        assert_eq!(
            format_table_ref("my_catalog", "my_schema", "my_table").unwrap(),
            "my_catalog.my_schema.my_table"
        );
    }

    #[test]
    fn test_format_table_ref_rejects_injection() {
        assert!(format_table_ref("catalog; DROP TABLE", "schema", "table").is_err());
    }

    #[test]
    fn test_valid_gcp_project_ids() {
        assert!(validate_gcp_project_id("rocky-sandbox").is_ok());
        assert!(validate_gcp_project_id("my-gcp-project-123").is_ok());
        assert!(validate_gcp_project_id("my-project-1").is_ok());
        // Lower bound: 6 chars total (1 leading letter + 4 middle + 1 tail).
        assert!(validate_gcp_project_id("abc12d").is_ok());
    }

    #[test]
    fn test_invalid_gcp_project_ids() {
        // Empty / too short.
        assert!(validate_gcp_project_id("").is_err());
        assert!(validate_gcp_project_id("abc12").is_err());
        // Must start with a letter.
        assert!(validate_gcp_project_id("1-project").is_err());
        assert!(validate_gcp_project_id("-project").is_err());
        // Cannot end with a hyphen.
        assert!(validate_gcp_project_id("rocky-sandbox-").is_err());
        // No uppercase, dots, underscores, spaces.
        assert!(validate_gcp_project_id("Rocky-Sandbox").is_err());
        assert!(validate_gcp_project_id("rocky.sandbox").is_err());
        assert!(validate_gcp_project_id("rocky_sandbox").is_err());
        assert!(validate_gcp_project_id("rocky sandbox").is_err());
        // Injection attempts.
        assert!(validate_gcp_project_id("'; DROP TABLE users; --").is_err());
        assert!(validate_gcp_project_id("project`backtick").is_err());
    }

    // ----- reject_statement_terminator -----

    fn rejects(sql: &str) -> bool {
        reject_statement_terminator("ctx", sql).is_err()
    }

    #[test]
    fn terminator_accepts_ordinary_fragments() {
        for sql in [
            "amount > 0",
            "amount >= 0 AND status != 'cancelled'",
            "region = 'US'",
            "md5(CAST(customer_id AS VARCHAR) || '|' || CAST(order_date AS VARCHAR))",
            "created_at > current_date - interval 30 day",
            "name LIKE 'O''Brien%'",
            "\"my col\" > 0",
            "",
            "   ",
        ] {
            assert!(
                reject_statement_terminator("ctx", sql).is_ok(),
                "should accept: {sql}"
            );
        }
    }

    #[test]
    fn terminator_refuses_top_level_semicolon() {
        // The injection class: close the parenthesis Rocky wraps the fragment
        // in, end the statement, start another, comment out the tail.
        assert!(rejects("1=1) OR (1=1); SELECT 1; --"));
        assert!(rejects("amount > 0; SELECT 1"));
        // A lone terminator, no payload.
        assert!(rejects(";"));
    }

    #[test]
    fn terminator_refuses_a_single_trailing_semicolon() {
        // Deliberate strictness call: a trailing `;` is REFUSED, not trimmed.
        // Every seam splices the fragment inside parentheses, so `(amount > 0;)`
        // is already a syntax error at execute time — refusing costs no working
        // config and avoids silently rewriting the author's SQL.
        assert!(rejects("amount > 0;"));
        assert!(rejects("amount > 0 ;  "));
    }

    #[test]
    fn terminator_skips_semicolons_inside_a_string_literal() {
        assert!(reject_statement_terminator("ctx", "note = 'a;b'").is_ok());
        // `''` escapes a quote and stays inside the literal.
        assert!(reject_statement_terminator("ctx", "note = 'it''s a;b'").is_ok());
        assert!(reject_statement_terminator("ctx", "note = 'a''; DROP TABLE t; --'").is_ok());
    }

    #[test]
    fn terminator_skips_semicolons_inside_a_double_quoted_identifier() {
        assert!(reject_statement_terminator("ctx", "\"odd;col\" > 0").is_ok());
        assert!(reject_statement_terminator("ctx", "\"a\"\"b;c\" > 0").is_ok());
    }

    #[test]
    fn terminator_skips_semicolons_inside_a_line_comment() {
        assert!(reject_statement_terminator("ctx", "amount > 0 -- ; not a statement\n").is_ok());
        // Running off the end of the fragment is fine — it only comments out
        // the remainder of the generated statement.
        assert!(reject_statement_terminator("ctx", "amount > 0 -- ; trailing").is_ok());
    }

    #[test]
    fn terminator_skips_semicolons_inside_a_block_comment() {
        assert!(reject_statement_terminator("ctx", "amount /* ; */ > 0").is_ok());
        assert!(reject_statement_terminator("ctx", "/* a ; b */ amount > 0").is_ok());
    }

    #[test]
    fn terminator_refuses_a_semicolon_after_a_closed_region() {
        assert!(rejects("note = 'a;b'; DROP TABLE t"));
        assert!(rejects("amount /* c */ > 0; DROP TABLE t"));
        assert!(rejects("amount > 0 -- c\n; DROP TABLE t"));
    }

    #[test]
    fn terminator_refuses_an_unbalanced_quote() {
        // The double-embedding hazard: `unique_expr` splices `key_expr` into
        // both the SELECT and the GROUP BY, so an odd quote count pairs across
        // the two copies and turns template text into string content.
        assert!(rejects("x = 'unterminated"));
        assert!(rejects("x = \"unterminated"));
        assert!(rejects("x /* unterminated"));
        // …and that is what makes an in-string `;` safe to skip: it can only
        // be skipped in a fragment whose quotes are balanced.
        assert!(rejects("x' ; DROP TABLE t; --"));
    }

    #[test]
    fn terminator_does_not_treat_dollar_quotes_or_backticks_as_regions() {
        // Deliberate deviation from Snowflake's `count_statements`: `$$` and
        // `//` are Snowflake-only and backticks are not Snowflake, so skipping
        // them would hide a `;` another dialect executes. Refuse instead.
        assert!(rejects("x = $$ ; DROP TABLE t; $$"));
        assert!(rejects("`odd;col` > 0"));
        assert!(rejects("amount > 0 // ; DROP TABLE t"));
    }

    #[test]
    fn terminator_block_comments_do_not_nest() {
        // Flat scan: the comment ends at the FIRST `*/`, so the `;` is
        // top-level and refused. Nesting would hide it.
        assert!(rejects("/* a /* b */ ; */ amount > 0"));
    }

    #[test]
    fn terminator_error_names_the_context() {
        let err =
            reject_statement_terminator("expression test `expression` on db.sc.orders", "a;b")
                .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("expression test `expression` on db.sc.orders"),
            "{msg}"
        );
        assert!(msg.contains("statement terminator"), "{msg}");
    }

    #[test]
    fn test_format_principal() {
        assert_eq!(
            format_principal("Data Engineers").unwrap(),
            "`Data Engineers`"
        );
        assert_eq!(
            format_principal("user@domain.com").unwrap(),
            "`user@domain.com`"
        );
    }
}
