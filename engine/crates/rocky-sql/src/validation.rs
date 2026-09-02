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

    #[error(
        "{context}: SQL fragment contains {token}, which the warehouses Rocky targets read \
         differently. Rocky refuses it rather than guess which reading applies. Rewrite the \
         fragment using plain '...' / \"...\" literals, -- or /* */ comments"
    )]
    AmbiguousSqlToken {
        context: String,
        token: &'static str,
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

/// Refuses a user-supplied SQL *fragment* that Rocky cannot lex with certainty.
///
/// A fragment is a piece of SQL an author writes in config (a declarative
/// check's `expression`, `filter` or `key_expr`) that Rocky splices into a
/// statement it builds. Rocky generates for five warehouses — Databricks/Spark,
/// Snowflake, BigQuery, DuckDB and Trino — and their lexers disagree about
/// several constructs. This function therefore accepts only the subset all five
/// read identically, and refuses everything else.
///
/// # The accepted subset
///
/// - `'…'` and `"…"`, where the only escape is `''` / `""` doubling;
/// - `-- …` line comments, ending at `\n` or `\r`;
/// - non-nested `/* … */` block comments;
/// - any other text with no `;` in it.
///
/// # What is refused, and why each one
///
/// | Refused | Reason |
/// |---|---|
/// | `;` outside a literal or comment | it ends Rocky's statement and starts another |
/// | a `\` inside a quoted literal | Snowflake, DuckDB and BigQuery read `\'` as an escaped quote; standard SQL reads it as the quote that closes the literal. The two readings pair the *following* quotes differently, so a `;` this scan sees as string content can be top-level at the warehouse. `rocky-snowflake/src/governance.rs` documents the same bypass for tag literals. |
/// | a triple quote | a BigQuery triple-quoted string; elsewhere an empty literal followed by another quote |
/// | `$$…$$` / `$tag$…$tag$` | dollar quoting is Snowflake and DuckDB; on the others the quotes inside it are real quotes |
/// | a backtick | an identifier quote on Databricks and BigQuery, a syntax error on Snowflake — and a `'` inside one is inert for the first group, a literal opener for the second |
/// | `//` | a line comment on Snowflake only; elsewhere the rest of the line is live SQL |
/// | `/*` inside a block comment | Spark nests block comments, the others end at the first `*/` |
/// | an unterminated `'…'`, `"…"` or `/* … */` | see below |
///
/// Every one of these is a *refusal*, never a guess. Refusing an exotic but
/// legal fragment is a loud error the author can fix in one line. Mis-lexing one
/// is the defect this function exists to prevent.
///
/// # Why unterminated regions are refused
///
/// Some templates splice the same fragment **twice** into one statement
/// (`generate_test_sql_inner`'s `unique_expr` arm repeats `key_expr` in the
/// `SELECT` and the `GROUP BY`; `quarantine::build_quarantine_ctas` repeats a
/// predicate in the error-label column and the `WHERE`). A fragment with an odd
/// number of quotes pairs its quotes *across* the two copies: template text
/// becomes string content and the tail of the second copy becomes live SQL.
/// Requiring every region to close inside the fragment keeps each copy
/// self-contained.
///
/// # The invariant this actually gives you
///
/// **Within the accepted subset, this scan and every target warehouse agree on
/// which characters are literal or comment text. So a fragment that passes
/// contributes no statement terminator, however many times it is embedded.**
///
/// That is the whole guarantee. It does **not** prove the fragment is a single
/// expression:
///
/// - **Parentheses are not tracked.** A fragment can close the parenthesis Rocky
///   wraps it in and add its own clauses (`1=1) OR (1=1`), or append a `UNION`.
///   That reshapes the query Rocky built without needing a `;` at all.
/// - **Reads are not bounded.** A correlated subquery, or a DuckDB `read_csv` /
///   `read_text` call, is still evaluated with the project's warehouse
///   credentials.
///
/// `context` names the field and the table under check; it is quoted verbatim in
/// the error so the author knows which line to fix.
///
/// # Relationship to `count_statements`
///
/// The scan shape is borrowed from the live-verified `count_statements` in
/// `rocky-snowflake/src/connector.rs`, but the two have opposite jobs. That one
/// *counts* statements for one dialect, so it models Snowflake's lexer as
/// closely as it can — including its backslash and dollar-quote rules. This one
/// must be right for five lexers at once, so where they differ it refuses
/// instead of picking one.
///
/// # Errors
///
/// [`ValidationError::StatementTerminator`] for a top-level `;`,
/// [`ValidationError::AmbiguousSqlToken`] for a construct the dialects read
/// differently, and [`ValidationError::UnterminatedSqlRegion`] for a region that
/// does not close inside the fragment.
pub fn reject_statement_terminator(context: &str, sql: &str) -> Result<(), ValidationError> {
    let ambiguous = |token: &'static str| ValidationError::AmbiguousSqlToken {
        context: context.to_string(),
        token,
    };
    let unterminated = |region: &'static str| ValidationError::UnterminatedSqlRegion {
        context: context.to_string(),
        region,
    };

    let c: Vec<char> = sql.chars().collect();
    let at = |i: usize| c.get(i).copied();
    let mut i = 0usize;

    while i < c.len() {
        match c[i] {
            ';' => {
                return Err(ValidationError::StatementTerminator {
                    context: context.to_string(),
                });
            }
            // Databricks and BigQuery quote identifiers with backticks;
            // Snowflake rejects them outright. A quote inside one is inert for
            // the first group and a literal opener for the second, which shifts
            // every later quote pairing. Rocky identifiers are `[A-Za-z0-9_]+`
            // anyway; use `"..."` for anything that needs quoting.
            '`' => return Err(ambiguous("a backtick-quoted identifier")),
            // A dollar-quote opener. A bare `$` inside a word (`col$1`) is not
            // an opener and stays allowed.
            '$' => {
                let mut j = i + 1;
                while matches!(at(j), Some(ch) if ch.is_ascii_alphanumeric() || ch == '_') {
                    j += 1;
                }
                if at(j) == Some('$') {
                    return Err(ambiguous("a dollar-quoted string"));
                }
                i += 1;
            }
            '\'' | '"' => {
                let quote = c[i];
                // Three in a row opens a BigQuery triple-quoted string, and is
                // an empty literal plus a stray quote everywhere else. A bare
                // `''` (empty literal) is unambiguous and stays allowed.
                if at(i + 1) == Some(quote) && at(i + 2) == Some(quote) {
                    return Err(ambiguous(if quote == '\'' {
                        "a triple-quoted string (three single quotes)"
                    } else {
                        "a triple-quoted string (three double quotes)"
                    }));
                }
                let region = if quote == '\'' {
                    "string literal (')"
                } else {
                    "quoted identifier or string (\")"
                };
                i += 1;
                let mut closed = false;
                while i < c.len() {
                    if c[i] == '\\' {
                        return Err(ambiguous("a backslash escape inside a quoted literal"));
                    }
                    if c[i] == quote {
                        if at(i + 1) == Some(quote) {
                            i += 2; // doubling: still inside the literal
                            continue;
                        }
                        closed = true;
                        i += 1;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return Err(unterminated(region));
                }
            }
            '-' if at(i + 1) == Some('-') => {
                // `-- ...` runs to end of line on every dialect. A LONE `\r`
                // ends it too (Postgres, DuckDB and Snowflake all treat a
                // carriage return as the line end), so stopping only at `\n`
                // would keep swallowing text those lexers had already resumed
                // reading as live SQL — including a `;`. Ending the comment
                // EARLIER than a given dialect might is always the safe
                // direction: it can only surface more top-level characters.
                //
                // Running off the end of the fragment is allowed: it comments
                // out the remainder of the generated statement, which is a
                // syntax error, not a way to smuggle a terminator past this
                // scan.
                i += 2;
                while i < c.len() && c[i] != '\n' && c[i] != '\r' {
                    i += 1;
                }
            }
            // A Snowflake-only line comment. Every other dialect keeps reading
            // the rest of the line as SQL, so neither skipping it nor ignoring
            // it is safe for all five.
            '/' if at(i + 1) == Some('/') => return Err(ambiguous("a `//` line comment")),
            // A BigQuery-only line comment, and the same split as `//`: on
            // BigQuery the rest of the line is comment, everywhere else it is
            // live SQL. Ignoring it is the dangerous half — a quote inside a
            // `#` comment would move THIS scanner's quote state while BigQuery
            // resumed reading SQL at the newline, so a later terminator could
            // be classified as string content.
            '#' => return Err(ambiguous("a `#` line comment")),
            '/' if at(i + 1) == Some('*') => {
                i += 2;
                let mut closed = false;
                while i < c.len() {
                    if c[i] == '/' && at(i + 1) == Some('*') {
                        // Spark nests block comments; the others end at the
                        // first `*/`. The two readings leave different text
                        // outside the comment.
                        return Err(ambiguous("a nested `/*` block comment"));
                    }
                    if c[i] == '*' && at(i + 1) == Some('/') {
                        closed = true;
                        i += 2;
                        break;
                    }
                    i += 1;
                }
                if !closed {
                    return Err(unterminated("block comment (/* */)"));
                }
            }
            _ => i += 1,
        }
    }
    Ok(())
}

/// Refuses a value Rocky cannot safely wrap in a `'…'` SQL literal.
///
/// Rocky encodes a config-supplied string constant — a declarative check's
/// `accepted_values`, for instance — by doubling single quotes. That is enough
/// only where `''` is the *only* escape. Snowflake, DuckDB and BigQuery also
/// honour a backslash escape, so a value ending in `\` escapes the first quote
/// of the `''` pair Rocky emits and the second quote closes the literal — the
/// bypass `rocky-snowflake/src/governance.rs` documents for tag values.
///
/// Escaping backslashes as well closes the bypass on those three but changes the
/// *value* on Trino and standard-SQL DuckDB, where a doubled backslash is two
/// literal backslashes. No single encoding is correct everywhere, and these call
/// sites have no dialect to ask. So a backslash is refused and the author is
/// told which value to change.
///
/// The proper fix is dialect-owned literal encoding or parameter binding at
/// these call sites; this is the cheap, sound guard until that lands.
///
/// # Errors
///
/// Returns [`ValidationError::AmbiguousSqlToken`] when `value` contains a
/// backslash.
pub fn reject_unquotable_literal(context: &str, value: &str) -> Result<(), ValidationError> {
    if value.contains('\\') {
        return Err(ValidationError::AmbiguousSqlToken {
            context: context.to_string(),
            token: "a backslash in a string value (dialects disagree whether it escapes the \
                    next character, so quote-doubling alone cannot contain it)",
        });
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
    fn terminator_ends_a_line_comment_at_a_carriage_return() {
        // Postgres, DuckDB and Snowflake end a `--` comment at a lone `\r`, so
        // a scan that stopped only at `\n` would keep treating live SQL as
        // comment text and skip the `;` in it.
        assert!(rejects("amount > 0 -- c\r; SELECT 1"));
        // CRLF behaves the same way.
        assert!(rejects("amount > 0 -- c\r\n; SELECT 1"));
        // A comment that really does run to the end is still fine.
        assert!(reject_statement_terminator("ctx", "amount > 0 -- ; trailing").is_ok());
    }

    #[test]
    fn terminator_refuses_a_hash_line_comment() {
        // Same split as `//`, found by the round-3 review. BigQuery reads `#`
        // to end of line as a comment; the other four keep reading SQL. So a
        // quote inside a `#` run moves THIS scanner's quote state while
        // BigQuery has already resumed live SQL at the newline — and a `;`
        // after it lands top-level there while the scan files it as string
        // content. The fragment below is balanced to the scan and carries no
        // scan-visible top-level `;`.
        let f = "x = 1 # it's fine\n AND y = 'a' ; SELECT 1; -- '";
        let err = reject_statement_terminator("ctx", f).unwrap_err();
        assert!(err.to_string().contains('#'), "{err}");
        // A `#` inside a string literal is ordinary text, not a comment.
        assert!(reject_statement_terminator("ctx", "tag = '#1'").is_ok());
    }

    #[test]
    fn terminator_refuses_a_backslash_inside_a_literal() {
        // The defect this rule exists for. On Snowflake / DuckDB / BigQuery a
        // backslash escapes the next quote, so the warehouse closes the literal
        // one quote LATER than a doubling-only scan does. Every quote after that
        // point is paired differently, and a `;` the scan filed as string
        // content is top-level at the warehouse. The fragment below is balanced
        // (four quotes) and has no scan-visible top-level `;`, so a scan that
        // merely ignored backslashes would ACCEPT it.
        let f = r"x = 'a\'b') ; SELECT 1; -- '";
        let err = reject_statement_terminator("ctx", f).unwrap_err();
        assert!(err.to_string().contains("backslash"), "{err}");
        // Same shape without the escape is still refused, by the `;` rule.
        assert!(rejects("x = 'ab') ; SELECT 1; -- '"));
    }

    #[test]
    fn terminator_refuses_a_backslash_even_with_no_semicolon() {
        // Refuse the construct, not just the payload — the scan cannot tell
        // where the literal ends, so nothing after it can be trusted.
        assert!(rejects(r"x = 'a\'b'"));
        assert!(rejects(r#"x = "a\"b""#));
        assert!(rejects(r"x LIKE 'a\%'"));
    }

    #[test]
    fn terminator_refuses_triple_quotes() {
        // BigQuery reads three quotes as a triple-quoted string; the others read
        // an empty literal plus a stray quote. Different text ends up inside.
        assert!(rejects("x = '''a ; b'''"));
        assert!(rejects("x = \"\"\"a ; b\"\"\""));
    }

    #[test]
    fn terminator_still_accepts_an_empty_literal() {
        // `''` on its own is unambiguous everywhere — only THREE quotes are the
        // ambiguous case, so the triple-quote rule must not swallow this.
        assert!(reject_statement_terminator("ctx", "x = ''").is_ok());
        assert!(reject_statement_terminator("ctx", "x = '' OR y = ''").is_ok());
        assert!(reject_statement_terminator("ctx", "x = 'it''s'").is_ok());
    }

    #[test]
    fn terminator_refuses_a_nested_block_comment() {
        // Spark nests block comments, so it reads the whole thing as a comment;
        // the others end at the first `*/`, which leaves the quote and the `;`
        // outside. Two readings, so refuse.
        assert!(rejects("/* a /* b */ ' */ ; SELECT 1"));
        assert!(rejects("x /* a /* b */ */ > 0"));
    }

    #[test]
    fn terminator_refuses_dollar_quotes_but_not_a_dollar_in_a_word() {
        assert!(rejects("x = $$ ; SELECT 1; $$"));
        assert!(rejects("x = $tag$ ; SELECT 1; $tag$"));
        // A `$` that opens nothing is ordinary text.
        assert!(reject_statement_terminator("ctx", "col$1 > 0").is_ok());
        assert!(reject_statement_terminator("ctx", "amount > 0 AND x$ = 1").is_ok());
    }

    #[test]
    fn unquotable_literal_refuses_a_backslash() {
        // Quote-doubling alone cannot contain a value ending in a backslash:
        // the backslash escapes the first quote of the `''` pair Rocky emits and
        // the second closes the literal. Same class as the scanner rule above.
        let err = reject_unquotable_literal("accepted_values on t", "ends_with_a_backslash\\")
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("backslash"), "{msg}");
        assert!(msg.contains("accepted_values on t"), "{msg}");
        assert!(reject_unquotable_literal("ctx", r"a\b").is_err());
    }

    #[test]
    fn unquotable_literal_accepts_ordinary_values() {
        for v in ["pending", "shipped", "O'Brien", "a;b", "US/Eastern", ""] {
            assert!(
                reject_unquotable_literal("ctx", v).is_ok(),
                "should accept: {v}"
            );
        }
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
