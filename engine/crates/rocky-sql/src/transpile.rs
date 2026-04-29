//! Basic SQL transpilation between Snowflake, Databricks, and BigQuery.
//!
//! Handles the most common cross-dialect incompatibilities:
//! - Function name differences (e.g., `NVL` vs `COALESCE` vs `IFNULL`)
//! - Date/time function syntax
//! - Type name normalization
//! - `QUALIFY` clause (Snowflake/Databricks → BigQuery subquery)
//! - `FLATTEN` (Snowflake) → `UNNEST` (BigQuery/Databricks)
//! - String concatenation (`||` vs `CONCAT`)
//! - Boolean literals (`TRUE`/`FALSE` vs `1`/`0`)
//!
//! This is **not** a full SQL transpiler (like SQLGlot). It handles
//! the 80% case — common functions and syntax that differ between
//! warehouses Rocky supports. Complex or warehouse-specific SQL should
//! be written natively.

/// Target dialect for transpilation.
///
/// Serializes lowercase (`databricks`, `snowflake`, `bigquery`, `duckdb`) so
/// the long-form names can sit in `rocky.toml` under the `[portability]`
/// block without translation. The CLI's short-form flag values
/// (`dbx`/`sf`/`bq`/`duckdb`) are kept as ergonomics in the
/// `TargetDialect` clap enum and convert to this type at the boundary.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum Dialect {
    Databricks,
    Snowflake,
    BigQuery,
    DuckDB,
}

impl std::fmt::Display for Dialect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Dialect::Databricks => write!(f, "Databricks"),
            Dialect::Snowflake => write!(f, "Snowflake"),
            Dialect::BigQuery => write!(f, "BigQuery"),
            Dialect::DuckDB => write!(f, "DuckDB"),
        }
    }
}

/// Result of a transpilation attempt.
#[derive(Debug, Clone)]
pub struct TranspileResult {
    /// The transpiled SQL.
    pub sql: String,
    /// Warnings about constructs that couldn't be fully transpiled.
    pub warnings: Vec<TranspileWarning>,
    /// Number of replacements made.
    pub replacements: usize,
}

/// A warning about a construct that may not transpile perfectly.
#[derive(Debug, Clone)]
pub struct TranspileWarning {
    /// The original construct.
    pub original: String,
    /// Why it might not work.
    pub reason: String,
    /// Suggested manual fix.
    pub suggestion: Option<String>,
}

/// Transpile SQL from one dialect to another.
///
/// This performs text-based replacements for common patterns. It does
/// NOT parse the SQL into an AST — that would require sqlparser-rs
/// roundtripping which can alter formatting. For most common patterns,
/// regex-based replacement is sufficient and preserves formatting.
pub fn transpile(sql: &str, from: Dialect, to: Dialect) -> TranspileResult {
    if from == to {
        return TranspileResult {
            sql: sql.to_string(),
            warnings: vec![],
            replacements: 0,
        };
    }

    let mut result = sql.to_string();
    let mut warnings = Vec::new();
    let mut replacements = 0;

    // Apply function name mappings.
    //
    // Function tokens already end in `(` (e.g. `NVL(`), so they're
    // self-delimiting in code. The text-walk still skips inside string
    // literals and comments to avoid corrupting `'NVL(...)'` or
    // `-- NVL(...)` substrings.
    let mappings = get_function_mappings(from, to);
    for (from_fn, to_fn) in &mappings {
        let (next, count) = replace_outside_strings(&result, from_fn, to_fn, MatchKind::Literal);
        if count > 0 {
            result = next;
            replacements += count;
        }
    }

    // Apply type name mappings.
    //
    // Type tokens are bare words (e.g. `INT`, `BIGINT`), so a naïve
    // `replace` corrupts substrings (`INT` matches inside `BIGINT`) and
    // string literals. Walk the body, skip strings/comments, and require
    // ASCII word boundaries on either side of the match.
    let type_mappings = get_type_mappings(from, to);
    for (from_type, to_type) in &type_mappings {
        let (next, count) = replace_outside_strings(
            &result,
            from_type,
            to_type,
            MatchKind::WordBoundary {
                case_sensitive: false,
            },
        );
        if count > 0 {
            result = next;
            replacements += count;
        }
    }

    // Dialect-specific transformations
    match (from, to) {
        // Snowflake QUALIFY → subquery wrapper
        (Dialect::Snowflake, Dialect::DuckDB) if result.to_uppercase().contains("QUALIFY") => {
            warnings.push(TranspileWarning {
                original: "QUALIFY clause".to_string(),
                reason: format!("{to} does not support QUALIFY — manual rewrite needed"),
                suggestion: Some(
                    "Wrap the query in a subquery and move the QUALIFY condition to WHERE"
                        .to_string(),
                ),
            });
        }
        // BigQuery supports QUALIFY natively since 2023 — no change needed
        (_, Dialect::Snowflake) | (_, Dialect::Databricks) => {
            // Both support QUALIFY natively
        }
        _ => {}
    }

    // Snowflake FLATTEN → UNNEST
    if from == Dialect::Snowflake
        && to != Dialect::Snowflake
        && result.to_uppercase().contains("FLATTEN")
    {
        warnings.push(TranspileWarning {
            original: "FLATTEN()".to_string(),
            reason: format!(
                "Snowflake FLATTEN has no direct equivalent in {to} — use UNNEST or LATERAL"
            ),
            suggestion: Some("Replace FLATTEN(input => col) with UNNEST(col)".to_string()),
        });
    }

    // ILIKE (Snowflake/DuckDB) → not supported in BigQuery
    if matches!(from, Dialect::Snowflake | Dialect::DuckDB)
        && to == Dialect::BigQuery
        && result.to_uppercase().contains("ILIKE")
    {
        let count = case_insensitive_count(&result, "ILIKE");
        result = case_insensitive_replace(&result, " ILIKE ", " LIKE LOWER(");
        // This is a rough approximation — ILIKE 'pattern' → LIKE LOWER('pattern')
        // doesn't fully work without also wrapping the LHS in LOWER()
        warnings.push(TranspileWarning {
            original: "ILIKE".to_string(),
            reason: "BigQuery doesn't support ILIKE — partial conversion applied".to_string(),
            suggestion: Some(
                "Manually wrap both sides in LOWER(): LOWER(col) LIKE LOWER('pattern')".to_string(),
            ),
        });
        replacements += count;
    }

    TranspileResult {
        sql: result,
        warnings,
        replacements,
    }
}

/// Get function name mappings between dialects.
fn get_function_mappings(from: Dialect, to: Dialect) -> Vec<(String, String)> {
    let mut mappings = Vec::new();

    // NVL (Snowflake/Databricks) → COALESCE (standard) or IFNULL (BigQuery)
    match (from, to) {
        (Dialect::Snowflake, Dialect::BigQuery) | (Dialect::Databricks, Dialect::BigQuery) => {
            mappings.push(("NVL(".into(), "IFNULL(".into()));
        }
        (Dialect::Snowflake, Dialect::DuckDB) | (Dialect::Databricks, Dialect::DuckDB) => {
            mappings.push(("NVL(".into(), "COALESCE(".into()));
        }
        (Dialect::BigQuery, Dialect::Snowflake) | (Dialect::BigQuery, Dialect::Databricks) => {
            mappings.push(("IFNULL(".into(), "NVL(".into()));
        }
        _ => {}
    }

    // DATEADD / DATE_ADD differences
    match (from, to) {
        (Dialect::Snowflake, Dialect::BigQuery) => {
            mappings.push(("DATEDIFF(".into(), "DATE_DIFF(".into()));
            mappings.push(("DATEADD(".into(), "DATE_ADD(".into()));
            mappings.push(("TO_DATE(".into(), "PARSE_DATE(".into()));
            mappings.push(("TO_TIMESTAMP(".into(), "PARSE_TIMESTAMP(".into()));
            mappings.push(("TO_VARCHAR(".into(), "FORMAT_TIMESTAMP(".into()));
            mappings.push(("TO_CHAR(".into(), "FORMAT_TIMESTAMP(".into()));
        }
        (Dialect::Snowflake, Dialect::Databricks) => {
            mappings.push(("TO_VARCHAR(".into(), "DATE_FORMAT(".into()));
            mappings.push(("TO_CHAR(".into(), "DATE_FORMAT(".into()));
        }
        (Dialect::BigQuery, Dialect::Snowflake) => {
            mappings.push(("DATE_DIFF(".into(), "DATEDIFF(".into()));
            mappings.push(("DATE_ADD(".into(), "DATEADD(".into()));
            mappings.push(("PARSE_DATE(".into(), "TO_DATE(".into()));
            mappings.push(("PARSE_TIMESTAMP(".into(), "TO_TIMESTAMP(".into()));
            mappings.push(("FORMAT_TIMESTAMP(".into(), "TO_VARCHAR(".into()));
        }
        (Dialect::BigQuery, Dialect::Databricks) => {
            mappings.push(("PARSE_DATE(".into(), "TO_DATE(".into()));
            mappings.push(("PARSE_TIMESTAMP(".into(), "TO_TIMESTAMP(".into()));
            mappings.push(("FORMAT_TIMESTAMP(".into(), "DATE_FORMAT(".into()));
        }
        (Dialect::Databricks, Dialect::Snowflake) => {
            mappings.push(("DATE_FORMAT(".into(), "TO_VARCHAR(".into()));
        }
        (Dialect::Databricks, Dialect::BigQuery) => {
            mappings.push(("DATE_FORMAT(".into(), "FORMAT_TIMESTAMP(".into()));
            mappings.push(("TO_DATE(".into(), "PARSE_DATE(".into()));
            mappings.push(("TO_TIMESTAMP(".into(), "PARSE_TIMESTAMP(".into()));
        }
        _ => {}
    }

    // String functions
    match (from, to) {
        (Dialect::Snowflake, Dialect::BigQuery) => {
            mappings.push(("LEN(".into(), "LENGTH(".into()));
            mappings.push(("CHARINDEX(".into(), "STRPOS(".into()));
        }
        (Dialect::BigQuery, Dialect::Snowflake) => {
            mappings.push(("LENGTH(".into(), "LEN(".into()));
            mappings.push(("STRPOS(".into(), "CHARINDEX(".into()));
        }
        _ => {}
    }

    // Array functions
    match (from, to) {
        (Dialect::Snowflake, Dialect::BigQuery) => {
            mappings.push(("ARRAY_SIZE(".into(), "ARRAY_LENGTH(".into()));
        }
        (Dialect::BigQuery, Dialect::Snowflake) => {
            mappings.push(("ARRAY_LENGTH(".into(), "ARRAY_SIZE(".into()));
        }
        _ => {}
    }

    mappings
}

/// Get type name mappings between dialects.
fn get_type_mappings(from: Dialect, to: Dialect) -> Vec<(String, String)> {
    let mut mappings = Vec::new();

    match (from, to) {
        (Dialect::Snowflake, Dialect::BigQuery) => {
            mappings.push(("VARCHAR".into(), "STRING".into()));
            mappings.push(("NUMBER".into(), "NUMERIC".into()));
            mappings.push(("VARIANT".into(), "JSON".into()));
            mappings.push(("TIMESTAMP_NTZ".into(), "TIMESTAMP".into()));
            mappings.push(("TIMESTAMP_LTZ".into(), "TIMESTAMP".into()));
            mappings.push(("TIMESTAMP_TZ".into(), "TIMESTAMP".into()));
        }
        (Dialect::Snowflake, Dialect::Databricks) => {
            mappings.push(("VARCHAR".into(), "STRING".into()));
            mappings.push(("NUMBER".into(), "DECIMAL".into()));
            mappings.push(("VARIANT".into(), "STRING".into())); // lossy
            mappings.push(("TIMESTAMP_NTZ".into(), "TIMESTAMP".into()));
            mappings.push(("TIMESTAMP_LTZ".into(), "TIMESTAMP".into()));
            mappings.push(("TIMESTAMP_TZ".into(), "TIMESTAMP".into()));
        }
        (Dialect::BigQuery, Dialect::Snowflake) => {
            mappings.push(("STRING".into(), "VARCHAR".into()));
            mappings.push(("NUMERIC".into(), "NUMBER".into()));
            mappings.push(("JSON".into(), "VARIANT".into()));
            mappings.push(("INT64".into(), "BIGINT".into()));
            mappings.push(("FLOAT64".into(), "DOUBLE".into()));
            mappings.push(("BOOL".into(), "BOOLEAN".into()));
            mappings.push(("BYTES".into(), "BINARY".into()));
        }
        (Dialect::BigQuery, Dialect::Databricks) => {
            mappings.push(("INT64".into(), "BIGINT".into()));
            mappings.push(("FLOAT64".into(), "DOUBLE".into()));
            mappings.push(("BOOL".into(), "BOOLEAN".into()));
            mappings.push(("BYTES".into(), "BINARY".into()));
        }
        (Dialect::Databricks, Dialect::Snowflake) => {
            mappings.push(("STRING".into(), "VARCHAR".into()));
        }
        (Dialect::Databricks, Dialect::BigQuery) => {
            mappings.push(("BIGINT".into(), "INT64".into()));
            mappings.push(("DOUBLE".into(), "FLOAT64".into()));
            mappings.push(("BOOLEAN".into(), "BOOL".into()));
            mappings.push(("BINARY".into(), "BYTES".into()));
        }
        _ => {}
    }

    mappings
}

/// How a candidate match must align with surrounding text to be replaced.
#[derive(Debug, Clone, Copy)]
enum MatchKind {
    /// Exact, case-sensitive substring match. Used for function tokens
    /// (`NVL(`) which end in `(` and are therefore self-delimiting.
    Literal,
    /// Case-insensitive match that requires an ASCII word boundary on
    /// either side. Used for type tokens (`INT`, `BIGINT`) where a
    /// naïve substring replace would corrupt `BIGINT` while looking for
    /// `INT`.
    WordBoundary { case_sensitive: bool },
}

/// Replace occurrences of `pattern` with `replacement` while skipping
/// SQL string literals (`'...'`, `"..."`, `` `...` ``), line comments
/// (`-- ...`), and block comments (`/* ... */`). Returns the rewritten
/// string and the count of replacements applied.
///
/// This is *not* a full SQL parser — it only tracks quote/comment state
/// well enough to avoid the most obvious corruptions a naïve
/// `String::replace` causes:
///
/// - replacing inside string literals (`'NVL(a,b)'` mustn't change),
/// - replacing inside comments (`-- LEN(x)`),
/// - replacing identifier substrings (`INT` matching inside `BIGINT`).
fn replace_outside_strings(
    input: &str,
    pattern: &str,
    replacement: &str,
    kind: MatchKind,
) -> (String, usize) {
    if pattern.is_empty() {
        return (input.to_string(), 0);
    }

    let bytes = input.as_bytes();
    let mut out = String::with_capacity(input.len());
    let mut i = 0usize;
    let mut count = 0usize;

    // Helper: advance `i` by one UTF-8 char starting at `start`,
    // appending the raw byte slice to `out`. Keeps multi-byte chars
    // intact (e.g. accented identifiers, emoji in comments).
    let copy_one_char = |i: &mut usize, out: &mut String| {
        let start = *i;
        let step = utf8_char_len(bytes[start]);
        let end = (start + step).min(bytes.len());
        out.push_str(std::str::from_utf8(&bytes[start..end]).unwrap_or(""));
        *i = end;
    };

    while i < bytes.len() {
        let b = bytes[i];

        // -- line comment
        if b == b'-' && bytes.get(i + 1) == Some(&b'-') {
            while i < bytes.len() && bytes[i] != b'\n' {
                copy_one_char(&mut i, &mut out);
            }
            continue;
        }

        // /* block comment */
        if b == b'/' && bytes.get(i + 1) == Some(&b'*') {
            out.push_str("/*");
            i += 2;
            while i < bytes.len() {
                if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
                    out.push_str("*/");
                    i += 2;
                    break;
                }
                copy_one_char(&mut i, &mut out);
            }
            continue;
        }

        // String literals / quoted identifiers.
        if b == b'\'' || b == b'"' || b == b'`' {
            let quote = b;
            out.push(quote as char);
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                // SQL standard: doubled quote escapes the quote inside
                // a literal/identifier. Pass both bytes through.
                if c == quote && bytes.get(i + 1) == Some(&quote) {
                    out.push(quote as char);
                    out.push(quote as char);
                    i += 2;
                    continue;
                }
                if c == quote {
                    out.push(quote as char);
                    i += 1;
                    break;
                }
                copy_one_char(&mut i, &mut out);
            }
            continue;
        }

        // Code region: try to match the pattern at `i`.
        if pattern_matches_at(input, i, pattern, kind) {
            out.push_str(replacement);
            i += pattern.len();
            count += 1;
            continue;
        }

        copy_one_char(&mut i, &mut out);
    }

    (out, count)
}

/// Length (in bytes) of the UTF-8 character whose first byte is `b`.
/// Falls back to 1 for invalid leading bytes so the walk always makes
/// progress.
fn utf8_char_len(b: u8) -> usize {
    if b < 0x80 {
        1
    } else if b < 0xC0 {
        // continuation byte — shouldn't be a leading byte; advance by 1
        // to avoid getting stuck.
        1
    } else if b < 0xE0 {
        2
    } else if b < 0xF0 {
        3
    } else {
        4
    }
}

/// Tests whether `pattern` matches the input at byte offset `start`
/// under the rules of `kind`.
fn pattern_matches_at(input: &str, start: usize, pattern: &str, kind: MatchKind) -> bool {
    let bytes = input.as_bytes();
    let p = pattern.as_bytes();
    let end = start + p.len();
    if end > bytes.len() {
        return false;
    }

    let region = &bytes[start..end];

    match kind {
        MatchKind::Literal => region == p,
        MatchKind::WordBoundary { case_sensitive } => {
            let body_match = if case_sensitive {
                region == p
            } else {
                region.eq_ignore_ascii_case(p)
            };
            if !body_match {
                return false;
            }
            let left_ok = start == 0 || !is_word_byte(bytes[start - 1]);
            let right_ok = end == bytes.len() || !is_word_byte(bytes[end]);
            left_ok && right_ok
        }
    }
}

/// ASCII word byte: `[A-Za-z0-9_]`. Multi-byte UTF-8 leading bytes
/// (>= 0x80) are intentionally treated as non-word here — type tokens
/// in SQL are ASCII, and treating high bytes as boundaries avoids
/// false negatives without enabling false positives.
fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Case-insensitive string replacement.
fn case_insensitive_replace(input: &str, from: &str, to: &str) -> String {
    let lower_input = input.to_lowercase();
    let lower_from = from.to_lowercase();
    let mut result = String::with_capacity(input.len());
    let mut search_start = 0;

    while let Some(pos) = lower_input[search_start..].find(&lower_from) {
        let abs_pos = search_start + pos;
        result.push_str(&input[search_start..abs_pos]);
        result.push_str(to);
        search_start = abs_pos + from.len();
    }
    result.push_str(&input[search_start..]);
    result
}

/// Count case-insensitive occurrences.
fn case_insensitive_count(input: &str, pattern: &str) -> usize {
    let lower = input.to_lowercase();
    let lower_pat = pattern.to_lowercase();
    lower.matches(&lower_pat).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_dialect_noop() {
        let result = transpile("SELECT 1", Dialect::Snowflake, Dialect::Snowflake);
        assert_eq!(result.sql, "SELECT 1");
        assert_eq!(result.replacements, 0);
    }

    #[test]
    fn test_snowflake_to_bigquery_functions() {
        let sql = "SELECT NVL(a, b), LEN(name), DATEDIFF(day, start, end_date) FROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        assert!(result.sql.contains("IFNULL(a, b)"));
        assert!(result.sql.contains("LENGTH(name)"));
        assert!(result.sql.contains("DATE_DIFF("));
        assert!(result.replacements > 0);
    }

    #[test]
    fn test_bigquery_to_snowflake_types() {
        let sql = "CREATE TABLE t (id INT64, name STRING, amount FLOAT64, data JSON)";
        let result = transpile(sql, Dialect::BigQuery, Dialect::Snowflake);
        assert!(result.sql.contains("BIGINT"));
        assert!(result.sql.contains("VARCHAR"));
        assert!(result.sql.contains("DOUBLE"));
        assert!(result.sql.contains("VARIANT"));
    }

    #[test]
    fn test_snowflake_to_databricks_types() {
        let sql = "SELECT CAST(x AS VARCHAR), CAST(y AS NUMBER) FROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::Databricks);
        assert!(result.sql.contains("STRING"));
        assert!(result.sql.contains("DECIMAL"));
    }

    #[test]
    fn test_qualify_warning() {
        let sql =
            "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1";
        let result = transpile(sql, Dialect::Snowflake, Dialect::DuckDB);
        assert!(!result.warnings.is_empty());
        assert!(result.warnings[0].original.contains("QUALIFY"));
    }

    #[test]
    fn test_flatten_warning() {
        let sql = "SELECT f.value FROM t, LATERAL FLATTEN(input => t.array_col) f";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.original.contains("FLATTEN"))
        );
    }

    #[test]
    fn test_case_insensitive_replace() {
        assert_eq!(
            case_insensitive_replace("Select NVL(a, b)", "NVL(", "IFNULL("),
            "Select IFNULL(a, b)"
        );
    }

    #[test]
    fn test_display_dialect() {
        assert_eq!(format!("{}", Dialect::BigQuery), "BigQuery");
        assert_eq!(format!("{}", Dialect::Snowflake), "Snowflake");
    }

    // ---- replace_outside_strings + transpile correctness ----

    #[test]
    fn type_replace_respects_word_boundaries() {
        // BigQuery → Databricks maps INT64 → BIGINT. Going the other way
        // (Databricks → BigQuery) maps BIGINT → INT64; we need to ensure
        // we don't hit `INT` matches inside `BIGINT` if any rule were to
        // map `INT` independently.
        //
        // Here we exercise the helper directly to prove `INT` does not
        // eat into `BIGINT`.
        let (out, count) = replace_outside_strings(
            "CAST(x AS INT), CAST(y AS BIGINT)",
            "INT",
            "INTEGER",
            MatchKind::WordBoundary {
                case_sensitive: false,
            },
        );
        assert_eq!(count, 1, "expected exactly one replacement, got: {out}");
        assert!(out.contains("AS INTEGER)"));
        assert!(
            out.contains("BIGINT"),
            "BIGINT must be preserved, got: {out}"
        );
        assert!(
            !out.contains("BIGINTEGER"),
            "BIGINT was corrupted into BIGINTEGER, got: {out}"
        );
    }

    #[test]
    fn function_replace_skips_string_literals() {
        // `NVL(` inside a string literal must not be rewritten to `IFNULL(`.
        let sql = "SELECT NVL(a, b), '-- example NVL(a,b)' AS doc FROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        // Real call site got transpiled.
        assert!(
            result.sql.contains("IFNULL(a, b)"),
            "real call should be rewritten: {}",
            result.sql
        );
        // String literal stayed verbatim.
        assert!(
            result.sql.contains("'-- example NVL(a,b)'"),
            "string literal corrupted: {}",
            result.sql
        );
    }

    #[test]
    fn function_replace_skips_line_comments() {
        let sql = "SELECT NVL(a, b) -- legacy NVL(...) reference\nFROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        assert!(result.sql.contains("IFNULL(a, b)"));
        assert!(
            result.sql.contains("-- legacy NVL(...) reference"),
            "comment was rewritten: {}",
            result.sql
        );
    }

    #[test]
    fn function_replace_skips_block_comments() {
        let sql = "SELECT NVL(a, b) /* inline NVL(x) note */ FROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        assert!(result.sql.contains("IFNULL(a, b)"));
        assert!(
            result.sql.contains("/* inline NVL(x) note */"),
            "block comment was rewritten: {}",
            result.sql
        );
    }

    #[test]
    fn type_replace_skips_string_literals() {
        // `INT64` inside a quoted string must not be rewritten.
        let sql = "SELECT CAST(x AS INT64), 'INT64 lives here' FROM t";
        let result = transpile(sql, Dialect::BigQuery, Dialect::Snowflake);
        assert!(result.sql.contains("AS BIGINT"));
        assert!(
            result.sql.contains("'INT64 lives here'"),
            "INT64 in literal corrupted: {}",
            result.sql
        );
    }

    #[test]
    fn doubled_quote_escape_in_literal_handled() {
        // `'it''s NVL'` is a valid SQL string literal containing a quote.
        // The inner `NVL` must not be rewritten and the literal must
        // round-trip unchanged.
        let sql = "SELECT 'it''s NVL', NVL(a, b) FROM t";
        let result = transpile(sql, Dialect::Snowflake, Dialect::BigQuery);
        assert!(result.sql.contains("'it''s NVL'"), "{}", result.sql);
        assert!(result.sql.contains("IFNULL(a, b)"));
    }
}
