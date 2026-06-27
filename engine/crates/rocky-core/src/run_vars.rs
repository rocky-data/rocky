//! Per-run variables (`rocky run --var name=value`) substituted into model SQL.
//!
//! A run variable is an explicit, parse-visible placeholder of the form
//! `@var(name)` (or `@var(name, default)`) that the compiler resolves to a
//! caller-supplied **string** at compile/render time. The substitution is
//! textual — the operator owns any quoting or casting in their SQL:
//!
//! ```sql
//! SELECT * FROM raw.base WHERE region = '@var(region)'
//! ```
//!
//! invoked as `rocky run --var region=us` renders to
//!
//! ```sql
//! SELECT * FROM raw.base WHERE region = 'us'
//! ```
//!
//! This sits in the same `@`-family as the `@start_date` / `@end_date`
//! `time_interval` placeholders: a run-time substitution that stays visible
//! in the source. It is deliberately **distinct** from the config-time
//! `${ENV}` interpolation that `rocky.toml` performs while parsing — `${ENV}`
//! resolves connection/config values before the engine ever sees a model,
//! whereas `@var()` resolves the run's logical inputs at compile time.
//!
//! A `@var(name)` reference with no supplied value and no inline default is a
//! **missing** variable: [`substitute_run_vars`] reports its name so the
//! compiler can raise a clear error naming it. References that appear only
//! inside a SQL comment (`--` line or `/* … */` block) are ignored — they are
//! neither substituted nor counted as missing.
//!
//! ## Trust boundary
//!
//! The substitution is **textual, with no escaping of the substituted value** —
//! by design, for parity with dbt's `{{ var() }}`. The operator owns SQL
//! quoting and casting; only the variable *name* is validated (as a SQL
//! identifier). This is safe for the operator-supplied CLI `--var` values that
//! are the only source today. If a future caller ever feeds `--var` values
//! from a less-trusted source — for example a Dagster `run_vars` passthrough
//! driven by partition keys or external config — those values must be escaped
//! or bound before substitution rather than spliced in raw.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use regex::Regex;

/// Matches `@var(name)` and `@var(name, default)`.
///
/// - group 1 (`name`): a SQL-identifier-shaped variable name.
/// - group 2 (`default`): everything after the first comma up to the closing
///   paren, optional. Because it is `[^)]*`, an inline default may contain
///   spaces, commas, quotes, and `=`, but **not** a literal `)`.
static VAR_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"@var\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*(?:,\s*([^)]*?))?\s*\)")
        .expect("hardcoded @var() regex compiles")
});

/// Sentinel substituted for a *missing* required variable.
///
/// Leaving the raw `@var(x)` token in place can make the downstream SQL
/// parser emit a cryptic error that buries the clean "missing var" diagnostic
/// the compiler raises. Substituting a parseable `NULL` keeps the SQL valid in
/// both string (`'NULL'`) and bare (`= NULL`) positions; the compile is
/// aborted by the error diagnostic anyway, so this SQL is never executed.
const MISSING_SENTINEL: &str = "NULL";

/// Error parsing a `--var name=value` pair from the CLI.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RunVarParseError {
    /// The argument had no `=` separator.
    #[error("invalid --var '{0}': expected name=value (no '=' found)")]
    MissingEquals(String),
    /// The name portion (before `=`) was empty.
    #[error("invalid --var '{0}': variable name is empty")]
    EmptyName(String),
    /// The name portion was not a valid SQL identifier.
    #[error(
        "invalid --var name '{0}': must match [A-Za-z_][A-Za-z0-9_]* \
         (letters, digits, underscore; not starting with a digit)"
    )]
    InvalidName(String),
}

/// A resolved map of run variables, keyed by name.
///
/// Values are raw strings; the operator manages any SQL quoting/casting in
/// their model SQL around the `@var()` placeholder.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunVars {
    vars: BTreeMap<String, String>,
}

impl RunVars {
    /// An empty set of run variables.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether any variables are set.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.vars.is_empty()
    }

    /// Number of variables set.
    #[must_use]
    pub fn len(&self) -> usize {
        self.vars.len()
    }

    /// Look up a variable's value by name.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.vars.get(name).map(String::as_str)
    }

    /// Insert a single already-validated `name`/`value` pair, overwriting any
    /// previous value for `name`.
    pub fn insert(&mut self, name: impl Into<String>, value: impl Into<String>) {
        self.vars.insert(name.into(), value.into());
    }

    /// Parse a list of raw `name=value` CLI arguments into a [`RunVars`].
    ///
    /// The value may contain `=` (the split is on the **first** `=` only), so
    /// `--var filter=a=b` sets `filter` to `a=b`. A later occurrence of a name
    /// overwrites an earlier one.
    ///
    /// # Errors
    ///
    /// Returns [`RunVarParseError`] when an argument has no `=`, an empty
    /// name, or a name that is not a valid SQL identifier.
    pub fn parse_pairs<I, S>(pairs: I) -> Result<Self, RunVarParseError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut vars = BTreeMap::new();
        for raw in pairs {
            let raw = raw.as_ref();
            let Some((name, value)) = raw.split_once('=') else {
                return Err(RunVarParseError::MissingEquals(raw.to_string()));
            };
            if name.is_empty() {
                return Err(RunVarParseError::EmptyName(raw.to_string()));
            }
            if !is_valid_var_name(name) {
                return Err(RunVarParseError::InvalidName(name.to_string()));
            }
            vars.insert(name.to_string(), value.to_string());
        }
        Ok(Self { vars })
    }
}

/// Whether `name` is a valid run-variable identifier: `[A-Za-z_][A-Za-z0-9_]*`.
fn is_valid_var_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Result of a [`substitute_run_vars`] pass over one model's SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Substitution {
    /// SQL with every `@var()` occurrence resolved (supplied value, inline
    /// default, or [`MISSING_SENTINEL`] for missing-required).
    pub sql: String,
    /// Names of variables referenced without a supplied value and without an
    /// inline default — in first-seen order, de-duplicated.
    pub missing: Vec<String>,
}

/// Substitute every `@var(name)` / `@var(name, default)` occurrence in `sql`.
///
/// Resolution order for each occurrence:
/// 1. the supplied value from `vars`, when present;
/// 2. otherwise the inline default, when the reference is `@var(name, default)`;
/// 3. otherwise it is **missing**: the occurrence is replaced with a parseable
///    sentinel and `name` is recorded in [`Substitution::missing`].
///
/// The replacement is always literal text — a value containing `$` is **not**
/// interpreted as a regex capture reference.
///
/// References that fall inside a SQL comment — a `--` line comment or a
/// `/* … */` block comment — are **ignored**: left in the text verbatim and
/// never recorded as missing. Only executable SQL (including string literals,
/// where the operator owns the quoting) drives substitution. A `@var(name)`
/// mentioned only in a comment therefore does not turn into a missing-variable
/// error. Comment detection is string-literal-aware, so a `--` or `/*` inside a
/// `'…'` / `"…"` literal does **not** start a comment.
#[must_use]
pub fn substitute_run_vars(sql: &str, vars: &RunVars) -> Substitution {
    // Fast path: nothing to do (and no comment scan) when the marker is absent.
    if !sql.contains("@var(") {
        return Substitution {
            sql: sql.to_string(),
            missing: Vec::new(),
        };
    }

    let comments = comment_spans(sql);
    let in_comment = |pos: usize| {
        comments
            .iter()
            .any(|&(start, end)| pos >= start && pos < end)
    };

    let mut missing: Vec<String> = Vec::new();
    let resolved = VAR_RE.replace_all(sql, |caps: &regex::Captures<'_>| {
        let whole = caps.get(0).expect("group 0 always present");
        // A reference inside a comment is left exactly as written.
        if in_comment(whole.start()) {
            return whole.as_str().to_string();
        }
        let name = &caps[1];
        if let Some(value) = vars.get(name) {
            return value.to_string();
        }
        if let Some(default) = caps.get(2) {
            return default.as_str().trim().to_string();
        }
        if !missing.iter().any(|m| m == name) {
            missing.push(name.to_string());
        }
        MISSING_SENTINEL.to_string()
    });
    Substitution {
        sql: resolved.into_owned(),
        missing,
    }
}

/// State of the [`comment_spans`] scanner outside of any comment.
///
/// String-literal tracking lets the scanner tell a real comment opener from a
/// `--` / `/*` that merely sits inside a quoted string.
enum ScanMode {
    /// Executable SQL (substitution applies here).
    Code,
    /// Inside a single-quoted `'…'` string literal.
    SingleQuote,
    /// Inside a double-quoted `"…"` identifier/string.
    DoubleQuote,
}

/// Byte ranges (`start..end`, half-open) of every SQL comment in `sql`.
///
/// Recognises `--` line comments (up to but excluding the terminating newline)
/// and `/* … */` block comments (an unterminated block runs to end of input).
/// Comment openers inside a `'…'` or `"…"` literal are not treated as comments.
/// Returned ranges are non-overlapping and in ascending order; all boundaries
/// land on ASCII delimiters, so slicing `sql` at them is always char-safe.
fn comment_spans(sql: &str) -> Vec<(usize, usize)> {
    let bytes = sql.as_bytes();
    let n = bytes.len();
    let mut spans = Vec::new();
    let mut mode = ScanMode::Code;
    let mut i = 0;
    while i < n {
        match mode {
            ScanMode::Code => match bytes[i] {
                b'\'' => {
                    mode = ScanMode::SingleQuote;
                    i += 1;
                }
                b'"' => {
                    mode = ScanMode::DoubleQuote;
                    i += 1;
                }
                b'-' if i + 1 < n && bytes[i + 1] == b'-' => {
                    let start = i;
                    i += 2;
                    while i < n && bytes[i] != b'\n' {
                        i += 1;
                    }
                    spans.push((start, i));
                }
                b'/' if i + 1 < n && bytes[i + 1] == b'*' => {
                    let start = i;
                    i += 2;
                    while i < n && !(bytes[i] == b'*' && i + 1 < n && bytes[i + 1] == b'/') {
                        i += 1;
                    }
                    // Consume the closing `*/` when present; an unterminated
                    // block comment ends at EOF.
                    if i < n {
                        i += 2;
                    }
                    spans.push((start, i));
                }
                _ => i += 1,
            },
            ScanMode::SingleQuote => {
                if bytes[i] == b'\'' {
                    // A doubled `''` is an escaped quote — stay in the string.
                    if i + 1 < n && bytes[i + 1] == b'\'' {
                        i += 2;
                    } else {
                        mode = ScanMode::Code;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
            ScanMode::DoubleQuote => {
                if bytes[i] == b'"' {
                    if i + 1 < n && bytes[i + 1] == b'"' {
                        i += 2;
                    } else {
                        mode = ScanMode::Code;
                        i += 1;
                    }
                } else {
                    i += 1;
                }
            }
        }
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_simple_pair() {
        let vars = RunVars::parse_pairs(["region=us"]).unwrap();
        assert_eq!(vars.get("region"), Some("us"));
        assert_eq!(vars.len(), 1);
    }

    #[test]
    fn parse_value_with_equals_splits_on_first() {
        let vars = RunVars::parse_pairs(["clause=a=b=c"]).unwrap();
        assert_eq!(vars.get("clause"), Some("a=b=c"));
    }

    #[test]
    fn parse_empty_value_is_allowed() {
        let vars = RunVars::parse_pairs(["region="]).unwrap();
        assert_eq!(vars.get("region"), Some(""));
    }

    #[test]
    fn parse_no_equals_errors() {
        let err = RunVars::parse_pairs(["region"]).unwrap_err();
        assert_eq!(err, RunVarParseError::MissingEquals("region".to_string()));
    }

    #[test]
    fn parse_empty_name_errors() {
        let err = RunVars::parse_pairs(["=us"]).unwrap_err();
        assert_eq!(err, RunVarParseError::EmptyName("=us".to_string()));
    }

    #[test]
    fn parse_invalid_name_errors() {
        let err = RunVars::parse_pairs(["1region=us"]).unwrap_err();
        assert_eq!(err, RunVarParseError::InvalidName("1region".to_string()));
    }

    #[test]
    fn later_pair_overwrites_earlier() {
        let vars = RunVars::parse_pairs(["region=us", "region=eu"]).unwrap();
        assert_eq!(vars.get("region"), Some("eu"));
    }

    #[test]
    fn substitute_supplied_value() {
        let vars = RunVars::parse_pairs(["region=us"]).unwrap();
        let out = substitute_run_vars("WHERE region = '@var(region)'", &vars);
        assert_eq!(out.sql, "WHERE region = 'us'");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn substitute_bare_position() {
        let vars = RunVars::parse_pairs(["threshold=100"]).unwrap();
        let out = substitute_run_vars("WHERE amount >= @var(threshold)", &vars);
        assert_eq!(out.sql, "WHERE amount >= 100");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn substitute_default_used_when_unset() {
        let vars = RunVars::new();
        let out = substitute_run_vars("WHERE d = '@var(drop_date, 2024-01-01)'", &vars);
        assert_eq!(out.sql, "WHERE d = '2024-01-01'");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn substitute_default_overridden_by_supplied() {
        let vars = RunVars::parse_pairs(["drop_date=2025-12-31"]).unwrap();
        let out = substitute_run_vars("WHERE d = '@var(drop_date, 2024-01-01)'", &vars);
        assert_eq!(out.sql, "WHERE d = '2025-12-31'");
    }

    #[test]
    fn substitute_missing_required_reports_name() {
        let vars = RunVars::new();
        let out = substitute_run_vars("WHERE region = '@var(region)'", &vars);
        assert_eq!(out.missing, vec!["region".to_string()]);
        // Sentinel keeps the SQL parseable.
        assert_eq!(out.sql, "WHERE region = 'NULL'");
    }

    #[test]
    fn substitute_missing_deduped_in_order() {
        let vars = RunVars::new();
        let out = substitute_run_vars("@var(b) @var(a) @var(b)", &vars);
        assert_eq!(out.missing, vec!["b".to_string(), "a".to_string()]);
    }

    #[test]
    fn substitute_value_with_dollar_is_literal() {
        // A value containing `$1` must not be treated as a capture reference.
        let vars = RunVars::parse_pairs(["money=$1,000"]).unwrap();
        let out = substitute_run_vars("SELECT '@var(money)'", &vars);
        assert_eq!(out.sql, "SELECT '$1,000'");
    }

    #[test]
    fn substitute_repeated_occurrences() {
        let vars = RunVars::parse_pairs(["x=Z"]).unwrap();
        let out = substitute_run_vars("@var(x) @var(x)", &vars);
        assert_eq!(out.sql, "Z Z");
    }

    #[test]
    fn substitute_default_with_comma_and_quotes() {
        let vars = RunVars::new();
        let out = substitute_run_vars("IN (@var(list, 'a', 'b'))", &vars);
        assert_eq!(out.sql, "IN ('a', 'b')");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn substitute_no_var_is_identity() {
        let vars = RunVars::new();
        let sql = "SELECT * FROM t WHERE a = 1";
        let out = substitute_run_vars(sql, &vars);
        assert_eq!(out.sql, sql);
        assert!(out.missing.is_empty());
    }

    #[test]
    fn var_in_line_comment_is_ignored() {
        // The headline B3 case: a comment mentions @var but the executable
        // SQL has none, so it must compile clean (no missing, no rewrite).
        let vars = RunVars::new();
        let sql = "-- filter by @var(threshold) later\nSELECT region FROM raw__sales.sales";
        let out = substitute_run_vars(sql, &vars);
        assert_eq!(out.sql, sql);
        assert!(out.missing.is_empty());
    }

    #[test]
    fn var_in_block_comment_is_ignored() {
        let vars = RunVars::new();
        let sql = "SELECT 1 /* todo: @var(threshold) */ FROM t";
        let out = substitute_run_vars(sql, &vars);
        assert_eq!(out.sql, sql);
        assert!(out.missing.is_empty());
    }

    #[test]
    fn var_in_trailing_line_comment_not_missing_but_code_var_is() {
        // The executable @var is substituted/missing as usual; the commented
        // one is left verbatim and never counted.
        let vars = RunVars::new();
        let sql = "SELECT region FROM t WHERE region = '@var(region)' -- TODO @var(other)";
        let out = substitute_run_vars(sql, &vars);
        assert_eq!(
            out.sql,
            "SELECT region FROM t WHERE region = 'NULL' -- TODO @var(other)"
        );
        assert_eq!(out.missing, vec!["region".to_string()]);
    }

    #[test]
    fn var_inside_string_literal_still_substitutes_despite_dashes() {
        // `--` inside a string literal must NOT start a comment, so the later
        // @var in executable position is still substituted.
        let vars = RunVars::parse_pairs(["y=Z"]).unwrap();
        let out = substitute_run_vars("WHERE note = 'a -- b' AND x = @var(y)", &vars);
        assert_eq!(out.sql, "WHERE note = 'a -- b' AND x = Z");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn var_inside_string_with_block_marker_still_substitutes() {
        let vars = RunVars::parse_pairs(["y=Z"]).unwrap();
        let out = substitute_run_vars("WHERE note = 'a /* b' AND x = @var(y)", &vars);
        assert_eq!(out.sql, "WHERE note = 'a /* b' AND x = Z");
        assert!(out.missing.is_empty());
    }

    #[test]
    fn comment_spans_skips_markers_inside_strings() {
        // No comment: the `--` and `/*` live inside string literals.
        assert!(comment_spans("SELECT '-- not a comment', '/* nor this */'").is_empty());
        // A real line comment after a string is detected.
        let spans = comment_spans("SELECT 'x' -- c");
        assert_eq!(spans.len(), 1);
    }
}
