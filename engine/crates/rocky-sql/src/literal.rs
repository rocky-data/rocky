//! Dialect-owned encoding of SQL string literals.
//!
//! Splicing a user-supplied value into `'…'` by hand is wrong on at least
//! one warehouse whatever the hand-written rule is: the five dialects Rocky
//! ships split two ways on how their lexer reads a single-quoted string.
//! This module holds both encodings in one exhaustive `match` so a dialect
//! states a one-word *fact* ([`LiteralEscape`]) instead of carrying its own
//! copy of an algorithm.

/// How a warehouse's SQL lexer reads a single-quoted string literal.
///
/// This is a lexer fact, not a policy: each dialect answers which form its
/// parser accepts, and [`encode_string_literal`] owns the encoding for both.
/// The `match` there is exhaustive with no `_` arm, so adding a variant is a
/// compile error until its encoding is written and reviewed. It is
/// deliberately **not** `#[non_exhaustive]`, which would force a `_` arm on
/// every out-of-crate match — the opposite of what this type is for.
///
/// No `Serialize`/`Deserialize` yet, against the usual engine convention:
/// nothing persists or transports this value today. It becomes serializable
/// when the adapter SDK carries the fact in `AdapterManifest`, which is
/// separate, deferred work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiteralEscape {
    /// The lexer does not process backslash escapes inside `'…'`. A quote is
    /// escaped by doubling it (`''`); a backslash stands for itself.
    ///
    /// Trino and DuckDB. DuckDB's `E'…'` prefix *does* process backslashes,
    /// but Rocky never emits that form.
    Standard,

    /// The lexer processes backslash escapes inside `'…'`. A quote is escaped
    /// as `\'`, a backslash as `\\`, and a line break as `\n` / `\r`.
    ///
    /// Snowflake, Databricks/Spark and BigQuery. All three document `\n` and
    /// `\r`, and BigQuery's quoted string cannot hold a raw line break at all,
    /// so encoding it is the only form correct on every one of them.
    Backslash,
}

/// Encodes `value` as a complete single-quoted SQL string literal for `rule`.
///
/// The return value **includes the surrounding quotes**, so callers splice it
/// in whole (`format!("WHERE c = {literal}")`) rather than re-adding quotes at
/// every sink and disagreeing about which lexical form was produced.
///
/// There is no error path: both rules apply to any `&str`, and the result
/// decodes back to `value` on every dialect that answers `rule`. That is a
/// claim about the *lexer*, and nothing wider — a value can still be too long
/// for the warehouse's statement-size limit, or outside what its text type
/// holds. Refusal is a separate concern and stays with the per-field
/// validators.
///
/// # Examples
///
/// ```
/// use rocky_sql::literal::{LiteralEscape, encode_string_literal};
///
/// assert_eq!(encode_string_literal(LiteralEscape::Standard, "it's"), "'it''s'");
/// assert_eq!(encode_string_literal(LiteralEscape::Backslash, "it's"), r"'it\'s'");
///
/// // A backslash is literal under `Standard` and doubled under `Backslash`.
/// assert_eq!(encode_string_literal(LiteralEscape::Standard, r"C:\tmp"), r"'C:\tmp'");
/// assert_eq!(encode_string_literal(LiteralEscape::Backslash, r"C:\tmp"), r"'C:\\tmp'");
///
/// // A line break stays raw where the lexer reads no escapes, and becomes
/// // `\n` where it does — BigQuery rejects a raw one outright.
/// assert_eq!(encode_string_literal(LiteralEscape::Standard, "a\nb"), "'a\nb'");
/// assert_eq!(encode_string_literal(LiteralEscape::Backslash, "a\nb"), r"'a\nb'");
/// ```
#[must_use]
pub fn encode_string_literal(rule: LiteralEscape, value: &str) -> String {
    let body = match rule {
        // Trino and DuckDB read no escapes here, so a line break has to stay
        // raw — both accept one inside `'…'`, and `\n` would be two literal
        // characters.
        LiteralEscape::Standard => value.replace('\'', "''"),
        // Order is load-bearing, and only the FIRST pass matters: it is the
        // only one that rewrites an existing backslash, so the backslashes
        // the later passes add can never be doubled. Doing the quote first
        // instead would double them — `a\'b` would come out as `a` plus FOUR
        // backslashes plus a bare `'`, an even run that escapes itself and
        // leaves the quote free to close the literal. That is the bypass
        // `rocky-snowflake`'s governance encoder documents.
        LiteralEscape::Backslash => value
            .replace('\\', r"\\")
            .replace('\n', r"\n")
            .replace('\r', r"\r")
            .replace('\'', r"\'"),
    };
    format!("'{body}'")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_doubles_a_quote() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, "it's"),
            "'it''s'"
        );
    }

    #[test]
    fn standard_leaves_a_backslash_alone() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, r"C:\tmp"),
            r"'C:\tmp'"
        );
    }

    /// The bypass the Snowflake governance encoder documents, stated for
    /// `Standard`: a lexer that does not read backslashes cannot be escaped
    /// out of the literal by one, so `\'` needs only the quote doubled.
    #[test]
    fn standard_encodes_a_backslash_quote_pair() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, r"a\'b"),
            r"'a\''b'"
        );
    }

    #[test]
    fn standard_keeps_a_trailing_backslash() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, r"a\"),
            r"'a\'"
        );
    }

    #[test]
    fn standard_encodes_an_empty_value() {
        assert_eq!(encode_string_literal(LiteralEscape::Standard, ""), "''");
    }

    #[test]
    fn standard_passes_a_newline_through() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, "a\nb"),
            "'a\nb'"
        );
    }

    #[test]
    fn backslash_escapes_a_quote_with_a_backslash() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, "it's"),
            r"'it\'s'"
        );
    }

    #[test]
    fn backslash_doubles_a_backslash() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, r"C:\tmp"),
            r"'C:\\tmp'"
        );
    }

    /// This is the ordering mutation's target. Quote-first turns `a\'b` into
    /// `a\\'b`, and the backslash pass then doubles BOTH of those, giving
    /// `a` + four backslashes + a bare `'`. An even run of backslashes
    /// escapes itself, so nothing escapes the quote and it closes the literal
    /// early. Backslash-first gives the odd run below.
    #[test]
    fn backslash_encodes_a_backslash_quote_pair_without_reopening_the_bypass() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, r"a\'b"),
            r"'a\\\'b'"
        );
    }

    #[test]
    fn backslash_doubles_a_trailing_backslash() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, r"a\"),
            r"'a\\'"
        );
    }

    #[test]
    fn backslash_encodes_an_empty_value() {
        assert_eq!(encode_string_literal(LiteralEscape::Backslash, ""), "''");
    }

    /// A line break becomes `\n` / `\r`, not a raw one. Snowflake and
    /// Databricks accept both forms; BigQuery accepts only the escape — its
    /// spec says a quoted string "can't contain newlines, even when preceded
    /// by a backslash". Encoding it is what makes one `Backslash` rule
    /// correct on all three.
    #[test]
    fn backslash_encodes_a_line_break() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, "a\nb"),
            r"'a\nb'"
        );
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, "a\r\nb"),
            r"'a\r\nb'"
        );
    }

    /// The escape the line-break pass writes must not itself be doubled: a
    /// value already holding the two characters `\` and `n` has to survive as
    /// a backslash followed by an `n`, distinct from a real line break.
    #[test]
    fn backslash_keeps_a_literal_backslash_n_distinct_from_a_line_break() {
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, r"a\nb"),
            r"'a\\nb'"
        );
        assert_ne!(
            encode_string_literal(LiteralEscape::Backslash, r"a\nb"),
            encode_string_literal(LiteralEscape::Backslash, "a\nb")
        );
    }

    /// Every character other than `'`, `\` and a line break is carried
    /// verbatim, so a value survives a round trip through any dialect that
    /// reads the escapes back.
    #[test]
    fn both_rules_carry_other_characters_verbatim() {
        let value = "tab\there — ünïcode 漢字 \"double\" `tick` ; -- /* */ %_";
        assert_eq!(
            encode_string_literal(LiteralEscape::Standard, value),
            format!("'{value}'")
        );
        assert_eq!(
            encode_string_literal(LiteralEscape::Backslash, value),
            format!("'{value}'")
        );
    }
}
