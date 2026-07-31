//! Canonicalization of a spelled SQL table identifier into comparable parts.
//!
//! A table reference harvested from the SQL lineage walk arrives as the string
//! sqlparser rendered from its `ObjectName` — **with** whatever quote
//! characters the author wrote (`"main"."orders"`). Every consumer that wants
//! to match such a reference against a model's structured `config.target`
//! parts has to strip those quotes the same way, so the stripping lives here
//! once rather than once per consumer.
//!
//! Two consumers share it today and fail closed in opposite directions, which
//! is why this returns the parts rather than a verdict:
//!
//! * `rocky_compiler::resolve` derives DAG **ordering** edges — an
//!   un-canonicalizable identity yields no producer, so no edge.
//! * `rocky_cli::commands::containment` derives failure-**closure** edges — an
//!   un-canonicalizable identity is ambiguous, so the reader is withheld.

/// Split a possibly-quoted SQL table identifier into its lowercased, unquoted
/// parts, or `None` when it can't be cleanly canonicalized.
///
/// Quote styles recognized: double-quote `"…"` (DuckDB / Snowflake /
/// Postgres), backtick `` `…` `` (BigQuery / Databricks), bracket `[…]`
/// (T-SQL). Inside a quoted segment a `.` is a literal, not a separator; a
/// doubled closing quote (`""`, ` `` `) is an escaped quote character.
///
/// Lowercasing is **unconditional** and takes no dialect rules, deliberately:
/// the question a caller asks with these parts is *"could this reference and
/// that target be the same warehouse object?"* — [`crate::defer::CollisionIdentity`]'s
/// question, whose doc records why answering "different" for two spellings
/// that name one object is the unrecoverable direction. See that type before
/// making this case-aware.
///
/// Returns `None` — so the caller **fails closed** in whichever direction is
/// its own — on any identity that can't be unambiguously slotted into a
/// `schema.table` index: unbalanced quotes, an empty segment (`a..b`, a
/// trailing dot), or a segment that after unquoting still contains a `.` (a
/// quoted identifier with an embedded dot). Doing string surgery here rather
/// than a naive `replace('"', "")` is deliberate — the naive form would
/// mis-split a quoted identifier that legitimately contains a dot.
#[must_use]
pub fn canonicalize_identifier(read: &str) -> Option<Vec<String>> {
    let mut parts: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut chars = read.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' | '`' | '[' => {
                let close = if ch == '[' { ']' } else { ch };
                loop {
                    match chars.next() {
                        // Unbalanced quote — cannot canonicalize.
                        None => return None,
                        Some(c) if c == close => {
                            // A doubled closing quote (`""` / ` `` `) is an
                            // escaped literal, not the end of the segment.
                            // Brackets have no doubling convention.
                            if close != ']' && chars.peek() == Some(&close) {
                                chars.next();
                                cur.push(close);
                            } else {
                                break;
                            }
                        }
                        Some(c) => cur.push(c),
                    }
                }
            }
            '.' => parts.push(std::mem::take(&mut cur)),
            c => cur.push(c),
        }
    }
    parts.push(cur);
    // An empty segment or an embedded dot (from a quoted identifier) can't be
    // slotted into the `schema.table` index — fail closed.
    if parts.iter().any(|p| p.is_empty() || p.contains('.')) {
        return None;
    }
    Some(parts.into_iter().map(|p| p.to_lowercase()).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parts(v: &[&str]) -> Option<Vec<String>> {
        Some(v.iter().map(|s| (*s).to_string()).collect())
    }

    #[test]
    fn unquoted_and_quoted_spellings_canonicalize_alike() {
        assert_eq!(
            canonicalize_identifier("main.orders"),
            parts(&["main", "orders"])
        );
        assert_eq!(
            canonicalize_identifier("\"MAIN\".\"Orders\""),
            parts(&["main", "orders"]),
            "double quotes stripped and folded"
        );
        assert_eq!(
            canonicalize_identifier("`cat`.`marts`.`t`"),
            parts(&["cat", "marts", "t"]),
            "backticks stripped"
        );
        assert_eq!(
            canonicalize_identifier("[cat].[marts].[t]"),
            parts(&["cat", "marts", "t"]),
            "brackets stripped"
        );
        assert_eq!(
            canonicalize_identifier("main.\"orders\""),
            parts(&["main", "orders"]),
            "mixed quoting"
        );
        assert_eq!(
            canonicalize_identifier("\"a\"\"b\""),
            parts(&["a\"b"]),
            "a doubled quote is an escaped literal, not a terminator"
        );
    }

    #[test]
    fn un_slottable_identities_are_refused() {
        assert_eq!(
            canonicalize_identifier("\"main\".\"orders"),
            None,
            "unbalanced quote"
        );
        assert_eq!(canonicalize_identifier("\"we.ird\""), None, "embedded dot");
        assert_eq!(canonicalize_identifier("a..b"), None, "empty segment");
        assert_eq!(canonicalize_identifier(".orders"), None, "leading dot");
        assert_eq!(canonicalize_identifier("orders."), None, "trailing dot");
    }
}
