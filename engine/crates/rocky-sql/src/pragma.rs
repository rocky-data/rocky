//! Per-model linting pragmas embedded in SQL line comments.
//!
//! Models can opt out of specific lint constructs by adding a `rocky-allow`
//! pragma to their SQL body, anywhere on a line that begins with `--`:
//!
//! ```sql
//! -- rocky-allow: NVL, QUALIFY
//! SELECT NVL(a, b) FROM t QUALIFY ROW_NUMBER() OVER (...) = 1
//! ```
//!
//! Construct names are case-insensitive identifiers matched against the
//! lint emitter's own labels (e.g. `PortabilityIssue::construct`). The
//! parser is dialect-independent and intentionally permissive: any pragma
//! it doesn't recognize is silently ignored, so future lint codes can
//! introduce new pragma names without breaking existing models.
//!
//! Wave 1 ships only `rocky-allow` (used to suppress P001 portability
//! diagnostics). The same parser will back a future `[lints]`-block driven
//! per-model toggle for P002 — see `project_rocky_active_priority` Arc 7
//! wave 2 backlog.

use std::collections::HashSet;

/// Set of lint construct labels a single model has opted out of.
///
/// Labels are upper-cased on insert so callers can compare against the
/// canonical construct names emitted by the lint side
/// (`PortabilityIssue::construct` is uppercase).
#[derive(Debug, Clone, Default)]
pub struct ModelPragmas {
    allowed_constructs: HashSet<String>,
}

impl ModelPragmas {
    /// Build an empty pragma set (no opt-outs).
    pub fn empty() -> Self {
        Self::default()
    }

    /// True if `construct` (case-insensitive) was listed in any
    /// `-- rocky-allow:` pragma in the model's SQL.
    pub fn allows(&self, construct: &str) -> bool {
        self.allowed_constructs
            .contains(&construct.to_ascii_uppercase())
    }

    /// Borrow the underlying allow set (already upper-cased).
    pub fn allowed_constructs(&self) -> &HashSet<String> {
        &self.allowed_constructs
    }

    /// Insert a construct label into the allow set. Internal use; tests
    /// and the project-level allow merge use this directly to avoid
    /// rebuilding through the SQL scanner.
    pub fn insert(&mut self, construct: impl AsRef<str>) {
        self.allowed_constructs
            .insert(construct.as_ref().trim().to_ascii_uppercase());
    }
}

/// Scan a SQL body for all `-- rocky-allow:` pragmas and collect the
/// allowed constructs into a [`ModelPragmas`] set.
///
/// The scanner is line-based and only inspects line comments (`--`); block
/// comments and string literals are not parsed because the pragma syntax
/// is conventionally placed on its own header line above the SQL body.
/// Multiple pragmas in the same model accumulate.
pub fn parse_pragmas(sql: &str) -> ModelPragmas {
    let mut pragmas = ModelPragmas::empty();
    for line in sql.lines() {
        let trimmed = line.trim_start();
        let Some(rest) = trimmed.strip_prefix("--") else {
            continue;
        };
        let rest = rest.trim_start();
        let Some(payload) = rest
            .strip_prefix("rocky-allow:")
            .or_else(|| rest.strip_prefix("rocky-allow :"))
            .or_else(|| {
                rest.strip_prefix("ROCKY-ALLOW:")
                    .or_else(|| rest.strip_prefix("ROCKY-ALLOW :"))
            })
        else {
            continue;
        };
        for token in payload.split(',') {
            let cleaned = token.trim();
            if !cleaned.is_empty() {
                pragmas.insert(cleaned);
            }
        }
    }
    pragmas
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_sql_has_no_pragmas() {
        let p = parse_pragmas("SELECT 1");
        assert!(p.allowed_constructs().is_empty());
    }

    #[test]
    fn parses_single_construct() {
        let p = parse_pragmas("-- rocky-allow: NVL\nSELECT NVL(a, b) FROM t");
        assert!(p.allows("NVL"));
        assert!(!p.allows("QUALIFY"));
    }

    #[test]
    fn parses_comma_separated_list() {
        let p = parse_pragmas("-- rocky-allow: NVL, QUALIFY, ILIKE\nSELECT 1");
        assert!(p.allows("NVL"));
        assert!(p.allows("QUALIFY"));
        assert!(p.allows("ILIKE"));
        assert!(!p.allows("DATE_ADD"));
    }

    #[test]
    fn case_insensitive_matching() {
        let p = parse_pragmas("-- rocky-allow: nvl, Qualify\nSELECT 1");
        assert!(p.allows("NVL"));
        assert!(p.allows("nvl"));
        assert!(p.allows("QUALIFY"));
        assert!(p.allows("qualify"));
    }

    #[test]
    fn multiple_pragmas_accumulate() {
        let p =
            parse_pragmas("-- rocky-allow: NVL\n-- rocky-allow: QUALIFY\nSELECT NVL(a, b) FROM t");
        assert!(p.allows("NVL"));
        assert!(p.allows("QUALIFY"));
    }

    #[test]
    fn ignores_unrelated_comments() {
        let p = parse_pragmas("-- this is just a comment\n-- TODO: refactor\nSELECT 1");
        assert!(p.allowed_constructs().is_empty());
    }

    #[test]
    fn allows_indentation_before_dashes() {
        let p = parse_pragmas("    -- rocky-allow: NVL\n  SELECT 1");
        assert!(p.allows("NVL"));
    }

    #[test]
    fn handles_uppercase_pragma_keyword() {
        let p = parse_pragmas("-- ROCKY-ALLOW: NVL\nSELECT 1");
        assert!(p.allows("NVL"));
    }

    #[test]
    fn skips_empty_tokens_in_list() {
        let p = parse_pragmas("-- rocky-allow: NVL,, QUALIFY,\nSELECT 1");
        assert!(p.allows("NVL"));
        assert!(p.allows("QUALIFY"));
        assert_eq!(p.allowed_constructs().len(), 2);
    }

    #[test]
    fn ignores_pragma_without_colon() {
        // The colon is the discriminator — without it, this is an ordinary
        // comment that happens to mention the keyword, not a directive.
        let p = parse_pragmas("-- rocky-allow NVL\nSELECT 1");
        assert!(!p.allows("NVL"));
    }

    #[test]
    fn ignores_pragma_inside_string_literal() {
        // Pragma scanning is line-comment-only, so a string literal that
        // happens to contain the pragma syntax is correctly ignored.
        let p = parse_pragmas("SELECT '-- rocky-allow: NVL' AS msg FROM t");
        assert!(!p.allows("NVL"));
    }

    #[test]
    fn empty_pragma_with_no_constructs_is_noop() {
        let p = parse_pragmas("-- rocky-allow:\nSELECT 1");
        assert!(p.allowed_constructs().is_empty());
    }
}
