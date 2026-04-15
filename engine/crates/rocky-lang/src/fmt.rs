//! Rocky DSL formatter.
//!
//! Shared by the CLI (`rocky fmt`) and the LSP formatting provider so both
//! use identical formatting rules.

/// Pipeline keywords that are left-aligned at depth 0.
pub const PIPELINE_KEYWORDS: &[&str] = &[
    "from",
    "where",
    "group",
    "derive",
    "select",
    "join",
    "sort",
    "take",
    "distinct",
    "replicate",
];

/// Returns true if `trimmed` starts with a pipeline keyword followed by a
/// word boundary (whitespace, `{`, end-of-string, etc.).
pub fn is_pipeline_keyword(trimmed: &str) -> bool {
    for &kw in PIPELINE_KEYWORDS {
        if let Some(rest) = trimmed.strip_prefix(kw) {
            if rest.is_empty() || !rest.as_bytes()[0].is_ascii_alphanumeric() {
                return true;
            }
        }
    }
    false
}

/// Format a `.rocky` file's content.
///
/// Rules (same as the VS Code formatting provider):
/// 1. Trim trailing whitespace on every line.
/// 2. Normalize indentation: content inside `{ }` indented one level.
///    Top-level pipeline keywords left-aligned at depth 0.
/// 3. Collapse 3+ consecutive blank lines into exactly 2.
/// 4. Ensure file ends with exactly one newline.
pub fn format_rocky(text: &str, indent: &str) -> String {
    let mut lines: Vec<String> = text.split('\n').map(|l| l.trim_end().to_string()).collect();

    // Normalize indentation based on brace nesting.
    lines = normalize_indentation(&lines, indent);

    // Collapse 3+ consecutive blank lines into 2.
    lines = collapse_blank_lines(&lines);

    // Join and ensure single trailing newline.
    let mut result = lines.join("\n");
    let trimmed_end = result.trim_end_matches('\n');
    let len = trimmed_end.len();
    result.truncate(len);
    result.push('\n');
    result
}

/// Re-indent lines so that content inside `{ ... }` blocks is indented one
/// level deeper than the opening line. Top-level pipeline keywords sit at
/// column 0.
fn normalize_indentation(lines: &[String], indent: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::with_capacity(lines.len());
    let mut depth: usize = 0;

    for raw in lines {
        let trimmed = raw.trim();

        // Blank lines and pure-comment lines.
        if trimmed.is_empty() || trimmed.starts_with("--") {
            if trimmed.starts_with("--") && depth > 0 {
                out.push(format!("{}{trimmed}", indent.repeat(depth)));
            } else {
                out.push(trimmed.to_string());
            }
            continue;
        }

        // Closing brace: decrease depth *before* emitting.
        let leading_closes = count_leading_char(trimmed, b'}');
        if leading_closes > 0 {
            depth = depth.saturating_sub(leading_closes);
        }

        // Emit line at current depth.
        if depth == 0 && is_pipeline_keyword(trimmed) {
            out.push(trimmed.to_string());
        } else {
            out.push(format!("{}{trimmed}", indent.repeat(depth)));
        }

        // Opening braces: increase depth *after* emitting.
        let net = count_braces(trimmed);
        if net > 0 {
            depth += net as usize;
        } else if net < 0 {
            depth = depth.saturating_sub((-net) as usize);
        }
    }

    out
}

/// Count leading occurrences of `ch` at the very start of `trimmed`.
fn count_leading_char(trimmed: &str, ch: u8) -> usize {
    let mut n = 0;
    for &b in trimmed.as_bytes() {
        if b == ch {
            n += 1;
        } else {
            break;
        }
    }
    n
}

/// Returns the *net* brace count for a line (opens minus closes), ignoring
/// braces inside string literals and after `--` comments.
fn count_braces(line: &str) -> i32 {
    let code = strip_trailing_comment(line);
    let mut net: i32 = 0;
    let mut in_single = false;
    let mut in_double = false;

    for ch in code.chars() {
        if ch == '\'' && !in_double {
            in_single = !in_single;
        } else if ch == '"' && !in_single {
            in_double = !in_double;
        } else if !in_single && !in_double {
            if ch == '{' {
                net += 1;
            } else if ch == '}' {
                net -= 1;
            }
        }
    }
    net
}

/// Collapse 3+ consecutive blank lines into exactly 2.
fn collapse_blank_lines(lines: &[String]) -> Vec<String> {
    let mut out = Vec::with_capacity(lines.len());
    let mut blank_run = 0;

    for line in lines {
        if line.is_empty() {
            blank_run += 1;
            if blank_run <= 2 {
                out.push(String::new());
            }
        } else {
            blank_run = 0;
            out.push(line.clone());
        }
    }
    out
}

/// Strip trailing `-- ...` comment from a line, respecting string literals.
fn strip_trailing_comment(line: &str) -> &str {
    let bytes = line.as_bytes();
    let mut in_single = false;
    let mut in_double = false;

    if bytes.len() < 2 {
        return line;
    }

    for i in 0..bytes.len() - 1 {
        let ch = bytes[i];
        if ch == b'\'' && !in_double {
            in_single = !in_single;
        } else if ch == b'"' && !in_single {
            in_double = !in_double;
        } else if ch == b'-' && bytes[i + 1] == b'-' && !in_single && !in_double {
            return &line[..i];
        }
    }
    line
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_already_formatted() {
        let input = "from raw_orders\nwhere status != \"cancelled\"\n";
        assert_eq!(format_rocky(input, "    "), input);
    }

    #[test]
    fn test_trailing_whitespace() {
        let input = "from raw_orders   \nwhere status != \"cancelled\"  \n";
        let expected = "from raw_orders\nwhere status != \"cancelled\"\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_brace_indentation() {
        let input = "group customer_id {\ntotal: sum(amount),\ncount: count()\n}\n";
        let expected = "group customer_id {\n    total: sum(amount),\n    count: count()\n}\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_pipeline_keywords_left_aligned() {
        let input = "  from raw_orders\n  where status != \"cancelled\"\n  sort amount desc\n";
        let expected = "from raw_orders\nwhere status != \"cancelled\"\nsort amount desc\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_collapse_blank_lines() {
        let input = "from orders\n\n\n\n\nwhere true\n";
        let expected = "from orders\n\n\nwhere true\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_single_trailing_newline() {
        let input = "from orders\n\n\n";
        let expected = "from orders\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_no_trailing_newline_added() {
        let input = "from orders";
        let expected = "from orders\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_nested_braces() {
        let input = "derive {\na: 1,\nb: match {\ntrue => 2\n}\n}\n";
        let expected = "derive {\n    a: 1,\n    b: match {\n        true => 2\n    }\n}\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_comments_indented_at_depth() {
        let input = "group id {\n-- a comment\ntotal: sum(x)\n}\n";
        let expected = "group id {\n    -- a comment\n    total: sum(x)\n}\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_top_level_comments_not_indented() {
        let input = "-- top level comment\nfrom orders\n";
        assert_eq!(format_rocky(input, "    "), input);
    }

    #[test]
    fn test_braces_in_strings_ignored() {
        let input = "where name != \"some{thing}\"\n";
        assert_eq!(format_rocky(input, "    "), input);
    }

    #[test]
    fn test_full_pipeline() {
        let input = "\
-- Customer orders aggregation (Rocky DSL)
from raw_orders
where status != \"cancelled\"
group customer_id {
    total_revenue: sum(amount),
    order_count: count(),
    first_order: min(order_date)
}
where total_revenue > 0
";
        // Should be unchanged (already formatted).
        assert_eq!(format_rocky(input, "    "), input);
    }

    #[test]
    fn test_keyword_prefix_not_matched() {
        // "fromage" starts with "from" but is not a pipeline keyword
        let input = "    fromage something\n";
        // At depth 0, non-keyword lines get depth*indent prefix, which is ""
        let expected = "fromage something\n";
        assert_eq!(format_rocky(input, "    "), expected);
    }

    #[test]
    fn test_is_pipeline_keyword() {
        assert!(is_pipeline_keyword("from orders"));
        assert!(is_pipeline_keyword("from"));
        assert!(is_pipeline_keyword("where x > 1"));
        assert!(is_pipeline_keyword("group id {"));
        assert!(is_pipeline_keyword("derive {"));
        assert!(is_pipeline_keyword("select {"));
        assert!(is_pipeline_keyword("join orders on id"));
        assert!(is_pipeline_keyword("sort amount desc"));
        assert!(is_pipeline_keyword("take 10"));
        assert!(is_pipeline_keyword("distinct"));
        assert!(is_pipeline_keyword("replicate"));
        assert!(!is_pipeline_keyword("fromage"));
        assert!(!is_pipeline_keyword("selected"));
    }
}
