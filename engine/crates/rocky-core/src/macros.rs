//! Macro / template-include system for Rocky SQL models.
//!
//! Macros are reusable SQL fragments defined in `.sql` files under a
//! `macros/` directory. They provide a simple, non-Jinja approach to
//! eliminating repeated SQL patterns (surrogate key generation, standard
//! filters, SCD2 queries, etc.).
//!
//! ## Macro file format
//!
//! The first line of each `.sql` file declares the macro signature:
//!
//! ```sql
//! -- @macro surrogate_key(columns, separator)
//! MD5(CONCAT_WS({separator}, {columns}))
//! ```
//!
//! The declaration line uses the syntax `-- @macro name(param1, param2)`.
//! The rest of the file is the SQL body, where `{param}` placeholders
//! are replaced with the caller's arguments during expansion.
//!
//! ## Invocation syntax
//!
//! Models reference macros via `@macro_name(arg1, arg2)` in their SQL:
//!
//! ```sql
//! SELECT
//!     @surrogate_key(id || name, '|') AS sk,
//!     *
//! FROM source_table
//! ```
//!
//! ## Expansion
//!
//! Macro expansion is pure text substitution that runs before SQL parsing.
//! Nested macros are supported: if a macro body contains `@other_macro(…)`
//! calls, those are expanded recursively (up to a configurable depth limit
//! to prevent infinite recursion).
//!
//! ## Constraints
//!
//! - Macros are pure: no side effects, no mutable state.
//! - Arguments are literal text — no expression evaluation.
//! - Argument delimiters respect parenthesis nesting, so
//!   `@my_macro(CONCAT(a, b), c)` correctly parses as two arguments.

use std::path::Path;

use thiserror::Error;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from macro loading and expansion.
#[derive(Debug, Error)]
pub enum MacroError {
    #[error("failed to read macro file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    #[error("macro directory does not exist: {0}")]
    DirNotFound(String),

    #[error(
        "macro file '{path}' is missing the declaration line (expected `-- @macro name(params)`)"
    )]
    MissingDeclaration { path: String },

    #[error("invalid macro declaration in '{path}': {reason}")]
    InvalidDeclaration { path: String, reason: String },

    #[error("duplicate macro name '{name}' (first defined in '{first}', redefined in '{second}')")]
    DuplicateName {
        name: String,
        first: String,
        second: String,
    },

    #[error("unknown macro '@{name}' referenced in SQL")]
    UnknownMacro { name: String },

    #[error("macro '@{name}' expects {expected} argument(s) but got {actual}")]
    ArgCountMismatch {
        name: String,
        expected: usize,
        actual: usize,
    },

    #[error("macro expansion exceeded maximum depth ({depth}); possible circular reference")]
    MaxDepthExceeded { depth: usize },
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A single macro definition loaded from a `.sql` file.
#[derive(Debug, Clone)]
pub struct MacroDef {
    /// Macro name (e.g., `surrogate_key`).
    pub name: String,
    /// Ordered parameter names (e.g., `["columns", "separator"]`).
    pub params: Vec<String>,
    /// SQL body template with `{param}` placeholders.
    pub body: String,
    /// Path to the source file (for diagnostics).
    pub source_path: String,
}

/// Maximum recursion depth for nested macro expansion.
const MAX_EXPANSION_DEPTH: usize = 16;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Parses the declaration line `-- @macro name(param1, param2)`.
///
/// Returns `(name, params)` on success.
fn parse_declaration(line: &str, path: &str) -> Result<(String, Vec<String>), MacroError> {
    let trimmed = line.trim();

    let Some(rest) = trimmed.strip_prefix("--") else {
        return Err(MacroError::MissingDeclaration {
            path: path.to_string(),
        });
    };
    let rest = rest.trim();

    let Some(rest) = rest.strip_prefix("@macro") else {
        return Err(MacroError::MissingDeclaration {
            path: path.to_string(),
        });
    };
    let rest = rest.trim();

    // Split into name and params: `name(param1, param2)`
    let Some(paren_start) = rest.find('(') else {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "missing opening parenthesis".to_string(),
        });
    };

    let Some(paren_end) = rest.rfind(')') else {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "missing closing parenthesis".to_string(),
        });
    };

    if paren_end <= paren_start {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "malformed parentheses".to_string(),
        });
    }

    let name = rest[..paren_start].trim().to_string();
    if name.is_empty() {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "macro name is empty".to_string(),
        });
    }

    // Validate macro name: only alphanumeric and underscores.
    if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: format!(
                "macro name '{name}' contains invalid characters (only a-z, A-Z, 0-9, _ allowed)"
            ),
        });
    }

    let params_str = &rest[paren_start + 1..paren_end];
    let params: Vec<String> = if params_str.trim().is_empty() {
        Vec::new()
    } else {
        params_str
            .split(',')
            .map(|p| p.trim().to_string())
            .collect()
    };

    // Validate parameter names.
    for p in &params {
        if p.is_empty() {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: "empty parameter name".to_string(),
            });
        }
        if !p.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: format!("parameter name '{p}' contains invalid characters"),
            });
        }
    }

    Ok((name, params))
}

/// Parses a single macro `.sql` file into a [`MacroDef`].
fn parse_macro_file(content: &str, path: &str) -> Result<MacroDef, MacroError> {
    let mut lines = content.lines();

    let first_line = lines.next().ok_or_else(|| MacroError::MissingDeclaration {
        path: path.to_string(),
    })?;

    let (name, params) = parse_declaration(first_line, path)?;

    let body: String = lines.collect::<Vec<_>>().join("\n").trim().to_string();

    Ok(MacroDef {
        name,
        params,
        body,
        source_path: path.to_string(),
    })
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Discovers and loads all `.sql` macro files from a directory.
///
/// Non-recursive — only top-level `.sql` files are discovered. Files are
/// sorted by name for deterministic ordering. Returns an error if two files
/// define macros with the same name.
pub fn load_macros_from_dir(dir: &Path) -> Result<Vec<MacroDef>, MacroError> {
    if !dir.is_dir() {
        return Err(MacroError::DirNotFound(dir.display().to_string()));
    }

    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| MacroError::ReadFile {
            path: dir.display().to_string(),
            source: e,
        })?
        .filter_map(std::result::Result::ok)
        .collect();

    // Sort for deterministic ordering.
    entries.sort_by_key(std::fs::DirEntry::file_name);

    let mut macros = Vec::new();

    for entry in entries {
        let path = entry.path();
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");

        if ext != "sql" {
            continue;
        }

        let path_str = path.display().to_string();
        let content = std::fs::read_to_string(&path).map_err(|e| MacroError::ReadFile {
            path: path_str.clone(),
            source: e,
        })?;

        let macro_def = parse_macro_file(&content, &path_str)?;

        // Check for duplicates.
        if let Some(existing) = macros.iter().find(|m: &&MacroDef| m.name == macro_def.name) {
            return Err(MacroError::DuplicateName {
                name: macro_def.name,
                first: existing.source_path.clone(),
                second: path_str,
            });
        }

        macros.push(macro_def);
    }

    Ok(macros)
}

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

/// Splits a macro argument string into individual arguments, respecting
/// parenthesis nesting so that `CONCAT(a, b), c` parses as two arguments:
/// `["CONCAT(a, b)", "c"]`.
fn split_args(s: &str) -> Vec<String> {
    let mut args = Vec::new();
    let mut current = String::new();
    let mut depth: usize = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                current.push(ch);
            }
            ')' => {
                depth = depth.saturating_sub(1);
                current.push(ch);
            }
            ',' if depth == 0 => {
                args.push(current.trim().to_string());
                current = String::new();
            }
            _ => {
                current.push(ch);
            }
        }
    }

    let trimmed = current.trim().to_string();
    if !trimmed.is_empty() || !args.is_empty() {
        args.push(trimmed);
    }

    args
}

// ---------------------------------------------------------------------------
// Expansion
// ---------------------------------------------------------------------------

/// Expands all `@macro_name(args)` invocations in the given SQL string.
///
/// Nested macros are expanded recursively up to [`MAX_EXPANSION_DEPTH`].
/// Returns the fully-expanded SQL, or an error if a referenced macro is
/// unknown, an argument count mismatches, or the recursion limit is hit.
pub fn expand_macros(sql: &str, macros: &[MacroDef]) -> Result<String, MacroError> {
    expand_macros_inner(sql, macros, 0)
}

fn expand_macros_inner(sql: &str, macros: &[MacroDef], depth: usize) -> Result<String, MacroError> {
    if depth > MAX_EXPANSION_DEPTH {
        return Err(MacroError::MaxDepthExceeded {
            depth: MAX_EXPANSION_DEPTH,
        });
    }

    let mut result = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == '@'
            && i + 1 < len
            && (chars[i + 1].is_ascii_alphabetic() || chars[i + 1] == '_')
        {
            // Potential macro invocation. Read the name.
            let name_start = i + 1;
            let mut j = name_start;
            while j < len && (chars[j].is_ascii_alphanumeric() || chars[j] == '_') {
                j += 1;
            }

            // Check if followed by '(' — if not, it's a plain @identifier
            // (e.g., @start_date used by time_interval models).
            if j < len && chars[j] == '(' {
                let name: String = chars[name_start..j].iter().collect();

                // Find the matching closing paren, respecting nesting.
                let args_start = j + 1;
                let mut paren_depth: usize = 1;
                let mut k = args_start;
                while k < len && paren_depth > 0 {
                    match chars[k] {
                        '(' => paren_depth += 1,
                        ')' => paren_depth -= 1,
                        _ => {}
                    }
                    if paren_depth > 0 {
                        k += 1;
                    }
                }

                if paren_depth != 0 {
                    // Unmatched paren — just emit the `@` literally.
                    result.push('@');
                    i += 1;
                    continue;
                }

                let args_str: String = chars[args_start..k].iter().collect();
                let args = split_args(&args_str);

                // Look up the macro.
                let Some(macro_def) = macros.iter().find(|m| m.name == name) else {
                    return Err(MacroError::UnknownMacro { name });
                };

                // Validate arg count.
                let actual = if args.len() == 1 && args[0].is_empty() {
                    0
                } else {
                    args.len()
                };
                if actual != macro_def.params.len() {
                    return Err(MacroError::ArgCountMismatch {
                        name: name.clone(),
                        expected: macro_def.params.len(),
                        actual,
                    });
                }

                // Substitute parameters into the body.
                let mut expanded = macro_def.body.clone();
                for (param, arg) in macro_def.params.iter().zip(&args) {
                    expanded = expanded.replace(&format!("{{{param}}}"), arg);
                }

                // Recursively expand nested macros in the result.
                let expanded = expand_macros_inner(&expanded, macros, depth + 1)?;
                result.push_str(&expanded);

                // Advance past the closing paren.
                i = k + 1;
            } else {
                // Not a macro call — preserve the `@identifier` literally.
                result.push('@');
                i += 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // -- Declaration parsing --------------------------------------------------

    #[test]
    fn parse_declaration_basic() {
        let (name, params) =
            parse_declaration("-- @macro surrogate_key(columns, separator)", "test.sql").unwrap();
        assert_eq!(name, "surrogate_key");
        assert_eq!(params, vec!["columns", "separator"]);
    }

    #[test]
    fn parse_declaration_no_params() {
        let (name, params) = parse_declaration("-- @macro current_ts()", "test.sql").unwrap();
        assert_eq!(name, "current_ts");
        assert!(params.is_empty());
    }

    #[test]
    fn parse_declaration_single_param() {
        let (name, params) = parse_declaration("-- @macro safe_divide(expr)", "test.sql").unwrap();
        assert_eq!(name, "safe_divide");
        assert_eq!(params, vec!["expr"]);
    }

    #[test]
    fn parse_declaration_extra_whitespace() {
        let (name, params) =
            parse_declaration("--   @macro   my_macro(  a ,  b  )  ", "test.sql").unwrap();
        assert_eq!(name, "my_macro");
        assert_eq!(params, vec!["a", "b"]);
    }

    #[test]
    fn parse_declaration_missing_prefix() {
        let err = parse_declaration("@macro foo(x)", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::MissingDeclaration { .. }));
    }

    #[test]
    fn parse_declaration_missing_parens() {
        let err = parse_declaration("-- @macro foo", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
    }

    #[test]
    fn parse_declaration_empty_name() {
        let err = parse_declaration("-- @macro (x, y)", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
    }

    #[test]
    fn parse_declaration_invalid_name_chars() {
        let err = parse_declaration("-- @macro my-macro(x)", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
    }

    #[test]
    fn parse_declaration_empty_param_name() {
        let err = parse_declaration("-- @macro foo(a, , b)", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
    }

    // -- Macro file parsing ---------------------------------------------------

    #[test]
    fn parse_macro_file_basic() {
        let content = "-- @macro surrogate_key(columns, sep)\nMD5(CONCAT_WS({sep}, {columns}))";
        let m = parse_macro_file(content, "test.sql").unwrap();
        assert_eq!(m.name, "surrogate_key");
        assert_eq!(m.params, vec!["columns", "sep"]);
        assert_eq!(m.body, "MD5(CONCAT_WS({sep}, {columns}))");
    }

    #[test]
    fn parse_macro_file_multiline_body() {
        let content = "\
-- @macro scd2_merge(source, target, key)
MERGE INTO {target} t
USING {source} s
ON t.{key} = s.{key}
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *";
        let m = parse_macro_file(content, "test.sql").unwrap();
        assert_eq!(m.name, "scd2_merge");
        assert!(m.body.contains("MERGE INTO {target} t"));
        assert!(m.body.contains("WHEN NOT MATCHED THEN INSERT *"));
    }

    #[test]
    fn parse_macro_file_empty_body() {
        let content = "-- @macro noop()";
        let m = parse_macro_file(content, "test.sql").unwrap();
        assert_eq!(m.name, "noop");
        assert!(m.body.is_empty());
    }

    #[test]
    fn parse_macro_file_empty_file() {
        let err = parse_macro_file("", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::MissingDeclaration { .. }));
    }

    // -- Argument splitting ---------------------------------------------------

    #[test]
    fn split_args_simple() {
        let args = split_args("a, b, c");
        assert_eq!(args, vec!["a", "b", "c"]);
    }

    #[test]
    fn split_args_nested_parens() {
        let args = split_args("CONCAT(a, b), c");
        assert_eq!(args, vec!["CONCAT(a, b)", "c"]);
    }

    #[test]
    fn split_args_deeply_nested() {
        let args = split_args("COALESCE(NULLIF(a, ''), DEFAULT(b, c)), d");
        assert_eq!(args, vec!["COALESCE(NULLIF(a, ''), DEFAULT(b, c))", "d"]);
    }

    #[test]
    fn split_args_single() {
        let args = split_args("hello");
        assert_eq!(args, vec!["hello"]);
    }

    #[test]
    fn split_args_empty() {
        let args = split_args("");
        assert!(args.is_empty());
    }

    // -- Directory loading ----------------------------------------------------

    #[test]
    fn load_macros_from_dir_basic() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(
            dir.path().join("surrogate_key.sql"),
            "-- @macro surrogate_key(cols)\nMD5({cols})\n",
        )
        .unwrap();

        fs::write(
            dir.path().join("active_filter.sql"),
            "-- @macro active_filter(col)\n{col} = TRUE\n",
        )
        .unwrap();

        let macros = load_macros_from_dir(dir.path()).unwrap();
        assert_eq!(macros.len(), 2);
        // Sorted by filename.
        assert_eq!(macros[0].name, "active_filter");
        assert_eq!(macros[1].name, "surrogate_key");
    }

    #[test]
    fn load_macros_from_dir_ignores_non_sql() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(dir.path().join("macro.sql"), "-- @macro foo()\nSELECT 1\n").unwrap();

        fs::write(dir.path().join("readme.md"), "# Macros\n").unwrap();
        fs::write(dir.path().join("notes.txt"), "some notes").unwrap();

        let macros = load_macros_from_dir(dir.path()).unwrap();
        assert_eq!(macros.len(), 1);
        assert_eq!(macros[0].name, "foo");
    }

    #[test]
    fn load_macros_from_dir_duplicate_error() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(dir.path().join("a.sql"), "-- @macro dup()\nSELECT 1\n").unwrap();

        fs::write(dir.path().join("b.sql"), "-- @macro dup()\nSELECT 2\n").unwrap();

        let err = load_macros_from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, MacroError::DuplicateName { .. }));
    }

    #[test]
    fn load_macros_from_dir_not_found() {
        let err = load_macros_from_dir(Path::new("/nonexistent/macros")).unwrap_err();
        assert!(matches!(err, MacroError::DirNotFound(_)));
    }

    // -- Expansion ------------------------------------------------------------

    #[test]
    fn expand_single_macro() {
        let macros = vec![MacroDef {
            name: "surrogate_key".to_string(),
            params: vec!["cols".to_string(), "sep".to_string()],
            body: "MD5(CONCAT_WS({sep}, {cols}))".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "SELECT @surrogate_key(id || name, '|') AS sk FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(
            result,
            "SELECT MD5(CONCAT_WS('|', id || name)) AS sk FROM t"
        );
    }

    #[test]
    fn expand_no_arg_macro() {
        let macros = vec![MacroDef {
            name: "now_utc".to_string(),
            params: vec![],
            body: "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "SELECT @now_utc() AS ts";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS ts");
    }

    #[test]
    fn expand_multiple_invocations() {
        let macros = vec![MacroDef {
            name: "safe_cast".to_string(),
            params: vec!["expr".to_string(), "ty".to_string()],
            body: "TRY_CAST({expr} AS {ty})".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "SELECT @safe_cast(col1, INT) AS a, @safe_cast(col2, STRING) AS b";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(
            result,
            "SELECT TRY_CAST(col1 AS INT) AS a, TRY_CAST(col2 AS STRING) AS b"
        );
    }

    #[test]
    fn expand_nested_macros() {
        let macros = vec![
            MacroDef {
                name: "clean".to_string(),
                params: vec!["col".to_string()],
                body: "TRIM(LOWER({col}))".to_string(),
                source_path: "test.sql".to_string(),
            },
            MacroDef {
                name: "clean_key".to_string(),
                params: vec!["col".to_string()],
                body: "MD5(@clean({col}))".to_string(),
                source_path: "test.sql".to_string(),
            },
        ];

        let sql = "SELECT @clean_key(email) AS key FROM users";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT MD5(TRIM(LOWER(email))) AS key FROM users");
    }

    #[test]
    fn expand_preserves_at_identifiers() {
        // `@start_date` and `@end_date` are used by time_interval models
        // and should be preserved when they don't have parentheses.
        let macros = vec![MacroDef {
            name: "my_macro".to_string(),
            params: vec!["x".to_string()],
            body: "UPPER({x})".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql =
            "SELECT * FROM t WHERE ts > @start_date AND ts < @end_date AND name = @my_macro(col)";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(
            result,
            "SELECT * FROM t WHERE ts > @start_date AND ts < @end_date AND name = UPPER(col)"
        );
    }

    #[test]
    fn expand_args_with_nested_parens() {
        let macros = vec![MacroDef {
            name: "wrap".to_string(),
            params: vec!["expr".to_string()],
            body: "({expr})".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "SELECT @wrap(COALESCE(a, b, DEFAULT(c))) FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT (COALESCE(a, b, DEFAULT(c))) FROM t");
    }

    #[test]
    fn expand_unknown_macro_error() {
        let macros: Vec<MacroDef> = vec![];
        let sql = "SELECT @nonexistent(x) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::UnknownMacro { name } if name == "nonexistent"));
    }

    #[test]
    fn expand_wrong_arg_count_error() {
        let macros = vec![MacroDef {
            name: "needs_two".to_string(),
            params: vec!["a".to_string(), "b".to_string()],
            body: "{a} + {b}".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "SELECT @needs_two(x) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(
            err,
            MacroError::ArgCountMismatch {
                expected: 2,
                actual: 1,
                ..
            }
        ));
    }

    #[test]
    fn expand_max_depth_error() {
        // Create a self-referencing macro to trigger the depth limit.
        let macros = vec![MacroDef {
            name: "recurse".to_string(),
            params: vec!["x".to_string()],
            body: "@recurse({x})".to_string(),
            source_path: "test.sql".to_string(),
        }];

        let sql = "@recurse(1)";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::MaxDepthExceeded { .. }));
    }

    #[test]
    fn expand_no_macros_passthrough() {
        let macros: Vec<MacroDef> = vec![];
        let sql = "SELECT id, name FROM users WHERE active = TRUE";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, sql);
    }

    #[test]
    fn expand_at_sign_not_followed_by_ident() {
        let macros: Vec<MacroDef> = vec![];
        let sql = "SELECT email, name @ 'domain' FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, sql);
    }

    // -- Integration: load + expand -------------------------------------------

    #[test]
    fn load_and_expand_integration() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(
            dir.path().join("sk.sql"),
            "-- @macro sk(cols, sep)\nMD5(CONCAT_WS({sep}, {cols}))",
        )
        .unwrap();

        fs::write(
            dir.path().join("is_active.sql"),
            "-- @macro is_active(col)\n{col} IS NOT NULL AND {col} = TRUE",
        )
        .unwrap();

        let macros = load_macros_from_dir(dir.path()).unwrap();
        assert_eq!(macros.len(), 2);

        let sql = "\
SELECT
    @sk(id || region, '|') AS surrogate_key,
    *
FROM customers
WHERE @is_active(is_active_flag)";

        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("MD5(CONCAT_WS('|', id || region))"));
        assert!(result.contains("is_active_flag IS NOT NULL AND is_active_flag = TRUE"));
        assert!(!result.contains("@sk"));
        assert!(!result.contains("@is_active"));
    }
}
