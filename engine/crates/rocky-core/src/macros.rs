//! Macro / template-include system for Rocky SQL models.
//!
//! Macros are reusable SQL fragments defined in `.sql` or `.sql-macro` files
//! under a `macros/` directory. They provide a simple, non-Jinja approach to
//! eliminating repeated SQL patterns (surrogate key generation, standard
//! filters, SCD2 queries, etc.).
//!
//! ## Macro file format
//!
//! **`.sql` files** — the declaration is on the first line as a comment:
//!
//! ```sql
//! -- @macro surrogate_key(columns, separator)
//! MD5(CONCAT_WS({separator}, {columns}))
//! ```
//!
//! **`.sql-macro` files** — use the plan's explicit syntax:
//!
//! ```sql
//! macro current_month(date_col: date) -> bool {
//!     @date_col BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE
//! }
//! ```
//!
//! ## Parameter types
//!
//! Parameters can optionally declare a type annotation:
//!
//! ```text
//! -- @macro safe_divide(numerator: expr, denominator: expr)
//! -- @macro is_recent(col: column, days: number)
//! ```
//!
//! Supported types: `column`, `string`, `number`, `bool`, `expr`, `any`.
//! Bare names without a type default to `any`.
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
    UnknownMacro {
        name: String,
        /// Byte offset of the `@` in the input SQL (if available).
        call_offset: Option<usize>,
    },

    #[error("macro '@{name}' expects {expected} argument(s) but got {actual}")]
    ArgCountMismatch {
        name: String,
        expected: usize,
        actual: usize,
        /// Byte offset of the `@` in the input SQL (if available).
        call_offset: Option<usize>,
    },

    #[error("{0}")]
    TypeMismatch(Box<MacroTypeMismatch>),

    #[error("macro expansion exceeded maximum depth ({depth}); possible circular reference")]
    MaxDepthExceeded { depth: usize },
}

/// Detail for a macro parameter type mismatch (boxed inside [`MacroError::TypeMismatch`]
/// to keep the overall enum small enough for clippy's `result_large_err` lint).
#[derive(Debug)]
pub struct MacroTypeMismatch {
    pub macro_name: String,
    pub param: String,
    pub expected: MacroParamType,
    pub actual: String,
    pub arg: String,
    /// Byte offset of the `@` in the input SQL (if available).
    pub call_offset: Option<usize>,
    /// Source path of the macro definition.
    pub def_path: String,
}

impl std::fmt::Display for MacroTypeMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "macro '@{}' parameter '{}' expects type {}, but argument '{}' looks like {}",
            self.macro_name, self.param, self.expected, self.arg, self.actual
        )
    }
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// Type annotation for a macro parameter.
///
/// Used for best-effort validation at expansion time. `Any` disables
/// validation (the default when no type annotation is given).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MacroParamType {
    /// No constraint — accepts anything.
    Any,
    /// A bare column name (identifier characters only).
    Column,
    /// A string literal (starts and ends with `'` or `"`).
    String,
    /// A numeric literal.
    Number,
    /// A boolean literal (`TRUE` / `FALSE`).
    Bool,
    /// An arbitrary SQL expression (no validation beyond non-empty).
    Expr,
}

impl std::fmt::Display for MacroParamType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MacroParamType::Any => write!(f, "any"),
            MacroParamType::Column => write!(f, "column"),
            MacroParamType::String => write!(f, "string"),
            MacroParamType::Number => write!(f, "number"),
            MacroParamType::Bool => write!(f, "bool"),
            MacroParamType::Expr => write!(f, "expr"),
        }
    }
}

impl MacroParamType {
    /// Parse a type annotation string.
    fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "any" => Some(MacroParamType::Any),
            "column" => Some(MacroParamType::Column),
            "string" | "str" => Some(MacroParamType::String),
            "number" | "int" | "integer" | "float" | "numeric" => Some(MacroParamType::Number),
            "bool" | "boolean" => Some(MacroParamType::Bool),
            "expr" | "expression" => Some(MacroParamType::Expr),
            // Plan-level types mapped for forward compatibility.
            "date" | "timestamp" => Some(MacroParamType::Expr),
            "model" => Some(MacroParamType::Column),
            _ => None,
        }
    }

    /// Best-effort check: does `arg` look like a value of this type?
    fn matches_arg(&self, arg: &str) -> bool {
        let trimmed = arg.trim();
        match self {
            MacroParamType::Any | MacroParamType::Expr => true,
            MacroParamType::Column => {
                !trimmed.is_empty()
                    && trimmed
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
            }
            MacroParamType::String => {
                (trimmed.starts_with('\'') && trimmed.ends_with('\''))
                    || (trimmed.starts_with('"') && trimmed.ends_with('"'))
            }
            MacroParamType::Number => {
                !trimmed.is_empty()
                    && trimmed
                        .chars()
                        .next()
                        .is_some_and(|c| c.is_ascii_digit() || c == '-')
                    && trimmed.chars().skip(1).all(|c| {
                        c.is_ascii_digit()
                            || c == '.'
                            || c == '_'
                            || c == 'e'
                            || c == 'E'
                            || c == '+'
                            || c == '-'
                    })
            }
            MacroParamType::Bool => {
                let upper = trimmed.to_uppercase();
                upper == "TRUE" || upper == "FALSE"
            }
        }
    }

    /// Describe what an argument looks like (for error messages).
    fn describe_arg(arg: &str) -> &'static str {
        let trimmed = arg.trim();
        if trimmed.is_empty() {
            return "empty";
        }
        if (trimmed.starts_with('\'') && trimmed.ends_with('\''))
            || (trimmed.starts_with('"') && trimmed.ends_with('"'))
        {
            return "a string literal";
        }
        if trimmed
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_digit() || c == '-')
            && trimmed.chars().skip(1).all(|c| {
                c.is_ascii_digit()
                    || c == '.'
                    || c == '_'
                    || c == 'e'
                    || c == 'E'
                    || c == '+'
                    || c == '-'
            })
        {
            return "a number";
        }
        let upper = trimmed.to_uppercase();
        if upper == "TRUE" || upper == "FALSE" {
            return "a boolean";
        }
        if trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        {
            return "a column reference";
        }
        "an expression"
    }
}

/// A single macro parameter with name and optional type.
#[derive(Debug, Clone)]
pub struct MacroParam {
    /// Parameter name (e.g., `columns`).
    pub name: String,
    /// Type constraint (defaults to `Any`).
    pub param_type: MacroParamType,
}

/// A single macro definition loaded from a `.sql` or `.sql-macro` file.
#[derive(Debug, Clone)]
pub struct MacroDef {
    /// Macro name (e.g., `surrogate_key`).
    pub name: String,
    /// Ordered typed parameters.
    pub params: Vec<MacroParam>,
    /// SQL body template with `{param}` placeholders.
    pub body: String,
    /// Path to the source file (for diagnostics).
    pub source_path: String,
    /// Optional return type annotation (parsed but not enforced — stored
    /// for display in LSP hover and documentation).
    pub return_type: Option<String>,
    /// Doc comment lines (consecutive `--` comment lines preceding the
    /// declaration, with the `--` prefix stripped).
    pub doc_lines: Vec<String>,
}

impl MacroDef {
    /// Build a Markdown-formatted signature for LSP hover.
    pub fn signature(&self) -> String {
        let params: Vec<String> = self
            .params
            .iter()
            .map(|p| {
                if p.param_type == MacroParamType::Any {
                    p.name.clone()
                } else {
                    format!("{}: {}", p.name, p.param_type)
                }
            })
            .collect();
        let ret = match &self.return_type {
            Some(rt) => format!(" -> {rt}"),
            None => String::new(),
        };
        format!("@{}({}){}", self.name, params.join(", "), ret)
    }

    /// Build a Markdown documentation string for LSP hover.
    pub fn hover_markdown(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("**Macro:** `{}`", self.signature()));
        if !self.doc_lines.is_empty() {
            lines.push(String::new());
            for doc in &self.doc_lines {
                lines.push(format!("> {doc}"));
            }
        }
        lines.push(String::new());
        lines.push(format!("*Defined in:* `{}`", self.source_path));
        lines.join("\n")
    }
}

/// Maximum recursion depth for nested macro expansion.
const MAX_EXPANSION_DEPTH: usize = 16;

// ---------------------------------------------------------------------------
// Parsing — `.sql` format (comment-based declaration)
// ---------------------------------------------------------------------------

/// Parses a parameter that may have a type annotation: `name` or `name: type`.
fn parse_typed_param(s: &str, path: &str) -> Result<MacroParam, MacroError> {
    let trimmed = s.trim();
    if let Some((name_part, type_part)) = trimmed.split_once(':') {
        let name = name_part.trim().to_string();
        let type_str = type_part.trim();

        if name.is_empty() {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: "empty parameter name".to_string(),
            });
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: format!("parameter name '{name}' contains invalid characters"),
            });
        }
        let param_type =
            MacroParamType::from_str(type_str).ok_or_else(|| MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: format!(
                    "unknown parameter type '{type_str}' \
                     (expected: any, column, string, number, bool, expr)"
                ),
            })?;
        Ok(MacroParam { name, param_type })
    } else {
        // No type annotation.
        let name = trimmed.to_string();
        if name.is_empty() {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: "empty parameter name".to_string(),
            });
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(MacroError::InvalidDeclaration {
                path: path.to_string(),
                reason: format!("parameter name '{name}' contains invalid characters"),
            });
        }
        Ok(MacroParam {
            name,
            param_type: MacroParamType::Any,
        })
    }
}

/// Parses the comment declaration line `-- @macro name(param1, param2)`.
///
/// Returns `(name, params, return_type)` on success.
fn parse_declaration(
    line: &str,
    path: &str,
) -> Result<(String, Vec<MacroParam>, Option<String>), MacroError> {
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

    parse_name_and_params(rest, path)
}

/// Shared parser for `name(param1: type, param2) -> ret_type` portion.
fn parse_name_and_params(
    rest: &str,
    path: &str,
) -> Result<(String, Vec<MacroParam>, Option<String>), MacroError> {
    let Some(paren_start) = rest.find('(') else {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "missing opening parenthesis".to_string(),
        });
    };

    let Some(paren_end) = rest.find(')') else {
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
    let params: Vec<MacroParam> = if params_str.trim().is_empty() {
        Vec::new()
    } else {
        params_str
            .split(',')
            .map(|p| parse_typed_param(p, path))
            .collect::<Result<Vec<_>, _>>()?
    };

    // Check for `-> return_type` after the closing paren.
    let after_paren = rest[paren_end + 1..].trim();
    let return_type = if let Some(rt) = after_paren.strip_prefix("->") {
        let rt = rt.trim();
        if rt.is_empty() {
            None
        } else {
            Some(rt.to_string())
        }
    } else {
        None
    };

    Ok((name, params, return_type))
}

/// Parses a `.sql` macro file (comment-based declaration on the first line).
fn parse_macro_file(content: &str, path: &str) -> Result<MacroDef, MacroError> {
    let lines: Vec<&str> = content.lines().collect();
    if lines.is_empty() {
        return Err(MacroError::MissingDeclaration {
            path: path.to_string(),
        });
    }

    // Collect doc comments: consecutive `--` lines before the declaration.
    // The declaration line is the first `-- @macro` line. Any `--` comments
    // before it (that are NOT the declaration) are doc comments.
    let decl_idx = lines
        .iter()
        .position(|l| l.trim().starts_with("-- @macro") || l.trim().starts_with("--@macro"))
        .ok_or_else(|| MacroError::MissingDeclaration {
            path: path.to_string(),
        })?;

    let mut doc_lines = Vec::new();
    for line in &lines[..decl_idx] {
        let trimmed = line.trim();
        if let Some(comment) = trimmed.strip_prefix("--") {
            doc_lines.push(comment.trim().to_string());
        } else if trimmed.is_empty() {
            // Allow blank lines in the doc comment block.
            doc_lines.push(String::new());
        } else {
            // Non-comment non-blank line resets the doc block.
            doc_lines.clear();
        }
    }
    // Trim leading/trailing empty lines from doc comments.
    while doc_lines.first().is_some_and(String::is_empty) {
        doc_lines.remove(0);
    }
    while doc_lines.last().is_some_and(String::is_empty) {
        doc_lines.pop();
    }

    let (name, params, return_type) = parse_declaration(lines[decl_idx], path)?;

    let body: String = lines[decl_idx + 1..].join("\n").trim().to_string();

    Ok(MacroDef {
        name,
        params,
        body,
        source_path: path.to_string(),
        return_type,
        doc_lines,
    })
}

// ---------------------------------------------------------------------------
// Parsing — `.sql-macro` format (explicit syntax)
// ---------------------------------------------------------------------------

/// Parses a `.sql-macro` file using the explicit `macro name(params) -> ret { body }` syntax.
fn parse_sql_macro_file(content: &str, path: &str) -> Result<MacroDef, MacroError> {
    let trimmed = content.trim();

    // Collect doc comments before the `macro` keyword.
    let lines: Vec<&str> = content.lines().collect();
    let mut doc_lines = Vec::new();
    for line in &lines {
        let t = line.trim();
        if t.starts_with("--") {
            if let Some(comment) = t.strip_prefix("--") {
                doc_lines.push(comment.trim().to_string());
            }
        } else {
            break;
        }
    }
    while doc_lines.first().is_some_and(String::is_empty) {
        doc_lines.remove(0);
    }
    while doc_lines.last().is_some_and(String::is_empty) {
        doc_lines.pop();
    }

    // Strip leading doc-comment lines.
    let body_start = trimmed
        .lines()
        .position(|l| {
            let t = l.trim();
            !t.is_empty() && !t.starts_with("--")
        })
        .unwrap_or(0);
    let rest: String = trimmed
        .lines()
        .skip(body_start)
        .collect::<Vec<_>>()
        .join("\n");
    let rest = rest.trim();

    // Expect `macro name(params) [-> ret] { body }`.
    let Some(after_keyword) = rest.strip_prefix("macro") else {
        return Err(MacroError::MissingDeclaration {
            path: path.to_string(),
        });
    };
    let after_keyword = after_keyword.trim_start();

    // Find the opening brace for the body.
    let Some(brace_start) = after_keyword.find('{') else {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "missing opening brace `{`".to_string(),
        });
    };

    let signature = &after_keyword[..brace_start];
    let (name, params, return_type) = parse_name_and_params(signature, path)?;

    // Extract body between outermost braces.
    let body_section = &after_keyword[brace_start + 1..];
    let Some(brace_end) = body_section.rfind('}') else {
        return Err(MacroError::InvalidDeclaration {
            path: path.to_string(),
            reason: "missing closing brace `}`".to_string(),
        });
    };
    let body = body_section[..brace_end].trim().to_string();

    Ok(MacroDef {
        name,
        params,
        body,
        source_path: path.to_string(),
        return_type,
        doc_lines,
    })
}

// ---------------------------------------------------------------------------
// Discovery
// ---------------------------------------------------------------------------

/// Discovers and loads all macro files from a directory.
///
/// Recognizes `.sql` and `.sql-macro` files. Non-recursive — only top-level
/// files are discovered. Files are sorted by name for deterministic ordering.
/// Returns an error if two files define macros with the same name.
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

        // Check for .sql-macro first (two-part extension).
        let is_sql_macro = path
            .file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.ends_with(".sql-macro"));

        let is_sql = !is_sql_macro
            && path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| e == "sql");

        if !is_sql && !is_sql_macro {
            continue;
        }

        let path_str = path.display().to_string();
        let content = std::fs::read_to_string(&path).map_err(|e| MacroError::ReadFile {
            path: path_str.clone(),
            source: e,
        })?;

        let macro_def = if is_sql_macro {
            parse_sql_macro_file(&content, &path_str)?
        } else {
            parse_macro_file(&content, &path_str)?
        };

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
            let call_byte_offset = i;

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
                    return Err(MacroError::UnknownMacro {
                        name,
                        call_offset: Some(call_byte_offset),
                    });
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
                        call_offset: Some(call_byte_offset),
                    });
                }

                // Validate argument types.
                let effective_args: &[String] = if actual == 0 { &[] } else { &args };
                for (param, arg) in macro_def.params.iter().zip(effective_args) {
                    if param.param_type != MacroParamType::Any && !param.param_type.matches_arg(arg)
                    {
                        return Err(MacroError::TypeMismatch(Box::new(MacroTypeMismatch {
                            macro_name: name.clone(),
                            param: param.name.clone(),
                            expected: param.param_type.clone(),
                            actual: MacroParamType::describe_arg(arg).to_string(),
                            arg: arg.clone(),
                            call_offset: Some(call_byte_offset),
                            def_path: macro_def.source_path.clone(),
                        })));
                    }
                }

                // Substitute parameters into the body.
                let mut expanded = macro_def.body.clone();
                for (param, arg) in macro_def.params.iter().zip(effective_args) {
                    expanded = expanded.replace(&format!("{{{}}}", param.name), arg);
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

    // -- Helpers for tests that don't need typed params -----------------------

    fn untyped_param(name: &str) -> MacroParam {
        MacroParam {
            name: name.to_string(),
            param_type: MacroParamType::Any,
        }
    }

    fn typed_param(name: &str, ty: MacroParamType) -> MacroParam {
        MacroParam {
            name: name.to_string(),
            param_type: ty,
        }
    }

    fn simple_macro(name: &str, params: &[&str], body: &str) -> MacroDef {
        MacroDef {
            name: name.to_string(),
            params: params.iter().map(|p| untyped_param(p)).collect(),
            body: body.to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }
    }

    // -- Declaration parsing --------------------------------------------------

    #[test]
    fn parse_declaration_basic() {
        let (name, params, _ret) =
            parse_declaration("-- @macro surrogate_key(columns, separator)", "test.sql").unwrap();
        assert_eq!(name, "surrogate_key");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "columns");
        assert_eq!(params[0].param_type, MacroParamType::Any);
        assert_eq!(params[1].name, "separator");
    }

    #[test]
    fn parse_declaration_no_params() {
        let (name, params, _ret) = parse_declaration("-- @macro current_ts()", "test.sql").unwrap();
        assert_eq!(name, "current_ts");
        assert!(params.is_empty());
    }

    #[test]
    fn parse_declaration_single_param() {
        let (name, params, _ret) =
            parse_declaration("-- @macro safe_divide(expr)", "test.sql").unwrap();
        assert_eq!(name, "safe_divide");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0].name, "expr");
    }

    #[test]
    fn parse_declaration_extra_whitespace() {
        let (name, params, _ret) =
            parse_declaration("--   @macro   my_macro(  a ,  b  )  ", "test.sql").unwrap();
        assert_eq!(name, "my_macro");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "a");
        assert_eq!(params[1].name, "b");
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

    // -- Typed parameter parsing ---------------------------------------------

    #[test]
    fn parse_declaration_typed_params() {
        let (name, params, _ret) =
            parse_declaration("-- @macro is_recent(col: column, days: number)", "test.sql")
                .unwrap();
        assert_eq!(name, "is_recent");
        assert_eq!(params.len(), 2);
        assert_eq!(params[0].name, "col");
        assert_eq!(params[0].param_type, MacroParamType::Column);
        assert_eq!(params[1].name, "days");
        assert_eq!(params[1].param_type, MacroParamType::Number);
    }

    #[test]
    fn parse_declaration_mixed_typed_untyped() {
        let (_, params, _) =
            parse_declaration("-- @macro mixed(a, b: string, c: bool, d)", "test.sql").unwrap();
        assert_eq!(params[0].param_type, MacroParamType::Any);
        assert_eq!(params[1].param_type, MacroParamType::String);
        assert_eq!(params[2].param_type, MacroParamType::Bool);
        assert_eq!(params[3].param_type, MacroParamType::Any);
    }

    #[test]
    fn parse_declaration_with_return_type() {
        let (name, params, ret) = parse_declaration(
            "-- @macro current_month(date_col: column) -> bool",
            "test.sql",
        )
        .unwrap();
        assert_eq!(name, "current_month");
        assert_eq!(params.len(), 1);
        assert_eq!(ret.as_deref(), Some("bool"));
    }

    #[test]
    fn parse_declaration_unknown_type_error() {
        let err = parse_declaration("-- @macro bad(x: foobar)", "test.sql").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
    }

    // -- Macro file parsing ---------------------------------------------------

    #[test]
    fn parse_macro_file_basic() {
        let content = "-- @macro surrogate_key(columns, sep)\nMD5(CONCAT_WS({sep}, {columns}))";
        let m = parse_macro_file(content, "test.sql").unwrap();
        assert_eq!(m.name, "surrogate_key");
        assert_eq!(m.params.len(), 2);
        assert_eq!(m.params[0].name, "columns");
        assert_eq!(m.params[1].name, "sep");
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

    #[test]
    fn parse_macro_file_with_doc_comments() {
        let content = "\
-- Checks if a date column falls in the current month.
-- Useful for filtering recent data.
-- @macro current_month(date_col: column) -> bool
{date_col} BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE";
        let m = parse_macro_file(content, "test.sql").unwrap();
        assert_eq!(m.name, "current_month");
        assert_eq!(m.doc_lines.len(), 2);
        assert_eq!(
            m.doc_lines[0],
            "Checks if a date column falls in the current month."
        );
        assert_eq!(m.doc_lines[1], "Useful for filtering recent data.");
        assert_eq!(m.return_type.as_deref(), Some("bool"));
    }

    // -- `.sql-macro` file format -------------------------------------------

    #[test]
    fn parse_sql_macro_file_basic() {
        let content = "macro current_month(date_col: column) -> bool {\n    {date_col} BETWEEN DATE_TRUNC('month', CURRENT_DATE) AND CURRENT_DATE\n}";
        let m = parse_sql_macro_file(content, "test.sql-macro").unwrap();
        assert_eq!(m.name, "current_month");
        assert_eq!(m.params.len(), 1);
        assert_eq!(m.params[0].name, "date_col");
        assert_eq!(m.params[0].param_type, MacroParamType::Column);
        assert_eq!(m.return_type.as_deref(), Some("bool"));
        assert!(m.body.contains("BETWEEN DATE_TRUNC('month', CURRENT_DATE)"));
    }

    #[test]
    fn parse_sql_macro_file_with_docs() {
        let content = "\
-- Filter rows to current month only.
macro active_filter(col: column) -> bool {
    {col} = TRUE
}";
        let m = parse_sql_macro_file(content, "test.sql-macro").unwrap();
        assert_eq!(m.name, "active_filter");
        assert_eq!(m.doc_lines.len(), 1);
        assert_eq!(m.doc_lines[0], "Filter rows to current month only.");
    }

    #[test]
    fn parse_sql_macro_file_no_return_type() {
        let content = "macro wrap(expr) {\n    ({expr})\n}";
        let m = parse_sql_macro_file(content, "test.sql-macro").unwrap();
        assert_eq!(m.name, "wrap");
        assert!(m.return_type.is_none());
    }

    #[test]
    fn parse_sql_macro_file_missing_brace() {
        let err = parse_sql_macro_file("macro foo(x) -> int", "test.sql-macro").unwrap_err();
        assert!(matches!(err, MacroError::InvalidDeclaration { .. }));
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

    #[test]
    fn load_macros_from_dir_sql_macro_extension() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(
            dir.path().join("current_month.sql-macro"),
            "macro current_month(date_col: column) -> bool {\n    {date_col} >= DATE_TRUNC('month', CURRENT_DATE)\n}",
        )
        .unwrap();

        fs::write(
            dir.path().join("sk.sql"),
            "-- @macro sk(cols)\nMD5({cols})\n",
        )
        .unwrap();

        let macros = load_macros_from_dir(dir.path()).unwrap();
        assert_eq!(macros.len(), 2);
        assert_eq!(macros[0].name, "current_month");
        assert_eq!(macros[1].name, "sk");
    }

    #[test]
    fn load_macros_from_dir_duplicate_across_formats() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(dir.path().join("dup.sql"), "-- @macro dup()\nSELECT 1\n").unwrap();

        fs::write(
            dir.path().join("dup.sql-macro"),
            "macro dup() {\n    SELECT 2\n}",
        )
        .unwrap();

        let err = load_macros_from_dir(dir.path()).unwrap_err();
        assert!(matches!(err, MacroError::DuplicateName { .. }));
    }

    // -- Expansion ------------------------------------------------------------

    #[test]
    fn expand_single_macro() {
        let macros = vec![simple_macro(
            "surrogate_key",
            &["cols", "sep"],
            "MD5(CONCAT_WS({sep}, {cols}))",
        )];

        let sql = "SELECT @surrogate_key(id || name, '|') AS sk FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(
            result,
            "SELECT MD5(CONCAT_WS('|', id || name)) AS sk FROM t"
        );
    }

    #[test]
    fn expand_no_arg_macro() {
        let macros = vec![simple_macro(
            "now_utc",
            &[],
            "CURRENT_TIMESTAMP AT TIME ZONE 'UTC'",
        )];

        let sql = "SELECT @now_utc() AS ts";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS ts");
    }

    #[test]
    fn expand_multiple_invocations() {
        let macros = vec![simple_macro(
            "safe_cast",
            &["expr", "ty"],
            "TRY_CAST({expr} AS {ty})",
        )];

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
            simple_macro("clean", &["col"], "TRIM(LOWER({col}))"),
            simple_macro("clean_key", &["col"], "MD5(@clean({col}))"),
        ];

        let sql = "SELECT @clean_key(email) AS key FROM users";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT MD5(TRIM(LOWER(email))) AS key FROM users");
    }

    #[test]
    fn expand_preserves_at_identifiers() {
        // `@start_date` and `@end_date` are used by time_interval models
        // and should be preserved when they don't have parentheses.
        let macros = vec![simple_macro("my_macro", &["x"], "UPPER({x})")];

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
        let macros = vec![simple_macro("wrap", &["expr"], "({expr})")];

        let sql = "SELECT @wrap(COALESCE(a, b, DEFAULT(c))) FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT (COALESCE(a, b, DEFAULT(c))) FROM t");
    }

    #[test]
    fn expand_unknown_macro_error() {
        let macros: Vec<MacroDef> = vec![];
        let sql = "SELECT @nonexistent(x) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::UnknownMacro { ref name, .. } if name == "nonexistent"));
    }

    #[test]
    fn expand_wrong_arg_count_error() {
        let macros = vec![simple_macro("needs_two", &["a", "b"], "{a} + {b}")];

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
        let macros = vec![simple_macro("recurse", &["x"], "@recurse({x})")];

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

    // -- Type validation at expansion time ------------------------------------

    #[test]
    fn expand_typed_column_param_accepts_identifier() {
        let macros = vec![MacroDef {
            name: "upper_col".to_string(),
            params: vec![typed_param("col", MacroParamType::Column)],
            body: "UPPER({col})".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT @upper_col(user_name) FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT UPPER(user_name) FROM t");
    }

    #[test]
    fn expand_typed_column_param_rejects_expression() {
        let macros = vec![MacroDef {
            name: "upper_col".to_string(),
            params: vec![typed_param("col", MacroParamType::Column)],
            body: "UPPER({col})".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT @upper_col(a + b) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::TypeMismatch(..)));
    }

    #[test]
    fn expand_typed_string_param_accepts_literal() {
        let macros = vec![MacroDef {
            name: "wrap_str".to_string(),
            params: vec![typed_param("s", MacroParamType::String)],
            body: "CONCAT({s}, '!')".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT @wrap_str('hello') FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT CONCAT('hello', '!') FROM t");
    }

    #[test]
    fn expand_typed_string_param_rejects_number() {
        let macros = vec![MacroDef {
            name: "wrap_str".to_string(),
            params: vec![typed_param("s", MacroParamType::String)],
            body: "CONCAT({s}, '!')".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT @wrap_str(42) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::TypeMismatch(..)));
    }

    #[test]
    fn expand_typed_number_param_accepts_number() {
        let macros = vec![MacroDef {
            name: "limit_n".to_string(),
            params: vec![typed_param("n", MacroParamType::Number)],
            body: "LIMIT {n}".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT * FROM t @limit_n(100)";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT * FROM t LIMIT 100");
    }

    #[test]
    fn expand_typed_bool_param_accepts_true() {
        let macros = vec![MacroDef {
            name: "flag_filter".to_string(),
            params: vec![
                typed_param("col", MacroParamType::Column),
                typed_param("val", MacroParamType::Bool),
            ],
            body: "{col} = {val}".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT * FROM t WHERE @flag_filter(active, TRUE)";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT * FROM t WHERE active = TRUE");
    }

    #[test]
    fn expand_typed_bool_param_rejects_string() {
        let macros = vec![MacroDef {
            name: "flag_filter".to_string(),
            params: vec![
                typed_param("col", MacroParamType::Column),
                typed_param("val", MacroParamType::Bool),
            ],
            body: "{col} = {val}".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT * FROM t WHERE @flag_filter(active, 'yes')";
        let err = expand_macros(sql, &macros).unwrap_err();
        assert!(matches!(err, MacroError::TypeMismatch(..)));
    }

    #[test]
    fn expand_any_param_accepts_everything() {
        let macros = vec![simple_macro("echo", &["x"], "{x}")];

        // All of these should pass with Any type.
        assert!(expand_macros("@echo(col_name)", &macros).is_ok());
        assert!(expand_macros("@echo('string')", &macros).is_ok());
        assert!(expand_macros("@echo(42)", &macros).is_ok());
        assert!(expand_macros("@echo(a + b)", &macros).is_ok());
    }

    #[test]
    fn expand_expr_param_accepts_complex_expression() {
        let macros = vec![MacroDef {
            name: "safe_div".to_string(),
            params: vec![
                typed_param("num", MacroParamType::Expr),
                typed_param("den", MacroParamType::Expr),
            ],
            body: "CASE WHEN ({den}) = 0 THEN NULL ELSE ({num}) / ({den}) END".to_string(),
            source_path: "test.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "SELECT @safe_div(revenue - cost, total_units) FROM t";
        let result = expand_macros(sql, &macros).unwrap();
        assert!(result.contains("(revenue - cost)"));
        assert!(result.contains("(total_units)"));
    }

    // -- Source span in error messages ----------------------------------------

    #[test]
    fn expand_error_includes_call_offset() {
        let macros: Vec<MacroDef> = vec![];
        // The `@` for `@bad_macro` is at byte offset 7.
        let sql = "SELECT @bad_macro(x) FROM t";
        let err = expand_macros(sql, &macros).unwrap_err();
        match err {
            MacroError::UnknownMacro { call_offset, .. } => {
                assert_eq!(call_offset, Some(7));
            }
            _ => panic!("expected UnknownMacro error"),
        }
    }

    #[test]
    fn expand_type_mismatch_error_includes_def_path() {
        let macros = vec![MacroDef {
            name: "typed_fn".to_string(),
            params: vec![typed_param("n", MacroParamType::Number)],
            body: "{n}".to_string(),
            source_path: "macros/typed_fn.sql".to_string(),
            return_type: None,
            doc_lines: Vec::new(),
        }];

        let sql = "@typed_fn('not a number')";
        let err = expand_macros(sql, &macros).unwrap_err();
        match err {
            MacroError::TypeMismatch(detail) => {
                assert_eq!(detail.def_path, "macros/typed_fn.sql");
                assert_eq!(detail.macro_name, "typed_fn");
                assert_eq!(detail.param, "n");
            }
            _ => panic!("expected TypeMismatch error"),
        }
    }

    // -- Signature / hover Markdown ------------------------------------------

    #[test]
    fn macro_def_signature_untyped() {
        let m = simple_macro("sk", &["cols", "sep"], "MD5(...)");
        assert_eq!(m.signature(), "@sk(cols, sep)");
    }

    #[test]
    fn macro_def_signature_typed_with_return() {
        let m = MacroDef {
            name: "current_month".to_string(),
            params: vec![typed_param("date_col", MacroParamType::Column)],
            body: "...".to_string(),
            source_path: "macros/current_month.sql".to_string(),
            return_type: Some("bool".to_string()),
            doc_lines: vec!["Filters to current month.".to_string()],
        };
        assert_eq!(m.signature(), "@current_month(date_col: column) -> bool");
    }

    #[test]
    fn macro_def_hover_markdown() {
        let m = MacroDef {
            name: "sk".to_string(),
            params: vec![untyped_param("cols")],
            body: "MD5({cols})".to_string(),
            source_path: "macros/sk.sql".to_string(),
            return_type: None,
            doc_lines: vec!["Generate a surrogate key.".to_string()],
        };
        let md = m.hover_markdown();
        assert!(md.contains("**Macro:** `@sk(cols)`"));
        assert!(md.contains("Generate a surrogate key."));
        assert!(md.contains("macros/sk.sql"));
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

    #[test]
    fn load_and_expand_mixed_formats() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(
            dir.path().join("clean.sql"),
            "-- @macro clean(col)\nTRIM(LOWER({col}))",
        )
        .unwrap();

        fs::write(
            dir.path().join("clean_key.sql-macro"),
            "macro clean_key(col: column) -> string {\n    MD5(@clean({col}))\n}",
        )
        .unwrap();

        let macros = load_macros_from_dir(dir.path()).unwrap();
        assert_eq!(macros.len(), 2);

        let sql = "SELECT @clean_key(email) AS key FROM users";
        let result = expand_macros(sql, &macros).unwrap();
        assert_eq!(result, "SELECT MD5(TRIM(LOWER(email))) AS key FROM users");
    }

    // -- Param type matching edge cases --------------------------------------

    #[test]
    fn column_type_accepts_dotted_qualified() {
        assert!(MacroParamType::Column.matches_arg("schema.table_name"));
    }

    #[test]
    fn column_type_rejects_expression_with_spaces() {
        assert!(!MacroParamType::Column.matches_arg("a + b"));
    }

    #[test]
    fn number_type_accepts_negative() {
        assert!(MacroParamType::Number.matches_arg("-42"));
    }

    #[test]
    fn number_type_accepts_decimal() {
        assert!(MacroParamType::Number.matches_arg("3.14"));
    }

    #[test]
    fn number_type_accepts_scientific() {
        assert!(MacroParamType::Number.matches_arg("1e10"));
    }

    #[test]
    fn string_type_accepts_single_quotes() {
        assert!(MacroParamType::String.matches_arg("'hello world'"));
    }

    #[test]
    fn string_type_accepts_double_quotes() {
        assert!(MacroParamType::String.matches_arg("\"hello world\""));
    }

    #[test]
    fn bool_type_case_insensitive() {
        assert!(MacroParamType::Bool.matches_arg("true"));
        assert!(MacroParamType::Bool.matches_arg("FALSE"));
        assert!(MacroParamType::Bool.matches_arg("True"));
    }
}
