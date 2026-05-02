//! WASM bindings for Rocky's pure-Rust compiler pipeline.
//!
//! Exposes an API surface for in-browser use:
//! - SQL lineage extraction (`rocky-sql`)
//! - Rocky DSL parsing, lowering, and formatting (`rocky-lang`)
//! - SQL identifier validation (`rocky-sql`)
//! - SQL transpilation between dialects (`rocky-sql`)
//! - Combined model compilation (parse + lower + lineage)
//!
//! All functions exchange data as JSON strings for simplest WASM interop.

use wasm_bindgen::prelude::*;

// ---------------------------------------------------------------------------
// SQL exports
// ---------------------------------------------------------------------------

/// Extract column-level lineage from a SQL SELECT statement.
///
/// Returns a JSON-serialized `LineageResult` on success,
/// or a JSON object `{"error": "..."}` on failure.
#[wasm_bindgen]
pub fn compile_sql(sql: &str) -> String {
    match rocky_sql::lineage::extract_lineage(sql) {
        Ok(result) => serde_json::to_string(&result)
            .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#)),
        Err(e) => format!(r#"{{"error":{}}}"#, serde_json::json!(e)),
    }
}

/// Transpile SQL between warehouse dialects.
///
/// Supported dialects: `"snowflake"`, `"databricks"`, `"bigquery"`, `"duckdb"`.
///
/// Returns a JSON object on success:
/// ```json
/// {"sql": "...", "warnings": [...], "replacements": 3}
/// ```
/// Returns `{"error": "..."}` on failure (e.g., unknown dialect name).
#[wasm_bindgen]
pub fn transpile_sql(sql: &str, from_dialect: &str, to_dialect: &str) -> String {
    let from = match parse_dialect(from_dialect) {
        Some(d) => d,
        None => {
            return serde_json::json!({
                "error": format!("unknown source dialect: {from_dialect}. Expected one of: snowflake, databricks, bigquery, duckdb")
            })
            .to_string();
        }
    };
    let to = match parse_dialect(to_dialect) {
        Some(d) => d,
        None => {
            return serde_json::json!({
                "error": format!("unknown target dialect: {to_dialect}. Expected one of: snowflake, databricks, bigquery, duckdb")
            })
            .to_string();
        }
    };

    let result = rocky_sql::transpile::transpile(sql, from, to);

    let warnings: Vec<serde_json::Value> = result
        .warnings
        .iter()
        .map(|w| {
            serde_json::json!({
                "original": w.original,
                "reason": w.reason,
                "suggestion": w.suggestion,
            })
        })
        .collect();

    serde_json::json!({
        "sql": result.sql,
        "warnings": warnings,
        "replacements": result.replacements,
    })
    .to_string()
}

/// Validate whether a string is a legal SQL identifier.
///
/// Returns `true` if the identifier matches `^[a-zA-Z0-9_]+$`.
#[wasm_bindgen]
pub fn validate_identifier(name: &str) -> bool {
    rocky_sql::validation::validate_identifier(name).is_ok()
}

// ---------------------------------------------------------------------------
// Rocky DSL exports
// ---------------------------------------------------------------------------

/// Parse a Rocky DSL source string into a JSON AST.
///
/// Returns a JSON-serialized `RockyFile` on success,
/// or a JSON object `{"error": "..."}` on failure.
#[wasm_bindgen]
pub fn parse_rocky(source: &str) -> String {
    match rocky_lang::parse(source) {
        Ok(ast) => serde_json::to_string(&ast)
            .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#)),
        Err(e) => format!(r#"{{"error":{}}}"#, serde_json::json!(e.to_string())),
    }
}

/// Parse a Rocky DSL source string and lower it to SQL.
///
/// Returns the SQL string on success,
/// or a JSON object `{"error": "..."}` on failure.
#[wasm_bindgen]
pub fn lower_rocky_to_sql(source: &str) -> String {
    let ast = match rocky_lang::parse(source) {
        Ok(ast) => ast,
        Err(e) => return format!(r#"{{"error":{}}}"#, serde_json::json!(e.to_string())),
    };
    match rocky_lang::lower::lower_to_sql(&ast) {
        Ok(sql) => sql,
        Err(e) => format!(r#"{{"error":{}}}"#, serde_json::json!(e)),
    }
}

/// Format a Rocky DSL source string.
///
/// Normalizes indentation, collapses excessive blank lines, and ensures
/// a single trailing newline. The `indent` parameter controls the
/// indentation string (e.g., `"    "` for 4 spaces, `"\t"` for tabs).
///
/// Returns the formatted source text.
#[wasm_bindgen]
pub fn format_rocky(source: &str, indent: &str) -> String {
    rocky_lang::fmt::format_rocky(source, indent)
}

/// Return parse errors with positions for a Rocky DSL source string.
///
/// Returns a JSON array of error objects. The parser currently has no error
/// recovery, so the array contains at most one element. Format:
/// ```json
/// [{"offset": 42, "line": 3, "column": 5, "message": "...", "expected": "...", "found": "..."}]
/// ```
/// An empty array `[]` means no errors.
#[wasm_bindgen]
pub fn get_parse_errors(source: &str) -> String {
    match rocky_lang::parse(source) {
        Ok(_) => "[]".to_string(),
        Err(e) => {
            let (offset, expected, found) = match &e {
                rocky_lang::ParseError::UnexpectedToken {
                    expected,
                    found,
                    offset,
                } => (*offset, expected.to_string(), found.to_string()),
                rocky_lang::ParseError::UnexpectedEof { expected } => (
                    source.len().saturating_sub(1),
                    expected.to_string(),
                    "EOF".to_string(),
                ),
                rocky_lang::ParseError::InvalidNumber { value } => {
                    (0, "valid number".to_string(), value.clone())
                }
                rocky_lang::ParseError::EmptyFile => {
                    (0, "pipeline step".to_string(), "empty file".to_string())
                }
                rocky_lang::ParseError::TooDeeplyNested {
                    depth,
                    limit,
                    offset,
                } => (
                    *offset,
                    format!("expression nested no deeper than {limit}"),
                    format!("depth {depth}"),
                ),
            };

            // Convert byte offset to line/column.
            let (line, col) = byte_offset_to_line_col(source, offset);

            serde_json::json!([{
                "offset": offset,
                "line": line,
                "column": col,
                "message": e.to_string(),
                "expected": expected,
                "found": found,
            }])
            .to_string()
        }
    }
}

/// Compile a Rocky DSL model end-to-end: parse, lower to SQL, extract lineage.
///
/// Returns a JSON object combining all phases:
/// ```json
/// {
///   "ast": { ... },
///   "sql": "SELECT ...",
///   "lineage": { "source_tables": [...], "columns": [...] },
///   "errors": []
/// }
/// ```
///
/// If parsing fails, `ast`, `sql`, and `lineage` are `null` and `errors`
/// contains the parse error. If lowering fails, `sql` and `lineage` are
/// `null`. If lineage extraction fails, `lineage` is `null` with a warning
/// in `errors`.
#[wasm_bindgen]
pub fn compile_rocky_model(source: &str) -> String {
    let mut errors: Vec<serde_json::Value> = Vec::new();

    // Phase 1: Parse
    let ast = match rocky_lang::parse(source) {
        Ok(ast) => ast,
        Err(e) => {
            let (offset, expected, found) = match &e {
                rocky_lang::ParseError::UnexpectedToken {
                    expected,
                    found,
                    offset,
                } => (*offset, expected.to_string(), found.to_string()),
                rocky_lang::ParseError::UnexpectedEof { expected } => (
                    source.len().saturating_sub(1),
                    expected.to_string(),
                    "EOF".to_string(),
                ),
                rocky_lang::ParseError::InvalidNumber { value } => {
                    (0, "valid number".to_string(), value.clone())
                }
                rocky_lang::ParseError::EmptyFile => {
                    (0, "pipeline step".to_string(), "empty file".to_string())
                }
                rocky_lang::ParseError::TooDeeplyNested {
                    depth,
                    limit,
                    offset,
                } => (
                    *offset,
                    format!("expression nested no deeper than {limit}"),
                    format!("depth {depth}"),
                ),
            };
            let (line, col) = byte_offset_to_line_col(source, offset);

            errors.push(serde_json::json!({
                "phase": "parse",
                "severity": "error",
                "offset": offset,
                "line": line,
                "column": col,
                "message": e.to_string(),
                "expected": expected,
                "found": found,
            }));

            return serde_json::json!({
                "ast": null,
                "sql": null,
                "lineage": null,
                "errors": errors,
            })
            .to_string();
        }
    };

    let ast_json = serde_json::to_value(&ast).ok();

    // Phase 2: Lower to SQL
    let sql = match rocky_lang::lower::lower_to_sql(&ast) {
        Ok(sql) => sql,
        Err(e) => {
            errors.push(serde_json::json!({
                "phase": "lower",
                "severity": "error",
                "message": e,
            }));

            return serde_json::json!({
                "ast": ast_json,
                "sql": null,
                "lineage": null,
                "errors": errors,
            })
            .to_string();
        }
    };

    // Extract lineage from lowered SQL
    let lineage = match rocky_sql::lineage::extract_lineage(&sql) {
        Ok(result) => serde_json::to_value(&result).ok(),
        Err(e) => {
            errors.push(serde_json::json!({
                "phase": "lineage",
                "severity": "warning",
                "message": format!("lineage extraction failed: {e}"),
            }));
            None
        }
    };

    serde_json::json!({
        "ast": ast_json,
        "sql": sql,
        "lineage": lineage,
        "errors": errors,
    })
    .to_string()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Convert a byte offset to 1-based line and column numbers.
///
/// The offset is clamped to `source.len()` and walked back to the
/// nearest preceding char boundary so a multi-byte UTF-8 character at
/// the end of the buffer (or a deliberately mid-char offset from
/// adversarial LSP/WASM input) cannot panic the slice in `&source[..n]`.
fn byte_offset_to_line_col(source: &str, offset: usize) -> (usize, usize) {
    let mut clamped = offset.min(source.len());
    while clamped > 0 && !source.is_char_boundary(clamped) {
        clamped -= 1;
    }
    let before = &source[..clamped];
    let line = before.bytes().filter(|b| *b == b'\n').count() + 1;
    let col = before.rfind('\n').map_or(clamped, |pos| clamped - pos - 1) + 1;
    (line, col)
}

/// Parse a dialect name string into the transpile `Dialect` enum.
fn parse_dialect(name: &str) -> Option<rocky_sql::transpile::Dialect> {
    match name.to_lowercase().as_str() {
        "snowflake" => Some(rocky_sql::transpile::Dialect::Snowflake),
        "databricks" => Some(rocky_sql::transpile::Dialect::Databricks),
        "bigquery" => Some(rocky_sql::transpile::Dialect::BigQuery),
        "duckdb" => Some(rocky_sql::transpile::Dialect::DuckDB),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SQL lineage ----

    #[test]
    fn test_compile_sql_success() {
        let result = compile_sql("SELECT id, name FROM catalog.schema.users");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("source_tables").is_some());
        assert!(parsed.get("columns").is_some());
    }

    #[test]
    fn test_compile_sql_error() {
        let result = compile_sql("");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("error").is_some());
    }

    // ---- Transpile ----

    #[test]
    fn test_transpile_sql_snowflake_to_bigquery() {
        let result = transpile_sql(
            "SELECT NVL(a, b), LEN(name) FROM t",
            "snowflake",
            "bigquery",
        );
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let sql = parsed["sql"].as_str().unwrap();
        assert!(sql.contains("IFNULL(a, b)"));
        assert!(sql.contains("LENGTH(name)"));
        assert!(parsed["replacements"].as_u64().unwrap() > 0);
    }

    #[test]
    fn test_transpile_sql_same_dialect() {
        let result = transpile_sql("SELECT 1", "duckdb", "duckdb");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["sql"].as_str().unwrap(), "SELECT 1");
        assert_eq!(parsed["replacements"].as_u64().unwrap(), 0);
    }

    #[test]
    fn test_transpile_sql_unknown_dialect() {
        let result = transpile_sql("SELECT 1", "postgres", "duckdb");
        assert!(result.contains("error"));
        assert!(result.contains("postgres"));
    }

    #[test]
    fn test_transpile_sql_with_warnings() {
        let result = transpile_sql(
            "SELECT * FROM t QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1",
            "snowflake",
            "duckdb",
        );
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let warnings = parsed["warnings"].as_array().unwrap();
        assert!(!warnings.is_empty());
        assert!(
            warnings[0]["original"]
                .as_str()
                .unwrap()
                .contains("QUALIFY")
        );
    }

    // ---- Rocky DSL parse ----

    #[test]
    fn test_parse_rocky_success() {
        let result = parse_rocky("from orders\nselect { id, total }");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("pipeline").is_some());
    }

    #[test]
    fn test_parse_rocky_error() {
        let result = parse_rocky("invalid @@@ garbage");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("error").is_some());
    }

    // ---- Rocky DSL lower ----

    #[test]
    fn test_lower_rocky_to_sql_success() {
        let result = lower_rocky_to_sql("from orders\nselect { id, total }");
        // Should produce SQL, not a JSON error object
        assert!(!result.contains(r#""error""#));
        assert!(result.to_uppercase().contains("SELECT"));
    }

    #[test]
    fn test_lower_rocky_to_sql_error() {
        let result = lower_rocky_to_sql("invalid @@@ garbage");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert!(parsed.get("error").is_some());
    }

    // ---- Rocky DSL format ----

    #[test]
    fn test_format_rocky_normalizes_indentation() {
        let messy = "group customer_id {\ntotal: sum(amount),\ncount: count()\n}\n";
        let formatted = format_rocky(messy, "    ");
        assert!(formatted.contains("    total: sum(amount),"));
        assert!(formatted.contains("    count: count()"));
    }

    #[test]
    fn test_format_rocky_already_formatted() {
        let input = "from raw_orders\nwhere status != \"cancelled\"\n";
        assert_eq!(format_rocky(input, "    "), input);
    }

    #[test]
    fn test_format_rocky_trailing_whitespace() {
        let input = "from raw_orders   \nselect { id }  \n";
        let formatted = format_rocky(input, "    ");
        assert!(!formatted.contains("   \n"));
    }

    // ---- Parse errors ----

    #[test]
    fn test_get_parse_errors_success() {
        let result = get_parse_errors("from orders\nselect { id }");
        assert_eq!(result, "[]");
    }

    #[test]
    fn test_get_parse_errors_failure() {
        let result = get_parse_errors("filter x > 1");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert!(arr[0].get("offset").is_some());
        assert!(arr[0].get("line").is_some());
        assert!(arr[0].get("column").is_some());
        assert!(arr[0].get("message").is_some());
    }

    #[test]
    fn test_get_parse_errors_empty_file() {
        let result = get_parse_errors("");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["found"], "empty file");
    }

    // ---- Identifier validation ----

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("my_table"));
        assert!(validate_identifier("CamelCase"));
        assert!(validate_identifier("table123"));
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(!validate_identifier(""));
        assert!(!validate_identifier("has space"));
        assert!(!validate_identifier("has-dash"));
        assert!(!validate_identifier("DROP TABLE users--"));
    }

    // ---- Compile model (end-to-end) ----

    #[test]
    fn test_compile_rocky_model_success() {
        let result = compile_rocky_model("from orders\nselect { id, total }");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        // AST should be present
        assert!(parsed["ast"].is_object());
        assert!(parsed["ast"]["pipeline"].is_array());

        // SQL should be generated
        let sql = parsed["sql"].as_str().unwrap();
        assert!(sql.to_uppercase().contains("SELECT"));

        // Errors should be empty
        let errors = parsed["errors"].as_array().unwrap();
        assert!(errors.is_empty());
    }

    #[test]
    fn test_compile_rocky_model_parse_error() {
        let result = compile_rocky_model("invalid @@@ garbage");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        assert!(parsed["ast"].is_null());
        assert!(parsed["sql"].is_null());
        assert!(parsed["lineage"].is_null());

        let errors = parsed["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["phase"], "parse");
        assert_eq!(errors[0]["severity"], "error");
        assert!(errors[0].get("line").is_some());
        assert!(errors[0].get("column").is_some());
    }

    #[test]
    fn test_compile_rocky_model_empty_file() {
        let result = compile_rocky_model("");
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        assert!(parsed["ast"].is_null());
        let errors = parsed["errors"].as_array().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0]["phase"], "parse");
    }

    #[test]
    fn test_compile_rocky_model_with_aggregation() {
        let source = "from orders\ngroup customer_id {\n  total: sum(amount),\n  cnt: count()\n}";
        let result = compile_rocky_model(source);
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();

        assert!(parsed["ast"].is_object());
        assert!(parsed["sql"].is_string());
        let errors = parsed["errors"].as_array().unwrap();
        assert!(errors.is_empty());
    }

    // ---- Helpers ----

    #[test]
    fn test_parse_dialect_valid() {
        assert!(parse_dialect("snowflake").is_some());
        assert!(parse_dialect("Snowflake").is_some());
        assert!(parse_dialect("DATABRICKS").is_some());
        assert!(parse_dialect("bigquery").is_some());
        assert!(parse_dialect("duckdb").is_some());
    }

    #[test]
    fn test_parse_dialect_invalid() {
        assert!(parse_dialect("postgres").is_none());
        assert!(parse_dialect("mysql").is_none());
        assert!(parse_dialect("").is_none());
    }

    // ---- byte_offset_to_line_col regression coverage ----

    #[test]
    fn byte_offset_to_line_col_handles_offset_past_end() {
        // Empty source, large offset: no panic, returns (1, 1).
        assert_eq!(byte_offset_to_line_col("", 999), (1, 1));
        // Non-empty source, offset past end: clamped to len, no panic.
        let (line, _) = byte_offset_to_line_col("abc", 999);
        assert_eq!(line, 1);
    }

    #[test]
    fn byte_offset_to_line_col_handles_multibyte_at_eof() {
        // `é` is two bytes in UTF-8. An offset that lands one byte past
        // the end (or in the middle of the multi-byte char) used to
        // panic via `&source[..offset]`. The clamped + char-boundary
        // walk must keep this safe.
        let src = "let a = \"é\""; // 12 bytes total
        for offset in 0..=src.len() + 4 {
            // Should never panic.
            let _ = byte_offset_to_line_col(src, offset);
        }
    }

    #[test]
    fn byte_offset_to_line_col_handles_midchar_offset() {
        // `日本語` = 9 bytes; an offset of 1 lands inside the first
        // codepoint. We expect graceful walk-back to byte 0.
        let src = "日本語";
        let (line, col) = byte_offset_to_line_col(src, 1);
        assert_eq!(line, 1);
        assert_eq!(col, 1, "expected column 1 after walk-back to boundary");
    }
}
