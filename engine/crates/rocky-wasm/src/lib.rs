//! WASM bindings for Rocky's pure-Rust compiler pipeline.
//!
//! Exposes a minimal API surface for in-browser use:
//! - SQL lineage extraction (`rocky-sql`)
//! - Rocky DSL parsing and lowering (`rocky-lang`)
//! - SQL identifier validation (`rocky-sql`)
//!
//! All functions exchange data as JSON strings for simplest WASM interop.

use wasm_bindgen::prelude::*;

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

/// Validate whether a string is a legal SQL identifier.
///
/// Returns `true` if the identifier matches `^[a-zA-Z0-9_]+$`.
#[wasm_bindgen]
pub fn validate_identifier(name: &str) -> bool {
    rocky_sql::validation::validate_identifier(name).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
