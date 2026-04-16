//! dbt macro detection and expansion.
//!
//! Detects Jinja macro usages in dbt SQL (beyond ref/source/config/this/is_incremental)
//! and attempts to resolve them via known expansions or manifest fallback.

use std::collections::HashMap;

use regex::Regex;

use super::dbt::ImportWarning;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A detected macro usage in dbt SQL.
#[derive(Debug, Clone)]
pub struct MacroUsage {
    /// Package name, e.g. "dbt_utils". None for project-local macros.
    pub package: Option<String>,
    /// Macro name, e.g. "star", "generate_surrogate_key".
    pub name: String,
    /// Raw argument string inside the parentheses.
    pub args: String,
    /// Byte span (start, end) in the source SQL.
    pub span: (usize, usize),
}

/// How a macro was resolved.
#[derive(Debug, Clone, PartialEq)]
pub enum MacroResolution {
    /// Successfully expanded to SQL.
    Expanded(String),
    /// Resolved by using compiled_code from manifest.json.
    ManifestFallback,
    /// Cannot expand; returns a warning message.
    Unsupported(String),
}

// ---------------------------------------------------------------------------
// Macro detection
// ---------------------------------------------------------------------------

/// Detect macro usages in dbt SQL.
///
/// Finds patterns like `{{ dbt_utils.star(...) }}` and `{{ custom_macro(...) }}`.
/// Skips already-handled macros: ref(), source(), config(), this, is_incremental().
pub fn detect_macros(sql: &str) -> Vec<MacroUsage> {
    // Match {{ optional_package.macro_name(...) }} patterns.
    // The argument matcher handles one level of nested parens (for ref() calls inside args).
    let re =
        Regex::new(r"\{\{\s*((?:(\w+)\.)?(\w+))\s*\(((?:[^()]*|\([^()]*\))*)\)\s*\}\}").unwrap();

    let skip_names = ["ref", "source", "config", "this", "var", "is_incremental"];

    let mut usages = Vec::new();

    for caps in re.captures_iter(sql) {
        let full_match = caps.get(0).unwrap();
        let package = caps.get(2).map(|m| m.as_str().to_string());
        let name = caps[3].to_string();
        let args = caps[4].to_string();

        // Skip already-handled macros
        if package.is_none() && skip_names.contains(&name.as_str()) {
            continue;
        }

        // Skip adapter dispatch
        if package.is_none() && name == "adapter" {
            continue;
        }

        usages.push(MacroUsage {
            package,
            name,
            args: args.trim().to_string(),
            span: (full_match.start(), full_match.end()),
        });
    }

    usages
}

// ---------------------------------------------------------------------------
// dbt_utils.star() expansion
// ---------------------------------------------------------------------------

/// Expand a `dbt_utils.star()` call using known column names.
///
/// Parses arguments:
/// - `from=ref('model')` -> resolve model name
/// - `except=[...]` -> exclude these columns
/// - `relation_alias=...` -> prefix with alias
pub fn expand_star(
    args: &str,
    known_columns: &HashMap<String, Vec<String>>,
) -> Result<String, String> {
    let model_name = extract_star_from_arg(args)?;
    let except = extract_star_except(args);
    let alias = extract_star_alias(args);

    let columns = known_columns
        .get(&model_name)
        .ok_or_else(|| format!("model '{model_name}' not found in known columns"))?;

    let filtered: Vec<String> = columns
        .iter()
        .filter(|c| !except.contains(&c.as_str()))
        .map(|c| match &alias {
            Some(a) => format!("{a}.{c}"),
            None => c.clone(),
        })
        .collect();

    if filtered.is_empty() {
        return Err(format!(
            "no columns remaining after applying except filter to '{model_name}'"
        ));
    }

    Ok(filtered.join(", "))
}

fn extract_star_from_arg(args: &str) -> Result<String, String> {
    // Match: from=ref('model_name') or from=ref("model_name") or just ref('model_name')
    let re = Regex::new(r#"(?:from\s*=\s*)?ref\s*\(\s*['"](\w+)['"]\s*\)"#).unwrap();
    if let Some(caps) = re.captures(args) {
        return Ok(caps[1].to_string());
    }
    Err("could not parse from=ref('model') in star() arguments".to_string())
}

fn extract_star_except(args: &str) -> Vec<&str> {
    // Match: except=['col1', 'col2']
    let re = Regex::new(r"except\s*=\s*\[([^\]]*)\]").unwrap();
    if let Some(caps) = re.captures(args) {
        let inner = caps.get(1).unwrap().as_str();
        return inner
            .split(',')
            .map(|s| s.trim().trim_matches('\'').trim_matches('"'))
            .filter(|s| !s.is_empty())
            .collect();
    }
    Vec::new()
}

fn extract_star_alias(args: &str) -> Option<String> {
    // Match: relation_alias='o' or relation_alias="o"
    let re = Regex::new(r#"relation_alias\s*=\s*['"](\w+)['"]"#).unwrap();
    re.captures(args).map(|caps| caps[1].to_string())
}

// ---------------------------------------------------------------------------
// Macro resolution orchestrator
// ---------------------------------------------------------------------------

/// Attempt to resolve all detected macros in SQL.
///
/// Resolution strategy per macro:
/// 1. `dbt_utils.star()` -> expand using known columns
/// 2. Any macro with manifest compiled SQL available -> use manifest SQL
/// 3. Unknown macro without manifest -> replace with TODO comment + warning
pub fn resolve_macros(
    sql: &str,
    macros: &[MacroUsage],
    known_columns: &HashMap<String, Vec<String>>,
    manifest_sql: Option<&str>,
) -> (String, Vec<ImportWarning>, Vec<MacroResolution>) {
    let mut warnings = Vec::new();
    let mut resolutions = Vec::new();

    // If there are no macros, return as-is
    if macros.is_empty() {
        return (sql.to_string(), warnings, resolutions);
    }

    // If we have manifest SQL, use it for all macros (it has everything pre-resolved)
    if let Some(compiled) = manifest_sql {
        for m in macros {
            resolutions.push(MacroResolution::ManifestFallback);
            let full_name = match &m.package {
                Some(pkg) => format!("{pkg}.{}", m.name),
                None => m.name.clone(),
            };
            tracing::debug!("macro '{full_name}' resolved via manifest compiled SQL");
        }
        return (compiled.to_string(), warnings, resolutions);
    }

    // Process macros from end to start so byte offsets remain valid
    let mut result = sql.to_string();
    let mut sorted_macros: Vec<&MacroUsage> = macros.iter().collect();
    sorted_macros.sort_by_key(|m| std::cmp::Reverse(m.span.0));

    for m in sorted_macros {
        let full_name = match &m.package {
            Some(pkg) => format!("{pkg}.{}", m.name),
            None => m.name.clone(),
        };

        // Try to expand known macros
        let resolution = if m.package.as_deref() == Some("dbt_utils") && m.name == "star" {
            match expand_star(&m.args, known_columns) {
                Ok(expanded) => {
                    result.replace_range(m.span.0..m.span.1, &expanded);
                    MacroResolution::Expanded(expanded)
                }
                Err(e) => {
                    let todo = format!("/* TODO: {full_name}() - {e} */");
                    result.replace_range(m.span.0..m.span.1, &todo);
                    warnings.push(ImportWarning {
                        model: String::new(), // filled in by caller
                        category: super::dbt::WarningCategory::UnsupportedMacro,
                        message: format!("could not expand {full_name}(): {e}"),
                        suggestion: Some(
                            "use manifest.json import for full Jinja resolution".to_string(),
                        ),
                    });
                    MacroResolution::Unsupported(e)
                }
            }
        } else {
            let todo = format!("/* TODO: unsupported macro: {full_name}() */");
            result.replace_range(m.span.0..m.span.1, &todo);
            warnings.push(ImportWarning {
                model: String::new(),
                category: super::dbt::WarningCategory::UnsupportedMacro,
                message: format!("unsupported macro: {full_name}()"),
                suggestion: Some("use manifest.json import for full Jinja resolution".to_string()),
            });
            MacroResolution::Unsupported(format!("no expansion available for {full_name}()"))
        };

        resolutions.push(resolution);
    }

    (result, warnings, resolutions)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_dbt_utils_star() {
        let sql =
            "SELECT {{ dbt_utils.star(from=ref('stg_orders')) }} FROM {{ ref('stg_orders') }}";
        let macros = detect_macros(sql);
        assert_eq!(macros.len(), 1);
        assert_eq!(macros[0].package.as_deref(), Some("dbt_utils"));
        assert_eq!(macros[0].name, "star");
        assert!(macros[0].args.contains("ref('stg_orders')"));
    }

    #[test]
    fn test_detect_custom_macro() {
        let sql = "SELECT {{ generate_surrogate_key('col1', 'col2') }} FROM t";
        let macros = detect_macros(sql);
        assert_eq!(macros.len(), 1);
        assert!(macros[0].package.is_none());
        assert_eq!(macros[0].name, "generate_surrogate_key");
    }

    #[test]
    fn test_skip_ref_source_config() {
        let sql = r#"
            {{ config(materialized='table') }}
            SELECT * FROM {{ ref('orders') }}
            JOIN {{ source('raw', 'customers') }}
        "#;
        let macros = detect_macros(sql);
        assert!(macros.is_empty());
    }

    #[test]
    fn test_multiple_macros_detected() {
        let sql = r#"
            SELECT
                {{ dbt_utils.star(from=ref('orders')) }},
                {{ dbt_utils.generate_surrogate_key('id', 'date') }}
            FROM {{ ref('orders') }}
        "#;
        let macros = detect_macros(sql);
        assert_eq!(macros.len(), 2);
    }

    #[test]
    fn test_no_macros_detected() {
        let sql = "SELECT id, name FROM orders WHERE id > 0";
        let macros = detect_macros(sql);
        assert!(macros.is_empty());
    }

    // --- star() expansion tests ---

    #[test]
    fn test_expand_star_all_columns() {
        let mut cols = HashMap::new();
        cols.insert(
            "stg_orders".to_string(),
            vec![
                "order_id".to_string(),
                "customer_id".to_string(),
                "amount".to_string(),
            ],
        );

        let result = expand_star("from=ref('stg_orders')", &cols).unwrap();
        assert_eq!(result, "order_id, customer_id, amount");
    }

    #[test]
    fn test_expand_star_with_except() {
        let mut cols = HashMap::new();
        cols.insert(
            "stg_orders".to_string(),
            vec![
                "order_id".to_string(),
                "internal_id".to_string(),
                "amount".to_string(),
            ],
        );

        let result = expand_star("from=ref('stg_orders'), except=['internal_id']", &cols).unwrap();
        assert_eq!(result, "order_id, amount");
    }

    #[test]
    fn test_expand_star_with_alias() {
        let mut cols = HashMap::new();
        cols.insert(
            "stg_orders".to_string(),
            vec!["order_id".to_string(), "amount".to_string()],
        );

        let result = expand_star("from=ref('stg_orders'), relation_alias='o'", &cols).unwrap();
        assert_eq!(result, "o.order_id, o.amount");
    }

    #[test]
    fn test_expand_star_model_not_found() {
        let cols = HashMap::new();
        let result = expand_star("from=ref('nonexistent')", &cols);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    // --- resolve_macros tests ---

    #[test]
    fn test_resolve_with_manifest_fallback() {
        let sql = "SELECT {{ dbt_utils.star(from=ref('orders')) }} FROM orders";
        let macros = detect_macros(sql);
        let cols = HashMap::new();
        let manifest_sql = "SELECT order_id, amount FROM orders";

        let (resolved, warnings, resolutions) =
            resolve_macros(sql, &macros, &cols, Some(manifest_sql));
        assert_eq!(resolved, manifest_sql);
        assert!(warnings.is_empty());
        assert!(matches!(resolutions[0], MacroResolution::ManifestFallback));
    }

    #[test]
    fn test_resolve_star_without_manifest() {
        let sql = "SELECT {{ dbt_utils.star(from=ref('orders')) }} FROM orders";
        let macros = detect_macros(sql);
        let mut cols = HashMap::new();
        cols.insert(
            "orders".to_string(),
            vec!["id".to_string(), "amount".to_string()],
        );

        let (resolved, warnings, resolutions) = resolve_macros(sql, &macros, &cols, None);
        assert!(resolved.contains("id, amount"));
        assert!(warnings.is_empty());
        assert!(matches!(&resolutions[0], MacroResolution::Expanded(s) if s == "id, amount"));
    }

    #[test]
    fn test_resolve_unknown_macro_without_manifest() {
        let sql = "SELECT {{ custom_func('a') }} FROM t";
        let macros = detect_macros(sql);
        let cols = HashMap::new();

        let (resolved, warnings, _) = resolve_macros(sql, &macros, &cols, None);
        assert!(resolved.contains("TODO: unsupported macro: custom_func()"));
        assert_eq!(warnings.len(), 1);
    }

    #[test]
    fn test_resolve_no_macros() {
        let sql = "SELECT 1 FROM t";
        let macros = detect_macros(sql);

        let (resolved, warnings, _) = resolve_macros(sql, &macros, &HashMap::new(), None);
        assert_eq!(resolved, sql);
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_detect_macros_skip_var() {
        let sql = "SELECT {{ var('my_var') }} FROM t";
        let macros = detect_macros(sql);
        assert!(macros.is_empty());
    }
}
