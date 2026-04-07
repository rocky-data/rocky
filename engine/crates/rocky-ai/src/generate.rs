//! Intent → model generation with compile-verify loop.

use crate::client::{AiError, LlmClient, LlmResponse};
use crate::prompt;

/// Result of a successful model generation.
#[derive(Debug)]
pub struct GeneratedModel {
    /// Generated source code.
    pub source: String,
    /// Format: "rocky" or "sql".
    pub format: String,
    /// Suggested model name.
    pub name: String,
    /// Number of compilation attempts.
    pub attempts: usize,
    /// LLM response metadata.
    pub llm_response: LlmResponse,
}

/// Generate a model from a natural language intent.
///
/// Calls the LLM, then attempts to compile the result. If compilation
/// fails, feeds diagnostics back to the LLM for up to `max_attempts`.
pub async fn generate_model(
    intent: &str,
    model_names: &[String],
    source_tables: &[(String, Vec<String>)],
    format: &str,
    client: &LlmClient,
    max_attempts: usize,
) -> Result<GeneratedModel, AiError> {
    let system = prompt::build_system_prompt(model_names, source_tables, format);
    let mut error_context: Option<String> = None;

    for attempt in 1..=max_attempts {
        let response = client
            .generate(&system, intent, error_context.as_deref())
            .await?;

        let source = extract_code(&response.content);

        // Try to parse/validate the generated code
        let validation = validate_generated_code(&source, format);

        match validation {
            Ok(name) => {
                return Ok(GeneratedModel {
                    source,
                    format: format.to_string(),
                    name,
                    attempts: attempt,
                    llm_response: response,
                });
            }
            Err(errors) => {
                if attempt < max_attempts {
                    tracing::warn!(
                        attempt,
                        errors = errors.as_str(),
                        "generated model failed validation, retrying"
                    );
                    error_context = Some(errors);
                } else {
                    return Err(AiError::CompileFailed {
                        attempts: max_attempts,
                    });
                }
            }
        }
    }

    Err(AiError::CompileFailed {
        attempts: max_attempts,
    })
}

/// Extract code from LLM response, stripping markdown fences if present.
pub fn extract_code(content: &str) -> String {
    let trimmed = content.trim();

    // Strip ```rocky or ```sql fences
    if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest
            .strip_prefix("rocky")
            .or_else(|| rest.strip_prefix("sql"))
            .unwrap_or(rest);
        if let Some(code) = rest.strip_suffix("```") {
            return code.trim().to_string();
        }
    }

    trimmed.to_string()
}

/// Validate generated code using the compiler (type-check + contract validation).
/// Returns the suggested model name on success, or error string on failure.
fn validate_generated_code(source: &str, format: &str) -> Result<String, String> {
    if source.trim().is_empty() {
        return Err("generated code is empty".to_string());
    }

    // Step 1: Parse
    let (sql, name) = match format {
        "rocky" => {
            let rocky_file =
                rocky_lang::parse(source).map_err(|e| format!("Rocky DSL parse error: {e}"))?;
            let sql = rocky_lang::lower::lower_to_sql(&rocky_file)
                .map_err(|e| format!("Rocky DSL lowering error: {e}"))?;
            let name = source
                .lines()
                .find_map(|line| {
                    let trimmed = line.trim();
                    trimmed
                        .strip_prefix("from ")
                        .and_then(|rest| rest.split_whitespace().next())
                        .map(|model| format!("gen_{model}"))
                })
                .unwrap_or_else(|| "generated_model".to_string());
            (sql, name)
        }
        "sql" => {
            rocky_sql::parser::parse_single_statement(source)
                .map_err(|e| format!("SQL parse error: {e}"))?;
            ("generated_model".to_string(), source.to_string())
        }
        _ => return Err(format!("unknown format: {format}")),
    };

    // Step 2: Compile — run the full compiler pipeline for type checking
    // Create a temporary model and compile it
    let model = rocky_core::models::Model {
        config: rocky_core::models::ModelConfig {
            name: name.clone(),
            depends_on: vec![],
            strategy: rocky_core::models::StrategyConfig::default(),
            target: rocky_core::models::TargetConfig {
                catalog: "generated".to_string(),
                schema: "ai".to_string(),
                table: name.clone(),
            },
            sources: vec![],
            intent: None,
            freshness: None,
            tests: vec![],
            format: None,
            format_options: None,
        },
        sql: if format == "rocky" {
            sql
        } else {
            source.to_string()
        },
        file_path: format!("generated/{name}.{format}"),
        contract_path: None,
    };

    match rocky_compiler::project::Project::from_models(vec![model]) {
        Ok(project) => {
            // Build semantic graph and type-check
            let graph = rocky_compiler::semantic::build_semantic_graph(
                &project,
                &std::collections::HashMap::new(),
            )
            .map_err(|e| format!("semantic analysis failed: {e}"))?;

            let type_result = rocky_compiler::typecheck::typecheck_project(
                &graph,
                &std::collections::HashMap::new(),
                None,
            );

            // Collect errors (warnings are acceptable)
            let errors: Vec<String> = type_result
                .diagnostics
                .iter()
                .filter(|d| d.is_error())
                .map(|d| d.to_string())
                .collect();

            if errors.is_empty() {
                Ok(name)
            } else {
                Err(errors.join("\n"))
            }
        }
        Err(e) => Err(format!("project loading failed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_code_plain() {
        assert_eq!(
            extract_code("from orders\nselect { id }"),
            "from orders\nselect { id }"
        );
    }

    #[test]
    fn test_extract_code_fenced() {
        let input = "```rocky\nfrom orders\nselect { id }\n```";
        assert_eq!(extract_code(input), "from orders\nselect { id }");
    }

    #[test]
    fn test_extract_code_sql_fenced() {
        let input = "```sql\nSELECT * FROM orders\n```";
        assert_eq!(extract_code(input), "SELECT * FROM orders");
    }

    #[test]
    fn test_validate_rocky() {
        let result = validate_generated_code("from orders\nselect { id }", "rocky");
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gen_orders");
    }

    #[test]
    fn test_validate_sql() {
        let result = validate_generated_code("SELECT id FROM orders", "sql");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty() {
        let result = validate_generated_code("", "rocky");
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_invalid_rocky() {
        let result = validate_generated_code("this is not valid rocky", "rocky");
        assert!(result.is_err());
    }
}
