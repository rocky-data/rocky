//! Intent → model generation with schema-grounded prompting and project-aware
//! compile-verify loop.

use std::collections::HashMap;

use rocky_compiler::types::TypedColumn;
use rocky_core::ir::ColumnInfo;
use rocky_core::models::Model;

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

/// Project-aware validation context. When provided, the generated model is
/// typechecked alongside the existing project models so upstream column
/// types propagate into its output schema. Callers should pass project
/// models that already typecheck cleanly; upstream errors are surfaced
/// rather than swallowed.
pub struct ValidationContext<'a> {
    pub project_models: &'a [Model],
    pub source_schemas: &'a HashMap<String, Vec<TypedColumn>>,
    pub source_column_info: &'a HashMap<String, Vec<ColumnInfo>>,
}

/// Generate a model from a natural language intent.
///
/// Renders the system prompt from typed upstream schemas (sources + existing
/// models) — this is the "schema grounding" that drives first-attempt compile
/// success. Then runs a compile-verify loop: parse → semantic graph →
/// typecheck. When `context` is `Some`, the generated model is typechecked
/// inside the full project graph so upstream column types propagate into
/// its output schema (rather than every upstream column resolving to
/// `RockyType::Unknown`, as in the isolated path). Rocky's typechecker is
/// lenient on unresolved column references, so this is NOT a hard
/// "no hallucinated columns" guarantee — the prompt grounding does most of
/// that work, and warehouse execution catches the rest.
pub async fn generate_model(
    intent: &str,
    model_schemas: &[(String, Vec<TypedColumn>)],
    source_tables: &[(String, Vec<TypedColumn>)],
    format: &str,
    client: &LlmClient,
    max_attempts: usize,
    context: Option<&ValidationContext<'_>>,
) -> Result<GeneratedModel, AiError> {
    let system = prompt::build_system_prompt(model_schemas, source_tables, format);
    let mut error_context: Option<String> = None;

    for attempt in 1..=max_attempts {
        let response = client
            .generate(&system, intent, error_context.as_deref())
            .await?;

        let source = extract_code(&response.content);
        let validation = validate_generated_code(&source, format, context);

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

/// Validate generated code. When `context` is `Some`, typechecks against the
/// full project (upstream typed schemas visible); otherwise isolates the
/// generated model (legacy behavior, used in unit tests).
fn validate_generated_code(
    source: &str,
    format: &str,
    context: Option<&ValidationContext<'_>>,
) -> Result<String, String> {
    if source.trim().is_empty() {
        return Err("generated code is empty".to_string());
    }

    let (sql, name) = parse_to_sql(source, format)?;
    let generated = build_generated_model(&name, sql, source, format);

    let (models, source_schemas, source_column_info) = match context {
        Some(ctx) => {
            let mut models: Vec<Model> = ctx.project_models.to_vec();
            models.push(generated);
            (
                models,
                ctx.source_schemas.clone(),
                ctx.source_column_info.clone(),
            )
        }
        None => (vec![generated], HashMap::new(), HashMap::new()),
    };

    let project = rocky_compiler::project::Project::from_models(models)
        .map_err(|e| format!("project loading failed: {e}"))?;

    let graph = rocky_compiler::semantic::build_semantic_graph(&project, &source_column_info)
        .map_err(|e| format!("semantic analysis failed: {e}"))?;

    let type_result = rocky_compiler::typecheck::typecheck_project(&graph, &source_schemas, None);

    // Surface every error-severity diagnostic in the merged project. The
    // caller is responsible for passing project models that already
    // typecheck cleanly; if an upstream errors here, the generated model's
    // compile-verify retry loop won't rescue it, and silently swallowing
    // upstream diagnostics would hide real problems.
    let errors: Vec<String> = type_result
        .diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(std::string::ToString::to_string)
        .collect();

    if errors.is_empty() {
        Ok(name)
    } else {
        Err(errors.join("\n"))
    }
}

/// Parse Rocky DSL or raw SQL to a SQL string + suggested name.
fn parse_to_sql(source: &str, format: &str) -> Result<(String, String), String> {
    match format {
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
            Ok((sql, name))
        }
        "sql" => {
            rocky_sql::parser::parse_single_statement(source)
                .map_err(|e| format!("SQL parse error: {e}"))?;
            Ok((source.to_string(), "generated_model".to_string()))
        }
        _ => Err(format!("unknown format: {format}")),
    }
}

/// Build a `Model` for the generated code.
fn build_generated_model(name: &str, sql: String, source: &str, format: &str) -> Model {
    rocky_core::models::Model {
        config: rocky_core::models::ModelConfig {
            name: name.to_string(),
            depends_on: vec![],
            strategy: rocky_core::models::StrategyConfig::default(),
            target: rocky_core::models::TargetConfig {
                catalog: "generated".to_string(),
                schema: "ai".to_string(),
                table: name.to_string(),
            },
            sources: vec![],
            adapter: None,
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
    fn test_validate_rocky_isolated() {
        let result = validate_generated_code("from orders\nselect { id }", "rocky", None);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "gen_orders");
    }

    #[test]
    fn test_validate_sql_isolated() {
        let result = validate_generated_code("SELECT id FROM orders", "sql", None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty() {
        let result = validate_generated_code("", "rocky", None);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_invalid_rocky() {
        let result = validate_generated_code("this is not valid rocky", "rocky", None);
        assert!(result.is_err());
    }

    /// Build an upstream `Model` by hand. Used in project-aware validation tests.
    fn upstream_model(name: &str, sql: &str) -> Model {
        rocky_core::models::Model {
            config: rocky_core::models::ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: rocky_core::models::StrategyConfig::default(),
                target: rocky_core::models::TargetConfig {
                    catalog: "test".to_string(),
                    schema: "test".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
            },
            sql: sql.to_string(),
            file_path: format!("upstream/{name}.sql"),
            contract_path: None,
        }
    }

    #[test]
    fn test_project_aware_compiles_alongside_upstream() {
        // With project context, the generated model is typechecked inside
        // a graph that already contains `orders`. Its output schema is
        // derived from real upstream types rather than `RockyType::Unknown`,
        // which matters when downstream models (or contract validation)
        // consume the generated model.
        let upstream = upstream_model(
            "orders",
            "SELECT CAST(1 AS BIGINT) AS id, CAST('x' AS VARCHAR) AS name",
        );
        let upstream_models = vec![upstream];
        let empty_schemas = HashMap::new();
        let empty_cols = HashMap::new();
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
            source_column_info: &empty_cols,
        };

        let valid = "from orders\nselect { id, name }";
        let result = validate_generated_code(valid, "rocky", Some(&ctx));
        assert!(
            result.is_ok(),
            "expected Ok for real-upstream reference, got {result:?}"
        );
    }
}
