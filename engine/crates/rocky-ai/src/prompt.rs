//! System prompt builder for AI model generation.
//!
//! Constructs the LLM system prompt from project context:
//! DSL spec, available models, their schemas, and target dialect.

/// Build the system prompt for model generation.
///
/// Includes Rocky DSL syntax reference, available source schemas,
/// existing model names, and generation instructions.
pub fn build_system_prompt(
    model_names: &[String],
    source_tables: &[(String, Vec<String>)], // (table_name, column_names)
    format: &str,
) -> String {
    let mut prompt = String::new();

    prompt.push_str("You are a data transformation expert. Generate Rocky model files.\n\n");

    // DSL reference
    if format == "rocky" {
        prompt.push_str("# Rocky DSL Syntax\n\n");
        prompt.push_str("Rocky DSL is a pipeline-oriented language. Key constructs:\n");
        prompt.push_str("- `from <model>` — start pipeline from a model or source\n");
        prompt.push_str("- `where <predicate>` — filter rows (use == for equality, != for NULL-safe inequality)\n");
        prompt.push_str("- `group <keys> { name: agg(col) }` — aggregate\n");
        prompt.push_str("- `derive { name: expr }` — add computed columns\n");
        prompt.push_str("- `select { col1, col2 }` — choose columns\n");
        prompt.push_str("- `join <model> as <alias> on <key> { keep col1, col2 }` — join\n");
        prompt.push_str("- `sort <col> [asc|desc]` — order results\n");
        prompt.push_str("- `take <n>` — limit rows\n");
        prompt.push_str("- `distinct` — deduplicate\n");
        prompt.push_str("- `@2025-01-01` — date literal\n");
        prompt.push_str("- `!=` compiles to IS DISTINCT FROM (NULL-safe)\n\n");
    } else {
        prompt.push_str("# SQL Model Format\n\n");
        prompt.push_str("Generate a standard SQL SELECT statement.\n");
        prompt.push_str("Reference other models by bare name (e.g., FROM orders).\n\n");
    }

    // Available models
    if !model_names.is_empty() {
        prompt.push_str("# Existing Models\n\n");
        prompt.push_str("These models are available as dependencies:\n");
        for name in model_names {
            prompt.push_str(&format!("- {name}\n"));
        }
        prompt.push('\n');
    }

    // Source schemas
    if !source_tables.is_empty() {
        prompt.push_str("# Available Source Tables\n\n");
        for (table, columns) in source_tables {
            prompt.push_str(&format!("- {table}: {}\n", columns.join(", ")));
        }
        prompt.push('\n');
    }

    prompt.push_str("# Instructions\n\n");
    prompt.push_str("Generate ONLY the model source code. No explanation, no markdown fencing.\n");
    prompt.push_str("Use existing models as dependencies where appropriate.\n");

    prompt
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_prompt_rocky() {
        let prompt = build_system_prompt(
            &["orders".to_string(), "customers".to_string()],
            &[(
                "source.raw.orders".to_string(),
                vec!["id".to_string(), "amount".to_string()],
            )],
            "rocky",
        );
        assert!(prompt.contains("Rocky DSL Syntax"));
        assert!(prompt.contains("orders"));
        assert!(prompt.contains("source.raw.orders"));
    }

    #[test]
    fn test_build_prompt_sql() {
        let prompt = build_system_prompt(&[], &[], "sql");
        assert!(prompt.contains("SQL Model Format"));
        assert!(!prompt.contains("Rocky DSL"));
    }
}
