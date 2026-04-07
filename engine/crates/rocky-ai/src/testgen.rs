//! Generate test assertions from model intent.
//!
//! Uses the LLM to analyze a model's intent and schema to produce SQL
//! assertions that verify the model satisfies its business requirements.

use rocky_compiler::compile::CompileResult;
use rocky_core::models::Model;
use serde::{Deserialize, Serialize};

use crate::client::{AiError, LlmClient};

/// A generated test assertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestAssertion {
    pub name: String,
    pub sql: String,
    pub description: String,
}

/// Generate test assertions for a model based on its intent and schema.
pub async fn generate_tests(
    model: &Model,
    compile_result: &CompileResult,
    client: &LlmClient,
) -> Result<Vec<TestAssertion>, AiError> {
    let model_name = &model.config.name;
    let intent = model.config.intent.as_deref().unwrap_or("");

    // Build column context
    let mut columns_desc = String::new();
    if let Some(cols) = compile_result.type_check.typed_models.get(model_name) {
        for col in cols {
            let nullable = if col.nullable {
                " (nullable)"
            } else {
                " (not null)"
            };
            columns_desc.push_str(&format!(
                "  - {}: {:?}{}\n",
                col.name, col.data_type, nullable
            ));
        }
    }

    // Build target table reference
    let target = &model.config.target;
    let full_table = format!("{}.{}.{}", target.catalog, target.schema, target.table);

    let system = "You are a data quality engineer generating test assertions for SQL models.\n\
        \n\
        Rules:\n\
        - Each test is a SQL query that returns 0 rows when the assertion holds\n\
        - Cover: not-null constraints, grain uniqueness, value range expectations, referential integrity\n\
        - Use the full table name provided\n\
        - Return tests as a JSON array of objects with fields: name, sql, description\n\
        - Return ONLY the JSON array, no markdown fences, no explanation";

    let user = if intent.is_empty() {
        format!(
            "Model: {model_name}\nTable: {full_table}\n\n\
             Source code:\n```\n{}\n```\n\n\
             Columns:\n{columns_desc}\n\
             Generate test assertions based on the code.",
            model.sql
        )
    } else {
        format!(
            "Model: {model_name}\nTable: {full_table}\n\
             Intent: {intent}\n\n\
             Source code:\n```\n{}\n```\n\n\
             Columns:\n{columns_desc}\n\
             Generate test assertions that verify this model satisfies its intent.",
            model.sql
        )
    };

    let response = client.generate(system, &user, None).await?;

    // Parse JSON response
    let content = response.content.trim();
    // Strip markdown fences if present
    let json_str = if content.starts_with("```") {
        content
            .lines()
            .skip(1)
            .take_while(|l| !l.starts_with("```"))
            .collect::<Vec<&str>>()
            .join("\n")
    } else {
        content.to_string()
    };

    let assertions: Vec<TestAssertion> =
        serde_json::from_str(&json_str).map_err(|e| AiError::Api {
            message: format!("Failed to parse test assertions: {e}"),
        })?;

    Ok(assertions)
}

/// Save test assertions to SQL files in a tests directory.
pub fn save_tests(
    model_name: &str,
    assertions: &[TestAssertion],
    tests_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    std::fs::create_dir_all(tests_dir)?;

    for assertion in assertions {
        let file_name = format!(
            "{}_{}.sql",
            model_name,
            assertion.name.replace(' ', "_").to_lowercase()
        );
        let path = tests_dir.join(&file_name);

        let content = format!(
            "-- Test: {}\n-- {}\n{}\n",
            assertion.name, assertion.description, assertion.sql
        );

        std::fs::write(path, content)?;
    }

    Ok(())
}
