//! Generate intent descriptions from existing model code.
//!
//! Uses the LLM to analyze model source code, column schemas, and upstream
//! dependencies to produce a human-readable description of what the model does.

use rocky_compiler::compile::CompileResult;
use rocky_core::models::Model;

use crate::client::{AiError, LlmClient};

/// Generate an intent description for a model.
pub async fn explain_model(
    model: &Model,
    compile_result: &CompileResult,
    client: &LlmClient,
) -> Result<String, AiError> {
    let model_name = &model.config.name;

    // Build context: column schemas
    let mut columns_desc = String::new();
    if let Some(cols) = compile_result.type_check.typed_models.get(model_name) {
        for col in cols {
            let nullable = if col.nullable { " (nullable)" } else { "" };
            columns_desc.push_str(&format!(
                "  - {}: {:?}{}\n",
                col.name, col.data_type, nullable
            ));
        }
    }

    // Build context: upstream models and their schemas
    let mut upstream_desc = String::new();
    if let Some(schema) = compile_result.semantic_graph.model_schema(model_name) {
        for up_name in &schema.upstream {
            upstream_desc.push_str(&format!("\nUpstream model '{up_name}':\n"));
            if let Some(cols) = compile_result.type_check.typed_models.get(up_name) {
                for col in cols {
                    upstream_desc.push_str(&format!("  - {}: {:?}\n", col.name, col.data_type));
                }
            }
        }
    }

    let system = "You are a data engineer explaining SQL/Rocky transformation models.\n\
        \n\
        Rules:\n\
        - Describe what the model does in 2-3 sentences\n\
        - Focus on business logic, not SQL mechanics\n\
        - Include the grain (what one row represents)\n\
        - Mention key filters, joins, and aggregations\n\
        - Return ONLY the description text, no markdown, no quotes, no prefix";

    let user = format!(
        "Model: {model_name}\n\n\
         Source code:\n```\n{}\n```\n\n\
         Output columns:\n{columns_desc}\n\
         {upstream_desc}\n\
         Describe what this model does.",
        model.sql
    );

    let response = client.generate(system, &user, None).await?;

    // Clean up: trim whitespace, remove accidental quotes
    let intent = response.content.trim().trim_matches('"').trim().to_string();

    Ok(intent)
}

/// Write intent to a model's TOML sidecar config file.
pub fn save_intent_to_config(model: &Model, intent: &str) -> Result<(), std::io::Error> {
    let toml_path = if model.file_path.ends_with(".rocky") {
        format!("{}.toml", model.file_path)
    } else if model.file_path.ends_with(".sql") {
        model.file_path.replace(".sql", ".toml")
    } else {
        format!("{}.toml", model.file_path)
    };

    let path = std::path::Path::new(&toml_path);

    // Read existing config or start fresh
    let mut config: toml::Value = if path.exists() {
        let content = std::fs::read_to_string(path)?;
        toml::from_str(&content).unwrap_or(toml::Value::Table(toml::map::Map::new()))
    } else {
        toml::Value::Table(toml::map::Map::new())
    };

    // Set intent field
    if let toml::Value::Table(ref mut table) = config {
        table.insert(
            "intent".to_string(),
            toml::Value::String(intent.to_string()),
        );
        if !table.contains_key("name") {
            table.insert(
                "name".to_string(),
                toml::Value::String(model.config.name.clone()),
            );
        }
    }

    let toml_str = toml::to_string_pretty(&config).map_err(std::io::Error::other)?;

    std::fs::write(path, toml_str)
}
