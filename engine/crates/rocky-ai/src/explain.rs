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
///
/// If the sidecar already exists but cannot be parsed as TOML, this
/// function returns an error rather than silently overwriting the file
/// with a fresh `intent = "..."` document — the original behavior would
/// erase whatever the user was editing (e.g. mid-keystroke during an
/// editor flush, or a hand-rolled TOML with a typo). The caller can
/// surface the message to the user, who is expected to fix or remove
/// the file before retrying.
pub fn save_intent_to_config(model: &Model, intent: &str) -> Result<(), std::io::Error> {
    let toml_path = if model.file_path.ends_with(".rocky") {
        format!("{}.toml", model.file_path)
    } else if model.file_path.ends_with(".sql") {
        model.file_path.replace(".sql", ".toml")
    } else {
        format!("{}.toml", model.file_path)
    };

    let path = std::path::Path::new(&toml_path);

    // Read existing config or start fresh. Refuse to clobber a sidecar
    // that exists but doesn't parse — overwriting it would delete the
    // user's in-flight edits.
    let mut config: toml::Value = if path.exists() {
        let content = std::fs::read_to_string(path)?;
        match toml::from_str(&content) {
            Ok(value) => value,
            Err(err) => {
                return Err(std::io::Error::other(format!(
                    "refusing to overwrite unparseable TOML at {}: {err}; \
                     please fix the file first or remove it and rerun",
                    path.display()
                )));
            }
        }
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

#[cfg(test)]
mod save_intent_tests {
    use super::*;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};
    use std::io::Write;

    fn make_model(file_path: String) -> Model {
        Model {
            sql: String::new(),
            file_path,
            contract_path: None,
            config: ModelConfig {
                name: "test_model".to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "c".into(),
                    schema: "s".into(),
                    table: "test_model".into(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                retention: None,
            },
        }
    }

    #[test]
    fn save_intent_refuses_to_overwrite_unparseable_toml() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("model.sql");
        let toml_path = dir.path().join("model.toml");

        // Pre-populate a broken TOML sidecar.
        let broken = "name = \"unfinished string\nintent = \"old\"\n";
        let mut f = std::fs::File::create(&toml_path).unwrap();
        f.write_all(broken.as_bytes()).unwrap();
        drop(f);

        let model = make_model(sql_path.to_string_lossy().to_string());
        let result = save_intent_to_config(&model, "the new intent");

        // Must return an error, not silently overwrite.
        let err = result.expect_err("save should refuse to overwrite unparseable TOML");
        let msg = format!("{err}");
        assert!(
            msg.contains("refusing to overwrite unparseable TOML"),
            "unexpected error message: {msg}"
        );

        // The original (broken) file must be untouched.
        let on_disk = std::fs::read_to_string(&toml_path).unwrap();
        assert_eq!(
            on_disk, broken,
            "save_intent_to_config clobbered an unparseable file"
        );
    }

    #[test]
    fn save_intent_creates_fresh_toml_when_missing() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("model.sql");
        let model = make_model(sql_path.to_string_lossy().to_string());

        save_intent_to_config(&model, "some intent").unwrap();

        let toml_path = dir.path().join("model.toml");
        let content = std::fs::read_to_string(&toml_path).unwrap();
        assert!(content.contains("intent"));
        assert!(content.contains("some intent"));
        assert!(content.contains("test_model"));
    }

    #[test]
    fn save_intent_merges_into_valid_toml() {
        let dir = tempfile::tempdir().unwrap();
        let sql_path = dir.path().join("model.sql");
        let toml_path = dir.path().join("model.toml");

        std::fs::write(&toml_path, "name = \"existing_name\"\nowner = \"team\"\n").unwrap();

        let model = make_model(sql_path.to_string_lossy().to_string());
        save_intent_to_config(&model, "updated").unwrap();

        let content = std::fs::read_to_string(&toml_path).unwrap();
        // Pre-existing fields preserved.
        assert!(content.contains("existing_name"));
        assert!(content.contains("owner"));
        assert!(content.contains("team"));
        // New intent added.
        assert!(content.contains("updated"));
    }
}
