//! `rocky ai` — AI intent layer: generate, explain, sync, test.

use std::path::PathBuf;

use anyhow::{Context, Result};

use rocky_ai::client::{AiConfig, LlmClient};
use rocky_ai::generate;
use rocky_compiler::compile::{CompilerConfig, compile};

use crate::output::{
    AiExplainOutput, AiExplanation, AiGenerateOutput, AiSyncOutput, AiSyncProposal,
    AiTestAssertion, AiTestModelResult, AiTestOutput, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ANTHROPIC_API_KEY_VAR: &str = "ANTHROPIC_API_KEY";

/// Create an LLM client from environment.
fn make_client() -> Result<LlmClient> {
    let api_key = std::env::var(ANTHROPIC_API_KEY_VAR)
        .context("ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.")?;

    let config = AiConfig {
        provider: "anthropic".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        api_key,
        default_format: "rocky".to_string(),
        max_attempts: 3,
    };

    LlmClient::new(config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Compile the project from a models directory.
fn compile_project(models_dir: &str) -> Result<rocky_compiler::compile::CompileResult> {
    let config = CompilerConfig {
        models_dir: PathBuf::from(models_dir),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
    };
    compile(&config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Execute `rocky ai "intent"` — generate a model from natural language.
pub async fn run_ai(intent: &str, format: Option<&str>, output_json: bool) -> Result<()> {
    let client = make_client()?;
    let fmt = format.unwrap_or("rocky");

    let model_names = Vec::new();
    let source_tables = Vec::new();

    let result = generate::generate_model(intent, &model_names, &source_tables, fmt, &client, 3)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if output_json {
        let output = AiGenerateOutput {
            version: VERSION.to_string(),
            command: "ai".to_string(),
            intent: intent.to_string(),
            format: result.format.clone(),
            name: result.name.clone(),
            source: result.source.clone(),
            attempts: result.attempts,
        };
        print_json(&output)?;
    } else {
        println!("Generated model: {} ({})", result.name, result.format);
        println!("Attempts: {}", result.attempts);
        println!();
        println!("{}", result.source);
    }

    Ok(())
}

/// Execute `rocky ai sync` — detect schema changes and propose intent-guided updates.
pub async fn run_ai_sync(
    models_dir: &str,
    apply: bool,
    model_filter: Option<&str>,
    with_intent: bool,
    output_json: bool,
) -> Result<()> {
    let client = make_client()?;
    let result = compile_project(models_dir)?;

    // For now, use the current compilation as both "previous" and "current".
    // In practice, the previous would come from the state store.
    // Detect changes against models that have intent and upstream changes.
    let models_with_intent: Vec<&rocky_core::models::Model> = result
        .project
        .models
        .iter()
        .filter(|m| {
            if with_intent && m.config.intent.is_none() {
                return false;
            }
            if let Some(filter) = model_filter {
                return m.config.name == filter;
            }
            m.config.intent.is_some()
        })
        .collect();

    if models_with_intent.is_empty() {
        if output_json {
            let output = AiSyncOutput {
                version: VERSION.to_string(),
                command: "ai_sync".to_string(),
                proposals: vec![],
            };
            print_json(&output)?;
        } else {
            println!(
                "No models with intent found. Use `rocky ai explain --save` to add intent to models."
            );
        }
        return Ok(());
    }

    let mut proposals = Vec::new();

    for model in &models_with_intent {
        // Get upstream changes for this model
        let upstream_changes = Vec::new(); // TODO: compare against previous compilation from state store

        let proposal = rocky_ai::sync::sync_model(model, &upstream_changes, &client, &result)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        proposals.push(proposal);
    }

    if output_json {
        let typed_proposals: Vec<AiSyncProposal> = proposals
            .iter()
            .map(|p| AiSyncProposal {
                model: p.model.clone(),
                intent: p.intent.clone(),
                diff: p.diff.clone(),
                proposed_source: p.proposed_source.clone(),
            })
            .collect();
        let output = AiSyncOutput {
            version: VERSION.to_string(),
            command: "ai_sync".to_string(),
            proposals: typed_proposals,
        };
        print_json(&output)?;
    } else {
        for proposal in &proposals {
            println!(
                "Model: {} (intent: \"{}\")",
                proposal.model, proposal.intent
            );
            println!("{}", proposal.diff);
            println!();
        }

        if apply {
            for proposal in &proposals {
                if let Some(model) = result.project.model(&proposal.model) {
                    std::fs::write(&model.file_path, &proposal.proposed_source)?;
                    println!("Updated: {}", model.file_path);
                }
            }
        } else if !proposals.is_empty() {
            println!("Run with --apply to update models.");
        }
    }

    Ok(())
}

/// Execute `rocky ai explain` — generate intent descriptions from code.
pub async fn run_ai_explain(
    models_dir: &str,
    model_name: Option<&str>,
    all: bool,
    save: bool,
    output_json: bool,
) -> Result<()> {
    let client = make_client()?;
    let result = compile_project(models_dir)?;

    let models_to_explain: Vec<&rocky_core::models::Model> = result
        .project
        .models
        .iter()
        .filter(|m| {
            if let Some(name) = model_name {
                return m.config.name == name;
            }
            if all {
                return m.config.intent.is_none();
            }
            false
        })
        .collect();

    if models_to_explain.is_empty() && model_name.is_none() {
        println!("No models to explain. Specify a model name or use --all.");
        return Ok(());
    }

    let mut explanations: Vec<AiExplanation> = Vec::new();

    for model in &models_to_explain {
        let intent = rocky_ai::explain::explain_model(model, &result, &client)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        if save {
            rocky_ai::explain::save_intent_to_config(model, &intent)?;
            if !output_json {
                println!("Saved intent for {}: {}", model.config.name, intent);
            }
        } else if !output_json {
            println!("{}: {}", model.config.name, intent);
        }

        explanations.push(AiExplanation {
            model: model.config.name.clone(),
            intent,
            saved: save,
        });
    }

    if output_json {
        let output = AiExplainOutput {
            version: VERSION.to_string(),
            command: "ai_explain".to_string(),
            explanations,
        };
        print_json(&output)?;
    }

    Ok(())
}

/// Execute `rocky ai test` — generate test assertions from intent.
pub async fn run_ai_test(
    models_dir: &str,
    model_name: Option<&str>,
    all: bool,
    save: bool,
    output_json: bool,
) -> Result<()> {
    let client = make_client()?;
    let result = compile_project(models_dir)?;

    let models_to_test: Vec<&rocky_core::models::Model> = result
        .project
        .models
        .iter()
        .filter(|m| {
            if let Some(name) = model_name {
                return m.config.name == name;
            }
            if all {
                return true; // Test all models (with or without intent)
            }
            false
        })
        .collect();

    if models_to_test.is_empty() && model_name.is_none() {
        println!("Specify a model name or use --all.");
        return Ok(());
    }

    let mut all_results: Vec<AiTestModelResult> = Vec::new();

    for model in &models_to_test {
        let assertions = rocky_ai::testgen::generate_tests(model, &result, &client)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        if save {
            let tests_dir = PathBuf::from(models_dir)
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .join("tests");
            rocky_ai::testgen::save_tests(&model.config.name, &assertions, &tests_dir)?;
            if !output_json {
                println!("Saved {} tests for {}", assertions.len(), model.config.name);
            }
        } else if !output_json {
            println!("Tests for {}:", model.config.name);
            for a in &assertions {
                println!("  - {}: {}", a.name, a.description);
            }
        }

        let typed_tests: Vec<AiTestAssertion> = assertions
            .iter()
            .map(|a| AiTestAssertion {
                name: a.name.clone(),
                description: a.description.clone(),
                sql: Some(a.sql.clone()),
            })
            .collect();
        all_results.push(AiTestModelResult {
            model: model.config.name.clone(),
            tests: typed_tests,
            saved: save,
        });
    }

    if output_json {
        let output = AiTestOutput {
            version: VERSION.to_string(),
            command: "ai_test".to_string(),
            results: all_results,
        };
        print_json(&output)?;
    }

    Ok(())
}
