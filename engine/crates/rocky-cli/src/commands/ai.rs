//! `rocky ai` — AI intent layer: generate, explain, sync, test.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::generate;
use rocky_compiler::compile::{CompileResult, CompilerConfig, compile};
use rocky_compiler::types::TypedColumn;
use rocky_core::redacted::RedactedString;

use crate::output::{
    AiExplainOutput, AiExplanation, AiGenerateOutput, AiSyncOutput, AiSyncProposal,
    AiTestAssertion, AiTestModelResult, AiTestOutput, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ANTHROPIC_API_KEY_VAR: &str = "ANTHROPIC_API_KEY";

/// Create an LLM client from environment + project config.
///
/// Reads `[ai] max_tokens` from `rocky.toml` when the config loads cleanly;
/// falls back to [`DEFAULT_MAX_TOKENS`] if the config is missing or
/// malformed (so `rocky ai` keeps working in greenfield projects without
/// any `rocky.toml`).
///
/// `api_key` is wrapped in [`RedactedString`] so any future `?config` /
/// `{:?}` formatting in trace output prints `***` instead of the secret.
fn make_client(config_path: &Path) -> Result<LlmClient> {
    let api_key = std::env::var(ANTHROPIC_API_KEY_VAR)
        .context("ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.")?;

    let max_tokens = rocky_core::config::load_rocky_config(config_path)
        .map(|cfg| cfg.ai.max_tokens)
        .unwrap_or(DEFAULT_MAX_TOKENS);

    let config = AiConfig {
        provider: "anthropic".to_string(),
        model: "claude-sonnet-4-6".to_string(),
        api_key: RedactedString::new(api_key),
        default_format: "rocky".to_string(),
        max_attempts: 3,
        max_tokens,
    };

    LlmClient::new(config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Compile the project from a models directory.
///
/// Wave-2 of Arc 7 wave 2: source schemas flow from the persisted cache
/// (populated by `rocky run` / `rocky discover --with-schemas` in later
/// PRs) so the AI prompt is grounded in real warehouse types when the
/// cache is warm. Cold cache degrades to empty, which matches the
/// pre-wave-2 behaviour this function had.
///
/// `cache_ttl_override` is the CLI `--cache-ttl` flag from PR 4.
fn compile_project(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    cache_ttl_override: Option<u64>,
) -> Result<CompileResult> {
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };

    let config = CompilerConfig {
        models_dir: PathBuf::from(models_dir),
        contracts_dir: None,
        source_schemas,
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };
    compile(&config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Typed schemas bucketed by origin for prompt rendering.
type SchemaBuckets = (
    Vec<(String, Vec<TypedColumn>)>,
    Vec<(String, Vec<TypedColumn>)>,
);

/// Split `CompileResult.type_check.typed_models` into (existing-models,
/// source-tables) for the AI prompt. Source tables are distinguished by the
/// dotted schema.table naming convention used in Rocky sources; anything
/// else is treated as an existing model. This is best-effort classification
/// — the prompt tolerates either bucket without losing correctness.
fn build_schema_context(result: &CompileResult) -> SchemaBuckets {
    let mut model_schemas = Vec::new();
    let mut source_tables = Vec::new();

    for (name, cols) in &result.type_check.typed_models {
        if name.contains('.') {
            source_tables.push((name.clone(), cols.clone()));
        } else {
            model_schemas.push((name.clone(), cols.clone()));
        }
    }

    (model_schemas, source_tables)
}

/// Execute `rocky ai "intent"` — generate a model from natural language.
///
/// Grounds the LLM prompt in the project's typed schemas (existing models
/// and, once warehouse discovery is plumbed through, source tables) so
/// generated code references real columns with correct types. The generated
/// model is then typechecked inside the full project graph rather than in
/// isolation — upstream typed schemas propagate into its output type, which
/// matters for downstream models and contract validation. Rocky's
/// typechecker is lenient on unresolved columns today, so schema grounding
/// in the prompt is the primary mechanism preventing hallucinated columns.
pub async fn run_ai(
    config_path: &Path,
    state_path: &Path,
    intent: &str,
    format: Option<&str>,
    models_dir: &str,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let client = make_client(config_path)?;
    let fmt = format.unwrap_or("rocky");

    // Best-effort compile of the project to ground the prompt.
    // If it fails (missing dir, parse errors, etc.) we degrade to unschema'd
    // generation rather than refusing.
    let compile_result =
        compile_project(config_path, state_path, models_dir, cache_ttl_override).ok();

    let (model_schemas, source_tables) = match &compile_result {
        Some(r) => build_schema_context(r),
        None => (Vec::new(), Vec::new()),
    };

    // TODO(arc7-wave2): promote stub once `rocky-ai`'s generate callpath is
    // audited — the `ValidationContext`-bound `source_schemas` differs from
    // the `CompilerConfig.source_schemas` wired above. Empty maps here keep
    // AI validation's behaviour unchanged; swapping them for the cache
    // loader would change prompt grounding in ways that want a dedicated
    // review against `rocky-ai::generate::ValidationContext` semantics.
    let empty_source_schemas = std::collections::HashMap::new();
    let empty_source_column_info = std::collections::HashMap::new();
    let validation_context = compile_result
        .as_ref()
        .map(|r| generate::ValidationContext {
            project_models: &r.project.models,
            source_schemas: &empty_source_schemas,
            source_column_info: &empty_source_column_info,
        });

    let result = generate::generate_model(
        intent,
        &model_schemas,
        &source_tables,
        fmt,
        &client,
        3,
        validation_context.as_ref(),
    )
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
#[allow(clippy::too_many_arguments)]
pub async fn run_ai_sync(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    apply: bool,
    model_filter: Option<&str>,
    with_intent: bool,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let client = make_client(config_path)?;
    let result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;

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
#[allow(clippy::too_many_arguments)]
pub async fn run_ai_explain(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: Option<&str>,
    all: bool,
    save: bool,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let client = make_client(config_path)?;
    let result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;

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
#[allow(clippy::too_many_arguments)]
pub async fn run_ai_test(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: Option<&str>,
    all: bool,
    save: bool,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let client = make_client(config_path)?;
    let result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;

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
