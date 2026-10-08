//! `rocky ai` — AI intent layer: generate, explain, sync, test.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::generate;
use rocky_ai::sidecar::{SidecarMaterialization, SidecarTarget, write_model_files};
use rocky_compiler::compile::{CompileResult, CompilerConfig, compile};
use rocky_compiler::types::TypedColumn;
use rocky_core::redacted::RedactedString;

use crate::output::{
    AiExplainOutput, AiExplanation, AiGenerateOutput, AiSyncOutput, AiSyncProposal,
    AiTestAssertion, AiTestModelResult, AiTestOutput, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ANTHROPIC_API_KEY_VAR: &str = "ANTHROPIC_API_KEY";

/// Pick the validation format for a model from its source file path: `.rocky`
/// files are Rocky DSL, everything else (notably `.sql`) is raw SQL.
fn proposed_source_format(file_path: &std::path::Path) -> &'static str {
    if file_path
        .extension()
        .is_some_and(|ext| ext.eq_ignore_ascii_case("rocky"))
    {
        "rocky"
    } else {
        "sql"
    }
}

/// Create an LLM client from environment + project config.
///
/// Reads `[ai] max_tokens` from `rocky.toml`. A project with NO `rocky.toml`
/// falls back to [`DEFAULT_MAX_TOKENS`], so `rocky ai` keeps working in a
/// greenfield project. A `rocky.toml` that is PRESENT and does not load
/// REFUSES (#1680) rather than silently using the default — the configured
/// ceiling is the one the operator chose, and a truncated completion is not a
/// visible failure.
///
/// This matters most for `rocky ai generate`, whose own compile is
/// `compile_project(..).ok()` — a deliberate degrade to unschema'd generation.
/// That `.ok()` swallows the compile-side config refusal, so this is the only
/// place `rocky ai generate` can see a broken config at all.
///
/// Credential-TOLERANT: `rocky ai` talks to the Anthropic API, never to the
/// warehouse, so an unset `${DATABRICKS_HOST}` must not block it (#1536).
/// Under the strict loader this site used before, that unset variable silently
/// discarded a configured `max_tokens`.
///
/// `api_key` is wrapped in [`RedactedString`] so any future `?config` /
/// `{:?}` formatting in trace output prints `***` instead of the secret.
fn make_client(config_path: &Path) -> Result<LlmClient> {
    // The config is read BEFORE the API key, deliberately. A broken
    // `rocky.toml` is a project-level fault the operator must fix either way,
    // and reading it first is what makes this refusal reachable from a unit
    // test without setting a process-wide environment variable (`set_var` is
    // `unsafe` on edition 2024 and racy across test threads). The only
    // behaviour this reorders is the message a user with NEITHER a key NOR a
    // loadable config sees: the config error rather than the key error.
    let max_tokens = rocky_core::config::load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?
        .map(|cfg| cfg.ai.max_tokens)
        .unwrap_or(DEFAULT_MAX_TOKENS);

    let api_key = std::env::var(ANTHROPIC_API_KEY_VAR)
        .context("ANTHROPIC_API_KEY not set. Set it to use `rocky ai`.")?;

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
/// Source schemas flow from the persisted cache (populated by
/// `rocky run` / `rocky discover --with-schemas`) so the AI prompt is
/// grounded in real warehouse types when the cache is warm. Cold cache
/// degrades to empty.
///
/// `cache_ttl_override` is the CLI `--cache-ttl` flag from PR 4.
fn compile_project(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    cache_ttl_override: Option<u64>,
) -> Result<CompileResult> {
    // A `rocky.toml` that is present and does not load refuses (#1625) — the
    // AI commands ground their prompts in these types, so a silently cold map
    // is a silently worse answer.
    let source_schemas = crate::source_schemas::load_project_source_schemas(
        config_path,
        state_path,
        cache_ttl_override,
    )?;

    let config = CompilerConfig {
        models_dir: PathBuf::from(models_dir),
        contracts_dir: None,
        source_schemas,
        ..Default::default()
    };
    compile(&config).map_err(|e| anyhow::anyhow!("{e}"))
}

/// Typed schemas bucketed by origin for prompt rendering.
///
/// `(existing-models, source-tables)`. Public so the in-process MCP server
/// (`rocky-mcp`) can consume the same bucketing via [`build_schema_context`].
pub type SchemaBuckets = (
    Vec<(String, Vec<TypedColumn>)>,
    Vec<(String, Vec<TypedColumn>)>,
);

/// Split `CompileResult.type_check.typed_models` into (existing-models,
/// source-tables) for the AI prompt. Source tables are distinguished by the
/// dotted schema.table naming convention used in Rocky sources; anything
/// else is treated as an existing model. This is best-effort classification
/// — the prompt tolerates either bucket without losing correctness.
///
/// Exposed for the in-process MCP server (`rocky-mcp`), whose `inspect_schema`
/// tool reuses the same (models, sources) bucketing the `rocky ai` prompt uses.
pub fn build_schema_context(result: &CompileResult) -> SchemaBuckets {
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
#[allow(clippy::too_many_arguments)]
pub async fn run_ai(
    config_path: &Path,
    state_path: &Path,
    intent: &str,
    format: Option<&str>,
    models_dir: &str,
    output_json: bool,
    cache_ttl_override: Option<u64>,
    materialization: &str,
    unique_key: Option<Vec<String>>,
    target: Option<&str>,
    overwrite: bool,
) -> Result<()> {
    let client = make_client(config_path)?;
    let fmt = format.unwrap_or("rocky");

    // Validate sidecar inputs up-front — failing here costs no LLM tokens.
    // Materialization parsing also refuses `incremental` (#1990) before we
    // make the API call.
    let parsed_materialization = SidecarMaterialization::parse(materialization, unique_key)
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let parsed_target_override = match target {
        Some(value) => Some(SidecarTarget::parse(value).map_err(|e| anyhow::anyhow!("{e}"))?),
        None => None,
    };

    // Sanity check: `--materialization merge` without `--unique-key` emits
    // a sidecar that `rocky run` rejects at load time (`StrategyConfig::Merge`
    // requires `unique_key`). Warn rather than fail so the legacy
    // "fill it in later" workflow still works.
    if let SidecarMaterialization::Merge { unique_key: None } = &parsed_materialization {
        tracing::warn!(
            "--materialization merge was passed without --unique-key; the emitted sidecar is incomplete and `rocky run` will reject it until you fill in [strategy] unique_key"
        );
    }

    // Best-effort compile of the project to ground the prompt.
    // If it fails (missing dir, parse errors, etc.) we degrade to unschema'd
    // generation rather than refusing.
    let compile_result =
        compile_project(config_path, state_path, models_dir, cache_ttl_override).ok();

    let (model_schemas, source_tables) = match &compile_result {
        Some(r) => build_schema_context(r),
        None => (Vec::new(), Vec::new()),
    };

    // Missing (not implemented): validation gets empty `source_schemas`
    // until `rocky-ai`'s generate callpath is audited. The
    // `ValidationContext`-bound `source_schemas` differs from
    // the `CompilerConfig.source_schemas` wired above. Empty maps here keep
    // AI validation's behaviour unchanged; swapping them for the cache
    // loader would change prompt grounding in ways that want a dedicated
    // review against `rocky-ai::generate::ValidationContext` semantics.
    let empty_source_schemas = std::collections::HashMap::new();
    let validation_context = compile_result
        .as_ref()
        .map(|r| generate::ValidationContext {
            project_models: &r.project.models,
            source_schemas: &empty_source_schemas,
        });

    // `--target` goes IN to generation, not on after it.
    //
    // It used to be applied only when writing the sidecar, so verification
    // typechecked the model against the default `generated.ai.<name>` and
    // then something else was written to disk. `rocky ai "<intent>" --target
    // c.s.shared` could therefore report a clean compile-verify while writing
    // a model that duplicates an existing model's physical target (#1302).
    // `generate_model` falls back to the same default when this is `None`.
    let result = generate::generate_model(
        intent,
        &model_schemas,
        &source_tables,
        fmt,
        &client,
        3,
        generate::Verification {
            context: validation_context.as_ref(),
            target: parsed_target_override.as_ref(),
            materialization: Some(&parsed_materialization),
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    // Literally the value verification used, not a second derivation of it.
    let resolved_target = result.target.clone();

    let dir = std::path::Path::new(models_dir);
    let written = write_model_files(
        dir,
        &result.name,
        &result.format,
        &result.source,
        &parsed_materialization,
        &resolved_target,
        overwrite,
    )
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    let body_path = written.body_path.display().to_string();
    let sidecar_path = written.sidecar_path.display().to_string();

    if output_json {
        let output = AiGenerateOutput {
            version: VERSION.to_string(),
            command: "ai".to_string(),
            intent: intent.to_string(),
            format: result.format.clone(),
            name: result.name.clone(),
            source: result.source.clone(),
            attempts: result.attempts,
            body_path: Some(body_path),
            sidecar_path: Some(sidecar_path),
        };
        print_json(&output)?;
    } else {
        println!("Generated model: {} ({})", result.name, result.format);
        println!("Attempts: {}", result.attempts);
        println!("Wrote: {body_path}");
        println!("Wrote: {sidecar_path}");
        println!();
        println!("{}", result.source);
    }

    Ok(())
}

/// Execute `rocky ai-sync` — detect schema changes and propose intent-guided updates.
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
                "No models with intent found. Use `rocky ai-explain --save` to add intent to models."
            );
        }
        return Ok(());
    }

    // Diff each model's upstream schemas against the baseline stored next to
    // the state store. A model with no baseline (first sync) proceeds on
    // intent alone, and its current upstream schemas become the baseline.
    let snapshot_path = ai_sync_snapshot_path(state_path);
    let mut snapshot = load_ai_sync_snapshot(&snapshot_path)?.unwrap_or_default();
    let mut diffs = Vec::with_capacity(models_with_intent.len());
    let mut first_seen = 0usize;
    for model in &models_with_intent {
        let diff = upstream_diff(&snapshot, &result, &model.config.name);
        if !diff.baseline_found {
            snapshot
                .models
                .insert(model.config.name.clone(), diff.current.clone());
            first_seen += 1;
        }
        diffs.push(diff);
    }
    if first_seen > 0 {
        save_ai_sync_snapshot(&snapshot_path, &snapshot)?;
    }

    let mut proposals = Vec::new();

    for (model, diff) in models_with_intent.iter().zip(&diffs) {
        let proposal = rocky_ai::sync::sync_model(model, &diff.changes, &client, &result)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;

        proposals.push(proposal);
    }

    if output_json {
        let typed_proposals: Vec<AiSyncProposal> = proposals
            .iter()
            .zip(&diffs)
            .map(|(p, d)| AiSyncProposal {
                model: p.model.clone(),
                intent: p.intent.clone(),
                diff: p.diff.clone(),
                proposed_source: p.proposed_source.clone(),
                upstream_baseline_found: d.baseline_found,
                upstream_changes: p
                    .upstream_changes
                    .iter()
                    .map(|c| c.details.clone())
                    .collect(),
            })
            .collect();
        let output = AiSyncOutput {
            version: VERSION.to_string(),
            command: "ai_sync".to_string(),
            proposals: typed_proposals,
        };
        print_json(&output)?;
    } else {
        if first_seen > 0 {
            println!(
                "Note: no upstream schema snapshot existed for {first_seen} model(s) (first sync). \
                 Their proposals follow declared intent only. Saved a baseline to {}.",
                snapshot_path.display()
            );
            println!();
        }
        for (proposal, diff) in proposals.iter().zip(&diffs) {
            println!(
                "Model: {} (intent: \"{}\")",
                proposal.model, proposal.intent
            );
            if diff.baseline_found {
                if diff.changes.is_empty() {
                    println!("Upstream changes: none since the baseline");
                } else {
                    println!("Upstream changes since the baseline:");
                    for change in &diff.changes {
                        println!("  - {}", change.details);
                    }
                }
            }
            println!("{}", proposal.diff);
            println!();
        }

        if apply {
            for proposal in &proposals {
                if let Some(model) = result.project.model(&proposal.model) {
                    // Validate the LLM-proposed source through the same
                    // parse + typecheck path used to compile-verify generated
                    // models BEFORE writing it. An unvalidated proposal that
                    // does not parse or typecheck must never land on disk.
                    let format = proposed_source_format(&model.file_path);
                    // `None` everywhere: this validates the proposed SQL in
                    // isolation, under a synthetic name, target and
                    // `full_refresh` strategy — NOT under the existing
                    // model's own config. That is weaker than it looks: a
                    // proposal that drops the partition column of a
                    // `time_interval` model passes here and fails the next
                    // real compile. Pre-existing, and out of scope for #1302,
                    // which is about `rocky ai`; tracked separately.
                    if let Err(diagnostics) = rocky_ai::generate::validate_proposed_source(
                        &proposal.proposed_source,
                        format,
                        None,
                        None,
                        None,
                    ) {
                        anyhow::bail!(
                            "refusing to apply proposal for model '{}': the proposed source \
                             does not compile.\n{}",
                            proposal.model,
                            diagnostics
                        );
                    }
                    std::fs::write(&model.file_path, &proposal.proposed_source)?;
                    println!("Updated: {}", model.file_path.display());
                    // The model is now synced against today's upstreams:
                    // advance its baseline. Saved per model so a later
                    // refusal in this loop keeps the ones already written.
                    if let Some(diff) = diffs.iter().find(|d| d.model == proposal.model) {
                        snapshot
                            .models
                            .insert(proposal.model.clone(), diff.current.clone());
                        save_ai_sync_snapshot(&snapshot_path, &snapshot)?;
                    }
                }
            }
        } else if !proposals.is_empty() {
            println!(
                "Run with --apply to update models. A model's upstream baseline advances \
                 only when --apply writes its proposal."
            );
        }
    }

    Ok(())
}

/// Stored upstream schemas `rocky ai-sync` diffs against, per synced model.
///
/// Lives in a JSON file next to the state store (see
/// [`ai_sync_snapshot_path`]): `model -> upstream name -> typed columns`, as
/// of the model's first sync or its last applied proposal. Keyed per model,
/// not globally, so applying one model's proposal cannot hide an upstream
/// change from another model that has not synced yet.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct AiSyncSnapshot {
    version: u32,
    models:
        std::collections::BTreeMap<String, std::collections::BTreeMap<String, Vec<TypedColumn>>>,
}

const AI_SYNC_SNAPSHOT_VERSION: u32 = 1;

/// `<state file>.ai-sync.json`, e.g. `models/.rocky-state.ai-sync.json`.
fn ai_sync_snapshot_path(state_path: &Path) -> PathBuf {
    state_path.with_extension("ai-sync.json")
}

/// Read the snapshot. `Ok(None)` only when the file does not exist; an
/// unreadable or malformed file is an error, never an empty baseline.
fn load_ai_sync_snapshot(path: &Path) -> Result<Option<AiSyncSnapshot>> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => {
            return Err(e).with_context(|| {
                format!("failed to read ai-sync schema snapshot {}", path.display())
            });
        }
    };
    let snapshot: AiSyncSnapshot = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "ai-sync schema snapshot {} is malformed; delete it to re-baseline",
            path.display()
        )
    })?;
    if snapshot.version != AI_SYNC_SNAPSHOT_VERSION {
        anyhow::bail!(
            "ai-sync schema snapshot {} has version {}, expected {}; delete it to re-baseline",
            path.display(),
            snapshot.version,
            AI_SYNC_SNAPSHOT_VERSION
        );
    }
    Ok(Some(snapshot))
}

/// Write the snapshot atomically (temp file + rename).
fn save_ai_sync_snapshot(path: &Path, snapshot: &AiSyncSnapshot) -> Result<()> {
    let snapshot = AiSyncSnapshot {
        version: AI_SYNC_SNAPSHOT_VERSION,
        models: snapshot.models.clone(),
    };
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(&snapshot)?)
        .with_context(|| format!("failed to write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("failed to write ai-sync schema snapshot {}", path.display()))?;
    Ok(())
}

/// One model's upstream diff for `rocky ai-sync`.
#[derive(Debug)]
struct UpstreamDiff {
    model: String,
    /// A baseline existed for this model.
    baseline_found: bool,
    /// Column changes on upstreams present in both the baseline and the
    /// current compile. Empty without a baseline.
    changes: Vec<rocky_ai::sync::SchemaChange>,
    /// The model's current upstream schemas (the next baseline).
    current: std::collections::BTreeMap<String, Vec<TypedColumn>>,
}

/// The typed schemas of `model`'s direct upstreams in this compile. An
/// upstream with no known schema (e.g. a cold source-schema cache) is left
/// out rather than recorded as empty.
fn current_upstream_schemas(
    result: &CompileResult,
    model: &str,
) -> std::collections::BTreeMap<String, Vec<TypedColumn>> {
    let Some(schema) = result.semantic_graph.model_schema(model) else {
        return std::collections::BTreeMap::new();
    };
    schema
        .upstream
        .iter()
        .filter_map(|up| {
            result
                .type_check
                .typed_models
                .get(up)
                .map(|cols| (up.clone(), cols.clone()))
        })
        .collect()
}

/// Diff `model`'s current upstream schemas against its stored baseline.
///
/// Only upstreams known on both sides are compared, so a newly added
/// dependency or a schema that is merely unknown this run is not reported
/// as an upstream change.
fn upstream_diff(snapshot: &AiSyncSnapshot, result: &CompileResult, model: &str) -> UpstreamDiff {
    let current = current_upstream_schemas(result, model);
    let Some(baseline) = snapshot.models.get(model) else {
        return UpstreamDiff {
            model: model.to_string(),
            baseline_found: false,
            changes: Vec::new(),
            current,
        };
    };
    let previous: indexmap::IndexMap<String, Vec<TypedColumn>> = baseline
        .iter()
        .filter(|(name, _)| current.contains_key(*name))
        .map(|(name, cols)| (name.clone(), cols.clone()))
        .collect();
    let now: indexmap::IndexMap<String, Vec<TypedColumn>> = current
        .iter()
        .filter(|(name, _)| baseline.contains_key(*name))
        .map(|(name, cols)| (name.clone(), cols.clone()))
        .collect();
    UpstreamDiff {
        model: model.to_string(),
        baseline_found: true,
        changes: rocky_ai::sync::detect_schema_changes_between(&previous, &now),
        current,
    }
}

/// Execute `rocky ai-explain` — generate intent descriptions from code.
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

/// Execute `rocky ai-test` — generate test assertions from intent.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proposed_source_format_detects_rocky_and_sql() {
        assert_eq!(
            proposed_source_format(std::path::Path::new("models/orders.rocky")),
            "rocky"
        );
        assert_eq!(
            proposed_source_format(std::path::Path::new("models/orders.ROCKY")),
            "rocky"
        );
        assert_eq!(
            proposed_source_format(std::path::Path::new("models/orders.sql")),
            "sql"
        );
        assert_eq!(
            proposed_source_format(std::path::Path::new("models/orders")),
            "sql"
        );
    }

    /// `ai-sync --apply` must validate a proposal before writing. An
    /// unparseable proposal is rejected and the on-disk file is left untouched.
    #[test]
    fn unvalidated_proposal_is_not_written_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let model_path = dir.path().join("orders.sql");
        let original = "SELECT id FROM orders";
        std::fs::write(&model_path, original).unwrap();

        let bad_proposal = "this is not sql at all ;;;";
        let format = proposed_source_format(&model_path);

        // Mirror the apply gate: validate first, only write on success.
        let validation =
            rocky_ai::generate::validate_proposed_source(bad_proposal, format, None, None, None);
        assert!(validation.is_err(), "bad proposal must fail validation");

        // The gate bails before writing — confirm the file is unchanged.
        let on_disk = std::fs::read_to_string(&model_path).unwrap();
        assert_eq!(
            on_disk, original,
            "an invalid proposal must not overwrite the model file"
        );
    }

    // ------------------------------------------------------------------
    // ai-sync upstream schema snapshot (defect 2). No API key needed: these
    // cover everything `run_ai_sync` does around the LLM call.
    // ------------------------------------------------------------------

    /// A two-model project: `stg` (SQL over literals) feeds `report`, which
    /// declares intent. `stg_sql` controls the upstream's columns.
    fn ai_sync_project(dir: &Path, stg_sql: &str) -> PathBuf {
        let models = dir.join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("stg.sql"), stg_sql).unwrap();
        std::fs::write(
            models.join("stg.toml"),
            "name = \"stg\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"stg\"\n",
        )
        .unwrap();
        std::fs::write(models.join("report.sql"), "SELECT id FROM stg").unwrap();
        std::fs::write(
            models.join("report.toml"),
            "name = \"report\"\nintent = \"one row per id\"\ndepends_on = [\"stg\"]\n\n\
             [strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"report\"\n",
        )
        .unwrap();
        models
    }

    fn ai_sync_compile(dir: &Path) -> CompileResult {
        compile_project(
            &dir.join("rocky.toml"),
            &dir.join("state.redb"),
            dir.join("models").to_str().unwrap(),
            None,
        )
        .unwrap()
    }

    #[test]
    fn ai_sync_first_run_has_no_baseline_and_records_one() {
        let tmp = tempfile::tempdir().unwrap();
        ai_sync_project(tmp.path(), "SELECT 1 AS id, 'a' AS name");
        let result = ai_sync_compile(tmp.path());
        let path = ai_sync_snapshot_path(&tmp.path().join("state.redb"));
        assert_eq!(path, tmp.path().join("state.ai-sync.json"));

        assert!(load_ai_sync_snapshot(&path).unwrap().is_none());
        let diff = upstream_diff(&AiSyncSnapshot::default(), &result, "report");
        assert!(!diff.baseline_found, "first run: no baseline");
        assert!(diff.changes.is_empty(), "first run proceeds on intent only");
        assert!(
            diff.current.contains_key("stg"),
            "the baseline must capture the upstream: {:?}",
            diff.current
        );

        let mut snapshot = AiSyncSnapshot::default();
        snapshot.models.insert("report".into(), diff.current);
        save_ai_sync_snapshot(&path, &snapshot).unwrap();
        let reloaded = load_ai_sync_snapshot(&path).unwrap().expect("saved");
        assert_eq!(reloaded.version, AI_SYNC_SNAPSHOT_VERSION);
        assert_eq!(reloaded.models["report"], snapshot.models["report"]);
    }

    #[test]
    fn ai_sync_diffs_upstream_columns_against_the_stored_baseline() {
        let tmp = tempfile::tempdir().unwrap();
        let models = ai_sync_project(tmp.path(), "SELECT 1 AS id, 'a' AS name");
        let before = ai_sync_compile(tmp.path());
        let mut snapshot = AiSyncSnapshot::default();
        snapshot
            .models
            .insert("report".into(), current_upstream_schemas(&before, "report"));

        // The upstream gains `amount` (kept apart from a removal so the
        // rename heuristic cannot pair them).
        std::fs::write(
            models.join("stg.sql"),
            "SELECT 1 AS id, 'a' AS name, 2.5 AS amount",
        )
        .unwrap();
        let after = ai_sync_compile(tmp.path());
        let diff = upstream_diff(&snapshot, &after, "report");
        assert!(diff.baseline_found);
        let details: Vec<&str> = diff.changes.iter().map(|c| c.details.as_str()).collect();
        assert!(
            diff.changes.iter().any(|c| matches!(
                &c.change_type,
                rocky_ai::sync::SchemaChangeType::ColumnAdded { name, .. } if name == "amount"
            )),
            "added column must be reported: {details:?}"
        );
        assert!(diff.changes.iter().all(|c| c.model == "stg"), "{details:?}");

        // The upstream drops `name`.
        std::fs::write(models.join("stg.sql"), "SELECT 1 AS id").unwrap();
        let dropped = ai_sync_compile(tmp.path());
        let removed = upstream_diff(&snapshot, &dropped, "report");
        let removed_details: Vec<&str> =
            removed.changes.iter().map(|c| c.details.as_str()).collect();
        assert!(
            removed.changes.iter().any(|c| matches!(
                &c.change_type,
                rocky_ai::sync::SchemaChangeType::ColumnRemoved { name } if name == "name"
            )),
            "removed column must be reported: {removed_details:?}"
        );

        // The changes reach the LLM prompt that `sync_model` sends.
        let report = after.project.model("report").unwrap();
        let (system, _user) = rocky_ai::sync::sync_prompts(report, &diff.changes, &after);
        for change in &diff.changes {
            assert!(system.contains(&change.details), "prompt: {system}");
        }

        // No upstream change since the baseline: nothing reported.
        let same = upstream_diff(&snapshot, &before, "report");
        assert!(
            same.baseline_found && same.changes.is_empty(),
            "{:?}",
            same.changes
        );
    }

    #[test]
    fn ai_sync_malformed_snapshot_is_an_error_not_an_empty_baseline() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.ai-sync.json");
        std::fs::write(&path, "{ not json").unwrap();
        let err = load_ai_sync_snapshot(&path).unwrap_err();
        assert!(format!("{err:#}").contains("malformed"), "{err:#}");

        std::fs::write(&path, r#"{"version": 99, "models": {}}"#).unwrap();
        let err = load_ai_sync_snapshot(&path).unwrap_err();
        assert!(format!("{err:#}").contains("version 99"), "{err:#}");
    }

    // ------------------------------------------------------------------
    // #1680 — the two config reads `rocky ai` makes.
    //
    // `compile_project` is #1667's converted caller (its shared-loader test
    // stays green if `.ok()` comes back here). `make_client` is the `[ai]`
    // secondary read, and it matters most for `rocky ai generate`, whose own
    // compile is `compile_project(..).ok()` — so `make_client` is the ONLY
    // place that subcommand can see a broken config at all.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1680: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// Loads under the credential-TOLERANT loader (#1536), refuses under the
    /// strict one: `${...}` is unset and sits in an adapter connection field.
    const UNSET_CREDENTIAL_CONFIG_1680: &str = "[adapters.wh]\ntype = \"databricks\"\n\
         host = \"${ROCKY_T_1680_UNSET}\"\n";

    /// The converted caller: a present-but-unloadable `rocky.toml` refuses the
    /// grounding compile, naming the file.
    #[test]
    fn ai_compile_project_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1680).unwrap();

        // `let ... else` rather than `expect_err`: `CompileResult` is not `Debug`.
        let Err(err) = compile_project(
            &cfg,
            &tmp.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        ) else {
            panic!("a present but unloadable rocky.toml must refuse the ai compile");
        };
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );
    }

    /// Honest-failure guard for the converted caller: no `rocky.toml`, no
    /// refusal.
    #[test]
    fn ai_compile_project_still_runs_without_any_config() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("m.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("m.toml"),
            "name = \"m\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = tmp.path().join("rocky.toml");
        assert!(!cfg.exists());

        compile_project(
            &cfg,
            &tmp.path().join("state.redb"),
            models_dir.to_str().unwrap(),
            None,
        )
        .expect("a missing rocky.toml must not refuse the ai compile");
    }

    /// The `[ai]` secondary read: a present-but-unloadable `rocky.toml` refuses
    /// the client build instead of silently falling back to
    /// `DEFAULT_MAX_TOKENS`. Asserted without touching `ANTHROPIC_API_KEY`,
    /// which `make_client` reads only after the config.
    #[test]
    fn ai_make_client_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1680).unwrap();

        let Err(err) = make_client(&cfg) else {
            panic!("a present but unloadable rocky.toml must refuse the ai client");
        };
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );
        assert!(
            !rendered.contains("ANTHROPIC_API_KEY"),
            "the config fault must be reported as itself, not as a missing key: {rendered}"
        );
    }

    /// Honest-failure, case (b): a VALID config whose adapter connection field
    /// holds an unset `${VAR}` must not refuse. Under the strict loader this
    /// site used before, that config silently discarded a configured
    /// `[ai] max_tokens`; the tolerant loader honours it.
    #[test]
    fn ai_make_client_tolerates_an_unset_credential_var() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = tmp.path().join("rocky.toml");
        std::fs::write(&cfg, UNSET_CREDENTIAL_CONFIG_1680).unwrap();

        // The loader swap is what makes the difference — pin both directions.
        assert!(
            rocky_core::config::load_rocky_config(&cfg).is_err(),
            "fixture must fail the STRICT loader, or this proves nothing"
        );
        assert!(
            rocky_core::config::load_optional_project_config(Some(&cfg))
                .expect("the tolerant loader must accept an unset adapter credential")
                .is_some()
        );

        // `make_client` gets past the config and stops only at the API key.
        match make_client(&cfg) {
            Ok(_) => {}
            Err(e) => {
                let rendered = format!("{e:#}");
                assert!(
                    rendered.contains("ANTHROPIC_API_KEY"),
                    "an unset credential var must not refuse the ai client: {rendered}"
                );
            }
        }
    }
}
