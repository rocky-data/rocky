//! Intent → model generation with schema-grounded prompting and project-aware
//! compile-verify loop.

use std::collections::{HashMap, HashSet};

use rocky_compiler::types::TypedColumn;
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
    /// The target this model was **verified against**, and therefore the one
    /// the caller must write. Returned rather than left for the caller to
    /// re-derive: the default depends on `name`, which is decided in here, and
    /// two independent derivations that must agree is exactly how #1302's
    /// verified-vs-written split happened in the first place.
    pub target: crate::sidecar::SidecarTarget,
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
}

/// What a generated model is verified against.
///
/// The two together decide what verification actually sees, which is the
/// distinction #1302 was about — a proposal checked in the wrong graph, or
/// against a target other than the one it will be written with, is verified
/// in name only.
#[derive(Default, Clone, Copy)]
pub struct Verification<'a> {
    /// Existing project models, so the proposal typechecks inside the real
    /// graph with upstream column types visible. `None` validates it in
    /// isolation.
    pub context: Option<&'a ValidationContext<'a>>,
    /// The target the model will be written with. `None` takes the default
    /// (`generated.ai.<name>`), the same one the sidecar writer derives.
    pub target: Option<&'a crate::sidecar::SidecarTarget>,
    /// The materialization the model will be written with. `None` is
    /// `full_refresh`, the sidecar writer's own default.
    ///
    /// Not cosmetic: the compiler excludes `ephemeral` models from
    /// duplicate-target detection because they materialize nothing, so
    /// verifying an ephemeral model as `full_refresh` rejects a collision
    /// that will not exist on disk.
    pub materialization: Option<&'a crate::sidecar::SidecarMaterialization>,
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
///
/// # Token budget
///
/// The retry loop tracks cumulative `output_tokens` returned by the LLM
/// across attempts. When the running total exceeds the client's
/// `max_tokens` (sourced from `[ai] max_tokens` in `rocky.toml`, default
/// [`crate::client::DEFAULT_MAX_TOKENS`]), the loop fail-stops with
/// [`AiError::TokenBudgetExceeded`] instead of issuing another retry.
/// This bounds the worst-case spend when the LLM produces a runaway
/// response that fails validation.
pub async fn generate_model(
    intent: &str,
    model_schemas: &[(String, Vec<TypedColumn>)],
    source_tables: &[(String, Vec<TypedColumn>)],
    format: &str,
    client: &LlmClient,
    max_attempts: usize,
    verify: Verification<'_>,
) -> Result<GeneratedModel, AiError> {
    // Once, not per attempt: the baseline depends only on the existing project,
    // which does not change across retries.
    let baseline = verify
        .context
        .map(baseline_error_keys)
        .transpose()
        .map_err(|e| {
            // The existing project itself will not load or compile. That is
            // not something generation can fix, and every attempt would hit
            // it identically, so fail before spending an LLM call.
            tracing::warn!(error = %e, "cannot baseline the project for AI verification");
            AiError::CompileFailed { attempts: 0 }
        })?;

    let system = prompt::build_system_prompt(model_schemas, source_tables, format);
    let mut error_context: Option<String> = None;
    let token_budget: u64 = u64::from(client.max_tokens());
    let mut consumed_output_tokens: u64 = 0;

    for attempt in 1..=max_attempts {
        let response = client
            .generate(&system, intent, error_context.as_deref())
            .await?;

        // Track cumulative output tokens. The Anthropic API caps each
        // individual response at `max_tokens`; this guard prevents a stuck
        // retry loop from billing N × max_tokens before fail-stopping.
        consumed_output_tokens =
            consumed_output_tokens.saturating_add(response.output_tokens.unwrap_or(0));

        let source = extract_code(&response.content);
        let validation = validate_generated_code(
            &source,
            format,
            verify.context,
            verify.target,
            verify.materialization,
            baseline.as_ref(),
        );

        match validation {
            Ok((name, resolved_target)) => {
                return Ok(GeneratedModel {
                    source,
                    format: format.to_string(),
                    name,
                    target: resolved_target,
                    attempts: attempt,
                    llm_response: response,
                });
            }
            Err(errors) => {
                if consumed_output_tokens > token_budget {
                    tracing::warn!(
                        attempt,
                        consumed_output_tokens,
                        budget = token_budget,
                        "AI token budget exceeded; aborting compile-verify retry loop"
                    );
                    return Err(AiError::TokenBudgetExceeded {
                        attempts: attempt,
                        consumed_output_tokens,
                        budget: client.max_tokens(),
                    });
                }
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

/// Rendered error diagnostics for the project *without* the generated model.
///
/// Verification asks "does what I just generated break anything?", and that is
/// a question about a **difference**, not about ownership. Two earlier attempts
/// at it were wrong in opposite directions: reporting every error in the merged
/// project failed generation on pre-existing upstream problems no retry could
/// fix, and filtering to diagnostics whose `model` is the generated one missed
/// the ones it causes elsewhere — a downstream join-key E001 or contract E011
/// names the *consumer*, not the new model that changed its upstream types.
///
/// Comparing against this baseline gets both right, and needs the same
/// `CompilerConfig` so a diagnostic cannot appear "new" merely because the two
/// compiles saw different inputs.
fn baseline_error_keys(ctx: &ValidationContext<'_>) -> Result<HashSet<String>, String> {
    let project = rocky_compiler::project::Project::from_models(ctx.project_models.to_vec())
        .map_err(|e| format!("project loading failed: {e}"))?;
    let config = rocky_compiler::compile::CompilerConfig {
        source_schemas: ctx.source_schemas.clone(),
        ..Default::default()
    };
    let compiled = rocky_compiler::compile::compile_project(project, &config, Vec::new())
        .map_err(|e| format!("compile failed: {e}"))?;
    Ok(compiled
        .diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(std::string::ToString::to_string)
        .collect())
}

/// Validate an LLM-proposed model source through the same parse + typecheck
/// path used to compile-verify freshly generated models.
///
/// `format` is `"rocky"` or `"sql"`. When `context` is `Some`, the proposal is
/// typechecked inside the full project graph (upstream schemas visible);
/// otherwise it is validated in isolation. Returns the merged error
/// diagnostics on failure.
///
/// This is the public entry point callers use to gate writing an unvalidated
/// proposal to disk (e.g. `rocky ai-sync --apply`).
pub fn validate_proposed_source(
    source: &str,
    format: &str,
    context: Option<&ValidationContext<'_>>,
    target: Option<&crate::sidecar::SidecarTarget>,
    materialization: Option<&crate::sidecar::SidecarMaterialization>,
) -> Result<(), String> {
    // No baseline: with no project context there are no other models, so every
    // error is necessarily the proposal's own.
    let baseline = context.map(baseline_error_keys).transpose()?;
    validate_generated_code(
        source,
        format,
        context,
        target,
        materialization,
        baseline.as_ref(),
    )
    .map(|_| ())
}

/// Validate generated code. When `context` is `Some`, typechecks against the
/// full project (upstream typed schemas visible); otherwise isolates the
/// generated model (legacy behavior, used in unit tests).
fn validate_generated_code(
    source: &str,
    format: &str,
    context: Option<&ValidationContext<'_>>,
    target: Option<&crate::sidecar::SidecarTarget>,
    materialization: Option<&crate::sidecar::SidecarMaterialization>,
    baseline: Option<&HashSet<String>>,
) -> Result<(String, crate::sidecar::SidecarTarget), String> {
    if source.trim().is_empty() {
        return Err("generated code is empty".to_string());
    }

    let (sql, name) = parse_to_sql(source, format)?;
    // `None` means the caller passed no `--target`, so the model keeps the
    // default the sidecar writer would also derive — one definition, not two
    // that have to agree.
    let resolved_target = target
        .cloned()
        .unwrap_or_else(|| crate::sidecar::SidecarTarget::default_for(&name));
    let strategy = materialization.map_or_else(
        rocky_core::models::StrategyConfig::default,
        crate::sidecar::SidecarMaterialization::to_strategy,
    );
    let generated = build_generated_model(&name, sql, source, format, &resolved_target, strategy);

    let (models, source_schemas) = match context {
        Some(ctx) => {
            let mut models: Vec<Model> = ctx.project_models.to_vec();
            models.push(generated);
            (models, ctx.source_schemas.clone())
        }
        None => (vec![generated], HashMap::new()),
    };

    let project = rocky_compiler::project::Project::from_models(models)
        .map_err(|e| format!("project loading failed: {e}"))?;

    // Go through the public compile entry point rather than hand-rolling
    // semantic graph + typecheck.
    //
    // The hand-rolled path saw only `TypeCheckResult` diagnostics, so every
    // diagnostic the compile pipeline merges around typecheck was invisible
    // here — concretely E036, the duplicate-physical-target error, which
    // meant AI generation could report a clean verify for a model colliding
    // with an existing one. That is not a subset that can be kept in sync by
    // hand: any diagnostic added to `compile_project` in future would have
    // gone missing here too, silently (#1302).
    let config = rocky_compiler::compile::CompilerConfig {
        source_schemas,
        ..Default::default()
    };
    let compiled = rocky_compiler::compile::compile_project(project, &config, Vec::new())
        .map_err(|e| format!("compile failed: {e}"))?;

    // Report the errors this generation *introduced*.
    //
    // Not "every error in the merged project": `rocky ai` validates with empty
    // `source_schemas` by a deliberate prior choice (see the `arc7-wave2` note
    // at its call site), so upstream columns degrade to nullable `Unknown` and
    // contract checks fire against models the real compile accepts. Failing on
    // those burns all three attempts on something no retry can fix — the
    // previous code's own comment observed as much.
    //
    // And not "errors naming the generated model" either, which is what I
    // tried first: a generated model that changes a downstream's upstream
    // types produces a join-key or contract error naming that *downstream*.
    // Ownership is not causation.
    //
    // The difference against a baseline compiled from the same inputs is both:
    // pre-existing noise cancels, and anything the new model caused — wherever
    // it surfaces — does not.
    let errors: Vec<String> = compiled
        .diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(std::string::ToString::to_string)
        .filter(|rendered| baseline.is_none_or(|b| !b.contains(rendered)))
        .collect();

    if errors.is_empty() {
        Ok((name, resolved_target))
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
/// `target` is the target the model will actually be written with, so
/// verification sees what lands on disk. `--target` used to be applied after
/// validation, which meant a generated model was checked against
/// `generated.ai.<name>` and then written somewhere else entirely — including,
/// possibly, on top of an existing model's physical target (#1302).
fn build_generated_model(
    name: &str,
    sql: String,
    source: &str,
    format: &str,
    target: &crate::sidecar::SidecarTarget,
    strategy: rocky_core::models::StrategyConfig,
) -> Model {
    rocky_core::models::Model {
        config: rocky_core::models::ModelConfig {
            name: name.to_string(),
            depends_on: vec![],
            strategy,
            target: rocky_core::models::TargetConfig {
                catalog: target.catalog.clone(),
                schema: target.schema.clone(),
                table: target.table.clone(),
            },
            sources: vec![],
            adapter: None,
            intent: None,
            freshness: None,
            tests: vec![],
            format: None,
            format_options: None,
            classification: Default::default(),
            tags: Default::default(),
            governance: Default::default(),
            retention: None,
            budget: None,
            skip: None,
            name_declared: String::new(),
            target_table_declared: String::new(),
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
        let result = validate_generated_code(
            "from orders\nselect { id }",
            "rocky",
            None,
            None,
            None,
            None,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap().0, "gen_orders");
    }

    #[test]
    fn test_validate_sql_isolated() {
        let result =
            validate_generated_code("SELECT id FROM orders", "sql", None, None, None, None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_empty() {
        let result = validate_generated_code("", "rocky", None, None, None, None);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_invalid_rocky() {
        let result =
            validate_generated_code("this is not valid rocky", "rocky", None, None, None, None);
        assert!(result.is_err());
    }

    // M3: the public gate `ai-sync --apply` uses before writing a proposal.

    #[test]
    fn validate_proposed_source_rejects_unparseable_sql() {
        // Garbage that is not a valid SQL statement must be rejected so it is
        // never written to a model file.
        let result =
            validate_proposed_source("this is not sql at all ;;;", "sql", None, None, None);
        assert!(result.is_err(), "unparseable SQL must be rejected");
    }

    #[test]
    fn validate_proposed_source_rejects_empty() {
        assert!(validate_proposed_source("", "sql", None, None, None).is_err());
    }

    #[test]
    fn validate_proposed_source_accepts_valid_sql() {
        assert!(validate_proposed_source("SELECT id FROM orders", "sql", None, None, None).is_ok());
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
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("upstream/{name}.sql"),
            contract_path: None,
        }
    }

    /// The headline of #1302: a generated model aimed at an existing model's
    /// physical target must fail verification.
    ///
    /// It did not, twice over — verification never saw E036 (it typechecked
    /// directly instead of compiling), and `--target` was applied after
    /// verification anyway, so the checked target was not the written one.
    #[test]
    fn a_generated_model_colliding_with_an_existing_target_fails_verification() {
        let upstream_models = vec![upstream_model(
            "orders",
            "SELECT CAST(1 AS BIGINT) AS id, CAST('x' AS VARCHAR) AS name",
        )];
        let empty_schemas = HashMap::new();
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
        };

        // `upstream_model` puts `orders` at test.test.orders.
        let collides = crate::sidecar::SidecarTarget::parse("test.test.orders").unwrap();
        let err = validate_generated_code(
            "from orders\nselect { id, name }",
            "rocky",
            Some(&ctx),
            Some(&collides),
            None,
            None,
        )
        .expect_err("a duplicate physical target must fail verification");
        assert!(
            err.contains("E036"),
            "the failure must be the duplicate-target diagnostic, got: {err}"
        );
    }

    /// The same generation with a target of its own still verifies — so the
    /// check above is refusing the collision, not the override.
    #[test]
    fn a_generated_model_with_its_own_target_still_verifies() {
        let upstream_models = vec![upstream_model(
            "orders",
            "SELECT CAST(1 AS BIGINT) AS id, CAST('x' AS VARCHAR) AS name",
        )];
        let empty_schemas = HashMap::new();
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
        };

        let distinct = crate::sidecar::SidecarTarget::parse("test.test.gen_orders").unwrap();
        let result = validate_generated_code(
            "from orders\nselect { id, name }",
            "rocky",
            Some(&ctx),
            Some(&distinct),
            None,
            None,
        );
        assert!(
            result.is_ok(),
            "expected Ok for a free target, got {result:?}"
        );
    }

    /// An `ephemeral` model cannot collide, so verifying it as `full_refresh`
    /// would reject a target conflict that will not exist on disk.
    ///
    /// The compiler excludes ephemerals from duplicate-target detection —
    /// they materialize nothing and are never a second writer — so the
    /// materialization has to reach verification for the same reason the
    /// target does. Without it, `--materialization ephemeral --target
    /// c.s.shared` fails on E036 while the sidecar it would have written is
    /// harmless.
    #[test]
    fn an_ephemeral_generation_does_not_collide_with_an_existing_target() {
        let upstream_models = vec![upstream_model(
            "orders",
            "SELECT CAST(1 AS BIGINT) AS id, CAST('x' AS VARCHAR) AS name",
        )];
        let empty_schemas = HashMap::new();
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
        };
        let collides = crate::sidecar::SidecarTarget::parse("test.test.orders").unwrap();

        // Same target that fails as full_refresh, above.
        let result = validate_generated_code(
            "from orders\nselect { id, name }",
            "rocky",
            Some(&ctx),
            Some(&collides),
            Some(&crate::sidecar::SidecarMaterialization::Ephemeral),
            None,
        );
        assert!(
            result.is_ok(),
            "an ephemeral model materializes nothing and cannot collide, got {result:?}"
        );
    }

    /// A diagnostic on an *upstream* model must not fail the generation.
    ///
    /// `rocky ai` validates with empty `source_schemas`, so upstream columns
    /// degrade to `Unknown, nullable = true` and checks the real compile
    /// satisfies can spuriously fire here. Reporting those would burn every
    /// retry on something no retry can fix — and the retry loop cannot fix an
    /// upstream problem by definition.
    #[test]
    fn an_upstream_error_does_not_fail_the_generated_model() {
        // Two upstream models sharing one physical target: an E036 collision
        // between them, entirely independent of what is generated.
        let mut a = upstream_model("up_a", "SELECT CAST(1 AS BIGINT) AS id");
        let mut b = upstream_model("up_b", "SELECT CAST(1 AS BIGINT) AS id");
        a.config.target.table = "shared".to_string();
        b.config.target.table = "shared".to_string();
        let upstream_models = vec![a, b];
        let empty_schemas = HashMap::new();
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
        };

        let free = crate::sidecar::SidecarTarget::parse("test.test.gen_thing").unwrap();
        // The baseline is what `generate_model` computes once per generation.
        let baseline = baseline_error_keys(&ctx).unwrap();
        assert!(
            !baseline.is_empty(),
            "the fixture must actually produce a pre-existing error, or this \
             test proves nothing"
        );
        let result = validate_generated_code(
            "SELECT 1 AS id",
            "sql",
            Some(&ctx),
            Some(&free),
            None,
            Some(&baseline),
        );
        assert!(
            result.is_ok(),
            "the upstream pair collides with each other, not with the generated \
             model — that is not this generation's failure: {result:?}"
        );
    }

    /// The override is what the model actually carries — the ordering half of
    /// #1302, pinned directly rather than only through its consequence.
    #[test]
    fn the_target_override_reaches_the_verified_model() {
        let target = crate::sidecar::SidecarTarget::parse("wh.mart.fct_orders").unwrap();
        let model = build_generated_model(
            "gen",
            "SELECT 1 AS id".to_string(),
            "",
            "sql",
            &target,
            Default::default(),
        );
        // And the resolved target is *returned*, so the caller writes the one
        // that was verified rather than deriving its own.
        let (_, returned) =
            validate_generated_code("SELECT 1 AS id", "sql", None, Some(&target), None, None)
                .unwrap();
        assert_eq!(returned.catalog, "wh");
        assert_eq!(returned.schema, "mart");
        assert_eq!(returned.table, "fct_orders");
        let (name, defaulted) =
            validate_generated_code("SELECT 1 AS id", "sql", None, None, None, None).unwrap();
        assert_eq!(defaulted.catalog, "generated");
        assert_eq!(defaulted.schema, "ai");
        assert_eq!(defaulted.table, name);
        assert_eq!(model.config.target.catalog, "wh");
        assert_eq!(model.config.target.schema, "mart");
        assert_eq!(model.config.target.table, "fct_orders");

        // And with no override, the default matches what the sidecar writer
        // derives — one definition rather than two that must agree.
        let default = crate::sidecar::SidecarTarget::default_for("gen");
        let model = build_generated_model(
            "gen",
            "SELECT 1 AS id".to_string(),
            "",
            "sql",
            &default,
            Default::default(),
        );
        assert_eq!(model.config.target.catalog, "generated");
        assert_eq!(model.config.target.schema, "ai");
        assert_eq!(model.config.target.table, "gen");
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
        let ctx = ValidationContext {
            project_models: &upstream_models,
            source_schemas: &empty_schemas,
        };

        let valid = "from orders\nselect { id, name }";
        let result = validate_generated_code(valid, "rocky", Some(&ctx), None, None, None);
        assert!(
            result.is_ok(),
            "expected Ok for real-upstream reference, got {result:?}"
        );
    }
}
