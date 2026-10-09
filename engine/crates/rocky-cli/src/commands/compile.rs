//! `rocky compile` — type-check models, resolve dependencies, validate contracts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_compiler::cost_check;
use rocky_compiler::diagnostic::{self, Diagnostic, Severity};
use rocky_compiler::source_refs::{SourceProvenance, SourceSchemaOrigin};
use rocky_compiler::types::TypedColumn;
use rocky_core::config as rocky_config;
use rocky_core::macros::{expand_macros, load_macros_from_dir};
use rocky_core::models::strategy_for_output;
use rocky_core::secret_registry::render_placeholders;
use rocky_sql::portability::{self, PortabilityIssue};
use rocky_sql::pragma;
use rocky_sql::transpile::Dialect;

use crate::output::{CompileOutput, CostHint, FunctionDetail, ModelDetail, print_json};

use rocky_server::project_gates::ModelSqlForm;

use super::ModelNotFound;

/// Which models a command reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelScope {
    /// The models under the `--models` directory. The command was given one,
    /// or the caller names its own directory.
    Dir,
    /// No `--models` was named: every transformation pipeline's own models,
    /// in one project graph. A project with no transformation pipeline, or
    /// none with a model, reads the default `models/` directory instead.
    WholeProject,
}

/// Execute `rocky compile`.
///
/// `cache_ttl_override`: optional CLI flag value from the binary's
/// `--cache-ttl <seconds>` global arg. Replaces
/// `[cache.schemas] ttl_seconds` for this invocation only. `None`
/// keeps the config/default TTL.
#[allow(clippy::too_many_arguments)]
pub fn run_compile(
    config_path: Option<&Path>,
    state_path: &Path,
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    with_seed: bool,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    deny_warning_codes: &[String],
) -> Result<()> {
    run_compile_with_options(
        config_path,
        state_path,
        models_dir,
        ModelScope::Dir,
        contracts_dir,
        model_filter,
        output_json,
        do_expand_macros,
        target_dialect,
        with_seed,
        cache_ttl_override,
        run_vars,
        false,
        false,
        deny_warning_codes,
        None,
    )
}

/// [`run_compile`] with every invocation option.
///
/// `strict_sources` (`rocky compile --strict-sources`) escalates every W041
/// (a source column missing from a seed or untrusted cached schema) to E041,
/// and every W045 (a source table missing from a seed or cached table list)
/// to E045
/// for this invocation. It ORs with `[cache.schemas] strict_sources`; it can
/// turn strictness on, never off.
///
/// `strict_contracts` (`rocky compile --strict-contracts`) turns `I003` (a
/// contract declares a column type Rocky cannot check) into the `E059` error.
/// It ORs with `[contracts] strict`; it can turn strictness on, never off.
///
/// `selection` (`--select` / `--exclude`) scopes the report: the whole
/// project still compiles (types flow across models); only the selected
/// models' details and diagnostics are reported, and only their errors fail
/// the command — the same scoping `--model` applies.
///
/// `scope` ([`ModelScope`]) picks the models: `models_dir` alone, or every
/// transformation pipeline's models when no `--models` was named.
#[allow(clippy::too_many_arguments)]
pub fn run_compile_with_options(
    config_path: Option<&Path>,
    state_path: &Path,
    models_dir: &Path,
    scope: ModelScope,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    with_seed: bool,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    strict_sources: bool,
    strict_contracts: bool,
    deny_warning_codes: &[String],
    selection: Option<&crate::selection::SelectionArgs>,
) -> Result<()> {
    validate_deny_warning_codes(deny_warning_codes)?;
    let (mut output, text_data) = compile_inner(
        config_path,
        state_path,
        models_dir,
        scope,
        contracts_dir,
        model_filter,
        do_expand_macros,
        target_dialect,
        if with_seed {
            SeedUse::Required
        } else {
            SeedUse::IfPresent
        },
        cache_ttl_override,
        run_vars,
        strict_sources,
        strict_contracts,
        selection,
    )?;

    if deny_warnings(&mut output.diagnostics, deny_warning_codes) {
        output.has_errors = true;
    }

    if output_json {
        print_json(&output)?;
    } else {
        render_compile_text(&output, &text_data);
    }

    if output.has_errors {
        anyhow::bail!("compilation failed with errors");
    }

    Ok(())
}

/// Execute `rocky compile --dbt-project <DIR>` (experimental "attach mode").
///
/// Reads `<DIR>/target/manifest.json` (and its sibling `run_results.json`)
/// through the same importer `rocky import-dbt` uses, so it refuses the same
/// constructs with the same reasons. The translated project is written to a
/// private temp directory, compiled with [`run_compile_with_options`], and removed when
/// this function returns. Nothing is written under `<DIR>`: the config, the
/// models and the state path all point into the temp directory.
///
/// Attach notes (manifest read, warnings) go to stderr so `--output json`
/// keeps the unchanged [`CompileOutput`] shape on stdout. Diagnostics name
/// file paths inside the temp directory.
#[allow(clippy::too_many_arguments)]
pub fn run_compile_dbt_attach(
    dbt_project: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    output_json: bool,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    strict_sources: bool,
    strict_contracts: bool,
    deny_warning_codes: &[String],
    selection: Option<&crate::selection::SelectionArgs>,
) -> Result<()> {
    use rocky_compiler::import::dbt_attach;
    use rocky_compiler::import::emit::{self, EmitInputs, OverwritePolicy};

    if !dbt_project.is_dir() {
        anyhow::bail!("--dbt-project {} is not a directory", dbt_project.display());
    }

    let attached = dbt_attach::attach_dbt_project(dbt_project)?;

    eprintln!(
        "rocky compile (experimental dbt attach): read {} (manifest schema v{}), {} model(s)",
        attached.manifest_path.display(),
        attached.schema_version,
        attached.import.imported.len()
    );
    if let Some(reason) = &attached.profile.fallback_reason {
        eprintln!("  warning: <profiles.yml>: {reason}");
    }
    for w in &attached.import.warnings {
        eprintln!("  warning: {}: {}", w.model, w.message);
    }

    // `TempDir` removes the directory on drop, including on every early
    // return below. The project goes one level down so `emit_repo` sees a
    // fresh, absent directory and never needs `ReplaceContents`.
    let scratch = tempfile::Builder::new()
        .prefix("rocky-dbt-attach-")
        .tempdir()
        .context("failed to create a temp directory for dbt attach mode")?;
    let project_dir = scratch.path().join("project");

    emit::emit_repo(&EmitInputs {
        dbt_project_dir: dbt_project,
        out_dir: &project_dir,
        overwrite: OverwritePolicy::Reject,
        profile: &attached.profile,
        default_catalog: &attached.default_target.catalog,
        default_schema: &attached.default_target.schema,
        import: &attached.import,
        adapter_override_label: None,
    })
    .map_err(|e| anyhow::anyhow!("dbt attach mode could not materialize the project: {e}"))?;

    run_compile_with_options(
        Some(&project_dir.join("rocky.toml")),
        &scratch.path().join("state.redb"),
        &project_dir.join("models"),
        ModelScope::Dir,
        contracts_dir,
        model_filter,
        output_json,
        do_expand_macros,
        target_dialect,
        false,
        cache_ttl_override,
        run_vars,
        strict_sources,
        strict_contracts,
        deny_warning_codes,
        selection,
    )
}

/// Whether a compile runs the project's seed file for source schemas.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeedUse {
    /// `--with-seed`: the seed's tables only, and a missing or broken seed
    /// is an error.
    Required,
    /// The `rocky compile` default: the seed's tables fill what the schema
    /// cache lacks, when the project has a seed that runs.
    IfPresent,
    /// Never run the seed. For in-process callers ([`compile_output`]: the
    /// `rocky serve` API and the MCP compile tool), which compile on request
    /// and must not execute the project's seed SQL each time.
    Never,
}

/// Compile body shared by the JSON core ([`compile_output`]) and the text
/// renderer in [`run_compile`]. Returns the typed [`CompileOutput`] plus the
/// extra raw data the text path needs ([`CompileTextData`]).
///
/// Does no stdout printing and does not bail on compilation errors.
///
/// `cache_ttl_override`: optional CLI `--cache-ttl <seconds>` value that
/// replaces `[cache.schemas] ttl_seconds` for this invocation only.
#[allow(clippy::too_many_arguments)]
fn compile_inner(
    config_path: Option<&Path>,
    state_path: &Path,
    models_dir: &Path,
    scope: ModelScope,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    seed_use: SeedUse,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    strict_sources: bool,
    strict_contracts: bool,
    selection: Option<&crate::selection::SelectionArgs>,
) -> Result<(CompileOutput, CompileTextData)> {
    // Load the project config ONCE, and let a failure fail the command.
    //
    // This used to be four independent loads — schema cache, `[mask]` /
    // `[classifications]`, the portability lint, `[imports]` — each
    // swallowing its error with `.ok()` or `Err(_) =>`. One malformed file
    // therefore produced an empty mask, no portability lint, no imports
    // check and a cold schema cache, with nothing said (#1521).
    //
    // The rule that fixed it now lives in `load_optional_project_config`, which
    // every offline entry point shares (#1625) rather than restating: absent is
    // `None`, an unset `${VAR}` in adapter credentials is tolerated (#1536),
    // and every other failure refuses. Read that function before changing what
    // an unloadable config means here.
    //
    // Note `--config` defaults to `rocky.toml`, so this path runs even when
    // the user passed no flag.
    // The refusal NAMES the file. `ConfigError`'s own `Display` is
    // "failed to parse TOML: …", and model sidecars are TOML too, so a bare
    // message cannot tell the reader whether to fix `rocky.toml` or a
    // `<model>.toml`. The shared loader (#1625) decides WHAT refuses — only a
    // missing file is tolerated; this context says WHICH file refused.
    let project_config =
        rocky_config::load_optional_project_config(config_path).with_context(|| {
            format!(
                "failed to load config from {}",
                config_path
                    .map(|p| p.display().to_string())
                    .unwrap_or_default()
            )
        })?;

    // The project root: where `data/seed.sql` lives. A whole-project compile
    // reads it beside `rocky.toml`; a `--models <dir>` compile beside that
    // directory, as `--with-seed` always has.
    let config_file_path = config_path.unwrap_or_else(|| Path::new("rocky.toml"));
    let project_root = match scope {
        ModelScope::WholeProject => config_file_path.parent(),
        ModelScope::Dir => models_dir.parent(),
    }
    .unwrap_or_else(|| Path::new("."));

    // `source_schemas` precedence:
    //   1. `--with-seed` wins -> seed loader (explicit user intent,
    //      used for tests/playgrounds where the cache is irrelevant).
    //   2. Otherwise, the schema cache if `[cache.schemas] enabled`, with
    //      the project's seed file (when it has one) filling every table
    //      the cache does not hold. The seed runs in an in-memory DuckDB;
    //      nothing contacts the warehouse.
    //   3. Neither: empty map — typecheck degrades to Unknown.
    //
    // Each tier also records where its schemas came from, for the E041 /
    // W041 missing-source-column check: a seed is `Seed` (W041 unless
    // strict), a cache entry is `Cache` with its timestamp (E041 only within
    // `[cache.schemas] trusted_max_age_seconds`). `--strict-sources` and
    // `[cache.schemas] strict_sources` escalate every W041 to E041.
    let config_strict_sources = project_config
        .as_ref()
        .is_some_and(|config| config.cache.schemas.strict_sources);
    let (source_schemas, source_provenance) = if seed_use == SeedUse::Required {
        // Seed loader: run `data/seed.sql` in in-memory DuckDB, read
        // columns from its `information_schema`. Turns leaf .sql models
        // from `RockyType::Unknown` into concrete types for any project
        // that ships a runnable seed (the entire playground).
        let schemas = load_source_schemas_from_seed_at(project_root)?;
        let provenance = SourceProvenance::uniform(schemas.keys(), &SourceSchemaOrigin::Seed);
        (schemas, provenance)
    } else {
        let (mut schemas, mut provenance) = if let Some(config) = &project_config {
            // TTL-filtered load from `state.redb`'s `SCHEMA_CACHE` table.
            // Honours `[cache.schemas] enabled` + `ttl_seconds` (after
            // applying the optional CLI `--cache-ttl` override).
            let schema_cfg = config
                .cache
                .schemas
                .clone()
                .with_ttl_override(cache_ttl_override);
            crate::source_schemas::load_cached_source_schemas_with_provenance(
                &schema_cfg,
                state_path,
            )
        } else {
            (HashMap::new(), SourceProvenance::default())
        };
        match seed_use {
            SeedUse::IfPresent => {
                merge_default_seed_schemas(project_root, &mut schemas, &mut provenance);
            }
            SeedUse::Never | SeedUse::Required => {}
        }
        (schemas, provenance)
    };
    let source_provenance = source_provenance.with_strict(strict_sources || config_strict_sources);
    // `--strict-contracts` ORs with `[contracts] strict`; it can turn
    // strictness on, never off.
    let strict_contracts = strict_contracts
        || project_config
            .as_ref()
            .is_some_and(|config| config.contracts.strict);

    // Load `[mask]` + `[classifications.allow_unmasked]` for the W004
    // classification-tag completeness check. No rocky.toml (standalone
    // `rocky compile --models models/`) means both come through empty —
    // W004 never fires, matching the pre-check behaviour. The same
    // resolved `RockyConfig` also supplies the project `[freshness]`
    // block: it is the last rung of each model's freshness precedence
    // chain (#1435), and when it declares an `expected_lag_seconds` every
    // model is covered and W005 stays silent.
    let (mask, allow_unmasked, project_freshness) = match &project_config {
        Some(config) => (
            config.mask.clone(),
            config.classifications.allow_unmasked.clone(),
            config.freshness.clone(),
        ),
        None => (
            std::collections::BTreeMap::new(),
            Vec::new(),
            Default::default(),
        ),
    };

    // The warehouses each model runs on, from the pipelines that target
    // them (not every configured adapter). The compile types a `CAST` for
    // them, and the adapter gates below judge each model against them,
    // refusing when any one refuses.
    let model_targets = project_config
        .as_ref()
        .map(|config| ModelTargets::resolve(config, config_file_path));

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        required_explicit_contract_model: None,
        source_schemas,
        mask,
        allow_unmasked,
        project_freshness,
        run_vars: run_vars.clone(),
        source_provenance,
        strict_contracts,
        // The lints below (P001, E042/E043, imports E030/E033) judge each
        // model's SQL as authored. Against the inlined form, an ephemeral
        // model's defect would be reported again on every consumer, at
        // spans that do not exist in the consumer's file. The inlined form
        // is written back after them, for `--expand-macros`.
        preserve_authored_sql: true,
        external_dependencies: Default::default(),
        project: None,
        target_dialects: target_dialects_of(
            target_dialect,
            project_config.as_ref(),
            model_targets.as_ref(),
        ),
    };

    // Without `--models`, one compile over every transformation pipeline's
    // models: a model of one pipeline that reads another's output gets that
    // output's column types. A loader refusal (one model name in two files)
    // fails the command; it would fail `rocky run --dag` the same way.
    let whole_project = match (scope, &project_config) {
        (ModelScope::WholeProject, Some(project)) => {
            crate::models_loader::whole_project_models(config_file_path, project)?
        }
        (ModelScope::WholeProject, None) | (ModelScope::Dir, _) => None,
    };
    // `consumers/` belongs to the project, not to whichever models this
    // compile covers: read it from the config's directory and judge it against
    // every model in the project. A whole-project compile already holds them.
    let mut config = config;
    if let Some(project) = &project_config {
        let mut context = rocky_compiler::consumers::project_context(config_file_path, project);
        if let Some(models) = &whole_project {
            context.model_names = rocky_compiler::consumers::model_name_set(models);
        }
        config.project = Some(context);
    }
    let compiled = match whole_project {
        Some(models) => compile::compile_preloaded_models(models, &config),
        None => compile::compile(&config),
    };
    let mut result = match compiled {
        Ok(result) => result,
        Err(error) => match error.cycle_diagnostics() {
            Some(diagnostics) => return Ok(cycle_output(diagnostics)),
            None => return Err(error.into()),
        },
    };

    // `--model` may also name a user-defined function (`functions/`), valid
    // or not, to see its own diagnostics.
    if let Some(filter) = model_filter
        && result.project.model(filter).is_none()
        && !result.semantic_graph.functions().declares(filter)
    {
        return Err(anyhow::Error::new(ModelNotFound(filter.to_string())));
    }
    let selected: Option<std::collections::BTreeSet<String>> = match selection {
        Some(args) if args.is_active() => Some(crate::selection::resolve(
            args,
            &result.project,
            models_dir,
            &crate::selection::StateContext {
                config_path: config_path.unwrap_or_else(|| Path::new("rocky.toml")),
                state_path,
                cache_ttl_override,
            },
        )?),
        _ => None,
    };
    let in_scope = |name: &str| {
        model_filter.is_none_or(|filter| name == filter)
            && selected.as_ref().is_none_or(|set| set.contains(name))
    };
    let scoped = model_filter.is_some() || selected.is_some();

    if let Some(targets) = &model_targets {
        apply_adapter_gates(&mut result, targets);
    }

    // Portability lint. Effective target_dialect = CLI flag > [portability]
    // config > unset. Project-wide allow list and per-model `-- rocky-allow:`
    // pragmas suppress matching constructs before they become diagnostics.
    let portability_cfg = project_config.as_ref().map(|config| &config.portability);
    let effective_dialect =
        target_dialect.or_else(|| portability_cfg.and_then(|p| p.target_dialect));
    let project_allow: std::collections::HashSet<String> = portability_cfg
        .map(|p| {
            p.allow
                .iter()
                .map(|c| c.trim().to_ascii_uppercase())
                .collect()
        })
        .unwrap_or_default();

    if let Some(dialect) = effective_dialect {
        let mut portability_errors = false;
        for model in &result.project.models {
            let model_pragmas = pragma::parse_pragmas(&model.sql);
            for issue in portability::detect_portability_issues(&model.sql, dialect) {
                let upper = issue.construct.to_ascii_uppercase();
                if project_allow.contains(&upper) || model_pragmas.allows(&upper) {
                    continue;
                }
                result.diagnostics.push(build_p001_diagnostic(
                    &model.config.name,
                    &model.file_path.display().to_string(),
                    &issue,
                ));
                portability_errors = true;
            }
        }
        if portability_errors {
            result.has_errors = true;
        }
    }

    // Aggregate-argument and comparison-operand checks (E042/W042,
    // E043/W043). See `apply_operand_gates`.
    apply_operand_gates(
        &mut result,
        project_config.as_ref(),
        model_targets.as_ref(),
        target_dialect,
    );

    // Compute DAG-propagated cost estimates for all models.
    // Uses hardcoded stub statistics for leaf nodes — real catalog stats
    // (per-adapter `DESCRIBE DETAIL` / Iceberg snapshot summary) will replace
    // these stubs in a follow-up that wires the adapter registry here.
    // `rocky plan`'s cost preview uses the same heuristic.
    let cost_estimates = super::plan_cost::heuristic_cost_estimates(
        &result.project.dag_nodes,
        rocky_core::cost::WarehouseType::Databricks,
    );

    // Check per-model cost ceilings and emit E027 diagnostics for breaches.
    let ceiling_diagnostics =
        cost_check::check_cost_ceilings(&result.project.models, &cost_estimates);
    if !ceiling_diagnostics.is_empty() {
        result.diagnostics.extend(ceiling_diagnostics);
        result.has_errors = true;
    }

    // Cross-team-contracts: check the consumer's column references against
    // any imported producer snapshots (`[imports.<name>]`). A producer that
    // dropped a column the consumer still reads surfaces as E030; a
    // recipe-hash pin mismatch surfaces as E033. Projects with no `[imports]`
    // block incur no work — `imports_diagnostics` returns empty immediately.
    if let Some(path) = config_path
        && let Some(config) = &project_config
        && !config.imports.is_empty()
    {
        let config_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let import_diags = crate::commands::imports_check::imports_diagnostics(
            config,
            config_dir,
            &result.project.models,
        );
        if import_diags.iter().any(|d| d.severity == Severity::Error) {
            result.has_errors = true;
        }
        result.diagnostics.extend(import_diags);
    }

    // Every lint that reads model SQL has run on the authored text. Keep
    // that text for the miette source map (diagnostic spans point into it),
    // then write back the form that inlines ephemeral upstreams as CTEs —
    // the statement `rocky run` executes, which `--expand-macros` shows.
    // The E038 diagnostics this returns were already reported by
    // `compile::compile`, which ran the same checks.
    let authored_source_map: HashMap<String, String> = result
        .project
        .models
        .iter()
        .filter(|model| in_scope(&model.config.name))
        .map(|m| (m.file_path.display().to_string(), m.sql.clone()))
        .collect();
    let _already_reported = rocky_compiler::ephemeral::apply_ephemerals(&mut result.project, true);
    // SQL Server lifts every CTE to the head of the statement; check that
    // on the inlined SQL, the text `rocky run` sends (E054).
    if let Some(targets) = &model_targets {
        apply_inlined_sql_gates(&mut result, targets);
    }

    // Load macros and expand model SQL when --expand-macros is set.
    let expanded_sql = if do_expand_macros {
        let macros_dir = models_dir.join("../macros");
        let macro_defs = if macros_dir.is_dir() {
            load_macros_from_dir(&macros_dir)?
        } else {
            vec![]
        };

        let mut expanded = HashMap::new();
        for model in &result.project.models {
            if !in_scope(&model.config.name) {
                continue;
            }
            let sql = expand_macros(&model.sql, &macro_defs)?;
            expanded.insert(model.config.name.clone(), sql);
        }
        expanded
    } else {
        HashMap::new()
    };

    // E050 / W050 for transformation pipelines' declared source freshness
    // (`[[pipeline.<name>.sources]]`). The source schemas are the same map the
    // typecheck used (seed or schema cache), so a stale schema only warns.
    if let Some(config_file) = &project_config {
        for pipeline in config_file.pipelines.values() {
            if let Some(tx) = pipeline.as_transformation() {
                let diags = rocky_compiler::freshness::check_source_freshness(
                    &tx.sources,
                    &config.source_schemas,
                );
                if diags.iter().any(Diagnostic::is_error) {
                    result.has_errors = true;
                }
                result.diagnostics.extend(diags);
            }
        }
    }

    // Re-apply model filter to diagnostics (may now include E027).
    // A diagnostic can quote a sidecar value (a target collision names the
    // resolved target), so its text prints each resolved `${VAR}` value as
    // `${NAME}` (#1919). Its `model` is a model name and prints resolved:
    // dagster-rocky matches it against a contract file's stem.
    let diagnostics: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| in_scope(&d.model))
        .map(|d| Diagnostic {
            message: render_placeholders(&d.message).into(),
            suggestion: d.suggestion.as_deref().map(render_placeholders),
            ..d.clone()
        })
        .collect();

    let models_detail: Vec<ModelDetail> = result
        .project
        .models
        .iter()
        .filter(|model| in_scope(&model.config.name))
        .map(|model| {
            let cost_hint = cost_estimates.get(&model.config.name).map(|est| CostHint {
                estimated_rows: est.estimated_rows,
                estimated_bytes: est.estimated_bytes,
                estimated_cost_usd: est.estimated_compute_cost_usd,
                confidence: match est.confidence {
                    rocky_core::cost::Confidence::High => "high".to_string(),
                    rocky_core::cost::Confidence::Medium => "medium".to_string(),
                    rocky_core::cost::Confidence::Low => "low".to_string(),
                },
            });
            // Sidecar values were `${VAR}`-expanded before parsing. Tags print
            // each resolved value as `${NAME}` (#1919). The rest prints
            // resolved, exactly as `rocky run` and `rocky dag` print it,
            // because dagster-rocky matches on it:
            // - `name` / `depends_on`: matched against `rocky run`'s model
            //   names, contract file stems and other models' names;
            // - `target`: the asset key, matched against `rocky run`'s
            //   `asset_key`;
            // - `strategy` / `freshness`: structure (partition start and
            //   grain, column names). See `strategy_for_output`.
            ModelDetail {
                name: model.config.name.clone(),
                strategy: strategy_for_output(&model.config.strategy),
                target: model.config.target.clone(),
                freshness: model.config.freshness.clone(),
                contract_source: model.contract_path.as_ref().map(|_| "auto".to_string()),
                cost_hint,
                depends_on: model.config.depends_on.clone(),
                tags: model
                    .config
                    .tags
                    .iter()
                    .map(|(k, v)| (render_placeholders(k), render_placeholders(v)))
                    .collect(),
            }
        })
        .collect();

    // Data the text renderer needs from the raw `CompileResult` but which is
    // not carried on `CompileOutput`: execution order, per-model typed-column
    // counts, and the file_path -> SQL source map (for miette spans).
    let text_data = CompileTextData {
        execution_order: result
            .project
            .execution_order
            .iter()
            .filter(|name| in_scope(name))
            .cloned()
            .collect(),
        typed_column_counts: result
            .type_check
            .typed_models
            .iter()
            .filter(|(name, _)| in_scope(name))
            .map(|(name, cols)| (name.clone(), cols.len()))
            .collect(),
        source_map: authored_source_map,
    };

    let execution_layers = if scoped {
        result
            .project
            .layers
            .iter()
            .filter(|layer| layer.iter().any(|name| in_scope(name)))
            .count()
    } else {
        result.project.layers.len()
    };
    let has_errors = if scoped {
        diagnostics.iter().any(|d| d.severity == Severity::Error)
    } else {
        result.has_errors
    };

    let has_errors = has_errors || diagnostics.iter().any(|d| d.severity == Severity::Error);
    let functions = function_details(&result, model_filter);

    let output = CompileOutput::new(
        models_detail.len(),
        execution_layers,
        diagnostics,
        has_errors,
        result.timings.clone(),
    )
    .with_models_detail(models_detail)
    .with_expanded_sql(expanded_sql)
    .with_functions(functions);

    Ok((output, text_data))
}

/// The output of a compile refused by a dependency cycle: the E058
/// diagnostics, and no models (a cyclic project has no execution order, so
/// nothing past dependency resolution ran).
///
/// Every cycle diagnostic is reported, whatever `--model` or `--select`
/// names: a cycle anywhere stops the whole project from running.
fn cycle_output(diagnostics: &[Diagnostic]) -> (CompileOutput, CompileTextData) {
    let mut execution_order: Vec<String> = Vec::new();
    let mut source_map = HashMap::new();
    for d in diagnostics {
        if !execution_order.contains(&d.model) {
            execution_order.push(d.model.clone());
        }
        if let Some(span) = &d.span
            && let Ok(text) = std::fs::read_to_string(&span.file)
        {
            source_map.insert(span.file.clone(), text);
        }
    }
    let output = CompileOutput::new(
        0,
        0,
        diagnostics.to_vec(),
        true,
        compile::PhaseTimings::default(),
    );
    let text_data = CompileTextData {
        execution_order,
        typed_column_counts: HashMap::new(),
        source_map,
    };
    (output, text_data)
}

/// The project-level, per-model-target checks of `rocky compile`, for the
/// surfaces that compile through `rocky_compiler::compile` directly and
/// check one SQL form: `rocky serve` and `rocky lsp`. `rocky ci` and
/// `rocky test` run the same checks in two halves, around ephemeral
/// inlining: [`apply_authored_model_target_gates`] and
/// [`apply_inlined_model_target_gates`].
///
/// This is the ONE funnel for those surfaces. `rocky compile` runs the same
/// three steps ([`apply_adapter_gates`], [`apply_operand_gates`],
/// [`apply_inlined_sql_gates`]) itself, at its own points in a longer
/// pipeline, so a model is judged against the same warehouses everywhere.
/// E054 runs on both SQL forms. On [`ModelSqlForm::Authored`] SQL it can miss
/// a CTE that only inlining adds, but it never reports one the inlined
/// statement would not: inlining keeps every authored CTE.
///
/// `has_errors` is recomputed at the end. The callers read the flag, not the
/// diagnostics, to decide whether the project compiled.
pub fn apply_model_target_gates(
    result: &mut compile::CompileResult,
    config: &rocky_config::RockyConfig,
    config_path: &Path,
    _sql: ModelSqlForm,
) {
    let targets = ModelTargets::resolve(config, config_path);
    apply_adapter_gates(result, &targets);
    apply_operand_gates(result, Some(config), Some(&targets), None);
    apply_inlined_sql_gates(result, &targets);
    result.has_errors |= result.diagnostics.iter().any(Diagnostic::is_error);
}

/// The checks of [`apply_model_target_gates`] that read each model's
/// authored SQL: E044/W044, E051, E049, E053, E042/E043 and E057. Run them
/// before ephemeral upstreams are inlined, so an ephemeral's body is not
/// judged once more inside each consumer.
pub fn apply_authored_model_target_gates(
    result: &mut compile::CompileResult,
    config: &rocky_config::RockyConfig,
    config_path: &Path,
) {
    let targets = ModelTargets::resolve(config, config_path);
    apply_adapter_gates(result, &targets);
    apply_operand_gates(result, Some(config), Some(&targets), None);
    result.has_errors |= result.diagnostics.iter().any(Diagnostic::is_error);
}

/// The check of [`apply_model_target_gates`] that reads the SQL each model
/// executes: E054 (SQL Server lifts every CTE to the head of the statement).
/// Run it after ephemeral upstreams are inlined, on the statement
/// `rocky run` sends, so a CTE that only inlining adds is found too.
pub fn apply_inlined_model_target_gates(
    result: &mut compile::CompileResult,
    config: &rocky_config::RockyConfig,
    config_path: &Path,
) {
    let targets = ModelTargets::resolve(config, config_path);
    apply_inlined_sql_gates(result, &targets);
    result.has_errors |= result.diagnostics.iter().any(Diagnostic::is_error);
}

/// E044 -> W044 on PostgreSQL, E051, E049 and E053, judged against the
/// warehouses each model runs on.
fn apply_adapter_gates(result: &mut compile::CompileResult, targets: &ModelTargets<'_>) {
    // PostgreSQL accepts a column functionally dependent on a grouped
    // primary key, which Rocky cannot see: E044 is a warning (W044)
    // there. Redshift keeps E044.
    if rocky_compiler::group_by::downgrade_for_postgres(&mut result.diagnostics, |m| {
        runs_only_on_postgres(targets, m)
    }) > 0
    {
        result.has_errors = result.diagnostics.iter().any(Diagnostic::is_error);
    }
    // A warehouse that cannot create functions refuses them here (E051), at
    // compile time, rather than mid-run.
    let function_diags = function_adapter_diagnostics(targets, result);
    result.diagnostics.extend(function_diags);
    // Likewise a warehouse that cannot run the SCD2 snapshot MERGE.
    let snapshot_diags = snapshot_adapter_diagnostics(targets, result);
    result.diagnostics.extend(snapshot_diags);
    // And one with no upsert at all (ClickHouse): E053.
    let merge_diags = merge_adapter_diagnostics(targets, result);
    result.diagnostics.extend(merge_diags);
}

/// Aggregate-argument and comparison-operand checks (E042/W042, E043/W043),
/// and calls to functions the target warehouse does not have (E057, W057).
/// These judge against the warehouse that will run the SQL, so they need a
/// dialect the compiler core does not carry; see `operand_target_for` for
/// the precedence.
fn apply_operand_gates(
    result: &mut compile::CompileResult,
    project_config: Option<&rocky_config::RockyConfig>,
    targets: Option<&ModelTargets<'_>>,
    target_dialect: Option<Dialect>,
) {
    let target_for =
        |model: &str| operand_target_for(target_dialect, project_config, targets, model);
    let mut operand_diags = rocky_compiler::operand_check::check_operand_types_per_model(
        &result.project.models,
        &result.semantic_graph,
        &result.type_check.typed_models,
        &target_for,
    );
    // Calls to functions the target warehouse does not have: E057 on the
    // warehouses whose list was checked against a live engine, W057 on the
    // others (their list is built from documentation). A model is not judged against a warehouse other than the one `[portability]
    // target_dialect` says its SQL is written for: P001 covers portability.
    let written_for = project_config
        .and_then(|c| c.portability.target_dialect)
        .map(rocky_compiler::operand_check::OperandDialect::from);
    operand_diags.extend(rocky_compiler::function_check::check_unknown_functions(
        &result.project.models,
        result.semantic_graph.functions(),
        &target_for,
        written_for,
    ));
    if operand_diags.iter().any(|d| d.severity == Severity::Error) {
        result.has_errors = true;
    }
    result.diagnostics.extend(operand_diags);
}

/// SQL Server lifts every CTE to the head of the statement; check that on
/// the inlined SQL, the text `rocky run` sends (E054).
fn apply_inlined_sql_gates(result: &mut compile::CompileResult, targets: &ModelTargets<'_>) {
    let diags = sqlserver_cte_diagnostics(targets, result);
    result.diagnostics.extend(diags);
}

/// The warehouse dialects the E042/E043 operand checks judge `model` against.
///
/// Precedence: an explicit `--target-dialect` flag (every model), then the
/// adapter types of the warehouses the model runs on ([`ModelTargets`]; the
/// most severe verdict across them wins), then `[portability]
/// target_dialect`. With none of these the checks report the least severe
/// verdict across all dialects (warnings only).
fn operand_target_for(
    target_dialect: Option<Dialect>,
    config: Option<&rocky_config::RockyConfig>,
    targets: Option<&ModelTargets<'_>>,
    model: &str,
) -> rocky_compiler::operand_check::OperandTarget {
    operand_target_of(
        target_dialect,
        config,
        targets.map(|targets| targets.for_model(model)),
    )
}

/// The warehouse each model runs on, for the compile itself: a `CAST` to a
/// type whose width differs between warehouses is typed for it (#2333). The
/// same precedence as the operand checks ([`operand_target_for`]), so both
/// read one answer.
pub(crate) fn target_dialects(
    target_dialect: Option<Dialect>,
    config: Option<&rocky_config::RockyConfig>,
    config_path: &Path,
) -> rocky_compiler::operand_check::TargetDialects {
    let targets = config.map(|config| ModelTargets::resolve(config, config_path));
    target_dialects_of(target_dialect, config, targets.as_ref())
}

/// [`target_dialects`] given the resolved [`ModelTargets`].
fn target_dialects_of(
    target_dialect: Option<Dialect>,
    config: Option<&rocky_config::RockyConfig>,
    targets: Option<&ModelTargets<'_>>,
) -> rocky_compiler::operand_check::TargetDialects {
    use rocky_compiler::operand_check::TargetDialects;

    let mut out = TargetDialects::uniform(operand_target_of(
        target_dialect,
        config,
        targets.map(ModelTargets::for_unlisted_model),
    ));
    if let Some(targets) = targets {
        for model in targets.by_model.keys() {
            out.set(
                model.clone(),
                operand_target_for(target_dialect, config, Some(targets), model),
            );
        }
    }
    out
}

/// [`operand_target_for`] given the warehouses the model runs on.
fn operand_target_of(
    target_dialect: Option<Dialect>,
    config: Option<&rocky_config::RockyConfig>,
    adapters: Option<Vec<&rocky_config::AdapterConfig>>,
) -> rocky_compiler::operand_check::OperandTarget {
    use rocky_compiler::operand_check::{OperandDialect, OperandTarget};

    if let Some(dialect) = target_dialect {
        return Some(OperandDialect::from(dialect)).into();
    }
    if let Some(adapters) = adapters
        && !adapters.is_empty()
    {
        let mut dialects = Vec::new();
        let mut unruled = Vec::new();
        for adapter in adapters {
            match OperandDialect::from_adapter_type(&adapter.adapter_type) {
                Some(d) if !dialects.contains(&d) => dialects.push(d),
                Some(_) => {}
                None if !unruled.contains(&adapter.adapter_type) => {
                    unruled.push(adapter.adapter_type.clone());
                }
                None => {}
            }
        }
        if dialects.is_empty()
            && let Some(d) = config.and_then(|c| c.portability.target_dialect)
        {
            return Some(OperandDialect::from(d)).into();
        }
        return OperandTarget::Targets { dialects, unruled };
    }
    config
        .and_then(|c| c.portability.target_dialect)
        .map(OperandDialect::from)
        .into()
}

/// Refuse `--deny-warnings` codes that name no warning Rocky emits — an
/// unknown code (`W999`), a malformed one (`W42`, `W 042`), or an error code.
/// A typo there would otherwise escalate nothing and pass in silence.
fn validate_deny_warning_codes(codes: &[String]) -> Result<()> {
    let bad: Vec<&str> = codes
        .iter()
        .map(String::as_str)
        .filter(|c| !diagnostic::is_warning_code(c))
        .collect();
    if bad.is_empty() {
        return Ok(());
    }
    anyhow::bail!(
        "--deny-warnings: unknown or malformed warning code(s): {}. Valid codes: {}",
        bad.iter()
            .map(|c| format!("`{c}`"))
            .collect::<Vec<_>>()
            .join(", "),
        diagnostic::WARNING_CODES.join(", ")
    )
}

/// Escalate warning diagnostics whose code is listed in `--deny-warnings` to
/// errors. Codes match case-insensitively; non-warning diagnostics are left
/// as they are. Returns whether any diagnostic was escalated.
fn deny_warnings(diagnostics: &mut [Diagnostic], codes: &[String]) -> bool {
    let mut escalated = false;
    for diag in diagnostics {
        if diag.severity == Severity::Warning
            && codes
                .iter()
                .any(|c| c.trim().eq_ignore_ascii_case(&diag.code))
        {
            diag.severity = Severity::Error;
            escalated = true;
        }
    }
    escalated
}

/// `CompileOutput.functions`: every valid user-defined function with the
/// models that call it. Under `--model`, the selected function, or the
/// functions the selected model calls (and the functions those call).
fn function_details(
    result: &compile::CompileResult,
    model_filter: Option<&str>,
) -> Vec<FunctionDetail> {
    let registry = result.semantic_graph.functions();
    if registry.is_empty() {
        return Vec::new();
    }
    let usage = rocky_compiler::udf::function_usage(&result.project.models, registry);
    let wanted: Option<std::collections::HashSet<String>> = model_filter.map(|filter| {
        let roots: Vec<&str> = if registry.get(filter).is_some() {
            vec![filter]
        } else {
            usage
                .iter()
                .filter(|(_, callers)| callers.contains(filter))
                .map(|(name, _)| name.as_str())
                .collect()
        };
        registry
            .creation_order(roots)
            .into_iter()
            .map(|sig| sig.def.name.to_ascii_lowercase())
            .collect()
    });
    registry
        .functions()
        .filter(|sig| {
            wanted
                .as_ref()
                .is_none_or(|w| w.contains(&sig.def.name.to_ascii_lowercase()))
        })
        .map(|sig| FunctionDetail {
            name: sig.def.name.clone(),
            signature: sig.signature(),
            returns: sig.def.config.returns.trim().to_string(),
            description: sig.def.config.description.clone(),
            deterministic: sig.def.config.deterministic,
            called_by: usage
                .get(&sig.def.name)
                .map(|callers| callers.iter().cloned().collect())
                .unwrap_or_default(),
            calls: sig.calls.iter().cloned().collect(),
        })
        .collect()
}

/// Whether an adapter block acts as a warehouse (the data role): every
/// block except a discovery-only type (`fivetran`, `airbyte`, …) or one
/// declared `kind = "discovery"`. An adapter type Rocky does not know counts
/// as a warehouse.
fn is_warehouse(adapter: &rocky_config::AdapterConfig) -> bool {
    adapter.kind != Some(rocky_config::AdapterKind::Discovery)
        && rocky_core::adapter_capability::capability_for(&adapter.adapter_type)
            .is_none_or(|cap| cap.supports_data)
}

/// The warehouse adapters each model runs on, resolved from the pipelines
/// that actually target them — not from every configured adapter.
///
/// ```text
///   model ──(matches pipeline.<p>.models glob)──▶ transformation pipeline
///         ──(pipeline.<p>.target.adapter)──────▶ warehouse adapter
/// ```
///
/// - A model that a transformation pipeline's `models` glob loads runs on
///   that pipeline's target. Several pipelines can load one model; it then
///   has several targets.
/// - A model no transformation pipeline loads can still run through
///   `rocky run --models` on any pipeline, so it gets every pipeline's
///   target. With no pipeline at all, it gets every warehouse adapter.
/// - A pipeline whose model set cannot be read adds its target to every
///   model (fail closed).
///
/// The compile-time refusals (E049, E051, E053, E054, E042/E043) refuse a
/// model when ANY of its targets refuses it. An adapter that no pipeline
/// targets no longer hides a refusal.
struct ModelTargets<'c> {
    by_model: HashMap<String, Vec<&'c rocky_config::AdapterConfig>>,
    unclaimed: Vec<&'c rocky_config::AdapterConfig>,
    everywhere: Vec<&'c rocky_config::AdapterConfig>,
}

impl<'c> ModelTargets<'c> {
    fn resolve(config: &'c rocky_config::RockyConfig, config_path: &Path) -> Self {
        let warehouse = |name: &str| config.adapters.get(name).filter(|a| is_warehouse(a));
        let mut pipelines: Vec<(&String, &rocky_config::PipelineConfig)> =
            config.pipelines.iter().collect();
        pipelines.sort_by(|a, b| a.0.cmp(b.0));

        let mut by_model: HashMap<String, Vec<&'c rocky_config::AdapterConfig>> = HashMap::new();
        let mut everywhere = Vec::new();
        let mut unclaimed = Vec::new();
        for (_, pipeline) in &pipelines {
            let Some(adapter) = warehouse(pipeline.target_adapter()) else {
                continue;
            };
            push_unique(&mut unclaimed, adapter);
            let Some(tx) = pipeline.as_transformation() else {
                continue;
            };
            match pipeline_model_names(tx, config_path) {
                Some(names) => {
                    for name in names {
                        push_unique(by_model.entry(name).or_default(), adapter);
                    }
                }
                None => push_unique(&mut everywhere, adapter),
            }
        }
        if pipelines.is_empty() {
            let mut names: Vec<&String> = config.adapters.keys().collect();
            names.sort();
            for name in names {
                if let Some(adapter) = warehouse(name) {
                    push_unique(&mut unclaimed, adapter);
                }
            }
        }
        Self {
            by_model,
            unclaimed,
            everywhere,
        }
    }

    /// The warehouses `model` can run on. Empty when nothing is configured.
    fn for_model(&self, model: &str) -> Vec<&'c rocky_config::AdapterConfig> {
        match self.by_model.get(model) {
            Some(claimed) => self.with_everywhere(claimed.clone()),
            None => self.for_unlisted_model(),
        }
    }

    /// The warehouses a model no pipeline lists by name can run on.
    fn for_unlisted_model(&self) -> Vec<&'c rocky_config::AdapterConfig> {
        self.with_everywhere(self.unclaimed.clone())
    }

    fn with_everywhere(
        &self,
        mut out: Vec<&'c rocky_config::AdapterConfig>,
    ) -> Vec<&'c rocky_config::AdapterConfig> {
        for adapter in &self.everywhere {
            push_unique(&mut out, adapter);
        }
        out
    }
}

fn push_unique<'c>(
    list: &mut Vec<&'c rocky_config::AdapterConfig>,
    adapter: &'c rocky_config::AdapterConfig,
) {
    if !list.iter().any(|a| std::ptr::eq(*a, adapter)) {
        list.push(adapter);
    }
}

/// The names of the models a transformation pipeline loads, through the
/// same glob resolution `rocky run` uses. `None` when the set cannot be read.
fn pipeline_model_names(
    tx: &rocky_config::TransformationPipelineConfig,
    config_path: &Path,
) -> Option<Vec<String>> {
    match crate::models_loader::locate_models_dir(&tx.models, config_path).ok()? {
        crate::models_loader::ModelsDir::Absent(_) => Some(Vec::new()),
        crate::models_loader::ModelsDir::Present(dir) => {
            let glob = crate::models_loader::resolved_models_glob(&tx.models, config_path);
            let models =
                crate::models_loader::load_project_models_matching(&dir, &glob, None).ok()?;
            Some(models.into_iter().map(|m| m.config.name).collect())
        }
    }
}

/// The SCD2 snapshot refusal of one warehouse, if it has one.
fn snapshot_refusal(adapter: &rocky_config::AdapterConfig) -> Option<&'static str> {
    match crate::registry::postgres_dialect_for_config(adapter) {
        Some(dialect) => dialect.snapshot_unsupported_reason(),
        None => crate::registry::warehouse_dialect_for_type(&adapter.adapter_type)
            .and_then(rocky_core::traits::SqlDialect::snapshot_unsupported_reason),
    }
}

/// The upsert refusal of one warehouse, if it has one.
fn merge_refusal(adapter: &rocky_config::AdapterConfig) -> Option<&'static str> {
    match crate::registry::postgres_dialect_for_config(adapter) {
        Some(dialect) => dialect.merge_unsupported_reason(),
        None => crate::registry::warehouse_dialect_for_type(&adapter.adapter_type)
            .and_then(rocky_core::traits::SqlDialect::merge_unsupported_reason),
    }
}

/// E051 for every valid function a model calls when a warehouse that a
/// calling model runs on ([`ModelTargets`]) cannot create it: Trino, every
/// adapter with no function DDL (ClickHouse, SQL Server, an unknown type),
/// and PostgreSQL / Redshift when their `CREATE FUNCTION` rendering
/// ([`rocky_core::functions::create_function_sql`]) refuses this function —
/// a `[target] catalog` on Redshift, a body holding the dollar-quote
/// delimiter, an argument used in a qualified reference with no positional
/// spelling. Those refusals would otherwise surface only at `rocky run`.
/// SQL Server is refused, not rendered: a T-SQL scalar UDF takes
/// `@`-prefixed parameters and must be called schema-qualified
/// (`dbo.f(x)`), so a model's bare `f(x)` call would not resolve to it.
/// Fail closed: one refusing target is enough.
fn function_adapter_diagnostics(
    targets: &ModelTargets<'_>,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    use rocky_core::functions::{FunctionDialect, create_function_sql};
    let registry = result.semantic_graph.functions();
    if registry.is_empty() {
        return Vec::new();
    }
    // Why `name` cannot be created on warehouse `w`, or `None` when it can.
    // `Err(())` is "no function DDL at all"; `Ok(reason)` is the rendering's
    // own refusal.
    let refusal = |name: &str, w: &Option<FunctionDialect>| -> Option<Result<String, ()>> {
        match w {
            Some(FunctionDialect::Trino) | None => Some(Err(())),
            Some(
                FunctionDialect::DuckDb
                | FunctionDialect::Snowflake
                | FunctionDialect::Databricks
                | FunctionDialect::BigQuery,
            ) => None,
            Some(dialect @ (FunctionDialect::Postgres | FunctionDialect::Redshift)) => {
                let def = &registry.get(name)?.def;
                // An invalid definition already has its own E051.
                if !def.validation_problems().is_empty() {
                    return None;
                }
                create_function_sql(def, *dialect)
                    .err()
                    .map(|e| Ok(e.to_string()))
            }
        }
    };
    let usage = rocky_compiler::udf::function_usage(&result.project.models, registry);
    let mut names: Vec<&String> = usage.keys().collect();
    names.sort();
    names
        .into_iter()
        .filter_map(|name| {
            let mut warehouses: Vec<&rocky_config::AdapterConfig> = Vec::new();
            for caller in &usage[name] {
                for adapter in targets.for_model(caller) {
                    push_unique(&mut warehouses, adapter);
                }
            }
            let mut types: Vec<&str> = Vec::new();
            let mut rendered: Vec<String> = Vec::new();
            for adapter in warehouses {
                let dialect = FunctionDialect::from_dialect_name(&adapter.adapter_type);
                match refusal(name, &dialect) {
                    None => {}
                    Some(reason) => {
                        types.push(adapter.adapter_type.as_str());
                        if let Ok(reason) = reason
                            && !rendered.contains(&reason)
                        {
                            rendered.push(reason);
                        }
                    }
                }
            }
            if types.is_empty() {
                return None;
            }
            types.sort_unstable();
            types.dedup();
            let types = types.join(", ");
            let diagnostic = if rendered.is_empty() {
                Diagnostic::error(
                    diagnostic::E051,
                    name,
                    format!(
                        "function `{name}` cannot be created: Rocky cannot create persistent \
                         user-defined functions on the configured warehouse ({types})"
                    ),
                )
                .with_suggestion(
                    "inline the expression in the calling models, or create the routine \
                     outside Rocky",
                )
            } else {
                Diagnostic::error(
                    diagnostic::E051,
                    name,
                    format!(
                        "function `{name}` cannot be created on the configured warehouse \
                         ({types}): {}",
                        rendered.join("; ")
                    ),
                )
                .with_suggestion("change the function definition as the message says")
            };
            Some(diagnostic)
        })
        .collect()
}

/// E049 for every snapshot model when a warehouse it runs on
/// ([`ModelTargets`]) cannot run the SCD2 snapshot SQL
/// ([`rocky_core::traits::SqlDialect::snapshot_unsupported_reason`]):
/// PostgreSQL under `merge_mode = "on_conflict"`, Redshift, SQL Server.
/// Fail closed: one refusing target is enough.
fn snapshot_adapter_diagnostics(
    targets: &ModelTargets<'_>,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    result
        .project
        .models
        .iter()
        .filter(|m| {
            matches!(
                m.config.strategy,
                rocky_core::models::StrategyConfig::Snapshot { .. }
            )
        })
        .filter_map(|m| {
            let (adapter_type, reason) = targets
                .for_model(&m.config.name)
                .into_iter()
                .find_map(|a| snapshot_refusal(a).map(|r| (a.adapter_type.clone(), r)))?;
            Some(
                Diagnostic::error(
                    diagnostic::E049,
                    &m.config.name,
                    format!(
                        "snapshot model `{}` cannot run on the configured {adapter_type} \
                         warehouse: {reason}",
                        m.config.name
                    ),
                )
                .with_suggestion(
                    "use a warehouse that supports MERGE (PostgreSQL 15+ with merge_mode = \
                     \"merge\"), or change the model's strategy",
                ),
            )
        })
        .collect()
}

/// E053 for every model that updates rows by key — `merge`, or `incremental`
/// with a `unique_key` — when a warehouse it runs on ([`ModelTargets`]) has
/// no upsert to render it with
/// ([`rocky_core::traits::SqlDialect::merge_unsupported_reason`]):
/// ClickHouse. Fail closed: one refusing target is enough.
fn merge_adapter_diagnostics(
    targets: &ModelTargets<'_>,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    result
        .project
        .models
        .iter()
        .filter_map(|m| {
            let kind = match &m.config.strategy {
                rocky_core::models::StrategyConfig::Merge { .. } => "merge",
                rocky_core::models::StrategyConfig::Incremental { unique_key, .. }
                    if !unique_key.is_empty() =>
                {
                    "incremental with a unique_key"
                }
                _ => return None,
            };
            let (adapter_type, reason) = targets
                .for_model(&m.config.name)
                .into_iter()
                .find_map(|a| merge_refusal(a).map(|r| (a.adapter_type.clone(), r)))?;
            Some(
                Diagnostic::error(
                    diagnostic::E053,
                    &m.config.name,
                    format!(
                        "model `{}` ({kind}) cannot run on the configured {adapter_type} \
                         warehouse: {reason}",
                        m.config.name
                    ),
                )
                .with_suggestion(
                    "use `delete_insert` (replace rows by partition key), `incremental` without \
                     a unique_key (append), or `full_refresh`",
                ),
            )
        })
        .collect()
}

/// E054 for every model whose SQL (ephemeral upstreams already inlined)
/// SQL Server cannot run because its CTEs cannot be lifted to one leading
/// `WITH` ([`rocky_sqlserver::tsql::hoist_ctes`]), when a warehouse the model
/// runs on ([`ModelTargets`]) is SQL Server. Fail closed.
fn sqlserver_cte_diagnostics(
    targets: &ModelTargets<'_>,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    result
        .project
        .models
        .iter()
        .filter(|m| {
            !matches!(
                m.config.strategy,
                rocky_core::models::StrategyConfig::Ephemeral
            )
        })
        .filter(|m| {
            targets
                .for_model(&m.config.name)
                .iter()
                .any(|a| a.adapter_type == "sqlserver")
        })
        .filter(|m| rocky_sqlserver::tsql::hoist_ctes(&m.sql).is_none())
        .map(|m| {
            Diagnostic::error(
                diagnostic::E054,
                &m.config.name,
                format!(
                    "model `{}` cannot run on SQL Server: T-SQL accepts `WITH` only at the \
                     start of a statement, and Rocky cannot lift this model's CTEs there \
                     without changing what a name refers to",
                    m.config.name
                ),
            )
            .with_suggestion(
                "give each CTE a distinct name that no table, column or alias in the model \
                 also uses, or move nested `WITH` clauses to the top of the model",
            )
        })
        .collect()
}

/// Whether every warehouse `model` runs on is PostgreSQL (and there is at
/// least one). Redshift does not count: it has no functional-dependence rule.
fn runs_only_on_postgres(targets: &ModelTargets<'_>, model: &str) -> bool {
    let adapters = targets.for_model(model);
    !adapters.is_empty() && adapters.iter().all(|a| a.adapter_type == "postgres")
}

/// Extra data the `rocky compile` text renderer needs from the raw
/// `CompileResult` but which is intentionally not carried on the
/// `JsonSchema`-backed [`CompileOutput`].
///
/// Internal-only — never serialized — so it can change freely without
/// touching the JSON schema cascade.
struct CompileTextData {
    /// Model names in execution (topological) order.
    execution_order: Vec<String>,
    /// Per-model count of resolved typed columns.
    typed_column_counts: HashMap<String, usize>,
    /// `file_path -> model SQL`, used so miette can render source spans.
    source_map: HashMap<String, String>,
}

/// Side-effect-free core of `rocky compile`: compile the project, run the
/// portability lint, expand macros, compute cost ceilings, and assemble the
/// typed [`CompileOutput`].
///
/// Does no stdout printing and does not bail on compilation errors — the
/// `has_errors` flag rides on the returned struct so an in-process caller can
/// inspect the diagnostics. The `run_compile` wrapper prints and bails.
///
/// `cache_ttl_override`: optional CLI `--cache-ttl <seconds>` value that
/// replaces `[cache.schemas] ttl_seconds` for this invocation only.
// Reusable typed-output core for the in-process MCP server (`rocky-mcp`). The
// `run_compile` wrapper uses `compile_inner` directly so it can also render
// text mode.
#[allow(clippy::too_many_arguments)]
pub fn compile_output(
    config_path: Option<&Path>,
    state_path: &Path,
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    with_seed: bool,
    cache_ttl_override: Option<u64>,
) -> Result<CompileOutput> {
    let (output, _text_data) = compile_inner(
        config_path,
        state_path,
        models_dir,
        ModelScope::Dir,
        contracts_dir,
        model_filter,
        do_expand_macros,
        target_dialect,
        // These callers compile on request; only an explicit `with_seed`
        // runs the project's seed SQL.
        if with_seed {
            SeedUse::Required
        } else {
            SeedUse::Never
        },
        cache_ttl_override,
        // `compile_output` backs commands that don't expose `--var`
        // (ci / dag); an `@var()` model would surface an E028 diagnostic.
        &rocky_core::run_vars::RunVars::new(),
        // No `--strict-sources` flag on these surfaces; `[cache.schemas]
        // strict_sources` still applies.
        false,
        // Likewise `[contracts] strict` still applies.
        false,
        None,
    )?;
    Ok(output)
}

/// Render the text-mode output for `rocky compile`. Mirrors the historical
/// inline `else` branch byte-for-byte.
fn render_compile_text(output: &CompileOutput, text_data: &CompileTextData) {
    let error_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    let warning_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Warning)
        .count();

    // Print model status
    for model_name in &text_data.execution_order {
        let model_diags: Vec<_> = output
            .diagnostics
            .iter()
            .filter(|d| d.model == *model_name)
            .collect();
        let has_model_errors = model_diags.iter().any(|d| d.severity == Severity::Error);

        if has_model_errors {
            println!("  \u{2717} {model_name}");
        } else {
            let col_count = text_data
                .typed_column_counts
                .get(model_name)
                .copied()
                .unwrap_or(0);
            println!("  \u{2713} {model_name} ({col_count} columns)");
        }
    }

    // Print expanded SQL when --expand-macros is set (text mode).
    if !output.expanded_sql.is_empty() {
        println!();
        for model_name in &text_data.execution_order {
            if let Some(sql) = output.expanded_sql.get(model_name) {
                println!("  -- {model_name} (expanded)");
                for line in sql.lines() {
                    println!("  {line}");
                }
                println!();
            }
        }
    }

    // Render diagnostics with miette (rich source spans when available)
    if !output.diagnostics.is_empty() {
        let rendered = diagnostic::render_diagnostics(&output.diagnostics, &text_data.source_map);
        print!("{rendered}");
    }

    println!(
        "  Compiled: {} models, {} errors, {} warnings",
        output.models, error_count, warning_count,
    );
}

/// Build an error-severity P001 diagnostic from a portability issue.
///
/// `file_path` is threaded into `SourceSpan` so the diagnostic shows up
/// against the model file. We don't yet track per-construct byte offsets,
/// so the span defaults to line 1 — wave 2 can sharpen this.
fn build_p001_diagnostic(
    model_name: &str,
    file_path: &str,
    issue: &PortabilityIssue,
) -> Diagnostic {
    let supported = issue
        .supported_by
        .iter()
        .map(Dialect::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "{} is not portable to {} (supported by: {})",
        issue.construct, issue.target, supported,
    );
    Diagnostic::error("P001", model_name, message)
        .with_span(diagnostic::SourceSpan {
            file: file_path.to_string(),
            line: 1,
            col: 1,
        })
        .with_suggestion(issue.suggestion.clone())
}

/// Load source schemas from a project's `data/seed.sql` by running it
/// against an in-memory DuckDB.
///
/// Resolution: walks up from `models_dir` looking for `data/seed.sql`. The
/// playground convention is `<project>/models/` + `<project>/data/seed.sql`,
/// so the parent of `models_dir` is the standard place to look.
///
/// On any failure (missing file, seed-execution error, information_schema
/// query error) returns an `anyhow` error with the full chain — surfaces
/// to the user via the standard CLI error path. Silent fall-through would
/// be worse here than a hard fail because the user explicitly opted in
/// via `--with-seed`.
///
/// Result keys are `"<schema>.<table>"` — the same shape the SQL lineage
/// extractor produces from a model's `FROM <schema>.<table>` clause, so
/// the typecheck `typed_models` injection in
/// `rocky-compiler/src/typecheck.rs:152` lands the type info on the path
/// the producing-edge lookup walks.
pub(crate) fn load_source_schemas_from_seed(
    models_dir: &Path,
) -> Result<HashMap<String, Vec<TypedColumn>>> {
    load_source_schemas_from_seed_at(models_dir.parent().unwrap_or(Path::new(".")))
}

/// Fill `schemas` with the tables of the project's seed file that it does
/// not already hold, recorded as [`SourceSchemaOrigin::Seed`].
///
/// The default source-schema tier of `rocky compile`: with no `--with-seed`,
/// a project that ships `data/seed.sql` is still typed from it, so a model
/// that reads a column the seed's tables lack gets W041 with no flag. A
/// schema-cache entry wins over a seed table of the same name: it describes
/// the warehouse.
///
/// Never fails the compile. The user did not ask for the seed, so a seed
/// that does not run is logged and skipped, and the compile goes on with the
/// schemas it had.
fn merge_default_seed_schemas(
    project_root: &Path,
    schemas: &mut HashMap<String, Vec<TypedColumn>>,
    provenance: &mut SourceProvenance,
) {
    // A build without DuckDB cannot run a seed; the default tier is silent.
    if cfg!(not(feature = "duckdb")) || !seed_file(project_root).is_file() {
        return;
    }
    match load_source_schemas_from_seed_at(project_root) {
        Ok(seed) => {
            for (key, columns) in seed {
                if schemas.contains_key(&key) {
                    continue;
                }
                provenance
                    .origins
                    .insert(key.clone(), SourceSchemaOrigin::Seed);
                schemas.insert(key, columns);
            }
        }
        Err(e) => tracing::warn!(
            error = %format!("{e:#}"),
            "the seed file did not run; compiling without its source schemas"
        ),
    }
}

fn seed_file(project_root: &Path) -> std::path::PathBuf {
    project_root.join("data").join("seed.sql")
}

/// [`load_source_schemas_from_seed`] for the seed at
/// `<project_root>/data/seed.sql`.
#[cfg(feature = "duckdb")]
pub(crate) fn load_source_schemas_from_seed_at(
    project_root: &Path,
) -> Result<HashMap<String, Vec<TypedColumn>>> {
    use anyhow::Context;
    use rocky_duckdb::DuckDbConnector;

    let seed_path = seed_file(project_root);
    if !seed_path.is_file() {
        anyhow::bail!(
            "--with-seed requested but no seed file found at {}",
            seed_path.display()
        );
    }

    let seed_sql = std::fs::read_to_string(&seed_path)
        .with_context(|| format!("failed to read seed file: {}", seed_path.display()))?;

    // In-memory DuckDB; dropped at end of fn so no temp files leak.
    let conn = DuckDbConnector::in_memory()
        .map_err(|e| anyhow::anyhow!("failed to open in-memory DuckDB for --with-seed: {e}"))?;
    conn.execute_statement(&seed_sql)
        .map_err(|e| anyhow::anyhow!("seed execution failed for {}: {e}", seed_path.display()))?;

    // The same derivation `rocky test` and `rocky ci` type their compile
    // from, on the database they then execute in.
    rocky_engine::test_runner::source_schemas_from_db(&conn)
}

/// Stub used when the binary is built without the `duckdb` feature. The
/// flag exists in the clap definition unconditionally so feature-stripped
/// builds give a clear error rather than a silent no-op.
#[cfg(not(feature = "duckdb"))]
pub(crate) fn load_source_schemas_from_seed_at(
    _project_root: &Path,
) -> Result<HashMap<String, Vec<TypedColumn>>> {
    anyhow::bail!("--with-seed requires the `duckdb` feature; rebuild with `--features duckdb`");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn write_model(dir: &Path, name: &str, sql: &str) {
        let sql_path = dir.join(format!("{name}.sql"));
        let toml_path = dir.join(format!("{name}.toml"));
        fs::write(&sql_path, sql).unwrap();
        fs::write(
            &toml_path,
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    /// Write a minimal `rocky.toml` next to a models dir, with optional
    /// `[portability]` section content. Returns the path so tests can pass
    /// it as the new `config_path` argument. Uses the shape committed in
    /// `examples/playground/pocs/00-foundations/00-playground-default/rocky.toml`.
    fn write_rocky_toml(dir: &Path, portability_block: &str) -> std::path::PathBuf {
        let path = dir.join("rocky.toml");
        let body = format!(
            r#"[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.p]
strategy = "full_refresh"

[pipeline.p.source.discovery]
adapter = "default"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "c"
schema_template = "s"

{portability_block}
"#
        );
        fs::write(&path, body).unwrap();
        path
    }

    /// A transformation project rooted at `root` with the given `[adapter.*]`
    /// blocks. Returns the config path.
    fn adapter_project(root: &Path, adapters: &str) -> std::path::PathBuf {
        fs::create_dir_all(root.join("models")).unwrap();
        let path = root.join("rocky.toml");
        fs::write(
            &path,
            format!(
                "{adapters}\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 target = {{ adapter = \"wh\" }}\n"
            ),
        )
        .unwrap();
        path
    }

    fn compile_codes(root: &Path, config: &Path) -> Vec<(String, String)> {
        let out = compile_output(
            Some(config),
            &root.join("state.redb"),
            &root.join("models"),
            None,
            None,
            false,
            None,
            false,
            None,
        )
        .unwrap();
        out.diagnostics
            .iter()
            .map(|d| (d.code.to_string(), d.model.clone()))
            .collect()
    }

    const PG: &str = "[adapter.wh]\ntype = \"postgres\"\nhost = \"localhost\"\n\
                      database = \"d\"\nusername = \"u\"\npassword = \"x\"\n";

    fn udf_project(root: &Path, adapters: &str) -> std::path::PathBuf {
        let config = adapter_project(root, adapters);
        fs::create_dir_all(root.join("functions")).unwrap();
        fs::write(
            root.join("functions/dbl.toml"),
            "returns = \"DOUBLE\"\n\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n",
        )
        .unwrap();
        fs::write(root.join("functions/dbl.sql"), "x * 2\n").unwrap();
        write_model(&root.join("models"), "uf", "SELECT dbl(1.0) AS a2");
        config
    }

    const TRINO: &str = "[adapter.wh]\ntype = \"trino\"\nhost = \"localhost\"\n";

    /// Trino cannot create persistent functions. With no other warehouse
    /// configured, a UDF is refused at compile time (E051) instead of
    /// failing mid-run.
    #[test]
    fn udf_on_warehouse_without_function_ddl_is_e051() {
        for adapters in [
            TRINO.to_string(),
            PG.replace("postgres", "sqlserver"),
            // A discovery-only adapter is not a warehouse.
            format!(
                "{TRINO}\n[adapter.src]\ntype = \"fivetran\"\nkind = \"discovery\"\n\
                     destination_id = \"d\"\napi_key = \"k\"\napi_secret = \"s\"\n"
            ),
        ] {
            let dir = TempDir::new().unwrap();
            let config = udf_project(dir.path(), &adapters);
            let codes = compile_codes(dir.path(), &config);
            assert!(
                codes.contains(&("E051".to_string(), "dbl".to_string())),
                "{adapters}: {codes:?}"
            );
        }
    }

    /// PostgreSQL and Redshift create SQL functions, so a project whose
    /// only warehouse is one of them compiles clean.
    #[test]
    fn udf_on_postgres_or_redshift_is_not_e051() {
        for adapters in [PG.to_string(), PG.replace("postgres", "redshift")] {
            let dir = TempDir::new().unwrap();
            let config = udf_project(dir.path(), &adapters);
            let codes = compile_codes(dir.path(), &config);
            assert!(
                !codes.iter().any(|(c, _)| c == "E051"),
                "{adapters}: {codes:?}"
            );
        }
    }

    /// PostgreSQL / Redshift render the function's DDL at compile time, so a
    /// definition their rendering refuses is E051 now rather than at run:
    /// a Redshift `[target] catalog`, a body with the dollar-quote
    /// delimiter, an argument in a qualified reference. A capable adapter
    /// beside them that no pipeline targets does not hide the refusal.
    #[test]
    fn udf_refused_by_postgres_or_redshift_rendering_is_e051() {
        let rs = PG.replace("postgres", "redshift");
        let cases: [(&str, &str, &str, &str); 4] = [
            (
                rs.as_str(),
                "[target]\ncatalog = \"c\"\nschema = \"s\"\n",
                "x * 2",
                "catalog",
            ),
            (rs.as_str(), "", "x || '$$'", "$$"),
            (rs.as_str(), "", "x.field", "qualified"),
            (PG, "", "x || '$rocky$'", "$rocky$"),
        ];
        for (adapters, target, body, needle) in cases {
            let dir = TempDir::new().unwrap();
            let config = udf_project(dir.path(), adapters);
            fs::write(
                dir.path().join("functions/dbl.toml"),
                format!(
                    "returns = \"DOUBLE\"\n\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n\n{target}"
                ),
            )
            .unwrap();
            fs::write(dir.path().join("functions/dbl.sql"), body).unwrap();
            let out = compile_output(
                Some(&config),
                &dir.path().join("state.redb"),
                &dir.path().join("models"),
                None,
                None,
                false,
                None,
                false,
                None,
            )
            .unwrap();
            let e051: Vec<_> = out
                .diagnostics
                .iter()
                .filter(|d| &*d.code == "E051" && d.model == "dbl")
                .collect();
            assert_eq!(e051.len(), 1, "{body}: {:?}", out.diagnostics);
            assert!(
                e051[0].message.contains(needle),
                "{body}: {}",
                e051[0].message
            );

            // A DuckDB adapter that no pipeline targets does not hide the
            // refusal: the calling model still runs on the refusing target.
            let dir = TempDir::new().unwrap();
            let both =
                format!("{adapters}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
            let config = udf_project(dir.path(), &both);
            fs::write(
                dir.path().join("functions/dbl.toml"),
                format!(
                    "returns = \"DOUBLE\"\n\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n\n{target}"
                ),
            )
            .unwrap();
            fs::write(dir.path().join("functions/dbl.sql"), body).unwrap();
            let codes = compile_codes(dir.path(), &config);
            assert!(
                codes.contains(&("E051".to_string(), "dbl".to_string())),
                "{body}: {codes:?}"
            );
        }
    }

    /// A capable adapter that no pipeline targets does not hide the
    /// refusal: the pipeline runs the model on Trino.
    #[test]
    fn udf_with_an_unused_capable_adapter_is_still_e051() {
        let dir = TempDir::new().unwrap();
        let adapters =
            format!("{TRINO}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
        let config = udf_project(dir.path(), &adapters);
        let codes = compile_codes(dir.path(), &config);
        assert!(
            codes.contains(&("E051".to_string(), "dbl".to_string())),
            "{codes:?}"
        );
    }

    fn snapshot_project(root: &Path, adapters: &str) -> std::path::PathBuf {
        let config = adapter_project(root, adapters);
        let models = root.join("models");
        fs::write(
            models.join("snap.sql"),
            "SELECT 1 AS id, CAST('2024-01-01' AS TIMESTAMP) AS updated_at",
        )
        .unwrap();
        fs::write(
            models.join("snap.toml"),
            "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\n\
             updated_at = \"updated_at\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        )
        .unwrap();
        config
    }

    /// PostgreSQL under `merge_mode = "on_conflict"` and Redshift cannot run
    /// the SCD2 snapshot MERGE: compile refuses the snapshot model (E049).
    #[test]
    fn snapshot_on_warehouse_without_merge_is_e049() {
        for adapters in [
            format!("{PG}\n[adapter.wh.extra]\nmerge_mode = \"on_conflict\"\n"),
            PG.replace("postgres", "redshift"),
            PG.replace("postgres", "sqlserver"),
        ] {
            let dir = TempDir::new().unwrap();
            let config = snapshot_project(dir.path(), &adapters);
            let codes = compile_codes(dir.path(), &config);
            assert!(
                codes.contains(&("E049".to_string(), "snap".to_string())),
                "{adapters}: {codes:?}"
            );
        }
        // PostgreSQL 15+ (the default `merge_mode = "merge"`) runs them.
        let dir = TempDir::new().unwrap();
        let config = snapshot_project(dir.path(), PG);
        let codes = compile_codes(dir.path(), &config);
        assert!(!codes.iter().any(|(c, _)| c == "E049"), "{codes:?}");
    }

    const CH: &str = "[adapter.wh]\ntype = \"clickhouse\"\nhost = \"localhost\"\n";

    /// A ClickHouse-only project refuses every model that updates rows by key
    /// (E053), refuses snapshots (E049) and UDFs (E051), and leaves the
    /// strategies ClickHouse runs alone.
    #[test]
    fn clickhouse_refuses_merge_snapshot_and_udf_only() {
        let dir = TempDir::new().unwrap();
        let config = snapshot_project(dir.path(), CH);
        let models = dir.path().join("models");
        let model = |name: &str, toml: &str| {
            fs::write(models.join(format!("{name}.sql")), "SELECT 1 AS id, 2 AS v").unwrap();
            fs::write(
                models.join(format!("{name}.toml")),
                format!("{toml}\n[target]\ncatalog = \"\"\nschema = \"s\"\n"),
            )
            .unwrap();
        };
        model(
            "m_merge",
            "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n",
        );
        model(
            "m_upsert",
            "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"id\"\n\
             unique_key = [\"id\"]\n",
        );
        model("m_full", "[strategy]\ntype = \"full_refresh\"\n");
        model(
            "m_di",
            "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"id\"]\n",
        );
        model("m_view", "[strategy]\ntype = \"view\"\n");
        let codes = compile_codes(dir.path(), &config);
        for name in ["m_merge", "m_upsert"] {
            assert!(
                codes.contains(&("E053".to_string(), name.to_string())),
                "{name}: {codes:?}"
            );
        }
        for name in ["m_full", "m_di", "m_view"] {
            assert!(
                !codes.iter().any(|(c, m)| c.starts_with('E') && m == name),
                "{name} must compile clean: {codes:?}"
            );
        }
        assert!(
            codes.contains(&("E049".to_string(), "snap".to_string())),
            "{codes:?}"
        );

        let dir = TempDir::new().unwrap();
        let config = udf_project(dir.path(), CH);
        let codes = compile_codes(dir.path(), &config);
        assert!(
            codes.contains(&("E051".to_string(), "dbl".to_string())),
            "{codes:?}"
        );
    }

    const SS: &str = "[adapter.wh]\ntype = \"sqlserver\"\nhost = \"localhost\"\n\
                      database = \"an\"\nusername = \"u\"\npassword = \"x\"\n";

    /// Two ephemeral upstreams that each end in `WITH final AS …`, read by a
    /// consumer with its own `final`, lift on SQL Server (the nested names
    /// are renamed in scope). A nested CTE named like a column the outer
    /// query reads cannot be lifted: E054 when the pipeline targets SQL Server,
    /// even with an unused DuckDB adapter configured beside it.
    #[test]
    fn sqlserver_cte_lifting_is_checked_at_compile() {
        let write = |models: &Path, name: &str, sql: &str, strategy: &str| {
            fs::write(models.join(format!("{name}.sql")), sql).unwrap();
            fs::write(
                models.join(format!("{name}.toml")),
                format!(
                    "{strategy}\n[strategy]\ntype = \"{}\"\n\n[target]\ncatalog = \"an\"\nschema = \"marts\"\n",
                    if strategy.is_empty() { "full_refresh" } else { "ephemeral" }
                ),
            )
            .unwrap();
        };
        let project = |root: &Path, adapters: &str| {
            let config = adapter_project(root, adapters);
            let models = root.join("models");
            write(&models, "raw", "SELECT 1 AS id, 2 AS v", "");
            let eph = "depends_on = [\"raw\"]";
            write(
                &models,
                "stg_a",
                "WITH final AS (SELECT id, v FROM raw) SELECT * FROM final",
                eph,
            );
            write(
                &models,
                "stg_b",
                "WITH final AS (SELECT id, v + 1 AS w FROM raw) SELECT * FROM final",
                eph,
            );
            fs::write(
                models.join("fct.sql"),
                "WITH final AS (SELECT a.id, a.v, b.w FROM stg_a AS a JOIN stg_b AS b ON a.id = b.id) \
                 SELECT * FROM final",
            )
            .unwrap();
            fs::write(
                models.join("fct.toml"),
                "depends_on = [\"stg_a\", \"stg_b\"]\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"an\"\nschema = \"marts\"\n",
            )
            .unwrap();
            fs::write(
                models.join("bad.sql"),
                "SELECT v FROM (WITH v AS (SELECT id AS v FROM raw) SELECT v FROM v) AS s",
            )
            .unwrap();
            fs::write(
                models.join("bad.toml"),
                "depends_on = [\"raw\"]\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"an\"\nschema = \"marts\"\n",
            )
            .unwrap();
            config
        };

        let dir = TempDir::new().unwrap();
        let config = project(dir.path(), SS);
        let codes = compile_codes(dir.path(), &config);
        let e054: Vec<&str> = codes
            .iter()
            .filter(|(c, _)| c == "E054")
            .map(|(_, m)| m.as_str())
            .collect();
        assert_eq!(e054, vec!["bad"], "{codes:?}");

        let dir = TempDir::new().unwrap();
        let adapters = format!("{SS}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
        let config = project(dir.path(), &adapters);
        let codes = compile_codes(dir.path(), &config);
        let e054: Vec<&str> = codes
            .iter()
            .filter(|(c, _)| c == "E054")
            .map(|(_, m)| m.as_str())
            .collect();
        assert_eq!(e054, vec!["bad"], "{codes:?}");
    }

    /// The red-team repro: the pipeline targets ClickHouse and a DuckDB
    /// adapter sits unused beside it. The DuckDB adapter used to silence
    /// E053, and `rocky run` then failed with "ClickHouse has no MERGE".
    #[test]
    fn clickhouse_merge_with_an_unused_capable_adapter_is_still_e053() {
        let dir = TempDir::new().unwrap();
        let adapters = format!("{CH}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
        let config = adapter_project(dir.path(), &adapters);
        let models = dir.path().join("models");
        fs::write(models.join("m.sql"), "SELECT 1 AS id").unwrap();
        fs::write(
            models.join("m.toml"),
            "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n\n[target]\ncatalog = \"\"\nschema = \"s\"\n",
        )
        .unwrap();
        let codes = compile_codes(dir.path(), &config);
        assert!(
            codes.contains(&("E053".to_string(), "m".to_string())),
            "{codes:?}"
        );
    }

    /// A project with two transformation pipelines: `models/duck/**` runs on
    /// DuckDB (`[adapter.local]`), `models/other/**` on the `wh` adapter.
    fn split_project(root: &Path, wh: &str) -> std::path::PathBuf {
        fs::create_dir_all(root.join("models/duck")).unwrap();
        fs::create_dir_all(root.join("models/other")).unwrap();
        let path = root.join("rocky.toml");
        fs::write(
            &path,
            format!(
                "{wh}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n\n\
                 [pipeline.duck]\ntype = \"transformation\"\nmodels = \"models/duck/**\"\n\
                 target = {{ adapter = \"local\" }}\n\n\
                 [pipeline.other]\ntype = \"transformation\"\nmodels = \"models/other/**\"\n\
                 target = {{ adapter = \"wh\" }}\n"
            ),
        )
        .unwrap();
        path
    }

    fn write_strategy_model(dir: &Path, name: &str, sql: &str, strategy: &str) {
        fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        fs::write(
            dir.join(format!("{name}.toml")),
            format!("{strategy}\n[target]\ncatalog = \"\"\nschema = \"s\"\n"),
        )
        .unwrap();
    }

    fn codes_of<'a>(codes: &'a [(String, String)], code: &str) -> Vec<&'a str> {
        codes
            .iter()
            .filter(|(c, _)| c == code)
            .map(|(_, m)| m.as_str())
            .collect()
    }

    /// Each model is judged against the warehouse its own pipeline targets:
    /// the model in the DuckDB pipeline is not refused, the same shape in
    /// the SQL Server / ClickHouse pipeline is.
    #[test]
    fn adapter_gates_follow_each_models_pipeline_target() {
        // SQL Server: E054 (CTE lifting) and E049 (snapshot) only on its side.
        let dir = TempDir::new().unwrap();
        let config = split_project(dir.path(), SS);
        let bad_cte = "SELECT v FROM (WITH v AS (SELECT 1 AS v) SELECT v FROM v) AS s";
        let snap = "SELECT 1 AS id, CAST('2024-01-01' AS TIMESTAMP) AS updated_at";
        let snap_strategy = "[strategy]\ntype = \"snapshot\"\nunique_key = \"id\"\n\
                             strategy = \"timestamp\"\nupdated_at = \"updated_at\"\n";
        let fr = "[strategy]\ntype = \"full_refresh\"\n";
        let duck = dir.path().join("models/duck");
        let other = dir.path().join("models/other");
        write_strategy_model(&duck, "cte_duck", bad_cte, fr);
        write_strategy_model(&other, "cte_ss", bad_cte, fr);
        write_strategy_model(&duck, "snap_duck", snap, snap_strategy);
        write_strategy_model(&other, "snap_ss", snap, snap_strategy);
        let codes = compile_codes(dir.path(), &config);
        assert_eq!(codes_of(&codes, "E054"), vec!["cte_ss"], "{codes:?}");
        assert_eq!(codes_of(&codes, "E049"), vec!["snap_ss"], "{codes:?}");

        // ClickHouse: E053 only on its side.
        let dir = TempDir::new().unwrap();
        let config = split_project(dir.path(), CH);
        let merge = "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n";
        let duck = dir.path().join("models/duck");
        let other = dir.path().join("models/other");
        write_strategy_model(&duck, "m_duck", "SELECT 1 AS id", merge);
        write_strategy_model(&other, "m_ch", "SELECT 1 AS id", merge);
        let codes = compile_codes(dir.path(), &config);
        assert_eq!(codes_of(&codes, "E053"), vec!["m_ch"], "{codes:?}");
    }

    /// A model that two pipelines load runs on both targets; one refusing
    /// target refuses it (fail closed).
    #[test]
    fn a_model_on_two_targets_is_refused_when_either_refuses() {
        let dir = TempDir::new().unwrap();
        fs::create_dir_all(dir.path().join("models")).unwrap();
        let config = dir.path().join("rocky.toml");
        fs::write(
            &config,
            format!(
                "{CH}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n\n\
                 [pipeline.a]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 target = {{ adapter = \"local\" }}\n\n\
                 [pipeline.b]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 target = {{ adapter = \"wh\" }}\n"
            ),
        )
        .unwrap();
        write_strategy_model(
            &dir.path().join("models"),
            "m",
            "SELECT 1 AS id",
            "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\n",
        );
        let codes = compile_codes(dir.path(), &config);
        assert_eq!(codes_of(&codes, "E053"), vec!["m"], "{codes:?}");
    }

    fn compile_in(root: &Path, config: &Path) -> CompileOutput {
        compile_output(
            Some(config),
            &root.join("state.redb"),
            &root.join("models"),
            None,
            None,
            false,
            None,
            false,
            None,
        )
        .unwrap()
    }

    /// A GROUP BY that reads a non-grouped column: E044 everywhere but
    /// PostgreSQL, which accepts it when the column depends on a grouped
    /// primary key (W044). Redshift has no such rule and keeps E044.
    #[test]
    fn group_by_on_postgres_is_w044_and_on_redshift_stays_e044() {
        let group = |adapters: &str| {
            let dir = TempDir::new().unwrap();
            let config = adapter_project(dir.path(), adapters);
            let models = dir.path().join("models");
            write_model(&models, "raw", "SELECT 1 AS id, 'x' AS status, 2 AS amount");
            write_model(
                &models,
                "agg",
                "SELECT id, status, SUM(amount) AS total FROM raw GROUP BY id",
            );
            let out = compile_in(dir.path(), &config);
            let found: Vec<(String, Severity)> = out
                .diagnostics
                .iter()
                .filter(|d| d.model == "agg" && (&*d.code == "E044" || &*d.code == "W044"))
                .map(|d| (d.code.to_string(), d.severity))
                .collect();
            (found, out.has_errors)
        };
        let (pg, pg_errors) = group(PG);
        assert_eq!(pg, vec![("W044".to_string(), Severity::Warning)]);
        assert!(!pg_errors, "W044 alone must not fail the compile");
        let (rs, rs_errors) = group(&PG.replace("postgres", "redshift"));
        assert_eq!(rs, vec![("E044".to_string(), Severity::Error)]);
        assert!(rs_errors);
        let (duck, _) = group("[adapter.wh]\ntype = \"duckdb\"\npath = \":memory:\"\n");
        assert_eq!(duck, vec![("E044".to_string(), Severity::Error)]);
    }

    /// The operand checks judge each model against its pipeline's target:
    /// `SUM(text)` is E042 on PostgreSQL (no implicit text cast), and a
    /// ClickHouse target says it has no rules rather than "no target
    /// dialect configured".
    #[test]
    fn operand_checks_use_the_models_target_dialect() {
        let sum_text = |adapters: &str| {
            let dir = TempDir::new().unwrap();
            let config = adapter_project(dir.path(), adapters);
            fs::create_dir_all(dir.path().join("data")).unwrap();
            fs::write(
                dir.path().join("data/seed.sql"),
                "CREATE SCHEMA raw; CREATE TABLE raw.t (id INTEGER, status VARCHAR);",
            )
            .unwrap();
            write_model(
                &dir.path().join("models"),
                "agg",
                "SELECT SUM(status) AS s FROM raw.t",
            );
            compile_output(
                Some(&config),
                &dir.path().join("state.redb"),
                &dir.path().join("models"),
                None,
                None,
                false,
                None,
                true,
                None,
            )
            .unwrap()
            .diagnostics
            .into_iter()
            .filter(|d| &*d.code == "E042" || &*d.code == "W042")
            .map(|d| (d.code.to_string(), d.message.to_string()))
            .collect::<Vec<_>>()
        };
        let pg = sum_text(PG);
        assert_eq!(pg.len(), 1, "{pg:?}");
        assert_eq!(pg[0].0, "E042", "{pg:?}");
        assert!(pg[0].1.contains("PostgreSQL"), "{pg:?}");
        let ch = sum_text(CH);
        assert_eq!(ch.len(), 1, "{ch:?}");
        assert_eq!(ch[0].0, "W042", "{ch:?}");
        assert!(ch[0].1.contains("no operand rules"), "{ch:?}");
        assert!(!ch[0].1.contains("no target dialect configured"), "{ch:?}");
    }

    /// #2333: `rocky compile` types a cast for the warehouse the model's
    /// pipeline writes to. `FLOAT` is 64-bit on PostgreSQL, so a `Float64`
    /// contract passes; `--target-dialect duckdb` makes it 32-bit (`E011`);
    /// on ClickHouse, which has no width table, the type is not checked
    /// (`I003`).
    #[test]
    fn a_cast_is_typed_for_the_warehouse_the_model_runs_on() {
        let contract_codes = |adapters: &str, target_dialect: Option<Dialect>| {
            let dir = TempDir::new().unwrap();
            let config = adapter_project(dir.path(), adapters);
            fs::create_dir_all(dir.path().join("data")).unwrap();
            fs::write(
                dir.path().join("data/seed.sql"),
                "CREATE SCHEMA raw; CREATE TABLE raw.t (id INTEGER NOT NULL);",
            )
            .unwrap();
            let models = dir.path().join("models");
            write_model(&models, "m", "SELECT CAST(id AS FLOAT) AS f FROM raw.t");
            fs::write(
                models.join("m.contract.toml"),
                "[[columns]]\nname = \"f\"\ntype = \"Float64\"\n",
            )
            .unwrap();
            compile_output(
                Some(&config),
                &dir.path().join("state.redb"),
                &models,
                None,
                None,
                false,
                target_dialect,
                true,
                None,
            )
            .unwrap()
            .diagnostics
            .into_iter()
            .filter(|d| d.model == "m" && matches!(&*d.code, "E011" | "I003" | "E059"))
            .map(|d| d.code.to_string())
            .collect::<Vec<_>>()
        };
        assert_eq!(contract_codes(PG, None), Vec::<String>::new());
        assert_eq!(contract_codes(PG, Some(Dialect::DuckDB)), vec!["E011"]);
        assert_eq!(contract_codes(CH, None), vec!["I003"]);
    }

    /// `--deny-warnings` refuses codes that name no warning.
    #[test]
    fn deny_warnings_refuses_unknown_and_malformed_codes() {
        validate_deny_warning_codes(&["W042".into(), " w043 ".into(), "P002".into()]).unwrap();
        for bad in ["W999", "W42", "W 042", "E042", ""] {
            let err = validate_deny_warning_codes(&[bad.to_string()]).unwrap_err();
            let msg = err.to_string();
            assert!(msg.contains("unknown or malformed"), "{bad}: {msg}");
            assert!(msg.contains("W042"), "the valid list: {msg}");
        }
    }

    #[test]
    fn build_p001_has_error_severity_and_code() {
        let issue = PortabilityIssue {
            construct: "NVL".to_string(),
            supported_by: vec![Dialect::Snowflake, Dialect::Databricks],
            target: Dialect::BigQuery,
            suggestion: "use COALESCE".to_string(),
        };
        let diag = build_p001_diagnostic("m", "m.sql", &issue);
        assert_eq!(&*diag.code, "P001");
        assert_eq!(diag.severity, Severity::Error);
        assert_eq!(diag.model, "m");
        assert!(diag.message.contains("NVL"));
        assert!(diag.message.contains("BigQuery"));
        assert_eq!(diag.suggestion.as_deref(), Some("use COALESCE"));
    }

    #[test]
    fn compile_with_bigquery_target_flags_nvl_as_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // With target_dialect = BigQuery, NVL should trigger P001 and the
        // compile should bail with the generic "compilation failed" error.
        let err = run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::BigQuery),
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "expected bail, got: {err}"
        );
    }

    /// #1919: a sidecar value expanded from `${VAR}` prints only as `${NAME}`
    /// in `rocky compile --output json` (`models_detail`), never as the value.
    /// The target is the exception: it prints resolved, as `rocky run`'s
    /// `asset_key` does.
    #[test]
    fn compile_output_prints_a_resolved_sidecar_value_as_its_placeholder() {
        const SECRET: &str = "rocky_1919_compile_secret_d00d";
        const CATALOG: &str = "rocky_1919_compile_catalog";
        const COLUMN: &str = "rocky_1919_compile_ts_col";
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(
            models_dir.join("m1.sql"),
            "SELECT 1 AS id, CURRENT_DATE AS ts",
        )
        .unwrap();
        fs::write(
            models_dir.join("m1.toml"),
            "name = \"m1\"\n\n\
             [strategy]\ntype = \"incremental\"\ntimestamp_column = \"${ROCKY_T1919_COMPILE_COL}\"\n\n\
             [target]\ncatalog = \"${ROCKY_T1919_COMPILE_CATALOG}\"\nschema = \"s\"\ntable = \"m1\"\n\n\
             [tags]\nowner = \"${ROCKY_T1919_COMPILE}\"\n",
        )
        .unwrap();
        // SAFETY: test-only; the variable name is unique to this test.
        unsafe {
            std::env::set_var("ROCKY_T1919_COMPILE", SECRET);
            std::env::set_var("ROCKY_T1919_COMPILE_CATALOG", CATALOG);
            std::env::set_var("ROCKY_T1919_COMPILE_COL", COLUMN);
        }
        let out = compile_output(
            None,
            &dir.path().join(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            false,
            None,
            false,
            None,
        );
        // SAFETY: as above.
        unsafe {
            std::env::remove_var("ROCKY_T1919_COMPILE");
            std::env::remove_var("ROCKY_T1919_COMPILE_CATALOG");
            std::env::remove_var("ROCKY_T1919_COMPILE_COL");
        }
        let out = out.expect("compiles");
        assert_eq!(out.models_detail.len(), 1, "PRECONDITION: the model loaded");
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains(SECRET), "leaked: {json}");
        let detail = serde_json::to_value(&out.models_detail[0]).unwrap();
        assert_eq!(detail["target"]["catalog"], CATALOG);
        // A strategy column is structure, printed as the engine runs it.
        assert_eq!(detail["strategy"]["timestamp_column"], COLUMN);
        assert_eq!(detail["tags"]["owner"], "${ROCKY_T1919_COMPILE}");
    }

    #[test]
    fn compile_without_target_dialect_does_not_run_lint() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // No target_dialect → no P001, compile succeeds.
        run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("compile should succeed without lint");
    }

    #[test]
    fn compile_with_snowflake_target_accepts_nvl() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");

        // NVL is native to Snowflake, so the lint should produce no issues.
        run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::Snowflake),
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("snowflake target should accept NVL");
    }

    // ---- Wave 2: [portability] block + pragma ----

    #[test]
    fn config_target_dialect_drives_lint_when_no_flag() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        let err = run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "config target_dialect should fire P001: {err}",
        );
    }

    #[test]
    fn flag_overrides_config_target_dialect() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        // Config says snowflake (NVL native), flag overrides to bigquery
        // (NVL not portable). The flag must win and the lint must fire.
        let config = write_rocky_toml(
            dir.path(),
            "[portability]\ntarget_dialect = \"snowflake\"\n",
        );

        let err = run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            Some(Dialect::BigQuery),
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(err.to_string().contains("compilation failed"));
    }

    #[test]
    fn config_allow_list_suppresses_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let config = write_rocky_toml(
            dir.path(),
            "[portability]\ntarget_dialect = \"bigquery\"\nallow = [\"NVL\"]\n",
        );

        // Project-wide allow-list of NVL → no P001 → compile succeeds.
        run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("allow-listed NVL should not trip the lint");
    }

    #[test]
    fn per_model_pragma_suppresses_p001() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(
            &models_dir,
            "m1",
            "-- rocky-allow: NVL\nSELECT NVL(a, b) AS c FROM t",
        );
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        // Pragma exempts this model, so the lint should not fire.
        run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("pragma-exempted model should not trip the lint");
    }

    #[test]
    fn pragma_is_per_model_not_project_wide() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // m1 has the pragma; m2 does not. m2 should still trip the lint.
        write_model(
            &models_dir,
            "m1",
            "-- rocky-allow: NVL\nSELECT NVL(a, b) AS c FROM t",
        );
        write_model(&models_dir, "m2", "SELECT NVL(d, e) AS f FROM t");
        let config = write_rocky_toml(dir.path(), "[portability]\ntarget_dialect = \"bigquery\"\n");

        let err = run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "m2 should still trip the lint: {err}",
        );
    }

    #[test]
    fn missing_config_file_falls_through_to_flag_only() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT NVL(a, b) AS c FROM t");
        let nonexistent = dir.path().join("nope.toml");

        // Config file doesn't exist → portability config silently None →
        // flag-only behavior remains. With no flag, no lint fires.
        run_compile(
            Some(&nonexistent),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("missing config should fall through, not error");
    }

    #[test]
    fn malformed_config_file_fails_compile() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT 1 AS id");
        let config = dir.path().join("rocky.toml");
        fs::write(&config, "[portability\n").unwrap();

        let err = run_compile(
            Some(&config),
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();

        // `{err:#}` renders the WHOLE anyhow chain. `to_string()` would show
        // only the outermost context, and this assertion is about the parse
        // error surviving all the way out.
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to parse TOML"),
            "the underlying parse error must survive to the surface: {rendered}"
        );
        // SIXTEENTH ROUND — and it must name the FILE. Model sidecars are TOML
        // too; "failed to parse TOML" alone sends the reader at the wrong one.
        assert!(
            rendered.contains("failed to load config from")
                && rendered.contains(config.to_string_lossy().as_ref()),
            "the refusal must name rocky.toml as the file to fix: {rendered}"
        );
    }

    // ---- --with-seed source-schema loading ----

    #[cfg(feature = "duckdb")]
    fn write_seed(project_dir: &Path, sql: &str) {
        let data_dir = project_dir.join("data");
        fs::create_dir_all(&data_dir).unwrap();
        fs::write(data_dir.join("seed.sql"), sql).unwrap();
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn with_seed_populates_source_schemas_for_leaf_model() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(
            &models_dir,
            "leaf",
            "SELECT order_id, customer_id, amount FROM raw__orders.orders",
        );
        write_seed(
            dir.path(),
            "CREATE SCHEMA IF NOT EXISTS raw__orders;\n\
             CREATE TABLE raw__orders.orders (\n\
                order_id BIGINT,\n\
                customer_id BIGINT,\n\
                amount DECIMAL(10, 2)\n\
             );\n",
        );

        // Compile with --with-seed: should succeed AND the leaf model's
        // typed columns should pick up real types instead of Unknown.
        // (We assert the bail-free path here; the typed-output assertion
        // lives in compile_with_seed_resolves_unknown_types_to_concrete.)
        run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("with-seed compile should succeed");
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn with_seed_bails_when_seed_file_missing() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "leaf", "SELECT 1 AS x");
        // Note: no data/seed.sql written.

        let err = run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("--with-seed"), "msg: {msg}");
        assert!(msg.contains("seed.sql"), "msg: {msg}");
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn with_seed_bails_when_seed_sql_invalid() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "leaf", "SELECT 1 AS x");
        write_seed(dir.path(), "DEFINITELY NOT VALID SQL;");

        let err = run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("seed execution failed"),
            "msg: {err}",
        );
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn with_seed_resolves_unknown_types_to_concrete() {
        // Build a project with a leaf model and inspect the typed output
        // by going through the rocky-compiler API directly with the
        // source_schemas the seed loader would produce.
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "leaf", "SELECT id, name FROM raw__users.users");
        write_seed(
            dir.path(),
            "CREATE SCHEMA IF NOT EXISTS raw__users;\n\
             CREATE TABLE raw__users.users (id BIGINT, name VARCHAR);\n",
        );

        let source_schemas = load_source_schemas_from_seed(&models_dir).unwrap();
        let cols = source_schemas
            .get("raw__users.users")
            .expect("seed loader should produce raw__users.users entry");
        assert_eq!(cols.len(), 2);
        let by_name: HashMap<&str, &TypedColumn> =
            cols.iter().map(|c| (c.name.as_str(), c)).collect();
        assert!(matches!(
            by_name["id"].data_type,
            rocky_compiler::types::RockyType::Int64,
        ));
        assert!(matches!(
            by_name["name"].data_type,
            rocky_compiler::types::RockyType::String,
        ));
    }

    // ---- cache-backed source_schemas ----

    /// End-to-end wiring check: seed a `SchemaCacheEntry` in `state.redb`,
    /// run `rocky compile` without `--with-seed`, and confirm the cached
    /// entry lands in `CompilerConfig.source_schemas` so the leaf model's
    /// types come out concrete instead of `Unknown`.
    #[test]
    fn compile_reads_typed_columns_from_schema_cache() {
        use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
        use rocky_core::state::StateStore;

        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(
            &models_dir,
            "leaf",
            "SELECT order_id, amount FROM raw__orders.orders",
        );
        let state_path = dir.path().join(".rocky-state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let key = schema_cache_key("cat", "raw__orders", "orders");
            store
                .write_schema_cache_entry(
                    &key,
                    &SchemaCacheEntry {
                        columns: vec![
                            StoredColumn {
                                name: "order_id".into(),
                                data_type: "BIGINT".into(),
                                nullable: false,
                            },
                            StoredColumn {
                                name: "amount".into(),
                                data_type: "DECIMAL(10, 2)".into(),
                                nullable: true,
                            },
                        ],
                        cached_at: chrono::Utc::now(),
                    },
                )
                .unwrap();
        }

        // Need a `rocky.toml` with `[cache.schemas]` on defaults so the
        // cache-read path activates.
        let config = write_rocky_toml(dir.path(), "");

        // Confirm the compile succeeds — wiring check. Typed-model
        // assertions go through the compiler API to peek at typed
        // columns; see the separate cache_loader_surfaces_typed_columns
        // test below.
        run_compile(
            Some(&config),
            &state_path,
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("compile with cache-backed source_schemas should succeed");
    }

    /// Direct check on the cache-loader round trip: the helper returns the
    /// exact shape `CompilerConfig.source_schemas` expects, via the same
    /// `default_type_mapper` wave-1 uses. Guards against drift between the
    /// `StoredColumn -> TypedColumn` mapping and the cache read path.
    #[test]
    fn cache_loader_surfaces_typed_columns() {
        use rocky_compiler::schema_cache::load_source_schemas_from_cache;
        use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
        use rocky_core::state::StateStore;

        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let key = schema_cache_key("cat", "raw__events", "clicks");
            store
                .write_schema_cache_entry(
                    &key,
                    &SchemaCacheEntry {
                        columns: vec![StoredColumn {
                            name: "user_id".into(),
                            data_type: "BIGINT".into(),
                            nullable: false,
                        }],
                        cached_at: chrono::Utc::now(),
                    },
                )
                .unwrap();
        }

        let store = StateStore::open_read_only(&state_path).unwrap();
        let map =
            load_source_schemas_from_cache(&store, chrono::Utc::now(), chrono::Duration::hours(24))
                .unwrap();

        let cols = map
            .get("raw__events.clicks")
            .expect("catalog prefix stripped; leaf uses <schema>.<table>");
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].name, "user_id");
        assert!(matches!(
            cols[0].data_type,
            rocky_compiler::types::RockyType::Int64,
        ));
        assert!(!cols[0].nullable);
    }

    /// Write a model sidecar with an optional `[budget]` block.
    fn write_model_with_budget(
        dir: &Path,
        name: &str,
        sql: &str,
        max_usd: Option<f64>,
        max_bytes_scanned: Option<u64>,
    ) {
        let sql_path = dir.join(format!("{name}.sql"));
        fs::write(&sql_path, sql).unwrap();

        let mut toml_body = format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        );
        let has_budget = max_usd.is_some() || max_bytes_scanned.is_some();
        if has_budget {
            toml_body.push_str("\n[budget]\n");
            if let Some(usd) = max_usd {
                toml_body.push_str(&format!("max_usd = {usd}\n"));
            }
            if let Some(bytes) = max_bytes_scanned {
                toml_body.push_str(&format!("max_bytes_scanned = {bytes}\n"));
            }
        }
        let toml_path = dir.join(format!("{name}.toml"));
        fs::write(&toml_path, toml_body).unwrap();
    }

    /// A model whose `[budget] max_usd` is tighter than the stub estimate
    /// must cause `rocky compile` to emit E027 and exit with an error.
    ///
    /// Stub leaf stats: 10_000 rows × 256 bytes → estimated_compute_cost_usd
    /// ≈ $0.0000228 (Databricks pricing constants).  A ceiling of $0.000001
    /// is well below that, so the breach is guaranteed deterministically.
    #[test]
    fn compile_emits_e027_when_usd_ceiling_exceeded() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // max_usd is far below the stub estimate (~$0.0000228).
        write_model_with_budget(
            &models_dir,
            "expensive_model",
            "SELECT 1 AS x",
            Some(0.000001),
            None,
        );

        let err = run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true, // output_json: true so we can inspect the JSON
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "E027 should cause compilation failure, got: {err}",
        );
    }

    /// A model without a `[budget]` block must compile successfully even
    /// when cost estimates are non-zero.
    #[test]
    fn compile_no_budget_no_e027() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT 1 AS x");

        run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("model without budget should compile cleanly");
    }

    /// A model with a `[budget] max_bytes_scanned` below the stub estimate
    /// must emit E027.  Stub leaf: 10_000 rows × 256 bytes = 2_560_000
    /// estimated_bytes; ceiling of 100_000 < 2_560_000.
    #[test]
    fn compile_emits_e027_when_bytes_ceiling_exceeded() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model_with_budget(
            &models_dir,
            "big_scan",
            "SELECT 1 AS x",
            None,
            Some(100_000), // 100 KB — below the 2.56 MB stub estimate
        );

        let err = run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("compilation failed"),
            "E027 bytes breach should cause compilation failure, got: {err}",
        );
    }

    /// A model with a `[budget]` ceiling above the stub estimate must compile cleanly.
    #[test]
    fn compile_generous_budget_passes() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        // max_usd = $1.00 >> stub estimate (~$0.000023); should pass.
        write_model_with_budget(&models_dir, "cheap_model", "SELECT 1 AS x", Some(1.0), None);

        run_compile(
            None,
            Path::new(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("generous budget should not trigger E027");
    }

    /// Compile without a state file must not silently fail or create a
    /// stray `state.redb` beside the models directory. Cold-cache path is
    /// the default experience for a fresh project.
    #[test]
    fn compile_without_state_file_degrades_gracefully() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "m1", "SELECT 1 AS x");
        let config = write_rocky_toml(dir.path(), "");
        let state_path = dir.path().join(".rocky-state.redb");
        assert!(!state_path.exists(), "precondition: no state file");

        run_compile(
            Some(&config),
            &state_path,
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            false,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("compile without state file should succeed");
        assert!(
            !state_path.exists(),
            "compile must not create state.redb as a side effect"
        );
    }

    // ---- E041 / W041: missing external source columns ----

    /// The brief's reference seed.
    const REFERENCE_SEED: &str = "CREATE SCHEMA raw;\n\
        CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, \
        status VARCHAR, order_date DATE);\n\
        CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n";

    const D1: &str = "SELECT order_id, customer_id, order_total FROM raw.orders";

    fn seeded_project(seed: &str, models: &[(&str, &str)]) -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        for (name, sql) in models {
            write_model(&models_dir, name, sql);
        }
        write_seed(dir.path(), seed);
        (dir, models_dir)
    }

    fn compile_seeded(
        config: Option<&Path>,
        models_dir: &Path,
        strict_sources: bool,
    ) -> CompileOutput {
        compile_inner(
            config,
            &models_dir.join(".rocky-state.redb"),
            models_dir,
            ModelScope::Dir,
            None,
            None,
            false,
            None,
            SeedUse::Required,
            None,
            &rocky_core::run_vars::RunVars::new(),
            strict_sources,
            false,
            None,
        )
        .expect("compile should produce output")
        .0
    }

    fn count(output: &CompileOutput, code: &str) -> usize {
        output
            .diagnostics
            .iter()
            .filter(|d| d.code.as_ref() == code)
            .count()
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn d1_with_seed_warns_w041_and_exits_zero() {
        let (_dir, models_dir) = seeded_project(REFERENCE_SEED, &[("stg_orders", D1)]);
        let output = compile_seeded(None, &models_dir, false);
        assert!(!output.has_errors, "{:?}", output.diagnostics);
        assert_eq!(count(&output, "W041"), 1, "{:?}", output.diagnostics);
        let w041 = output
            .diagnostics
            .iter()
            .find(|d| d.code.as_ref() == "W041");
        let w041 = w041.unwrap();
        assert!(w041.message.contains("order_total"), "{}", w041.message);
        assert!(w041.message.contains("raw.orders"), "{}", w041.message);

        // The process-level contract: `run_compile` returns Ok (exit 0).
        run_compile(
            None,
            &models_dir.join(".rocky-state.redb"),
            &models_dir,
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            &[],
        )
        .expect("a seed-backed W041 must not fail the compile");
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn d1_with_seed_and_strict_sources_refuses_e041() {
        let (_dir, models_dir) = seeded_project(REFERENCE_SEED, &[("stg_orders", D1)]);
        let output = compile_seeded(None, &models_dir, true);
        assert!(output.has_errors);
        assert_eq!(count(&output, "E041"), 1, "{:?}", output.diagnostics);
        assert_eq!(count(&output, "W041"), 0);

        let err = run_compile_with_options(
            None,
            &models_dir.join(".rocky-state.redb"),
            &models_dir,
            ModelScope::Dir,
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            true,
            false,
            &[],
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("compilation failed"), "{err}");
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn strict_sources_config_key_escalates_like_the_flag() {
        let (dir, models_dir) = seeded_project(REFERENCE_SEED, &[("stg_orders", D1)]);
        let config = write_rocky_toml(dir.path(), "[cache.schemas]\nstrict_sources = true\n");
        let output = compile_seeded(Some(&config), &models_dir, false);
        assert!(output.has_errors);
        assert_eq!(count(&output, "E041"), 1, "{:?}", output.diagnostics);
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn reference_valid_controls_stay_clean_even_under_strict_sources() {
        let (_dir, models_dir) = seeded_project(
            REFERENCE_SEED,
            &[
                (
                    "stg_orders",
                    "SELECT order_id, customer_id, amount FROM raw.orders",
                ),
                (
                    "fct_revenue",
                    "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
                     JOIN raw.customers c ON o.customer_id = c.customer_id \
                     GROUP BY c.customer_name",
                ),
                (
                    "v1",
                    "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
                ),
                ("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value"),
                (
                    "v3",
                    "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
                ),
                (
                    "v4",
                    "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
                ),
                (
                    "g1_s2",
                    "WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) \
                     SELECT stg_orders.amount FROM stg_orders",
                ),
                ("stg_star", "SELECT * FROM raw.orders"),
                ("g1_s3", "SELECT s.amount FROM stg_star AS s"),
            ],
        );
        let output = compile_seeded(None, &models_dir, true);
        assert_eq!(
            count(&output, "E041") + count(&output, "W041"),
            0,
            "{:?}",
            output.diagnostics
        );
    }

    #[test]
    #[cfg(feature = "duckdb")]
    fn g1_s4_stale_seed_stays_exit_zero_without_escalation() {
        // The seed lacks `amount`; the warehouse has it.
        let (_dir, models_dir) = seeded_project(
            "CREATE SCHEMA raw;\n\
             CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, status VARCHAR);\n",
            &[(
                "stg_orders",
                "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
            )],
        );
        let output = compile_seeded(None, &models_dir, false);
        assert!(!output.has_errors, "{:?}", output.diagnostics);
        assert_eq!(count(&output, "W041"), 1, "{:?}", output.diagnostics);
    }

    /// A cache entry inside `[cache.schemas] trusted_max_age_seconds` is
    /// authoritative: D1 refuses without any strict flag. Outside it (or with
    /// the key unset) the same entry only warns.
    #[test]
    fn d1_against_trusted_cache_entry_refuses_e041() {
        use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
        use rocky_core::state::StateStore;

        let dir = TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        write_model(&models_dir, "stg_orders", D1);
        let state_path = dir.path().join(".rocky-state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let columns = ["order_id", "customer_id", "amount", "status", "order_date"]
                .into_iter()
                .map(|name| StoredColumn {
                    name: name.into(),
                    data_type: "BIGINT".into(),
                    nullable: true,
                })
                .collect();
            store
                .write_schema_cache_entry(
                    &schema_cache_key("cat", "raw", "orders"),
                    &SchemaCacheEntry {
                        columns,
                        cached_at: chrono::Utc::now() - chrono::Duration::minutes(10),
                    },
                )
                .unwrap();
        }
        let compile_with = |cache_block: &str| {
            let config = write_rocky_toml(dir.path(), cache_block);
            compile_inner(
                Some(&config),
                &state_path,
                &models_dir,
                ModelScope::Dir,
                None,
                None,
                false,
                None,
                SeedUse::IfPresent,
                None,
                &rocky_core::run_vars::RunVars::new(),
                false,
                false,
                None,
            )
            .unwrap()
            .0
        };

        let fresh_cache = compile_with("[cache.schemas]\ntrusted_max_age_seconds = 3600\n");
        assert!(fresh_cache.has_errors);
        assert_eq!(
            count(&fresh_cache, "E041"),
            1,
            "{:?}",
            fresh_cache.diagnostics
        );

        let aged = compile_with("[cache.schemas]\ntrusted_max_age_seconds = 60\n");
        assert!(!aged.has_errors, "{:?}", aged.diagnostics);
        assert_eq!(count(&aged, "W041"), 1, "{:?}", aged.diagnostics);

        let unset = compile_with("");
        assert!(!unset.has_errors, "{:?}", unset.diagnostics);
        assert_eq!(count(&unset, "W041"), 1, "{:?}", unset.diagnostics);
    }

    // ---- whole-project compile (no `--models`) ----

    /// Two transformation pipelines: `transform` (models/**) and
    /// `reporting` (reporting/**). `models/stg` reads the seed table
    /// `src.orders`; `reporting/rep` reads `stg`. The contract on `rep`
    /// declares `id` as text, while the seed makes it BIGINT.
    #[cfg(feature = "duckdb")]
    fn scaffold_two_pipeline_project() -> TempDir {
        let dir = TempDir::new().unwrap();
        fs::write(
            dir.path().join("rocky.toml"),
            "[adapter]\ntype = \"duckdb\"\npath = \"w.duckdb\"\n\n\
             [pipeline.transform]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             [pipeline.transform.target]\n\n\
             [pipeline.reporting]\ntype = \"transformation\"\nmodels = \"reporting/**\"\n\
             depends_on = [\"transform\"]\n[pipeline.reporting.target]\n",
        )
        .unwrap();
        let models_dir = dir.path().join("models");
        let reporting = dir.path().join("reporting");
        fs::create_dir_all(&models_dir).unwrap();
        fs::create_dir_all(&reporting).unwrap();
        write_model(&models_dir, "stg", "SELECT id, status FROM src.orders");
        write_model(&reporting, "rep", "SELECT id FROM stg");
        write_seed(
            dir.path(),
            "CREATE SCHEMA src;\n\
             CREATE TABLE src.orders AS SELECT 1::BIGINT AS id, 'a' AS status;\n",
        );
        let contracts = dir.path().join("contracts");
        fs::create_dir_all(&contracts).unwrap();
        fs::write(
            contracts.join("rep.contract.toml"),
            "[[columns]]\nname = \"id\"\ntype = \"String\"\nnullable = true\n",
        )
        .unwrap();
        dir
    }

    #[cfg(feature = "duckdb")]
    fn compile_scoped(
        dir: &Path,
        models_dir: &Path,
        scope: ModelScope,
        seed_use: SeedUse,
    ) -> CompileOutput {
        compile_inner(
            Some(&dir.join("rocky.toml")),
            &dir.join("state.redb"),
            models_dir,
            scope,
            Some(&dir.join("contracts")),
            None,
            false,
            None,
            seed_use,
            None,
            &rocky_core::run_vars::RunVars::new(),
            false,
            false,
            None,
        )
        .unwrap()
        .0
    }

    /// No `--models`: both pipelines' models compile in one graph, and the
    /// reporting model gets the transform model's column types, so its
    /// contract mismatch is E011. `--models reporting` keeps today's
    /// meaning: one directory, `stg` unseen, the type Unknown, no E011.
    #[test]
    #[cfg(feature = "duckdb")]
    fn whole_project_compile_types_flow_across_pipelines() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();

        let whole = compile_scoped(
            root,
            &root.join("models"),
            ModelScope::WholeProject,
            SeedUse::IfPresent,
        );
        assert_eq!(whole.models, 2, "{:?}", whole.models_detail);
        assert!(
            whole
                .diagnostics
                .iter()
                .any(|d| &*d.code == "E011" && d.model == "rep"),
            "{:?}",
            whole.diagnostics
        );

        let one_dir = compile_scoped(
            root,
            &root.join("reporting"),
            ModelScope::Dir,
            SeedUse::IfPresent,
        );
        assert_eq!(one_dir.models, 1);
        assert!(
            !one_dir.diagnostics.iter().any(|d| &*d.code == "E011"),
            "{:?}",
            one_dir.diagnostics
        );
    }

    /// A consumer that reads a model of another pipeline is not an E060 when
    /// the compile covers one directory: `consumers/` is the project's and is
    /// judged against every model in it. A name that is a model nowhere is
    /// still an E060, scoped or not.
    #[test]
    #[cfg(feature = "duckdb")]
    fn consumers_are_judged_against_the_whole_project_in_a_scoped_compile() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();
        fs::create_dir_all(root.join("consumers")).unwrap();
        fs::write(
            root.join("consumers").join("board.toml"),
            "depends_on = [\"stg\", \"rep\"]\n",
        )
        .unwrap();
        let e060 = |o: &CompileOutput| {
            o.diagnostics
                .iter()
                .filter(|d| &*d.code == "E060")
                .map(|d| d.message.to_string())
                .collect::<Vec<_>>()
        };

        let one_dir = compile_scoped(
            root,
            &root.join("reporting"),
            ModelScope::Dir,
            SeedUse::IfPresent,
        );
        assert_eq!(one_dir.models, 1);
        assert!(e060(&one_dir).is_empty(), "{:?}", one_dir.diagnostics);

        fs::write(
            root.join("consumers").join("board.toml"),
            "depends_on = [\"stg\", \"nowhere\"]\n",
        )
        .unwrap();
        let broken = compile_scoped(
            root,
            &root.join("reporting"),
            ModelScope::Dir,
            SeedUse::IfPresent,
        );
        let messages = e060(&broken);
        assert_eq!(messages.len(), 1, "{messages:?}");
        assert!(messages[0].contains("`nowhere`"), "{}", messages[0]);
    }

    /// The control: with a contract that matches, the whole-project compile
    /// is clean.
    #[test]
    #[cfg(feature = "duckdb")]
    fn whole_project_compile_is_clean_on_a_valid_project() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();
        fs::write(
            root.join("contracts").join("rep.contract.toml"),
            "[[columns]]\nname = \"id\"\ntype = \"Int64\"\nnullable = true\n",
        )
        .unwrap();

        let whole = compile_scoped(
            root,
            &root.join("models"),
            ModelScope::WholeProject,
            SeedUse::IfPresent,
        );
        assert_eq!(whole.models, 2);
        assert!(!whole.has_errors, "{:?}", whole.diagnostics);
        assert!(
            !whole
                .diagnostics
                .iter()
                .any(|d| d.severity == Severity::Warning && d.code.starts_with("W04")),
            "{:?}",
            whole.diagnostics
        );
    }

    /// With no `--with-seed`, a project's `data/seed.sql` still types the
    /// compile: a column the seed table lacks is W041.
    #[test]
    #[cfg(feature = "duckdb")]
    fn bare_compile_uses_the_seed_without_the_flag() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();
        write_model(
            &root.join("models"),
            "stg",
            "SELECT id, status, segment FROM src.orders",
        );

        let whole = compile_scoped(
            root,
            &root.join("models"),
            ModelScope::WholeProject,
            SeedUse::IfPresent,
        );
        assert!(
            whole
                .diagnostics
                .iter()
                .any(|d| &*d.code == "W041" && d.message.contains("segment")),
            "{:?}",
            whole.diagnostics
        );
    }

    /// `compile_output` (the `rocky serve` API, the MCP compile tool) never
    /// runs the seed unasked: the same project gives no W041 there.
    #[test]
    #[cfg(feature = "duckdb")]
    fn compile_output_does_not_run_the_seed_unasked() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();
        write_model(
            &root.join("models"),
            "stg",
            "SELECT id, status, segment FROM src.orders",
        );

        let out = compile_output(
            Some(&root.join("rocky.toml")),
            &root.join("state.redb"),
            &root.join("models"),
            None,
            None,
            false,
            None,
            false,
            None,
        )
        .unwrap();
        assert!(
            !out.diagnostics.iter().any(|d| &*d.code == "W041"),
            "{:?}",
            out.diagnostics
        );
    }

    /// A seed the user did not ask for never fails the compile: a seed that
    /// does not run is skipped, and the compile goes on untyped. Under
    /// `--with-seed` the same seed refuses (pinned above).
    #[test]
    #[cfg(feature = "duckdb")]
    fn a_broken_default_seed_does_not_fail_the_compile() {
        let dir = scaffold_two_pipeline_project();
        let root = dir.path();
        write_seed(root, "DEFINITELY NOT VALID SQL;");

        let whole = compile_scoped(
            root,
            &root.join("models"),
            ModelScope::WholeProject,
            SeedUse::IfPresent,
        );
        assert_eq!(whole.models, 2);
        assert!(
            !whole.diagnostics.iter().any(|d| &*d.code == "E011"),
            "untyped compile: {:?}",
            whole.diagnostics
        );
    }
}
