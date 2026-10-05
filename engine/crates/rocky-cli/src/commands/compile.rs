//! `rocky compile` — type-check models, resolve dependencies, validate contracts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_compiler::compile::{self, CompilerConfig, default_type_mapper};
use rocky_compiler::cost_check;
use rocky_compiler::diagnostic::{self, Diagnostic, Severity};
use rocky_compiler::source_refs::{SourceProvenance, SourceSchemaOrigin};
use rocky_compiler::types::TypedColumn;
use rocky_core::config as rocky_config;
use rocky_core::macros::{expand_macros, load_macros_from_dir};
use rocky_core::secret_registry::{render_placeholders, render_placeholders_in};
use rocky_sql::portability::{self, PortabilityIssue};
use rocky_sql::pragma;
use rocky_sql::transpile::Dialect;

use crate::output::{CompileOutput, CostHint, FunctionDetail, ModelDetail, print_json};

use super::ModelNotFound;

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
        contracts_dir,
        model_filter,
        output_json,
        do_expand_macros,
        target_dialect,
        with_seed,
        cache_ttl_override,
        run_vars,
        false,
        deny_warning_codes,
        None,
    )
}

/// [`run_compile`] with every invocation option.
///
/// `strict_sources` (`rocky compile --strict-sources`) escalates every W041
/// (a source column missing from a seed or untrusted cached schema) to E041
/// for this invocation. It ORs with `[cache.schemas] strict_sources`; it can
/// turn strictness on, never off.
///
/// `selection` (`--select` / `--exclude`) scopes the report: the whole
/// project still compiles (types flow across models); only the selected
/// models' details and diagnostics are reported, and only their errors fail
/// the command — the same scoping `--model` applies.
#[allow(clippy::too_many_arguments)]
pub fn run_compile_with_options(
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
    strict_sources: bool,
    deny_warning_codes: &[String],
    selection: Option<&crate::selection::SelectionArgs>,
) -> Result<()> {
    let (mut output, text_data) = compile_inner(
        config_path,
        state_path,
        models_dir,
        contracts_dir,
        model_filter,
        do_expand_macros,
        target_dialect,
        with_seed,
        cache_ttl_override,
        run_vars,
        strict_sources,
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
        contracts_dir,
        model_filter,
        output_json,
        do_expand_macros,
        target_dialect,
        false,
        cache_ttl_override,
        run_vars,
        strict_sources,
        deny_warning_codes,
        selection,
    )
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
    contracts_dir: Option<&Path>,
    model_filter: Option<&str>,
    do_expand_macros: bool,
    target_dialect: Option<Dialect>,
    with_seed: bool,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    strict_sources: bool,
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

    // `source_schemas` precedence:
    //   1. `--with-seed` wins -> seed loader (explicit user intent,
    //      used for tests/playgrounds where the cache is irrelevant).
    //   2. Otherwise, the schema cache if `[cache.schemas] enabled`.
    //   3. Cold-cache fallback: empty map — typecheck degrades to
    //      Unknown.
    //
    // Each tier also records where its schemas came from, for the E041 /
    // W041 missing-source-column check: a seed is `Seed` (W041 unless
    // strict), a cache entry is `Cache` with its timestamp (E041 only within
    // `[cache.schemas] trusted_max_age_seconds`). `--strict-sources` and
    // `[cache.schemas] strict_sources` escalate every W041 to E041.
    let config_strict_sources = project_config
        .as_ref()
        .is_some_and(|config| config.cache.schemas.strict_sources);
    let (source_schemas, source_provenance) = if with_seed {
        // Seed loader: run `data/seed.sql` in in-memory DuckDB, read
        // columns from its `information_schema`. Turns leaf .sql models
        // from `RockyType::Unknown` into concrete types for any project
        // that ships a runnable seed (the entire playground).
        let schemas = load_source_schemas_from_seed(models_dir)?;
        let provenance = SourceProvenance::uniform(schemas.keys(), &SourceSchemaOrigin::Seed);
        (schemas, provenance)
    } else if let Some(config) = &project_config {
        // TTL-filtered load from `state.redb`'s `SCHEMA_CACHE` table.
        // Honours `[cache.schemas] enabled` + `ttl_seconds` (after
        // applying the optional CLI `--cache-ttl` override).
        let schema_cfg = config
            .cache
            .schemas
            .clone()
            .with_ttl_override(cache_ttl_override);
        crate::source_schemas::load_cached_source_schemas_with_provenance(&schema_cfg, state_path)
    } else {
        (HashMap::new(), SourceProvenance::default())
    };
    let source_provenance = source_provenance.with_strict(strict_sources || config_strict_sources);

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
        // The lints below (P001, E042/E043, imports E030/E033) judge each
        // model's SQL as authored. Against the inlined form, an ephemeral
        // model's defect would be reported again on every consumer, at
        // spans that do not exist in the consumer's file. The inlined form
        // is written back after them, for `--expand-macros`.
        preserve_authored_sql: true,
    };

    let mut result = compile::compile(&config)?;

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

    // A warehouse that cannot create functions refuses them here (E051), at
    // compile time, rather than mid-run.
    if let Some(config) = &project_config {
        result
            .diagnostics
            .extend(function_adapter_diagnostics(config, &result));
        // Likewise a warehouse that cannot run the SCD2 snapshot MERGE.
        result
            .diagnostics
            .extend(snapshot_adapter_diagnostics(config, &result));
        // And one with no upsert at all (ClickHouse): E053.
        result
            .diagnostics
            .extend(merge_adapter_diagnostics(config, &result));
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
    // E043/W043). These judge against the warehouse that will run the SQL,
    // so they need a dialect the compiler core does not carry; see
    // `resolve_operand_dialect` for the precedence.
    let operand_dialect = resolve_operand_dialect(target_dialect, project_config.as_ref());
    let operand_diags = rocky_compiler::operand_check::check_operand_types(
        &result.project.models,
        &result.semantic_graph,
        &result.type_check.typed_models,
        operand_dialect,
    );
    if operand_diags.iter().any(|d| d.severity == Severity::Error) {
        result.has_errors = true;
    }
    result.diagnostics.extend(operand_diags);

    // Compute DAG-propagated cost estimates for all models.
    // Uses hardcoded stub statistics for leaf nodes — real catalog stats
    // (per-adapter `DESCRIBE DETAIL` / Iceberg snapshot summary) will replace
    // these stubs in a follow-up that wires the adapter registry here.
    let cost_estimates = {
        use rocky_core::cost::{TableStats, WarehouseType, propagate_costs};
        let dag_nodes = &result.project.dag_nodes;
        let mut base_stats = std::collections::HashMap::new();
        for node in dag_nodes {
            if node.depends_on.is_empty() {
                base_stats.insert(
                    node.name.clone(),
                    TableStats {
                        row_count: 10_000,
                        avg_row_bytes: 256,
                    },
                );
            }
        }
        propagate_costs(dag_nodes, &base_stats, WarehouseType::Databricks).unwrap_or_default()
    };

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
    if let Some(config) = &project_config {
        result
            .diagnostics
            .extend(sqlserver_cte_diagnostics(config, &result));
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
    // `${NAME}` (#1919).
    let diagnostics: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| in_scope(&d.model))
        .map(|d| Diagnostic {
            message: render_placeholders(&d.message).into(),
            model: render_placeholders(&d.model),
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
            // Sidecar values were `${VAR}`-expanded before parsing. Every
            // field below except `target` is written with each resolved value
            // as `${NAME}` (#1919). The copies are for printing only.
            // `target` prints resolved on purpose: dagster-rocky matches the
            // asset key it builds from it against `rocky run`'s `asset_key`,
            // which carries the resolved coordinates.
            let render = render_placeholders;
            Ok(ModelDetail {
                name: render(&model.config.name),
                strategy: render_placeholders_in(&model.config.strategy)
                    .context("failed to render the model strategy for output")?,
                target: model.config.target.clone(),
                freshness: render_placeholders_in(&model.config.freshness)
                    .context("failed to render the model freshness for output")?,
                contract_source: model.contract_path.as_ref().map(|_| "auto".to_string()),
                cost_hint,
                depends_on: model.config.depends_on.iter().map(|d| render(d)).collect(),
                tags: model
                    .config
                    .tags
                    .iter()
                    .map(|(k, v)| (render(k), render(v)))
                    .collect(),
            })
        })
        .collect::<Result<_>>()?;

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

/// The warehouse dialect the E042/E043 operand checks judge against.
///
/// Precedence: an explicit `--target-dialect` flag, then the adapter type of
/// the pipelines' target adapter (only when every pipeline targets the same
/// dialect, or — with no pipelines — when every warehouse adapter does), then
/// `[portability] target_dialect`. `None` when nothing resolves; the checks
/// then report the least severe verdict across all dialects (warnings only).
fn resolve_operand_dialect(
    target_dialect: Option<Dialect>,
    config: Option<&rocky_config::RockyConfig>,
) -> Option<rocky_compiler::operand_check::OperandDialect> {
    use rocky_compiler::operand_check::OperandDialect;

    if let Some(dialect) = target_dialect {
        return Some(dialect.into());
    }
    let config = config?;
    let adapter_dialect = |name: &str| {
        config
            .adapters
            .get(name)
            .and_then(|a| OperandDialect::from_adapter_type(&a.adapter_type))
    };
    let dialects: std::collections::HashSet<OperandDialect> = if config.pipelines.is_empty() {
        config
            .adapters
            .values()
            .filter_map(|a| OperandDialect::from_adapter_type(&a.adapter_type))
            .collect()
    } else {
        config
            .pipelines
            .values()
            .filter_map(|p| adapter_dialect(p.target_adapter()))
            .collect()
    };
    if dialects.len() == 1 {
        return dialects.into_iter().next();
    }
    config.portability.target_dialect.map(Into::into)
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

/// The adapter blocks that act as warehouses (the data role): every block
/// except a discovery-only type (`fivetran`, `airbyte`, …) or one declared
/// `kind = "discovery"`. An adapter type Rocky does not know counts as a
/// warehouse, so the checks below treat it as unable to run the feature.
fn warehouse_adapters(
    config: &rocky_config::RockyConfig,
) -> impl Iterator<Item = &rocky_config::AdapterConfig> {
    config.adapters.values().filter(|a| {
        a.kind != Some(rocky_config::AdapterKind::Discovery)
            && rocky_core::adapter_capability::capability_for(&a.adapter_type)
                .is_none_or(|cap| cap.supports_data)
    })
}

/// E051 for every valid function a model calls when every warehouse adapter
/// the project configures cannot create it: Trino, every adapter with no
/// function DDL (ClickHouse, SQL Server, an unknown type), and PostgreSQL /
/// Redshift when their `CREATE FUNCTION` rendering
/// ([`rocky_core::functions::create_function_sql`]) refuses this function —
/// a `[target] catalog` on Redshift, a body holding the dollar-quote
/// delimiter, an argument used in a qualified reference with no positional
/// spelling. Those refusals would otherwise surface only at `rocky run`.
/// SQL Server is refused, not rendered: a T-SQL scalar UDF takes
/// `@`-prefixed parameters and must be called schema-qualified
/// (`dbo.f(x)`), so a model's bare `f(x)` call would not resolve to it. A project
/// that also configures a capable warehouse is not refused here —
/// `rocky run` refuses at the boundary if the model runs on the other one.
fn function_adapter_diagnostics(
    config: &rocky_config::RockyConfig,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    use rocky_core::functions::{FunctionDialect, create_function_sql};
    let registry = result.semantic_graph.functions();
    let warehouses: Vec<(&str, Option<FunctionDialect>)> = warehouse_adapters(config)
        .map(|a| {
            (
                a.adapter_type.as_str(),
                FunctionDialect::from_dialect_name(&a.adapter_type),
            )
        })
        .collect();
    if registry.is_empty() || warehouses.is_empty() {
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
    let mut types: Vec<&str> = warehouses.iter().map(|(t, _)| *t).collect();
    types.sort_unstable();
    types.dedup();
    let types = types.join(", ");
    let usage = rocky_compiler::udf::function_usage(&result.project.models, registry);
    usage
        .keys()
        .filter_map(|name| {
            let reasons: Vec<Result<String, ()>> = warehouses
                .iter()
                .map(|(_, w)| refusal(name, w))
                .collect::<Option<_>>()?;
            let rendered: Vec<String> = reasons.into_iter().filter_map(Result::ok).collect();
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

/// E049 for every snapshot model when every warehouse adapter the project
/// configures cannot run the SCD2 snapshot SQL
/// ([`rocky_core::traits::SqlDialect::snapshot_unsupported_reason`]):
/// PostgreSQL under `merge_mode = "on_conflict"`, and Redshift. Conservative
/// like E051: a project that also configures a capable warehouse is not
/// refused here; `rocky run` refuses at the boundary instead.
fn snapshot_adapter_diagnostics(
    config: &rocky_config::RockyConfig,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    let mut reasons: Vec<(String, &'static str)> = Vec::new();
    for adapter in warehouse_adapters(config) {
        let reason = match crate::registry::postgres_dialect_for_config(adapter) {
            Some(dialect) => dialect.snapshot_unsupported_reason(),
            None => crate::registry::warehouse_dialect_for_type(&adapter.adapter_type)
                .and_then(rocky_core::traits::SqlDialect::snapshot_unsupported_reason),
        };
        // One capable (or unknown) warehouse is enough to stay silent.
        let Some(reason) = reason else {
            return Vec::new();
        };
        reasons.push((adapter.adapter_type.clone(), reason));
    }
    let Some((adapter_type, reason)) = reasons.first() else {
        return Vec::new();
    };
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
        .map(|m| {
            Diagnostic::error(
                diagnostic::E049,
                &m.config.name,
                format!(
                    "snapshot model `{}` cannot run on the configured {adapter_type} warehouse: \
                     {reason}",
                    m.config.name
                ),
            )
            .with_suggestion(
                "use a warehouse that supports MERGE (PostgreSQL 15+ with merge_mode = \"merge\"), \
                 or change the model's strategy",
            )
        })
        .collect()
}

/// E053 for every model that updates rows by key — `merge`, or `incremental`
/// with a `unique_key` — when every warehouse adapter the project configures
/// has no upsert to render it with
/// ([`rocky_core::traits::SqlDialect::merge_unsupported_reason`]):
/// ClickHouse. Conservative like E049: a project that also configures a
/// capable warehouse is not refused here; `rocky run` refuses at SQL
/// generation if the model runs on ClickHouse.
fn merge_adapter_diagnostics(
    config: &rocky_config::RockyConfig,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    let mut refusal: Option<(String, &'static str)> = None;
    for adapter in warehouse_adapters(config) {
        let reason = match crate::registry::postgres_dialect_for_config(adapter) {
            Some(dialect) => dialect.merge_unsupported_reason(),
            None => crate::registry::warehouse_dialect_for_type(&adapter.adapter_type)
                .and_then(rocky_core::traits::SqlDialect::merge_unsupported_reason),
        };
        // One capable (or unknown) warehouse is enough to stay silent.
        let Some(reason) = reason else {
            return Vec::new();
        };
        refusal.get_or_insert((adapter.adapter_type.clone(), reason));
    }
    let Some((adapter_type, reason)) = refusal else {
        return Vec::new();
    };
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
/// `WITH` ([`rocky_sqlserver::tsql::hoist_ctes`]), when every warehouse
/// adapter the project configures is SQL Server. Conservative like E053: a
/// project that also configures another warehouse is not refused here.
fn sqlserver_cte_diagnostics(
    config: &rocky_config::RockyConfig,
    result: &compile::CompileResult,
) -> Vec<Diagnostic> {
    let mut warehouses = warehouse_adapters(config).peekable();
    if warehouses.peek().is_none() || !warehouses.all(|a| a.adapter_type == "sqlserver") {
        return Vec::new();
    }
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
        contracts_dir,
        model_filter,
        do_expand_macros,
        target_dialect,
        with_seed,
        cache_ttl_override,
        // `compile_output` backs commands that don't expose `--var`
        // (ci / dag); an `@var()` model would surface an E028 diagnostic.
        &rocky_core::run_vars::RunVars::new(),
        // No `--strict-sources` flag on these surfaces; `[cache.schemas]
        // strict_sources` still applies.
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
#[cfg(feature = "duckdb")]
pub(crate) fn load_source_schemas_from_seed(
    models_dir: &Path,
) -> Result<HashMap<String, Vec<TypedColumn>>> {
    use anyhow::Context;
    use rocky_duckdb::DuckDbConnector;

    let project_root = models_dir.parent().unwrap_or(Path::new("."));
    let seed_path = project_root.join("data").join("seed.sql");
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

    // One round-trip pulls every (schema, table, column, type, nullable)
    // tuple. Filtering out DuckDB's internal schemas keeps the resulting
    // map scoped to user-created tables.
    let info_sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable \
                    FROM information_schema.columns \
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
                    ORDER BY table_schema, table_name, ordinal_position";
    let result = conn
        .execute_sql(info_sql)
        .map_err(|e| anyhow::anyhow!("information_schema query failed: {e}"))?;

    let mut by_table: HashMap<String, Vec<TypedColumn>> = HashMap::new();
    for row in &result.rows {
        let schema = row[0].as_str().unwrap_or_default();
        let table = row[1].as_str().unwrap_or_default();
        let column = row[2].as_str().unwrap_or_default();
        let data_type = row[3].as_str().unwrap_or_default();
        let nullable = row[4]
            .as_str()
            .map(|s| s.eq_ignore_ascii_case("yes") || s == "true" || s == "1")
            .unwrap_or(true);

        if schema.is_empty() || table.is_empty() || column.is_empty() {
            continue;
        }

        let key = format!("{schema}.{table}");
        by_table.entry(key).or_default().push(TypedColumn {
            name: column.to_string(),
            data_type: default_type_mapper(data_type),
            nullable,
        });
    }

    Ok(by_table)
}

/// Stub used when the binary is built without the `duckdb` feature. The
/// flag exists in the clap definition unconditionally so feature-stripped
/// builds give a clear error rather than a silent no-op.
#[cfg(not(feature = "duckdb"))]
pub(crate) fn load_source_schemas_from_seed(
    _models_dir: &Path,
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
    /// delimiter, an argument in a qualified reference. A capable warehouse
    /// beside them keeps compile quiet.
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

            // DuckDB beside it can create the function: no compile refusal.
            let dir = TempDir::new().unwrap();
            let both =
                format!("{adapters}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
            let config = udf_project(dir.path(), &both);
            fs::write(dir.path().join("functions/dbl.sql"), body).unwrap();
            let codes = compile_codes(dir.path(), &config);
            assert!(!codes.iter().any(|(c, _)| c == "E051"), "{body}: {codes:?}");
        }
    }

    /// A capable warehouse beside Trino keeps compile quiet: the run
    /// refuses at the boundary if the model lands on Trino.
    #[test]
    fn udf_with_a_capable_warehouse_configured_is_not_e051() {
        let dir = TempDir::new().unwrap();
        let adapters =
            format!("{TRINO}\n[adapter.local]\ntype = \"duckdb\"\npath = \":memory:\"\n");
        let config = udf_project(dir.path(), &adapters);
        let codes = compile_codes(dir.path(), &config);
        assert!(!codes.iter().any(|(c, _)| c == "E051"), "{codes:?}");
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
    /// query reads cannot be lifted: E054 on a SQL Server-only project, and
    /// silence when another warehouse is configured.
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
        assert!(!codes.iter().any(|(c, _)| c == "E054"), "{codes:?}");
    }

    /// A warehouse with MERGE beside ClickHouse keeps compile quiet; the run
    /// refuses at SQL generation if the model lands on ClickHouse.
    #[test]
    fn clickhouse_merge_with_a_capable_warehouse_is_not_e053() {
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
        assert!(!codes.iter().any(|(c, _)| c == "E053"), "{codes:?}");
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
             [strategy]\ntype = \"incremental\"\ntimestamp_column = \"${ROCKY_T1919_COMPILE}\"\n\n\
             [target]\ncatalog = \"${ROCKY_T1919_COMPILE_CATALOG}\"\nschema = \"s\"\ntable = \"m1\"\n\n\
             [tags]\nowner = \"${ROCKY_T1919_COMPILE}\"\n",
        )
        .unwrap();
        // SAFETY: test-only; the variable name is unique to this test.
        unsafe {
            std::env::set_var("ROCKY_T1919_COMPILE", SECRET);
            std::env::set_var("ROCKY_T1919_COMPILE_CATALOG", CATALOG);
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
        }
        let out = out.expect("compiles");
        assert_eq!(out.models_detail.len(), 1, "PRECONDITION: the model loaded");
        let json = serde_json::to_string(&out).unwrap();
        assert!(!json.contains(SECRET), "leaked: {json}");
        let detail = serde_json::to_value(&out.models_detail[0]).unwrap();
        assert_eq!(detail["target"]["catalog"], CATALOG);
        assert_eq!(
            detail["strategy"]["timestamp_column"],
            "${ROCKY_T1919_COMPILE}"
        );
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
            None,
            None,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            strict_sources,
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
            None,
            None,
            true,
            false,
            None,
            true,
            None,
            &rocky_core::run_vars::RunVars::new(),
            true,
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
                None,
                None,
                false,
                None,
                false,
                None,
                &rocky_core::run_vars::RunVars::new(),
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
}
