//! `rocky import-dbt` — import a dbt project as a runnable Rocky repo.
//!
//! Walks the source dbt project, translates each model body to plain SQL +
//! sidecar TOML, and emits a self-contained Rocky directory layout under
//! `--output-dir`:
//!
//! ```text
//! <out>/
//! ├── rocky.toml
//! ├── models/
//! │   ├── _defaults.toml
//! │   ├── <name>.sql
//! │   └── <name>.toml
//! ├── seeds/
//! └── MIGRATION-NOTES.md
//! ```
//!
//! By design, the importer translates models, refs/sources,
//! `{{ config() }}`, the canonical dbt generic tests (`unique`, `not_null`,
//! `accepted_values`, `relationships`), seeds, and `profiles.yml` adapter
//! shape. It does **not** expand dbt macros, walk `dbt_packages/`, or port
//! singular tests — Rocky has no Jinja runtime, so those need a manual
//! pass. Known limitations are listed in `MIGRATION-NOTES.md`. The goal is
//! a `rocky compile`-clean repo, not a line-for-line dbt clone.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::Result;

use rocky_compiler::import::{
    dbt,
    dbt::{
        HookKind, ImportDbtStructuredWarning as CompilerStructuredWarning, ImportResult,
        ImportWarning, MicrobatchMode, WarningCategory,
    },
    dbt_manifest, dbt_profiles,
    dbt_profiles::{AdapterKind, ProfileResolution, StubReason},
    emit,
    emit::{EmitInputs, OverwritePolicy},
    report,
};
use rocky_core::models::TargetConfig;

use crate::output::{
    ImportDbtEmission, ImportDbtFailure, ImportDbtHookKind, ImportDbtOutput,
    ImportDbtStructuredWarning, ImportDbtWarning, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky import-dbt`.
#[allow(clippy::too_many_arguments)]
pub fn run_import_dbt(
    dbt_project: &Path,
    output_dir: &Path,
    manifest_path: Option<&Path>,
    no_manifest: bool,
    target_adapter: Option<&str>,
    overwrite: bool,
    skip_unit_tests: bool,
    microbatch_as: &str,
    output_json: bool,
) -> Result<()> {
    let microbatch_mode = MicrobatchMode::from_flag(microbatch_as);

    // Resolve adapter shape — profile detection unless --target-adapter overrides.
    let profile = resolve_profile(dbt_project, target_adapter);
    let adapter_override_label = target_adapter.map(std::string::ToString::to_string);

    let default_target = default_target_from_profile(&profile);

    // Run the existing importer to produce the per-model translation result.
    let mut import_result = run_importer(
        dbt_project,
        &default_target,
        manifest_path,
        no_manifest,
        skip_unit_tests,
        microbatch_mode,
    )?;

    // A `profiles.yml` that was present but couldn't be resolved fell back to
    // a stub DuckDB adapter. Surface it loudly (a warning + in `--output json`)
    // so the migration never silently defaults to duckdb.
    if let Some(reason) = &profile.fallback_reason {
        tracing::warn!(reason = %reason, "dbt profiles.yml fell back to a stub DuckDB adapter");
        import_result.warnings.push(ImportWarning {
            model: "<profiles.yml>".to_string(),
            category: WarningCategory::ProfileFallback,
            message: reason.clone(),
            suggestion: Some(
                "edit the [adapter] block in the emitted rocky.toml to point at your real warehouse".to_string(),
            ),
        });
    }

    // Capture which models had their `view` materialization flattened to
    // FullRefresh by the importer — we re-rewrite those to Ephemeral at
    // emit time per the importer's `view → ephemeral` mapping.
    let view_models = collect_view_flattened_models(&import_result);

    let policy = if overwrite {
        OverwritePolicy::ReplaceContents
    } else {
        OverwritePolicy::Reject
    };

    let emission = emit::emit_repo(&EmitInputs {
        dbt_project_dir: dbt_project,
        out_dir: output_dir,
        overwrite: policy,
        profile: &profile,
        default_catalog: &default_target.catalog,
        default_schema: &default_target.schema,
        import: &import_result,
        view_models_to_make_ephemeral: view_models,
        adapter_override_label,
    })
    .map_err(|e| anyhow::anyhow!("{e}"))?;

    if output_json {
        let warning_details: Vec<ImportDbtWarning> = import_result
            .warnings
            .iter()
            .map(|w| ImportDbtWarning {
                model: w.model.clone(),
                category: format!("{:?}", w.category),
                message: w.message.clone(),
                suggestion: w.suggestion.clone(),
            })
            .collect();

        let structured_warnings: Vec<ImportDbtStructuredWarning> = import_result
            .structured_warnings
            .iter()
            .map(to_cli_structured_warning)
            .collect();

        let failed_details: Vec<ImportDbtFailure> = import_result
            .failed
            .iter()
            .map(|f| ImportDbtFailure {
                name: f.name.clone(),
                reason: f.reason.clone(),
            })
            .collect();

        let migration_report = report::generate_report(&import_result, None, None);

        let emission_payload = ImportDbtEmission {
            out_dir: emission.out_dir.display().to_string(),
            rocky_toml_path: emission.rocky_toml_path.display().to_string(),
            migration_notes_path: emission.migration_notes_path.display().to_string(),
            models_translated_count: emission.models_translated,
            models_skipped_count: emission.models_skipped,
            seeds_copied_count: emission.seeds_copied,
            adapter_type: profile.kind.rocky_type().to_string(),
            original_dbt_adapter_type: profile.original_type.clone(),
            required_env_vars: profile.env_vars.vars.clone(),
        };

        let output = ImportDbtOutput {
            version: VERSION.to_string(),
            command: "import-dbt".to_string(),
            import_method: format!("{:?}", import_result.import_method),
            project_name: import_result.project_name.clone(),
            dbt_version: import_result.dbt_version.clone(),
            imported: import_result.imported.len(),
            warnings: import_result.warnings.len(),
            failed: import_result.failed.len(),
            sources_found: import_result.sources_found,
            sources_mapped: import_result.sources_mapped,
            tests_found: import_result.tests_found,
            tests_converted: import_result.tests_converted,
            tests_converted_custom: import_result.tests_converted_custom,
            tests_skipped: import_result.tests_skipped,
            unit_tests_found: import_result.unit_tests_found,
            unit_tests_converted: import_result.unit_tests_converted,
            unit_tests_skipped: import_result.unit_tests_skipped,
            constructs_dropped: import_result.constructs_dropped,
            contracts_dropped: import_result.contracts_dropped,
            macros_detected: import_result.macros_detected,
            imported_models: import_result
                .imported
                .iter()
                .map(|m| m.name.clone())
                .collect(),
            warning_details,
            structured_warnings,
            failed_details,
            report: serde_json::to_value(&migration_report).unwrap_or(serde_json::Value::Null),
            emission: Some(emission_payload),
        };
        print_json(&output)?;
    } else {
        let migration_report = report::generate_report(&import_result, None, None);
        print!("{}", report::format_report(&migration_report));

        println!(
            "Output:  {} models translated, {} seeds copied → {}",
            emission.models_translated,
            emission.seeds_copied,
            emission.out_dir.display()
        );
        println!(
            "         rocky.toml         → {}",
            emission.rocky_toml_path.display()
        );
        println!(
            "         MIGRATION-NOTES.md → {}",
            emission.migration_notes_path.display()
        );
        println!();

        if !import_result.warnings.is_empty() {
            println!("Warnings:");
            for w in &import_result.warnings {
                println!("  {}: {}", w.model, w.message);
                if let Some(ref s) = w.suggestion {
                    println!("    -> {s}");
                }
            }
            println!();
        }

        if !import_result.failed.is_empty() {
            println!("Failed:");
            for f in &import_result.failed {
                println!("  {}: {}", f.name, f.reason);
            }
            println!();
        }
    }

    Ok(())
}

fn resolve_profile(dbt_project: &Path, target_adapter: Option<&str>) -> ProfileResolution {
    if let Some(forced) = target_adapter {
        let kind = AdapterKind::from_dbt_type(forced);
        return match kind {
            AdapterKind::DuckDb if forced.eq_ignore_ascii_case("duckdb") => {
                dbt_profiles::stub_resolution(StubReason::ForcedDuckDb)
            }
            kind => dbt_profiles::resolution_for_kind(
                kind,
                &format!("{forced} (forced via --target-adapter)"),
            ),
        };
    }

    // Honor `dbt_project.yml`'s `profile:` key so the emitted adapter matches
    // the warehouse dbt actually targets — not the alphabetically-first profile
    // in profiles.yml.
    let profile_name = dbt::read_project_profile_name(dbt_project);
    match dbt_profiles::resolve_from_project(dbt_project, profile_name.as_deref()) {
        Some(resolved) => resolved,
        None => dbt_profiles::stub_resolution(StubReason::ProfilesAbsent),
    }
}

fn default_target_from_profile(profile: &ProfileResolution) -> TargetConfig {
    TargetConfig {
        catalog: profile
            .database
            .clone()
            .unwrap_or_else(|| "warehouse".to_string()),
        schema: profile.schema.clone().unwrap_or_else(|| "main".to_string()),
        table: String::new(),
    }
}

fn run_importer(
    dbt_project: &Path,
    default_target: &TargetConfig,
    manifest_path: Option<&Path>,
    no_manifest: bool,
    skip_unit_tests: bool,
    microbatch_mode: MicrobatchMode,
) -> Result<ImportResult> {
    if no_manifest {
        // The regex (`--no-manifest`) path keeps the default microbatch
        // mapping; `--microbatch-as=time_interval` needs the compiled body
        // only the manifest path carries.
        return dbt::import_dbt_project(dbt_project, default_target)
            .map_err(|e| anyhow::anyhow!("{e}"));
    }

    let manifest_file = match manifest_path {
        Some(p) => {
            if p.exists() {
                Some(p.to_path_buf())
            } else {
                anyhow::bail!("manifest file not found: {}", p.display());
            }
        }
        None => {
            let default = dbt_project.join("target/manifest.json");
            if default.exists() {
                Some(default)
            } else {
                None
            }
        }
    };

    if let Some(ref mf) = manifest_file {
        let manifest = dbt_manifest::parse_manifest(mf).map_err(|e| anyhow::anyhow!("{e}"))?;
        let mut result =
            dbt::import_from_manifest(&manifest, default_target, skip_unit_tests, microbatch_mode);
        // The manifest path doesn't itself walk schema.yml files for tests.
        // Apply the dbt-tests pass against the project root so manifest
        // imports also pick up the four canonical generic tests.
        dbt::apply_dbt_tests(dbt_project, default_target, &mut result);
        Ok(result)
    } else {
        dbt::import_dbt_project(dbt_project, default_target).map_err(|e| anyhow::anyhow!("{e}"))
    }
}

fn collect_view_flattened_models(_result: &ImportResult) -> BTreeSet<String> {
    // Wave 2: `view` maps to `StrategyConfig::View` directly, so there's
    // no longer a flattening step to compensate for at emit time. The
    // BTreeSet is retained on `EmitInputs` for back-compat and always
    // empty here.
    BTreeSet::new()
}

/// Translate a `rocky-compiler`-side structured warning into the
/// CLI-output flavor (which has `JsonSchema` derives + serde tags
/// suitable for codegen cascade).
fn to_cli_structured_warning(w: &CompilerStructuredWarning) -> ImportDbtStructuredWarning {
    match w {
        CompilerStructuredWarning::UnsupportedMaterialization {
            model,
            dbt_materialization,
            action,
        } => ImportDbtStructuredWarning::UnsupportedMaterialization {
            model: model.clone(),
            dbt_materialization: dbt_materialization.clone(),
            action: action.clone(),
        },
        CompilerStructuredWarning::DroppedDatabricksTags { model, tags } => {
            ImportDbtStructuredWarning::DroppedDatabricksTags {
                model: model.clone(),
                tags: tags.clone(),
            }
        }
        CompilerStructuredWarning::DroppedHook {
            model,
            hook_kind,
            sql,
        } => ImportDbtStructuredWarning::DroppedHook {
            model: model.clone(),
            hook_kind: match hook_kind {
                HookKind::Pre => ImportDbtHookKind::Pre,
                HookKind::Post => ImportDbtHookKind::Post,
            },
            sql: sql.clone(),
        },
        CompilerStructuredWarning::DroppedOnSchemaChange {
            model,
            dbt_value,
            rocky_equivalent,
        } => ImportDbtStructuredWarning::DroppedOnSchemaChange {
            model: model.clone(),
            dbt_value: dbt_value.clone(),
            rocky_equivalent: rocky_equivalent.clone(),
        },
        CompilerStructuredWarning::UnresolvableMacro {
            model,
            macro_name,
            first_call_site_line,
        } => ImportDbtStructuredWarning::UnresolvableMacro {
            model: model.clone(),
            macro_name: macro_name.clone(),
            first_call_site_line: *first_call_site_line,
        },
        CompilerStructuredWarning::MicrobatchMissingEventTime { model } => {
            ImportDbtStructuredWarning::MicrobatchMissingEventTime {
                model: model.clone(),
            }
        }
        CompilerStructuredWarning::MicrobatchMapped { model, mapped_to } => {
            ImportDbtStructuredWarning::MicrobatchMapped {
                model: model.clone(),
                mapped_to: mapped_to.clone(),
            }
        }
        CompilerStructuredWarning::DroppedConstruct {
            construct,
            name,
            detail,
        } => ImportDbtStructuredWarning::DroppedConstruct {
            construct: construct.clone(),
            name: name.clone(),
            detail: detail.clone(),
        },
        CompilerStructuredWarning::DroppedContract {
            model,
            typed_columns,
            constraints,
            contract_path,
        } => ImportDbtStructuredWarning::DroppedContract {
            model: model.clone(),
            typed_columns: *typed_columns,
            constraints: *constraints,
            contract_path: contract_path.clone(),
        },
    }
}
