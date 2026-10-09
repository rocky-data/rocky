//! `rocky docs` — generate project documentation.
//!
//! Discovers models from the models directory, compiles them offline for
//! column types and lineage, and writes one of:
//!
//! - a static documentation site (a directory; the default),
//! - a single self-contained HTML catalog (when the output path ends in
//!   `.html`), or
//! - the project graph as Parquet tables (`--format parquet`).

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::{info, warn};

use rocky_compiler::compile::{self, CompileResult, CompilerConfig};
use rocky_compiler::contracts::CompilerContract;
use rocky_core::docs::{build_doc_index, generate_index_html};
use rocky_core::docs_site::render_site;
use rocky_core::project_docs::{DocColumnEdge, DocContract, DocContractColumn, ProjectDocs};
use rocky_ir::ColumnInfo;

use super::docs_parquet::write_parquet_tables;
use crate::output::{DocsOutput, print_json};

/// Output shape of `rocky docs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum DocsFormat {
    /// A static site in a directory, or one HTML file when the output path
    /// ends in `.html`.
    Site,
    /// The project graph as Parquet tables in a directory.
    Parquet,
}

/// Execute `rocky docs`: discover models and generate documentation.
#[allow(clippy::too_many_arguments)]
pub fn run_docs(
    config_path: &Path,
    models_dir: &Path,
    output_path: &Path,
    state_path: &Path,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
    json: bool,
    selection: Option<&crate::selection::SelectionArgs>,
    format: DocsFormat,
    contracts_dir: Option<&Path>,
) -> Result<()> {
    let start = Instant::now();

    // Load config.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    // Discover models (top level + subdirs, including `.rocky` DSL files).
    // This load is strict: a model file that cannot be parsed — including a
    // malformed `.rocky` — fails the docs build here, before any rendering.
    // Only COMPILE failures degrade below; a docs page silently missing an
    // unparseable model would misrepresent the project.
    let models = crate::models_loader::load_project_models(models_dir, Some(&rocky_cfg.freshness))?;

    // Zero models is a refusal, not an empty page. The loader treats a
    // missing directory as empty, so a typo'd `--models` would render a
    // blank "catalog" at exit 0 — the selector-matched-nothing shape
    // #1428 closed for test/estimate/retention. Same wording as those
    // siblings. This also guards the preloaded compile below, which —
    // unlike `compile()` — has no `NoModels` rejection of its own.
    if models.is_empty() {
        anyhow::bail!("no models found in {}", models_dir.display());
    }

    // `--select` / `--exclude` narrow which models the catalog documents.
    // Selection resolves against the full project so graph operators see
    // every edge; an empty selection is refused like an empty project.
    let all_models = models;
    let models = match selection {
        Some(args) if args.is_active() => {
            let project =
                crate::selection::project_from_models(all_models.clone(), &args.run_vars)?;
            let selected = crate::selection::resolve(
                args,
                &project,
                models_dir,
                &crate::selection::StateContext {
                    config_path,
                    state_path,
                    cache_ttl_override,
                },
            )?;
            anyhow::ensure!(!selected.is_empty(), "the selection matched no models");
            all_models
                .iter()
                .filter(|m| selected.contains(&m.config.name))
                .cloned()
                .collect::<Vec<_>>()
        }
        _ => all_models.clone(),
    };

    let models_count = models.len();

    info!(
        models = models_count,
        output = %output_path.display(),
        "generating documentation"
    );

    // Per-column descriptions from sidecar `[columns]` tables (best-effort).
    let column_docs =
        rocky_core::models::load_column_docs_from_tree(models_dir).unwrap_or_default();

    // Column schemas come from the offline compile step — the same type
    // inference `rocky compile` reports. No warehouse connection: source
    // schemas load from the TTL-filtered schema cache when enabled, and a
    // cold cache degrades leaf models to `UNKNOWN` types instead of
    // failing. A project that loads but does not compile degrades to no
    // column tables, which is what every project got before this map was
    // wired up (#1444).
    // Compile the whole project: a selected model's columns infer from its
    // unselected upstreams.
    let compiled = compile_for_docs(
        &rocky_cfg,
        all_models.clone(),
        models_dir,
        state_path,
        cache_ttl_override,
        run_vars,
    );
    let column_map = compiled.as_ref().map(column_map_of);
    let index = build_doc_index(&models, &rocky_cfg, column_map.as_ref(), Some(&column_docs));
    let contracts = load_doc_contracts(&all_models, contracts_dir);
    let selected: HashSet<&str> = models.iter().map(|m| m.config.name.as_str()).collect();
    let all_names: HashSet<&str> = all_models.iter().map(|m| m.config.name.as_str()).collect();
    let lineage = compiled
        .as_ref()
        .map(|result| lineage_edges_of(result, &selected, &all_names))
        .unwrap_or_default();
    let docs = ProjectDocs::build(
        index,
        &models,
        models_dir,
        &contracts,
        lineage,
        compiled.is_some(),
    );
    let index = &docs.index;

    // A `[columns]` description whose column the compile step cannot see
    // has nowhere to render. That was this command's silent-drop bug
    // (#1444) — when it still happens for one column, say so. Matching is
    // ASCII-case-insensitive, like every other column lookup in Rocky
    // (`rocky_core::column_map`).
    for model in &index.models {
        let Some(docs) = column_docs.get(&model.name) else {
            continue;
        };
        let rendered: HashSet<String> = model
            .columns
            .iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect();
        let mut orphaned: Vec<&str> = docs
            .keys()
            .filter(|name| !rendered.contains(&name.to_ascii_lowercase()))
            .map(String::as_str)
            .collect();
        if !orphaned.is_empty() {
            orphaned.sort_unstable();
            warn!(
                model = %model.name,
                columns = orphaned.join(", "),
                "sidecar [columns] descriptions do not match any compiled column and are not rendered"
            );
        }
    }

    let single_file = format == DocsFormat::Site
        && output_path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("html"));
    let (written, shape): (Vec<String>, &str) = match format {
        DocsFormat::Parquet => (write_parquet_tables(&docs, output_path)?, "parquet"),
        DocsFormat::Site if single_file => {
            if let Some(parent) = output_path.parent() {
                std::fs::create_dir_all(parent).context(format!(
                    "failed to create output directory {}",
                    parent.display()
                ))?;
            }
            std::fs::write(output_path, generate_index_html(&docs.index)).context(format!(
                "failed to write documentation to {}",
                output_path.display()
            ))?;
            (
                vec![
                    output_path
                        .file_name()
                        .map_or_else(String::new, |n| n.to_string_lossy().into_owned()),
                ],
                "html",
            )
        }
        DocsFormat::Site => (write_site(&docs, output_path)?, "site"),
    };

    let duration_ms = start.elapsed().as_millis() as u64;
    let sources_count = docs.sources.len();

    if json {
        let output = DocsOutput {
            version: env!("CARGO_PKG_VERSION").into(),
            command: "docs".into(),
            output_path: output_path.display().to_string(),
            models_count,
            pipelines_count: rocky_cfg.pipelines.len(),
            duration_ms,
            format: shape.into(),
            sources_count,
            files: written,
        };
        print_json(&output)?;
    } else {
        println!(
            "Documentation generated: {} ({shape}, {} models, {} sources, {} files, {} ms)",
            output_path.display(),
            models_count,
            sources_count,
            written.len(),
            duration_ms
        );
    }

    Ok(())
}

/// Write the site into `dir`. Page files left over from an earlier run
/// (`models/*.html`, `sources/*.html`) are removed first so a deleted model
/// does not keep a stale page; nothing else in `dir` is touched.
fn write_site(docs: &ProjectDocs, dir: &Path) -> Result<Vec<String>> {
    for sub in ["models", "sources"] {
        let pages = dir.join(sub);
        let Ok(entries) = std::fs::read_dir(&pages) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().is_some_and(|e| e == "html") && path.is_file() {
                std::fs::remove_file(&path)
                    .with_context(|| format!("failed to remove stale page {}", path.display()))?;
            }
        }
    }
    let files = render_site(docs);
    for file in &files {
        let path = dir.join(&file.path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).context(format!(
                "failed to create output directory {}",
                parent.display()
            ))?;
        }
        std::fs::write(&path, &file.content)
            .context(format!("failed to write {}", path.display()))?;
    }
    Ok(files.into_iter().map(|f| f.path).collect())
}

/// Contracts for the docs: the ones auto-discovered next to each model, then
/// those in `contracts_dir` (which win on a clash, as in `rocky compile`).
/// A contract that cannot be read is skipped with a warning; docs never fail
/// on it.
fn load_doc_contracts(
    models: &[rocky_core::models::Model],
    contracts_dir: Option<&Path>,
) -> HashMap<String, DocContract> {
    let mut map: HashMap<String, CompilerContract> =
        match rocky_compiler::contracts::discover_contracts_from_models(models) {
            Ok(found) => found,
            Err(error) => {
                warn!(%error, "model contracts could not be read; docs omit them");
                HashMap::new()
            }
        };
    if let Some(dir) = contracts_dir {
        match rocky_compiler::contracts::load_contracts(dir) {
            Ok(found) => map.extend(found),
            Err(error) => warn!(%error, "contracts directory could not be read; docs omit it"),
        }
    }
    map.into_iter()
        .map(|(name, c)| {
            (
                name,
                DocContract {
                    columns: c
                        .columns
                        .into_iter()
                        .map(|col| DocContractColumn {
                            name: col.name,
                            type_name: col.type_name,
                            nullable: col.nullable,
                            description: col.description,
                        })
                        .collect(),
                    required: c.rules.required,
                    protected: c.rules.protected,
                    no_new_nullable: c.rules.no_new_nullable,
                },
            )
        })
        .collect()
}

/// Column lineage from the compiled graph, restricted to the documented
/// models. An edge is kept when its target is selected and its source is
/// either selected or not a project model at all (an external table). Edges
/// from unselected project models are dropped, so they never show up as
/// sources.
fn lineage_edges_of(
    result: &CompileResult,
    selected: &HashSet<&str>,
    all_models: &HashSet<&str>,
) -> Vec<DocColumnEdge> {
    result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| {
            selected.contains(&*e.target.model)
                && (selected.contains(&*e.source.model) || !all_models.contains(&*e.source.model))
        })
        .map(|e| DocColumnEdge {
            source_model: e.source.model.to_string(),
            source_column: e.source.column.to_string(),
            target_model: e.target.model.to_string(),
            target_column: e.target.column.to_string(),
            transform: e.transform.to_string(),
        })
        .collect()
}

/// Best-effort column schemas for the docs page, from the offline compile
/// step ([`build_doc_index`]'s documented `column_map` source).
///
/// Mirrors `rocky compile`'s source-schema tiers minus the `--with-seed`
/// opt-in: the TTL-filtered schema cache when `[cache.schemas]` enables it
/// (honouring the global `--cache-ttl` override), otherwise empty
/// (typecheck degrades to `UNKNOWN`, it does not fail).
///
/// Returns `None` — and warns — when the project does not compile cleanly.
/// That covers both a hard compile error and a result carrying error
/// diagnostics: an errored compile can hold sentinel-derived types (a
/// missing `@var` becomes a parseable `NULL`, E028), and publishing those
/// as the model's documented schema would be wrong, not merely incomplete.
/// `rocky docs` is a reporting command, so it renders without column tables
/// (and without lineage) rather than refusing.
fn compile_for_docs(
    rocky_cfg: &rocky_core::config::RockyConfig,
    models: Vec<rocky_core::models::Model>,
    models_dir: &Path,
    state_path: &Path,
    cache_ttl_override: Option<u64>,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Option<CompileResult> {
    let schema_cache_cfg = rocky_cfg
        .cache
        .schemas
        .clone()
        .with_ttl_override(cache_ttl_override);
    let compiler_cfg = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        required_explicit_contract_model: None,
        source_schemas: crate::source_schemas::load_cached_source_schemas(
            &schema_cache_cfg,
            state_path,
        ),
        mask: rocky_cfg.mask.clone(),
        allow_unmasked: rocky_cfg.classifications.allow_unmasked.clone(),
        project_freshness: rocky_cfg.freshness.clone(),
        run_vars: run_vars.clone(),
        source_provenance: Default::default(),
        preserve_authored_sql: true,
        external_dependencies: Default::default(),
    };
    // The models are already loaded (and were loaded strictly), so compile
    // them directly instead of re-reading the directory — one load, and the
    // compile describes exactly the set the docs page lists.
    let result = match compile::compile_preloaded_models(models, &compiler_cfg) {
        Ok(result) => result,
        Err(error) => {
            // Compilation is all-or-nothing (matching `rocky compile`), so
            // one failing model costs every model its column table.
            // Degrading silently is this command's original bug — name it.
            warn!(%error, "project does not compile; docs render without column metadata");
            return None;
        }
    };
    if result.has_errors {
        let first = result
            .diagnostics
            .iter()
            .find(|d| d.is_error())
            .map_or_else(String::new, |d| format!(": {}", d.message));
        warn!(
            "project compiles with errors; docs render without column metadata{first} \
             (pass --var NAME=VALUE if a run variable is required)"
        );
        return None;
    }
    Some(result)
}

/// Per-model column schemas from a successful compile.
fn column_map_of(result: &CompileResult) -> HashMap<String, Vec<ColumnInfo>> {
    result
        .type_check
        .typed_models
        .iter()
        .map(|(name, cols)| {
            let cols = cols
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: c.data_type.to_string(),
                    nullable: c.nullable,
                })
                .collect();
            (name.clone(), cols)
        })
        .collect()
}
