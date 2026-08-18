//! `rocky docs` — generate project documentation as a single-page HTML catalog.
//!
//! Discovers models from the models directory, builds a documentation index,
//! renders it as self-contained HTML, and writes it to the output path.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::{info, warn};

use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::docs::{build_doc_index, generate_index_html};
use rocky_ir::ColumnInfo;

use crate::output::{DocsOutput, print_json};

/// Execute `rocky docs`: discover models and generate HTML documentation.
pub fn run_docs(
    config_path: &Path,
    models_dir: &Path,
    output_path: &Path,
    state_path: &Path,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Load config.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    // Discover models (top level + subdirs, including `.rocky` DSL files).
    let models = crate::models_loader::load_project_models(models_dir)?;

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
    // failing. A project that does not compile degrades to no column
    // tables, which is what every project got before this map was wired
    // up (#1444).
    let column_map = infer_column_map(&rocky_cfg, models_dir, state_path);
    let index = build_doc_index(&models, &rocky_cfg, column_map.as_ref(), Some(&column_docs));

    // A `[columns]` description whose column the compile step cannot see
    // has nowhere to render. That was this command's silent-drop bug
    // (#1444) — when it still happens for one column, say so.
    for model in &index.models {
        let Some(docs) = column_docs.get(&model.name) else {
            continue;
        };
        let rendered: HashSet<&str> = model.columns.iter().map(|c| c.name.as_str()).collect();
        let mut orphaned: Vec<&str> = docs
            .keys()
            .map(String::as_str)
            .filter(|name| !rendered.contains(*name))
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

    // Render HTML.
    let html = generate_index_html(&index);

    // Ensure parent directory exists.
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent).context(format!(
            "failed to create output directory {}",
            parent.display()
        ))?;
    }

    // Write the HTML file.
    std::fs::write(output_path, &html).context(format!(
        "failed to write documentation to {}",
        output_path.display()
    ))?;

    let duration_ms = start.elapsed().as_millis() as u64;

    if json {
        let output = DocsOutput {
            version: env!("CARGO_PKG_VERSION").into(),
            command: "docs".into(),
            output_path: output_path.display().to_string(),
            models_count,
            pipelines_count: rocky_cfg.pipelines.len(),
            duration_ms,
        };
        print_json(&output)?;
    } else {
        println!(
            "Documentation generated: {} ({} models, {} ms)",
            output_path.display(),
            models_count,
            duration_ms
        );
    }

    Ok(())
}

/// Best-effort column schemas for the docs page, from the offline compile
/// step ([`build_doc_index`]'s documented `column_map` source).
///
/// Mirrors `rocky compile`'s source-schema tiers minus the `--with-seed`
/// opt-in: the TTL-filtered schema cache when `[cache.schemas]` enables it,
/// otherwise empty (typecheck degrades to `UNKNOWN`, it does not fail).
///
/// Returns `None` when the project does not compile at all — `rocky docs`
/// is a reporting command, so it renders without column tables rather than
/// refusing.
fn infer_column_map(
    rocky_cfg: &rocky_core::config::RockyConfig,
    models_dir: &Path,
    state_path: &Path,
) -> Option<HashMap<String, Vec<ColumnInfo>>> {
    let compiler_cfg = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: crate::source_schemas::load_cached_source_schemas(
            &rocky_cfg.cache.schemas,
            state_path,
        ),
        source_column_info: HashMap::new(),
        mask: rocky_cfg.mask.clone(),
        allow_unmasked: rocky_cfg.classifications.allow_unmasked.clone(),
        project_freshness_default: rocky_cfg.freshness.has_default(),
        run_vars: rocky_core::run_vars::RunVars::new(),
    };
    let result = match compile::compile(&compiler_cfg) {
        Ok(result) => result,
        Err(error) => {
            // Project load is all-or-nothing (matching `rocky compile`), so
            // one unparseable model costs every model its column table.
            // Degrading silently is this command's original bug — name it.
            warn!(%error, "project does not compile; docs render without column metadata");
            return None;
        }
    };
    Some(
        result
            .type_check
            .typed_models
            .into_iter()
            .map(|(name, cols)| {
                let cols = cols
                    .into_iter()
                    .map(|c| ColumnInfo {
                        name: c.name,
                        data_type: c.data_type.to_string(),
                        nullable: c.nullable,
                    })
                    .collect();
                (name, cols)
            })
            .collect(),
    )
}
