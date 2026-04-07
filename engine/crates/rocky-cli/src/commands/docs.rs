//! `rocky docs` — generate project documentation as a single-page HTML catalog.
//!
//! Discovers models from the models directory, builds a documentation index,
//! renders it as self-contained HTML, and writes it to the output path.

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::docs::{build_doc_index, generate_index_html};

use crate::output::{DocsOutput, print_json};

/// Execute `rocky docs`: discover models and generate HTML documentation.
pub fn run_docs(
    config_path: &Path,
    models_dir: &Path,
    output_path: &Path,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Load config.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    // Discover models.
    let models = rocky_core::models::load_models_from_dir(models_dir).context(format!(
        "failed to discover models in {}",
        models_dir.display()
    ))?;

    let models_count = models.len();

    info!(
        models = models_count,
        output = %output_path.display(),
        "generating documentation"
    );

    // Build the documentation index (no column map — would need warehouse connection).
    let index = build_doc_index(&models, &rocky_cfg, None);

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
