//! Shared model-directory loader for CLI commands.
//!
//! The canonical "load every model in the project" path: top-level dir plus
//! one level of immediate subdirectories, including **both** `.sql` (sidecar
//! `.toml`) and `.rocky` DSL files. Commands that need the model list (but not
//! the resolved DAG) should use this instead of
//! [`rocky_core::models::load_models_from_dir`], which collects only `.sql`
//! files and therefore silently drops `.rocky` DSL models.

use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::models::Model;

/// Load all models under `models_dir` (top level + immediate subdirectories),
/// including `.rocky` DSL files. Subdirectory load failures are skipped
/// silently — same tolerance the previous per-command loops had.
pub fn load_project_models(models_dir: &Path) -> Result<Vec<Model>> {
    let mut all = rocky_compiler::project::load_dir_models(models_dir)
        .map_err(|e| anyhow::anyhow!("{e}"))
        .context(format!(
            "failed to load models from {}",
            models_dir.display()
        ))?;

    if let Ok(entries) = std::fs::read_dir(models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir()
                && let Ok(sub) = rocky_compiler::project::load_dir_models(&entry.path())
            {
                all.extend(sub);
            }
        }
    }
    Ok(all)
}
