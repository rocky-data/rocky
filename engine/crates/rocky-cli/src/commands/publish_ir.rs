//! `rocky publish-ir` — compile a producer project and write a snapshot of
//! its typed [`rocky_ir::ProjectIr`] for consumers to vendor.
//!
//! This is the producer half of the cross-team-contracts flow. A producer
//! runs `rocky publish-ir` to emit a JSON snapshot of its compiled project;
//! consumers vendor that file and reference it via an `[imports.<name>]`
//! block in their own `rocky.toml`. The consumer's `rocky compile` then
//! diffs the snapshot to catch columns the producer dropped that the
//! consumer still reads (diagnostic E030).
//!
//! The snapshot is the serialized `ProjectIr` — Rocky's existing typed wire
//! format. We deliberately reuse [`project_ir_from_compile`] (the same
//! lowering `rocky ci-diff` uses) rather than inventing a new artifact shape.
//!
//! ## Why `--with-seed` matters
//!
//! Snapshots are only useful if they carry `typed_columns`. Those are
//! resolved by the type checker from the project's source schemas. For a
//! self-contained DuckDB producer, those schemas come from `data/seed.sql`
//! via `--with-seed`. Without it, leaf models resolve to `Unknown` columns,
//! the snapshot's `typed_columns` are empty, and the consumer-side diff can
//! never detect a dropped column. The flag is therefore opt-in but
//! load-bearing for the contract to mean anything.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};

use rocky_compiler::compile::{self, CompilerConfig};

use super::ci_diff::project_ir_from_compile;
use super::compile::load_source_schemas_from_seed;

/// Execute `rocky publish-ir`.
///
/// Compiles the producer project at `models_dir` (optionally seeding source
/// schemas from `data/seed.sql` when `with_seed` is set), lowers the result
/// into a [`rocky_ir::ProjectIr`], and writes it as JSON to `out_path`.
///
/// # Errors
///
/// Returns an error if compilation fails, the seed loader fails, or the
/// snapshot cannot be written.
pub fn run_publish_ir(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    out_path: &Path,
    with_seed: bool,
) -> Result<()> {
    let source_schemas = if with_seed {
        load_source_schemas_from_seed(models_dir)?
    } else {
        HashMap::new()
    };

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(Path::to_path_buf),
        source_schemas,
        source_column_info: HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: Vec::new(),
        project_freshness_default: false,
    };

    let result = compile::compile(&config)?;
    if result.has_errors {
        anyhow::bail!(
            "cannot publish IR: producer project failed to compile ({} diagnostic(s))",
            result.diagnostics.len()
        );
    }

    let project_ir = project_ir_from_compile(&result);
    let recipe_hash = project_ir.recipe_hash().to_hex().to_string();

    if let Some(parent) = out_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create output directory {}", parent.display()))?;
    }

    let json = serde_json::to_string_pretty(&project_ir)
        .context("failed to serialize ProjectIr snapshot")?;
    std::fs::write(out_path, json)
        .with_context(|| format!("failed to write snapshot to {}", out_path.display()))?;

    println!(
        "published IR snapshot: {} model(s) -> {} (recipe_hash {})",
        project_ir.models.len(),
        out_path.display(),
        recipe_hash,
    );

    Ok(())
}
