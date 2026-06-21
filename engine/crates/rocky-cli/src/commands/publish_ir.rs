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
use rocky_ir::ProjectIr;

use super::ci_diff::project_ir_from_compile;
use super::compile::load_source_schemas_from_seed;

/// Names of models in `ir` whose `typed_columns` are empty.
///
/// A published snapshot is only enforceable if its models carry resolved
/// column types: the consumer-side contract diff in
/// [`crate::commands::imports_check`] matches a producer's dropped/narrowed
/// columns against what a consumer reads, and a model with no `typed_columns`
/// contributes nothing to that diff. A snapshot that is *entirely* degenerate
/// looks enforced to a consumer but can never fire E030/E031/E032 — the
/// "silent no-op" footgun this guard exists to surface.
///
/// Gated on IR shape (empty `typed_columns`), not on whether `--with-seed`
/// was passed: an intermediate-only project can resolve types without a seed,
/// and an *incomplete* seed leaves some leaves degenerate even when the flag
/// is set. The shape is the thing that actually matters.
fn degenerate_models(ir: &ProjectIr) -> Vec<String> {
    ir.models
        .iter()
        .filter(|m| m.typed_columns.is_empty())
        .map(|m| m.target.full_name())
        .collect()
}

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

    // Guard the silent-no-op footgun. A snapshot whose models carry no
    // `typed_columns` looks enforced to a consumer but can never detect a
    // dropped/narrowed column. If *every* model is degenerate the snapshot is
    // worthless and almost certainly a mistake (typically a missing
    // `--with-seed` on a self-contained DuckDB producer), so we refuse to
    // write it. If only some models are degenerate we publish but warn loudly,
    // naming the unenforceable targets.
    let degenerate = degenerate_models(&project_ir);
    if !project_ir.models.is_empty() && degenerate.len() == project_ir.models.len() {
        anyhow::bail!(
            "refusing to publish a degenerate IR snapshot: all {} model(s) have empty \
             typed_columns, so a consumer importing this snapshot can never detect a dropped \
             or narrowed column (the contract would look enforced but check nothing). For a \
             self-contained DuckDB producer, pass --with-seed so leaf models resolve concrete \
             column types.",
            project_ir.models.len(),
        );
    }
    if !degenerate.is_empty() {
        eprintln!(
            "warning: {} of {} model(s) have empty typed_columns and will not be enforceable \
             by a consumer's import contract: {}. Pass --with-seed (or supply source schemas) \
             so these models resolve concrete column types.",
            degenerate.len(),
            project_ir.models.len(),
            degenerate.join(", "),
        );
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_ir::{
        GovernanceConfig, MaterializationStrategy, ModelIr, RockyType, TargetRef, TypedColumn,
    };

    fn model(catalog: &str, table: &str, columns: &[&str]) -> ModelIr {
        let mut m = ModelIr::transformation(
            TargetRef {
                catalog: catalog.to_string(),
                schema: "core".to_string(),
                table: table.to_string(),
            },
            MaterializationStrategy::FullRefresh,
            Vec::new(),
            format!("SELECT * FROM raw.{table}"),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        m.typed_columns = columns
            .iter()
            .map(|c| TypedColumn {
                name: (*c).to_string(),
                data_type: RockyType::Int64,
                nullable: false,
            })
            .collect();
        m
    }

    fn project(models: Vec<ModelIr>) -> ProjectIr {
        ProjectIr {
            models,
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    #[test]
    fn degenerate_models_flags_only_empty_column_models() {
        let ir = project(vec![
            model("shop", "orders", &["id", "amount"]),
            model("shop", "customers", &[]), // degenerate
            model("shop", "items", &["id"]),
        ]);
        let degenerate = degenerate_models(&ir);
        assert_eq!(degenerate, vec!["shop.core.customers".to_string()]);
    }

    #[test]
    fn degenerate_models_empty_when_all_typed() {
        let ir = project(vec![
            model("shop", "orders", &["id"]),
            model("shop", "items", &["sku"]),
        ]);
        assert!(degenerate_models(&ir).is_empty());
    }

    #[test]
    fn degenerate_models_flags_every_model_when_all_empty() {
        // The dangerous "looks enforced but checks nothing" snapshot — the
        // case run_publish_ir hard-errors on.
        let ir = project(vec![
            model("shop", "orders", &[]),
            model("shop", "items", &[]),
        ]);
        let degenerate = degenerate_models(&ir);
        assert_eq!(degenerate.len(), 2);
        assert_eq!(degenerate.len(), ir.models.len());
    }
}
