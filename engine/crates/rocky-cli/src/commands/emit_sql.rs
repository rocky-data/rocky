//! `rocky emit-sql` — render the runnable SQL each transformation model would
//! emit, without a warehouse connection or the Rocky engine to run it.
//!
//! This is the **tested exit path**: a project can always be reduced to plain,
//! dialect-correct SQL files that feed a hand-SQL / dbt fallback, so depending
//! on Rocky is never a one-way door. The SQL is generated through the same
//! [`sql_gen::generate_transformation_sql_with_warehouse`] path `rocky run`
//! uses, and declared surrogate-key columns are wrapped in exactly as they are
//! at materialization.
//!
//! ## Scope of the runnable guarantee
//!
//! Full-refresh models emit a complete `CREATE OR REPLACE TABLE … AS …` that
//! runs as-is against a fresh warehouse and matches what a run executes in the
//! resolved dialect (see the dialect note below).
//! Incremental and merge models emit their **steady-state** statement (a bare
//! `INSERT` / `MERGE` that operates on an existing target); `rocky run`
//! bootstraps the target table on first build and threads the incremental
//! watermark from state, neither of which a static emit can reproduce. Those
//! files carry a leading note to that effect.
//!
//! The dialect is the project's configured target adapter type (resolved from
//! `rocky.toml` without credentials); with no resolvable config it defaults to
//! DuckDB. All models render in this one resolved dialect, so for a project
//! whose models target more than one adapter, the emitted SQL matches
//! `rocky run` only for the models whose target uses that dialect. Output is
//! one `<model>.sql` file per model when `--out-dir` is given,
//! otherwise the concatenated SQL is printed to stdout, both in dependency
//! order. Models that produce no standalone SQL — ephemeral (inlined as CTEs)
//! or strategies that cannot render offline (e.g. Snowflake `DynamicTable`,
//! which needs a live compute-warehouse name) — are reported on stderr rather
//! than silently dropped.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::models::SurrogateKeySpec;
use rocky_core::sql_gen;
use tracing::{debug, info};

use super::plan::dialect_for_adapter_type;
use crate::registry;

/// Resolve the project's target dialect from `rocky.toml` (no credentials),
/// falling back to DuckDB when no config resolves. Mirrors the resolution in
/// [`super::plan::plan_preview_output`] so emitted SQL matches the plan preview.
fn resolve_dialect(config_path: Option<&Path>) -> Box<dyn rocky_core::traits::SqlDialect> {
    let adapter_type = config_path
        .and_then(|p| rocky_core::config::load_rocky_config(p).ok())
        .and_then(|cfg| {
            let target_adapter_name = registry::resolve_replication_pipeline(&cfg, None)
                .ok()
                .map(|(_, pipeline)| pipeline.target.adapter.clone());
            target_adapter_name
                .and_then(|name| cfg.adapters.get(&name).map(|a| a.adapter_type.clone()))
                .or_else(|| cfg.adapters.values().next().map(|a| a.adapter_type.clone()))
        })
        .unwrap_or_else(|| "duckdb".to_string());
    dialect_for_adapter_type(&adapter_type)
}

/// One model's emitted SQL: its name and the joined runnable statement(s).
struct EmittedModel {
    name: String,
    sql: String,
    /// `true` for incremental/merge-style statements that operate on an
    /// existing target (bare `INSERT`/`MERGE`). `rocky run` bootstraps the
    /// target table on first build and threads the incremental watermark from
    /// state — neither of which a static emit reproduces — so this SQL is the
    /// steady-state operation, not a from-scratch build.
    assumes_existing_target: bool,
}

/// The result of an emit: the rendered models plus any that produced no
/// standalone SQL (ephemeral, or strategies that cannot render offline), so the
/// caller can surface what was *not* written rather than silently dropping it.
struct EmitResult {
    models: Vec<EmittedModel>,
    skipped: Vec<String>,
}

/// Whether a strategy emits a statement that assumes the target already exists
/// (so the emitted SQL is the steady-state op, not a complete from-scratch build).
fn assumes_existing_target(strategy: &rocky_ir::MaterializationStrategy) -> bool {
    use rocky_ir::MaterializationStrategy::*;
    matches!(
        strategy,
        Incremental { .. } | Merge { .. } | DeleteInsert { .. }
    )
}

/// Compile `models_dir` in-process and render the runnable SQL per
/// transformation model, applying declared surrogate-key columns so the output
/// matches what `rocky run` would execute. `model_filter` restricts to a single
/// model by name.
fn emit_models(
    config_path: Option<&Path>,
    models_dir: &Path,
    model_filter: Option<&str>,
) -> Result<EmitResult> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let dialect = resolve_dialect(config_path);

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
        project_freshness_default: false,
    };
    let result = match compile::compile(&config) {
        Ok(r) => r,
        // A replication-only project has no compiled transformation models —
        // there is no transformation SQL to emit. Return empty with a note
        // rather than erroring.
        Err(rocky_compiler::compile::CompileError::Project(
            rocky_compiler::project::ProjectError::NoModels { .. },
        )) => {
            info!(
                models_dir = %models_dir.display(),
                "emit-sql: project has no compiled transformation models — nothing to emit"
            );
            return Ok(EmitResult {
                models: Vec::new(),
                skipped: Vec::new(),
            });
        }
        Err(e) => {
            return Err(anyhow::Error::from(e).context("failed to compile models for emit-sql"));
        }
    };

    // Iterate in the project's topological execution order so the emitted files
    // are runnable in sequence (a model never precedes one it reads). Models not
    // listed in `execution_order` (defensive) fall to the end in IR order.
    let project_ir = super::ci_diff::project_ir_from_compile(&result);
    let ir_by_name: HashMap<&str, &rocky_ir::ModelIr> = project_ir
        .models
        .iter()
        .map(|m| (m.name.as_ref(), m))
        .collect();
    let mut ordered: Vec<&rocky_ir::ModelIr> = result
        .project
        .execution_order
        .iter()
        .filter_map(|name| ir_by_name.get(name.as_str()).copied())
        .collect();
    for m in &project_ir.models {
        if !result
            .project
            .execution_order
            .iter()
            .any(|n| n == m.name.as_ref())
        {
            ordered.push(m);
        }
    }

    // Declared surrogate-key specs, applied per model so the emitted SELECT is
    // wrapped identically to the materialization path (see `apply_surrogate_keys`).
    let surrogate_keys: HashMap<String, Vec<SurrogateKeySpec>> =
        rocky_core::models::load_surrogate_keys_from_dir(models_dir)
            .context("invalid surrogate_key configuration")?;

    let mut emitted = Vec::new();
    let mut skipped = Vec::new();
    for model_ir in ordered {
        let model_name = model_ir.name.as_ref();
        if let Some(f) = model_filter
            && model_name != f
        {
            continue;
        }

        let mut model_ir = model_ir.clone();
        if let Some(specs) = surrogate_keys.get(model_name) {
            rocky_core::models::apply_surrogate_keys(&mut model_ir, specs, dialect.as_ref());
        }

        match sql_gen::generate_transformation_sql_with_warehouse(&model_ir, dialect.as_ref(), None)
        {
            Ok(stmts) if stmts.is_empty() => {
                // Ephemeral models inline as CTEs — no standalone statement.
                debug!(
                    model = model_name,
                    "emit-sql: no standalone statement (ephemeral)"
                );
                skipped.push(format!("{model_name} (ephemeral — inlined as a CTE)"));
            }
            Ok(stmts) => {
                // Join multi-statement strategies (e.g. predrop + CTAS) into one
                // runnable script, each statement terminated with `;`.
                let sql = stmts
                    .iter()
                    .map(|s| format!("{};", s.trim_end_matches(';')))
                    .collect::<Vec<_>>()
                    .join("\n\n");
                emitted.push(EmittedModel {
                    name: model_name.to_string(),
                    sql,
                    assumes_existing_target: assumes_existing_target(&model_ir.materialization),
                });
            }
            Err(e) => {
                debug!(
                    model = model_name,
                    error = %e,
                    "emit-sql: skipping model whose SQL cannot be rendered offline"
                );
                skipped.push(format!("{model_name} (cannot render offline: {e})"));
            }
        }
    }
    Ok(EmitResult {
        models: emitted,
        skipped,
    })
}

/// True when `name` is safe to use as a single `<name>.sql` filename component:
/// non-empty, no path separators, no parent-/absolute-path escape. Model names
/// are normally plain identifiers; a name that fails this is anomalous, so the
/// `--out-dir` path skips it with a report rather than risking a `std::fs::write`
/// outside `out_dir` via a traversing join.
fn is_safe_file_stem(name: &str) -> bool {
    !name.is_empty()
        && !name.contains('/')
        && !name.contains('\\')
        && !Path::new(name).is_absolute()
        && Path::new(name).components().count() == 1
        && Path::new(name).file_name().and_then(|n| n.to_str()) == Some(name)
}

/// `rocky emit-sql` entry point. Writes one `<model>.sql` per model into
/// `out_dir` when given, otherwise prints the concatenated SQL to stdout.
pub fn run_emit_sql(
    config_path: Option<&Path>,
    models_dir: &Path,
    model_filter: Option<&str>,
    out_dir: Option<&Path>,
) -> Result<()> {
    let EmitResult {
        models,
        mut skipped,
    } = emit_models(config_path, models_dir, model_filter)?;

    if models.is_empty() {
        println!("emit-sql: no transformation SQL to emit.");
        report_skipped(&skipped);
        return Ok(());
    }

    match out_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("failed to create out-dir {}", dir.display()))?;
            let mut written = 0usize;
            for m in &models {
                if !is_safe_file_stem(&m.name) {
                    skipped.push(format!(
                        "{} (unsafe model name for a file path; not written)",
                        m.name
                    ));
                    continue;
                }
                let path = dir.join(format!("{}.sql", m.name));
                std::fs::write(&path, format!("{}\n", file_body(m)))
                    .with_context(|| format!("failed to write {}", path.display()))?;
                written += 1;
            }
            println!(
                "emit-sql: wrote {written} model(s) to {} in dependency order",
                dir.display()
            );
        }
        None => {
            for (i, m) in models.iter().enumerate() {
                if i > 0 {
                    println!();
                }
                println!("-- model: {}", m.name);
                println!("{}", file_body(m));
            }
        }
    }
    report_skipped(&skipped);
    Ok(())
}

/// The SQL written for one model, prefixed with a note for incremental/merge
/// statements that operate on an existing target (so a reader running the file
/// against a fresh warehouse understands why a bare `INSERT`/`MERGE` expects the
/// table to already exist).
fn file_body(m: &EmittedModel) -> String {
    if m.assumes_existing_target {
        format!(
            "-- NOTE: incremental/merge statement — operates on an existing target.\n\
             -- `rocky run` bootstraps the table on first build and threads the\n\
             -- incremental watermark from state; this static SQL does neither.\n{}",
            m.sql
        )
    } else {
        m.sql.clone()
    }
}

/// Surface models that produced no standalone SQL on stderr, so the user never
/// mistakes the emitted set for the complete project.
fn report_skipped(skipped: &[String]) {
    if skipped.is_empty() {
        return;
    }
    eprintln!("emit-sql: {} model(s) not emitted:", skipped.len());
    for s in skipped {
        eprintln!("  - {s}");
    }
}

// The dialect-specific assertions below resolve the DuckDB dialect (the
// credential-free default); gate on the `duckdb` feature so they run against
// the real dialect rather than the Databricks fallback.
#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;

    /// Write a minimal full-refresh model + sidecar to `dir`.
    fn write_model(dir: &Path, name: &str, sql: &str, extra_toml: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!("[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n{extra_toml}"),
        )
        .unwrap();
    }

    #[test]
    fn emits_full_refresh_ctas_for_duckdb() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "m", "SELECT 1 AS id", "");

        // No config → DuckDB dialect.
        let emitted = emit_models(None, dir.path(), None).unwrap().models;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].name, "m");
        assert!(!emitted[0].assumes_existing_target);
        assert_eq!(
            emitted[0].sql,
            "CREATE OR REPLACE TABLE main.m AS\nSELECT 1 AS id;"
        );
    }

    #[test]
    fn applies_declared_surrogate_key_in_emitted_sql() {
        let dir = tempfile::tempdir().unwrap();
        write_model(
            dir.path(),
            "keyed",
            "SELECT order_id FROM upstream",
            "\n[[surrogate_key]]\nname = \"order_key\"\ncolumns = [\"order_id\"]\n",
        );

        let emitted = emit_models(None, dir.path(), None).unwrap().models;
        assert_eq!(emitted.len(), 1);
        // The emitted SQL wraps the SELECT with the dbt-form hash column, exactly
        // as the materialization path does.
        assert!(
            emitted[0].sql.contains("AS order_key"),
            "surrogate-key column must be wrapped into the emitted SQL:\n{}",
            emitted[0].sql
        );
        assert!(emitted[0].sql.contains("__rocky_keyed"));
    }

    #[test]
    fn emits_in_topological_order() {
        let dir = tempfile::tempdir().unwrap();
        // `downstream` reads `upstream`, so it must be emitted after it even
        // though it sorts first alphabetically.
        write_model(dir.path(), "downstream", "SELECT id FROM upstream", "");
        write_model(dir.path(), "upstream", "SELECT 1 AS id", "");

        let emitted = emit_models(None, dir.path(), None).unwrap().models;
        let names: Vec<&str> = emitted.iter().map(|m| m.name.as_str()).collect();
        let up = names.iter().position(|n| *n == "upstream").unwrap();
        let down = names.iter().position(|n| *n == "downstream").unwrap();
        assert!(up < down, "upstream must precede downstream: {names:?}");
    }

    #[test]
    fn model_filter_restricts_output() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "a", "SELECT 1 AS id", "");
        write_model(dir.path(), "b", "SELECT 2 AS id", "");

        let emitted = emit_models(None, dir.path(), Some("b")).unwrap().models;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].name, "b");
    }

    #[test]
    fn merge_model_is_flagged_and_annotated() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("dim.sql"), "SELECT 1 AS id, 'x' AS val\n").unwrap();
        std::fs::write(
            dir.path().join("dim.toml"),
            "[strategy]\ntype = \"merge\"\nunique_key = [\"id\"]\nupdate_columns = [\"val\"]\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n",
        )
        .unwrap();

        let emitted = emit_models(None, dir.path(), None).unwrap().models;
        assert_eq!(emitted.len(), 1);
        // A merge emits a statement operating on an existing target, so it is
        // flagged and the written file carries the bootstrap/watermark caveat.
        assert!(emitted[0].assumes_existing_target);
        assert!(emitted[0].sql.starts_with("MERGE INTO"));
        let body = file_body(&emitted[0]);
        assert!(
            body.contains("-- NOTE: incremental/merge"),
            "merge file must carry the existing-target note:\n{body}"
        );
    }

    /// The tested exit path: write SQL to disk, then run it **directly** against
    /// DuckDB with no Rocky engine involved, and confirm it materializes the
    /// expected table. Proves the emitted files are genuinely runnable.
    #[test]
    fn emitted_sql_runs_directly_against_duckdb() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        write_model(&models, "m", "SELECT 'ok' AS status", "");

        let out = dir.path().join("sql");
        run_emit_sql(None, &models, None, Some(&out)).unwrap();
        let emitted = std::fs::read_to_string(out.join("m.sql")).unwrap();

        // Execute the emitted SQL against a fresh DuckDB — no `rocky run`.
        let db = dir.path().join("t.duckdb");
        let conn = rocky_duckdb::DuckDbConnector::open(&db).expect("open db");
        conn.execute_sql(emitted.trim().trim_end_matches(';'))
            .expect("emitted SQL must run directly");

        let r = conn
            .execute_sql("SELECT status FROM main.m")
            .expect("query materialized table");
        assert_eq!(r.rows.len(), 1);
        assert_eq!(r.rows[0][0].as_str(), Some("ok"));
    }

    #[test]
    fn is_safe_file_stem_rejects_path_traversal() {
        // Plain identifiers and dotted names are fine.
        assert!(is_safe_file_stem("stg_orders"));
        assert!(is_safe_file_stem("orders.v2"));
        // Anything that could escape `out_dir` via the join is rejected.
        for bad in [
            "",
            "..",
            ".",
            "../evil",
            "a/b",
            "a\\b",
            "/abs",
            "/etc/passwd",
        ] {
            assert!(!is_safe_file_stem(bad), "expected {bad:?} to be rejected");
        }
    }
}
