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
//! `rocky.toml` without credentials); with no project file at all it defaults
//! to DuckDB. A `rocky.toml` that exists but does not load is an error, not a
//! fallback. All models render in this one resolved dialect, so for a project
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

use super::plan::preview_dialect;

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
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<EmitResult> {
    use rocky_compiler::compile::{self, CompilerConfig};

    // The SAME resolution `plan_preview_output` uses — literally the same
    // function, so "emitted SQL matches the plan preview" is true by
    // construction. A `rocky.toml` that exists but does not load refuses here
    // too, rather than emitting DuckDB SQL for a broken Snowflake project. An
    // unset `${CREDENTIAL}` is not that case: this command connects to
    // nothing and is documented as needing none, so the placeholder is
    // tolerated (`preview_dialect`).
    let dialect = preview_dialect(config_path)?;

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
        project_freshness_default: false,
        run_vars: run_vars.clone(),
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

    // A successful compile can still carry error diagnostics — e.g. E028 for a
    // required `@var(...)` with no supplied value, which substitutes the
    // MISSING_SENTINEL ("NULL") into the SQL. `rocky compile`/`run` refuse to
    // proceed in that case; emit-sql must too, rather than emitting (and, with
    // `--out-dir`, persisting) provably-wrong SQL. Mirrors the compile command.
    if result.has_errors {
        let errors: Vec<String> = result
            .diagnostics
            .iter()
            .filter(|d| d.is_error())
            .map(|d| format!("{}: {} ({})", d.code, d.message, d.model))
            .collect();
        anyhow::bail!(
            "emit-sql: compilation failed with errors:\n  {}",
            errors.join("\n  ")
        );
    }

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
    //
    // Loaded ONLY for the models this invocation will emit. The whole-tree load
    // meant a malformed `[[surrogate_key]]` on ANY model failed an emit narrowed
    // to a different one (#1537) — `validate_surrogate_key_spec` returns a
    // `ModelError` for the tree, not for the model asked about. An unnarrowed
    // emit still covers every model, so a bad spec is still reported; it is now
    // reported by the invocations that would actually apply it.
    //
    // Selection is by the compiled models' own `file_path`, the same derivation
    // `run` uses (`commands/run.rs`, the `selected_model_paths` set). Filtering
    // on the filename stem instead would silently drop the key of a model whose
    // sidecar renames it with `name = "..."` — a wrong-SQL failure, strictly
    // worse than the noisy one being fixed.
    let selected_model_paths: std::collections::HashSet<std::path::PathBuf> = result
        .project
        .models
        .iter()
        .filter(|m| model_filter.is_none_or(|f| m.config.name == f))
        .map(|m| std::path::PathBuf::from(&m.file_path))
        .collect();
    let surrogate_keys: HashMap<String, Vec<SurrogateKeySpec>> =
        rocky_core::models::load_surrogate_keys_from_tree_filtered(models_dir, |path| {
            selected_model_paths.contains(path)
        })
        .context("invalid surrogate_key configuration")?;

    let mut emitted = Vec::new();
    let mut skipped = Vec::new();
    let mut filter_matched = false;
    for model_ir in ordered {
        let model_name = model_ir.name.as_ref();
        if let Some(f) = model_filter {
            if model_name != f {
                continue;
            }
            filter_matched = true;
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
    // An explicit `--model <name>` that matched nothing is a user error (a
    // typo, or a model that doesn't exist), not an empty success — otherwise
    // `emit-sql --model X --out-dir out/` writes nothing and exits 0 while a
    // deploy script believes it emitted. Mirrors `rocky cost --model`.
    if let Some(f) = model_filter
        && !filter_matched
    {
        anyhow::bail!("emit-sql: model '{f}' not found (no transformation model with that name)");
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
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    let EmitResult {
        models,
        mut skipped,
    } = emit_models(config_path, models_dir, model_filter, run_vars)?;

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
        let emitted = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].name, "m");
        assert!(!emitted[0].assumes_existing_target);
        assert_eq!(
            emitted[0].sql,
            "CREATE OR REPLACE TABLE main.m AS\nSELECT 1 AS id;"
        );
    }

    /// SIXTEENTH ROUND, finding 1 — `emit-sql` shares
    /// [`super::plan::preview_dialect`] with `plan_preview`, so it inherited
    /// the refusal. Pinned HERE as well as on the plan side, because sharing a
    /// helper is only half the guarantee: a future caller could re-inline a
    /// tolerant copy and the plan-side test would stay green.
    ///
    /// Both arms in one test, because the tolerated case is the load-bearing
    /// half — `--config` defaults to `rocky.toml`, so the standalone
    /// `--models <dir>` invocation reaches this with a path that does not
    /// exist, and it must still emit.
    #[test]
    fn emit_refuses_a_malformed_config_but_tolerates_an_absent_one() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "m", "SELECT 1 AS id", "");
        let vars = rocky_core::run_vars::RunVars::new();

        // ABSENT — the standalone case. Emits in the default dialect.
        let absent = dir.path().join("rocky.toml");
        assert!(!absent.exists());
        let emitted = emit_models(Some(&absent), dir.path(), None, &vars)
            .expect("an ABSENT config is the standalone case, not a malformed one")
            .models;
        assert_eq!(emitted.len(), 1, "the absent-config arm must still emit");

        // MALFORMED — unterminated table header, so TOML parsing fails.
        std::fs::write(&absent, "[adapter.default\ntype = \"snowflake\"\n").unwrap();
        // `EmitResult` is not `Debug`, so match rather than `expect_err`.
        let Err(err) = emit_models(Some(&absent), dir.path(), None, &vars) else {
            panic!("a malformed rocky.toml must refuse, not emit DuckDB SQL");
        };
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config"),
            "the refusal must name the config as the cause: {rendered}"
        );
    }

    /// SEVENTEENTH ROUND, finding 1 — `emit-sql`'s own pin.
    ///
    /// The published contract for this command is "without a warehouse
    /// connection" and "resolved from `rocky.toml` without credentials"
    /// (`docs/.../reference/commands/modeling.md`). Round sixteen's shared
    /// `preview_dialect` briefly expanded env vars, so an initialized
    /// Databricks project with `${DATABRICKS_HOST}` unset exited 1 here.
    ///
    /// Pinned on this side as well as the plan side for the reason the
    /// neighbouring test gives: sharing a helper is only half the guarantee.
    #[test]
    fn emit_tolerates_an_unset_credential_placeholder() {
        assert!(
            std::env::var("ROCKY_DEFINITELY_NOT_SET_EMIT_HOST").is_err(),
            "premise: the variable is unset"
        );
        let dir = tempfile::tempdir().unwrap();
        // Written by hand rather than via `write_model`: that helper pins
        // `catalog = ""`, and the Databricks dialect refuses a two-part
        // reference, so the model would be SKIPPED and the test would pass
        // on an empty result set for the wrong reason.
        std::fs::write(dir.path().join("m.sql"), "SELECT 1 AS id\n").unwrap();
        std::fs::write(
            dir.path().join("m.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"main\"\ntable = \"m\"\n",
        )
        .unwrap();
        let cfg = dir.path().join("rocky.toml");
        std::fs::write(
            &cfg,
            "[adapter.default]\ntype = \"databricks\"\n\
             host = \"${ROCKY_DEFINITELY_NOT_SET_EMIT_HOST}\"\n\
             http_path = \"/sql/1.0/warehouses/abc\"\ntoken = \"pat\"\n",
        )
        .unwrap();

        let emitted = emit_models(
            Some(&cfg),
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .expect("emit-sql opens no connection, so an unset credential must not stop it")
        .models;
        assert_eq!(emitted.len(), 1, "the model must still render");
        assert!(
            emitted[0].sql.contains("CREATE OR REPLACE TABLE"),
            "unexpected SQL: {}",
            emitted[0].sql
        );
        // This arm does NOT prove the dialect: a trivial CTAS renders
        // identically in Databricks and DuckDB, so asserting on this SQL
        // could not tell a correct resolution from a silent fallback. The
        // two-sided dialect claim is pinned where it is observable, on the
        // shared resolver — `plan::tests::
        // plan_preview_tolerates_an_unset_credential_placeholder` asserts
        // `preview_dialect(...).name() == "snowflake"`.
    }

    /// SEVENTEENTH ROUND, finding 2 — the parity pin, placed HERE because
    /// this is the one test module that can reach both renderers.
    ///
    /// `plan_preview_output` used to skip `apply_surrogate_keys` while this
    /// command and `rocky run` both applied it. A reviewer reading the
    /// preview — over MCP, an agent about to approve a model — saw SQL
    /// missing a column the run would materialize.
    ///
    /// Asserted as EQUALITY, not as "the preview also mentions the column".
    /// A containment check would go green again the moment the two renderers
    /// diverged in some other way, which is the failure this pin is for. The
    /// only normalization is the statement terminator: `emit-sql` joins a
    /// model's statements into a runnable script and terminates each with
    /// `;`, while the preview returns them unterminated.
    #[test]
    fn plan_preview_and_emit_sql_render_the_same_keyed_sql() {
        let dir = tempfile::tempdir().unwrap();
        write_model(
            dir.path(),
            "keyed",
            "SELECT order_id FROM upstream",
            "\n[[surrogate_key]]\nname = \"order_key\"\ncolumns = [\"order_id\"]\n",
        );

        let emitted = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
        assert_eq!(emitted.len(), 1);

        let preview = crate::commands::plan_preview_output(None, dir.path(), None, None).unwrap();
        assert_eq!(
            preview.statements.len(),
            1,
            "the preview must render the same one model"
        );

        assert_eq!(
            format!("{};", preview.statements[0].sql.trim_end_matches(';')),
            emitted[0].sql,
            "the offline preview and `emit-sql` must render the same SQL"
        );
        // Named explicitly so a future reader sees WHICH column the equality
        // is protecting; equality alone would pass if both dropped it.
        assert!(
            preview.statements[0].sql.contains("AS order_key"),
            "the preview must carry the declared surrogate key:\n{}",
            preview.statements[0].sql
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

        let emitted = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
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

    /// A malformed `[[surrogate_key]]` on one model must not fail an emit
    /// narrowed to a different one (#1537). `run` already behaves this way; the
    /// preview inherited an unfiltered load rather than choosing one.
    #[test]
    fn narrowed_emit_ignores_a_malformed_key_on_another_model() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "orders", "SELECT 1 AS id", "");
        // `columns = []` fails `validate_surrogate_key_spec`: a key must list at
        // least one input column.
        write_model(
            dir.path(),
            "customers",
            "SELECT 2 AS id",
            "\n[[surrogate_key]]\nname = \"customer_key\"\ncolumns = []\n",
        );

        let emitted = emit_models(
            None,
            dir.path(),
            Some("orders"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .expect("a malformed key on `customers` must not fail an emit of `orders`")
        .models;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].name, "orders");
    }

    /// The other half of the same rule: narrowing must not become a way to
    /// smuggle a bad spec past validation. An emit that covers the offending
    /// model still refuses.
    #[test]
    fn unnarrowed_emit_still_reports_a_malformed_key() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "orders", "SELECT 1 AS id", "");
        write_model(
            dir.path(),
            "customers",
            "SELECT 2 AS id",
            "\n[[surrogate_key]]\nname = \"customer_key\"\ncolumns = []\n",
        );

        let err = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        // `EmitResult` is not `Debug` (production type, no test-only derive);
        // discard the Ok value so `expect_err` can report.
        .map(|_| ())
        .expect_err("an unnarrowed emit covers `customers`, so it must still refuse");
        assert!(
            format!("{err:#}").contains("surrogate_key"),
            "the refusal must name the surrogate_key configuration:\n{err:#}"
        );

        // And narrowing TO the offending model refuses too — the spec is only
        // skipped for invocations that would not apply it.
        emit_models(
            None,
            dir.path(),
            Some("customers"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .map(|_| ())
        .expect_err("narrowing to `customers` must still refuse its own bad key");
    }

    /// Guards the over-filtering direction: the narrowed model must still get
    /// its OWN key. Selection is by the compiled model's `file_path`, so a
    /// sidecar that renames the model still resolves.
    #[test]
    fn narrowed_emit_still_applies_its_own_key() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "other", "SELECT 1 AS id", "");
        write_model(
            dir.path(),
            "keyed",
            "SELECT order_id FROM upstream",
            "\n[[surrogate_key]]\nname = \"order_key\"\ncolumns = [\"order_id\"]\n",
        );

        let emitted = emit_models(
            None,
            dir.path(),
            Some("keyed"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
        assert_eq!(emitted.len(), 1);
        assert!(
            emitted[0].sql.contains("AS order_key"),
            "narrowing must not drop the selected model's own key:\n{}",
            emitted[0].sql
        );
    }

    #[test]
    fn emits_in_topological_order() {
        let dir = tempfile::tempdir().unwrap();
        // `downstream` reads `upstream`, so it must be emitted after it even
        // though it sorts first alphabetically.
        write_model(dir.path(), "downstream", "SELECT id FROM upstream", "");
        write_model(dir.path(), "upstream", "SELECT 1 AS id", "");

        let emitted = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
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

        let emitted = emit_models(
            None,
            dir.path(),
            Some("b"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].name, "b");
    }

    #[test]
    fn unknown_model_filter_errors() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "a", "SELECT 1 AS id", "");
        // Regression: a `--model` that matches nothing must error, not silently
        // succeed with an empty result (which a deploy script reads as success).
        let err = emit_models(
            None,
            dir.path(),
            Some("nope"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .err()
        .expect("unknown --model must error");
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[test]
    fn missing_required_var_errors_instead_of_emitting_sentinel() {
        let dir = tempfile::tempdir().unwrap();
        write_model(dir.path(), "m", "SELECT '@var(region)' AS r", "");
        // Regression: emit-sql must honor compile errors (E028) rather than
        // emit the MISSING_SENTINEL ("NULL") for a required @var with no value.
        let err = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .err()
        .expect("missing required @var must error");
        assert!(err.to_string().contains("compilation failed"), "{err}");
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

        let emitted = emit_models(
            None,
            dir.path(),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap()
        .models;
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
        run_emit_sql(
            None,
            &models,
            None,
            Some(&out),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
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
