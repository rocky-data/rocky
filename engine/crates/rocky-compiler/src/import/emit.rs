//! Emit a runnable Rocky repo from a dbt project import result.
//!
//! Wraps [`super::dbt::ImportResult`] (the per-model translation output) and
//! [`super::dbt_profiles::ProfileResolution`] (the adapter mapping) into a
//! self-contained directory layout:
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
//! Macros, `dbt_packages/`, and singular tests are skipped by design — Rocky
//! has no Jinja runtime — and surface under "Known limitations" in
//! `MIGRATION-NOTES.md`. Canonical generic tests on models translate to
//! `[[tests]]` blocks. The goal is a `rocky compile`-clean repo, not a
//! line-for-line dbt clone.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use rocky_core::models::{ModelConfig, StrategyConfig};
use rocky_core::tests::{CompositeKind, TestDecl, TestSeverity, TestType};
use rocky_core::unit_test::UnitTestDef;
use serde::Serialize;

use super::dbt::{ImportResult, ImportedModel};
use super::dbt_profiles::ProfileResolution;

/// Outcome of emitting a Rocky repo on disk.
#[derive(Debug, Clone)]
pub struct EmissionResult {
    /// Resolved output directory (absolute or as supplied by the caller).
    pub out_dir: PathBuf,
    /// Number of dbt models successfully translated and written to disk.
    pub models_translated: usize,
    /// Number of dbt model files seen but not translated (failed entries).
    pub models_skipped: usize,
    /// Number of files copied from `<dbt_project>/seeds/` into `<out>/seeds/`.
    pub seeds_copied: usize,
    /// Path to the generated `MIGRATION-NOTES.md`.
    pub migration_notes_path: PathBuf,
    /// Path to the generated `rocky.toml`.
    pub rocky_toml_path: PathBuf,
    /// Models whose `materialized` fell through to the importer's catch-all
    /// (`full_refresh + TODO`). Surfaced in MIGRATION-NOTES.
    pub unknown_materializations: Vec<String>,
}

/// What the caller wants the importer to do when `out_dir` already exists.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverwritePolicy {
    /// Refuse if `out_dir` exists and is non-empty.
    Reject,
    /// Remove existing contents of `out_dir` before writing.
    ReplaceContents,
}

/// Inputs gathered before emission.
pub struct EmitInputs<'a> {
    pub dbt_project_dir: &'a Path,
    pub out_dir: &'a Path,
    pub overwrite: OverwritePolicy,
    pub profile: &'a ProfileResolution,
    pub default_catalog: &'a str,
    pub default_schema: &'a str,
    pub import: &'a ImportResult,
    /// Legacy: extra models whose `view` materialization was flattened to
    /// `full_refresh` by an older version of the importer. The Wave 2
    /// `view → StrategyConfig::View` mapping eliminates this code path
    /// for new imports, but the field is retained as the BTreeSet
    /// surface for callers that still pass it (always empty in
    /// post-Wave-2 callers).
    pub view_models_to_make_ephemeral: BTreeSet<String>,
    /// Adapter override applied via `--target-adapter`, if any. Drives
    /// MIGRATION-NOTES wording.
    pub adapter_override_label: Option<String>,
}

/// Emit a runnable Rocky repo from the importer's result.
pub fn emit_repo(inputs: &EmitInputs<'_>) -> Result<EmissionResult, String> {
    prepare_out_dir(inputs.out_dir, inputs.overwrite)?;

    let models_dir = inputs.out_dir.join("models");
    std::fs::create_dir_all(&models_dir)
        .map_err(|e| format!("failed to create {}: {e}", models_dir.display()))?;

    let mut unknown_materializations: Vec<String> = Vec::new();
    let mut translated = 0usize;

    for model in &inputs.import.imported {
        let mut model = clone_model(model);
        if inputs.view_models_to_make_ephemeral.contains(&model.name) {
            model.config.strategy = StrategyConfig::Ephemeral;
        }
        // Strategy classification — dbt's `materialized` keys we don't map
        // 1:1 (e.g. `materialized_view`, `dynamic_table`, `seed`) all
        // arrive here as `FullRefresh`. We can detect them by walking the
        // import warnings.
        write_model_files(&model, &models_dir)?;
        translated += 1;
    }

    // Collect models whose dbt materialization had no Rocky equivalent
    // and fell back to FullRefresh. Sourced from the typed structured
    // warnings (Wave 2) so we don't false-flag models that hit warnings
    // for unrelated reasons (dropped tags, hooks, on_schema_change).
    for w in &inputs.import.structured_warnings {
        if let super::dbt::ImportDbtStructuredWarning::UnsupportedMaterialization { model, .. } = w
            && !unknown_materializations.contains(model)
        {
            unknown_materializations.push(model.clone());
        }
    }

    write_models_defaults(&models_dir, inputs.default_catalog, inputs.default_schema)?;

    let rocky_toml_path = inputs.out_dir.join("rocky.toml");
    write_rocky_toml(
        &rocky_toml_path,
        inputs.profile,
        inputs.default_catalog,
        inputs.default_schema,
    )?;

    let seeds_copied = copy_seeds(inputs.dbt_project_dir, inputs.out_dir)?;

    let migration_notes_path = inputs.out_dir.join("MIGRATION-NOTES.md");
    write_migration_notes(
        &migration_notes_path,
        &MigrationContext {
            project_name: inputs.import.project_name.as_deref(),
            dbt_version: inputs.import.dbt_version.as_deref(),
            translated,
            models_skipped: inputs.import.failed.len(),
            seeds_copied,
            tests_skipped: inputs.import.tests_found,
            macros_detected: inputs.import.macros_detected,
            unit_tests_found: inputs.import.unit_tests_found,
            unit_tests_converted: inputs.import.unit_tests_converted,
            unit_tests_skipped: inputs.import.unit_tests_skipped,
            warnings: &inputs.import.warnings,
            structured_warnings: &inputs.import.structured_warnings,
            failed: &inputs.import.failed,
            unknown_materializations: &unknown_materializations,
            profile: inputs.profile,
            adapter_override_label: inputs.adapter_override_label.as_deref(),
        },
    )?;

    Ok(EmissionResult {
        out_dir: inputs.out_dir.to_path_buf(),
        models_translated: translated,
        models_skipped: inputs.import.failed.len(),
        seeds_copied,
        migration_notes_path,
        rocky_toml_path,
        unknown_materializations,
    })
}

fn clone_model(m: &ImportedModel) -> ImportedModel {
    ImportedModel {
        name: m.name.clone(),
        sql: m.sql.clone(),
        config: m.config.clone(),
        unit_tests: m.unit_tests.clone(),
    }
}

fn prepare_out_dir(out_dir: &Path, policy: OverwritePolicy) -> Result<(), String> {
    if out_dir.exists() {
        let is_empty = out_dir
            .read_dir()
            .map(|mut it| it.next().is_none())
            .unwrap_or(false);
        if !is_empty {
            match policy {
                OverwritePolicy::Reject => {
                    return Err(format!(
                        "{} already exists and is non-empty (pass --overwrite to replace contents)",
                        out_dir.display()
                    ));
                }
                OverwritePolicy::ReplaceContents => {
                    for entry in std::fs::read_dir(out_dir)
                        .map_err(|e| format!("read_dir({}): {e}", out_dir.display()))?
                    {
                        let entry = entry.map_err(|e| e.to_string())?;
                        let path = entry.path();
                        if path.is_dir() {
                            std::fs::remove_dir_all(&path)
                                .map_err(|e| format!("rm_rf({}): {e}", path.display()))?;
                        } else {
                            std::fs::remove_file(&path)
                                .map_err(|e| format!("rm({}): {e}", path.display()))?;
                        }
                    }
                }
            }
        }
    } else {
        std::fs::create_dir_all(out_dir)
            .map_err(|e| format!("failed to create {}: {e}", out_dir.display()))?;
    }
    Ok(())
}

fn write_model_files(model: &ImportedModel, models_dir: &Path) -> Result<(), String> {
    let sql_path = models_dir.join(format!("{}.sql", model.name));
    let toml_path = models_dir.join(format!("{}.toml", model.name));

    // Annotate untranslated Jinja in the body so reviewers can find it.
    let annotated_sql = annotate_unsupported_jinja(&model.sql);
    std::fs::write(&sql_path, annotated_sql)
        .map_err(|e| format!("failed to write {}: {e}", sql_path.display()))?;

    let mut toml_body = render_model_sidecar(&model.config);
    if !model.unit_tests.is_empty() {
        toml_body.push_str(&render_unit_tests(&model.name, &model.unit_tests));
    }
    std::fs::write(&toml_path, toml_body)
        .map_err(|e| format!("failed to write {}: {e}", toml_path.display()))?;
    Ok(())
}

/// Serialize a model's unit tests as `[[test]]` blocks. Uses the `toml`
/// crate so the array-of-tables nesting (`[[test]]`, `[[test.given]]`,
/// `[test.expect]`) matches what the [`UnitTestDef`] deserializer expects.
///
/// Each test is serialized individually so a single unrepresentable test
/// (e.g. a `null` fixture value, which TOML can't express) is skipped with
/// a warning rather than aborting the whole import. The upstream importer
/// already drops such tests in `apply_dbt_unit_tests`; this is the
/// defense-in-depth backstop so emission can never fail on a stray shape.
pub(crate) fn render_unit_tests(model: &str, tests: &[UnitTestDef]) -> String {
    #[derive(Serialize)]
    struct Wrapper<'a> {
        test: [&'a UnitTestDef; 1],
    }
    let mut out = String::new();
    for test in tests {
        match toml::to_string(&Wrapper { test: [test] }) {
            Ok(body) => {
                out.push('\n');
                out.push_str(&body);
            }
            Err(e) => {
                tracing::warn!(
                    model = %model,
                    unit_test = %test.name,
                    reason = %e,
                    "skipping unit_test that can't be serialized to sidecar TOML",
                );
            }
        }
    }
    out
}

/// Inject a comment line above any TODO/Jinja-leftover marker so reviewers
/// can grep for `# TODO: dbt-jinja-not-translated` in generated bodies.
fn annotate_unsupported_jinja(sql: &str) -> String {
    if !sql.contains("TODO: unsupported Jinja") {
        return sql.to_string();
    }
    let mut out = String::with_capacity(sql.len() + 64);
    out.push_str("-- TODO: dbt-jinja-not-translated — see MIGRATION-NOTES.md\n");
    out.push_str(sql);
    out
}

fn render_model_sidecar(config: &ModelConfig) -> String {
    // Lean serializer — matches the pattern used by `rocky ai` for sidecars
    // (see CHANGELOG #414): we deliberately do NOT serialize empty default
    // collections (`depends_on = []`, etc.) so the file stays compact.
    let mut out = String::new();
    out.push_str(&format!("name = \"{}\"\n", config.name));
    if !config.depends_on.is_empty() {
        let quoted: Vec<String> = config
            .depends_on
            .iter()
            .map(|s| format!("\"{s}\""))
            .collect();
        out.push_str(&format!("depends_on = [{}]\n", quoted.join(", ")));
    }
    if let Some(intent) = &config.intent {
        out.push_str(&format!(
            "intent = \"{}\"\n",
            intent.replace('\\', "\\\\").replace('"', "\\\"")
        ));
    }
    out.push('\n');

    out.push_str("[strategy]\n");
    match &config.strategy {
        StrategyConfig::FullRefresh => {
            out.push_str("type = \"full_refresh\"\n");
        }
        StrategyConfig::Incremental { timestamp_column } => {
            out.push_str("type = \"incremental\"\n");
            out.push_str(&format!("timestamp_column = \"{timestamp_column}\"\n"));
        }
        StrategyConfig::Merge {
            unique_key,
            update_columns,
        } => {
            out.push_str("type = \"merge\"\n");
            let keys: Vec<String> = unique_key.iter().map(|k| format!("\"{k}\"")).collect();
            out.push_str(&format!("unique_key = [{}]\n", keys.join(", ")));
            if let Some(cols) = update_columns {
                let cs: Vec<String> = cols.iter().map(|c| format!("\"{c}\"")).collect();
                out.push_str(&format!("update_columns = [{}]\n", cs.join(", ")));
            }
        }
        StrategyConfig::Ephemeral => {
            out.push_str("type = \"ephemeral\"\n");
        }
        StrategyConfig::View => {
            out.push_str("type = \"view\"\n");
        }
        StrategyConfig::MaterializedView => {
            out.push_str("type = \"materialized_view\"\n");
        }
        StrategyConfig::DynamicTable { target_lag } => {
            out.push_str("type = \"dynamic_table\"\n");
            out.push_str(&format!("target_lag = \"{target_lag}\"\n"));
        }
        StrategyConfig::Microbatch {
            timestamp_column,
            granularity,
        } => {
            out.push_str("type = \"microbatch\"\n");
            out.push_str(&format!("timestamp_column = \"{timestamp_column}\"\n"));
            let g = match granularity {
                rocky_ir::TimeGrain::Hour => "hour",
                rocky_ir::TimeGrain::Day => "day",
                rocky_ir::TimeGrain::Month => "month",
                rocky_ir::TimeGrain::Year => "year",
            };
            out.push_str(&format!("granularity = \"{g}\"\n"));
        }
        StrategyConfig::DeleteInsert { partition_by } => {
            out.push_str("type = \"delete_insert\"\n");
            let keys: Vec<String> = partition_by.iter().map(|k| format!("\"{k}\"")).collect();
            out.push_str(&format!("partition_by = [{}]\n", keys.join(", ")));
        }
        // TimeInterval + ContentAddressed don't arise from the dbt
        // importer today; if we ever hit one, fall back to full_refresh
        // (the user can hand-edit the sidecar).
        _ => {
            out.push_str("type = \"full_refresh\"\n");
        }
    }
    out.push('\n');

    out.push_str("[target]\n");
    out.push_str(&format!("catalog = \"{}\"\n", config.target.catalog));
    out.push_str(&format!("schema = \"{}\"\n", config.target.schema));
    out.push_str(&format!("table = \"{}\"\n", config.target.table));

    if !config.tags.is_empty() {
        out.push('\n');
        out.push_str("[tags]\n");
        for (k, v) in &config.tags {
            let key = if !k.is_empty()
                && k.chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
            {
                k.clone()
            } else {
                format!("{k:?}")
            };
            out.push_str(&format!("{key} = \"{v}\"\n"));
        }
    }

    if !config.sources.is_empty() {
        out.push('\n');
        for src in &config.sources {
            out.push_str("[[sources]]\n");
            out.push_str(&format!("catalog = \"{}\"\n", src.catalog));
            out.push_str(&format!("schema = \"{}\"\n", src.schema));
            out.push_str(&format!("table = \"{}\"\n", src.table));
        }
    }

    if !config.tests.is_empty() {
        for test in &config.tests {
            out.push('\n');
            out.push_str(&render_test_decl(test));
        }
    }
    out
}

/// Escape a string for a double-quoted TOML basic string (backslash + quote).
fn toml_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Serialise a [`TestDecl`] as a `[[tests]]` block matching the canonical
/// Rocky model sidecar shape (`type = "..."`, `column = "..."`, plus
/// type-specific fields). Mirrors the derived serde untagged shape used
/// by `rocky-core::tests` so the emitted TOML round-trips through the
/// model loader.
fn render_test_decl(test: &TestDecl) -> String {
    let mut out = String::new();
    out.push_str("[[tests]]\n");
    match &test.test_type {
        TestType::NotNull => {
            out.push_str("type = \"not_null\"\n");
        }
        TestType::Unique => {
            out.push_str("type = \"unique\"\n");
        }
        TestType::AcceptedValues { values } => {
            out.push_str("type = \"accepted_values\"\n");
            let escaped: Vec<String> = values
                .iter()
                .map(|v| format!("\"{}\"", v.replace('\\', "\\\\").replace('"', "\\\"")))
                .collect();
            out.push_str(&format!("values = [{}]\n", escaped.join(", ")));
        }
        TestType::Relationships {
            to_table,
            to_column,
        } => {
            out.push_str("type = \"relationships\"\n");
            out.push_str(&format!("to_table = \"{to_table}\"\n"));
            out.push_str(&format!("to_column = \"{to_column}\"\n"));
        }
        TestType::Composite { kind, columns } => {
            out.push_str("type = \"composite\"\n");
            let kind_str = match kind {
                CompositeKind::Unique => "unique",
            };
            out.push_str(&format!("kind = \"{kind_str}\"\n"));
            let cols: Vec<String> = columns.iter().map(|c| format!("\"{c}\"")).collect();
            out.push_str(&format!("columns = [{}]\n", cols.join(", ")));
        }
        // dbt_expectations / dbt_utils long-tail mappings.
        TestType::InRange { min, max } => {
            out.push_str("type = \"in_range\"\n");
            if let Some(min) = min {
                out.push_str(&format!("min = \"{}\"\n", toml_escape(min)));
            }
            if let Some(max) = max {
                out.push_str(&format!("max = \"{}\"\n", toml_escape(max)));
            }
        }
        TestType::RegexMatch { pattern } => {
            out.push_str("type = \"regex_match\"\n");
            out.push_str(&format!("pattern = \"{}\"\n", toml_escape(pattern)));
        }
        TestType::Expression { expression } => {
            out.push_str("type = \"expression\"\n");
            out.push_str(&format!("expression = \"{}\"\n", toml_escape(expression)));
        }
        // The dbt importer maps only the canonical built-ins, composite
        // uniqueness, and the dbt_expectations/dbt_utils long-tail
        // (in_range / regex_match / expression) to `TestDecl`. Anything else
        // here would be programmer error — emit toml that won't load is worse
        // than a panic, so refuse.
        other => {
            unreachable!(
                "rocky import-dbt does not produce this TestDecl variant; got {:?}",
                std::mem::discriminant(other)
            );
        }
    }
    if let Some(col) = &test.column {
        out.push_str(&format!("column = \"{col}\"\n"));
    }
    if test.severity != TestSeverity::Error {
        out.push_str("severity = \"warning\"\n");
    }
    if let Some(filter) = &test.filter {
        out.push_str(&format!(
            "filter = \"{}\"\n",
            filter.replace('\\', "\\\\").replace('"', "\\\"")
        ));
    }
    out
}

fn write_models_defaults(
    models_dir: &Path,
    default_catalog: &str,
    default_schema: &str,
) -> Result<(), String> {
    let body =
        format!("[target]\ncatalog = \"{default_catalog}\"\nschema = \"{default_schema}\"\n");
    let path = models_dir.join("_defaults.toml");
    std::fs::write(&path, body).map_err(|e| format!("failed to write {}: {e}", path.display()))
}

fn write_rocky_toml(
    path: &Path,
    profile: &ProfileResolution,
    default_catalog: &str,
    default_schema: &str,
) -> Result<(), String> {
    // Per-model `[target]` lives in each sidecar (also surfaced via
    // `models/_defaults.toml`); the pipeline-level target only carries the
    // adapter ref. The `default_catalog`/`default_schema` arguments are kept
    // in the function signature for symmetry with `models/_defaults.toml` and
    // logged in a leading comment so reviewers can see which values the
    // sidecars inherit.
    let mut out = String::new();
    out.push_str("# rocky.toml — generated by `rocky import-dbt`\n");
    out.push_str("# Connection fields use ${VAR} env-var substitution. Set the env vars\n");
    out.push_str("# listed in MIGRATION-NOTES.md before running `rocky run`.\n");
    out.push_str(&format!(
        "# Default per-model target: catalog={default_catalog}, schema={default_schema} (see models/_defaults.toml).\n\n"
    ));
    out.push_str(&profile.adapter_toml);
    if !profile.adapter_toml.ends_with('\n') {
        out.push('\n');
    }
    out.push('\n');
    out.push_str("[pipeline.default]\n");
    out.push_str("type = \"transformation\"\n");
    out.push_str("models = \"models/**\"\n\n");
    out.push_str("[pipeline.default.target]\n");
    out.push_str("adapter = \"default\"\n");

    std::fs::write(path, out).map_err(|e| format!("failed to write {}: {e}", path.display()))
}

fn copy_seeds(dbt_project_dir: &Path, out_dir: &Path) -> Result<usize, String> {
    let src = dbt_project_dir.join("seeds");
    if !src.exists() {
        return Ok(0);
    }
    let dst = out_dir.join("seeds");
    std::fs::create_dir_all(&dst)
        .map_err(|e| format!("failed to create {}: {e}", dst.display()))?;
    let mut count = 0;
    copy_dir_recursive(&src, &dst, &mut count)?;
    Ok(count)
}

fn copy_dir_recursive(src: &Path, dst: &Path, count: &mut usize) -> Result<(), String> {
    for entry in std::fs::read_dir(src).map_err(|e| format!("read_dir({}): {e}", src.display()))? {
        let entry = entry.map_err(|e| e.to_string())?;
        let from = entry.path();
        let to = dst.join(entry.file_name());
        if from.is_dir() {
            std::fs::create_dir_all(&to)
                .map_err(|e| format!("create_dir({}): {e}", to.display()))?;
            copy_dir_recursive(&from, &to, count)?;
        } else {
            std::fs::copy(&from, &to)
                .map_err(|e| format!("copy({} -> {}): {e}", from.display(), to.display()))?;
            *count += 1;
        }
    }
    Ok(())
}

struct MigrationContext<'a> {
    project_name: Option<&'a str>,
    dbt_version: Option<&'a str>,
    translated: usize,
    models_skipped: usize,
    seeds_copied: usize,
    tests_skipped: usize,
    macros_detected: usize,
    unit_tests_found: usize,
    unit_tests_converted: usize,
    unit_tests_skipped: usize,
    warnings: &'a [super::dbt::ImportWarning],
    structured_warnings: &'a [super::dbt::ImportDbtStructuredWarning],
    failed: &'a [super::dbt::ImportFailure],
    unknown_materializations: &'a [String],
    profile: &'a ProfileResolution,
    adapter_override_label: Option<&'a str>,
}

/// Render the structured-warnings block of `MIGRATION-NOTES.md`. Each
/// variant gets a per-model bullet that includes the dropped payload
/// (tag values, hook SQL, macro names) so the user can paste it
/// directly into the Rocky config.
fn write_structured_warnings(
    out: &mut String,
    warnings: &[super::dbt::ImportDbtStructuredWarning],
) {
    use super::dbt::{HookKind, ImportDbtStructuredWarning as W};
    // Group warnings by model to keep the output scannable.
    let mut by_model: std::collections::BTreeMap<&str, Vec<&W>> = std::collections::BTreeMap::new();
    for w in warnings {
        let model = match w {
            W::UnsupportedMaterialization { model, .. }
            | W::DroppedDatabricksTags { model, .. }
            | W::DroppedHook { model, .. }
            | W::DroppedOnSchemaChange { model, .. }
            | W::UnresolvableMacro { model, .. }
            | W::MicrobatchMissingEventTime { model }
            | W::MicrobatchMapped { model, .. } => model.as_str(),
            // Project-level drops have no owning model — group by their name.
            W::DroppedConstruct { name, .. } => name.as_str(),
        };
        by_model.entry(model).or_default().push(w);
    }
    for (model, items) in by_model {
        out.push_str(&format!("### `{model}`\n\n"));
        for w in items {
            match w {
                W::UnsupportedMaterialization {
                    dbt_materialization,
                    action,
                    ..
                } => {
                    out.push_str(&format!(
                        "- **Unsupported materialization** `{dbt_materialization}` — {action}\n"
                    ));
                }
                W::DroppedDatabricksTags { tags, .. } => {
                    out.push_str(
                        "- **Dropped `databricks_tags`** — copy into the model sidecar's `[classification]` block or wire via `rocky-databricks` governance:\n",
                    );
                    for (k, v) in tags {
                        out.push_str(&format!("  - `{k}` = `{v}`\n"));
                    }
                }
                W::DroppedHook { hook_kind, sql, .. } => {
                    let event = match hook_kind {
                        HookKind::Pre => "on_model_start",
                        HookKind::Post => "on_model_end",
                    };
                    let kind_label = match hook_kind {
                        HookKind::Pre => "pre_hook",
                        HookKind::Post => "post_hook",
                    };
                    out.push_str(&format!(
                        "- **Dropped `{kind_label}`** — translate into `[[hook]] event = \"{event}\"` in `rocky.toml`:\n"
                    ));
                    out.push_str(&format!("  ```sql\n  {sql}\n  ```\n"));
                }
                W::DroppedOnSchemaChange {
                    dbt_value,
                    rocky_equivalent,
                    ..
                } => {
                    out.push_str(&format!(
                        "- **Dropped `on_schema_change = '{dbt_value}'`** — set Rocky `[drift]` policy: {rocky_equivalent}\n"
                    ));
                }
                W::UnresolvableMacro {
                    macro_name,
                    first_call_site_line,
                    ..
                } => {
                    out.push_str(&format!(
                        "- **Unresolvable Jinja macro** `{macro_name}()` first called at line {first_call_site_line} — hand-port the macro logic\n"
                    ));
                }
                W::MicrobatchMissingEventTime { .. } => {
                    out.push_str(
                        "- **Microbatch missing `event_time`** — fell back to `full_refresh`. Add `event_time = '<timestamp_column>'` to the model's dbt config block\n",
                    );
                }
                W::MicrobatchMapped { mapped_to, .. } => {
                    if mapped_to == "merge" {
                        out.push_str(
                            "- **dbt microbatch → idempotent `merge`** — partition-replace became key-upsert; rows removed from the source window are not deleted. Review the `[strategy]` block.\n",
                        );
                    } else {
                        out.push_str(
                            "- **dbt microbatch imported append-only** — re-inserts the lookback window every run. Add a `unique_key` (maps to an idempotent merge) or convert to a time-interval strategy.\n",
                        );
                    }
                }
                W::DroppedConstruct {
                    construct,
                    name,
                    detail,
                } => {
                    out.push_str(&format!("- **Dropped {construct}** `{name}` — {detail}\n"));
                }
            }
        }
        out.push('\n');
    }
}

fn write_migration_notes(path: &Path, ctx: &MigrationContext<'_>) -> Result<(), String> {
    let mut out = String::new();
    out.push_str("# Migration notes\n\n");
    out.push_str("Generated by `rocky import-dbt`. This file summarises the translation\n");
    out.push_str("of your dbt project into a runnable Rocky repo. Items under \"Known\n");
    out.push_str("limitations\" are out of scope by design — Rocky has no Jinja runtime —\n");
    out.push_str("and need a manual pass before `rocky run` will reproduce the dbt\n");
    out.push_str("behaviour.\n\n");

    out.push_str("## Overview\n\n");
    if let Some(name) = ctx.project_name {
        out.push_str(&format!("- dbt project: `{name}`\n"));
    }
    if let Some(v) = ctx.dbt_version {
        out.push_str(&format!("- dbt version: `{v}`\n"));
    }
    out.push_str(&format!(
        "- Adapter mapping: dbt `{}` → Rocky `{}`\n",
        ctx.profile.original_type,
        ctx.profile.kind.rocky_type()
    ));
    if let Some(label) = ctx.adapter_override_label {
        out.push_str(&format!("- Adapter override: `{label}`\n"));
    }
    out.push('\n');

    out.push_str("## Counts\n\n");
    out.push_str(&format!("- Models translated: {}\n", ctx.translated));
    out.push_str(&format!("- Models skipped: {}\n", ctx.models_skipped));
    out.push_str(&format!("- Seeds copied: {}\n", ctx.seeds_copied));
    out.push_str(&format!(
        "- dbt tests detected (canonical four mapped to `[[tests]]`; non-canonical surfaced as warnings): {}\n",
        ctx.tests_skipped
    ));
    out.push_str(&format!(
        "- dbt unit tests detected: {} (converted to Rocky `[[test]]` sidecars: {}, skipped: {})\n",
        ctx.unit_tests_found, ctx.unit_tests_converted, ctx.unit_tests_skipped,
    ));
    out.push_str(&format!(
        "- dbt macros detected (not translated — see Known limitations): {}\n",
        ctx.macros_detected
    ));
    out.push('\n');

    out.push_str("## Required env vars\n\n");
    if ctx.profile.env_vars.vars.is_empty() {
        out.push_str("- None — DuckDB adapter uses a local file path.\n");
    } else {
        for v in &ctx.profile.env_vars.vars {
            out.push_str(&format!("- `${{{v}}}`\n"));
        }
    }
    out.push('\n');

    out.push_str("## Known limitations\n\n");
    out.push_str(
        "- **dbt generic tests outside the canonical four** (`unique`, `not_null`, `accepted_values`, ",
    );
    out.push_str(
        "`relationships` are translated to `[[tests]]` on each model sidecar). Anything else ",
    );
    out.push_str(
        "(`dbt_utils.*`, `dbt_expectations.*`, project-defined generic tests, model-level tests) ",
    );
    out.push_str("is surfaced under the **Warnings** section below — not stubbed in the SQL.\n");
    out.push_str("- **Singular tests** in `tests/` (custom SQL) — copy and rewrite manually.\n");
    out.push_str("- **dbt macros / `dbt_packages/`** — Rocky has no Jinja runtime. Hand-port any ");
    out.push_str("logic to plain SQL or to a Rocky AI prompt.\n");
    out.push_str("- **`{% if %}` / `{% for %}` / `{{ var() }}`** outside of `is_incremental()` — ");
    out.push_str("the body is emitted verbatim with `# TODO: dbt-jinja-not-translated` comments ");
    out.push_str("flagging the lines you need to revisit.\n");
    if !ctx.unknown_materializations.is_empty() {
        out.push_str("- **Unmapped `materialized` values** (treated as `full_refresh`):\n");
        for name in ctx.unknown_materializations {
            out.push_str(&format!("  - `{name}`\n"));
        }
    }
    if matches!(
        ctx.profile.kind,
        super::dbt_profiles::AdapterKind::Unmapped(_)
    ) {
        out.push_str(&format!(
            "- **Adapter `{}` is not natively supported by Rocky** — the generated repo stubs ",
            ctx.profile.original_type
        ));
        out.push_str(
            "DuckDB so the project still loads. Replace the `[adapter]` block in `rocky.toml` once a Rocky adapter for your warehouse exists.\n",
        );
    }
    out.push('\n');

    if !ctx.structured_warnings.is_empty() {
        out.push_str("## Items to translate manually\n\n");
        out.push_str(
            "The dbt config below couldn't be auto-translated. Each entry points at the matching Rocky surface.\n\n",
        );
        write_structured_warnings(&mut out, ctx.structured_warnings);
    }

    if !ctx.warnings.is_empty() {
        out.push_str("## Warnings\n\n");
        for w in ctx.warnings {
            out.push_str(&format!(
                "- `{}` — {:?}: {}\n",
                w.model, w.category, w.message
            ));
        }
        out.push('\n');
    }

    if !ctx.failed.is_empty() {
        out.push_str("## Failed models\n\n");
        for f in ctx.failed {
            out.push_str(&format!("- `{}` — {}\n", f.name, f.reason));
        }
        out.push('\n');
    }

    out.push_str("## Next steps\n\n");
    out.push_str("1. Review `rocky.toml` and set the env vars listed above.\n");
    out.push_str(
        "2. Run `rocky compile` from the output directory to type-check the translated models.\n",
    );
    out.push_str("3. Run `rocky test` to exercise any seed data.\n");
    out.push_str("4. See the [migration guide](https://rocky-data.github.io/rocky/guides/migrate-from-dbt/) for the long tail.\n");

    std::fs::write(path, out).map_err(|e| format!("failed to write {}: {e}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::dbt::ImportMethod;
    use crate::import::dbt_profiles::{AdapterKind, resolution_for_kind};
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    fn make_model(name: &str, strategy: StrategyConfig, sql: &str) -> ImportedModel {
        ImportedModel {
            name: name.to_string(),
            sql: sql.to_string(),
            unit_tests: vec![],
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy,
                target: TargetConfig {
                    catalog: "warehouse".to_string(),
                    schema: "main".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
        }
    }

    fn empty_result(imported: Vec<ImportedModel>) -> ImportResult {
        ImportResult {
            imported,
            warnings: vec![],
            structured_warnings: vec![],
            failed: vec![],
            sources_found: 0,
            sources_mapped: 0,
            import_method: ImportMethod::Regex,
            project_name: Some("test_proj".to_string()),
            dbt_version: None,
            tests_found: 0,
            tests_converted: 0,
            tests_converted_custom: 0,
            tests_skipped: 0,
            macros_detected: 0,
            macros_expanded: 0,
            macros_manifest_resolved: 0,
            macros_unsupported: 0,
            unit_tests_found: 0,
            unit_tests_converted: 0,
            unit_tests_skipped: 0,
            constructs_dropped: 0,
        }
    }

    #[test]
    fn emits_layout_with_models_and_rocky_toml() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();

        let imported = vec![make_model(
            "stg_orders",
            StrategyConfig::FullRefresh,
            "SELECT 1 AS id",
        )];
        let result = empty_result(imported);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        let emission = emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        assert_eq!(emission.models_translated, 1);
        assert!(out_dir.path().join("rocky.toml").exists());
        assert!(out_dir.path().join("models/_defaults.toml").exists());
        assert!(out_dir.path().join("models/stg_orders.sql").exists());
        assert!(out_dir.path().join("models/stg_orders.toml").exists());
        assert!(out_dir.path().join("MIGRATION-NOTES.md").exists());

        let toml_body =
            std::fs::read_to_string(out_dir.path().join("models/stg_orders.toml")).unwrap();
        assert!(toml_body.contains("[strategy]"));
        assert!(toml_body.contains("type = \"full_refresh\""));
        assert!(toml_body.contains("[target]"));
    }

    #[test]
    fn view_models_get_rewritten_to_ephemeral() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();
        // Importer flattens `view` to FullRefresh and surfaces a warning;
        // emit re-applies the `view → ephemeral` mapping at write time.
        let imported = vec![make_model(
            "v_users",
            StrategyConfig::FullRefresh,
            "SELECT 1",
        )];
        let result = empty_result(imported);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        let mut view_set = BTreeSet::new();
        view_set.insert("v_users".to_string());

        emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: view_set,
            adapter_override_label: None,
        })
        .unwrap();

        let body = std::fs::read_to_string(out_dir.path().join("models/v_users.toml")).unwrap();
        assert!(body.contains("type = \"ephemeral\""));
    }

    #[test]
    fn copies_seeds_directory() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dbt_dir.path().join("seeds")).unwrap();
        std::fs::write(dbt_dir.path().join("seeds/users.csv"), "id,name\n1,a\n").unwrap();

        let out_dir = tempfile::TempDir::new().unwrap();
        let result = empty_result(vec![]);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        let emission = emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        assert_eq!(emission.seeds_copied, 1);
        assert!(out_dir.path().join("seeds/users.csv").exists());
    }

    #[test]
    fn rejects_non_empty_dir_without_overwrite() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(out_dir.path().join("existing.txt"), "noise").unwrap();

        let result = empty_result(vec![]);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        let err = emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::Reject,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap_err();
        assert!(err.contains("--overwrite"));
    }

    #[test]
    fn replaces_contents_with_overwrite() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();
        std::fs::write(out_dir.path().join("stale.txt"), "old").unwrap();

        let result = empty_result(vec![make_model(
            "x",
            StrategyConfig::FullRefresh,
            "SELECT 1",
        )]);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        assert!(!out_dir.path().join("stale.txt").exists());
        assert!(out_dir.path().join("rocky.toml").exists());
    }

    #[test]
    fn migration_notes_lists_required_env_vars_for_databricks() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();
        let result = empty_result(vec![]);
        let profile = resolution_for_kind(AdapterKind::Databricks, "databricks");

        emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "analytics",
            default_schema: "marts",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        let notes = std::fs::read_to_string(out_dir.path().join("MIGRATION-NOTES.md")).unwrap();
        assert!(notes.contains("DATABRICKS_TOKEN"));
        assert!(notes.contains("Required env vars"));
        assert!(notes.contains("Known limitations"));
        assert!(
            !notes.to_lowercase().contains("v0"),
            "GA framing: MIGRATION-NOTES must not reference 'v0'"
        );
    }

    #[test]
    fn emits_unit_test_blocks_that_round_trip() {
        use rocky_core::unit_test::{TestExpectation, TestFixture, UnitTestDef};

        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();

        let mut model = make_model("stg_orders", StrategyConfig::FullRefresh, "SELECT 1 AS id");
        model.unit_tests = vec![UnitTestDef {
            name: "stamps_order_key".into(),
            description: Some("order key stamped via md5".into()),
            given: vec![TestFixture {
                model_ref: "int_orders".into(),
                rows: vec![serde_json::json!({ "order_id": 1001, "customer_id": 50 })],
            }],
            expect: TestExpectation {
                rows: vec![serde_json::json!({ "order_key": "abc", "order_id": 1001 })],
                ordered: false,
            },
        }];

        let result = empty_result(vec![model]);
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        let body = std::fs::read_to_string(out_dir.path().join("models/stg_orders.toml"))
            .expect("sidecar written");
        assert!(
            body.contains("[[test]]"),
            "sidecar must declare a [[test]] block:\n{body}"
        );
        assert!(body.contains("stamps_order_key"));

        // Round-trip via the public UnitTestDef deserializer. The sidecar
        // contains other top-level keys (`name`, `[strategy]`, etc.) — we
        // pull `test` out by hand so the assertion focuses on the new
        // surface.
        #[derive(serde::Deserialize)]
        struct UnitTestsOnly {
            #[serde(default)]
            test: Vec<UnitTestDef>,
        }
        let parsed: UnitTestsOnly = toml::from_str(&body).expect("sidecar parses");
        assert_eq!(parsed.test.len(), 1);
        let ut = &parsed.test[0];
        assert_eq!(ut.name, "stamps_order_key");
        assert_eq!(ut.given.len(), 1);
        assert_eq!(ut.given[0].model_ref, "int_orders");
        assert_eq!(ut.expect.rows.len(), 1);
    }

    #[test]
    fn renders_composite_uniqueness_test() {
        let decl = TestDecl {
            test_type: TestType::Composite {
                kind: CompositeKind::Unique,
                columns: vec!["order_id".to_string(), "line_number".to_string()],
            },
            column: None,
            severity: TestSeverity::Error,
            filter: None,
        };
        let toml = render_test_decl(&decl);
        assert!(toml.contains("type = \"composite\""), "{toml}");
        assert!(toml.contains("kind = \"unique\""), "{toml}");
        assert!(
            toml.contains("columns = [\"order_id\", \"line_number\"]"),
            "{toml}"
        );
        // Model-level: no `column =` line.
        assert!(!toml.contains("column ="), "{toml}");
    }

    #[test]
    fn migration_notes_counts_unit_tests() {
        let dbt_dir = tempfile::TempDir::new().unwrap();
        let out_dir = tempfile::TempDir::new().unwrap();
        let mut result = empty_result(vec![]);
        result.unit_tests_found = 5;
        result.unit_tests_converted = 4;
        result.unit_tests_skipped = 1;
        let profile = resolution_for_kind(AdapterKind::DuckDb, "duckdb");

        emit_repo(&EmitInputs {
            dbt_project_dir: dbt_dir.path(),
            out_dir: out_dir.path(),
            overwrite: OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "warehouse",
            default_schema: "main",
            import: &result,
            view_models_to_make_ephemeral: BTreeSet::new(),
            adapter_override_label: None,
        })
        .unwrap();

        let notes = std::fs::read_to_string(out_dir.path().join("MIGRATION-NOTES.md")).unwrap();
        assert!(notes.contains("dbt unit tests detected: 5"));
        assert!(notes.contains("converted to Rocky `[[test]]` sidecars: 4"));
        assert!(notes.contains("skipped: 1"));
    }
}
