//! dbt project ingestion.
//!
//! Imports dbt SQL models by extracting `{{ ref() }}`, `{{ source() }}`, and
//! `{{ config() }}` Jinja expressions and converting them to Rocky model files.
//!
//! **Supported:**
//! - `{{ ref('model_name') }}` -> bare table ref
//! - `{{ source('source_name', 'table_name') }}` -> fully qualified ref
//! - `{{ config(materialized='incremental', unique_key='id') }}` -> ModelConfig
//! - `{{ this }}` -> target table ref
//! - Manifest incremental models require a matching full-refresh compile
//!   artifact pair and must not set `full_refresh=false`
//!
//! **Import paths:**
//! - **Manifest (preferred):** uses `compiled_code` from `target/manifest.json`
//! - **Regex (fallback):** regex-based Jinja extraction from raw `.sql` files
//!
//! **Not supported (produces diagnostics):**
//! - Raw Jinja control flow, custom Jinja macros, Python models

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use regex::Regex;

use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};
use rocky_core::unit_test::{TestExpectation, TestFixture, UnitTestDef};

use super::dbt_manifest::{
    self, DbtManifest, DbtManifestNode, DbtNodeConfig, DbtUnitTestExpect, DbtUnitTestGiven,
    UniqueKeyValue,
};
use super::dbt_project::{self, DbtProjectConfig};
use super::dbt_sources;

const RAW_INCREMENTAL_ERROR: &str = "contains an unresolved reference to dbt's `is_incremental()` macro; \
the raw SQL importer cannot preserve dbt's false-on-bootstrap, true-on-existing-target semantics \
without either referencing a missing target during bootstrap or deleting bounded incremental \
logic. Run `dbt compile --full-refresh` and import its compiled SQL from manifest.json with \
the matching run_results.json, or rewrite the model with a Rocky-supported strategy";

const RAW_INCREMENTAL_EVIDENCE_REFUSED: &str = "is an effectively incremental dbt model. \
The raw importer has no compiled SQL or per-model run_results.json evidence and cannot prove \
the first run contains all rows. Run `dbt compile --full-refresh`, then import manifest.json \
with its matching run_results.json";

const RAW_JINJA_CONTROL_REFUSED: &str = "raw import cannot evaluate Jinja control flow; \
run `dbt compile --full-refresh` and import with the manifest";
const RAW_CONFIG_UNRESOLVED: &str = "raw import cannot resolve a dbt config expression; \
run `dbt compile --full-refresh` and import with the manifest";
const RAW_VERSIONED_REFUSED: &str = "raw import cannot resolve versioned model properties; \
run `dbt compile --full-refresh` and import with the manifest";

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// How a dbt microbatch model is translated on import.
///
/// dbt microbatch idempotently replaces each event-time partition. Rocky
/// can express that faithfully as a [`StrategyConfig::TimeInterval`] model
/// (bounded `@start_date`/`@end_date` window + lookback), but doing so
/// requires rewriting the model body to reference those placeholders — not
/// always safe. The default keeps the historical `merge` mapping for
/// back-compat; `--microbatch-as=time_interval` opts into the
/// bounded-window translation, falling back to `merge` (loudly) whenever the
/// body can't be rewritten safely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MicrobatchMode {
    /// Map to an idempotent `merge` (or append-only when no `unique_key`).
    /// The historical default — preserves existing import behavior.
    #[default]
    Merge,
    /// Map to a `time_interval` model with a bounded per-partition window,
    /// deriving granularity/lookback from the dbt config. Falls back to
    /// `Merge` with a warning when the body can't be rewritten safely.
    TimeInterval,
}

impl MicrobatchMode {
    /// Parse the `--microbatch-as` CLI value. Unknown values map to the
    /// default ([`MicrobatchMode::Merge`]); the caller validates the flag.
    pub fn from_flag(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "time_interval" => Self::TimeInterval,
            _ => Self::Merge,
        }
    }
}

/// How the import was performed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportMethod {
    /// Used manifest.json (compiled SQL, all Jinja resolved).
    Manifest,
    /// Used regex-based Jinja extraction from raw .sql files.
    Regex,
}

/// Category of import warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WarningCategory {
    /// A dbt materialization Rocky has no equivalent for.
    UnsupportedMaterialization,
    /// Jinja control flow that cannot be translated faithfully.
    JinjaControlFlow,
    /// Custom Jinja macro that couldn't be resolved.
    UnsupportedMacro,
    /// Manifest is older than source SQL files.
    StaleManifest,
    /// `{{ source() }}` reference with no matching source definition.
    MissingSource,
    /// dbt generic test outside the four canonical built-ins
    /// (`unique` / `not_null` / `accepted_values` / `relationships`).
    UnsupportedTest,
    /// A `manifest.unit_tests` entry references a model that wasn't
    /// imported (typo, filtered out, or upstream failure). The unit
    /// test is dropped.
    OrphanUnitTest,
    /// A `manifest.unit_tests` entry uses a non-`dict` `format` for
    /// `given.rows` or `expect.rows` (typically `csv` or `sql`). Inline
    /// `format = "dict"` is the only shape supported today.
    UnsupportedUnitTestFormat,
    /// A `manifest.unit_tests` entry has fixture/expectation rows that
    /// can't be represented in the Rocky sidecar TOML — typically a `null`
    /// field value or a non-object row element (TOML has no null type).
    /// The test is dropped so it never aborts the rest of the import.
    UnserializableUnitTest,
    /// A `profiles.yml` was present but could not be parsed/resolved (bad
    /// YAML, unresolvable merge keys, or an `env_var()` `type` with no
    /// default), so the importer emitted a stub DuckDB adapter. Surfaced
    /// loudly so the migration never silently defaults to duckdb.
    ProfileFallback,
    /// An enforced dbt model `contract` was written to a
    /// `{model}.contract.toml`, but a column type or constraint has no Rocky
    /// check.
    DroppedContract,
    /// A dbt construct was mapped to a native Rocky equivalent rather than
    /// dropped — informational, not a degradation. Today this covers
    /// `{{ var('x') }}` → `@var(x)` (Rocky's per-run variable marker).
    MappedConstruct,
}

/// A warning produced during import.
#[derive(Debug, Clone)]
pub struct ImportWarning {
    pub model: String,
    pub category: WarningCategory,
    pub message: String,
    pub suggestion: Option<String>,
}

/// Lifecycle hook kind for [`ImportDbtStructuredWarning::DroppedHook`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookKind {
    Pre,
    Post,
}

/// A structured warning carrying typed payload data for dbt-side config
/// that Rocky can't translate automatically. Surfaces in
/// `ImportDbtOutput.structured_warnings` so callers (Dagster, vscode) can
/// route specific kinds (e.g. dropped tags, dropped hooks) into UI
/// affordances without parsing free-form `message` text.
///
/// Coexists with the flat `ImportWarning` surface — string warnings stay
/// for back-compat with existing orchestrators.
#[derive(Debug, Clone)]
pub enum ImportDbtStructuredWarning {
    /// dbt materialization that has no direct Rocky equivalent. The
    /// importer fell back to the closest match (typically `FullRefresh`).
    UnsupportedMaterialization {
        model: String,
        dbt_materialization: String,
        action: String,
    },
    /// dbt-databricks `databricks_tags` block was dropped — Rocky's
    /// `[classification]` block + `rocky-databricks` governance surface
    /// covers the same use case but requires manual config.
    DroppedDatabricksTags {
        model: String,
        tags: BTreeMap<String, String>,
    },
    /// A `pre_hook` or `post_hook` was dropped — Rocky supports lifecycle
    /// hooks via the `[[hook]]` block in `rocky.toml` but the importer
    /// doesn't auto-translate per-model dbt hooks.
    DroppedHook {
        model: String,
        hook_kind: HookKind,
        sql: String,
    },
    /// `on_schema_change` was dropped — Rocky exposes the equivalent
    /// behavior via per-pipeline `[drift]` policy.
    DroppedOnSchemaChange {
        model: String,
        dbt_value: String,
        rocky_equivalent: String,
    },
    /// A custom Jinja macro call survived compilation (i.e. dbt's compile
    /// step didn't inline it because it's defined out-of-tree). The user
    /// has to hand-port the macro or rewrite the model.
    UnresolvableMacro {
        model: String,
        macro_name: String,
        first_call_site_line: usize,
    },
    /// A microbatch model is missing `event_time` — dbt-databricks
    /// requires this field. The importer falls back to `FullRefresh` and
    /// surfaces the gap so the user can either add `event_time` to the
    /// dbt source or pick a non-microbatch strategy.
    MicrobatchMissingEventTime { model: String },
    /// A dbt microbatch model was remapped. With a `unique_key` it becomes an
    /// idempotent Rocky `merge` (`mapped_to = "merge"`); without one it stays
    /// append-only (`mapped_to = "append"`) and will re-insert the lookback
    /// window every run, so it must be converted manually.
    MicrobatchMapped { model: String, mapped_to: String },
    /// A dbt construct the importer does not translate was detected and
    /// skipped (snapshot, source freshness, grants, meta, metric, semantic
    /// model, an exposure or an exposure dependency that is not a model).
    /// Surfaced with a count so a migration is never silently lossy.
    DroppedConstruct {
        construct: String,
        name: String,
        detail: String,
    },
    /// A dbt model `contract` (`contract: { enforced: true }`) was written to
    /// a `{model}.contract.toml`, but part of it has no Rocky check.
    DroppedContract {
        model: String,
        /// Number of columns whose `data_type` has no Rocky type name.
        typed_columns: usize,
        /// Number of constraints Rocky does not check, other than
        /// `not_null` and `primary_key`.
        constraints: usize,
        /// Number of `not_null` and `primary_key` constraints. The contract
        /// does not check them: Rocky cannot prove a column NOT NULL from the
        /// sources, so `nullable = false` would refuse a valid model.
        not_null_constraints: usize,
        /// Path (relative to the emitted repo) of the generated contract.
        contract_path: String,
    },
}

/// A model that failed to import.
#[derive(Debug, Clone)]
pub struct ImportFailure {
    pub name: String,
    pub reason: String,
}

/// Result of importing a dbt project.
pub struct ImportResult {
    /// Successfully imported models (name, SQL, config).
    pub imported: Vec<ImportedModel>,
    /// Free-form warnings — string `message` + category enum. Existing
    /// orchestrator-visible surface; kept stable for back-compat.
    pub warnings: Vec<ImportWarning>,
    /// Typed structured warnings — payload-carrying variants that
    /// downstream UIs can pattern-match on (e.g. dropped tags, dropped
    /// hooks). New in Wave 2.
    pub structured_warnings: Vec<ImportDbtStructuredWarning>,
    /// Models that could not be imported.
    pub failed: Vec<ImportFailure>,
    /// Number of dbt source definitions found.
    pub sources_found: usize,
    /// Number of sources successfully mapped to Rocky config.
    pub sources_mapped: usize,
    /// Import method used.
    pub import_method: ImportMethod,
    /// dbt project name (if dbt_project.yml was found).
    pub project_name: Option<String>,
    /// dbt version (from manifest metadata).
    pub dbt_version: Option<String>,
    /// Test conversion stats (Phase 2).
    pub tests_found: usize,
    /// Number of tests converted to Rocky contracts.
    pub tests_converted: usize,
    /// Number of tests converted as custom SQL.
    pub tests_converted_custom: usize,
    /// Number of tests that could not be converted.
    pub tests_skipped: usize,
    /// Macro detection stats (Phase 2).
    pub macros_detected: usize,
    /// Number of macros successfully expanded.
    pub macros_expanded: usize,
    /// Number of macros resolved via manifest.
    pub macros_manifest_resolved: usize,
    /// Number of unsupported macros.
    pub macros_unsupported: usize,
    /// Total dbt `manifest.unit_tests` entries seen.
    pub unit_tests_found: usize,
    /// Number of unit tests translated to Rocky `[[test]]` sidecar
    /// blocks.
    pub unit_tests_converted: usize,
    /// Number of unit tests skipped — orphan target model, non-`dict`
    /// fixture format, or any other shape the importer can't faithfully
    /// translate.
    pub unit_tests_skipped: usize,
    /// Number of dbt resources the importer does not translate that were
    /// detected and skipped (snapshots, metrics, semantic models), plus the
    /// exposures and exposure dependencies that could not be carried over to
    /// a consumer. Surfaced so a migration is never silently lossy.
    pub constructs_dropped: usize,
    /// Number of dbt models whose enforced `contract` was written to a
    /// `{model}.contract.toml` but not fully: a column type Rocky has no name
    /// for, or a constraint Rocky does not check (`unique`, `check`, ...).
    pub contracts_dropped: usize,
    /// Downstream consumers built from dbt exposures. Each is written as a
    /// `consumers/<name>.toml` file and reads only models that were imported.
    pub consumers: Vec<rocky_core::consumers::Consumer>,
}

/// A successfully imported model.
pub struct ImportedModel {
    pub name: String,
    pub sql: String,
    pub config: ModelConfig,
    /// Unit tests harvested from `manifest.unit_tests` for this model.
    /// Emitted as `[[test]]` blocks alongside the sidecar TOML; not yet
    /// wired into the runtime test runner.
    pub unit_tests: Vec<UnitTestDef>,
    /// Body of `<name>.contract.toml`, generated from an enforced dbt
    /// contract. Written next to the model SQL.
    pub contract_toml: Option<String>,
}

// ---------------------------------------------------------------------------
// Import from manifest (fast path)
// ---------------------------------------------------------------------------

/// Read the `profile:` key declared in `<dbt_project>/dbt_project.yml`.
///
/// Returns `None` when the file is absent, unparseable, or carries no
/// `profile:` key. Used to select the matching profile from `profiles.yml`
/// instead of guessing the alphabetically-first one.
pub fn read_project_profile_name(dbt_project: &Path) -> Option<String> {
    let yml_path = dbt_project.join("dbt_project.yml");
    if !yml_path.exists() {
        return None;
    }
    match dbt_project::from_yaml(&yml_path) {
        Ok(cfg) => cfg.profile,
        Err(e) => {
            tracing::warn!("failed to parse dbt_project.yml for profile selection: {e}");
            None
        }
    }
}

/// Import models from a parsed dbt manifest.
///
/// Uses `compiled_code` (all Jinja resolved) for each model node, falling
/// back to `raw_code` if compiled_code is absent.
///
/// When `skip_unit_tests` is set, `manifest.unit_tests` entries are counted
/// but not converted (see [`apply_dbt_unit_tests`]) — backing the
/// `rocky import-dbt --skip-unit-tests` flag.
pub fn import_from_manifest(
    manifest: &DbtManifest,
    default_target: &TargetConfig,
    skip_unit_tests: bool,
    microbatch_mode: MicrobatchMode,
) -> ImportResult {
    let mut result = ImportResult {
        imported: Vec::new(),
        warnings: Vec::new(),
        structured_warnings: Vec::new(),
        failed: Vec::new(),
        sources_found: manifest.sources.len(),
        sources_mapped: manifest.sources.len(),
        import_method: ImportMethod::Manifest,
        project_name: Some(manifest.metadata.project_name.clone()),
        dbt_version: if manifest.metadata.dbt_version.is_empty() {
            None
        } else {
            Some(manifest.metadata.dbt_version.clone())
        },
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
        contracts_dropped: 0,
        consumers: Vec::new(),
    };

    // A manifest with no compiled SQL means every model falls back to the
    // reduced-fidelity raw-code path; detect it before importing so we can warn
    // loudly rather than emit a plausible-but-wrong repo.
    let model_count = manifest.nodes.len();
    let with_compiled = manifest
        .nodes
        .values()
        .filter(|n| n.compiled_code.is_some())
        .count();

    // Map every imported model's dbt `unique_id` to its bare Rocky name + the
    // compiled relation strings to search for, so each node's compiled body can
    // have its qualified upstream model refs rewritten to bare names. (FR-046)
    let model_relations = build_model_relation_map(manifest, default_target);

    for node in manifest.nodes.values() {
        import_manifest_node(
            node,
            default_target,
            microbatch_mode,
            manifest.full_refresh_compiled,
            &manifest.successfully_compiled_nodes,
            &model_relations,
            &manifest.groups,
            manifest.metadata.adapter_type.as_deref(),
            &mut result,
        );
    }

    if model_count > 0 && with_compiled == 0 {
        result.warnings.push(ImportWarning {
            model: "<manifest>".to_string(),
            category: WarningCategory::StaleManifest,
            message: format!(
                "none of the {model_count} manifest nodes carry compiled SQL — every model was \
                 imported via the reduced-fidelity raw-code path, which can mis-render Jinja. The \
                 import likely looks complete but is not faithful."
            ),
            suggestion: Some(
                "regenerate the manifest with `dbt compile --full-refresh` (including any required --vars) and re-import".to_string(),
            ),
        });
    }

    // Surface the resource classes the importer does not translate so a
    // migration is never silently lossy.
    record_dropped_constructs(&manifest.dropped, &mut result);
    import_exposures(manifest, &mut result);

    apply_dbt_unit_tests(manifest, &mut result, skip_unit_tests);

    result
}

/// Emit a structured `DroppedConstruct` warning per non-zero dropped resource
/// class (snapshots, metrics, semantic models, exposures) and bump
/// `constructs_dropped`.
fn record_dropped_constructs(dropped: &dbt_manifest::DbtDroppedCounts, result: &mut ImportResult) {
    for (construct, count, detail) in [
        (
            "snapshot",
            dropped.snapshots,
            "the snapshot could not be read from the manifest",
        ),
        (
            "metric",
            dropped.metrics,
            "MetricFlow metrics are not imported — keep your semantic layer in dbt or a metrics tool",
        ),
        (
            "semantic_model",
            dropped.semantic_models,
            "MetricFlow semantic models are not imported",
        ),
    ] {
        if count == 0 {
            continue;
        }
        result.constructs_dropped += count;
        result.warnings.push(ImportWarning {
            model: "<project>".to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: format!("{count} {construct}(s) skipped — {detail}"),
            suggestion: None,
        });
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedConstruct {
                construct: construct.to_string(),
                name: format!("{count} total"),
                detail: detail.to_string(),
            });
    }
}

/// The model name at the end of a `model.<project>.<name>` unique id.
fn extract_tail(id: &str) -> String {
    id.splitn(3, '.').nth(2).unwrap_or(id).to_string()
}

/// Turn each dbt exposure into a downstream consumer.
///
/// A consumer keeps a dependency only when it names a model that was imported,
/// because a name that is not a model is an `E060` compile error and the
/// emitted repo has to compile. Everything else an exposure reads (a source, a
/// seed, a model that failed to import) is listed in the migration notes. An
/// exposure whose name is not a valid consumer name is listed and not written.
fn import_exposures(manifest: &DbtManifest, result: &mut ImportResult) {
    let imported: std::collections::HashSet<String> =
        result.imported.iter().map(|m| m.name.clone()).collect();
    // Keyed by lowercase name: `Board.toml` and `board.toml` are one file on a
    // case-insensitive filesystem (macOS and Windows defaults), so the second
    // would silently overwrite the first.
    let mut seen: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for exposure in &manifest.exposures {
        let mut note = |construct: &str, detail: String| {
            result.constructs_dropped += 1;
            result
                .structured_warnings
                .push(ImportDbtStructuredWarning::DroppedConstruct {
                    construct: construct.to_string(),
                    name: exposure.name.clone(),
                    detail,
                });
        };
        if rocky_sql::validation::validate_identifier(&exposure.name).is_err() {
            note(
                "exposure",
                "its name is not a valid consumer name (letters, digits and underscores); \
                 rename it and add a file under consumers/ by hand"
                    .to_string(),
            );
            continue;
        }
        if let Some(first) = seen.get(&exposure.name.to_ascii_lowercase()) {
            let detail = if *first == exposure.name {
                "another exposure already has this name".to_string()
            } else {
                format!(
                    "its name differs from exposure `{first}` only by letter case, so on a \
                     case-insensitive filesystem both would be the one file consumers/{}.toml; \
                     not written. Rename one and add its file under consumers/ by hand",
                    exposure.name.to_ascii_lowercase()
                )
            };
            note("exposure", detail);
            continue;
        }
        seen.insert(exposure.name.to_ascii_lowercase(), exposure.name.clone());
        let mut depends_on = Vec::new();
        let mut not_carried: Vec<String> = exposure.other_dependencies.clone();
        for id in &exposure.models {
            // The importer names a versioned dbt model `<name>_v<N>`, so map
            // through the manifest node instead of reading the name off the id.
            let rocky_name = manifest
                .nodes
                .get(id)
                .and_then(|node| manifest_rocky_name(node).ok())
                .unwrap_or_else(|| extract_tail(id));
            if imported.contains(&rocky_name) {
                depends_on.push(rocky_name);
            } else {
                not_carried.push(format!("model {rocky_name} (not imported)"));
            }
        }
        depends_on.sort();
        depends_on.dedup();
        if !not_carried.is_empty() {
            note(
                "exposure dependency",
                format!(
                    "written to consumers/{}.toml without: {}. A consumer can only depend on \
                     imported models",
                    exposure.name,
                    not_carried.join(", ")
                ),
            );
        }
        result.consumers.push(rocky_core::consumers::Consumer {
            name: exposure.name.clone(),
            kind: exposure
                .kind
                .as_deref()
                .map(rocky_core::consumers::ConsumerKind::from_label)
                .unwrap_or_default(),
            owner: exposure.owner.clone(),
            url: exposure.url.clone(),
            description: exposure.description.clone(),
            depends_on,
            file_path: std::path::PathBuf::new(),
        });
    }
}

/// Walk a dbt project's `models/` tree for `schema.yml` files, convert any
/// `tests:` / `data_tests:` entries to canonical Rocky [`TestDecl`]s, attach
/// them to the matching imported model, and emit structured warnings for
/// tests outside the four canonical built-ins. Counter fields on
/// [`ImportResult`] are updated in place.
///
/// Both spellings of the key are accepted (`data_tests:` is dbt 1.7+; the
/// legacy `tests:` form still works on the dbt side and on the Rocky
/// importer). Unit tests from `manifest.unit_tests` are handled separately
/// by [`apply_dbt_unit_tests`].
///
/// The caller passes the dbt project root (or any directory the YAMLs live
/// under). Both the `models/` regex path and the manifest path use this
/// helper so test mapping works whether or not a manifest is present.
pub fn apply_dbt_tests(yaml_root: &Path, default_target: &TargetConfig, result: &mut ImportResult) {
    let model_yamls = match super::dbt_tests::parse_model_yamls(yaml_root) {
        Ok(map) if !map.is_empty() => map,
        _ => return,
    };

    // Build a name → (catalog, schema) lookup for relationship FQN resolution.
    let mut targets: std::collections::HashMap<String, (String, String)> =
        std::collections::HashMap::new();
    for m in &result.imported {
        targets.insert(
            m.config.name.clone(),
            (
                m.config.target.catalog.clone(),
                m.config.target.schema.clone(),
            ),
        );
    }
    let resolver = super::dbt_tests::ImportedTargetResolver {
        targets: &targets,
        default_catalog: &default_target.catalog,
        default_schema: &default_target.schema,
    };

    for (model_name, model_yaml) in &model_yamls {
        let total_tests: usize = model_yaml
            .columns
            .iter()
            .map(|c| c.tests.len())
            .sum::<usize>()
            + model_yaml.tests.len();
        if total_tests == 0 {
            // Still let YAML descriptions seed `intent` for matching imported models.
            attach_intent(result, model_name, model_yaml.description.as_deref());
            continue;
        }
        result.tests_found += total_tests;

        let (decls, unsupported) = super::dbt_tests::tests_to_test_decls(model_yaml, &resolver);

        // A model the importer refused (or did not pick up) has no sidecar,
        // so its tests are written nowhere: count them as skipped, not
        // converted (#2337).
        let model_imported = result.imported.iter().any(|m| &m.name == model_name);
        if !model_imported {
            result.tests_skipped += decls.len();
        }
        let decls = if model_imported { decls } else { Vec::new() };

        result.tests_converted += decls.len();
        // "custom" = converted tests that aren't the canonical column-level
        // built-ins (not_null / unique / accepted_values / relationships):
        // the composite (`unique_combination_of_columns`) conversion plus the
        // dbt_expectations / dbt_utils long-tail mappings (in_range,
        // regex_match, expression).
        result.tests_converted_custom += decls
            .iter()
            .filter(|d| {
                use rocky_core::tests::TestType;
                matches!(
                    d.test_type,
                    TestType::Composite { .. }
                        | TestType::InRange { .. }
                        | TestType::RegexMatch { .. }
                        | TestType::Expression { .. }
                )
            })
            .count();
        result.tests_skipped += unsupported.len();

        // Attach the decls to the matching imported model (if present).
        if let Some(imported) = result.imported.iter_mut().find(|m| &m.name == model_name) {
            imported.config.tests.extend(decls);
        }

        // Surface every unsupported test as a warning — including model-level
        // ones (column == None), which render as "model-level".
        for u in unsupported {
            let where_ = match &u.column {
                Some(c) => format!("column '{c}'"),
                None => "model-level".to_string(),
            };
            result.warnings.push(ImportWarning {
                model: u.model.clone(),
                category: WarningCategory::UnsupportedTest,
                message: format!(
                    "dbt test '{name}' on {where_} has no native Rocky equivalent (supported: unique, not_null, accepted_values, relationships, unique_combination_of_columns, and the dbt_expectations/dbt_utils range/regex/in-set/expression tests) — not translated",
                    name = u.test_name,
                ),
                suggestion: Some(
                    "rewrite as a Rocky `[[tests]]` of type `expression` or as a custom check in a quality pipeline".to_string(),
                ),
            });
        }

        attach_intent(result, model_name, model_yaml.description.as_deref());
    }
}

/// Walk `manifest.unit_tests`, translate each entry to a Rocky
/// [`UnitTestDef`], and attach it to the matching imported model. Entries
/// that target a model the importer didn't pick up emit an
/// [`WarningCategory::OrphanUnitTest`] warning and are counted as
/// skipped. Entries whose `expect.format` is anything other than `"dict"`
/// (or absent) emit [`WarningCategory::UnsupportedUnitTestFormat`] and
/// are also skipped — CSV / SQL fixtures aren't supported in the Rocky
/// sidecar shape today.
///
/// The `unit_tests_found`, `unit_tests_converted`, and
/// `unit_tests_skipped` counters on [`ImportResult`] are updated in
/// place.
///
/// When `skip_unit_tests` is set, every entry is still counted in
/// `unit_tests_found` but none are converted — each bumps
/// `unit_tests_skipped` so the report stays honest. This backs the
/// `rocky import-dbt --skip-unit-tests` escape hatch.
pub fn apply_dbt_unit_tests(
    manifest: &DbtManifest,
    result: &mut ImportResult,
    skip_unit_tests: bool,
) {
    for ut in manifest.unit_tests.values() {
        result.unit_tests_found += 1;

        if skip_unit_tests {
            result.unit_tests_skipped += 1;
            continue;
        }

        if let Some(format) = ut.expect.format.as_deref()
            && !is_dict_format(format)
        {
            result.warnings.push(ImportWarning {
                    model: ut.model.clone(),
                    category: WarningCategory::UnsupportedUnitTestFormat,
                    message: format!(
                        "dbt unit_test '{}' uses expect.format='{format}' — only inline `dict` rows are supported",
                        ut.name
                    ),
                    suggestion: Some(
                        "convert the expected rows to inline `format: dict` in the dbt unit_test, or hand-port to a Rocky [[test]] block".to_string(),
                    ),
                });
            result.unit_tests_skipped += 1;
            continue;
        }

        // dbt's per-given format defaults to `dict` (inline rows). Skip
        // the whole test if any input requests a non-dict shape — we
        // can't faithfully build a fixture from a CSV path the manifest
        // doesn't carry.
        let unsupported_given_format = ut.given.iter().find_map(|g| {
            g.format
                .as_deref()
                .filter(|f| !is_dict_format(f))
                .map(str::to_string)
        });
        if let Some(format) = unsupported_given_format {
            result.warnings.push(ImportWarning {
                model: ut.model.clone(),
                category: WarningCategory::UnsupportedUnitTestFormat,
                message: format!(
                    "dbt unit_test '{}' uses given.format='{format}' — only inline `dict` rows are supported",
                    ut.name
                ),
                suggestion: Some(
                    "inline the CSV fixture into the dbt unit_test as `format: dict`, or hand-port to a Rocky [[test]] block".to_string(),
                ),
            });
            result.unit_tests_skipped += 1;
            continue;
        }

        let Some(imported) = result.imported.iter_mut().find(|m| m.name == ut.model) else {
            result.warnings.push(ImportWarning {
                model: ut.model.clone(),
                category: WarningCategory::OrphanUnitTest,
                message: format!(
                    "dbt unit_test '{}' targets model '{}' which was not imported",
                    ut.name, ut.model
                ),
                suggestion: Some(
                    "drop the unit test or wait until the model imports cleanly".to_string(),
                ),
            });
            result.unit_tests_skipped += 1;
            continue;
        };

        let test_def = UnitTestDef {
            name: ut.name.clone(),
            description: ut.description.clone(),
            given: ut.given.iter().map(convert_unit_test_given).collect(),
            expect: convert_unit_test_expect(&ut.expect),
        };

        // Guard: the Rocky sidecar serializes `[[test]]` blocks to TOML,
        // which has no `null` type. A fixture/expectation row carrying a
        // `null` field value — or a non-object row element — can't be
        // represented, and the `toml` crate fails the whole serialize with
        // "unsupported unit type". Catch it here, per test, so one bad
        // fixture never aborts the import of every other model/seed/test.
        if let Err(reason) = check_unit_test_serializable(&test_def) {
            tracing::warn!(
                model = %ut.model,
                unit_test = %ut.name,
                reason = %reason,
                "dropping dbt unit_test that can't be represented in Rocky sidecar TOML",
            );
            result.warnings.push(ImportWarning {
                model: ut.model.clone(),
                category: WarningCategory::UnserializableUnitTest,
                message: format!(
                    "dbt unit_test '{}' can't be represented in the Rocky sidecar TOML ({reason}) — skipped",
                    ut.name
                ),
                suggestion: Some(
                    "TOML has no null type; replace `null` fixture values with a sentinel \
                     (or omit the column) in the dbt unit_test, then re-import".to_string(),
                ),
            });
            result.unit_tests_skipped += 1;
            continue;
        }

        imported.unit_tests.push(test_def);
        result.unit_tests_converted += 1;
    }
}

/// Check that a converted unit test round-trips to the sidecar TOML the
/// emitter writes. Returns `Err(reason)` when the `toml` crate rejects the
/// shape *after* null-stripping — e.g. a nested array/object TOML cannot
/// express.
///
/// Null fixture/expectation cells are no longer fatal: TOML has no `null`
/// type, so the emitter omits `null`-valued keys ([`emit::strip_null_fixture_cells`])
/// and the run-side fixture builder materializes the absent cell back to SQL
/// `NULL`. This check applies the same strip before serializing, so a test
/// whose only obstacle was a null cell now passes here and ports as
/// `CONVERTED`. (FR-045)
fn check_unit_test_serializable(test: &UnitTestDef) -> Result<(), String> {
    #[derive(serde::Serialize)]
    struct Wrapper<'a> {
        test: [&'a UnitTestDef; 1],
    }
    let stripped = crate::import::emit::strip_null_fixture_cells(test);
    toml::to_string(&Wrapper { test: [&stripped] })
        .map(|_| ())
        .map_err(|e| e.to_string())
}

/// `format` is treated as `dict` when absent or explicitly set to
/// `"dict"` (case-insensitive).
fn is_dict_format(format: &str) -> bool {
    format.eq_ignore_ascii_case("dict")
}

fn convert_unit_test_given(g: &DbtUnitTestGiven) -> TestFixture {
    TestFixture {
        model_ref: strip_ref_wrapper(&g.input),
        rows: g.rows.clone(),
    }
}

fn convert_unit_test_expect(e: &DbtUnitTestExpect) -> TestExpectation {
    TestExpectation {
        rows: e.rows.clone(),
        ordered: false,
    }
}

/// Strip dbt's `ref('foo')` / `source('a','b')` wrappers down to a bare
/// table ref. `ref('orders')` → `"orders"`,
/// `source('raw','orders')` → `"raw.orders"`. Unwrapped inputs (already
/// bare names) round-trip unchanged after trimming whitespace and
/// quotes.
pub(crate) fn strip_ref_wrapper(input: &str) -> String {
    let trimmed = input.trim();
    if let Some(inner) = strip_call(trimmed, "ref") {
        let bare = strip_single_arg(inner);
        if !bare.is_empty() {
            return bare;
        }
    }
    if let Some(inner) = strip_call(trimmed, "source")
        && let Some((src, tbl)) = split_source_args(inner)
    {
        return format!("{src}.{tbl}");
    }
    trimmed
        .trim_matches(|c: char| c == '\'' || c == '"')
        .to_string()
}

/// Match `<name>(<inner>)`, returning the inner span on success.
fn strip_call<'a>(input: &'a str, name: &str) -> Option<&'a str> {
    let after_name = input.strip_prefix(name)?.trim_start();
    let inner = after_name.strip_prefix('(')?.strip_suffix(')')?;
    Some(inner)
}

fn strip_single_arg(inner: &str) -> String {
    inner
        .trim()
        .trim_matches(|c: char| c == '\'' || c == '"')
        .to_string()
}

fn split_source_args(inner: &str) -> Option<(String, String)> {
    let mut parts = inner.split(',');
    let src = parts.next()?;
    let tbl = parts.next()?;
    if parts.next().is_some() {
        // More than two args — refuse rather than guess.
        return None;
    }
    let src = src.trim().trim_matches(|c: char| c == '\'' || c == '"');
    let tbl = tbl.trim().trim_matches(|c: char| c == '\'' || c == '"');
    if src.is_empty() || tbl.is_empty() {
        return None;
    }
    Some((src.to_string(), tbl.to_string()))
}

fn attach_intent(result: &mut ImportResult, model_name: &str, description: Option<&str>) {
    let Some(desc) = description else {
        return;
    };
    if let Some(imported) = result.imported.iter_mut().find(|m| m.name == model_name)
        && imported.config.intent.is_none()
    {
        imported.config.intent = Some(desc.to_string());
    }
}

/// An imported upstream model's identity for the compiled-body FQN→bare
/// rewrite: the bare Rocky name plus the compiled relation strings to search
/// for in a downstream body (longest first). (FR-046)
struct UpstreamModel {
    bare_name: String,
    fqn_candidates: Vec<String>,
    /// The upstream's own `depends_on` when it is `ephemeral`. dbt inlines an
    /// ephemeral model's compiled body into each consumer as a
    /// `__dbt__cte__<name>` CTE, so the consumer's `compiled_code` reads the
    /// ephemeral's upstreams by their qualified relation even though they are
    /// not in the consumer's own `depends_on`.
    ephemeral_deps: Vec<String>,
}

/// Resolve a model node's output `(catalog, schema, table)` the way the emitted
/// Rocky `[target]` block does: dbt `alias` overrides the table name, while
/// `config.schema` / `database` fall back to the import default target. Shared
/// by node emission and the FQN-fallback path so the two never drift.
fn resolve_node_coords(
    node: &DbtManifestNode,
    default_target: &TargetConfig,
) -> (String, String, String) {
    let schema = node
        .config
        .schema
        .clone()
        .unwrap_or_else(|| default_target.schema.clone());
    let catalog = if node.database.is_empty() {
        default_target.catalog.clone()
    } else {
        node.database.clone()
    };
    // dbt's default relation for version N of a versioned model is
    // `<name>_v<N>`, which is also the Rocky model name.
    let table = node
        .config
        .alias
        .clone()
        .unwrap_or_else(|| manifest_rocky_name(node).unwrap_or_else(|_| node.name.clone()));
    (catalog, schema, table)
}

/// Rocky model name of a manifest node: `<name>_v<N>` for a versioned dbt
/// model, else the node name. Errors on a version that is not a whole number.
fn manifest_rocky_name(node: &DbtManifestNode) -> Result<String, String> {
    super::dbt_governance::rocky_model_name(&node.name, node.governance.version.as_deref())
}

/// Build the upstream-model lookup keyed by dbt `unique_id`. Only `model.*`
/// nodes appear (the manifest parser already drops tests/seeds/sources), so a
/// `depends_on` scan that consults this map rewrites only genuine model
/// references and leaves `source()` FQNs qualified. (FR-046)
fn build_model_relation_map(
    manifest: &DbtManifest,
    default_target: &TargetConfig,
) -> HashMap<String, UpstreamModel> {
    manifest
        .nodes
        .iter()
        .map(|(id, node)| {
            (
                id.clone(),
                UpstreamModel {
                    bare_name: manifest_rocky_name(node).unwrap_or_else(|_| node.name.clone()),
                    fqn_candidates: relation_candidates(node, default_target),
                    ephemeral_deps: if node.config.materialized == "ephemeral" {
                        node.depends_on.nodes.clone()
                    } else {
                        Vec::new()
                    },
                },
            )
        })
        .collect()
}

/// The compiled-relation strings to match for an upstream model, longest first.
///
/// dbt emits `compiled_code` using the node's `relation_name` verbatim, so that
/// exact (adapter-quoted) form is the primary candidate; a de-quoted variant
/// (`a.b.c`) covers adapters/manifests that don't quote. When `relation_name`
/// is absent (older manifests) the FQN is reconstructed from the resolved
/// coordinates as a fallback. (FR-046)
fn relation_candidates(node: &DbtManifestNode, default_target: &TargetConfig) -> Vec<String> {
    let mut candidates: Vec<String> = Vec::new();
    match node
        .relation_name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Some(relation) => {
            candidates.push(relation.to_string());
            let dequoted = dequote_relation(relation);
            if dequoted != relation {
                candidates.push(dequoted);
            }
        }
        None => {
            let (catalog, schema, table) = resolve_node_coords(node, default_target);
            candidates.push(format!("{catalog}.{schema}.{table}"));
        }
    }
    candidates.retain(|c| !c.is_empty());
    // Longest first so a quoted relation is tried before its shorter de-quoted
    // form (the body only carries one, but ordering keeps the scan unambiguous).
    candidates.sort_by_key(|c| std::cmp::Reverse(c.len()));
    candidates.dedup();
    candidates
}

/// Strip identifier quoting (`"` and backtick) from a relation string:
/// `"db"."s"."t"` → `db.s.t`, `` `db`.`s`.`t` `` → `db.s.t`.
fn dequote_relation(relation: &str) -> String {
    relation
        .chars()
        .filter(|c| *c != '"' && *c != '`')
        .collect()
}

/// Rewrite dbt's compiled, fully-qualified upstream model references in a model
/// body back to bare Rocky model names.
///
/// dbt's `compiled_code` resolves `{{ ref('up') }}` to the upstream's physical
/// relation (`"db"."schema"."up"` on duckdb, backtick-quoted on databricks),
/// but Rocky resolves model references by BARE name — a qualified ref is treated
/// as an external/source table. Left as-is, a `SELECT *` over a qualified
/// upstream can't resolve its schema (E020 on microbatch/incremental models) and
/// imported unit-test fixtures (mocked by bare name) fail to bind with
/// "Catalog … does not exist".
///
/// For every `model.*` entry in this node's `depends_on` that was itself
/// imported, the upstream's relation FQN (and a de-quoted variant) is replaced
/// with the bare name. Genuine `source()` references stay qualified because
/// sources are `source.*` unique_ids and never appear in `models`. A `{{ this }}`
/// self-reference also stays untouched (the node isn't in its own
/// `depends_on`), which is correct — Rocky resolves the model's own physical
/// relation at run time. (FR-046)
fn rewrite_upstream_refs_to_bare(
    body: &str,
    node: &DbtManifestNode,
    models: &HashMap<String, UpstreamModel>,
) -> String {
    let mut out = body.to_string();
    for upstream_id in compiled_model_reads(node, models) {
        let Some(upstream) = models.get(&upstream_id) else {
            continue;
        };
        for needle in &upstream.fqn_candidates {
            out = replace_relation_ref(&out, needle, &upstream.bare_name);
        }
    }
    out
}

/// The model `unique_id`s a node's `compiled_code` can read by relation: its
/// own `depends_on` models plus, transitively, the upstreams of any
/// `ephemeral` model among them (whose body dbt inlined as a CTE).
fn compiled_model_reads(
    node: &DbtManifestNode,
    models: &HashMap<String, UpstreamModel>,
) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut order = Vec::new();
    let mut stack: Vec<String> = node.depends_on.nodes.iter().rev().cloned().collect();
    while let Some(id) = stack.pop() {
        if !id.starts_with("model.") || id == node.unique_id || !seen.insert(id.clone()) {
            continue;
        }
        if let Some(upstream) = models.get(&id) {
            stack.extend(upstream.ephemeral_deps.iter().rev().cloned());
        }
        order.push(id);
    }
    order
}

/// Replace every standalone occurrence of `needle` in `haystack` with
/// `replacement`, refusing matches that sit inside a longer identifier (a
/// neighbouring `[A-Za-z0-9_]`). The adapter-quoted relation form is already
/// self-bounding (the closing quote terminates it); the guard matters for the
/// de-quoted `a.b.c` variant, which must not clobber the prefix of
/// `a.b.c_daily`. (FR-046)
fn replace_relation_ref(haystack: &str, needle: &str, replacement: &str) -> String {
    if needle.is_empty() || !haystack.contains(needle) {
        return haystack.to_string();
    }
    let mut out = String::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(pos) = rest.find(needle) {
        // `before` is the char just left of the match: take it from the slice
        // we're about to keep, or from the already-emitted output when the
        // match sits at the start of `rest`.
        let before = if pos > 0 {
            rest[..pos].chars().next_back()
        } else {
            out.chars().next_back()
        };
        let after = rest[pos + needle.len()..].chars().next();
        let standalone = before.is_none_or(|c| !is_relation_ident_char(c))
            && after.is_none_or(|c| !is_relation_ident_char(c));
        out.push_str(&rest[..pos]);
        out.push_str(if standalone { replacement } else { needle });
        rest = &rest[pos + needle.len()..];
    }
    out.push_str(rest);
    out
}

/// Identifier characters for the relation-ref boundary guard. A relation match
/// is only rewritten when neither neighbour is one of these.
fn is_relation_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

#[allow(clippy::too_many_arguments)]
fn import_manifest_node(
    node: &DbtManifestNode,
    default_target: &TargetConfig,
    microbatch_mode: MicrobatchMode,
    manifest_full_refresh_compiled: bool,
    successfully_compiled_nodes: &std::collections::HashSet<String>,
    model_relations: &HashMap<String, UpstreamModel>,
    groups: &std::collections::BTreeMap<String, rocky_core::model_governance::GroupOwner>,
    adapter: Option<&str>,
    result: &mut ImportResult,
) {
    // A snapshot node converts to a `type = "snapshot"` model, or fails with
    // the reason; it is never dropped.
    let snapshot_strategy = match &node.config.snapshot {
        Some(cfg) => match super::dbt_snapshots::snapshot_strategy_from_dbt(cfg) {
            Ok(strategy) => Some(strategy),
            Err(reason) => {
                result.failed.push(ImportFailure {
                    name: node.name.clone(),
                    reason,
                });
                return;
            }
        },
        None => None,
    };
    // A versioned dbt model (`version: N`) becomes the Rocky model
    // `<name>_v<N>`; the emitter writes the shared version declaration.
    let rocky_name = match manifest_rocky_name(node) {
        Ok(name) => name,
        Err(reason) => {
            result.failed.push(ImportFailure {
                name: node.name.clone(),
                reason,
            });
            return;
        }
    };
    if node.config.materialized == "incremental" {
        if node.config.full_refresh == Some(false) {
            result.failed.push(ImportFailure {
                name: rocky_name.clone(),
                reason: INCREMENTAL_FULL_REFRESH_DISABLED.to_string(),
            });
            return;
        }
        if !manifest_full_refresh_compiled
            || !successfully_compiled_nodes.contains(&node.unique_id)
            || node.compiled_code.is_none()
        {
            result.failed.push(ImportFailure {
                name: rocky_name.clone(),
                reason: INCREMENTAL_COMPILE_EVIDENCE_REFUSED.to_string(),
            });
            return;
        }
    }

    // Resolve the model's output coordinates up front so the raw-code fallback
    // (for {{ this }}) and the emitted target use the same values. dbt `alias`
    // overrides the relation name; dropping it silently lands the data in a
    // table named after the node.
    let (catalog, schema, table) = resolve_node_coords(node, default_target);
    let this_ref = format!("{catalog}.{schema}.{table}");

    // The standard dbt watermark filter (`{% if is_incremental() %} WHERE
    // <col> > (SELECT MAX(<wm>) FROM {{ this }}) {% endif %}`) maps to a Rocky
    // `incremental` model. The compile-evidence gates above still apply.
    let conversion = if node.config.materialized == "incremental" {
        find_is_incremental_filter(&node.raw_code).and_then(|recognized| {
            map_is_incremental_conversion(
                &node.config,
                &node.name,
                Some(&recognized.watermark),
                recognized.filter_column.as_deref(),
            )
            .map(|converted| (recognized, converted))
        })
    } else {
        None
    };

    // Use compiled_code (Jinja resolved) if available, else raw_code. dbt's
    // compiled body carries qualified upstream model refs (`"db"."schema"."up"`);
    // rewrite those back to bare Rocky names so the imported repo compiles and
    // unit-tests. The raw_code fallback already lowers `{{ ref() }}` to a bare
    // name via `convert_jinja_to_sql`, so it needs no rewrite. (FR-046)
    let placeholder_sql = conversion.as_ref().and_then(|(recognized, _)| {
        if recognized.other_statement_tags {
            return None;
        }
        let converted = convert_jinja_to_sql(&recognized.rewritten, &this_ref);
        // An expression the converter cannot lower (a custom macro call)
        // would be left as a TODO comment; the compiled code is exact.
        (!converted.contains("TODO: unsupported Jinja")).then_some(converted)
    });
    if conversion.is_some() && placeholder_sql.is_none() {
        result.warnings.push(ImportWarning {
            model: node.name.clone(),
            category: WarningCategory::MappedConstruct,
            message: "raw_code has Jinja beyond the `is_incremental()` filter, so the model SQL \
                      is the full-refresh compiled_code with no `@incremental_filter` \
                      placeholder; Rocky filters the output on the watermark column instead, \
                      which `rocky compile` allows only for a passthrough column (E046)"
                .to_string(),
            suggestion: Some(
                "if `rocky compile` reports E046, add `WHERE @incremental_filter` to the \
                 imported SQL where dbt applied the filter, and set `filter_column` in the \
                 sidecar [strategy] block when it compares a qualified or renamed input column"
                    .to_string(),
            ),
        });
    }
    let mut sql = match (&placeholder_sql, &node.compiled_code) {
        (Some(converted), _) => converted.clone(),
        (None, Some(code)) => rewrite_upstream_refs_to_bare(code, node, model_relations),
        (None, None) if snapshot_strategy.is_some() => {
            // A legacy snapshot's raw_code is the whole `{% snapshot %}` block;
            // only its body is the SELECT.
            let body = super::dbt_snapshots::legacy_snapshot_blocks(&node.raw_code)
                .first()
                .map_or(node.raw_code.as_str(), |b| b.body);
            if body.contains("{%") {
                result.failed.push(ImportFailure {
                    name: node.name.clone(),
                    reason: RAW_JINJA_CONTROL_REFUSED.to_string(),
                });
                return;
            }
            convert_jinja_to_sql(body, &this_ref)
        }
        (None, None) => {
            if node.raw_code.contains("{%") {
                result.failed.push(ImportFailure {
                    name: node.name.clone(),
                    reason: RAW_JINJA_CONTROL_REFUSED.to_string(),
                });
                return;
            }
            if contains_unresolved_is_incremental(&node.raw_code) {
                result.failed.push(ImportFailure {
                    name: node.name.clone(),
                    reason: RAW_INCREMENTAL_ERROR.to_string(),
                });
                return;
            }
            result.warnings.push(ImportWarning {
                model: node.name.clone(),
                category: WarningCategory::JinjaControlFlow,
                message: "no compiled_code in manifest; using raw_code (may contain Jinja)"
                    .to_string(),
                suggestion: Some(
                    "run `dbt compile --full-refresh` to generate compiled SQL".to_string(),
                ),
            });
            convert_jinja_to_sql(&node.raw_code, &this_ref)
        }
    };

    // Map strategy from manifest config — covers all dbt materializations
    // (`table`, `view`, `materialized_view`, `incremental`, `ephemeral`,
    // `microbatch`) plus the `incremental_strategy` discriminator.
    let on_schema_change_mapped = conversion.is_some();
    let StrategyMappingOutput {
        strategy,
        warnings: strategy_warnings,
        structured,
    } = match (conversion, snapshot_strategy) {
        (Some((_, converted)), _) => StrategyMappingOutput {
            strategy: converted.strategy,
            warnings: converted.warnings,
            structured: Vec::new(),
        },
        (None, Some(strategy)) => StrategyMappingOutput {
            strategy,
            warnings: vec![super::dbt_snapshots::snapshot_import_note(&node.name)],
            structured: Vec::new(),
        },
        (None, None) => map_manifest_strategy(&node.config, &node.name, microbatch_mode),
    };

    // #1990: an incremental dbt model with no Rocky append equivalent falls
    // back to `full_refresh`. That is safe only when the SQL has no dbt
    // incremental branch. dbt compiles `is_incremental()` as true against an
    // existing target, so `compiled_code` can keep a delta filter such as
    // `WHERE updated_at > '<last load>'`; run as `full_refresh`, that would
    // replace the table with only the recent rows on every run. Refuse it
    // instead, before any warning claims a mapping.
    if matches!(strategy, StrategyConfig::FullRefresh)
        && matches!(
            node.config.materialized.as_str(),
            "incremental" | "microbatch"
        )
        && contains_unresolved_is_incremental(&node.raw_code)
    {
        result.failed.push(ImportFailure {
            name: node.name.clone(),
            reason: INCREMENTAL_FALLBACK_REFUSED.to_string(),
        });
        return;
    }

    result.warnings.extend(strategy_warnings);
    result.structured_warnings.extend(structured);

    // A `time_interval` strategy can only originate from microbatch translation
    // (`--microbatch-as=time_interval`). The model body must reference
    // `@start_date`/`@end_date` so the runtime can bound each partition; wrap
    // the original SELECT in a bounded subquery on the event-time column.
    if let StrategyConfig::TimeInterval { time_column, .. } = &strategy {
        sql = rewrite_body_for_time_interval(&sql, time_column);
    }

    // Surface dbt-databricks specifics that Rocky doesn't auto-translate
    // (databricks_tags, pre/post hooks, on_schema_change). Emitted as
    // structured warnings so the downstream UI can route them.
    collect_dropped_config_warnings(&node.config, &node.name, on_schema_change_mapped, result);

    // Surface a dropped model contract (`contract: { enforced: true }` +
    // column data_type/constraints). Rocky enforces contracts via a sidecar
    // the importer doesn't generate — point the user at where to author it.
    let contract_toml = collect_contract(node, &rocky_name, adapter, result);

    // Detect unresolvable Jinja macros that survived `dbt compile`. dbt's
    // compile step inlines in-tree macros, so anything still present
    // points at an out-of-tree macro the user needs to hand-port.
    collect_unresolvable_macros(&sql, &node.name, result);

    // Map dependencies. Resolve through the relation map first so a versioned
    // upstream (`model.p.orders.v1`) maps to `orders_v1`, not to `v1`. A
    // snapshot upstream (`snapshot.p.orders_snap`) is a Rocky model too.
    let mut depends_on: Vec<String> = node
        .depends_on
        .nodes
        .iter()
        .filter(|id| id.starts_with("model.") || id.starts_with("snapshot."))
        .map(|id| match model_relations.get(id) {
            Some(upstream) => upstream.bare_name.clone(),
            None => dbt_manifest::extract_model_name(id).to_string(),
        })
        .collect();
    // An inlined ephemeral's upstreams are read by this body too (by bare
    // name, after the rewrite above), so they are dependencies of it.
    if node.compiled_code.is_some() {
        for id in compiled_model_reads(node, model_relations) {
            if let Some(upstream) = model_relations.get(&id)
                && !depends_on.contains(&upstream.bare_name)
            {
                depends_on.push(upstream.bare_name.clone());
            }
        }
    }

    // Use description as intent
    let intent = node.description.clone();

    // dbt model governance: access, group (+ owner), version.
    let (governance, governance_warnings) = super::dbt_governance::governance_from_dbt(
        &super::dbt_governance::DbtGovernanceInput {
            name: &node.name,
            rocky_name: &rocky_name,
            access: node.governance.access.as_deref(),
            group: node.governance.group.as_deref(),
            version: node.governance.version.as_deref(),
            latest_version: node.governance.latest_version.as_deref(),
            deprecation_date: node.governance.deprecation_date.as_deref(),
        },
        groups,
    );
    result.warnings.extend(governance_warnings);

    let config = ModelConfig {
        name: rocky_name.clone(),
        depends_on,
        strategy,
        target: TargetConfig {
            catalog,
            schema,
            table,
        },
        sources: vec![],
        adapter: None,
        intent,
        freshness: None,
        tests: vec![],
        format: None,
        format_options: None,
        classification: Default::default(),
        tags: dbt_tags_to_map(&node.tags),
        governance,
        retention: None,
        budget: None,
        skip: None,
        name_declared: String::new(),
        target_table_declared: String::new(),
    };

    result.imported.push(ImportedModel {
        name: rocky_name,
        sql: sql.trim().to_string(),
        config,
        unit_tests: Vec::new(),
        contract_toml,
    });
}

/// Output of mapping a dbt node config to a Rocky strategy. Returns the
/// chosen [`StrategyConfig`] plus both warning kinds (the back-compat
/// string warnings + the new structured variants).
struct StrategyMappingOutput {
    strategy: StrategyConfig,
    warnings: Vec<ImportWarning>,
    structured: Vec<ImportDbtStructuredWarning>,
}

/// Map a dbt node config to a Rocky [`StrategyConfig`].
///
/// Covers `table` / `view` / `materialized_view` / `incremental`
/// (across all `incremental_strategy` values) / `ephemeral` /
/// `microbatch`. Unknown materializations fall back to `FullRefresh` with
/// a warning.
fn map_manifest_strategy(
    config: &DbtNodeConfig,
    model_name: &str,
    microbatch_mode: MicrobatchMode,
) -> StrategyMappingOutput {
    let mut warnings = Vec::new();
    let mut structured = Vec::new();

    let strategy = match config.materialized.as_str() {
        "table" => StrategyConfig::FullRefresh,
        "view" => StrategyConfig::View,
        "materialized_view" => StrategyConfig::MaterializedView,
        // Rocky inlines an ephemeral model into each consumer as a CTE, the
        // same contract as dbt. A consumer imported from `compiled_code`
        // already carries dbt's `__dbt__cte__<name>` CTE and runs as-is.
        "ephemeral" => StrategyConfig::Ephemeral,
        "incremental" => map_incremental_strategy(
            config,
            model_name,
            microbatch_mode,
            &mut warnings,
            &mut structured,
        ),
        "microbatch" => map_microbatch_strategy(
            config,
            model_name,
            microbatch_mode,
            &mut warnings,
            &mut structured,
        ),
        other => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "materialized='{other}' not recognized by Rocky — using full_refresh"
                ),
                suggestion: Some(
                    "set `type` in the emitted strategy block to a Rocky-supported value (full_refresh / merge / view / materialized_view / dynamic_table / time_interval / delete_insert / microbatch)".to_string(),
                ),
            });
            structured.push(ImportDbtStructuredWarning::UnsupportedMaterialization {
                model: model_name.to_string(),
                dbt_materialization: other.to_string(),
                action: "fell back to full_refresh".to_string(),
            });
            StrategyConfig::FullRefresh
        }
    };

    StrategyMappingOutput {
        strategy,
        warnings,
        structured,
    }
}

/// Map `materialized='incremental'` + `incremental_strategy=<...>` to the
/// appropriate Rocky strategy variant.
fn map_incremental_strategy(
    config: &DbtNodeConfig,
    model_name: &str,
    microbatch_mode: MicrobatchMode,
    warnings: &mut Vec<ImportWarning>,
    structured: &mut Vec<ImportDbtStructuredWarning>,
) -> StrategyConfig {
    let unique_keys: Option<Vec<String>> = config.unique_key.as_ref().map(|uk| match uk {
        UniqueKeyValue::Single(s) => vec![s.clone()],
        UniqueKeyValue::Multiple(v) => v.clone(),
    });

    // Default discriminator: `append` if no unique_key, else `merge`.
    // dbt-databricks treats unique_key as implying merge semantics when
    // incremental_strategy is unset.
    let strategy_kind = config
        .incremental_strategy
        .as_deref()
        .map(str::to_ascii_lowercase)
        .unwrap_or_else(|| {
            if unique_keys.is_some() {
                "merge".to_string()
            } else {
                "append".to_string()
            }
        });

    match strategy_kind.as_str() {
        "merge" => match unique_keys {
            Some(keys) if !keys.is_empty() => {
                // dbt `merge_exclude_columns` (update all-but-these) inverts
                // to an explicit `update_columns` — but that needs the full
                // physical column list, which the manifest does not carry
                // (it lives in catalog.json from `dbt docs generate`). Without
                // it we can't invert, so warn rather than silently update-all.
                if config.merge_update_columns.is_none() && config.merge_exclude_columns.is_some() {
                    warnings.push(ImportWarning {
                        model: model_name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "merge_exclude_columns can't be inverted from the manifest alone (it has no physical column list) — the emitted merge updates all columns".to_string(),
                        suggestion: Some(
                            "set the columns to update explicitly via the emitted [strategy] `update_columns`".to_string(),
                        ),
                    });
                }
                StrategyConfig::Merge {
                    unique_key: keys,
                    update_columns: config.merge_update_columns.clone(),
                }
            }
            _ => {
                warnings.push(ImportWarning {
                    model: model_name.to_string(),
                    category: WarningCategory::UnsupportedMaterialization,
                    message: format!(
                        "incremental_strategy='merge' requires unique_key — falling back to full_refresh. {NO_APPEND_EQUIVALENT}"
                    ),
                    suggestion: Some(
                        "add unique_key to the model config so it maps to merge".to_string(),
                    ),
                });
                push_append_fallback(structured, model_name, "incremental (merge, no unique_key)");
                StrategyConfig::FullRefresh
            }
        },
        "append" => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "incremental_strategy='append' mapped to full_refresh. {NO_APPEND_EQUIVALENT}"
                ),
                suggestion: Some(APPEND_SUGGESTION.to_string()),
            });
            push_append_fallback(structured, model_name, "incremental (append)");
            StrategyConfig::FullRefresh
        }
        "delete+insert" | "delete_insert" => {
            let partition_by = config.partition_by.clone().or_else(|| unique_keys.clone());
            match partition_by {
                Some(keys) => StrategyConfig::DeleteInsert { partition_by: keys },
                None => {
                    warnings.push(ImportWarning {
                        model: model_name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "incremental_strategy='delete+insert' has no partition_by or unique_key — emitted placeholder partition column".to_string(),
                        suggestion: Some(
                            "set `partition_by = ['<column>']` in the dbt config or override the emitted Rocky sidecar's [strategy] block".to_string(),
                        ),
                    });
                    StrategyConfig::DeleteInsert {
                        partition_by: vec!["partition_key".to_string()],
                    }
                }
            }
        }
        "insert_overwrite" => {
            // insert_overwrite is partition-overwrite semantics — map to
            // DeleteInsert by default. Time-partition variants need
            // time_interval which the user can opt into explicitly.
            let partition_by = config.partition_by.clone();
            let final_partition_by = match partition_by {
                Some(keys) => {
                    warnings.push(ImportWarning {
                        model: model_name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "incremental_strategy='insert_overwrite' mapped to delete_insert — review partition semantics".to_string(),
                        suggestion: Some(
                            "if the model is time-partitioned, set `type = \"time_interval\"` instead and define `time_column` / `granularity`".to_string(),
                        ),
                    });
                    keys
                }
                None => {
                    warnings.push(ImportWarning {
                        model: model_name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "incremental_strategy='insert_overwrite' has no partition_by — emitted placeholder partition column".to_string(),
                        suggestion: Some(
                            "set `partition_by = ['<column>']` in the dbt config or override the emitted Rocky sidecar's [strategy] block".to_string(),
                        ),
                    });
                    vec!["partition_key".to_string()]
                }
            };
            StrategyConfig::DeleteInsert {
                partition_by: final_partition_by,
            }
        }
        "microbatch" => {
            // Re-dispatch through the microbatch path for the same event_time
            // validation + granularity translation. This is the REAL dbt
            // microbatch form (`incremental_strategy='microbatch'`), so the
            // MicrobatchMapped structured warning must be threaded out to the
            // caller, not dropped into a local vec.
            map_microbatch_strategy(config, model_name, microbatch_mode, warnings, structured)
        }
        other => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "incremental_strategy='{other}' not recognized — falling back to full_refresh"
                ),
                suggestion: Some(
                    "use one of: append, merge, delete+insert, insert_overwrite, microbatch"
                        .to_string(),
                ),
            });
            push_append_fallback(structured, model_name, &format!("incremental ({other})"));
            StrategyConfig::FullRefresh
        }
    }
}

/// Record a model whose dbt `incremental` config fell back to `full_refresh`
/// as a structured `UnsupportedMaterialization`, the same shape the
/// unrecognised-materialization fallback uses, so it lands in
/// MIGRATION-NOTES.md's "Items to translate manually" list and not only among
/// the flat warnings.
fn push_append_fallback(
    structured: &mut Vec<ImportDbtStructuredWarning>,
    model_name: &str,
    dbt_materialization: &str,
) {
    structured.push(ImportDbtStructuredWarning::UnsupportedMaterialization {
        model: model_name.to_string(),
        dbt_materialization: dbt_materialization.to_string(),
        action: "fell back to full_refresh".to_string(),
    });
}

/// Why an append-style dbt model cannot keep its semantics in Rocky (#1990).
///
/// Rocky refuses `type = "incremental"` on transformation models (E037): with
/// no watermark to apply, it would re-insert every row on each run. The
/// importer therefore never emits it, and maps append semantics to
/// `full_refresh`, which rebuilds from the model SQL and cannot duplicate.
const NO_APPEND_EQUIVALENT: &str = "Rocky has no append strategy for transformation models: \
     an unfiltered append re-inserts every row on each run, so `incremental` is refused (E037) \
     and the model is imported as `full_refresh`, which replaces the table with the model SQL's \
     result on every run";

/// Why an incremental dbt model whose SQL uses `is_incremental()` is not
/// imported at all when it would fall back to `full_refresh` (#1990).
const INCREMENTAL_FALLBACK_REFUSED: &str = "is an incremental dbt model with no Rocky append \
     equivalent, and its SQL uses `is_incremental()`. dbt compiles that branch as true against an \
     existing table, so the compiled SQL can keep an incremental filter; imported as \
     `full_refresh`, it would replace the table with only the recent rows on every run. Rewrite \
     it by hand: remove the `is_incremental()` filter, then use merge with a unique_key or a \
     time_interval model with @start_date/@end_date";

const INCREMENTAL_COMPILE_EVIDENCE_REFUSED: &str = "is a dbt incremental model without successful \
     per-model evidence in a matching full-refresh `run_results.json` and `compiled_code` in \
     `manifest.json`. Its SQL may keep a delta filter and omit older rows on Rocky's first run. \
     Run `dbt compile --full-refresh` without `--select` (or include this model), then import \
     the resulting manifest.json and run_results.json together. dbt 2 writes no per-model \
     results for `compile`; Rocky then accepts the model's `compiled_code` from the same \
     invocation. If the model is still refused, `dbt run --full-refresh` writes per-model \
     results on dbt 1 and dbt 2 (it rebuilds the tables)";

const INCREMENTAL_FULL_REFRESH_DISABLED: &str = "is a dbt incremental model with effective \
     `full_refresh=false` config. That config overrides `dbt compile --full-refresh`, so \
     compiled SQL may still keep a delta filter. Remove the model's `full_refresh=false` \
     config, then run `dbt compile --full-refresh` without `--select` (or include this model) \
     and import the matching artifact pair";

/// An explicit `incremental_strategy` wins over `unique_key` in
/// `map_incremental_strategy`, so adding a key alone does not change an
/// explicit `'append'`: the strategy must be `'merge'` or unset as well.
const APPEND_SUGGESTION: &str = "add a unique_key and set incremental_strategy to 'merge' (or \
     leave it unset) so the model maps to merge, or hand-author a time_interval model with \
     @start_date/@end_date";

/// Map `materialized='microbatch'` (or `incremental_strategy='microbatch'`)
/// to a Rocky strategy. Emits a
/// [`ImportDbtStructuredWarning::MicrobatchMissingEventTime`] + falls back
/// to `FullRefresh` if `event_time` is absent.
///
/// When `microbatch_mode` is [`MicrobatchMode::TimeInterval`] and the
/// `event_time` field is a valid SQL identifier, this produces a
/// [`StrategyConfig::TimeInterval`] with bounded per-partition windows
/// (granularity + lookback derived from the dbt `batch_size`/`lookback`
/// config). The caller is responsible for rewriting the model body to
/// reference `@start_date`/`@end_date` (see [`rewrite_body_for_time_interval`]).
/// When the `event_time` field can't be rewritten safely, it falls back to
/// the `merge` mapping and warns that bounded-window semantics were not
/// preserved.
///
/// The default [`MicrobatchMode::Merge`] keeps the historical behavior: dbt
/// microbatch idempotently replaces each batch partition, so it maps to an
/// idempotent Rocky merge (key-upsert) — or append-only when no `unique_key`.
fn map_microbatch_strategy(
    config: &DbtNodeConfig,
    model_name: &str,
    microbatch_mode: MicrobatchMode,
    warnings: &mut Vec<ImportWarning>,
    structured: &mut Vec<ImportDbtStructuredWarning>,
) -> StrategyConfig {
    let Some(event_time) = config.event_time.clone() else {
        warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: "microbatch model is missing required `event_time` config — falling back to full_refresh".to_string(),
            suggestion: Some(
                "add `event_time = '<timestamp_column>'` to the model's dbt config block".to_string(),
            ),
        });
        structured.push(ImportDbtStructuredWarning::MicrobatchMissingEventTime {
            model: model_name.to_string(),
        });
        return StrategyConfig::FullRefresh;
    };

    // `--microbatch-as=time_interval`: try the faithful bounded-window
    // mapping. The body rewrite injects `@start_date`/`@end_date` bounds on
    // `event_time`, so the column must be a safe SQL identifier; if it isn't,
    // fall through to the merge mapping and warn that the bounded-window
    // semantics were not preserved.
    if microbatch_mode == MicrobatchMode::TimeInterval {
        if rocky_sql::validation::validate_identifier(&event_time).is_ok() {
            let granularity = microbatch_granularity(config.batch_size.as_deref());
            structured.push(ImportDbtStructuredWarning::MicrobatchMapped {
                model: model_name.to_string(),
                mapped_to: "time_interval".to_string(),
            });
            return StrategyConfig::TimeInterval {
                time_column: event_time,
                granularity,
                lookback: config.lookback.unwrap_or(0),
                // dbt has no equivalent of Rocky's batch_size (partition
                // count per SQL statement); default to atomic per-partition.
                batch_size: std::num::NonZeroU32::new(1).expect("1 is non-zero"),
                first_partition: None,
            };
        }
        warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: format!(
                "microbatch event_time '{event_time}' is not a safe SQL identifier — \
                 could not rewrite the body to a bounded time_interval window; mapped to merge instead, \
                 so dbt's bounded-window semantics were not preserved"
            ),
            suggestion: Some(
                "set `event_time` to a plain column name in the dbt config, or hand-author a Rocky time_interval model with @start_date/@end_date".to_string(),
            ),
        });
        // Fall through to the merge mapping below.
    }

    // The default import mode maps keyed dbt microbatch models to merge.
    // The time_interval import mode above wraps the body with bounded windows.
    // Without a key in the default mode, use full_refresh rather than emit an
    // unbounded append.
    let unique_keys: Option<Vec<String>> = config.unique_key.as_ref().map(|uk| match uk {
        UniqueKeyValue::Single(s) => vec![s.clone()],
        UniqueKeyValue::Multiple(v) => v.clone(),
    });

    match unique_keys {
        Some(keys) if !keys.is_empty() => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: "dbt microbatch mapped to an idempotent Rocky merge(unique_key); partition-replace becomes key-upsert, so rows removed from the source window are not deleted".to_string(),
                suggestion: Some(
                    "review the emitted [strategy] block; for true partition-replace use a time-interval model with @start_date/@end_date".to_string(),
                ),
            });
            structured.push(ImportDbtStructuredWarning::MicrobatchMapped {
                model: model_name.to_string(),
                mapped_to: "merge".to_string(),
            });
            StrategyConfig::Merge {
                unique_key: keys,
                update_columns: config.merge_update_columns.clone(),
            }
        }
        _ => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "dbt microbatch without a unique_key mapped to full_refresh. {NO_APPEND_EQUIVALENT}"
                ),
                suggestion: Some(
                    "add a unique_key (dbt microbatch normally has one) so it maps to an idempotent merge, or convert to a time-interval strategy with @start_date/@end_date".to_string(),
                ),
            });
            structured.push(ImportDbtStructuredWarning::MicrobatchMapped {
                model: model_name.to_string(),
                mapped_to: "full_refresh".to_string(),
            });
            // No keyed merge is possible in the default mode. A full rebuild
            // is safe; callers can select time_interval mode for windows.
            StrategyConfig::FullRefresh
        }
    }
}

/// Map dbt's microbatch `batch_size` (`hour` / `day` / `month` / `year`) to a
/// Rocky [`TimeGrain`]. dbt's `batch_size` is a *granularity*, not a count —
/// it names the size of each partition window. Unrecognized or absent values
/// default to `Day`, matching dbt's most common configuration.
fn microbatch_granularity(batch_size: Option<&str>) -> rocky_ir::TimeGrain {
    match batch_size.map(str::to_ascii_lowercase).as_deref() {
        Some("hour") => rocky_ir::TimeGrain::Hour,
        Some("month") => rocky_ir::TimeGrain::Month,
        Some("year") => rocky_ir::TimeGrain::Year,
        // "day" and anything unrecognized.
        _ => rocky_ir::TimeGrain::Day,
    }
}

/// Rewrite a model body so a `time_interval` strategy can bound each
/// partition. dbt's `compiled_code` for a microbatch model carries no
/// event-time filter (dbt injects the window at run time), so we wrap the
/// original SELECT in a bounded subquery keyed on `event_time`:
///
/// ```sql
/// SELECT * FROM (
///   <original body>
/// ) AS _rocky_microbatch
/// WHERE event_time >= @start_date AND event_time < @end_date
/// ```
///
/// Wrapping the whole query is safe against CTEs, `GROUP BY`, and set
/// operations, and keeps `event_time` in the output so the compiler's
/// `time_column` validation passes. The half-open `>= @start_date AND
/// < @end_date` bound matches the runtime's partition filter in
/// `sql_gen::generate_transformation_sql` (insert_overwrite_partition).
///
/// The caller has already validated `event_time` as a SQL identifier before
/// choosing the `time_interval` strategy.
fn rewrite_body_for_time_interval(sql: &str, event_time: &str) -> String {
    let body = sql.trim().trim_end_matches(';');
    format!(
        "SELECT * FROM (\n{body}\n) AS _rocky_microbatch\nWHERE {event_time} >= @start_date AND {event_time} < @end_date"
    )
}

/// Collect structured warnings for dbt config Rocky can't auto-translate
/// (databricks_tags, pre/post hooks, on_schema_change). These are
/// dropped-on-purpose with an explicit pointer at the Rocky equivalent.
///
/// `on_schema_change_mapped` is true when the model became a Rocky
/// `incremental` model, whose sidecar carries `on_schema_change` itself
/// (see [`map_is_incremental_conversion`]); it is not dropped then.
fn collect_dropped_config_warnings(
    config: &DbtNodeConfig,
    model_name: &str,
    on_schema_change_mapped: bool,
    result: &mut ImportResult,
) {
    if !config.databricks_tags.is_empty() {
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedDatabricksTags {
                model: model_name.to_string(),
                tags: config.databricks_tags.clone(),
            });
        result.warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: format!(
                "{} databricks tag(s) dropped — Rocky's [classification] block + rocky-databricks governance surface covers the same use case",
                config.databricks_tags.len()
            ),
            suggestion: Some(
                "copy the dropped tags into the model sidecar's [classification] block, or configure them via rocky-databricks governance".to_string(),
            ),
        });
    }

    for sql in &config.pre_hook {
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedHook {
                model: model_name.to_string(),
                hook_kind: HookKind::Pre,
                sql: sql.clone(),
            });
        result.warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: "pre_hook dropped — Rocky supports lifecycle hooks via the [[hook]] block in rocky.toml".to_string(),
            suggestion: Some(
                "translate the pre-hook SQL into an `[[hook]] event = \"on_model_start\"` entry in the emitted rocky.toml".to_string(),
            ),
        });
    }

    for sql in &config.post_hook {
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedHook {
                model: model_name.to_string(),
                hook_kind: HookKind::Post,
                sql: sql.clone(),
            });
        result.warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: "post_hook dropped — Rocky supports lifecycle hooks via the [[hook]] block in rocky.toml".to_string(),
            suggestion: Some(
                "translate the post-hook SQL into an `[[hook]] event = \"on_model_end\"` entry in the emitted rocky.toml".to_string(),
            ),
        });
    }

    if let Some(value) = config
        .on_schema_change
        .as_deref()
        .filter(|_| !on_schema_change_mapped)
    {
        let rocky_equivalent = on_schema_change_to_rocky(value);
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedOnSchemaChange {
                model: model_name.to_string(),
                dbt_value: value.to_string(),
                rocky_equivalent: rocky_equivalent.clone(),
            });
        result.warnings.push(ImportWarning {
            model: model_name.to_string(),
            category: WarningCategory::UnsupportedMaterialization,
            message: format!(
                "on_schema_change='{value}' dropped — Rocky exposes the equivalent via per-pipeline [drift] policy ({rocky_equivalent})"
            ),
            suggestion: Some(
                "set the matching [drift] policy in the pipeline section of the emitted rocky.toml".to_string(),
            ),
        });
    }
}

/// Generate the `<model>.contract.toml` for an enforced dbt contract.
///
/// Returns the file body. When part of the contract has no Rocky check (a
/// column type Rocky has no name for, or a constraint other than `not_null`
/// and `primary_key`), it also bumps `contracts_dropped` and warns, so that
/// loss is never silent. An un-enforced `contract` block (dbt's default)
/// carries no semantics and yields nothing.
fn collect_contract(
    node: &DbtManifestNode,
    rocky_name: &str,
    adapter: Option<&str>,
    result: &mut ImportResult,
) -> Option<String> {
    let contract = super::dbt_contract::contract_from_node(node, adapter)?;
    let contract_path = format!("models/{rocky_name}.contract.toml");
    if contract.untyped_columns > 0
        || contract.unmapped_constraints > 0
        || contract.not_null_constraints > 0
    {
        result.contracts_dropped += 1;
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::DroppedContract {
                model: node.name.clone(),
                typed_columns: contract.untyped_columns,
                constraints: contract.unmapped_constraints,
                not_null_constraints: contract.not_null_constraints,
                contract_path: contract_path.clone(),
            });
        result.warnings.push(ImportWarning {
            model: node.name.clone(),
            category: WarningCategory::DroppedContract,
            message: format!(
                "{contract_path} was generated from the enforced dbt contract, but {} column type(s) \
                 are not checked (no Rocky type for them on this warehouse), {} `not_null` or \
                 `primary_key` constraint(s) are not checked (Rocky cannot prove NOT NULL from the \
                 sources), and {} other constraint(s) (`unique`, `check`, ...) are not checked",
                contract.untyped_columns, contract.not_null_constraints, contract.unmapped_constraints
            ),
            suggestion: Some(format!(
                "review {contract_path}; check the unchecked rules with a data test (a `not_null` \
                 test for a not_null constraint) or a `[[checks]]` block"
            )),
        });
    }
    Some(contract.toml)
}

/// Translate dbt's `on_schema_change` values to a human-readable Rocky
/// drift-policy hint. Used for the structured warning's
/// `rocky_equivalent` field.
fn on_schema_change_to_rocky(value: &str) -> String {
    match value.to_ascii_lowercase().as_str() {
        "ignore" => "drift policy 'ignore' (skip drift detection)".to_string(),
        "fail" => "drift policy 'strict' (fail on drift)".to_string(),
        "append_new_columns" => {
            "drift policy 'evolve' (allow safe widening + new columns)".to_string()
        }
        "sync_all_columns" => "drift policy 'evolve' with column-removal allowed".to_string(),
        other => format!("(no direct equivalent for '{other}' — set [drift] manually)"),
    }
}

/// Detect Jinja macro calls that survived `dbt compile` and surface each
/// distinct macro as an `UnresolvableMacro` structured warning. dbt
/// resolves in-tree macros at compile time, so anything still present
/// points at an out-of-tree macro (e.g. a custom org-wide library).
fn collect_unresolvable_macros(sql: &str, model_name: &str, result: &mut ImportResult) {
    let usages = super::dbt_macros::detect_macros(sql);
    if usages.is_empty() {
        return;
    }
    // Track first occurrence per (package, name) so we emit one warning
    // per macro, not per call site. dbt projects often call the same
    // macro 100+ times within a single model body.
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for u in &usages {
        let full_name = match &u.package {
            Some(pkg) => format!("{pkg}.{}", u.name),
            None => u.name.clone(),
        };
        if !seen.insert(full_name.clone()) {
            continue;
        }
        let line = sql[..u.span.0].matches('\n').count() + 1;
        result
            .structured_warnings
            .push(ImportDbtStructuredWarning::UnresolvableMacro {
                model: model_name.to_string(),
                macro_name: full_name,
                first_call_site_line: line,
            });
    }
    result.macros_detected += seen.len();
    result.macros_unsupported += seen.len();
}

// ---------------------------------------------------------------------------
// Import from raw SQL files (regex path)
// ---------------------------------------------------------------------------

/// Join an untrusted relative model path under `base`, rejecting any path that
/// would escape the project root.
///
/// `dbt_project.yml`'s `model-paths` is third-party input. A path that is
/// absolute or contains a `..` (`ParentDir`) component could steer the import
/// into reading files outside the project. This rejects those syntactically
/// (no filesystem access required), and — when the joined path exists —
/// canonicalizes it and asserts it stays within the canonicalized `base`,
/// catching symlink-based escapes too. A not-yet-existing joined path that
/// passed the syntactic check is allowed through (the caller already tolerates
/// missing model dirs).
pub(super) fn safe_join_under(base: &Path, rel: &Path) -> Result<std::path::PathBuf, String> {
    use std::path::Component;

    if rel.is_absolute() {
        return Err(format!(
            "model path '{}' is absolute — model paths must be relative to the dbt project",
            rel.display()
        ));
    }
    if rel.components().any(|c| matches!(c, Component::ParentDir)) {
        return Err(format!(
            "model path '{}' contains a '..' component — model paths may not escape the dbt project",
            rel.display()
        ));
    }

    let joined = base.join(rel);

    // Defense in depth: if the path exists, canonicalize and confirm
    // containment (catches symlink escapes the syntactic check can't see).
    if joined.exists() {
        let canon_base = base.canonicalize().map_err(|e| {
            format!(
                "failed to canonicalize project root {}: {e}",
                base.display()
            )
        })?;
        let canon_joined = joined.canonicalize().map_err(|e| {
            format!(
                "failed to canonicalize model path {}: {e}",
                joined.display()
            )
        })?;
        if !canon_joined.starts_with(&canon_base) {
            return Err(format!(
                "model path '{}' resolves to {}, outside the dbt project at {}",
                rel.display(),
                canon_joined.display(),
                canon_base.display()
            ));
        }
    }

    Ok(joined)
}

/// Import a dbt project directory.
///
/// Scans `dbt_project/models/` for `.sql` files, extracts Jinja refs/sources,
/// and produces Rocky model files. Optionally uses `dbt_project.yml` for
/// project-level config and source definitions.
pub fn import_dbt_project(
    dbt_dir: &Path,
    default_target: &TargetConfig,
) -> Result<ImportResult, String> {
    // Try to load dbt_project.yml
    let project_config = {
        let yml_path = dbt_dir.join("dbt_project.yml");
        if yml_path.exists() {
            match dbt_project::from_yaml(&yml_path) {
                Ok(cfg) => Some(cfg),
                Err(e) => {
                    tracing::warn!("failed to parse dbt_project.yml: {e}");
                    None
                }
            }
        } else {
            None
        }
    };

    // Determine model paths. Model paths come from an untrusted
    // `dbt_project.yml`; reject any that escape the project root (absolute or
    // containing a `..` component) so an import can't be steered into reading
    // files outside `dbt_dir`.
    let model_dirs: Vec<std::path::PathBuf> = match &project_config {
        Some(cfg) => {
            let mut dirs = Vec::with_capacity(cfg.model_paths.len());
            for p in &cfg.model_paths {
                dirs.push(safe_join_under(dbt_dir, p)?);
            }
            dirs
        }
        None => vec![dbt_dir.join("models")],
    };

    // Scan for source definitions
    let mut all_sources = Vec::new();
    for dir in &model_dirs {
        if dir.exists() {
            match dbt_sources::scan_sources_in_dir(dir) {
                Ok(sources) => all_sources.extend(sources),
                Err(e) => tracing::warn!("failed to scan sources in {}: {e}", dir.display()),
            }
        }
    }
    let source_map = dbt_sources::sources_to_rocky_config(&all_sources, &default_target.catalog);
    let sources_found: usize = all_sources.iter().map(|s| s.tables.len()).sum();
    let sources_mapped = source_map.len();

    let mut result = ImportResult {
        imported: Vec::new(),
        warnings: Vec::new(),
        structured_warnings: Vec::new(),
        failed: Vec::new(),
        sources_found,
        sources_mapped,
        import_method: ImportMethod::Regex,
        project_name: project_config.as_ref().map(|c| c.name.clone()),
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
        contracts_dropped: 0,
        consumers: Vec::new(),
    };

    // Verify at least one model directory exists
    let any_exists = model_dirs.iter().any(|d| d.exists());
    if !any_exists {
        return Err(format!(
            "no models directory found (checked: {})",
            model_dirs
                .iter()
                .map(|d| d.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    let mut model_yamls = HashMap::new();
    let mut versioned_names = HashSet::new();
    for dir in &model_dirs {
        let parsed = super::dbt_tests::parse_model_yamls(dir)?;
        versioned_names.extend(
            parsed
                .values()
                .flat_map(|model| model.versioned_names.iter().cloned()),
        );
        model_yamls.extend(parsed);
    }
    // dbt governance from YAML: access, group (+ owners), versions. A versioned
    // model whose versions are plain `<name>_v<N>.sql` files with no
    // per-version overrides imports as-is; any other versioned model still
    // needs the manifest and stays refused.
    let mut yaml_governance = super::dbt_governance::YamlGovernance::default();
    for dir in &model_dirs {
        super::dbt_governance::parse_governance_yamls(dir, &mut yaml_governance);
    }
    let mut simple_versions: HashMap<String, (String, u32)> = HashMap::new();
    for (name, gov) in &yaml_governance.models {
        if let Some(stems) = gov.simple_versions(name) {
            for (stem, v) in stems {
                versioned_names.remove(&stem);
                simple_versions.insert(stem, (name.clone(), v));
            }
        }
    }
    let settings = RawModelSettings {
        project: &project_config,
        model_yamls: &model_yamls,
        versioned_names: &versioned_names,
    };

    for dir in &model_dirs {
        if dir.exists() {
            visit_dbt_models(
                dir,
                dir,
                default_target,
                &settings,
                &source_map,
                &mut result,
                0,
            )?;
        }
    }

    // dbt snapshots (legacy `{% snapshot %}` blocks and YAML snapshots) live
    // under `snapshot-paths`, not the model paths; convert them to
    // `type = "snapshot"` models instead of dropping them.
    super::dbt_snapshots::import_raw_snapshots(dbt_dir, default_target, &source_map, &mut result)?;
    apply_yaml_governance(&mut result, &yaml_governance, &simple_versions);

    // Phase 2: Scan model YAML files for test definitions and convert them
    // to canonical Rocky `[[tests]]` (`TestDecl`) entries on each imported
    // model. Tests outside the four canonical built-ins emit structured
    // warnings; we deliberately do NOT stub them as TODO comments in the
    // generated rocky.toml.
    for dir in &model_dirs {
        if dir.exists() {
            apply_dbt_tests(dir, default_target, &mut result);
        }
    }

    // Phase 2: Detect macros in imported model SQL
    for model in &result.imported {
        let macros = super::dbt_macros::detect_macros(&model.sql);
        result.macros_detected += macros.len();
        // Without manifest or compile result, all are unsupported
        result.macros_unsupported += macros.len();
    }

    Ok(result)
}

/// An empty raw-path result, for unit tests of the per-construct importers.
#[cfg(test)]
pub(super) fn empty_import_result() -> ImportResult {
    ImportResult {
        imported: Vec::new(),
        warnings: Vec::new(),
        structured_warnings: Vec::new(),
        failed: Vec::new(),
        sources_found: 0,
        sources_mapped: 0,
        import_method: ImportMethod::Regex,
        project_name: None,
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
        contracts_dropped: 0,
        consumers: Vec::new(),
    }
}

/// Attach YAML-declared access, group and version metadata to raw-imported
/// models. `simple_versions` maps a version file stem to `(model, version)`.
fn apply_yaml_governance(
    result: &mut ImportResult,
    yaml: &super::dbt_governance::YamlGovernance,
    simple_versions: &HashMap<String, (String, u32)>,
) {
    for model in &mut result.imported {
        let (base, version) = match simple_versions.get(&model.name) {
            Some((base, v)) => (base.as_str(), Some(v.to_string())),
            None => (model.name.as_str(), None),
        };
        let Some(gov) = yaml.models.get(base) else {
            continue;
        };
        let version_entry = version.as_deref().and_then(|v| {
            gov.versions.iter().find(|e| {
                super::dbt_governance::parse_version(&e.v)
                    .map(|n| n.to_string())
                    .as_deref()
                    == Some(v)
            })
        });
        let latest = gov.latest_version.clone().or_else(|| {
            gov.versions
                .iter()
                .filter_map(|e| super::dbt_governance::parse_version(&e.v))
                .max()
                .map(|n| n.to_string())
        });
        let deprecation = version_entry
            .and_then(|e| e.deprecation_date.clone())
            .or_else(|| version.as_ref().and(gov.deprecation_date.clone()));
        let (governance, warnings) = super::dbt_governance::governance_from_dbt(
            &super::dbt_governance::DbtGovernanceInput {
                name: base,
                rocky_name: &model.name,
                access: gov.access.as_deref(),
                group: gov.group.as_deref(),
                version: version.as_deref(),
                latest_version: latest.as_deref(),
                deprecation_date: deprecation.as_deref(),
            },
            &yaml.groups,
        );
        let tags = std::mem::take(&mut model.config.governance.tags);
        model.config.governance = governance;
        model.config.governance.tags = tags;
        result.warnings.extend(warnings);
    }
}

struct RawModelSettings<'a> {
    project: &'a Option<DbtProjectConfig>,
    model_yamls: &'a HashMap<String, super::dbt_tests::DbtModelYaml>,
    versioned_names: &'a HashSet<String>,
}

fn visit_dbt_models(
    dir: &Path,
    models_root: &Path,
    default_target: &TargetConfig,
    settings: &RawModelSettings<'_>,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
    depth: usize,
) -> Result<(), String> {
    if depth > super::MAX_IMPORT_RECURSION_DEPTH {
        return Err(format!(
            "model directory tree exceeds the maximum import depth of {} at {} — \
             refusing to recurse further (possible symlink cycle)",
            super::MAX_IMPORT_RECURSION_DEPTH,
            dir.display()
        ));
    }

    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if super::is_traversable_subdir(&entry) {
            visit_dbt_models(
                &path,
                models_root,
                default_target,
                settings,
                source_map,
                result,
                depth + 1,
            )?;
        } else if path.extension().is_some_and(|ext| ext == "sql") {
            let name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            let rel_path = path.strip_prefix(models_root).unwrap_or(path.as_path());

            match import_single_model(&path, &name, rel_path, default_target, settings, source_map)
            {
                Ok((model, warnings)) => {
                    result.warnings.extend(warnings);
                    result.imported.push(model);
                }
                Err(e) => {
                    result.failed.push(ImportFailure { name, reason: e });
                }
            }
        }
    }

    Ok(())
}

fn import_single_model(
    path: &Path,
    name: &str,
    rel_path: &Path,
    default_target: &TargetConfig,
    settings: &RawModelSettings<'_>,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
) -> Result<(ImportedModel, Vec<ImportWarning>), String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("failed to read: {e}"))?;

    // The raw converter keeps the body of statement tags. Even a condition
    // unrelated to is_incremental() can leave a bounded query as full SQL.
    // The one exception is an incremental model's `is_incremental()` block,
    // handled once the materialization is known.
    let has_statement_tags = content.contains("{%");
    if has_statement_tags && !contains_unresolved_is_incremental(&content) {
        return Err(RAW_JINJA_CONTROL_REFUSED.to_string());
    }
    if settings.versioned_names.contains(name) {
        return Err(RAW_VERSIONED_REFUSED.to_string());
    }
    let config_calls = dbt_config_calls(&content);
    let config_starts = Regex::new(r"\{\{\s*-?\s*config\b").unwrap();
    if config_starts.find_iter(&content).count() != config_calls.len()
        || config_calls
            .iter()
            .any(|call| !raw_config_is_resolvable(call))
    {
        return Err(RAW_CONFIG_UNRESOLVED.to_string());
    }

    let resolved_project_config = settings
        .project
        .as_ref()
        .map(|project| dbt_project::resolve_model_config(project, rel_path));
    let yaml_materialization = settings
        .model_yamls
        .get(name)
        .and_then(|model| model.materialized.as_deref());
    let inline_materialization = inline_dbt_materialization(&content);
    let effective_materialization = inline_materialization
        .clone()
        .or_else(|| yaml_materialization.map(str::to_string))
        .or_else(|| {
            resolved_project_config
                .as_ref()
                .map(|config| config.materialized.clone())
        });
    if effective_materialization
        .as_deref()
        .is_some_and(|value| !value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(RAW_CONFIG_UNRESOLVED.to_string());
    }
    let mut warnings = Vec::new();

    // An incremental model converts only through its `is_incremental()`
    // block: the standard watermark filter becomes `@incremental_filter`
    // (`TRUE` on the first run, so the first run loads every row); any other
    // use is commented out as a TODO and the watermark is left unset, so
    // `rocky compile` refuses the model (E037) until a human adds one. An
    // incremental model with no `is_incremental()` use stays refused.
    let mut incremental_strategy = None;
    let mut todo_blocks = Vec::new();
    let content_processed = if effective_materialization.as_deref() == Some("incremental") {
        if !contains_unresolved_is_incremental(&content) {
            return Err(RAW_INCREMENTAL_EVIDENCE_REFUSED.to_string());
        }
        let node_config = raw_dbt_node_config(&content, "incremental");
        if let Some(recognized) = recognize_is_incremental_filter(&content) {
            let Some(converted) = map_is_incremental_conversion(
                &node_config,
                name,
                Some(&recognized.watermark),
                recognized.filter_column.as_deref(),
            ) else {
                return Err(RAW_JINJA_CONTROL_REFUSED.to_string());
            };
            incremental_strategy = Some(converted.strategy);
            warnings.extend(converted.warnings);
            recognized.rewritten
        } else {
            let Some(converted) = map_is_incremental_conversion(&node_config, name, None, None)
            else {
                return Err(RAW_JINJA_CONTROL_REFUSED.to_string());
            };
            let (stripped, blocks) = comment_out_is_incremental_blocks(&content);
            if stripped.contains("{%") {
                return Err(RAW_JINJA_CONTROL_REFUSED.to_string());
            }
            if blocks.is_empty() || contains_unresolved_is_incremental(&stripped) {
                return Err(RAW_INCREMENTAL_ERROR.to_string());
            }
            warnings.push(ImportWarning {
                model: name.to_string(),
                category: WarningCategory::JinjaControlFlow,
                message: format!(
                    "{} dbt `is_incremental()` block(s) not translated; imported as a \
                     `-- {IS_INCREMENTAL_TODO}` comment with no watermark, so `rocky compile` \
                     refuses the model (E037) until one is set",
                    blocks.len()
                ),
                suggestion: Some(
                    "set `timestamp_column` in the sidecar [strategy] block and put \
                     `WHERE @incremental_filter` where the commented block was (plus \
                     `filter_column` when it compares a qualified or renamed input column)"
                        .to_string(),
                ),
            });
            incremental_strategy = Some(converted.strategy);
            warnings.extend(converted.warnings);
            todo_blocks = blocks;
            stripped
        }
    } else {
        if has_statement_tags {
            return Err(RAW_JINJA_CONTROL_REFUSED.to_string());
        }
        // Refuse is_incremental() before general Jinja handling. This must
        // catch compound conditions too: otherwise the generic fallback
        // removes the control tags and applies the guarded body
        // unconditionally.
        if contains_unresolved_is_incremental(&content) {
            return Err(RAW_INCREMENTAL_ERROR.to_string());
        }
        content
    };

    // `{{ var('x') }}` is now mapped to Rocky's native per-run variable marker
    // `@var(x)` (with `{{ var('x', 'd') }}` -> `@var(x, d)`) during
    // `convert_jinja_to_sql`, so it is no longer an unsupported macro. Emit an
    // informational note so the operator knows to supply the value at run time
    // via `rocky run --var x=value` (or rely on the inline default).
    if content_processed.contains("{{ var(") {
        warnings.push(ImportWarning {
            model: name.to_string(),
            category: WarningCategory::MappedConstruct,
            message: "contains {{ var() }} — mapped to Rocky's `@var()` per-run variable marker"
                .to_string(),
            suggestion: Some(
                "supply values at run time with `rocky run --var name=value`, or rely on an \
                 inline default `@var(name, default)`"
                    .to_string(),
            ),
        });
    }

    // Extract config block
    let (mut strategy, mut config_warnings) =
        extract_dbt_config(&content_processed, inline_materialization.as_deref());
    if let Some(converted) = incremental_strategy {
        // The generic mapping's warnings describe a strategy not applied.
        strategy = converted;
        config_warnings.clear();
    }
    warnings.extend(config_warnings.into_iter().map(|msg| ImportWarning {
        model: name.to_string(),
        category: WarningCategory::UnsupportedMaterialization,
        message: msg,
        suggestion: Some(
            "set `type = \"full_refresh\"` (or `\"view\"` for staging models) in the emitted sidecar".to_string(),
        ),
    }));

    // Apply project config inheritance
    let (resolved_schema, resolved_tags) = if let Some(resolved) = resolved_project_config {
        // Apply inherited materialization only when inline config does not set it.
        if inline_materialization.is_none() && matches!(strategy, StrategyConfig::FullRefresh) {
            match resolved.materialized.as_str() {
                "view" => {
                    strategy = StrategyConfig::View;
                }
                "materialized_view" => {
                    strategy = StrategyConfig::MaterializedView;
                }
                "ephemeral" => {
                    strategy = StrategyConfig::Ephemeral;
                }
                _ => {}
            }
        }
        (resolved.schema, resolved.tags)
    } else {
        (None, Vec::new())
    };

    // Resolve the model's output coordinates so {{ this }} substitutes the real
    // FQN and the emitted target uses the same alias-aware table name.
    let resolved_schema_str = resolved_schema.as_deref().unwrap_or(&default_target.schema);
    let resolved_table = extract_dbt_alias(&content_processed).unwrap_or_else(|| name.to_string());
    let this_ref = format!(
        "{}.{}.{}",
        default_target.catalog, resolved_schema_str, resolved_table
    );

    // Convert Jinja refs to plain SQL. Untranslated `is_incremental()` blocks
    // sit behind sentinels until now so the converter leaves their quoted
    // Jinja alone.
    let mut sql = convert_jinja_to_sql(&content_processed, &this_ref);
    for (sentinel, comment) in &todo_blocks {
        sql = sql.replace(sentinel.as_str(), comment);
    }

    // Resolve source references
    let mut model_sources = Vec::new();
    let source_re =
        Regex::new(r#"\{\{\s*source\s*\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}"#)
            .unwrap();
    for cap in source_re.captures_iter(&content_processed) {
        let src_name = cap[1].to_string();
        let tbl_name = cap[2].to_string();
        let key = (src_name.clone(), tbl_name.clone());
        if let Some(mapping) = source_map.get(&key) {
            model_sources.push(mapping.source_config.clone());
        } else {
            warnings.push(ImportWarning {
                model: name.to_string(),
                category: WarningCategory::MissingSource,
                message: format!("source('{src_name}', '{tbl_name}') not found in sources.yml"),
                suggestion: Some("add a sources.yml definition for this source".to_string()),
            });
        }
    }

    let config = ModelConfig {
        name: name.to_string(),
        depends_on: vec![], // Auto-resolved by compiler
        strategy,
        target: TargetConfig {
            catalog: default_target.catalog.clone(),
            schema: resolved_schema_str.to_string(),
            table: resolved_table,
        },
        sources: model_sources,
        adapter: None,
        intent: None,
        freshness: None,
        tests: vec![],
        format: None,
        format_options: None,
        classification: Default::default(),
        tags: dbt_tags_to_map(&resolved_tags),
        governance: Default::default(),
        retention: None,
        budget: None,
        skip: None,
        name_declared: String::new(),
        target_table_declared: String::new(),
    };

    Ok((
        ImportedModel {
            name: name.to_string(),
            sql,
            config,
            unit_tests: Vec::new(),
            contract_toml: None,
        },
        warnings,
    ))
}

// ---------------------------------------------------------------------------
// is_incremental() detection
// ---------------------------------------------------------------------------

/// Whether a raw Jinja statement or expression references dbt's
/// `is_incremental` macro.
///
/// This includes compound conditions and aliases that capture the callable
/// without invoking it in the same tag. Raw import cannot evaluate any of them
/// faithfully.
fn contains_unresolved_is_incremental(sql: &str) -> bool {
    let mut search_start = 0;

    while search_start < sql.len() {
        let rest = &sql[search_start..];
        let statement_start = rest.find("{%");
        let expression_start = rest.find("{{");
        let (relative_start, closing_delimiter) = match (statement_start, expression_start) {
            (Some(statement), Some(expression)) if statement <= expression => (statement, "%}"),
            (Some(_), Some(expression)) => (expression, "}}"),
            (Some(statement), None) => (statement, "%}"),
            (None, Some(expression)) => (expression, "}}"),
            (None, None) => break,
        };

        let tag_start = search_start + relative_start;
        let body_start = tag_start + 2;
        let Some(relative_end) = find_jinja_tag_end(&sql[body_start..], closing_delimiter) else {
            // The template is malformed, but keep scanning in case a later
            // complete tag contains the unsafe reference.
            search_start = body_start;
            continue;
        };
        let body_end = body_start + relative_end;

        if contains_unquoted_jinja_identifier(&sql[body_start..body_end], "is_incremental") {
            return true;
        }

        search_start = body_end + closing_delimiter.len();
    }

    false
}

/// Locate a Jinja tag's closing delimiter without treating delimiter text
/// inside a quoted string as the end of the tag.
fn find_jinja_tag_end(body: &str, closing_delimiter: &str) -> Option<usize> {
    let mut chars = body.char_indices();
    let mut quote = None;

    while let Some((index, ch)) = chars.next() {
        if let Some(delimiter) = quote {
            if ch == '\\' {
                chars.next();
            } else if ch == delimiter {
                quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            quote = Some(ch);
        } else if body[index..].starts_with(closing_delimiter) {
            return Some(index);
        }
    }

    None
}

/// Find a standalone Jinja identifier while ignoring quoted string contents.
/// A string value such as `{% set label = "is_incremental" %}` cannot capture
/// the macro callable and should not make an otherwise safe model fail.
fn contains_unquoted_jinja_identifier(body: &str, identifier: &str) -> bool {
    let mut chars = body.char_indices();
    let mut quote = None;

    while let Some((index, ch)) = chars.next() {
        if let Some(delimiter) = quote {
            if ch == '\\' {
                chars.next();
            } else if ch == delimiter {
                quote = None;
            }
            continue;
        }

        if ch == '\'' || ch == '"' {
            quote = Some(ch);
            continue;
        }

        if body[index..].starts_with(identifier) {
            let before = body[..index].chars().next_back();
            let after = body[index + identifier.len()..].chars().next();
            let standalone = before.is_none_or(|c| !is_jinja_identifier_char(c))
                && after.is_none_or(|c| !is_jinja_identifier_char(c));
            if standalone {
                return true;
            }
        }
    }

    false
}

fn is_jinja_identifier_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Result of detecting `{% if is_incremental() %}` in SQL.
#[derive(Debug, Clone)]
pub struct IncrementalDetection {
    pub timestamp_column: String,
}

/// Detect `{% if is_incremental() %}` blocks and extract the timestamp column.
///
/// Returns the processed SQL (with the incremental block removed or the else
/// block preserved) and an optional detection result.
pub fn detect_is_incremental(sql: &str) -> (String, Option<IncrementalDetection>) {
    let incr_re = Regex::new(
        r"(?si)\{%-?\s*if\s+is_incremental\(\)\s*-?%\}(.*?)(?:\{%-?\s*else\s*-?%\}(.*?))?\{%-?\s*endif\s*-?%\}"
    ).unwrap();

    let mut detection: Option<IncrementalDetection> = None;
    let mut processed = sql.to_string();

    if let Some(caps) = incr_re.captures(sql) {
        let incr_block = caps.get(1).map(|m| m.as_str()).unwrap_or("");
        let else_block = caps.get(2).map(|m| m.as_str());

        // Try to extract timestamp column from the incremental WHERE clause
        let ts_col = extract_timestamp_from_where(incr_block);

        detection = Some(IncrementalDetection {
            timestamp_column: ts_col.unwrap_or_else(|| "updated_at".to_string()),
        });

        // Replace the block: keep else block if present, otherwise remove
        let replacement = match else_block {
            Some(eb) => eb.trim().to_string(),
            None => String::new(),
        };

        // `NoExpand` so a `$` in the operator's else-block SQL (dollar-quoted
        // literals, Snowflake positional `$1`, a literal `$100`) is copied
        // verbatim instead of being interpreted as a capture-group reference.
        // Matches the guard already applied to the ref-rewrite at the bottom of
        // `convert_jinja_to_sql`.
        processed = incr_re
            .replace(&processed, regex::NoExpand(replacement.as_str()))
            .to_string();
    }

    (processed, detection)
}

/// Extract the timestamp column from a WHERE clause inside an is_incremental() block.
///
/// Matches patterns like:
/// - `WHERE updated_at > (SELECT MAX(updated_at) FROM ...)`
/// - `WHERE _fivetran_synced > ...`
fn extract_timestamp_from_where(block: &str) -> Option<String> {
    // Pattern: WHERE <col> > (SELECT MAX(<col>) FROM ...)
    let max_re = Regex::new(
        r"(?i)WHERE\s+(\w+)\s*>\s*\(\s*SELECT\s+(?:COALESCE\s*\(\s*)?MAX\s*\(\s*(\w+)\s*\)",
    )
    .unwrap();

    if let Some(caps) = max_re.captures(block) {
        return Some(caps[1].to_string());
    }

    // Pattern: WHERE <col> > <something> or WHERE <col> >= <something>
    let simple_re = Regex::new(r"(?i)WHERE\s+(\w+)\s*>=?\s*").unwrap();
    if let Some(caps) = simple_re.captures(block) {
        return Some(caps[1].to_string());
    }

    None
}

// ---------------------------------------------------------------------------
// is_incremental() watermark filter -> @incremental_filter
// ---------------------------------------------------------------------------

/// First line of the comment that replaces an `is_incremental()` block the
/// importer could not translate.
const IS_INCREMENTAL_TODO: &str = "TODO: dbt is_incremental() block not translated:";

/// dbt's standard watermark filter:
///
/// ```text
/// {% if is_incremental() %} <WHERE|AND> <lhs> > (SELECT MAX(<wm>) FROM {{ this }}) {% endif %}
/// ```
///
/// Only a strict `>` matches: Rocky's filter is strict, so `>=` would
/// silently change which rows load. An `{% else %}` branch does not match.
const IS_INCREMENTAL_FILTER_PATTERN: &str = r"(?is)\{%-?\s*if\s+is_incremental\s*\(\s*\)\s*-?%\}\s*(?P<kw>where|and)\s+(?P<lhs>(?:[a-z_][a-z0-9_]*\.)?[a-z_][a-z0-9_]*)\s*>\s*\(\s*select\s+max\s*\(\s*(?P<wm>[a-z_][a-z0-9_]*)\s*\)\s*from\s*\{\{-?\s*this\s*-?\}\}\s*\)\s*\{%-?\s*endif\s*-?%\}";

/// A recognized dbt watermark filter and the model SQL rewritten to Rocky's
/// `@incremental_filter` placeholder.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RecognizedIncrementalFilter {
    /// The `MAX(<wm>)` column: the model's watermark output column.
    watermark: String,
    /// The compared input expression (`<ident>` or `<alias>.<ident>`) when
    /// it is not the watermark column itself. Becomes the sidecar's
    /// `filter_column`.
    filter_column: Option<String>,
    /// The input with the block replaced by `<WHERE|AND> @incremental_filter`.
    rewritten: String,
    /// Whether the input holds a `{%` tag besides the recognized block.
    other_statement_tags: bool,
}

/// Find exactly one standard `is_incremental()` watermark filter in raw dbt
/// SQL and rewrite it to the placeholder. Other `{%` tags are allowed here
/// (and reported); any other reference to `is_incremental` is not.
fn find_is_incremental_filter(content: &str) -> Option<RecognizedIncrementalFilter> {
    let filter_re = Regex::new(IS_INCREMENTAL_FILTER_PATTERN).expect("valid regex");
    let mut found = filter_re.captures_iter(content);
    let caps = found.next()?;
    if found.next().is_some() {
        return None;
    }
    let whole = caps.get(0)?;
    let keyword = &caps["kw"];
    let lhs = &caps["lhs"];
    let watermark = caps["wm"].to_string();

    let before = &content[..whole.start()];
    let after = &content[whole.end()..];
    let mut replacement = String::new();
    if before
        .chars()
        .next_back()
        .is_some_and(|c| !c.is_whitespace())
    {
        replacement.push(' ');
    }
    replacement.push_str(keyword);
    replacement.push(' ');
    replacement.push_str(rocky_core::incremental_filter::PLACEHOLDER);
    if after.chars().next().is_some_and(|c| !c.is_whitespace()) {
        replacement.push(' ');
    }
    let rewritten = format!("{before}{replacement}{after}");

    // A second, unrecognized block, a `{% set %}` alias, or an
    // `{{ is_incremental() }}` expression means the filter is not the whole
    // incremental logic.
    if contains_unresolved_is_incremental(&rewritten) {
        return None;
    }
    let filter_column = (!lhs.eq_ignore_ascii_case(&watermark)).then(|| lhs.to_string());
    Some(RecognizedIncrementalFilter {
        watermark,
        filter_column,
        other_statement_tags: rewritten.contains("{%"),
        rewritten,
    })
}

/// Recognize the standard `is_incremental()` watermark filter when it is the
/// only Jinja statement in the file. See [`IS_INCREMENTAL_FILTER_PATTERN`].
fn recognize_is_incremental_filter(content: &str) -> Option<RecognizedIncrementalFilter> {
    find_is_incremental_filter(content).filter(|recognized| !recognized.other_statement_tags)
}

/// Replace every `{% if ... is_incremental ... %} ... {% endif %}` block with
/// a sentinel on its own line. Returns the new content and, per block, the
/// sentinel plus the inert `-- ` comment that quotes the block. The caller
/// swaps the comments in after Jinja conversion, so the converter never sees
/// the quoted Jinja. An unterminated block is left in place.
fn comment_out_is_incremental_blocks(content: &str) -> (String, Vec<(String, String)>) {
    let mut output = String::with_capacity(content.len());
    let mut blocks = Vec::new();
    let mut copied_to = 0;
    let mut cursor = 0;
    // (block start, nesting depth) while inside an is_incremental() block.
    let mut open: Option<(usize, usize)> = None;

    while let Some(relative) = content[cursor..].find("{%") {
        let tag_start = cursor + relative;
        let body_start = tag_start + 2;
        let Some(body_len) = find_jinja_tag_end(&content[body_start..], "%}") else {
            break;
        };
        let tag_end = body_start + body_len + 2;
        let body = content[body_start..body_start + body_len]
            .trim()
            .trim_start_matches('-')
            .trim_end_matches('-')
            .trim();
        let keyword = body
            .split(|c: char| !is_jinja_identifier_char(c))
            .next()
            .unwrap_or("");
        cursor = tag_end;

        match open {
            None => {
                if keyword == "if" && contains_unquoted_jinja_identifier(body, "is_incremental") {
                    open = Some((tag_start, 1));
                }
            }
            Some((start, depth)) => match keyword {
                "if" => open = Some((start, depth + 1)),
                "endif" if depth == 1 => {
                    let sentinel = format!("__rocky_is_incremental_todo_{}__", blocks.len());
                    let mut comment = format!("-- {IS_INCREMENTAL_TODO}");
                    for line in content[start..tag_end].lines() {
                        comment.push_str("\n-- ");
                        comment.push_str(line.trim_end());
                    }
                    output.push_str(&content[copied_to..start]);
                    if !output.is_empty() && !output.ends_with('\n') {
                        output.push('\n');
                    }
                    output.push_str(&sentinel);
                    output.push('\n');
                    copied_to = tag_end;
                    blocks.push((sentinel, comment));
                    open = None;
                }
                "endif" => open = Some((start, depth - 1)),
                _ => {}
            },
        }
    }
    output.push_str(&content[copied_to..]);
    (output, blocks)
}

/// The Rocky strategy of a dbt incremental model converted through its
/// `is_incremental()` block, plus the import warnings that explain it.
struct IncrementalConversion {
    strategy: StrategyConfig,
    warnings: Vec<ImportWarning>,
}

/// Map a dbt incremental model's config onto [`StrategyConfig::Incremental`].
///
/// `watermark` is `None` when the `is_incremental()` block was not
/// recognized: the sidecar then has no watermark and `rocky compile`
/// refuses it (E037) until a human adds one. Returns `None` for an
/// `incremental_strategy` the watermark conversion does not express
/// (`insert_overwrite`, `microbatch`, anything unrecognized); the caller
/// keeps its previous behavior for those.
fn map_is_incremental_conversion(
    config: &DbtNodeConfig,
    model_name: &str,
    watermark: Option<&str>,
    filter_column: Option<&str>,
) -> Option<IncrementalConversion> {
    let keys = || match &config.unique_key {
        Some(UniqueKeyValue::Single(key)) => vec![key.clone()],
        Some(UniqueKeyValue::Multiple(keys)) => keys.clone(),
        None => Vec::new(),
    };
    let kind = config
        .incremental_strategy
        .as_deref()
        .map(str::to_ascii_lowercase);
    // dbt `append` ignores unique_key; `merge` upserts on it, which Rocky's
    // keyed incremental MERGE expresses. `delete+insert` is NOT converted: it
    // deletes every target row of a key (often non-unique, such as a date)
    // before inserting, which a MERGE does not reproduce.
    let unique_key = match kind.as_deref() {
        None | Some("merge") => keys(),
        Some("append") => Vec::new(),
        Some(_) => return None,
    };

    let mut warnings = Vec::new();
    let mut warn = |category: WarningCategory, message: String, suggestion: &str| {
        warnings.push(ImportWarning {
            model: model_name.to_string(),
            category,
            message,
            suggestion: Some(suggestion.to_string()),
        });
    };

    let on_schema_change = match config
        .on_schema_change
        .as_deref()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        None | Some("fail") => rocky_ir::OnSchemaChange::Fail,
        Some("append_new_columns") => rocky_ir::OnSchemaChange::AppendNewColumns,
        Some("sync_all_columns") => {
            warn(
                WarningCategory::UnsupportedMaterialization,
                "on_schema_change='sync_all_columns' mapped to `append_new_columns`: Rocky adds \
                 new columns but does not remove dropped ones (a column removed from the model \
                 fails the run)"
                    .to_string(),
                "drop removed columns from the target by hand, or rebuild with `rocky run --full-refresh`",
            );
            rocky_ir::OnSchemaChange::AppendNewColumns
        }
        Some("ignore") => {
            warn(
                WarningCategory::UnsupportedMaterialization,
                "on_schema_change='ignore' mapped to `fail`: Rocky fails the run on a column \
                 mismatch instead of ignoring it"
                    .to_string(),
                "set `on_schema_change = \"append_new_columns\"` in the sidecar [strategy] block \
                 to add new columns instead",
            );
            rocky_ir::OnSchemaChange::Fail
        }
        Some(other) => {
            warn(
                WarningCategory::UnsupportedMaterialization,
                format!("on_schema_change='{other}' not recognized; mapped to `fail`"),
                "set `on_schema_change` in the sidecar [strategy] block to `fail` or `append_new_columns`",
            );
            rocky_ir::OnSchemaChange::Fail
        }
    };

    if !unique_key.is_empty()
        && (config.merge_update_columns.is_some() || config.merge_exclude_columns.is_some())
    {
        warn(
            WarningCategory::UnsupportedMaterialization,
            "merge_update_columns / merge_exclude_columns dropped: Rocky's keyed incremental \
             MERGE updates every column"
                .to_string(),
            "use a `merge` strategy with `update_columns` if only some columns may change",
        );
    }

    if let Some(watermark) = watermark {
        let compared = filter_column.unwrap_or(watermark);
        warn(
            WarningCategory::MappedConstruct,
            format!(
                "dbt `is_incremental()` filter on `{compared}` mapped to Rocky's \
                 `@incremental_filter` with `timestamp_column = \"{watermark}\"`"
            ),
            "review the emitted SQL and [strategy] block; the first run and \
             `rocky run --full-refresh` load every row",
        );
    }

    Some(IncrementalConversion {
        strategy: StrategyConfig::Incremental {
            timestamp_column: watermark.map(str::to_string),
            unique_key,
            lookback: None,
            on_schema_change,
            filter_column: filter_column.map(str::to_string),
        },
        warnings,
    })
}

// ---------------------------------------------------------------------------
// Config extraction
// ---------------------------------------------------------------------------

/// Extract strategy from dbt `{{ config() }}` block.
///
/// Parses the `materialized` + `incremental_strategy` + `unique_key`
/// fields, dispatching through [`map_manifest_strategy`] so the regex
/// path stays in sync with the manifest path. Other config fields
/// (`event_time`, `batch_size`, `databricks_tags`, `pre_hook`,
/// `post_hook`, `on_schema_change`) are best-effort — the regex path
/// gives up on multi-line / structured values and falls back to the
/// manifest path for richer recovery.
///
/// Returns the chosen [`StrategyConfig`] plus a list of free-form
/// warning messages.
///
/// Note: structured warnings produced by [`map_manifest_strategy`] are
/// discarded here on purpose — the regex path emits string warnings
/// only (the manifest path is the canonical surface for
/// structured-warning consumers).
/// Map a dbt tag list onto Rocky's key/value `[tags]` shape. dbt tags are bare
/// labels; each becomes `<tag> = "true"` so it stays a queryable key in
/// `ModelConfig.tags` (a `BTreeMap<String, String>`).
fn dbt_tags_to_map(tags: &[String]) -> std::collections::BTreeMap<String, String> {
    tags.iter()
        .filter(|t| !t.trim().is_empty())
        .map(|t| (t.clone(), "true".to_string()))
        .collect()
}

/// Best-effort parse of `alias='...'` from a model's `{{ config(...) }}` block
/// on the regex (no-manifest) path. dbt `alias` overrides the output relation
/// name; dropping it would silently route the model's data to a table named
/// after the file.
fn extract_dbt_alias(content: &str) -> Option<String> {
    dbt_config_calls(content)
        .into_iter()
        .filter_map(|call| single_string_value(call, "alias"))
        .next_back()
}

fn inline_dbt_materialization(content: &str) -> Option<String> {
    dbt_config_calls(content)
        .into_iter()
        .filter_map(|call| {
            split_literal_items(call)?.into_iter().find_map(|arg| {
                let (key, value) = arg.split_once('=')?;
                (key.trim() == "materialized")
                    .then(|| quoted_literal_contents(value.trim()).map(str::to_string))
                    .flatten()
            })
        })
        .next_back()
}

pub(super) fn dbt_config_calls(content: &str) -> Vec<&str> {
    let mut calls = Vec::new();
    let mut rest = content;
    while let Some(start) = rest.find("{{") {
        rest = &rest[start + 2..];
        let Some(end) = find_jinja_tag_end(rest, "}}") else {
            break;
        };
        let body = rest[..end].trim().trim_start_matches('-').trim();
        let body = body.trim_end_matches('-').trim();
        if let Some(args) = body.strip_prefix("config") {
            let args = args.trim();
            if let Some(args) = args.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
                calls.push(args);
            }
        }
        rest = &rest[end + 2..];
    }
    calls
}

fn raw_config_is_resolvable(call: &str) -> bool {
    // These arguments are only a best-effort literal subset. Any expression
    // requiring Jinja evaluation must use the manifest path.
    let Some(args) = split_literal_items(call) else {
        return false;
    };
    let mut seen = HashSet::new();
    args.into_iter().all(|arg| {
        let Some((key, value)) = arg.split_once('=') else {
            return false;
        };
        let key = key.trim();
        let value = value.trim();
        !key.is_empty()
            && key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
            && seen.insert(key)
            && if key == "materialized" {
                is_literal_materialization(value)
            } else {
                is_literal_config_value(value)
            }
    })
}

fn is_literal_materialization(value: &str) -> bool {
    let value = quoted_literal_contents(value);
    value.is_some_and(|value| {
        !value.is_empty() && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
    })
}

fn is_literal_config_value(value: &str) -> bool {
    if quoted_literal_contents(value).is_some()
        || matches!(value, "true" | "false" | "True" | "False" | "none" | "None")
        || value.parse::<f64>().is_ok()
    {
        return true;
    }
    if let Some(inner) = value.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return split_literal_items(inner).is_some_and(|items| {
            items
                .iter()
                .all(|item| is_literal_config_value(item.trim()))
        });
    }
    if let Some(inner) = value.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        return split_literal_items(inner).is_some_and(|items| {
            items.iter().all(|item| {
                item.split_once(':').is_some_and(|(key, value)| {
                    quoted_literal_contents(key.trim()).is_some()
                        && is_literal_config_value(value.trim())
                })
            })
        });
    }
    false
}

pub(super) fn quoted_literal_contents(value: &str) -> Option<&str> {
    let quote = value.chars().next()?;
    if quote != '\'' && quote != '"' || !value.ends_with(quote) || value.len() < 2 {
        return None;
    }
    let inner = &value[1..value.len() - 1];
    let mut escaped = false;
    for ch in inner.chars() {
        if escaped {
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == quote {
            return None;
        }
    }
    (!escaped).then_some(inner)
}

pub(super) fn split_literal_items(value: &str) -> Option<Vec<&str>> {
    if value.trim().is_empty() {
        return Some(Vec::new());
    }
    let mut items = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut stack = Vec::new();
    for (index, ch) in value.char_indices() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                quote = None;
            }
            continue;
        }
        match ch {
            '\'' | '"' => quote = Some(ch),
            '[' | '{' => stack.push(ch),
            ']' if stack.pop() != Some('[') => return None,
            '}' if stack.pop() != Some('{') => return None,
            '(' | ')' => return None,
            ',' if stack.is_empty() => {
                let item = value[start..index].trim();
                if item.is_empty() {
                    return None;
                }
                items.push(item);
                start = index + 1;
            }
            _ => {}
        }
    }
    if quote.is_some() || !stack.is_empty() {
        return None;
    }
    let last = value[start..].trim();
    if !last.is_empty() {
        items.push(last);
    }
    Some(items)
}

fn extract_dbt_config(
    content: &str,
    inline_materialization: Option<&str>,
) -> (StrategyConfig, Vec<String>) {
    let mut messages = Vec::new();

    if dbt_config_calls(content).is_empty() {
        return (StrategyConfig::FullRefresh, messages);
    }
    let synthetic = raw_dbt_node_config(content, inline_materialization.unwrap_or("table"));

    // The regex (`--no-manifest`) path always uses the default microbatch
    // mapping. The `--microbatch-as=time_interval` translation needs the
    // model's compiled body to rewrite (it injects `@start_date`/`@end_date`),
    // which only the manifest path reliably provides; opting it in on the
    // reduced-fidelity regex path would risk corrupting un-compiled Jinja.
    let mapping = map_manifest_strategy(&synthetic, "<regex-path>", MicrobatchMode::Merge);
    for w in mapping.warnings {
        messages.push(w.message);
    }

    (mapping.strategy, messages)
}

/// Build a [`DbtNodeConfig`] from the last inline `{{ config(...) }}` call of
/// a raw model, with `materialized` as given. Without a config call every
/// other field is unset.
fn raw_dbt_node_config(content: &str, materialized: &str) -> DbtNodeConfig {
    let calls = dbt_config_calls(content);
    let config_str = calls.last().copied().unwrap_or("");

    // Parse unique_key — accepts string-form (`unique_key='id'`) or
    // single-line list (`unique_key=['user_id', 'date']`).
    let unique_key = parse_dbt_unique_key(config_str);

    // Parse incremental_strategy as the strategy discriminator (NOT a
    // column name). This fixes the long-standing bug where
    // `incremental_strategy='merge'` was treated as
    // `timestamp_column = 'merge'`.
    let incremental_strategy = single_string_value(config_str, "incremental_strategy");

    // dbt-microbatch fields
    let event_time = single_string_value(config_str, "event_time");
    let batch_size = single_string_value(config_str, "batch_size");
    let lookback = single_string_value(config_str, "lookback").and_then(|s| s.parse::<u32>().ok());

    // The regex path doesn't recover databricks_tags / hooks (they're
    // multi-line in practice), so they're left empty. `on_schema_change` is a
    // single literal and feeds the incremental conversion.
    DbtNodeConfig {
        materialized: materialized.to_string(),
        full_refresh: None,
        schema: None,
        unique_key,
        incremental_strategy,
        event_time,
        batch_size,
        lookback,
        partition_by: None,
        databricks_tags: BTreeMap::new(),
        pre_hook: Vec::new(),
        post_hook: Vec::new(),
        on_schema_change: single_string_value(config_str, "on_schema_change"),
        // alias does not affect strategy selection; the regex path threads it
        // to target.table separately via extract_dbt_alias.
        alias: None,
        merge_update_columns: None,
        merge_exclude_columns: None,
        // The regex path doesn't recover the contract block (it's nested config
        // not present in the inline `config()` call); contract detection is
        // manifest-only.
        contract: None,
        snapshot: None,
    }
}

/// Parse `unique_key=...` from a dbt config block. Accepts both
/// `unique_key='id'` and `unique_key=['user_id', 'date']` shapes.
/// Returns `None` if the field is absent or malformed.
fn parse_dbt_unique_key(config_str: &str) -> Option<UniqueKeyValue> {
    let single = Regex::new(r#"unique_key\s*=\s*['"](\w+)['"]"#).unwrap();
    if let Some(c) = single.captures(config_str) {
        return Some(UniqueKeyValue::Single(c[1].to_string()));
    }
    let list = Regex::new(r#"unique_key\s*=\s*\[([^\]]+)\]"#).unwrap();
    if let Some(c) = list.captures(config_str) {
        let raw = c.get(1).map(|m| m.as_str()).unwrap_or("");
        let keys: Vec<String> = raw
            .split(',')
            .map(|s| s.trim().trim_matches(|c| c == '\'' || c == '"').to_string())
            .filter(|s| !s.is_empty())
            .collect();
        if !keys.is_empty() {
            return Some(UniqueKeyValue::Multiple(keys));
        }
    }
    None
}

/// Best-effort extraction of `key='value'` from a dbt config block.
/// Returns `None` if the key is absent.
fn single_string_value(config_str: &str, key: &str) -> Option<String> {
    let pattern = format!(r#"\b{key}\s*=\s*['"]([^'"]+)['"]"#);
    let re = Regex::new(&pattern).ok()?;
    re.captures(config_str).map(|c| c[1].to_string())
}

// ---------------------------------------------------------------------------
// Jinja -> SQL conversion
// ---------------------------------------------------------------------------

/// Convert dbt Jinja expressions to plain SQL.
/// Strip a single pair of matching surrounding quotes (`'...'` or `"..."`)
/// from `s`. Used to normalize a dbt `var('x', 'default')` literal default
/// into the bare text Rocky's `@var(x, default)` expects.
fn strip_surrounding_quotes(s: &str) -> &str {
    let bytes = s.as_bytes();
    if bytes.len() >= 2
        && (bytes[0] == b'\'' || bytes[0] == b'"')
        && bytes[bytes.len() - 1] == bytes[0]
    {
        &s[1..s.len() - 1]
    } else {
        s
    }
}

pub(super) fn convert_jinja_to_sql(content: &str, this_ref: &str) -> String {
    let mut sql = strip_dbt_config_tags(content);

    // {{ ref('model_name', v=2) }} / version=2 -> model_name_v2 (a pinned
    // version of a versioned model).
    let versioned_ref_re = Regex::new(
        r#"\{\{\s*ref\s*\(\s*['"](\w+)['"]\s*,\s*(?:v|version)\s*=\s*['"]?(\d+)['"]?\s*\)\s*\}\}"#,
    )
    .unwrap();
    sql = versioned_ref_re.replace_all(&sql, "${1}_v${2}").to_string();

    // {{ ref('model_name') }} -> model_name
    let ref_re = Regex::new(r#"\{\{\s*ref\s*\(\s*['"](\w+)['"]\s*\)\s*\}\}"#).unwrap();
    sql = ref_re.replace_all(&sql, "$1").to_string();

    // {{ source('source_name', 'table_name') }} -> source_name.table_name
    let source_re =
        Regex::new(r#"\{\{\s*source\s*\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}"#)
            .unwrap();
    sql = source_re.replace_all(&sql, "$1.$2").to_string();

    // {{ var('x') }} -> @var(x) and {{ var('x', 'd') }} -> @var(x, d).
    // Rocky's per-run variables (`rocky run --var x=value`) are the native
    // home for dbt vars: the marker stays parse-visible and is resolved at
    // compile/render time. The default is emitted verbatim minus any
    // surrounding quotes so a string default `'d'` and a bare default `42`
    // both round-trip to `@var(x, d)` / `@var(x, 42)`.
    let var_re =
        Regex::new(r#"\{\{\s*var\s*\(\s*['"](\w+)['"]\s*(?:,\s*([^)]*?))?\s*\)\s*\}\}"#).unwrap();
    sql = var_re
        .replace_all(&sql, |caps: &regex::Captures<'_>| {
            let name = &caps[1];
            match caps.get(2) {
                Some(default) => {
                    let default = strip_surrounding_quotes(default.as_str().trim());
                    format!("@var({name}, {default})")
                }
                None => format!("@var({name})"),
            }
        })
        .to_string();

    // {{ this }} -> the model's own fully-qualified name. Substituting a real
    // catalog.schema.table (NoExpand so dots/`$` are literal) avoids emitting a
    // bogus `__this__` identifier that loads via the sidecar but fails at the
    // warehouse.
    let this_re = Regex::new(r"\{\{\s*this\s*\}\}").unwrap();
    sql = this_re
        .replace_all(&sql, regex::NoExpand(this_ref))
        .to_string();

    // Replace unsupported Jinja blocks with TODO comments
    let block_re = Regex::new(r"\{%[^%]*%\}").unwrap();
    sql = block_re
        .replace_all(&sql, "/* TODO: unsupported Jinja block */")
        .to_string();

    // Replace remaining {{ ... }} with TODO
    let expr_re = Regex::new(r"\{\{[^}]*\}\}").unwrap();
    sql = expr_re
        .replace_all(&sql, "/* TODO: unsupported Jinja expression */")
        .to_string();

    sql.trim().to_string()
}

fn strip_dbt_config_tags(content: &str) -> String {
    let mut output = String::with_capacity(content.len());
    let mut rest = content;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let tag = &rest[start + 2..];
        let Some(end) = find_jinja_tag_end(tag, "}}") else {
            output.push_str(&rest[start..]);
            return output;
        };
        let body = tag[..end].trim().trim_start_matches('-').trim();
        let body = body.trim_end_matches('-').trim();
        if !body
            .strip_prefix("config")
            .and_then(|s| s.trim().strip_prefix('('))
            .is_some_and(|s| s.ends_with(')'))
        {
            output.push_str(&rest[start..start + 2 + end + 2]);
        }
        rest = &tag[end + 2..];
    }
    output.push_str(rest);
    output
}

// ---------------------------------------------------------------------------
// Write output
// ---------------------------------------------------------------------------

/// Write imported models to an output directory as Rocky sidecar format.
pub fn write_imported_models(models: &[ImportedModel], output_dir: &Path) -> Result<(), String> {
    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("failed to create {}: {e}", output_dir.display()))?;

    for model in models {
        let sql_path = output_dir.join(format!("{}.sql", model.name));
        let toml_path = output_dir.join(format!("{}.toml", model.name));

        std::fs::write(&sql_path, &model.sql)
            .map_err(|e| format!("failed to write {}: {e}", sql_path.display()))?;

        let toml_content = toml::to_string_pretty(&model.config)
            .map_err(|e| format!("failed to serialize config: {e}"))?;
        std::fs::write(&toml_path, toml_content)
            .map_err(|e| format!("failed to write {}: {e}", toml_path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_is_incremental_does_not_expand_dollar_in_else_block() {
        // Regression: the else-block SQL was passed as a `&str` replacement to
        // `Regex::replace`, so a `$` sequence was interpreted as a capture-group
        // reference and silently corrupted the imported SQL. `NoExpand` copies
        // it verbatim.
        let sql = "{% if is_incremental() %} WHERE synced > 1 \
                   {% else %} WHERE note = 'costs $100' {% endif %}";
        let (processed, _) = detect_is_incremental(sql);
        assert!(
            processed.contains("costs $100"),
            "`$100` must survive verbatim, got: {processed}"
        );
    }

    #[test]
    fn detect_is_incremental_preserves_dollar_quoted_and_positional() {
        let (processed, _) = detect_is_incremental(
            "{% if is_incremental() %}x{% else %} WHERE a = $1 AND tag = $$active$$ {% endif %}",
        );
        assert!(
            processed.contains("$1"),
            "positional `$1` preserved: {processed}"
        );
        assert!(
            processed.contains("$$active$$"),
            "dollar-quoted preserved: {processed}"
        );
    }

    // --- Jinja conversion tests ---

    #[test]
    fn test_convert_ref() {
        let input = "SELECT * FROM {{ ref('orders') }}";
        assert_eq!(
            convert_jinja_to_sql(input, "cat.sch.tbl"),
            "SELECT * FROM orders"
        );
    }

    #[test]
    fn test_convert_var_no_default() {
        // `{{ var('drop_date') }}` maps to Rocky's `@var(drop_date)` marker —
        // NOT a TODO / unsupported-macro placeholder.
        let input = "SELECT * FROM t WHERE d = '{{ var('drop_date') }}'";
        let out = convert_jinja_to_sql(input, "cat.sch.tbl");
        assert_eq!(out, "SELECT * FROM t WHERE d = '@var(drop_date)'");
        assert!(!out.contains("TODO"));
    }

    #[test]
    fn test_convert_var_with_string_default() {
        // `{{ var('x', 'd') }}` maps to `@var(x, d)` — surrounding quotes on
        // the literal default are stripped.
        let input = "SELECT {{ var('region', 'us') }} AS r";
        assert_eq!(
            convert_jinja_to_sql(input, "cat.sch.tbl"),
            "SELECT @var(region, us) AS r"
        );
    }

    #[test]
    fn test_convert_var_with_bare_default() {
        // A non-string default (e.g. a number) round-trips verbatim.
        let input = "SELECT {{ var('threshold', 100) }} AS t";
        assert_eq!(
            convert_jinja_to_sql(input, "cat.sch.tbl"),
            "SELECT @var(threshold, 100) AS t"
        );
    }

    #[test]
    fn test_convert_source() {
        let input = "SELECT * FROM {{ source('raw', 'customers') }}";
        assert_eq!(
            convert_jinja_to_sql(input, "cat.sch.tbl"),
            "SELECT * FROM raw.customers"
        );
    }

    #[test]
    fn test_convert_config_removed() {
        let input = "{{ config(materialized='table') }}\nSELECT 1";
        assert_eq!(convert_jinja_to_sql(input, "cat.sch.tbl"), "SELECT 1");
    }

    #[test]
    fn test_convert_this() {
        let input = "SELECT * FROM {{ this }}";
        // {{ this }} resolves to the model's own FQN, not a bogus __this__.
        assert_eq!(
            convert_jinja_to_sql(input, "cat.sch.tbl"),
            "SELECT * FROM cat.sch.tbl"
        );
    }

    #[test]
    fn test_convert_unsupported_jinja() {
        let input = "{% if some_condition %}WHERE id > 0{% endif %}";
        let result = convert_jinja_to_sql(input, "cat.sch.tbl");
        assert!(result.contains("TODO: unsupported Jinja block"));
    }

    // --- Config extraction tests ---

    #[test]
    fn test_extract_config_incremental() {
        let input = "{{ config(materialized='incremental', unique_key='id') }}";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::Merge { .. }));
    }

    #[test]
    fn test_extract_config_materialized_ignores_nested_metadata_string() {
        let input = "{{ config(meta={'note': \"materialized='table'\"}, materialized='incremental', unique_key='id') }}";
        let inline_materialization = inline_dbt_materialization(input);
        assert_eq!(inline_materialization.as_deref(), Some("incremental"));
        let (strategy, _) = extract_dbt_config(input, inline_materialization.as_deref());
        assert!(matches!(strategy, StrategyConfig::Merge { .. }));
    }

    #[test]
    fn raw_import_refuses_top_level_incremental_after_nested_metadata_text() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("models/orders.sql"),
            "{{ config(meta={'note': \"materialized='table'\"}, materialized='incremental') }}\nSELECT * FROM source_orders WHERE id > 100",
        )
        .unwrap();
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].name, "orders");
        assert!(result.failed[0].reason.contains("effectively incremental"));
    }

    #[test]
    fn test_extract_config_table() {
        let input = "{{ config(materialized='table') }}";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::FullRefresh));
    }

    #[test]
    fn test_extract_config_view_maps_to_view_strategy() {
        // Wave 2: `materialized='view'` now maps to StrategyConfig::View
        // (no warning) instead of FullRefresh + warning.
        let input = "{{ config(materialized='view') }}";
        let (strategy, warnings) =
            extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::View));
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_extract_config_materialized_view() {
        // Wave 2: `materialized='materialized_view'` now maps to
        // StrategyConfig::MaterializedView (previously: dropped silently).
        let input = "{{ config(materialized='materialized_view') }}";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::MaterializedView));
    }

    #[test]
    fn test_extract_config_ephemeral_maps_to_ephemeral() {
        let input = "{{ config(materialized='ephemeral') }}";
        let (strategy, warnings) =
            extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::Ephemeral));
        assert!(warnings.is_empty(), "{warnings:?}");
    }

    #[test]
    fn test_extract_config_merge_with_unique_key_list() {
        // Regression: `incremental_strategy='merge'` + `unique_key=['user_id']`
        // must map to StrategyConfig::Merge, NOT to
        // `Incremental { timestamp_column: "merge" }`.
        let input = "{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['user_id']) }}";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        match strategy {
            StrategyConfig::Merge {
                unique_key,
                update_columns: _,
            } => assert_eq!(unique_key, vec!["user_id"]),
            other => panic!("expected Merge, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_config_incremental_strategy_is_not_a_column_name() {
        // Pin the parse-bug fix: `incremental_strategy='merge'` previously
        // captured 'merge' as a timestamp column. Assert that does NOT
        // happen anymore.
        let input = "{{ config(materialized='incremental', incremental_strategy='merge') }}";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        // Without unique_key, merge cannot apply, and the append fallback is
        // `full_refresh` (#1990: `incremental` is refused on transformation
        // models). The old failure mode was 'merge' landing in a timestamp
        // column; no strategy that carries one is emitted any more.
        assert!(
            matches!(strategy, StrategyConfig::FullRefresh),
            "merge without unique_key falls back to FullRefresh, got {strategy:?}"
        );
    }

    #[test]
    fn test_extract_no_config() {
        let input = "SELECT 1";
        let (strategy, _) = extract_dbt_config(input, inline_dbt_materialization(input).as_deref());
        assert!(matches!(strategy, StrategyConfig::FullRefresh));
    }

    #[test]
    fn test_multiple_refs() {
        let input =
            "SELECT * FROM {{ ref('orders') }} o JOIN {{ ref('customers') }} c ON o.id = c.id";
        let result = convert_jinja_to_sql(input, "cat.sch.tbl");
        assert_eq!(
            result,
            "SELECT * FROM orders o JOIN customers c ON o.id = c.id"
        );
    }

    // --- is_incremental() detection tests ---

    #[test]
    fn unresolved_incremental_detects_compound_and_indirect_jinja() {
        assert!(contains_unresolved_is_incremental(
            "{% if execute and is_incremental ( ) %}SELECT 1{% endif %}"
        ));
        assert!(contains_unresolved_is_incremental(
            "{% if execute %}SELECT 1{% elif target.name == 'prod' and is_incremental() %}SELECT 2{% endif %}"
        ));
        assert!(contains_unresolved_is_incremental(
            "{% if batch_id % 2 == 0 and is_incremental() %}SELECT 1{% endif %}"
        ));
        assert!(contains_unresolved_is_incremental(
            "{% set incremental = is_incremental() %}"
        ));
        assert!(contains_unresolved_is_incremental(
            "{% set incremental_check = is_incremental %}"
        ));
        assert!(contains_unresolved_is_incremental(
            "{% set incremental_check = '%}' and is_incremental %}"
        ));
        assert!(contains_unresolved_is_incremental("{{ is_incremental() }}"));
        assert!(!contains_unresolved_is_incremental(
            "{% set label = 'is_incremental' %}"
        ));
        assert!(!contains_unresolved_is_incremental(
            "{% set is_incrementally = true %}"
        ));
        assert!(!contains_unresolved_is_incremental(
            "SELECT 'is_incremental()' AS text"
        ));
    }

    #[test]
    fn test_detect_is_incremental_standard() {
        let sql = r#"
SELECT *
FROM source_table
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
"#;
        let (processed, detection) = detect_is_incremental(sql);
        assert!(detection.is_some());
        let det = detection.unwrap();
        assert_eq!(det.timestamp_column, "updated_at");
        // The incremental block should be removed
        assert!(!processed.contains("is_incremental"));
        assert!(processed.contains("SELECT *"));
        assert!(processed.contains("FROM source_table"));
    }

    #[test]
    fn test_detect_is_incremental_with_else() {
        let sql = r#"
SELECT *
FROM source_table
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% else %}
  WHERE 1=1
{% endif %}
"#;
        let (processed, detection) = detect_is_incremental(sql);
        assert!(detection.is_some());
        // Else block should be preserved
        assert!(processed.contains("WHERE 1=1"));
        assert!(!processed.contains("is_incremental"));
    }

    #[test]
    fn test_detect_is_incremental_fivetran_synced() {
        let sql = r#"
SELECT *
FROM raw.orders
{% if is_incremental() %}
  WHERE _fivetran_synced > (SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01') FROM {{ this }})
{% endif %}
"#;
        let (_, detection) = detect_is_incremental(sql);
        assert!(detection.is_some());
        assert_eq!(detection.unwrap().timestamp_column, "_fivetran_synced");
    }

    #[test]
    fn test_detect_is_incremental_none() {
        let sql = "SELECT * FROM orders";
        let (processed, detection) = detect_is_incremental(sql);
        assert!(detection.is_none());
        assert_eq!(processed, sql);
    }

    #[test]
    fn test_detect_is_incremental_simple_where() {
        let sql = r#"
{% if is_incremental() %}
  WHERE created_at >= '2020-01-01'
{% endif %}
"#;
        let (_, detection) = detect_is_incremental(sql);
        assert!(detection.is_some());
        assert_eq!(detection.unwrap().timestamp_column, "created_at");
    }

    // --- Manifest import tests ---

    #[test]
    fn test_import_from_manifest_basic() {
        let manifest_json = serde_json::json!({
            "metadata": {
                "dbt_schema_version": "v12",
                "dbt_version": "1.7.4",
                "generated_at": "2024-01-15T10:00:00Z",
                "project_name": "test_proj"
            },
            "nodes": {
                "model.test_proj.stg_orders": {
                    "unique_id": "model.test_proj.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT id, amount FROM raw_db.raw.orders",
                    "raw_code": "SELECT id, amount FROM {{ source('raw', 'orders') }}",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {
                        "id": { "name": "id", "description": "Order ID" }
                    },
                    "description": "Staged orders",
                    "tags": ["staging"],
                    "schema": "staging",
                    "database": "analytics"
                }
            },
            "sources": {}
        });

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();

        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "warehouse".to_string(),
            schema: "staging".to_string(),
            table: String::new(),
        };
        let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);

        assert_eq!(result.import_method, ImportMethod::Manifest);
        assert_eq!(result.imported.len(), 1);
        assert_eq!(result.imported[0].name, "stg_orders");
        assert_eq!(
            result.imported[0].sql,
            "SELECT id, amount FROM raw_db.raw.orders"
        );
        assert_eq!(
            result.imported[0].config.intent.as_deref(),
            Some("Staged orders")
        );
        assert_eq!(result.project_name.as_deref(), Some("test_proj"));
        assert_eq!(result.dbt_version.as_deref(), Some("1.7.4"));
    }

    #[test]
    fn test_apply_dbt_tests_converts_model_level_only_composite() {
        // Regression guard for the early-`continue` bug: a model whose ONLY
        // tests are model-level (no column tests) must still be converted.
        // apply_dbt_tests once summed only column tests when deciding whether
        // a model had any, so this model would have been skipped entirely.
        // Exercises the real YAML path (not tests_to_test_decls directly).
        let dir = tempfile::TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("fct_orders.sql"), "select 1 as order_id").unwrap();
        std::fs::write(
            models_dir.join("schema.yml"),
            r#"
models:
  - name: fct_orders
    tests:
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns: [order_id, line_number]
"#,
        )
        .unwrap();

        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.imported.len(), 1);

        assert_eq!(result.tests_found, 1, "model-level test must be found");
        assert_eq!(result.tests_converted, 1);
        assert_eq!(
            result.tests_converted_custom, 1,
            "the composite must count as a custom conversion"
        );
        assert_eq!(result.tests_skipped, 0);
    }

    #[test]
    fn refused_model_tests_are_not_counted_as_converted() {
        // #2337: a refused model has no sidecar, so its declared tests are
        // written nowhere and must not count as converted.
        let dir = tempfile::TempDir::new().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(models_dir.join("kept.sql"), "select 1 as id").unwrap();
        std::fs::write(
            models_dir.join("refused.sql"),
            "select 1 as id {% if target.name == 'prod' %}where id > 0{% endif %}",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("schema.yml"),
            r#"
models:
  - name: kept
    columns:
      - name: id
        tests: [not_null]
  - name: refused
    columns:
      - name: id
        tests: [not_null, unique]
"#,
        )
        .unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.imported.len(), 1);
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.tests_found, 3);
        assert_eq!(result.tests_converted, 1, "only the kept model's test");
        assert_eq!(result.tests_skipped, 2, "the refused model's tests");
        assert_eq!(result.imported[0].config.tests.len(), 1);
    }

    #[test]
    fn test_import_from_manifest_incremental_with_key() {
        let manifest_json = serde_json::json!({
            "metadata": { "project_name": "proj" },
            "nodes": {
                "model.proj.fct": {
                    "unique_id": "model.proj.fct",
                    "name": "fct",
                    "resource_type": "model",
                    "compiled_code": "SELECT * FROM stg",
                    "raw_code": "SELECT * FROM {{ ref('stg') }}",
                    "depends_on": { "nodes": ["model.proj.stg"], "macros": [] },
                    "config": {
                        "materialized": "incremental",
                        "unique_key": "id"
                    },
                    "columns": {},
                    "tags": [],
                    "schema": "marts",
                    "database": "db"
                }
            },
            "sources": {}
        });

        let result = import_from_manifest_json_with_evidence(&manifest_json, MicrobatchMode::Merge);

        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::Merge { .. }
        ));
        assert_eq!(result.imported[0].config.depends_on, vec!["stg"]);
    }

    /// dbt model governance survives a manifest import and the emitted repo
    /// loads with the same meaning: versions become `<name>_v<N>` plus a
    /// declaration, `ref(v=1)` pins `orders_v1`, access/group/owner carry over.
    #[test]
    fn manifest_import_carries_access_groups_and_versions() {
        let node = |id: &str, name: &str, version: Option<i64>, code: &str, deps: Vec<&str>| {
            let mut n = serde_json::json!({
                "unique_id": id,
                "name": name,
                "resource_type": "model",
                "compiled_code": code,
                "raw_code": code,
                "depends_on": { "nodes": deps, "macros": [] },
                "config": { "materialized": "table" },
                "columns": {},
                "tags": [],
                "schema": "s",
                "database": "d",
                "access": "public",
                "group": "finance",
            });
            if let Some(v) = version {
                n["version"] = serde_json::json!(v);
                n["latest_version"] = serde_json::json!(2);
                n["relation_name"] = serde_json::json!(format!("\"d\".\"s\".\"{name}_v{v}\""));
                if v == 1 {
                    n["deprecation_date"] = serde_json::json!("2026-12-31T00:00:00");
                }
            } else {
                n["access"] = serde_json::json!("private");
            }
            n
        };
        let manifest_json = serde_json::json!({
            "metadata": { "project_name": "proj" },
            "nodes": {
                "model.proj.orders.v1": node("model.proj.orders.v1", "orders", Some(1), "SELECT 1 AS id", vec![]),
                "model.proj.orders.v2": node("model.proj.orders.v2", "orders", Some(2), "SELECT 1 AS id, 2 AS amount", vec![]),
                "model.proj.reader": node(
                    "model.proj.reader", "reader", None,
                    "SELECT id FROM \"d\".\"s\".\"orders_v1\"",
                    vec!["model.proj.orders.v1"],
                ),
            },
            "sources": {},
            "groups": {
                "group.proj.finance": {
                    "name": "finance",
                    "owner": { "name": "Fin", "email": "fin@example.com" }
                }
            }
        });
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "d".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        let mut names: Vec<&str> = result.imported.iter().map(|m| m.name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(names, vec!["orders_v1", "orders_v2", "reader"]);
        let reader = result.imported.iter().find(|m| m.name == "reader").unwrap();
        assert_eq!(reader.config.depends_on, vec!["orders_v1".to_string()]);
        assert_eq!(reader.sql, "SELECT id FROM orders_v1");

        let out = tempfile::TempDir::new().unwrap();
        let profile = super::super::dbt_profiles::resolution_for_kind(
            super::super::dbt_profiles::AdapterKind::DuckDb,
            "duckdb",
        );
        super::super::emit::emit_repo(&super::super::emit::EmitInputs {
            dbt_project_dir: dir.path(),
            out_dir: out.path(),
            overwrite: super::super::emit::OverwritePolicy::ReplaceContents,
            profile: &profile,
            default_catalog: "d",
            default_schema: "s",
            import: &result,
            adapter_override_label: None,
        })
        .unwrap();
        let models_dir = out.path().join("models");
        let decl = std::fs::read_to_string(models_dir.join("orders.toml")).unwrap();
        assert!(decl.contains("latest_version = 2"), "{decl}");
        assert!(decl.contains("deprecation_date = \"2026-12-31\""), "{decl}");
        let group = std::fs::read_to_string(models_dir.join("groups/finance.toml")).unwrap();
        assert!(group.contains("email = \"fin@example.com\""), "{group}");

        // The emitted repo loads: versions stamped, alias added, reader pinned.
        let models = crate::project::Project::load_models(&models_dir, None).unwrap();
        let project = crate::project::Project::from_models(models).unwrap();
        let v1 = project.model("orders_v1").unwrap();
        let info = v1.config.governance.version.as_ref().unwrap();
        assert_eq!((info.version, info.latest_version), (Some(1), 2));
        assert_eq!(
            v1.config.governance.access,
            Some(rocky_core::model_governance::ModelAccess::Public)
        );
        assert_eq!(
            v1.config.governance.owner.as_ref().unwrap().name.as_deref(),
            Some("Fin")
        );
        assert!(project.model("orders").is_some(), "latest alias");
        let reader = project.model("reader").unwrap();
        assert_eq!(
            reader.config.governance.access,
            Some(rocky_core::model_governance::ModelAccess::Private)
        );
    }

    /// A dbt version that is not a whole number is refused with a reason,
    /// never silently renamed.
    #[test]
    fn manifest_import_refuses_non_integer_version() {
        let manifest_json = serde_json::json!({
            "metadata": { "project_name": "proj" },
            "nodes": {
                "model.proj.orders.v1.5": {
                    "unique_id": "model.proj.orders.v1.5",
                    "name": "orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "config": { "materialized": "table" },
                    "version": 1.5
                }
            },
            "sources": {}
        });
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "d".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);
        assert!(result.imported.is_empty());
        assert!(result.failed[0].reason.contains("not a whole number"));
    }

    #[test]
    fn test_import_from_manifest_view_maps_to_view() {
        // Wave 2: `materialized='view'` now maps to StrategyConfig::View
        // directly (previously: FullRefresh + warning).
        let manifest_json = serde_json::json!({
            "metadata": { "project_name": "proj" },
            "nodes": {
                "model.proj.v": {
                    "unique_id": "model.proj.v",
                    "name": "v",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "view" },
                    "columns": {},
                    "tags": [],
                    "schema": "s",
                    "database": "d"
                }
            },
            "sources": {}
        });

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();

        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);

        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::View
        ));
        // No "view not supported" warning anymore.
        assert!(
            result
                .warnings
                .iter()
                .all(|w| !w.message.contains("'view'")),
            "view should no longer emit an 'unsupported' warning"
        );
    }

    // --- Full project import tests ---

    #[test]
    fn test_import_dbt_project_with_project_yml() {
        let dir = tempfile::TempDir::new().unwrap();

        // Create dbt_project.yml
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            r#"
name: test_proj
model-paths: ["models"]
models:
  test_proj:
    staging:
      +materialized: view
      +schema: staging
"#,
        )
        .unwrap();

        // Create models/staging/
        std::fs::create_dir_all(dir.path().join("models/staging")).unwrap();

        std::fs::write(
            dir.path().join("models/staging/stg_orders.sql"),
            "SELECT * FROM {{ ref('raw_orders') }}",
        )
        .unwrap();

        let target = TargetConfig {
            catalog: "warehouse".to_string(),
            schema: "default".to_string(),
            table: String::new(),
        };

        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.import_method, ImportMethod::Regex);
        assert_eq!(result.project_name.as_deref(), Some("test_proj"));
        assert_eq!(result.imported.len(), 1);
        // Schema should come from project config
        assert_eq!(result.imported[0].config.target.schema, "staging");
    }

    #[test]
    fn test_import_dbt_project_with_sources() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("models")).unwrap();

        // Create _sources.yml
        std::fs::write(
            dir.path().join("models/_sources.yml"),
            r#"
sources:
  - name: raw
    database: raw_catalog
    schema: raw_schema
    tables:
      - name: orders
"#,
        )
        .unwrap();

        // Create model that references the source
        std::fs::write(
            dir.path().join("models/stg_orders.sql"),
            "SELECT * FROM {{ source('raw', 'orders') }}",
        )
        .unwrap();

        let target = TargetConfig {
            catalog: "warehouse".to_string(),
            schema: "staging".to_string(),
            table: String::new(),
        };

        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.sources_found, 1);
        assert_eq!(result.sources_mapped, 1);
        assert_eq!(result.imported.len(), 1);
        // Should have resolved the source
        assert_eq!(result.imported[0].config.sources.len(), 1);
        assert_eq!(result.imported[0].config.sources[0].catalog, "raw_catalog");
        assert_eq!(result.imported[0].config.sources[0].schema, "raw_schema");
    }

    fn import_raw_model(sql: &str) -> ImportResult {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("models")).unwrap();
        std::fs::write(dir.path().join("models/fct_events.sql"), sql).unwrap();
        let target = TargetConfig {
            catalog: "warehouse".to_string(),
            schema: "staging".to_string(),
            table: String::new(),
        };
        import_dbt_project(dir.path(), &target).unwrap()
    }

    /// An `is_incremental()` use the recognizer does not accept (a compound
    /// condition here) is imported, not dropped: the block becomes an inert
    /// TODO comment and the sidecar has no watermark, so `rocky compile`
    /// refuses the model (E037) until a human finishes it. Before WP6 the
    /// raw path refused the model outright.
    #[test]
    fn raw_unrecognized_incremental_guard_is_kept_as_a_todo() {
        let result = import_raw_model(
            r#"
{{ config(materialized='incremental') }}

SELECT *
FROM {{ ref('stg_events') }}
{% if execute and is_incremental ( ) %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
"#,
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(result.imported.len(), 1);
        let model = &result.imported[0];
        assert!(
            model
                .sql
                .contains("-- TODO: dbt is_incremental() block not translated:"),
            "{}",
            model.sql
        );
        assert!(
            model
                .sql
                .contains("--   WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})"),
            "the original block is quoted as a comment: {}",
            model.sql
        );
        assert!(!model.sql.contains("@incremental_filter"), "{}", model.sql);
        assert!(
            matches!(
                &model.config.strategy,
                StrategyConfig::Incremental {
                    timestamp_column: None,
                    ..
                }
            ),
            "{:?}",
            model.config.strategy
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.message.contains("not translated") && w.message.contains("E037")),
            "{:?}",
            result.warnings
        );
    }

    /// The standard watermark filter converts on the raw path: the block
    /// becomes the placeholder and the key carries over (MERGE upsert).
    #[test]
    fn raw_standard_incremental_filter_converts_to_placeholder() {
        let result = import_raw_model(
            r#"
{{ config(materialized='incremental', unique_key='id', on_schema_change='append_new_columns') }}

SELECT *
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
"#,
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        let model = &result.imported[0];
        assert!(
            model.sql.contains("WHERE @incremental_filter"),
            "{}",
            model.sql
        );
        assert!(!model.sql.contains("is_incremental"), "{}", model.sql);
        assert!(!model.sql.contains("{%"), "{}", model.sql);
        match &model.config.strategy {
            StrategyConfig::Incremental {
                timestamp_column,
                unique_key,
                lookback,
                on_schema_change,
                filter_column,
            } => {
                assert_eq!(timestamp_column.as_deref(), Some("event_time"));
                assert_eq!(unique_key, &vec!["id".to_string()]);
                assert!(lookback.is_none());
                assert_eq!(
                    *on_schema_change,
                    rocky_ir::OnSchemaChange::AppendNewColumns
                );
                assert!(filter_column.is_none());
            }
            other => panic!("expected incremental, got {other:?}"),
        }
    }

    /// `append` drops the key (dbt append ignores it); a qualified left side
    /// becomes `filter_column`; `AND` keeps its keyword.
    #[test]
    fn raw_append_filter_with_qualified_column_converts() {
        let result = import_raw_model(
            r#"
{{ config(materialized='incremental', incremental_strategy='append', unique_key='id') }}

SELECT e.id, e.synced_at AS event_time
FROM {{ ref('stg_events') }} e
WHERE e.id > 0
{%- if is_incremental() -%}
  AND e.synced_at > (select max(event_time) from {{this}})
{%- endif %}
"#,
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        let model = &result.imported[0];
        assert!(
            model.sql.contains("AND @incremental_filter"),
            "{}",
            model.sql
        );
        match &model.config.strategy {
            StrategyConfig::Incremental {
                timestamp_column,
                unique_key,
                filter_column,
                ..
            } => {
                assert_eq!(timestamp_column.as_deref(), Some("event_time"));
                assert!(unique_key.is_empty(), "append drops the key");
                assert_eq!(filter_column.as_deref(), Some("e.synced_at"));
            }
            other => panic!("expected incremental, got {other:?}"),
        }
    }

    #[test]
    fn recognizer_accepts_only_the_standard_strict_filter() {
        let ok = recognize_is_incremental_filter(
            "SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM {{ this }}) {% endif %}",
        )
        .expect("standard form");
        assert_eq!(ok.watermark, "ts");
        assert_eq!(ok.filter_column, None);
        assert!(
            ok.rewritten.contains("WHERE @incremental_filter"),
            "{}",
            ok.rewritten
        );

        for rejected in [
            // `>=` re-reads rows at the watermark: a different filter.
            "SELECT * FROM t {% if is_incremental() %} WHERE ts >= (SELECT MAX(ts) FROM {{ this }}) {% endif %}",
            // An else branch carries first-run logic the placeholder cannot.
            "SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM {{ this }}) {% else %} WHERE 1=1 {% endif %}",
            // The bound must come from the model's own table.
            "SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM other) {% endif %}",
            // Two blocks.
            "SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM {{ this }}) {% endif %} \
             UNION ALL SELECT * FROM u {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM {{ this }}) {% endif %}",
            // Other statement tags.
            "{% set x = 1 %} SELECT * FROM t {% if is_incremental() %} WHERE ts > (SELECT MAX(ts) FROM {{ this }}) {% endif %}",
            // A literal bound.
            "SELECT * FROM t {% if is_incremental() %} WHERE ts > '2024-01-01' {% endif %}",
        ] {
            assert!(
                recognize_is_incremental_filter(rejected).is_none(),
                "must not recognize: {rejected}"
            );
        }
    }

    #[test]
    fn test_import_missing_source_warning() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir_all(dir.path().join("models")).unwrap();

        // No _sources.yml, but model references a source
        std::fs::write(
            dir.path().join("models/stg.sql"),
            "SELECT * FROM {{ source('missing', 'tbl') }}",
        )
        .unwrap();

        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };

        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.warnings.iter().any(|w| {
            w.category == WarningCategory::MissingSource && w.message.contains("missing")
        }));
    }

    // ---------------------------------------------------------------------
    // Wave 2: dbt materialization mapping tests
    // ---------------------------------------------------------------------

    /// Helper: parse a manifest from a JSON value and run import.
    fn import_from_manifest_json(manifest_json: &serde_json::Value) -> ImportResult {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge)
    }

    fn import_from_manifest_json_with_mode(
        manifest_json: &serde_json::Value,
        mode: MicrobatchMode,
    ) -> ImportResult {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        import_from_manifest(&manifest, &target, false, mode)
    }

    /// Supply a matching compile pair for tests of downstream strategy and warnings.
    fn import_from_manifest_json_with_evidence(
        manifest_json: &serde_json::Value,
        mode: MicrobatchMode,
    ) -> ImportResult {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        let mut manifest_json = manifest_json.clone();
        manifest_json["metadata"]["invocation_id"] = serde_json::json!("strategy-test");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let results: Vec<_> = manifest_json["nodes"]
            .as_object()
            .unwrap()
            .values()
            .filter_map(|node| node["unique_id"].as_str())
            .map(|id| serde_json::json!({"unique_id": id, "status": "success"}))
            .collect();
        std::fs::write(
            dir.path().join("run_results.json"),
            serde_json::json!({"metadata":{"invocation_id":"strategy-test"},"args":{"full_refresh":true},"results":results}).to_string(),
        )
        .unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        import_from_manifest(&manifest, &target, false, mode)
    }

    #[test]
    fn test_microbatch_as_time_interval_emits_time_interval_strategy() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.events_daily": {
                "unique_id": "model.p.events_daily", "name": "events_daily",
                "resource_type": "model",
                "compiled_code": "SELECT event_ts, amount FROM raw.events",
                "raw_code": "SELECT event_ts, amount FROM raw.events",
                "depends_on": { "nodes": [], "macros": [] },
                "config": {
                    "materialized": "incremental",
                    "incremental_strategy": "microbatch",
                    "event_time": "event_ts",
                    "batch_size": "day",
                    "lookback": 3,
                    "unique_key": "id"
                },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });
        let result =
            import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::TimeInterval);
        let model = &result.imported[0];
        match &model.config.strategy {
            StrategyConfig::TimeInterval {
                time_column,
                granularity,
                lookback,
                batch_size,
                ..
            } => {
                assert_eq!(time_column, "event_ts");
                assert_eq!(*granularity, rocky_ir::TimeGrain::Day);
                assert_eq!(*lookback, 3);
                assert_eq!(batch_size.get(), 1);
            }
            other => panic!("expected TimeInterval, got {other:?}"),
        }
        // Body must reference @start_date/@end_date so the runtime can bound it.
        assert!(
            model.sql.contains("@start_date") && model.sql.contains("@end_date"),
            "rewritten body must inject the partition placeholders: {}",
            model.sql
        );
        assert!(
            model
                .sql
                .contains("event_ts >= @start_date AND event_ts < @end_date"),
            "half-open bound must match the runtime partition filter: {}",
            model.sql
        );
        assert!(
            result.structured_warnings.iter().any(|w| matches!(w,
                ImportDbtStructuredWarning::MicrobatchMapped { mapped_to, .. }
                    if mapped_to == "time_interval")),
            "must record the time_interval mapping"
        );
    }

    #[test]
    fn test_microbatch_default_mode_still_maps_to_merge() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.f": model_node("f",
                serde_json::json!({ "materialized": "microbatch", "event_time": "ts", "batch_size": "day", "unique_key": "id" }),
                serde_json::json!([])) },
            "sources": {}
        });
        // Default mode is Merge — back-compat: no time_interval emitted.
        let result = import_from_manifest_json(&manifest);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::Merge { .. }
        ));
        assert!(!result.imported[0].sql.contains("@start_date"));
    }

    #[test]
    fn test_microbatch_as_time_interval_unsafe_event_time_falls_back_to_merge() {
        // An event_time that isn't a plain identifier can't be safely
        // interpolated; the importer must fall back to merge and warn that
        // bounded-window semantics were not preserved.
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.f": model_node("f",
                serde_json::json!({ "materialized": "microbatch", "event_time": "ts; DROP TABLE x", "batch_size": "day", "unique_key": "id" }),
                serde_json::json!([])) },
            "sources": {}
        });
        let result = import_from_manifest_json_with_mode(&manifest, MicrobatchMode::TimeInterval);
        assert!(
            matches!(
                result.imported[0].config.strategy,
                StrategyConfig::Merge { .. }
            ),
            "unsafe event_time must fall back to merge"
        );
        assert!(
            result.warnings.iter().any(|w| w
                .message
                .contains("bounded-window semantics were not preserved")),
            "must warn that the bounded-window mapping was skipped"
        );
    }

    #[test]
    fn test_microbatch_granularity_maps_batch_size() {
        assert_eq!(
            microbatch_granularity(Some("hour")),
            rocky_ir::TimeGrain::Hour
        );
        assert_eq!(
            microbatch_granularity(Some("day")),
            rocky_ir::TimeGrain::Day
        );
        assert_eq!(
            microbatch_granularity(Some("month")),
            rocky_ir::TimeGrain::Month
        );
        assert_eq!(
            microbatch_granularity(Some("year")),
            rocky_ir::TimeGrain::Year
        );
        // Absent / unknown defaults to Day.
        assert_eq!(microbatch_granularity(None), rocky_ir::TimeGrain::Day);
        assert_eq!(
            microbatch_granularity(Some("weird")),
            rocky_ir::TimeGrain::Day
        );
    }

    #[test]
    fn test_manifest_view_maps_to_view_strategy() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.dim_customers": {
                    "unique_id": "model.p.dim_customers",
                    "name": "dim_customers",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "view" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::View
        ));
    }

    fn model_node(
        name: &str,
        config: serde_json::Value,
        tags: serde_json::Value,
    ) -> serde_json::Value {
        let mut node = serde_json::json!({
            "unique_id": format!("model.p.{name}"),
            "name": name,
            "resource_type": "model",
            "compiled_code": "SELECT 1",
            "raw_code": "SELECT 1",
            "depends_on": { "nodes": [], "macros": [] },
            "columns": {}, "schema": "s", "database": "d"
        });
        node["config"] = config;
        node["tags"] = tags;
        node
    }

    #[test]
    fn test_manifest_alias_overrides_target_table() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.m": model_node("m",
                serde_json::json!({ "materialized": "table", "alias": "renamed" }),
                serde_json::json!([])) },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        // alias drives the physical table; the logical name is unchanged.
        assert_eq!(result.imported[0].config.target.table, "renamed");
        assert_eq!(result.imported[0].config.name, "m");
    }

    #[test]
    fn test_manifest_microbatch_with_unique_key_maps_to_merge() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            // The REAL dbt microbatch form: materialized='incremental' +
            // incremental_strategy='microbatch' (there is no
            // materialized='microbatch' in dbt). This routes through
            // map_incremental_strategy, so it guards the structured-warning
            // threading.
            "nodes": { "model.p.f": model_node("f",
                serde_json::json!({ "materialized": "incremental", "incremental_strategy": "microbatch", "event_time": "ts", "batch_size": "day", "unique_key": "id" }),
                serde_json::json!([])) },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        assert!(
            matches!(
                result.imported[0].config.strategy,
                StrategyConfig::Merge { .. }
            ),
            "microbatch with a unique_key maps to an idempotent merge"
        );
        assert!(result.structured_warnings.iter().any(|w| matches!(w,
            ImportDbtStructuredWarning::MicrobatchMapped { mapped_to, .. } if mapped_to == "merge")));
    }

    #[test]
    fn test_manifest_enforced_contract_is_generated_and_partial_loss_warns() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.dim_customer": {
                "unique_id": "model.p.dim_customer", "name": "dim_customer",
                "resource_type": "model",
                "compiled_code": "SELECT 1", "raw_code": "SELECT 1",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "table", "contract": { "enforced": true } },
                "columns": {
                    "id": { "name": "id", "data_type": "bigint", "constraints": [{ "type": "not_null" }, { "type": "primary_key" }] },
                    "email": { "name": "email", "data_type": "varchar", "constraints": [{ "type": "unique" }] },
                    "loc": { "name": "loc", "data_type": "geography" }
                },
                "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        let toml = result.imported[0]
            .contract_toml
            .as_deref()
            .expect("an enforced contract generates a contract file");
        assert!(toml.contains("name = \"id\"\ntype = \"Int64\"\n"));
        assert!(
            !toml.contains("nullable = false"),
            "a not_null constraint must not become an E012 check: {toml}"
        );
        assert!(toml.contains("required = [\"email\", \"id\", \"loc\"]"));
        assert_eq!(
            result.contracts_dropped, 1,
            "a contract with unchecked parts must be counted"
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::DroppedContract),
            "must emit a DroppedContract string warning"
        );
        let structured = result
            .structured_warnings
            .iter()
            .find_map(|w| match w {
                ImportDbtStructuredWarning::DroppedContract {
                    typed_columns,
                    constraints,
                    not_null_constraints,
                    contract_path,
                    ..
                } => Some((
                    *typed_columns,
                    *constraints,
                    contract_path.clone(),
                    *not_null_constraints,
                )),
                _ => None,
            })
            .expect("must emit a DroppedContract structured warning");
        assert_eq!(structured.0, 1, "one column type has no Rocky name");
        assert_eq!(structured.1, 1, "one constraint is not checked");
        assert_eq!(structured.3, 2, "not_null and primary_key are not checked");
        assert!(
            structured.2.contains("dim_customer.contract.toml"),
            "warning must point at the contract sidecar path: {}",
            structured.2
        );
    }

    fn exposure_manifest() -> serde_json::Value {
        serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.revenue": {
                "unique_id": "model.p.revenue", "name": "revenue", "resource_type": "model",
                "compiled_code": "SELECT 1 AS id", "raw_code": "SELECT 1 AS id",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "table" },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {},
            "exposures": {
                "exposure.p.weekly_board": {
                    "name": "weekly_board",
                    "type": "dashboard",
                    "url": "https://bi.example.com/board",
                    "description": "Revenue by region",
                    "owner": { "name": "Ana", "email": "ana@example.com" },
                    "depends_on": { "nodes": [
                        "model.p.revenue", "model.p.gone", "source.p.shop.orders"
                    ] }
                },
                "exposure.p.ad_hoc": { "name": "ad_hoc", "type": "spreadsheet" },
                "exposure.p.bad": { "name": "bad name" }
            }
        })
    }

    #[test]
    fn test_manifest_exposures_become_consumers_over_imported_models() {
        let result = import_from_manifest_json(&exposure_manifest());
        let names: Vec<&str> = result.consumers.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["ad_hoc", "weekly_board"]);
        let board = &result.consumers[1];
        assert_eq!(board.kind, rocky_core::consumers::ConsumerKind::Dashboard);
        assert_eq!(board.owner.as_deref(), Some("Ana, ana@example.com"));
        assert_eq!(board.url.as_deref(), Some("https://bi.example.com/board"));
        assert_eq!(board.description.as_deref(), Some("Revenue by region"));
        // Only the imported model survives, so the emitted repo compiles.
        assert_eq!(board.depends_on, ["revenue"]);
        // An unknown dbt type is `other`.
        assert_eq!(
            result.consumers[0].kind,
            rocky_core::consumers::ConsumerKind::Other
        );
    }

    /// Two exposures that differ only by letter case would share one file on a
    /// case-insensitive filesystem. The second is refused, with a note.
    #[test]
    fn test_manifest_exposures_differing_only_by_case_are_not_both_written() {
        let mut manifest = exposure_manifest();
        manifest["exposures"] = serde_json::json!({
            "exposure.p.Board": { "name": "Board", "type": "dashboard" },
            "exposure.p.board": { "name": "board", "type": "dashboard" }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.consumers.len(), 1, "{:?}", result.consumers);
        let lowered: std::collections::HashSet<String> = result
            .consumers
            .iter()
            .map(|c| c.name.to_ascii_lowercase())
            .collect();
        assert_eq!(lowered.len(), result.consumers.len());
        assert!(result.structured_warnings.iter().any(|w| matches!(
            w,
            ImportDbtStructuredWarning::DroppedConstruct { detail, .. }
                if detail.contains("only by letter case")
        )));
    }

    #[test]
    fn test_manifest_exposure_leftovers_are_listed_in_the_notes() {
        let result = import_from_manifest_json(&exposure_manifest());
        let details: Vec<(String, String, String)> = result
            .structured_warnings
            .iter()
            .filter_map(|w| match w {
                ImportDbtStructuredWarning::DroppedConstruct {
                    construct,
                    name,
                    detail,
                } if construct.starts_with("exposure") => {
                    Some((construct.clone(), name.clone(), detail.clone()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(details.len(), 2, "{details:?}");
        let partial = details
            .iter()
            .find(|(_, name, _)| name == "weekly_board")
            .expect("partial exposure is listed");
        assert_eq!(partial.0, "exposure dependency");
        assert!(partial.2.contains("source shop.orders"), "{}", partial.2);
        assert!(
            partial.2.contains("model gone (not imported)"),
            "{}",
            partial.2
        );
        let refused = details
            .iter()
            .find(|(_, name, _)| name == "bad name")
            .expect("invalid name is listed");
        assert_eq!(refused.0, "exposure");
        // A fully mapped exposure (ad_hoc reads nothing) adds no note.
        assert!(details.iter().all(|(_, name, _)| name != "ad_hoc"));
    }

    #[test]
    fn test_manifest_unenforced_contract_is_not_dropped() {
        // dbt's default `contract: { enforced: false }` carries no semantics
        // to lose; it must not trip the dropped-contract warning.
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.m": {
                "unique_id": "model.p.m", "name": "m", "resource_type": "model",
                "compiled_code": "SELECT 1", "raw_code": "SELECT 1",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "table", "contract": { "enforced": false } },
                "columns": { "id": { "name": "id", "data_type": "bigint" } },
                "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.contracts_dropped, 0);
        assert!(
            !result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::DroppedContract)
        );
    }

    #[test]
    fn test_manifest_tags_carry_onto_model() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.m": model_node("m",
                serde_json::json!({ "materialized": "table" }),
                serde_json::json!(["finance", "daily"])) },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        let tags = &result.imported[0].config.tags;
        assert_eq!(tags.get("finance").map(String::as_str), Some("true"));
        assert_eq!(tags.get("daily").map(String::as_str), Some("true"));
    }

    #[test]
    fn test_manifest_sweep_reports_dropped_constructs() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": model_node("m", serde_json::json!({ "materialized": "table" }), serde_json::json!([])),
                "snapshot.p.snap": {
                    "unique_id": "snapshot.p.snap", "name": "snap",
                    "resource_type": "snapshot", "raw_code": ""
                }
            },
            "sources": {},
            "metrics": { "metric.p.rev": {} }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.imported.len(), 1, "only the model imports");
        // A snapshot is no longer a dropped construct: one it cannot read
        // (here, no config at all) is an import failure with the reason.
        assert_eq!(result.constructs_dropped, 1, "1 metric");
        assert!(
            result
                .failed
                .iter()
                .any(|f| f.name == "snap" && f.reason.contains("unique_key"))
        );
        assert!(!result.structured_warnings.iter().any(|w| matches!(w,
            ImportDbtStructuredWarning::DroppedConstruct { construct, .. } if construct == "snapshot")));
    }

    #[test]
    fn test_manifest_snapshot_node_converts_to_snapshot_model() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "snapshot.p.orders_snap": {
                    "unique_id": "snapshot.p.orders_snap", "name": "orders_snap",
                    "resource_type": "snapshot",
                    "compiled_code": "select * from \"d\".\"raw\".\"orders\"",
                    "raw_code": "{% snapshot orders_snap %}{{ config(unique_key='id') }} select * from {{ source('raw','orders') }} {% endsnapshot %}",
                    "depends_on": { "nodes": ["source.p.raw.orders"], "macros": [] },
                    "config": {
                        "materialized": "snapshot", "target_schema": "snapshots",
                        "unique_key": ["id", "region"], "strategy": "check",
                        "check_cols": "all", "hard_deletes": "invalidate",
                        "snapshot_meta_column_names": { "dbt_valid_from": "start_at", "dbt_scd_id": null }
                    },
                    "tags": [], "schema": "analytics_snapshots", "database": "d"
                },
                "model.p.current_orders": model_node("current_orders",
                    serde_json::json!({ "materialized": "table" }), serde_json::json!([]))
            },
            "sources": {}
        });
        let mut manifest = manifest;
        manifest["nodes"]["model.p.current_orders"]["depends_on"] =
            serde_json::json!({ "nodes": ["snapshot.p.orders_snap"], "macros": [] });
        let result = import_from_manifest_json(&manifest);
        assert!(
            result.failed.is_empty(),
            "{:?}",
            result.failed.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
        let snap = result
            .imported
            .iter()
            .find(|m| m.name == "orders_snap")
            .expect("snapshot imported");
        // dbt's resolved relation (after generate_schema_name), not the
        // configured `target_schema`, so the run continues dbt's table.
        assert_eq!(snap.config.target.schema, "analytics_snapshots");
        assert_eq!(snap.sql, "select * from \"d\".\"raw\".\"orders\"");
        let lowered = snap
            .config
            .strategy
            .snapshot_lowered()
            .expect("snapshot strategy");
        assert!(lowered.problems.is_empty(), "{:?}", lowered.problems);
        assert_eq!(lowered.spec.unique_key.len(), 2);
        assert_eq!(
            lowered.spec.hard_deletes,
            rocky_ir::SnapshotHardDeletes::Invalidate
        );
        assert_eq!(lowered.spec.meta_columns.valid_from, "start_at");
        assert_eq!(lowered.spec.meta_columns.scd_id, "dbt_scd_id");
        let consumer = result
            .imported
            .iter()
            .find(|m| m.name == "current_orders")
            .expect("consumer imported");
        assert_eq!(consumer.config.depends_on, vec!["orders_snap".to_string()]);
        assert_eq!(result.constructs_dropped, 0);
    }

    #[test]
    fn test_manifest_without_compiled_code_warns_stale() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.m": {
                "unique_id": "model.p.m", "name": "m", "resource_type": "model",
                "raw_code": "SELECT 1",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "table" },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| matches!(w.category, WarningCategory::StaleManifest)),
            "a manifest with no compiled SQL must warn loudly"
        );
    }

    #[test]
    fn manifest_raw_table_refuses_jinja_control_flow() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.orders": {
                "unique_id": "model.p.orders", "name": "orders", "resource_type": "model",
                "raw_code": "SELECT * FROM source_orders {% if false %} WHERE id > 100 {% endif %}",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "table" },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].name, "orders");
        assert!(result.failed[0].reason.contains(RAW_JINJA_CONTROL_REFUSED));
    }

    #[test]
    fn manifest_raw_fallback_refuses_append_incremental_guard() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.events": {
                "unique_id": "model.p.events", "name": "events", "resource_type": "model",
                "raw_code": "SELECT * FROM raw.events\n{% if target.name == 'prod' and is_incremental() %}\nWHERE event_time > (SELECT MAX(event_time) FROM {{ this }})\n{% endif %}",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "incremental" },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });

        let result = import_from_manifest_json(&manifest);
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].name, "events");
        assert!(result.failed[0].reason.contains("compiled_code"));
        assert!(
            result.failed[0]
                .reason
                .contains("dbt compile --full-refresh")
        );
    }

    #[test]
    fn manifest_raw_fallback_refuses_incremental_guard_for_keyed_merge() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": { "model.p.events": {
                "unique_id": "model.p.events", "name": "events", "resource_type": "model",
                "raw_code": "SELECT * FROM raw.events\n{% if is_incremental() %}\nWHERE event_time > (SELECT MAX(event_time) FROM {{ this }})\n{% endif %}",
                "depends_on": { "nodes": [], "macros": [] },
                "config": { "materialized": "incremental", "unique_key": "id" },
                "columns": {}, "tags": [], "schema": "s", "database": "d"
            }},
            "sources": {}
        });

        let result = import_from_manifest_json(&manifest);
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert_eq!(result.failed[0].name, "events");
        assert!(result.failed[0].reason.contains("compiled_code"));
    }

    #[test]
    fn test_manifest_materialized_view_maps_to_materialized_view() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.fct_revenue_mv": {
                    "unique_id": "model.p.fct_revenue_mv",
                    "name": "fct_revenue_mv",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "materialized_view" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::MaterializedView
        ));
    }

    #[test]
    fn test_manifest_microbatch_without_key_maps_to_full_refresh() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.events_daily": {
                    "unique_id": "model.p.events_daily",
                    "name": "events_daily",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "microbatch",
                        "event_time": "event_ts",
                        "batch_size": "day"
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.imported.len(), 1);
        // The default import mode has no keyed merge without a unique_key.
        // It rebuilds in full and warns. Time-interval mode remains available.
        assert!(
            matches!(
                result.imported[0].config.strategy,
                StrategyConfig::FullRefresh
            ),
            "expected FullRefresh, got {:?}",
            result.imported[0].config.strategy
        );
        assert!(
            result.structured_warnings.iter().any(|w| matches!(w,
                ImportDbtStructuredWarning::MicrobatchMapped { mapped_to, .. } if mapped_to == "full_refresh")),
            "the structured warning must say what it mapped to: {:?}",
            result.structured_warnings
        );
        assert!(
            result.warnings.iter().any(|w| w.message.contains("E037")),
            "the warning must say why there is no append mapping: {:?}",
            result.warnings
        );
    }

    #[test]
    fn test_manifest_microbatch_missing_event_time_warns_and_falls_back() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.broken_microbatch": {
                    "unique_id": "model.p.broken_microbatch",
                    "name": "broken_microbatch",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "microbatch", "batch_size": "hour" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::FullRefresh
        ));
        assert!(
            result.structured_warnings.iter().any(|w| matches!(
                w,
                ImportDbtStructuredWarning::MicrobatchMissingEventTime { model } if model == "broken_microbatch"
            )),
            "missing event_time must emit a structured warning"
        );
    }

    #[test]
    fn test_manifest_ephemeral_imports_as_ephemeral() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.eph": {
                    "unique_id": "model.p.eph",
                    "name": "eph",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "ephemeral" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::Ephemeral
        ));
        assert!(!result.structured_warnings.iter().any(|w| matches!(
            w,
            ImportDbtStructuredWarning::UnsupportedMaterialization { dbt_materialization, .. }
                if dbt_materialization == "ephemeral"
        )));
    }

    #[test]
    fn test_incremental_strategy_merge_regression() {
        // Pin the parse-bug fix: a manifest with
        // `incremental_strategy='merge'` + `unique_key=['user_id']` must
        // map to StrategyConfig::Merge, NOT
        // `Incremental { timestamp_column: "merge" }`.
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.fct_users": {
                    "unique_id": "model.p.fct_users",
                    "name": "fct_users",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "incremental",
                        "incremental_strategy": "merge",
                        "unique_key": ["user_id"]
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        match &result.imported[0].config.strategy {
            StrategyConfig::Merge {
                unique_key,
                update_columns: _,
            } => {
                assert_eq!(unique_key, &vec!["user_id".to_string()]);
            }
            other => panic!(
                "BUG REGRESSION: expected Merge {{ unique_key: ['user_id'] }}, got {other:?}"
            ),
        }
    }

    #[test]
    fn test_incremental_strategy_append_maps_to_full_refresh_with_a_warning() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.events_append": {
                    "unique_id": "model.p.events_append",
                    "name": "events_append",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "incremental",
                        "incremental_strategy": "append"
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        // #1990: an emitted `incremental` sidecar would fail `rocky compile`
        // with E037, so an append model rebuilds in full and says why.
        assert!(
            matches!(
                result.imported[0].config.strategy,
                StrategyConfig::FullRefresh
            ),
            "expected FullRefresh, got {:?}",
            result.imported[0].config.strategy
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.model == "events_append" && w.message.contains("E037")),
            "the append mapping must warn with the reason: {:?}",
            result.warnings
        );
        // Structured too, so MIGRATION-NOTES.md lists it under the models to
        // translate by hand, like the `ephemeral` fallback.
        assert!(
            result.structured_warnings.iter().any(|w| matches!(w,
                ImportDbtStructuredWarning::UnsupportedMaterialization { model, action, .. }
                    if model == "events_append" && action == "fell back to full_refresh")),
            "the append fallback must be a structured UnsupportedMaterialization: {:?}",
            result.structured_warnings
        );
    }

    /// #1990: dbt compiles `is_incremental()` as true against an existing
    /// table, so the compiled SQL keeps the delta filter. Imported as the
    /// `full_refresh` fallback, every run would replace the table with only
    /// the recent rows. The model is refused, not imported, and says why.
    #[test]
    // `>=` is not the standard filter Rocky converts (its filter is strict),
    // so this model still takes the #1990 refusal path.
    fn test_append_model_using_is_incremental_is_refused_not_full_refreshed() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.events_append": {
                    "unique_id": "model.p.events_append",
                    "name": "events_append",
                    "resource_type": "model",
                    "raw_code": "SELECT * FROM {{ ref('raw_events') }}\n{% if is_incremental() %}\nWHERE updated_at >= (SELECT MAX(updated_at) FROM {{ this }})\n{% endif %}",
                    "compiled_code": "SELECT * FROM raw_events\nWHERE updated_at > '2026-09-01 00:00:00'",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "incremental" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        assert!(
            result.imported.iter().all(|m| m.name != "events_append"),
            "a delta-filtered model must not be imported as full_refresh: {:?}",
            result
                .imported
                .iter()
                .map(|m| (&m.name, &m.config.strategy))
                .collect::<Vec<_>>()
        );
        let failure = result
            .failed
            .iter()
            .find(|f| f.name == "events_append")
            .expect("the model is reported as a failed import");
        assert!(
            failure.reason.contains("is_incremental()") && failure.reason.contains("merge"),
            "the refusal names the cause and the way out: {}",
            failure.reason
        );
        // Adding a key alone would import it as merge from the same compiled
        // delta SQL, whose first run loads only recent rows (#2059).
        assert!(
            failure
                .reason
                .contains("remove the `is_incremental()` filter")
                && !failure.reason.to_lowercase().contains("add a unique_key"),
            "the refusal must not steer into the keyed cold-start defect: {}",
            failure.reason
        );
        assert!(
            !result.warnings.iter().any(|w| w.model == "events_append"
                && w.message.contains("mapped to full_refresh")),
            "no warning may claim a full_refresh mapping for a refused model: {:?}",
            result.warnings
        );
    }

    /// The standard `>` filter converts from a manifest too: the SQL is
    /// rebuilt from `raw_code` with the placeholder, so the first run (and a
    /// full refresh) loads every row instead of the compiled delta.
    #[test]
    fn manifest_standard_incremental_filter_converts_from_raw_code() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.events_append": {
                    "unique_id": "model.p.events_append",
                    "name": "events_append",
                    "resource_type": "model",
                    "raw_code": "SELECT * FROM {{ ref('raw_events') }}\n{% if is_incremental() %}\nWHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})\n{% endif %}",
                    "compiled_code": "SELECT * FROM raw_events\nWHERE updated_at > '2026-09-01 00:00:00'",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "incremental" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        let model = result
            .imported
            .iter()
            .find(|m| m.name == "events_append")
            .expect("imported");
        assert!(
            model.sql.contains("WHERE @incremental_filter"),
            "{}",
            model.sql
        );
        assert!(
            !model.sql.contains("2026-09-01"),
            "not the compiled delta: {}",
            model.sql
        );
        assert!(
            matches!(
                &model.config.strategy,
                StrategyConfig::Incremental { timestamp_column: Some(ts), unique_key, .. }
                    if ts == "updated_at" && unique_key.is_empty()
            ),
            "{:?}",
            model.config.strategy
        );
    }

    /// A key does not make a delta-filtered compiled body safe on first run.
    #[test]
    fn test_keyed_incremental_without_compile_evidence_is_refused() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.events_keyed": {
                    "unique_id": "model.p.events_keyed",
                    "name": "events_keyed",
                    "resource_type": "model",
                    "raw_code": "SELECT * FROM {{ ref('raw_events') }}\n{% if is_incremental() %}\nWHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})\n{% endif %}",
                    "compiled_code": "SELECT * FROM raw_events\nWHERE updated_at > '2026-09-01 00:00:00'",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "incremental", "unique_key": "id" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        assert!(result.imported.iter().all(|m| m.name != "events_keyed"));
        let failure = result
            .failed
            .iter()
            .find(|f| f.name == "events_keyed")
            .expect("a keyed model without matching evidence is refused");
        assert!(
            failure.reason.contains("dbt compile --full-refresh"),
            "refusal names the remedy: {}",
            failure.reason
        );
    }

    #[test]
    fn real_dbt_compile_pair_guards_every_incremental_strategy() {
        let fixtures =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/dbt_incremental_compile");
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };

        let full = dbt_manifest::parse_manifest(&fixtures.join("full_refresh/manifest.json"))
            .expect("real full-refresh manifest parses");
        let accepted = import_from_manifest(&full, &target, false, MicrobatchMode::Merge);
        assert_eq!(accepted.failed.len(), 1, "{:?}", accepted.failed);
        assert_eq!(accepted.failed[0].name, "orders_pinned");
        assert!(accepted.failed[0].reason.contains("full_refresh=false"));
        assert!(
            accepted.failed[0]
                .reason
                .contains("dbt compile --full-refresh")
        );
        for name in ["orders_inc", "orders_macro", "orders_nokey"] {
            let model = accepted.imported.iter().find(|m| m.name == name).unwrap();
            assert!(
                !model.sql.contains("where updated_at >"),
                "{}: {}",
                name,
                model.sql
            );
            // The keyed model with the standard `is_incremental()` filter
            // converts to Rocky `incremental` (WP6); the macro-hidden filter
            // keeps the keyed merge mapping from the full-refresh SQL.
            match name {
                "orders_inc" => assert!(
                    matches!(
                        &model.config.strategy,
                        StrategyConfig::Incremental { timestamp_column: Some(ts), unique_key, .. }
                            if ts == "updated_at" && unique_key == &vec!["id".to_string()]
                    ),
                    "{:?}",
                    model.config.strategy
                ),
                // `delete+insert` keeps its own mapping: a keyed MERGE does
                // not delete the target rows of a non-unique key.
                "orders_nokey" => assert!(matches!(
                    model.config.strategy,
                    StrategyConfig::DeleteInsert { .. }
                )),
                _ => assert!(matches!(
                    model.config.strategy,
                    StrategyConfig::Merge { .. }
                )),
            }
        }
        assert_eq!(accepted.imported.len(), 3);

        let plain = dbt_manifest::parse_manifest(&fixtures.join("plain/manifest.json"))
            .expect("real plain manifest parses");
        let refused = import_from_manifest(&plain, &target, false, MicrobatchMode::Merge);
        assert!(refused.imported.is_empty());
        assert_eq!(refused.failed.len(), 4, "{:?}", refused.failed);
        for name in [
            "orders_inc",
            "orders_pinned",
            "orders_macro",
            "orders_nokey",
        ] {
            let failure = refused.failed.iter().find(|f| f.name == name).unwrap();
            assert!(failure.reason.contains("dbt compile --full-refresh"));
            if name == "orders_pinned" {
                assert!(failure.reason.contains("full_refresh=false"));
            }
        }
    }

    #[test]
    fn selective_full_refresh_compile_imports_only_successful_compiled_node() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/dbt_incremental_compile/select_orders_inc");
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let manifest = dbt_manifest::parse_manifest(&fixtures.join("manifest.json")).unwrap();
        let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);
        assert_eq!(
            result
                .imported
                .iter()
                .map(|model| model.name.as_str())
                .collect::<Vec<_>>(),
            vec!["orders_inc"]
        );
        assert!(!result.imported[0].sql.contains("where updated_at >"));
        assert_eq!(result.failed.len(), 3, "{:?}", result.failed);
        for name in ["orders_macro", "orders_nokey", "orders_pinned"] {
            let failure = result
                .failed
                .iter()
                .find(|failure| failure.name == name)
                .unwrap();
            if name == "orders_pinned" {
                assert!(failure.reason.contains("full_refresh=false"));
            } else {
                assert!(failure.reason.contains("dbt compile --full-refresh"));
                assert!(failure.reason.contains("--select"));
            }
        }
    }

    #[test]
    fn raw_import_refuses_inline_and_inherited_incremental_models() {
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        for (project_config, model_path, model_config) in [
            (
                "    +materialized: view\n",
                "models/orders.sql",
                "{{ config(materialized='incremental') }}",
            ),
            (
                "    +materialized: incremental\n",
                "models/orders.sql",
                "{{ config(tags=['daily']) }}",
            ),
            (
                "    +materialized: view\n    marts:\n      +materialized: incremental\n",
                "models/marts/orders.sql",
                "",
            ),
            (
                "    +materialized: view\n    marts:\n      +materialized: view\n      orders:\n        +materialized: incremental\n",
                "models/marts/orders.sql",
                "",
            ),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            std::fs::create_dir_all(dir.path().join(model_path).parent().unwrap()).unwrap();
            std::fs::write(
                dir.path().join("dbt_project.yml"),
                format!("name: p\nmodels:\n  p:\n{project_config}"),
            )
            .unwrap();
            std::fs::write(
                dir.path().join(model_path),
                format!(
                    "{model_config}\nselect * from orders_source\n{{% if delta_mode() %}}\nwhere updated_at > '2026-01-01'\n{{% endif %}}"
                ),
            )
            .unwrap();
            let result = import_dbt_project(dir.path(), &target).unwrap();
            assert!(result.imported.is_empty());
            assert_eq!(result.failed.len(), 1, "{:?}", result.failed);
            assert_eq!(result.failed[0].name, "orders");
            assert!(
                result.failed[0]
                    .reason
                    .contains("raw import cannot evaluate Jinja control flow")
            );
            assert!(
                result.failed[0]
                    .reason
                    .contains("dbt compile --full-refresh")
            );
        }

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nmodels:\n  p:\n    +materialized: incremental\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/orders.sql"),
            "{{ config(materialized='table') }}\nselect 1 as id",
        )
        .unwrap();
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.imported.len(), 1);
        assert!(result.failed.is_empty());

        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("models/properties.yml"),
            "models:\n  - name: orders\n    config:\n      materialized: incremental\n",
        )
        .unwrap();
        std::fs::write(
            dir.path().join("models/orders.sql"),
            "select * from orders_source\n{% if delta_mode() %}\nwhere updated_at > '2026-01-01'\n{% endif %}",
        )
        .unwrap();
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert!(
            result.failed[0]
                .reason
                .contains("dbt compile --full-refresh")
        );

        std::fs::write(
            dir.path().join("models/orders.sql"),
            "{{ config(materialized='table') }}\nselect 1 as id",
        )
        .unwrap();
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.imported.len(), 1);
        assert!(result.failed.is_empty());
    }

    #[test]
    fn raw_import_refuses_jinja_control_flow_before_emitting_sql() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        for (label, tag) in [
            ("if", "{% if delta_mode() %}where id > 10{% endif %}"),
            (
                "if_trim",
                "{%- if delta_mode() -%}where id > 10{%- endif -%}",
            ),
            ("for", "{% for c in columns %}{{ c }}{% endfor %}"),
            ("macro", "{% macro delta() %}where id > 10{% endmacro %}"),
            ("call", "{% call delta() %}where id > 10{% endcall %}"),
            (
                "wrapped_incremental",
                "{% macro delta() %}{{ is_incremental() }}{% endmacro %}{% if delta() %}where id > 10{% endif %}",
            ),
        ] {
            let dir = tempfile::TempDir::new().unwrap();
            std::fs::create_dir(dir.path().join("models")).unwrap();
            std::fs::write(
                dir.path().join("models/orders.sql"),
                format!("select * from source {tag}"),
            )
            .unwrap();
            let result = import_dbt_project(dir.path(), &target).unwrap();
            assert!(result.imported.is_empty(), "{label}");
            assert_eq!(result.failed.len(), 1, "{label}");
            assert!(
                result.failed[0]
                    .reason
                    .contains("raw import cannot evaluate Jinja control flow"),
                "{label}"
            );
            assert!(
                result.failed[0]
                    .reason
                    .contains("dbt compile --full-refresh"),
                "{label}"
            );
        }
    }

    #[test]
    fn raw_import_resolves_whitespace_and_last_inline_config() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        let sql = dir.path().join("models/orders.sql");
        std::fs::write(
            &sql,
            "{{- config(materialized='incremental') -}}\nselect 1 as id",
        )
        .unwrap();
        let refused = import_dbt_project(dir.path(), &target).unwrap();
        assert!(refused.imported.is_empty());
        assert!(refused.failed[0].reason.contains("effectively incremental"));

        std::fs::write(&sql, "{{ config(materialized='incremental') }}\n{{- config(materialized='table') -}}\nselect 1 as id").unwrap();
        let accepted = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(accepted.imported.len(), 1, "{:?}", accepted.failed);
        assert!(accepted.failed.is_empty());
        assert!(matches!(
            accepted.imported[0].config.strategy,
            StrategyConfig::FullRefresh
        ));
        assert_eq!(accepted.imported[0].sql.trim(), "select 1 as id");

        std::fs::write(&sql, "{{ config(materialized='table') }}\n{{ config(materialized='incremental') }}\nselect 1 as id").unwrap();
        let refused = import_dbt_project(dir.path(), &target).unwrap();
        assert!(refused.imported.is_empty());
        assert!(refused.failed[0].reason.contains("effectively incremental"));
    }

    #[test]
    fn raw_import_refuses_unresolved_config_expression() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        for expression in [
            "materialized=var('mode')",
            "**model_config",
            "schema=target.schema",
            "tags=['daily', var('tag')]",
            "meta={'owner': target.name}",
            "schema='safe' + var('suffix')",
            "schema='safe' + 'suffix'",
        ] {
            std::fs::write(
                dir.path().join("models/orders.sql"),
                format!("{{{{ config({expression}) }}}}\nselect 1 as id"),
            )
            .unwrap();
            let result = import_dbt_project(dir.path(), &target).unwrap();
            assert!(result.imported.is_empty(), "{expression}");
            assert!(
                result.failed[0]
                    .reason
                    .contains("cannot resolve a dbt config expression"),
                "{expression}"
            );
        }

        std::fs::write(
            dir.path().join("models/orders.sql"),
            "{{ config(materialized='incremental' }}\nselect 1 as id",
        )
        .unwrap();
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.imported.is_empty());
        assert!(
            result.failed[0]
                .reason
                .contains("cannot resolve a dbt config expression")
        );

        std::fs::write(
            dir.path().join("models/orders.sql"),
            "{{ config(alias='orders', tags=['daily)'], meta={'owner': 'team'}) }}\nselect 1 as id",
        )
        .unwrap();
        let accepted = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(accepted.imported.len(), 1, "{:?}", accepted.failed);
        assert_eq!(accepted.imported[0].sql, "select 1 as id");
        assert_eq!(accepted.imported[0].config.target.table, "orders");
    }

    #[test]
    fn raw_import_applies_root_model_default() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nmodels:\n  +materialized: incremental\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("models/orders.sql"), "select 1 as id").unwrap();
        let refused = import_dbt_project(dir.path(), &target).unwrap();
        assert!(refused.imported.is_empty());
        assert!(refused.failed[0].reason.contains("effectively incremental"));

        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nmodels:\n  +materialized: incremental\n  p:\n    +materialized: table\n",
        )
        .unwrap();
        let accepted = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(accepted.imported.len(), 1, "{:?}", accepted.failed);

        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nmodels:\n  +materialized: \"{{ var('mode') }}\"\n",
        )
        .unwrap();
        let refused = import_dbt_project(dir.path(), &target).unwrap();
        assert!(refused.imported.is_empty());
        assert!(
            refused.failed[0]
                .reason
                .contains("cannot resolve a dbt config expression")
        );
    }

    /// Raw import takes a versioned model whose versions are plain
    /// `<name>_v<N>.sql` files, and maps `ref(v=N)`, access and group.
    #[test]
    fn raw_import_takes_simple_versions_and_governance() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(
            dir.path().join("models/properties.yml"),
            "groups:\n  - name: finance\n    owner:\n      email: fin@example.com\nmodels:\n  - name: orders\n    access: public\n    group: finance\n    latest_version: 2\n    versions:\n      - v: 1\n        deprecation_date: 2026-12-31\n      - v: 2\n",
        )
        .unwrap();
        std::fs::write(dir.path().join("models/orders_v1.sql"), "select 1 as id").unwrap();
        std::fs::write(dir.path().join("models/orders_v2.sql"), "select 1 as id").unwrap();
        std::fs::write(
            dir.path().join("models/reader.sql"),
            "select id from {{ ref('orders', v=1) }}",
        )
        .unwrap();
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        let v1 = result
            .imported
            .iter()
            .find(|m| m.name == "orders_v1")
            .unwrap();
        let info = v1.config.governance.version.as_ref().unwrap();
        assert_eq!((info.version, info.latest_version), (Some(1), 2));
        assert!(info.deprecation_date.is_some());
        assert_eq!(
            v1.config.governance.access_group.as_deref(),
            Some("finance")
        );
        let reader = result.imported.iter().find(|m| m.name == "reader").unwrap();
        assert_eq!(reader.sql, "select id from orders_v1");
    }

    #[test]
    fn raw_import_refuses_versioned_properties() {
        let target = TargetConfig {
            catalog: "w".into(),
            schema: "s".into(),
            table: String::new(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::create_dir(dir.path().join("models")).unwrap();
        std::fs::write(dir.path().join("models/properties.yml"), "models:\n  - name: orders\n    versions:\n      - v: 1\n        config:\n          materialized: incremental\n      - v: 2\n        defined_in: orders_archive\n").unwrap();
        for name in ["orders", "orders_v1", "orders_archive"] {
            std::fs::write(
                dir.path().join(format!("models/{name}.sql")),
                "select 1 as id",
            )
            .unwrap();
        }
        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 3);
        assert!(
            result
                .failed
                .iter()
                .all(|failure| failure.reason.contains("versioned model properties"))
        );
    }

    #[test]
    fn incremental_requires_both_success_result_and_compiled_code() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/dbt_incremental_compile/select_orders_inc");
        let dir = tempfile::TempDir::new().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let results_path = dir.path().join("run_results.json");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join("manifest.json")).unwrap())
                .unwrap();
        let results: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join("run_results.json")).unwrap())
                .unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        for (manifest_value, results_value) in [
            (manifest.clone(), {
                let mut value = results.clone();
                value["results"][0]["status"] = serde_json::json!("error");
                value
            }),
            (
                {
                    let mut value = manifest.clone();
                    value["nodes"]["model.inc_probe.orders_inc"]
                        .as_object_mut()
                        .unwrap()
                        .remove("compiled_code");
                    value
                },
                results.clone(),
            ),
        ] {
            std::fs::write(&manifest_path, manifest_value.to_string()).unwrap();
            std::fs::write(&results_path, results_value.to_string()).unwrap();
            let parsed = dbt_manifest::parse_manifest(&manifest_path).unwrap();
            let imported = import_from_manifest(&parsed, &target, false, MicrobatchMode::Merge);
            assert!(imported.imported.is_empty());
            let failure = imported
                .failed
                .iter()
                .find(|f| f.name == "orders_inc")
                .unwrap();
            assert!(failure.reason.contains("dbt compile --full-refresh"));
        }
    }

    #[test]
    fn incremental_accepts_compiled_code_when_compile_writes_no_results() {
        // dbt 2 writes `results: []` for `dbt compile --full-refresh`. The
        // matching invocation and the model's `compiled_code` are the evidence.
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/dbt_incremental_compile/select_orders_inc");
        let dir = tempfile::TempDir::new().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let results_path = dir.path().join("run_results.json");
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join("manifest.json")).unwrap())
                .unwrap();
        let mut results: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join("run_results.json")).unwrap())
                .unwrap();
        results["results"] = serde_json::json!([]);
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        std::fs::write(&manifest_path, manifest.to_string()).unwrap();
        std::fs::write(&results_path, results.to_string()).unwrap();
        let parsed = dbt_manifest::parse_manifest(&manifest_path).unwrap();
        let imported = import_from_manifest(&parsed, &target, false, MicrobatchMode::Merge);
        assert!(
            imported.imported.iter().any(|m| m.name == "orders_inc"),
            "{:?}",
            imported.failed
        );

        // A model dbt did not compile has no `compiled_code`: still refused.
        let mut uncompiled = manifest.clone();
        uncompiled["nodes"]["model.inc_probe.orders_inc"]
            .as_object_mut()
            .unwrap()
            .remove("compiled_code");
        std::fs::write(&manifest_path, uncompiled.to_string()).unwrap();
        let parsed = dbt_manifest::parse_manifest(&manifest_path).unwrap();
        let imported = import_from_manifest(&parsed, &target, false, MicrobatchMode::Merge);
        assert!(imported.imported.is_empty());
        assert!(imported.failed.iter().any(|f| f.name == "orders_inc"));

        // Without `full_refresh` in the args, empty results prove nothing.
        results["args"]["full_refresh"] = serde_json::json!(false);
        std::fs::write(&manifest_path, manifest.to_string()).unwrap();
        std::fs::write(&results_path, results.to_string()).unwrap();
        let parsed = dbt_manifest::parse_manifest(&manifest_path).unwrap();
        let imported = import_from_manifest(&parsed, &target, false, MicrobatchMode::Merge);
        assert!(imported.imported.is_empty());
    }

    #[test]
    fn incremental_rejects_missing_mismatched_and_false_evidence() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/dbt_incremental_compile/full_refresh");
        let dir = tempfile::TempDir::new().unwrap();
        let manifest_path = dir.path().join("manifest.json");
        let results_path = dir.path().join("run_results.json");
        std::fs::copy(fixtures.join("manifest.json"), &manifest_path).unwrap();
        let valid: serde_json::Value =
            serde_json::from_slice(&std::fs::read(fixtures.join("run_results.json")).unwrap())
                .unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };

        for evidence in [
            None,
            Some(
                serde_json::json!({"metadata": {"invocation_id": "other"}, "args": {"full_refresh": true}}),
            ),
            Some(
                serde_json::json!({"metadata": {"invocation_id": valid["metadata"]["invocation_id"]}, "args": {"full_refresh": false}}),
            ),
            Some(
                serde_json::json!({"metadata": {"invocation_id": valid["metadata"]["invocation_id"]}, "args": {}}),
            ),
            Some(serde_json::json!({"broken": true})),
        ] {
            if let Some(evidence) = evidence {
                std::fs::write(&results_path, evidence.to_string()).unwrap();
            } else if results_path.exists() {
                std::fs::remove_file(&results_path).unwrap();
            }
            let manifest = dbt_manifest::parse_manifest(&manifest_path).unwrap();
            let result = import_from_manifest(&manifest, &target, false, MicrobatchMode::Merge);
            assert!(result.imported.is_empty());
            assert_eq!(result.failed.len(), 4, "{:?}", result.failed);
            assert!(
                result
                    .failed
                    .iter()
                    .all(|f| f.reason.contains("dbt compile --full-refresh"))
            );
        }
    }

    #[test]
    fn test_incremental_strategy_delete_insert_maps_to_delete_insert() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.partitioned": {
                    "unique_id": "model.p.partitioned",
                    "name": "partitioned",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "incremental",
                        "incremental_strategy": "delete+insert",
                        "unique_key": ["dt"],
                        "partition_by": ["dt"]
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        match &result.imported[0].config.strategy {
            StrategyConfig::DeleteInsert { partition_by } => {
                assert_eq!(partition_by, &vec!["dt".to_string()]);
            }
            other => panic!("expected DeleteInsert, got {other:?}"),
        }
    }

    // ---------------------------------------------------------------------
    // Wave 2: structured warning tests
    // ---------------------------------------------------------------------

    #[test]
    fn test_dropped_databricks_tags_warning() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.dim_users": {
                    "unique_id": "model.p.dim_users",
                    "name": "dim_users",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "table",
                        "databricks_tags": { "owner": "data-team", "pii": "true" }
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        let found = result
            .structured_warnings
            .iter()
            .find_map(|w| match w {
                ImportDbtStructuredWarning::DroppedDatabricksTags { model, tags }
                    if model == "dim_users" =>
                {
                    Some(tags.clone())
                }
                _ => None,
            })
            .expect("expected DroppedDatabricksTags warning");
        assert_eq!(found.get("owner").map(String::as_str), Some("data-team"));
        assert_eq!(found.get("pii").map(String::as_str), Some("true"));
    }

    #[test]
    fn test_dropped_hook_warning() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.fct_orders": {
                    "unique_id": "model.p.fct_orders",
                    "name": "fct_orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "table",
                        "pre_hook": "ANALYZE TABLE foo COMPUTE STATISTICS",
                        "post_hook": ["GRANT SELECT ON {{ this }} TO ROLE analyst"]
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        let pre = result.structured_warnings.iter().find(|w| {
            matches!(
                w,
                ImportDbtStructuredWarning::DroppedHook { hook_kind, sql, .. }
                    if *hook_kind == HookKind::Pre && sql.contains("ANALYZE TABLE")
            )
        });
        assert!(pre.is_some(), "expected pre_hook structured warning");
        let post = result.structured_warnings.iter().find(|w| {
            matches!(
                w,
                ImportDbtStructuredWarning::DroppedHook { hook_kind, sql, .. }
                    if *hook_kind == HookKind::Post && sql.contains("GRANT SELECT")
            )
        });
        assert!(post.is_some(), "expected post_hook structured warning");
    }

    #[test]
    fn test_dropped_on_schema_change_warning() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.dim_x": {
                    "unique_id": "model.p.dim_x",
                    "name": "dim_x",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": {
                        "materialized": "incremental",
                        "unique_key": "id",
                        "on_schema_change": "append_new_columns"
                    },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json_with_evidence(&manifest, MicrobatchMode::Merge);
        let found = result.structured_warnings.iter().find_map(|w| match w {
            ImportDbtStructuredWarning::DroppedOnSchemaChange {
                dbt_value,
                rocky_equivalent,
                model,
            } if model == "dim_x" => Some((dbt_value.clone(), rocky_equivalent.clone())),
            _ => None,
        });
        let (dbt_value, rocky_equivalent) = found.expect("expected DroppedOnSchemaChange warning");
        assert_eq!(dbt_value, "append_new_columns");
        assert!(rocky_equivalent.contains("evolve"));
    }

    #[test]
    fn test_unresolvable_macro_warning() {
        // Synthetic: a compiled_sql with a {{ custom_macro(...) }} call
        // that dbt's compile step couldn't inline (i.e. the macro is
        // defined out-of-tree). The importer surfaces this as an
        // UnresolvableMacro structured warning with the call-site line.
        let compiled_sql = "SELECT id,\n  {{ custom_helper('a', 'b') }} AS computed\nFROM raw.t";
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.uses_macro": {
                    "unique_id": "model.p.uses_macro",
                    "name": "uses_macro",
                    "resource_type": "model",
                    "compiled_code": compiled_sql,
                    "raw_code": compiled_sql,
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {}
        });
        let result = import_from_manifest_json(&manifest);
        let found = result.structured_warnings.iter().find_map(|w| match w {
            ImportDbtStructuredWarning::UnresolvableMacro {
                model,
                macro_name,
                first_call_site_line,
            } if model == "uses_macro" => Some((macro_name.clone(), *first_call_site_line)),
            _ => None,
        });
        let (macro_name, line) = found.expect("expected UnresolvableMacro warning");
        assert_eq!(macro_name, "custom_helper");
        assert_eq!(line, 2, "macro is on line 2 of the compiled SQL");
    }

    // ---------------------------------------------------------------------
    // Unit-test bridge: manifest.unit_tests → Rocky `[[test]]` sidecar
    // ---------------------------------------------------------------------

    #[test]
    fn test_strip_ref_wrapper_handles_ref_source_and_bare() {
        assert_eq!(strip_ref_wrapper("ref('orders')"), "orders");
        assert_eq!(strip_ref_wrapper(" ref('orders') "), "orders");
        assert_eq!(strip_ref_wrapper("ref(\"orders\")"), "orders");
        assert_eq!(strip_ref_wrapper("source('raw', 'orders')"), "raw.orders");
        assert_eq!(
            strip_ref_wrapper("source(\"raw\",\"orders\")"),
            "raw.orders"
        );
        // Already-bare identifiers come through untouched.
        assert_eq!(strip_ref_wrapper("orders"), "orders");
        // Quoted bare identifiers shed the quotes.
        assert_eq!(strip_ref_wrapper("'orders'"), "orders");
        // Source with too many args refuses to guess.
        assert_eq!(
            strip_ref_wrapper("source('a','b','c')"),
            "source('a','b','c')"
        );
    }

    #[test]
    fn test_apply_dbt_unit_tests_attaches_to_imported_model() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.stg_orders": {
                    "unique_id": "model.p.stg_orders",
                    "name": "stg_orders",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                "unit_test.p.stg_orders.stamps_order_key": {
                    "unique_id": "unit_test.p.stg_orders.stamps_order_key",
                    "name": "stamps_order_key",
                    "model": "stg_orders",
                    "given": [
                        {
                            "input": "ref('int_orders')",
                            "rows": [{ "order_id": 1001, "customer_id": 50 }]
                        }
                    ],
                    "expect": {
                        "rows": [{ "order_key": "abc", "order_id": 1001 }],
                        "format": "dict"
                    },
                    "description": "order key",
                    "tags": []
                }
            }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 1);
        assert_eq!(result.unit_tests_skipped, 0);

        let imported = result
            .imported
            .iter()
            .find(|m| m.name == "stg_orders")
            .expect("model imported");
        assert_eq!(imported.unit_tests.len(), 1);
        let ut = &imported.unit_tests[0];
        assert_eq!(ut.name, "stamps_order_key");
        assert_eq!(ut.description.as_deref(), Some("order key"));
        assert_eq!(ut.given.len(), 1);
        assert_eq!(ut.given[0].model_ref, "int_orders");
        assert_eq!(ut.given[0].rows.len(), 1);
        assert_eq!(ut.expect.rows.len(), 1);
        assert!(!ut.expect.ordered);
    }

    #[test]
    fn test_apply_dbt_unit_tests_orphan_warns_and_skips() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {},
            "sources": {},
            "unit_tests": {
                "unit_test.p.missing.t": {
                    "unique_id": "unit_test.p.missing.t",
                    "name": "t",
                    "model": "missing_model",
                    "given": [],
                    "expect": { "rows": [], "format": "dict" },
                    "tags": []
                }
            }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 0);
        assert_eq!(result.unit_tests_skipped, 1);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::OrphanUnitTest
                    && w.message.contains("missing_model"))
        );
    }

    #[test]
    fn test_apply_dbt_unit_tests_non_dict_expect_format_skips() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                "unit_test.p.m.csv_case": {
                    "unique_id": "unit_test.p.m.csv_case",
                    "name": "csv_case",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [] }],
                    "expect": { "rows": [], "format": "csv" },
                    "tags": []
                }
            }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 0);
        assert_eq!(result.unit_tests_skipped, 1);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::UnsupportedUnitTestFormat),
        );
        // Nothing pushed onto the imported model.
        assert!(result.imported.iter().all(|m| m.unit_tests.is_empty()));
    }

    #[test]
    fn test_apply_dbt_unit_tests_non_dict_given_format_skips() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                "unit_test.p.m.csv_given": {
                    "unique_id": "unit_test.p.m.csv_given",
                    "name": "csv_given",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [], "format": "csv" }],
                    "expect": { "rows": [], "format": "dict" },
                    "tags": []
                }
            }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 0);
        assert_eq!(result.unit_tests_skipped, 1);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::UnsupportedUnitTestFormat),
        );
    }

    #[test]
    fn test_apply_dbt_unit_tests_treats_missing_format_as_dict() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                "unit_test.p.m.no_format": {
                    "unique_id": "unit_test.p.m.no_format",
                    "name": "no_format",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [{ "id": 1 }] }],
                    "expect": { "rows": [{ "id": 1 }] },
                    "tags": []
                }
            }
        });
        let result = import_from_manifest_json(&manifest);
        assert_eq!(result.unit_tests_converted, 1);
        assert_eq!(result.unit_tests_skipped, 0);
        let imported = result.imported.iter().find(|m| m.name == "m").unwrap();
        assert_eq!(imported.unit_tests.len(), 1);
    }

    /// FR-045: a `null` fixture/expectation cell (TOML has no null type) is no
    /// longer dropped. The null key is omitted on emit and the run-side builder
    /// materializes it back to SQL NULL, so the test ports as CONVERTED — not
    /// UnserializableUnitTest. Covers the mixed/union case (a column null in
    /// some rows, set in others) and asserts the emitted sidecar TOML is clean.
    #[test]
    fn test_apply_dbt_unit_tests_null_fixture_value_converts() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                // Null cells in BOTH given and expect, plus the mixed/union
                // case: `note` is set in row 1 but null in row 2.
                "unit_test.p.m.has_null": {
                    "unique_id": "unit_test.p.m.has_null",
                    "name": "has_null",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [
                        { "id": 1, "note": "x" },
                        { "id": 2, "note": null }
                    ] }],
                    "expect": { "rows": [{ "id": 1, "note": null }], "format": "dict" },
                    "tags": []
                }
            }
        });

        let result = import_from_manifest_json(&manifest);

        let imported = result
            .imported
            .iter()
            .find(|m| m.name == "m")
            .expect("model imported");

        // Counters: seen and CONVERTED, none skipped.
        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 1);
        assert_eq!(result.unit_tests_skipped, 0);

        // No UnserializableUnitTest warning — the null cell is no longer fatal.
        assert!(
            !result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::UnserializableUnitTest),
            "null fixture cells must not trigger UnserializableUnitTest"
        );

        assert_eq!(imported.unit_tests.len(), 1);
        assert_eq!(imported.unit_tests[0].name, "has_null");

        // The emitted sidecar TOML serializes cleanly with the null key omitted.
        let emitted = crate::import::emit::render_unit_tests("m", &imported.unit_tests);
        assert!(
            emitted.contains("[[test]]") && emitted.contains("has_null"),
            "has_null test must serialize to a [[test]] block:\n{emitted}"
        );
        // The omitted null cell renders the row WITHOUT a `note = ` for that
        // entry — re-parsing the sidecar must succeed (no stray null literal).
        let parsed: toml::Value =
            toml::from_str(&emitted).expect("emitted unit-test TOML must re-parse");
        assert!(
            parsed.get("test").is_some(),
            "parsed sidecar carries [[test]]"
        );
    }

    /// A test still genuinely unrepresentable after null-stripping (a `null`
    /// nested inside an array, which a key-strip can't reach) is still dropped
    /// with an UnserializableUnitTest warning, and never aborts the import.
    #[test]
    fn test_apply_dbt_unit_tests_nested_null_array_still_skips() {
        let manifest = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                // A null INSIDE an array — key-strip can't reach it, so TOML
                // still can't express the row.
                "unit_test.p.m.nested": {
                    "unique_id": "unit_test.p.m.nested",
                    "name": "nested",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [{ "id": 1, "tags": [null] }] }],
                    "expect": { "rows": [{ "id": 1 }], "format": "dict" },
                    "tags": []
                },
                // Valid sibling: must still round-trip.
                "unit_test.p.m.clean": {
                    "unique_id": "unit_test.p.m.clean",
                    "name": "clean",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [{ "id": 2 }] }],
                    "expect": { "rows": [{ "id": 2 }], "format": "dict" },
                    "tags": []
                }
            }
        });

        let result = import_from_manifest_json(&manifest);
        let imported = result
            .imported
            .iter()
            .find(|m| m.name == "m")
            .expect("model still imported despite the unserializable unit test");

        assert_eq!(result.unit_tests_found, 2);
        assert_eq!(result.unit_tests_converted, 1);
        assert_eq!(result.unit_tests_skipped, 1);
        assert_eq!(imported.unit_tests.len(), 1);
        assert_eq!(imported.unit_tests[0].name, "clean");
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.category == WarningCategory::UnserializableUnitTest
                    && w.message.contains("nested")),
            "expected an UnserializableUnitTest warning naming the dropped test"
        );
    }

    /// `--skip-unit-tests` counts every unit test as skipped, converts none,
    /// and never aborts — even when a null-bearing fixture is present.
    #[test]
    fn test_skip_unit_tests_flag_skips_everything() {
        let manifest_json = serde_json::json!({
            "metadata": { "project_name": "p" },
            "nodes": {
                "model.p.m": {
                    "unique_id": "model.p.m",
                    "name": "m",
                    "resource_type": "model",
                    "compiled_code": "SELECT 1",
                    "raw_code": "SELECT 1",
                    "depends_on": { "nodes": [], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "s", "database": "d"
                }
            },
            "sources": {},
            "unit_tests": {
                "unit_test.p.m.clean": {
                    "unique_id": "unit_test.p.m.clean",
                    "name": "clean",
                    "model": "m",
                    "given": [{ "input": "ref('u')", "rows": [{ "id": 2 }] }],
                    "expect": { "rows": [{ "id": 2 }], "format": "dict" },
                    "tags": []
                }
            }
        });
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();
        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };

        let result = import_from_manifest(
            &manifest,
            &target,
            /* skip_unit_tests */ true,
            MicrobatchMode::Merge,
        );

        assert_eq!(result.unit_tests_found, 1);
        assert_eq!(result.unit_tests_converted, 0);
        assert_eq!(result.unit_tests_skipped, 1);
        let imported = result.imported.iter().find(|m| m.name == "m").unwrap();
        assert!(imported.unit_tests.is_empty());
    }

    // --- L1: model-path traversal rejection ---

    #[test]
    fn safe_join_rejects_parent_dir_component() {
        let base = std::path::Path::new("/tmp/project");
        let err = safe_join_under(base, std::path::Path::new("../../etc"))
            .expect_err("a `..` path must be rejected");
        assert!(err.contains(".."), "error should mention the escape: {err}");
    }

    #[test]
    fn safe_join_rejects_absolute_path() {
        let base = std::path::Path::new("/tmp/project");
        let err = safe_join_under(base, std::path::Path::new("/etc/passwd"))
            .expect_err("an absolute path must be rejected");
        assert!(err.contains("absolute"), "error should explain: {err}");
    }

    #[test]
    fn safe_join_allows_normal_relative_path() {
        let base = std::path::Path::new("/tmp/project");
        let joined = safe_join_under(base, std::path::Path::new("models"))
            .expect("a normal relative path is allowed");
        assert_eq!(joined, std::path::Path::new("/tmp/project/models"));
    }

    /// A `dbt_project.yml` declaring a traversal `model-paths` must be rejected
    /// by the full import entry point, not silently read.
    #[test]
    fn import_rejects_traversal_model_path() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: evil\nmodel-paths: [\"../../etc\"]\n",
        )
        .unwrap();
        let target = TargetConfig {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: String::new(),
        };
        let err = match import_dbt_project(dir.path(), &target) {
            Err(e) => e,
            Ok(_) => panic!("traversal model path must abort the import"),
        };
        assert!(
            err.contains("..") || err.contains("escape"),
            "expected a traversal-rejection error, got: {err}"
        );
    }

    // --- M1: symlink-cycle recursion guard ---

    /// A directory symlink cycle (`loop -> ..`) inside the models tree must not
    /// drive the importer into unbounded recursion — the symlink is skipped
    /// (and the depth cap is a backstop), so the import terminates.
    #[cfg(unix)]
    #[test]
    fn import_terminates_on_directory_symlink_cycle() {
        let dir = tempfile::TempDir::new().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir(&models).unwrap();
        std::fs::write(models.join("ok.sql"), "SELECT 1 AS x").unwrap();
        // models/loop -> models (a cycle back into the tree being walked).
        std::os::unix::fs::symlink(&models, models.join("loop")).unwrap();

        let target = TargetConfig {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: String::new(),
        };
        // The key assertion is that this RETURNS (no stack overflow / hang).
        let result =
            import_dbt_project(dir.path(), &target).expect("import should terminate cleanly");
        assert!(
            result.imported.iter().any(|m| m.name == "ok"),
            "the real model should still be imported"
        );
    }

    // ---------------------------------------------------------------------
    // FR-046: compiled upstream model FQNs rewritten to bare model refs
    // ---------------------------------------------------------------------

    #[test]
    fn test_dequote_relation_strips_quotes_and_backticks() {
        assert_eq!(dequote_relation("\"db\".\"s\".\"t\""), "db.s.t");
        assert_eq!(dequote_relation("`cat`.`s`.`t`"), "cat.s.t");
        assert_eq!(dequote_relation("db.s.t"), "db.s.t");
    }

    #[test]
    fn test_replace_relation_ref_quoted_unquoted_and_boundary() {
        // Quoted (self-bounding) and unquoted forms both rewrite.
        assert_eq!(
            replace_relation_ref(
                "SELECT * FROM \"dev\".\"marts\".\"up\"",
                "\"dev\".\"marts\".\"up\"",
                "up"
            ),
            "SELECT * FROM up"
        );
        assert_eq!(
            replace_relation_ref("SELECT * FROM dev.marts.up", "dev.marts.up", "up"),
            "SELECT * FROM up"
        );
        // A same-prefix sibling must NOT be clobbered: `dev.marts.up` is not a
        // standalone ref inside `dev.marts.up_daily`.
        assert_eq!(
            replace_relation_ref("SELECT * FROM dev.marts.up_daily", "dev.marts.up", "up"),
            "SELECT * FROM dev.marts.up_daily"
        );
        // The quoted form is self-bounding: the closing quote means it's never
        // a substring of a longer quoted relation.
        assert_eq!(
            replace_relation_ref("FROM \"d\".\"s\".\"up2\"", "\"d\".\"s\".\"up\"", "up"),
            "FROM \"d\".\"s\".\"up2\""
        );
    }

    /// Build a minimal `DbtManifestNode` for the rewrite-helper unit test.
    fn bare_node(name: &str, deps: Vec<&str>) -> DbtManifestNode {
        DbtManifestNode {
            unique_id: format!("model.p.{name}"),
            name: name.to_string(),
            resource_type: "model".to_string(),
            compiled_code: None,
            raw_code: String::new(),
            depends_on: dbt_manifest::DbtDependsOn {
                nodes: deps.into_iter().map(String::from).collect(),
                macros: vec![],
            },
            config: DbtNodeConfig {
                materialized: "table".to_string(),
                full_refresh: None,
                schema: None,
                unique_key: None,
                incremental_strategy: None,
                event_time: None,
                batch_size: None,
                lookback: None,
                partition_by: None,
                databricks_tags: Default::default(),
                pre_hook: vec![],
                post_hook: vec![],
                on_schema_change: None,
                alias: None,
                merge_update_columns: None,
                merge_exclude_columns: None,
                contract: None,
                snapshot: None,
            },
            columns: HashMap::new(),
            description: None,
            tags: vec![],
            schema: String::new(),
            database: String::new(),
            relation_name: None,
            governance: Default::default(),
        }
    }

    /// dbt inlines an ephemeral upstream as a `__dbt__cte__` CTE, so the
    /// consumer's compiled body reads the EPHEMERAL's upstream by its qualified
    /// relation. That read is rewritten too, transitively through chained
    /// ephemerals, even though it is not in the consumer's own `depends_on`.
    #[test]
    fn test_rewrite_upstream_refs_reaches_through_inlined_ephemerals() {
        let model = |bare: &str, deps: Vec<&str>| UpstreamModel {
            bare_name: bare.to_string(),
            fqn_candidates: vec![format!("\"dev\".\"s\".\"{bare}\"")],
            ephemeral_deps: deps.into_iter().map(String::from).collect(),
        };
        let mut models = HashMap::new();
        models.insert("model.p.base".to_string(), model("base", vec![]));
        models.insert(
            "model.p.eph2".to_string(),
            model("eph2", vec!["model.p.base"]),
        );
        models.insert(
            "model.p.eph1".to_string(),
            model("eph1", vec!["model.p.eph2"]),
        );
        let node = bare_node("dn", vec!["model.p.eph1"]);

        let body = "with __dbt__cte__eph2 as (select * from \"dev\".\"s\".\"base\"), \
                    __dbt__cte__eph1 as (select * from __dbt__cte__eph2) \
                    select * from __dbt__cte__eph1";
        let rewritten = rewrite_upstream_refs_to_bare(body, &node, &models);
        assert!(rewritten.contains("select * from base)"), "{rewritten}");
        assert!(!rewritten.contains("\"dev\""), "{rewritten}");
        assert_eq!(
            compiled_model_reads(&node, &models),
            vec![
                "model.p.eph1".to_string(),
                "model.p.eph2".to_string(),
                "model.p.base".to_string()
            ]
        );
    }

    #[test]
    fn test_rewrite_upstream_refs_rewrites_models_preserves_sources() {
        // `dn` depends on a model (`up`) and a source (`raw.events`). Only the
        // model ref is rewritten to a bare name; the source FQN is preserved.
        let mut models = HashMap::new();
        models.insert(
            "model.p.up".to_string(),
            UpstreamModel {
                bare_name: "up".to_string(),
                fqn_candidates: vec![
                    "\"dev\".\"marts\".\"up\"".to_string(),
                    "dev.marts.up".to_string(),
                ],
                ephemeral_deps: vec![],
            },
        );
        let node = bare_node("dn", vec!["model.p.up", "source.p.raw.events"]);

        let body = "SELECT u.id FROM \"dev\".\"marts\".\"up\" u \
                    JOIN \"raw_db\".\"raw_schema\".\"events\" e ON u.id = e.id";
        let rewritten = rewrite_upstream_refs_to_bare(body, &node, &models);
        assert!(
            rewritten.contains("FROM up u"),
            "model ref must become bare: {rewritten}"
        );
        assert!(
            !rewritten.contains("\"dev\".\"marts\".\"up\""),
            "no qualified model ref may remain: {rewritten}"
        );
        assert!(
            rewritten.contains("\"raw_db\".\"raw_schema\".\"events\""),
            "source FQN must stay qualified: {rewritten}"
        );
    }

    /// A manifest mirroring real `dbt compile` output (duckdb, double-quoted
    /// relations): a microbatch `mb` (`SELECT * FROM {{ ref('up') }}`), a plain
    /// `dn` that refs `up`, an `up` that refs a `source()`, and a `src_model`
    /// that refs a different `source()`. (FR-046)
    fn fr046_manifest() -> serde_json::Value {
        serde_json::json!({
            "metadata": { "project_name": "fr046" },
            "nodes": {
                "model.fr046.up": {
                    "unique_id": "model.fr046.up", "name": "up", "resource_type": "model",
                    "relation_name": "\"dev\".\"marts\".\"up\"",
                    "compiled_code": "SELECT id, ts FROM \"raw_db\".\"raw_schema\".\"base\"",
                    "raw_code": "SELECT id, ts FROM {{ source('raw','base') }}",
                    "depends_on": { "nodes": ["source.fr046.raw.base"], "macros": [] },
                    "config": { "materialized": "table", "event_time": "ts" },
                    "columns": {}, "tags": [], "schema": "marts", "database": "dev"
                },
                "model.fr046.mb": {
                    "unique_id": "model.fr046.mb", "name": "mb", "resource_type": "model",
                    "relation_name": "\"dev\".\"marts\".\"mb\"",
                    "compiled_code": "SELECT * FROM \"dev\".\"marts\".\"up\"",
                    "raw_code": "SELECT * FROM {{ ref('up') }}",
                    "depends_on": { "nodes": ["model.fr046.up"], "macros": [] },
                    "config": { "materialized": "microbatch", "event_time": "ts", "batch_size": "day", "unique_key": "id" },
                    "columns": {}, "tags": [], "schema": "marts", "database": "dev"
                },
                "model.fr046.dn": {
                    "unique_id": "model.fr046.dn", "name": "dn", "resource_type": "model",
                    "relation_name": "\"dev\".\"marts\".\"dn\"",
                    "compiled_code": "SELECT id, ts FROM \"dev\".\"marts\".\"up\"",
                    "raw_code": "SELECT id, ts FROM {{ ref('up') }}",
                    "depends_on": { "nodes": ["model.fr046.up"], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "marts", "database": "dev"
                },
                "model.fr046.src_model": {
                    "unique_id": "model.fr046.src_model", "name": "src_model", "resource_type": "model",
                    "relation_name": "\"dev\".\"marts\".\"src_model\"",
                    "compiled_code": "SELECT event_id, ts FROM \"raw_db\".\"raw_schema\".\"events\"",
                    "raw_code": "SELECT event_id, ts FROM {{ source('raw','events') }}",
                    "depends_on": { "nodes": ["source.fr046.raw.events"], "macros": [] },
                    "config": { "materialized": "table" },
                    "columns": {}, "tags": [], "schema": "marts", "database": "dev"
                }
            },
            "sources": {
                "source.fr046.raw.base": { "unique_id": "source.fr046.raw.base", "name": "base", "source_name": "raw", "database": "raw_db", "schema": "raw_schema" },
                "source.fr046.raw.events": { "unique_id": "source.fr046.raw.events", "name": "events", "source_name": "raw", "database": "raw_db", "schema": "raw_schema" }
            }
        })
    }

    #[test]
    fn test_import_dbt_rewrites_compiled_model_fqn_to_bare_ref() {
        // Drives the REAL importer (manifest parse → import) and asserts the
        // emitted bodies reference bare model names while genuine source FQNs
        // stay qualified — the regression #984's typecheck test couldn't catch
        // because it hand-mocked a bare upstream instead of importer output.
        let result =
            import_from_manifest_json_with_mode(&fr046_manifest(), MicrobatchMode::TimeInterval);

        let model = |name: &str| {
            result
                .imported
                .iter()
                .find(|m| m.name == name)
                .unwrap_or_else(|| panic!("model '{name}' should have imported"))
        };

        // (a) microbatch body: bare `up`, no qualified FQN, wrapped for the
        // time_interval window.
        let mb = model("mb");
        assert!(
            !mb.sql.contains("\"dev\".\"marts\".\"up\"") && !mb.sql.contains("dev.marts.up"),
            "microbatch body must not keep the qualified upstream FQN: {}",
            mb.sql
        );
        assert!(
            mb.sql.contains("FROM up"),
            "microbatch body must reference the bare model name: {}",
            mb.sql
        );
        assert!(
            mb.sql.contains("@start_date") && mb.sql.contains("@end_date"),
            "microbatch body must still carry the time_interval window: {}",
            mb.sql
        );

        // (b) a plain downstream model resolves to a bare upstream ref.
        assert_eq!(model("dn").sql, "SELECT id, ts FROM up");

        // (c) genuine source() refs stay qualified — both the standalone
        // src_model and an upstream model's own source ref.
        assert!(
            model("src_model")
                .sql
                .contains("\"raw_db\".\"raw_schema\".\"events\""),
            "source FQN must stay qualified: {}",
            model("src_model").sql
        );
        assert!(
            model("up")
                .sql
                .contains("\"raw_db\".\"raw_schema\".\"base\""),
            "an upstream model's own source ref must stay qualified: {}",
            model("up").sql
        );
    }

    #[test]
    fn test_import_dbt_output_compiles_without_e020() {
        // The end-to-end proof #984 lacked: take the REAL importer output and
        // run it through the compiler. Before FR-046 the microbatch body kept
        // `SELECT * FROM "dev"."marts"."up"`, so SELECT* schema inference
        // couldn't resolve `up` and E020 fired. With bare refs, `up` resolves
        // transitively and E020 stays silent.
        use crate::project::Project;
        use crate::semantic::build_semantic_graph;
        use crate::typecheck::typecheck_project_with_models;
        use rocky_core::models::Model;

        let result =
            import_from_manifest_json_with_mode(&fr046_manifest(), MicrobatchMode::TimeInterval);
        let models: Vec<Model> = result
            .imported
            .iter()
            .map(|im| Model {
                drop_existing_kind: None,
                config: im.config.clone(),
                sql: im.sql.clone(),
                file_path: format!("models/{}.sql", im.name).into(),
                contract_path: None,
            })
            .collect();

        let project = Project::from_models(models).expect("imported models form a valid project");
        let graph = build_semantic_graph(&project, &HashMap::new())
            .expect("semantic graph builds from importer output");
        let typed =
            typecheck_project_with_models(&graph, &HashMap::new(), None, &project.models, None);

        assert!(
            typed
                .typed_models
                .get("mb")
                .is_some_and(|cols| cols.iter().any(|c| c.name == "ts")),
            "microbatch output schema must expose `ts` via the bare upstream: {:?}",
            typed.typed_models.get("mb")
        );
        assert!(
            !typed.diagnostics.iter().any(|d| &*d.code == "E020"),
            "E020 must not fire once the upstream ref is bare: {:?}",
            typed.diagnostics
        );
        assert!(
            !typed
                .diagnostics
                .iter()
                .any(crate::diagnostic::Diagnostic::is_error),
            "imported time_interval microbatch must compile: {:?}",
            typed.diagnostics
        );
    }
}
