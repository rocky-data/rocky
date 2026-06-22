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
//! - `{% if is_incremental() %}` -> Rocky incremental strategy
//!
//! **Import paths:**
//! - **Manifest (preferred):** uses `compiled_code` from `target/manifest.json`
//! - **Regex (fallback):** regex-based Jinja extraction from raw `.sql` files
//!
//! **Not supported (produces diagnostics):**
//! - Custom Jinja macros, `{% for %}`, `{{ var() }}`, Python models

use std::collections::{BTreeMap, HashMap};
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

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

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
    /// View or ephemeral materialization not natively supported.
    UnsupportedMaterialization,
    /// Jinja control flow other than `is_incremental()`.
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
    /// model, exposure). Surfaced with a count so a migration is never
    /// silently lossy.
    DroppedConstruct {
        construct: String,
        name: String,
        detail: String,
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
    /// detected and skipped (snapshots, metrics, semantic models, exposures).
    /// Surfaced so a migration is never silently lossy.
    pub constructs_dropped: usize,
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
}

// ---------------------------------------------------------------------------
// Import from manifest (fast path)
// ---------------------------------------------------------------------------

/// Import models from a parsed dbt manifest.
///
/// Uses `compiled_code` (all Jinja resolved) for each model node, falling
/// back to `raw_code` if compiled_code is absent.
pub fn import_from_manifest(manifest: &DbtManifest, default_target: &TargetConfig) -> ImportResult {
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

    for node in manifest.nodes.values() {
        import_manifest_node(node, default_target, &mut result);
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
                "regenerate the manifest with `dbt compile` (including any required --vars) and re-import".to_string(),
            ),
        });
    }

    // Surface the resource classes the importer does not translate so a
    // migration is never silently lossy.
    record_dropped_constructs(&manifest.dropped, &mut result);

    apply_dbt_unit_tests(manifest, &mut result);

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
            "Rocky has no snapshot pipeline yet — re-implement as a [snapshot] pipeline or keep it in dbt",
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
        (
            "exposure",
            dropped.exposures,
            "dbt exposures (downstream-usage docs) are not imported",
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

        result.tests_converted += decls.len();
        // "custom" = converted tests that aren't the canonical column-level
        // built-ins — today the composite (`unique_combination_of_columns`)
        // conversions. Previously always 0 because nothing custom converted.
        result.tests_converted_custom += decls
            .iter()
            .filter(|d| matches!(d.test_type, rocky_core::tests::TestType::Composite { .. }))
            .count();
        result.tests_skipped += unsupported.len();

        // Attach the decls to the matching imported model (if present).
        if let Some(imported) = result.imported.iter_mut().find(|m| &m.name == model_name) {
            imported.config.tests.extend(decls);
        }

        // Surface every unsupported test as a structured warning. Skip
        // model-level tests (column == None) — same rule today.
        for u in unsupported {
            let where_ = match &u.column {
                Some(c) => format!("column '{c}'"),
                None => "model-level".to_string(),
            };
            result.warnings.push(ImportWarning {
                model: u.model.clone(),
                category: WarningCategory::UnsupportedTest,
                message: format!(
                    "dbt test '{name}' on {where_} is outside the supported set (unique, not_null, accepted_values, relationships) — not translated",
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
pub fn apply_dbt_unit_tests(manifest: &DbtManifest, result: &mut ImportResult) {
    for ut in manifest.unit_tests.values() {
        result.unit_tests_found += 1;

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
        imported.unit_tests.push(test_def);
        result.unit_tests_converted += 1;
    }
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

fn import_manifest_node(
    node: &DbtManifestNode,
    default_target: &TargetConfig,
    result: &mut ImportResult,
) {
    // Resolve the model's output coordinates up front so the raw-code fallback
    // (for {{ this }}) and the emitted target use the same values. dbt `alias`
    // overrides the relation name; dropping it silently lands the data in a
    // table named after the node.
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
    let table = node
        .config
        .alias
        .clone()
        .unwrap_or_else(|| node.name.clone());
    let this_ref = format!("{catalog}.{schema}.{table}");

    // Use compiled_code (Jinja resolved) if available, else raw_code.
    let sql = match &node.compiled_code {
        Some(code) => code.clone(),
        None => {
            result.warnings.push(ImportWarning {
                model: node.name.clone(),
                category: WarningCategory::JinjaControlFlow,
                message: "no compiled_code in manifest; using raw_code (may contain Jinja)"
                    .to_string(),
                suggestion: Some("run `dbt compile` to generate compiled SQL".to_string()),
            });
            convert_jinja_to_sql(&node.raw_code, &this_ref)
        }
    };

    // Map strategy from manifest config — covers all dbt materializations
    // (`table`, `view`, `materialized_view`, `incremental`, `ephemeral`,
    // `microbatch`) plus the `incremental_strategy` discriminator.
    let StrategyMappingOutput {
        strategy,
        warnings: strategy_warnings,
        structured,
    } = map_manifest_strategy(&node.config, &node.name);
    result.warnings.extend(strategy_warnings);
    result.structured_warnings.extend(structured);

    // Surface dbt-databricks specifics that Rocky doesn't auto-translate
    // (databricks_tags, pre/post hooks, on_schema_change). Emitted as
    // structured warnings so the downstream UI can route them.
    collect_dropped_config_warnings(&node.config, &node.name, result);

    // Detect unresolvable Jinja macros that survived `dbt compile`. dbt's
    // compile step inlines in-tree macros, so anything still present
    // points at an out-of-tree macro the user needs to hand-port.
    collect_unresolvable_macros(&sql, &node.name, result);

    // Map dependencies
    let depends_on = dbt_manifest::depends_on_to_rocky(&node.depends_on.nodes);

    // Use description as intent
    let intent = node.description.clone();

    let config = ModelConfig {
        name: node.name.clone(),
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
        retention: None,
        budget: None,
        skip: None,
        name_declared: String::new(),
        target_table_declared: String::new(),
    };

    result.imported.push(ImportedModel {
        name: node.name.clone(),
        sql: sql.trim().to_string(),
        config,
        unit_tests: Vec::new(),
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
fn map_manifest_strategy(config: &DbtNodeConfig, model_name: &str) -> StrategyMappingOutput {
    let mut warnings = Vec::new();
    let mut structured = Vec::new();

    let strategy = match config.materialized.as_str() {
        "table" => StrategyConfig::FullRefresh,
        "view" => StrategyConfig::View,
        "materialized_view" => StrategyConfig::MaterializedView,
        "ephemeral" => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: "materialized='ephemeral' has no Rocky equivalent — using full_refresh"
                    .to_string(),
                suggestion: Some(
                    "ephemeral models inline into downstream queries; consider folding the SQL into the consumer or keeping it as a `full_refresh` table".to_string(),
                ),
            });
            structured.push(ImportDbtStructuredWarning::UnsupportedMaterialization {
                model: model_name.to_string(),
                dbt_materialization: "ephemeral".to_string(),
                action: "fell back to full_refresh".to_string(),
            });
            StrategyConfig::FullRefresh
        }
        "incremental" => {
            map_incremental_strategy(config, model_name, &mut warnings, &mut structured)
        }
        "microbatch" => map_microbatch_strategy(config, model_name, &mut warnings, &mut structured),
        other => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "materialized='{other}' not recognized by Rocky — using full_refresh"
                ),
                suggestion: Some(
                    "set `type` in the emitted strategy block to a Rocky-supported value (full_refresh / incremental / merge / view / materialized_view / dynamic_table / time_interval / delete_insert / microbatch)".to_string(),
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
                    message: "incremental_strategy='merge' requires unique_key — falling back to incremental(updated_at)".to_string(),
                    suggestion: Some(
                        "add unique_key to the model config or pick a non-merge incremental_strategy".to_string(),
                    ),
                });
                StrategyConfig::Incremental {
                    timestamp_column: "updated_at".to_string(),
                }
            }
        },
        "append" => StrategyConfig::Incremental {
            timestamp_column: "updated_at".to_string(),
        },
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
            map_microbatch_strategy(config, model_name, warnings, structured)
        }
        other => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: format!(
                    "incremental_strategy='{other}' not recognized — falling back to incremental(updated_at)"
                ),
                suggestion: Some(
                    "use one of: append, merge, delete+insert, insert_overwrite, microbatch".to_string(),
                ),
            });
            StrategyConfig::Incremental {
                timestamp_column: "updated_at".to_string(),
            }
        }
    }
}

/// Map `materialized='microbatch'` (or `incremental_strategy='microbatch'`)
/// to [`StrategyConfig::Microbatch`]. Emits a
/// [`ImportDbtStructuredWarning::MicrobatchMissingEventTime`] + falls back
/// to `FullRefresh` if `event_time` is absent.
fn map_microbatch_strategy(
    config: &DbtNodeConfig,
    model_name: &str,
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
    // dbt microbatch idempotently REPLACES each batch partition. Rocky's
    // Microbatch strategy emits an append-only INSERT (sql_gen.rs), so importing
    // it as-is silently re-inserts the lookback window every run. dbt microbatch
    // requires a `unique_key`, so map it to an idempotent Rocky merge instead;
    // only fall back to append-only (loudly) if a key is somehow absent.
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
                message: "dbt microbatch without a unique_key maps to an append-only incremental strategy — it re-inserts the lookback window on every run".to_string(),
                suggestion: Some(
                    "add a unique_key (dbt microbatch normally has one) so it maps to an idempotent merge, or convert to a time-interval strategy with @start_date/@end_date".to_string(),
                ),
            });
            structured.push(ImportDbtStructuredWarning::MicrobatchMapped {
                model: model_name.to_string(),
                mapped_to: "incremental_append".to_string(),
            });
            // Emit `incremental`, not `microbatch`: both lower to an
            // append-only INSERT (sql_gen), but `microbatch` misleadingly
            // implies dbt's idempotent partition-replace. `incremental` is
            // honest about the append semantics.
            StrategyConfig::Incremental {
                timestamp_column: event_time,
            }
        }
    }
}

/// Collect structured warnings for dbt config Rocky can't auto-translate
/// (databricks_tags, pre/post hooks, on_schema_change). These are
/// dropped-on-purpose with an explicit pointer at the Rocky equivalent.
fn collect_dropped_config_warnings(
    config: &DbtNodeConfig,
    model_name: &str,
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

    if let Some(value) = config.on_schema_change.as_deref() {
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
fn safe_join_under(base: &Path, rel: &Path) -> Result<std::path::PathBuf, String> {
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

    for dir in &model_dirs {
        if dir.exists() {
            visit_dbt_models(
                dir,
                dir,
                default_target,
                &project_config,
                &source_map,
                &mut result,
                0,
            )?;
        }
    }

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

fn visit_dbt_models(
    dir: &Path,
    models_root: &Path,
    default_target: &TargetConfig,
    project_config: &Option<DbtProjectConfig>,
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
                project_config,
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

            match import_single_model(
                &path,
                &name,
                rel_path,
                default_target,
                project_config,
                source_map,
            ) {
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
    project_config: &Option<DbtProjectConfig>,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
) -> Result<(ImportedModel, Vec<ImportWarning>), String> {
    let content = std::fs::read_to_string(path).map_err(|e| format!("failed to read: {e}"))?;

    let mut warnings = Vec::new();

    // Detect is_incremental() before general Jinja handling
    let (content_processed, incr_result) = detect_is_incremental(&content);

    // Check for remaining unsupported Jinja patterns
    if content_processed.contains("{%") {
        // `{% for %}` / `{% set %}` REFUSE: the regex conversion strips only
        // the `{% %}` delimiters, so a loop/assignment body survives exactly
        // once — a loop meant to emit N columns emits one broken fragment that
        // loads but is wrong. A loud failure beats a silent mis-render.
        let for_set_re = Regex::new(r"\{%-?\s*(for|set)\b").unwrap();
        if for_set_re.is_match(&content_processed) {
            return Err(
                "contains unsupported Jinja control flow ({% for %} or {% set %}) that the \
                 no-manifest importer cannot faithfully render — re-run after `dbt compile` (the \
                 manifest path resolves Jinja) or rewrite the model without loops/assignments"
                    .to_string(),
            );
        }
        // Other control flow ({% if %}) is still emitted with TODO markers and
        // a warning — it degrades (the body is applied unconditionally) but
        // stays inspectable for review, matching the long-standing behaviour.
        let block_re = Regex::new(r"\{%[^%]*%\}").unwrap();
        if block_re.is_match(&content_processed) {
            warnings.push(ImportWarning {
                model: name.to_string(),
                category: WarningCategory::JinjaControlFlow,
                message: "contains Jinja control flow ({% if %}) — emitted with TODO markers; the conditional body is applied unconditionally, so review the result".to_string(),
                suggestion: Some(
                    "use the manifest import path (`dbt compile`) for faithful Jinja resolution".to_string(),
                ),
            });
        }
    }
    if content_processed.contains("{{ var(") {
        warnings.push(ImportWarning {
            model: name.to_string(),
            category: WarningCategory::UnsupportedMacro,
            message: "contains {{ var() }} — not supported, replaced with TODO".to_string(),
            suggestion: Some(
                "replace with a literal value or use manifest.json import path".to_string(),
            ),
        });
    }

    // Extract config block
    let (mut strategy, config_warnings) = extract_dbt_config(&content_processed);
    warnings.extend(config_warnings.into_iter().map(|msg| ImportWarning {
        model: name.to_string(),
        category: WarningCategory::UnsupportedMaterialization,
        message: msg,
        suggestion: Some(
            "set `type = \"full_refresh\"` (or `\"ephemeral\"` for staging models) in the emitted sidecar".to_string(),
        ),
    }));

    // If is_incremental() was detected and config didn't already set incremental,
    // override the strategy
    if let Some(ref incr) = incr_result
        && matches!(strategy, StrategyConfig::FullRefresh)
    {
        strategy = StrategyConfig::Incremental {
            timestamp_column: incr.timestamp_column.clone(),
        };
    }

    // Apply project config inheritance
    let (resolved_schema, resolved_tags) = if let Some(proj) = project_config {
        let resolved = dbt_project::resolve_model_config(proj, rel_path);
        // Project-level materialization: only override if no model-level config
        if !content_processed.contains("config(") && matches!(strategy, StrategyConfig::FullRefresh)
        {
            match resolved.materialized.as_str() {
                "incremental" => {
                    strategy = StrategyConfig::Incremental {
                        timestamp_column: "updated_at".to_string(),
                    };
                }
                "view" => {
                    strategy = StrategyConfig::View;
                }
                "materialized_view" => {
                    strategy = StrategyConfig::MaterializedView;
                }
                "ephemeral" => {
                    warnings.push(ImportWarning {
                        model: name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "project config materialized='ephemeral' has no Rocky equivalent — using full_refresh".to_string(),
                        suggestion: Some(
                            "override per-model with `type = \"full_refresh\"` or fold the SQL into downstream models".to_string(),
                        ),
                    });
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

    // Convert Jinja refs to plain SQL.
    let sql = convert_jinja_to_sql(&content_processed, &this_ref);

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
        },
        warnings,
    ))
}

// ---------------------------------------------------------------------------
// is_incremental() detection
// ---------------------------------------------------------------------------

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

        processed = incr_re
            .replace(&processed, replacement.as_str())
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
    let config_re = Regex::new(r"\{\{\s*config\s*\(([^)]*)\)\s*\}\}").ok()?;
    let caps = config_re.captures(content)?;
    single_string_value(&caps[1], "alias")
}

fn extract_dbt_config(content: &str) -> (StrategyConfig, Vec<String>) {
    let mut messages = Vec::new();

    let config_re = Regex::new(r"\{\{\s*config\s*\(([^)]*)\)\s*\}\}").unwrap();
    let Some(captures) = config_re.captures(content) else {
        return (StrategyConfig::FullRefresh, messages);
    };

    let config_str = &captures[1];

    // Parse materialized
    let mat_re = Regex::new(r#"materialized\s*=\s*['"](\w+)['"]"#).unwrap();
    let materialized = mat_re
        .captures(config_str)
        .map(|c| c[1].to_string())
        .unwrap_or_else(|| "table".to_string());

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

    // Build a synthetic DbtNodeConfig — the regex path doesn't recover
    // databricks_tags / hooks / on_schema_change (they're multi-line in
    // practice), so they're left empty.
    let synthetic = DbtNodeConfig {
        materialized: materialized.clone(),
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
        on_schema_change: None,
        // alias does not affect strategy selection; the regex path threads it
        // to target.table separately via extract_dbt_alias.
        alias: None,
        merge_update_columns: None,
        merge_exclude_columns: None,
    };

    let mapping = map_manifest_strategy(&synthetic, "<regex-path>");
    for w in mapping.warnings {
        messages.push(w.message);
    }

    (mapping.strategy, messages)
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
fn convert_jinja_to_sql(content: &str, this_ref: &str) -> String {
    let mut sql = content.to_string();

    // Remove {{ config(...) }} blocks
    let config_re = Regex::new(r"\{\{\s*config\s*\([^)]*\)\s*\}\}\s*\n?").unwrap();
    sql = config_re.replace_all(&sql, "").to_string();

    // {{ ref('model_name') }} -> model_name
    let ref_re = Regex::new(r#"\{\{\s*ref\s*\(\s*['"](\w+)['"]\s*\)\s*\}\}"#).unwrap();
    sql = ref_re.replace_all(&sql, "$1").to_string();

    // {{ source('source_name', 'table_name') }} -> source_name.table_name
    let source_re =
        Regex::new(r#"\{\{\s*source\s*\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)\s*\}\}"#)
            .unwrap();
    sql = source_re.replace_all(&sql, "$1.$2").to_string();

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
        let (strategy, _) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::Merge { .. }));
    }

    #[test]
    fn test_extract_config_table() {
        let input = "{{ config(materialized='table') }}";
        let (strategy, _) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::FullRefresh));
    }

    #[test]
    fn test_extract_config_view_maps_to_view_strategy() {
        // Wave 2: `materialized='view'` now maps to StrategyConfig::View
        // (no warning) instead of FullRefresh + warning.
        let input = "{{ config(materialized='view') }}";
        let (strategy, warnings) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::View));
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_extract_config_materialized_view() {
        // Wave 2: `materialized='materialized_view'` now maps to
        // StrategyConfig::MaterializedView (previously: dropped silently).
        let input = "{{ config(materialized='materialized_view') }}";
        let (strategy, _) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::MaterializedView));
    }

    #[test]
    fn test_extract_config_ephemeral_warns() {
        let input = "{{ config(materialized='ephemeral') }}";
        let (strategy, warnings) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::FullRefresh));
        assert!(!warnings.is_empty());
    }

    #[test]
    fn test_extract_config_merge_with_unique_key_list() {
        // Regression: `incremental_strategy='merge'` + `unique_key=['user_id']`
        // must map to StrategyConfig::Merge, NOT to
        // `Incremental { timestamp_column: "merge" }`.
        let input = "{{ config(materialized='incremental', incremental_strategy='merge', unique_key=['user_id']) }}";
        let (strategy, _) = extract_dbt_config(input);
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
        let (strategy, _) = extract_dbt_config(input);
        // Without unique_key, this should fall back to Incremental(updated_at)
        // because merge requires unique_key — but the timestamp must not be 'merge'.
        if let StrategyConfig::Incremental { timestamp_column } = strategy {
            assert_ne!(
                timestamp_column, "merge",
                "BUG REGRESSION: incremental_strategy must NOT be parsed as a timestamp column"
            );
        }
    }

    #[test]
    fn test_extract_no_config() {
        let input = "SELECT 1";
        let (strategy, _) = extract_dbt_config(input);
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
        let result = import_from_manifest(&manifest, &target);

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

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, manifest_json.to_string()).unwrap();

        let manifest = dbt_manifest::parse_manifest(&path).unwrap();
        let target = TargetConfig {
            catalog: "w".to_string(),
            schema: "s".to_string(),
            table: String::new(),
        };
        let result = import_from_manifest(&manifest, &target);

        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::Merge { .. }
        ));
        assert_eq!(result.imported[0].config.depends_on, vec!["stg"]);
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
        let result = import_from_manifest(&manifest, &target);

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

    #[test]
    fn test_import_dbt_project_is_incremental_integration() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::create_dir_all(dir.path().join("models")).unwrap();

        std::fs::write(
            dir.path().join("models/fct_events.sql"),
            r#"
SELECT *
FROM {{ ref('stg_events') }}
{% if is_incremental() %}
  WHERE event_time > (SELECT MAX(event_time) FROM {{ this }})
{% endif %}
"#,
        )
        .unwrap();

        let target = TargetConfig {
            catalog: "warehouse".to_string(),
            schema: "staging".to_string(),
            table: String::new(),
        };

        let result = import_dbt_project(dir.path(), &target).unwrap();
        assert_eq!(result.imported.len(), 1);
        assert!(matches!(
            result.imported[0].config.strategy,
            StrategyConfig::Incremental {
                ref timestamp_column
            } if timestamp_column == "event_time"
        ));
        // Incremental block should be removed from SQL
        assert!(!result.imported[0].sql.contains("is_incremental"));
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
        import_from_manifest(&manifest, &target)
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
        let result = import_from_manifest_json(&manifest);
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
        assert_eq!(result.constructs_dropped, 2, "1 snapshot + 1 metric");
        assert!(result.structured_warnings.iter().any(|w| matches!(w,
            ImportDbtStructuredWarning::DroppedConstruct { construct, .. } if construct == "snapshot")));
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
    fn test_manifest_microbatch_without_key_maps_to_incremental() {
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
        // A microbatch without a unique_key maps to an append-only
        // `incremental` (not `microbatch`) — both lower to a bare INSERT, but
        // `incremental` doesn't misleadingly imply dbt's idempotent semantics.
        match &result.imported[0].config.strategy {
            StrategyConfig::Incremental { timestamp_column } => {
                assert_eq!(timestamp_column, "event_ts");
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
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
    fn test_manifest_ephemeral_emits_warning() {
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
            StrategyConfig::FullRefresh
        ));
        assert!(result.structured_warnings.iter().any(|w| matches!(
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
        let result = import_from_manifest_json(&manifest);
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
    fn test_incremental_strategy_append_maps_to_incremental() {
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
        let result = import_from_manifest_json(&manifest);
        match &result.imported[0].config.strategy {
            StrategyConfig::Incremental { timestamp_column } => {
                assert_ne!(timestamp_column, "merge");
                assert_ne!(timestamp_column, "append");
            }
            other => panic!("expected Incremental, got {other:?}"),
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
        let result = import_from_manifest_json(&manifest);
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
        let result = import_from_manifest_json(&manifest);
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
}
