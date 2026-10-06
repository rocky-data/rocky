//! dbt Hub package vendoring — the pure half of `rocky package`.
//!
//! `rocky package add fivetran/stripe` runs dbt once (`dbt deps` +
//! `dbt compile --full-refresh`) in a throwaway project, then hands the
//! resulting `target/manifest.json` to this module. Everything here is
//! deterministic and needs no dbt, no warehouse and no network, so CI covers
//! it with a recorded manifest:
//!
//! - [`parse_package_info`] reads the manifest fields the general importer
//!   does not keep (`package_name`, test nodes, source identifiers).
//! - [`import_package`] selects one package's models (plus the upstream
//!   models they read from other packages), runs the existing manifest
//!   importer over just those nodes, builds every model into one schema (so
//!   bare-name reads resolve at run time), declares its package sources, and
//!   maps the four canonical dbt generic tests.
//! - [`render_package_files`] renders the vendored `.sql` + `.toml` files.
//! - [`plan_update`] three-way compares freshly rendered files against the
//!   hashes in `rocky-packages.lock` and the files on disk, so a file the user
//!   edited is never overwritten.
//!
//! Package model names keep their dbt names. A name that collides with a
//! project model or another package's model is refused ([`find_collisions`]),
//! never silently prefixed: downstream project models read package models by
//! bare name, and a rename would break every such reference.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::path::Path;

use serde::{Deserialize, Serialize};

use rocky_core::models::{SourceConfig, StrategyConfig, TargetConfig};
use rocky_core::tests::{TestDecl, TestSeverity, TestType};

use super::dbt::{
    ImportFailure, ImportWarning, ImportedModel, MicrobatchMode, import_from_manifest,
};
use super::dbt_manifest::DbtManifest;

/// Lockfile name, at the project root next to `rocky.toml`.
pub const LOCKFILE_NAME: &str = "rocky-packages.lock";

/// Directory (relative to the project root) that holds vendored packages.
pub const PACKAGES_DIR: &str = "models/packages";

/// Suffix of the file `rocky package update` writes beside a locally edited
/// vendored file instead of overwriting it.
pub const INCOMING_SUFFIX: &str = ".incoming";

/// Current lockfile format version.
pub const LOCK_VERSION: u32 = 1;

/// First line of every vendored `.sql` file. Constant on purpose: a header
/// that carried the package version would make every file look changed
/// upstream on each version bump, and every locally edited file would then
/// get a spurious `.incoming` copy.
const SQL_HEADER: &str = "-- Vendored by `rocky package` from a dbt package. Edit freely: `rocky package update`\n\
     -- keeps edited files and writes the new upstream version beside them as `.incoming`.\n";

// ---------------------------------------------------------------------------
// Manifest fields the general importer drops
// ---------------------------------------------------------------------------

/// Package-level facts from `manifest.json` that [`DbtManifest`] does not
/// carry: which package owns each node, the generic test nodes, and the
/// physical identifiers of sources.
#[derive(Debug, Clone, Default)]
pub struct PackageManifestInfo {
    /// The root project name (the throwaway project `rocky package` creates).
    pub project_name: String,
    pub nodes: HashMap<String, InfoNode>,
    pub sources: HashMap<String, InfoSource>,
}

/// One manifest node, reduced to what package vendoring needs.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InfoNode {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub resource_type: String,
    #[serde(default)]
    pub package_name: String,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub depends_on: InfoDependsOn,
    #[serde(default)]
    pub test_metadata: Option<InfoTestMetadata>,
    #[serde(default)]
    pub attached_node: Option<String>,
    #[serde(default)]
    pub column_name: Option<String>,
    #[serde(default)]
    pub config: InfoNodeConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct InfoDependsOn {
    #[serde(default)]
    pub nodes: Vec<String>,
}

/// `test_metadata` on a generic test node.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InfoTestMetadata {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub kwargs: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct InfoNodeConfig {
    #[serde(default)]
    pub materialized: Option<String>,
    #[serde(default)]
    pub severity: Option<String>,
    #[serde(default, rename = "where")]
    pub where_clause: Option<String>,
    #[serde(default)]
    pub enabled: Option<bool>,
}

/// One manifest source, with the identifier dbt resolved from the package's
/// `*_schema` / `*_database` / `*_identifier` vars.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct InfoSource {
    #[serde(default)]
    pub package_name: String,
    #[serde(default)]
    pub source_name: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub identifier: Option<String>,
}

#[derive(Deserialize)]
struct RawInfo {
    #[serde(default)]
    metadata: RawInfoMetadata,
    #[serde(default)]
    nodes: HashMap<String, InfoNode>,
    #[serde(default)]
    sources: HashMap<String, InfoSource>,
}

#[derive(Deserialize, Default)]
struct RawInfoMetadata {
    #[serde(default)]
    project_name: Option<String>,
}

/// Read the package-level manifest facts. Companion to
/// [`super::dbt_manifest::parse_manifest`], which reads the same file for the
/// model bodies.
pub fn parse_package_info(path: &Path) -> Result<PackageManifestInfo, String> {
    let file =
        std::fs::File::open(path).map_err(|e| format!("failed to open {}: {e}", path.display()))?;
    let raw: RawInfo = serde_json::from_reader(std::io::BufReader::new(file))
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
    Ok(PackageManifestInfo {
        project_name: raw.metadata.project_name.unwrap_or_default(),
        nodes: raw.nodes,
        sources: raw.sources,
    })
}

// ---------------------------------------------------------------------------
// Import
// ---------------------------------------------------------------------------

/// A raw source table a package reads, as dbt resolved it at compile time.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct PackageSource {
    /// `<source_name>.<table>` as the package declares it (`stripe.charge`).
    pub name: String,
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// A dbt test on a package model that Rocky did not map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedTest {
    /// The dbt test node name.
    pub test: String,
    /// The model (or source) the test is attached to, when known.
    pub attached_to: Option<String>,
    pub reason: String,
}

/// One vendored model: the imported model plus where it came from.
pub struct VendoredModel {
    pub model: ImportedModel,
    /// dbt package that owns the model (the requested package, or one of its
    /// dependencies when the requested package reads its models).
    pub owner_package: String,
    /// dbt materialization as compiled (`table`, `view`, `incremental`, ...).
    pub dbt_materialized: String,
}

/// Result of importing one package from a compiled manifest.
pub struct PackageImport {
    /// dbt project name of the package (`stripe` for `fivetran/stripe`).
    pub package: String,
    pub models: Vec<VendoredModel>,
    pub failed: Vec<ImportFailure>,
    pub warnings: Vec<ImportWarning>,
    pub sources: Vec<PackageSource>,
    /// Generic tests mapped to Rocky `[[tests]]`.
    pub tests_mapped: usize,
    pub tests_dropped: Vec<DroppedTest>,
    /// dbt `incremental` models that did not become a Rocky incremental
    /// strategy (imported as full refresh, or failed).
    pub incremental_fallbacks: Vec<String>,
    pub dbt_version: Option<String>,
    /// Models refused because dbt compiled them while the relation a
    /// column-introspecting macro reads did not exist (also in `failed`).
    /// Outside [`BuildMode::BuildEmpty`] any entry refuses the whole package.
    pub introspection_refused: Vec<String>,
    /// Reasons the import as a whole cannot be vendored without leaving a
    /// project `rocky compile` rejects: a vendored model reads a model that
    /// was not vendored, or a dbt seed, or its SQL does not parse. Any entry
    /// refuses the package (E055) before anything is written.
    pub blocking: Vec<String>,
}

/// Import one package's models from a compiled manifest.
///
/// Selection: every enabled `model` / `snapshot` node whose `package_name` is
/// `package`, plus — transitively — any upstream model it depends on from
/// another package (older Fivetran packages split `*_source` staging into a
/// separate package). The root project's own nodes are never selected.
pub fn import_package(
    manifest: &DbtManifest,
    info: &PackageManifestInfo,
    package: &str,
    default_target: &TargetConfig,
) -> Result<PackageImport, String> {
    let selected = select_package_nodes(info, package)?;

    let mut filtered = manifest.clone();
    filtered.nodes.retain(|id, _| selected.contains_key(id));
    let selected_names: BTreeSet<String> =
        filtered.nodes.values().map(|n| n.name.clone()).collect();
    filtered
        .unit_tests
        .retain(|_, ut| selected_names.contains(&ut.model));
    filtered.dropped = Default::default();

    let mut result = import_from_manifest(&filtered, default_target, false, MicrobatchMode::Merge);

    // rocky name -> unique_id, for every selected node.
    let mut by_rocky_name: HashMap<String, String> = HashMap::new();
    for (id, node) in &filtered.nodes {
        let name =
            super::dbt_governance::rocky_model_name(&node.name, node.governance.version.as_deref())
                .unwrap_or_else(|_| node.name.clone());
        by_rocky_name.insert(name, id.clone());
    }

    let mut used_sources: BTreeSet<PackageSource> = BTreeSet::new();
    let mut models = Vec::new();
    for mut imported in std::mem::take(&mut result.imported) {
        let Some(id) = by_rocky_name.get(&imported.name) else {
            continue;
        };
        let node = &filtered.nodes[id];
        // Every vendored model builds into ONE schema, `default_target.schema`.
        // The SQL reads upstream package models by bare name (that is what
        // makes them DAG edges Rocky type-checks), and the warehouse resolves
        // a bare name through the connection's current schema. dbt's per-folder
        // `+schema` (`main_stg_stripe`, `main_stripe`) would put upstreams
        // where a bare read cannot reach them.
        imported.config.target.schema = default_target.schema.clone();
        if !node.database.is_empty() {
            imported.config.target.catalog = node.database.clone();
        }
        // Declare the package sources this model reads.
        let mut model_sources = Vec::new();
        for dep in &node.depends_on.nodes {
            if let Some(src) = info.sources.get(dep).and_then(package_source) {
                model_sources.push(SourceConfig {
                    catalog: src.catalog.clone(),
                    schema: src.schema.clone(),
                    table: src.table.clone(),
                });
                used_sources.insert(src);
            }
        }
        imported.config.sources = model_sources;
        models.push(VendoredModel {
            owner_package: selected[id].clone(),
            dbt_materialized: node.config.materialized.clone(),
            model: imported,
        });
    }
    models.sort_by(|a, b| a.model.name.cmp(&b.model.name));

    // A model dbt compiled before its upstream existed carries an
    // introspection placeholder or all-NULL columns instead of real ones.
    // Refuse it.
    let mut failed = std::mem::take(&mut result.failed);
    let mut introspection_refused = Vec::new();
    models.retain(|vm| {
        let reason = if has_introspection_placeholder(&vm.model.sql) {
            "a column-introspecting macro (dbt_utils.star) emitted a placeholder `*`"
        } else if projects_only_nulls(&vm.model.sql) {
            "a column-filling macro (Fivetran's fill_staging_columns) found no columns and \
             cast every column to NULL"
        } else {
            return true;
        };
        failed.push(ImportFailure {
            name: vm.model.name.clone(),
            reason: format!(
                "dbt compiled this model before the relation it introspects existed, so {reason}. \
                 Re-run with --build-empty, or import a project compiled after `dbt run --empty` \
                 with --compiled <dir>"
            ),
        });
        introspection_refused.push(vm.model.name.clone());
        false
    });
    result.failed = failed;

    // Incremental models that did not stay incremental.
    let mut incremental_fallbacks = Vec::new();
    for vm in &models {
        if vm.dbt_materialized == "incremental"
            && matches!(vm.model.config.strategy, StrategyConfig::FullRefresh)
        {
            incremental_fallbacks.push(vm.model.name.clone());
        }
    }
    for failure in &result.failed {
        let materialized = by_rocky_name
            .get(&failure.name)
            .and_then(|id| filtered.nodes.get(id))
            .map(|n| n.config.materialized.as_str());
        if materialized == Some("incremental") {
            incremental_fallbacks.push(failure.name.clone());
        }
    }
    incremental_fallbacks.sort();
    incremental_fallbacks.dedup();

    let (tests_mapped, tests_dropped) = map_generic_tests(info, &selected, &mut models);
    let blocking = integrity_problems(info, &by_rocky_name, &filtered, &models, &result.failed);

    Ok(PackageImport {
        blocking,
        introspection_refused,
        package: package.to_string(),
        models,
        failed: result.failed,
        warnings: result.warnings,
        sources: used_sources.into_iter().collect(),
        tests_mapped,
        tests_dropped,
        incremental_fallbacks,
        dbt_version: result.dbt_version,
    })
}

/// Why the vendored set would not compile on its own. Every dependency of a
/// vendored model must itself be vendored (a model or snapshot) or be a dbt
/// source; a seed, or a model that failed to import, would leave a dangling
/// read. Every vendored SQL body must parse the way `rocky compile` parses it.
fn integrity_problems(
    info: &PackageManifestInfo,
    by_rocky_name: &HashMap<String, String>,
    filtered: &DbtManifest,
    models: &[VendoredModel],
    failed: &[ImportFailure],
) -> Vec<String> {
    let vendored_ids: BTreeSet<&str> = models
        .iter()
        .filter_map(|vm| by_rocky_name.get(&vm.model.name).map(String::as_str))
        .collect();
    let failure_of = |name: &str| -> Option<&str> {
        failed
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.reason.as_str())
    };
    let mut problems = Vec::new();
    for vm in models {
        let name = &vm.model.name;
        let rendered = super::emit::annotate_unsupported_jinja(&vm.model.sql);
        if let Err(reason) = rocky_sql::lineage::extract_lineage(&rendered) {
            problems.push(format!(
                "model `{name}`: its compiled SQL does not parse, so `rocky compile` would \
                 reject it: {reason}"
            ));
        }
        let Some(node) = by_rocky_name
            .get(name)
            .and_then(|id| filtered.nodes.get(id))
        else {
            continue;
        };
        for dep in &node.depends_on.nodes {
            let Some(up) = info.nodes.get(dep) else {
                continue;
            };
            match up.resource_type.as_str() {
                "model" | "snapshot" => {
                    if vendored_ids.contains(dep.as_str()) || up.config.enabled == Some(false) {
                        continue;
                    }
                    let why = match failure_of(&up.name) {
                        Some(reason) => format!("it failed to import: {reason}"),
                        None if up.package_name == info.project_name => {
                            "it belongs to the build project, not a package".to_string()
                        }
                        None => "it was not selected".to_string(),
                    };
                    problems.push(format!(
                        "model `{name}` reads `{}`, which was not vendored ({why})",
                        up.name
                    ));
                }
                "seed" => problems.push(format!(
                    "model `{name}` reads dbt seed `{}` ({}); Rocky does not vendor seeds, so \
                     the read would point at a table nothing builds",
                    up.name, up.package_name
                )),
                _ => {}
            }
        }
    }
    problems.sort();
    problems.dedup();
    problems
}

/// Select `package`'s model nodes plus their upstream closure, mapped to the
/// package that owns each.
fn select_package_nodes(
    info: &PackageManifestInfo,
    package: &str,
) -> Result<BTreeMap<String, String>, String> {
    let is_model = |n: &InfoNode| {
        (n.resource_type == "model" || n.resource_type == "snapshot")
            && n.config.enabled != Some(false)
    };
    let mut selected: BTreeMap<String, String> = BTreeMap::new();
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut roots: Vec<&String> = info
        .nodes
        .iter()
        .filter(|(_, n)| n.package_name == package && is_model(n))
        .map(|(id, _)| id)
        .collect();
    roots.sort();
    if roots.is_empty() {
        let mut known: Vec<&str> = info
            .nodes
            .values()
            .filter(|n| is_model(n) && n.package_name != info.project_name)
            .map(|n| n.package_name.as_str())
            .collect();
        known.sort_unstable();
        known.dedup();
        return Err(format!(
            "the compiled manifest has no models for package '{package}' (packages with models: {})",
            if known.is_empty() {
                "none".to_string()
            } else {
                known.join(", ")
            }
        ));
    }
    for id in roots {
        selected.insert(id.clone(), package.to_string());
        queue.push_back(id.clone());
    }
    while let Some(id) = queue.pop_front() {
        let Some(node) = info.nodes.get(&id) else {
            continue;
        };
        for dep in &node.depends_on.nodes {
            if selected.contains_key(dep) {
                continue;
            }
            let Some(up) = info.nodes.get(dep) else {
                continue;
            };
            if !is_model(up) || up.package_name == info.project_name {
                continue;
            }
            selected.insert(dep.clone(), up.package_name.clone());
            queue.push_back(dep.clone());
        }
    }
    Ok(selected)
}

fn package_source(src: &InfoSource) -> Option<PackageSource> {
    let table = src
        .identifier
        .clone()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| src.name.clone());
    if table.is_empty() {
        return None;
    }
    Some(PackageSource {
        name: format!("{}.{}", src.source_name, src.name),
        catalog: src.database.clone().unwrap_or_default(),
        schema: src.schema.clone().unwrap_or_default(),
        table,
    })
}

/// Map dbt generic test nodes attached to vendored models onto Rocky
/// `[[tests]]`. Only the four built-ins (`not_null`, `unique`,
/// `accepted_values`, `relationships`) map; every other test (package
/// macros such as `dbt_utils.*`, singular tests, tests on sources) is
/// returned as dropped so the caller reports it.
fn map_generic_tests(
    info: &PackageManifestInfo,
    selected: &BTreeMap<String, String>,
    models: &mut [VendoredModel],
) -> (usize, Vec<DroppedTest>) {
    // unique_id -> index in `models`; model name -> target FQN.
    let mut index_of: HashMap<&str, usize> = HashMap::new();
    let mut target_of: HashMap<String, String> = HashMap::new();
    for (i, vm) in models.iter().enumerate() {
        let t = &vm.model.config.target;
        target_of.insert(
            vm.model.name.clone(),
            format!("{}.{}.{}", t.catalog, t.schema, t.table),
        );
        if let Some((id, _)) = selected
            .iter()
            .find(|(id, _)| info.nodes.get(*id).is_some_and(|n| n.name == vm.model.name))
        {
            index_of.insert(id.as_str(), i);
        }
    }

    let mut test_ids: Vec<&String> = info
        .nodes
        .iter()
        .filter(|(_, n)| n.resource_type == "test" && n.config.enabled != Some(false))
        .map(|(id, _)| id)
        .collect();
    test_ids.sort();

    let mut mapped = 0;
    let mut dropped = Vec::new();
    for id in test_ids {
        let test = &info.nodes[id];
        // Attach by `attached_node`, else by the single model it depends on.
        let attached = test.attached_node.clone().or_else(|| {
            let models: Vec<&String> = test
                .depends_on
                .nodes
                .iter()
                .filter(|d| d.starts_with("model.") || d.starts_with("snapshot."))
                .collect();
            (models.len() == 1).then(|| models[0].clone())
        });
        let Some(attached) = attached else {
            // A test touching nothing we vendored is not ours to report.
            continue;
        };
        if !selected.contains_key(&attached) {
            if attached.starts_with("source.")
                && info
                    .sources
                    .get(&attached)
                    .is_some_and(|s| selected.values().any(|p| *p == s.package_name))
            {
                dropped.push(DroppedTest {
                    test: test.name.clone(),
                    attached_to: Some(attached.clone()),
                    reason: "tests on dbt sources are not mapped; Rocky does not build sources"
                        .to_string(),
                });
            }
            continue;
        }
        let Some(&model_idx) = index_of.get(attached.as_str()) else {
            dropped.push(DroppedTest {
                test: test.name.clone(),
                attached_to: Some(attached.clone()),
                reason: "the model it tests was not imported".to_string(),
            });
            continue;
        };
        match test_decl(test, &target_of) {
            Ok(decl) => {
                models[model_idx].model.config.tests.push(decl);
                mapped += 1;
            }
            Err(reason) => dropped.push(DroppedTest {
                test: test.name.clone(),
                attached_to: Some(models[model_idx].model.name.clone()),
                reason,
            }),
        }
    }
    (mapped, dropped)
}

/// Convert one dbt generic test node into a Rocky [`TestDecl`].
fn test_decl(test: &InfoNode, target_of: &HashMap<String, String>) -> Result<TestDecl, String> {
    let Some(meta) = &test.test_metadata else {
        return Err("singular (custom SQL) test".to_string());
    };
    if meta.namespace.as_deref().is_some_and(|ns| ns != "dbt") {
        return Err(format!(
            "generic test `{}.{}` has no Rocky equivalent",
            meta.namespace.as_deref().unwrap_or_default(),
            meta.name
        ));
    }
    let kwarg = |key: &str| -> Option<&serde_json::Value> {
        meta.kwargs.get(key).or_else(|| {
            meta.kwargs
                .get("arguments")
                .and_then(|a| a.as_object())
                .and_then(|a| a.get(key))
        })
    };
    let column = kwarg("column_name")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| test.column_name.clone())
        .filter(|c| !c.is_empty());
    let severity = match test.config.severity.as_deref() {
        Some(s) if s.eq_ignore_ascii_case("warn") => TestSeverity::Warning,
        _ => TestSeverity::Error,
    };
    let filter = test
        .config
        .where_clause
        .clone()
        .filter(|w| !w.trim().is_empty());
    if let Some(w) = &filter {
        // The same gates Rocky applies when the test runs, applied now so a
        // filter it would refuse is reported as dropped instead.
        let context = format!("`where` of dbt test `{}`", test.name);
        rocky_sql::validation::reject_statement_terminator(&context, w)
            .map_err(|e| e.to_string())?;
        rocky_sql::check_expression::validate_check_expression(
            &context,
            w,
            rocky_sql::check_expression::dialect_for("generic").as_ref(),
            rocky_sql::check_expression::ExpressionUse::Filter,
        )
        .map_err(|e| e.to_string())?;
    }
    let test_type = match meta.name.as_str() {
        "not_null" => TestType::NotNull,
        "unique" => TestType::Unique,
        "accepted_values" => {
            let values = kwarg("values")
                .and_then(|v| v.as_array())
                .ok_or_else(|| "accepted_values without a literal `values` list".to_string())?;
            // Rocky's accepted_values compares string literals. Numeric (and
            // boolean) values become an expression test with bare literals,
            // so a numeric column is not compared to strings.
            if !values.is_empty() && values.iter().all(|v| v.is_number() || v.is_boolean()) {
                let col = column
                    .clone()
                    .ok_or_else(|| "`accepted_values` test without a column".to_string())?;
                rocky_sql::validation::validate_identifier(&col).map_err(|e| e.to_string())?;
                let list: Vec<String> = values.iter().map(ToString::to_string).collect();
                let expression = format!("{col} IS NULL OR {col} IN ({})", list.join(", "));
                rocky_sql::check_expression::validate_check_expression(
                    "accepted_values",
                    &expression,
                    rocky_sql::check_expression::dialect_for("generic").as_ref(),
                    rocky_sql::check_expression::ExpressionUse::SinglePredicate,
                )
                .map_err(|e| e.to_string())?;
                return Ok(TestDecl {
                    test_type: TestType::Expression { expression },
                    column: Some(col),
                    severity,
                    filter,
                });
            }
            let values: Vec<String> = values
                .iter()
                .map(|v| match v {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .collect();
            TestType::AcceptedValues { values }
        }
        "relationships" => {
            let to = kwarg("to")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "relationships without `to`".to_string())?;
            let field = kwarg("field")
                .and_then(|v| v.as_str())
                .ok_or_else(|| "relationships without `field`".to_string())?;
            let to_model = super::dbt::strip_ref_wrapper(to);
            let to_table = target_of.get(&to_model).cloned().ok_or_else(|| {
                format!("relationships target `{to}` is not a vendored model of this package")
            })?;
            TestType::Relationships {
                to_table,
                to_column: field.to_string(),
            }
        }
        other => return Err(format!("generic test `{other}` has no Rocky equivalent")),
    };
    if column.is_none() {
        return Err(format!("`{}` test without a column", meta.name));
    }
    Ok(TestDecl {
        test_type,
        column,
        severity,
        filter,
    })
}

// ---------------------------------------------------------------------------
// Rendering, collisions, lockfile, update planning
// ---------------------------------------------------------------------------

/// True when `name` is safe as one path component and a Rocky model name.
pub fn is_safe_package_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// True when `rel` is a `/`-separated path strictly inside `models/packages/`
/// with no `..`, `.`, empty or absolute component. Every path `rocky package`
/// reads, writes or deletes from the lockfile must pass this.
pub fn is_vendored_path(rel: &str) -> bool {
    let Some(rest) = rel
        .strip_prefix(PACKAGES_DIR)
        .and_then(|r| r.strip_prefix('/'))
    else {
        return false;
    };
    let parts: Vec<&str> = rest.split('/').collect();
    parts.len() >= 2
        && parts.iter().all(|p| {
            !p.is_empty() && *p != "." && *p != ".." && !p.contains('\\') && !p.contains(':')
        })
}

/// Project-relative directory of a vendored package.
pub fn package_dir(package: &str) -> String {
    format!("{PACKAGES_DIR}/{package}")
}

/// Render every vendored file, keyed by project-relative path (`/`-separated).
///
/// Models from a dependency package land under that package's own directory,
/// so the tree mirrors dbt's package boundaries.
pub fn render_package_files(import: &PackageImport) -> Result<BTreeMap<String, String>, String> {
    let mut files = BTreeMap::new();
    for vm in &import.models {
        let name = &vm.model.name;
        if !is_safe_package_name(name) || name.contains('-') {
            return Err(format!(
                "model name {name:?} is not a safe file name; refusing to vendor it"
            ));
        }
        if !is_safe_package_name(&vm.owner_package) {
            return Err(format!(
                "package name {:?} is not a safe directory name; refusing to vendor it",
                vm.owner_package
            ));
        }
        let dir = package_dir(&vm.owner_package);
        let sql = format!(
            "{SQL_HEADER}{}\n",
            super::emit::annotate_unsupported_jinja(&vm.model.sql)
        );
        let mut toml = super::emit::render_model_sidecar(&vm.model.config);
        if !vm.model.unit_tests.is_empty() {
            toml.push_str(&super::emit::render_unit_tests(name, &vm.model.unit_tests));
        }
        files.insert(format!("{dir}/{name}.sql"), sql);
        files.insert(format!("{dir}/{name}.toml"), toml);
    }
    Ok(files)
}

/// A package model whose name an existing model already uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Collision {
    /// The package model's name.
    pub model: String,
    /// The existing model's resolved name (differs from `model` only by case,
    /// or not at all).
    pub existing: String,
    /// `project` or the name of the package that owns the existing model.
    pub owner: String,
}

/// A model already in the project, keyed in the map [`find_collisions`] takes
/// by its lowercased name.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExistingModel {
    /// Resolved name: the sidecar's `name =` override, else the file stem.
    pub name: String,
    /// `project` or the lock entry name that vendored it.
    pub owner: String,
}

/// Names the new package's models would collide with. `existing` maps every
/// model already in the project, by lowercased resolved name, to its
/// definitions. Names compare case-insensitively: warehouses fold unquoted
/// identifiers, and case-insensitive file systems cannot hold both files.
/// An owner in `replacing` (the package being re-vendored) is expected to be
/// replaced and never collides.
pub fn find_collisions(
    import: &PackageImport,
    existing: &BTreeMap<String, BTreeSet<ExistingModel>>,
    replacing: &BTreeSet<String>,
) -> Vec<Collision> {
    let mut out = Vec::new();
    for vm in &import.models {
        let Some(defs) = existing.get(&vm.model.name.to_lowercase()) else {
            continue;
        };
        for def in defs.iter().filter(|d| !replacing.contains(&d.owner)) {
            out.push(Collision {
                model: vm.model.name.clone(),
                existing: def.name.clone(),
                owner: def.owner.clone(),
            });
        }
    }
    out
}

/// Package models whose names differ only by case (or match exactly, from two
/// owner packages). Such a pair cannot be vendored side by side.
pub fn case_duplicates(import: &PackageImport) -> Vec<(String, String)> {
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    let mut out = Vec::new();
    for vm in &import.models {
        let name = vm.model.name.as_str();
        match seen.get(&name.to_lowercase()) {
            Some(first) => out.push(((*first).to_string(), name.to_string())),
            None => {
                seen.insert(name.to_lowercase(), name);
            }
        }
    }
    out
}

/// The lowercased `catalog.schema.table` a target writes; an empty table
/// defaults to the model name, as the model loader does.
pub fn target_key(target: &TargetConfig, model_name: &str) -> String {
    let table = if target.table.is_empty() {
        model_name
    } else {
        target.table.as_str()
    };
    format!("{}.{}.{}", target.catalog, target.schema, table).to_lowercase()
}

/// A package model whose `[target]` table another model already writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TargetCollision {
    /// The package model.
    pub model: String,
    /// The lowercased `catalog.schema.table` both write.
    pub target: String,
    /// The other model's name.
    pub existing: String,
    /// `project`, a lock entry name, or `this package` for two models of the
    /// package being vendored.
    pub owner: String,
}

/// Target tables written twice: by two models of `import`, or by a model of
/// `import` and an existing model in `existing` (keyed by [`target_key`]),
/// except one owned by a package in `replacing`.
pub fn find_target_collisions(
    import: &PackageImport,
    existing: &BTreeMap<String, BTreeSet<ExistingModel>>,
    replacing: &BTreeSet<String>,
) -> Vec<TargetCollision> {
    let mut out = Vec::new();
    let mut seen: BTreeMap<String, &str> = BTreeMap::new();
    for vm in &import.models {
        let key = target_key(&vm.model.config.target, &vm.model.name);
        if let Some(first) = seen.get(&key) {
            out.push(TargetCollision {
                model: vm.model.name.clone(),
                target: key.clone(),
                existing: (*first).to_string(),
                owner: "this package".to_string(),
            });
        } else {
            seen.insert(key.clone(), &vm.model.name);
        }
        for def in existing
            .get(&key)
            .into_iter()
            .flatten()
            .filter(|d| !replacing.contains(&d.owner))
        {
            out.push(TargetCollision {
                model: vm.model.name.clone(),
                target: key.clone(),
                existing: def.name.clone(),
                owner: def.owner.clone(),
            });
        }
    }
    out
}

/// The name a model file resolves to: `name =` in its `<stem>.toml` sidecar
/// or its `---toml` frontmatter, else the file stem. Mirrors the model
/// loader's precedence without its env-var substitution, which a model name
/// does not use in practice.
pub fn resolved_model_name(model_path: &Path) -> Option<String> {
    let stem = model_path.file_stem()?.to_str()?.to_string();
    let declared = |toml_src: &str| -> Option<String> {
        let value: toml::Value = toml::from_str(toml_src).ok()?;
        value.get("name")?.as_str().map(str::to_string)
    };
    let sidecar = model_path.with_extension("toml");
    if let Ok(text) = std::fs::read_to_string(&sidecar) {
        return Some(declared(&text).unwrap_or(stem));
    }
    if let Ok(text) = std::fs::read_to_string(model_path)
        && let Some(rest) = text.trim_start().strip_prefix("---toml")
        && let Some((front, _)) = rest.split_once("\n---")
    {
        return Some(declared(front).unwrap_or(stem));
    }
    Some(stem)
}

/// Content hash recorded in the lockfile (`blake3:<hex>`).
pub fn content_hash(content: &str) -> String {
    hash_bytes(content.as_bytes())
}

/// [`content_hash`] of raw file bytes. `\r\n` is folded to `\n` first, so a
/// checkout with `core.autocrlf` does not make every vendored file look
/// edited. Bytes need not be UTF-8: a file saved in another encoding still
/// hashes, and so still reads as edited rather than as absent.
pub fn hash_bytes(bytes: &[u8]) -> String {
    let mut normalized = Vec::with_capacity(bytes.len());
    let mut iter = bytes.iter().peekable();
    while let Some(&b) = iter.next() {
        if b == b'\r' && iter.peek() == Some(&&b'\n') {
            continue;
        }
        normalized.push(b);
    }
    format!("blake3:{}", blake3::hash(&normalized).to_hex())
}

/// What is on disk at a vendored path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiskFile {
    /// No file.
    Absent,
    /// A file with this [`hash_bytes`] hash.
    Hash(String),
    /// A file that exists but could not be read. Treated as edited: it is
    /// never overwritten or deleted.
    Unreadable,
}

/// Probe a file for [`plan_update`]. Only `NotFound` counts as absent.
pub fn read_disk(path: &Path) -> DiskFile {
    match std::fs::read(path) {
        Ok(bytes) => DiskFile::Hash(hash_bytes(&bytes)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => DiskFile::Absent,
        Err(_) => DiskFile::Unreadable,
    }
}

/// Hash of the vars a package was compiled with (order-independent).
pub fn vars_hash(vars: &BTreeMap<String, String>) -> String {
    let canonical: String = vars.iter().map(|(k, v)| format!("{k}={v}\n")).collect();
    content_hash(&canonical)
}

/// `rocky-packages.lock`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PackagesLock {
    pub version: u32,
    #[serde(default, rename = "package")]
    pub packages: Vec<LockedPackage>,
}

/// One vendored package in the lockfile.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LockedPackage {
    /// dbt project name of the package (`stripe`); the directory under
    /// `models/packages/`.
    pub name: String,
    /// dbt Hub name (`fivetran/stripe`).
    pub hub: String,
    /// Version requirement as given to `add` (`>=1.0.0,<2.0.0`), or empty
    /// for "latest".
    #[serde(default)]
    pub version_spec: String,
    /// Version `dbt deps` resolved.
    pub version: String,
    pub dbt_version: String,
    /// Rocky adapter type dbt compiled against.
    pub adapter: String,
    /// Rocky adapter name in `rocky.toml`.
    #[serde(default)]
    pub adapter_name: String,
    /// dbt target schema used for the throwaway profile.
    #[serde(default)]
    pub target_schema: String,
    pub compiled_at: String,
    pub vars_hash: String,
    /// dbt vars (`key = "yaml value"`) replayed by `rocky package update`.
    #[serde(default)]
    pub vars: BTreeMap<String, String>,
    /// How the SQL was compiled; replayed by `rocky package update`.
    #[serde(default)]
    pub mode: BuildMode,
    /// Dependency packages whose models were vendored alongside.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub includes: Vec<String>,
    /// Raw source tables the package reads.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<PackageSource>,
    /// Project-relative path → hash of the content Rocky last wrote.
    #[serde(default)]
    pub files: BTreeMap<String, String>,
}

impl PackagesLock {
    /// Read the lockfile; an absent file is an empty lock.
    pub fn read(path: &Path) -> Result<Self, String> {
        match std::fs::read_to_string(path) {
            Ok(text) => {
                let lock: Self = toml::from_str(&text)
                    .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
                for pkg in &lock.packages {
                    if !is_safe_package_name(&pkg.name) {
                        return Err(format!(
                            "{}: package name {:?} is not a safe directory name",
                            path.display(),
                            pkg.name
                        ));
                    }
                    validate_vars(&pkg.vars)
                        .map_err(|e| format!("{}: package `{}`: {e}", path.display(), pkg.name))?;
                    if let Some(bad) = pkg.files.keys().find(|f| !is_vendored_path(f)) {
                        return Err(format!(
                            "{}: `{bad}` is not a path under {PACKAGES_DIR}/; refusing to \
                             read or delete it",
                            path.display()
                        ));
                    }
                }
                if lock.version > LOCK_VERSION {
                    return Err(format!(
                        "{} has lock version {}; this Rocky reads version {LOCK_VERSION}. Upgrade Rocky",
                        path.display(),
                        lock.version
                    ));
                }
                Ok(lock)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self {
                version: LOCK_VERSION,
                packages: Vec::new(),
            }),
            Err(e) => Err(format!("failed to read {}: {e}", path.display())),
        }
    }

    /// Render the lockfile, packages sorted by name.
    pub fn render(&self) -> Result<String, String> {
        let mut lock = self.clone();
        lock.version = LOCK_VERSION;
        lock.packages.sort_by(|a, b| a.name.cmp(&b.name));
        let body = toml::to_string_pretty(&lock).map_err(|e| e.to_string())?;
        Ok(format!(
            "# Generated by `rocky package`. Records what Rocky vendored under {PACKAGES_DIR}/\n\
             # and the hash of every file it wrote, so `rocky package update` can tell your\n\
             # edits from upstream changes. Commit this file.\n\n{body}"
        ))
    }

    /// Render and write the lockfile atomically (temp file + rename), so an
    /// interrupted write never leaves a half-written lock.
    pub fn write(&self, path: &Path) -> Result<(), String> {
        let text = self.render()?;
        let tmp = path.with_extension("lock.tmp");
        std::fs::write(&tmp, text)
            .map_err(|e| format!("failed to write {}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| format!("failed to replace {}: {e}", path.display()))
    }

    pub fn get(&self, name: &str) -> Option<&LockedPackage> {
        self.packages.iter().find(|p| p.name == name)
    }

    /// Insert or replace the entry for `pkg.name`.
    pub fn upsert(&mut self, pkg: LockedPackage) {
        self.packages.retain(|p| p.name != pkg.name);
        self.packages.push(pkg);
    }

    /// Package name that owns a vendored path, if any.
    pub fn owner_of(&self, path: &str) -> Option<&str> {
        self.packages
            .iter()
            .find(|p| p.files.contains_key(path))
            .map(|p| p.name.as_str())
    }
}

/// What `rocky package update` (and a re-`add`) does to each file.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UpdatePlan {
    /// New or changed files whose on-disk copy is untouched: written in place.
    pub write: BTreeMap<String, String>,
    /// Files changed upstream AND edited locally: the new upstream content is
    /// written to `<path>.incoming`; the user's file is left alone.
    pub incoming: BTreeMap<String, String>,
    /// Files no longer produced upstream whose on-disk copy is untouched:
    /// deleted.
    pub delete: Vec<String>,
    /// Edited files kept as they are (no upstream change, or removed upstream).
    pub kept_edited: Vec<String>,
    /// Files identical to what is already on disk.
    pub unchanged: Vec<String>,
    /// Hashes to record in the lockfile: always the hash of the latest
    /// upstream content, so a later update compares against it.
    pub lock_files: BTreeMap<String, String>,
}

/// Three-way compare: `planned` (fresh upstream render) vs `locked` (hash of
/// what Rocky last wrote) vs `disk` (current file content, `None` if absent).
///
/// | on disk vs lock | upstream vs lock | action |
/// |---|---|---|
/// | same (clean) | changed or new | write |
/// | same | same | unchanged |
/// | edited | changed | write `.incoming`, keep user file |
/// | edited | same | keep user file |
/// | clean | removed | delete |
/// | edited | removed | keep user file |
/// | absent | any | write (a deleted vendored file is restored) |
///
/// A file not in the lock but present on disk (the user created a file with
/// a vendored name before this run) is treated as edited, so it is never
/// overwritten silently.
pub fn plan_update(
    planned: &BTreeMap<String, String>,
    locked: &BTreeMap<String, String>,
    disk: &dyn Fn(&str) -> DiskFile,
) -> UpdatePlan {
    let mut plan = UpdatePlan::default();
    for (path, content) in planned {
        let new_hash = content_hash(content);
        plan.lock_files.insert(path.clone(), new_hash.clone());
        let current_hash = match disk(path) {
            DiskFile::Absent => {
                plan.write.insert(path.clone(), content.clone());
                continue;
            }
            DiskFile::Hash(h) => Some(h),
            DiskFile::Unreadable => None,
        };
        if current_hash.as_ref() == Some(&new_hash) {
            plan.unchanged.push(path.clone());
        } else if current_hash.is_some() && locked.get(path) == current_hash.as_ref() {
            plan.write.insert(path.clone(), content.clone());
        } else if locked.get(path) == Some(&new_hash) {
            // Upstream did not change; the user's edit stands.
            plan.kept_edited.push(path.clone());
        } else {
            plan.incoming.insert(path.clone(), content.clone());
        }
    }
    for (path, locked_hash) in locked {
        if planned.contains_key(path) {
            continue;
        }
        match disk(path) {
            DiskFile::Absent => {}
            DiskFile::Hash(h) if &h == locked_hash => plan.delete.push(path.clone()),
            DiskFile::Hash(_) | DiskFile::Unreadable => plan.kept_edited.push(path.clone()),
        }
    }
    // A model is a `.sql` + `.toml` pair. When upstream removes it and you
    // edited one half, keep both: a lone `.sql` would load with a default
    // config (no target, no strategy).
    let model_key = |p: &str| -> String {
        p.strip_suffix(".sql")
            .or_else(|| p.strip_suffix(".toml"))
            .unwrap_or(p)
            .to_string()
    };
    let kept_models: BTreeSet<String> = plan.kept_edited.iter().map(|p| model_key(p)).collect();
    let (keep, delete): (Vec<String>, Vec<String>) = std::mem::take(&mut plan.delete)
        .into_iter()
        .partition(|p| kept_models.contains(&model_key(p)));
    plan.delete = delete;
    for p in keep {
        // Still Rocky's content: keep recording it so a later update can
        // delete the pair once the edited half is clean again.
        if let Some(h) = locked.get(&p) {
            plan.lock_files.insert(p.clone(), h.clone());
        }
        plan.kept_edited.push(p);
    }
    plan.kept_edited.sort();
    plan
}

/// Model names the lock's files declare (`<dir>/<name>.sql`).
pub fn locked_model_names(pkg: &LockedPackage) -> BTreeSet<String> {
    pkg.files
        .keys()
        .filter_map(|p| p.strip_suffix(".sql"))
        .filter_map(|p| p.rsplit('/').next())
        .map(str::to_string)
        .collect()
}

/// Parse `package-lock.yml` (written by `dbt deps`, dbt 1.7+) and return
/// `(project name, resolved version)` for the hub package `hub`.
pub fn resolve_from_package_lock(text: &str, hub: &str) -> Result<(String, String), String> {
    #[derive(Deserialize)]
    struct Lock {
        #[serde(default)]
        packages: Vec<Entry>,
    }
    #[derive(Deserialize)]
    struct Entry {
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        package: Option<String>,
        #[serde(default)]
        version: Option<serde_yaml::Value>,
    }
    let lock: Lock =
        serde_yaml::from_str(text).map_err(|e| format!("failed to parse package-lock.yml: {e}"))?;
    let entry = lock
        .packages
        .into_iter()
        .find(|e| {
            e.package
                .as_deref()
                .is_some_and(|p| p.eq_ignore_ascii_case(hub))
        })
        .ok_or_else(|| format!("package-lock.yml has no entry for `{hub}`"))?;
    let name = entry.name.filter(|n| !n.is_empty()).ok_or_else(|| {
        format!(
            "package-lock.yml has no `name` for `{hub}`; dbt-core 1.8 or newer writes it — upgrade dbt"
        )
    })?;
    let version = match entry.version {
        Some(serde_yaml::Value::String(s)) => s,
        Some(serde_yaml::Value::Number(n)) => n.to_string(),
        _ => String::new(),
    };
    Ok((name, version))
}

// ---------------------------------------------------------------------------
// The throwaway dbt project
// ---------------------------------------------------------------------------

/// Text dbt-utils' `star()` writes in place of a column list when the relation
/// it introspects does not exist yet. A model compiled with it selects `*`
/// where the package meant an explicit list, which is plausible-looking and
/// wrong (it broke a `UNION ALL` in `fivetran/stripe`), so such a model is
/// refused rather than vendored.
pub const INTROSPECTION_PLACEHOLDER: &str =
    "No columns were returned. Maybe the relation doesn't exist yet";

/// Marker dbt-utils' `star()` writes instead of a column list when it runs
/// (`dbt run`, `dbt build`) and the relation it introspects has no columns.
/// `dbt compile` writes [`INTROSPECTION_PLACEHOLDER`] instead.
pub const INTROSPECTION_RUN_MARKER: &str = "no columns returned from star() macro";

/// True when `sql` carries either dbt-utils `star()` placeholder.
pub fn has_introspection_placeholder(sql: &str) -> bool {
    let lower = sql.to_lowercase();
    lower.contains(&INTROSPECTION_PLACEHOLDER.to_lowercase())
        || lower.contains(INTROSPECTION_RUN_MARKER)
}

/// True when the model's output reads a relation yet every output column
/// that carries data is a NULL literal: at least two columns trace, through
/// CTEs and subqueries, to `NULL` / `CAST(NULL AS t)`, and the rest are
/// constants. That is what column-filling macros such as Fivetran's
/// `fill_staging_columns` compile to when the relation they introspect does
/// not exist: each staging column becomes `cast(null as ...)` in a `fields`
/// CTE (beside a constant `source_relation`), and the final SELECT renames
/// them. The SQL runs and loads only NULLs.
///
/// Only the model's OUTPUT is judged, so an all-NULL padding CTE joined to
/// real columns is not flagged. A column the analysis cannot follow (a
/// function call, a physical table's column, a `*` over a physical table)
/// counts as data, so uncertainty never refuses a model. SQL that does not
/// parse is not flagged.
pub fn projects_only_nulls(sql: &str) -> bool {
    use sqlparser::ast::Statement;
    let Ok(statements) = rocky_sql::parser::parse_sql(sql) else {
        return false;
    };
    let Some(Statement::Query(query)) = statements.last() else {
        return false;
    };
    let (columns, reads) = null_flow::query_columns(query, &null_flow::Scope::default());
    let nulls = columns
        .iter()
        .filter(|c| c.state == null_flow::State::Null)
        .count();
    reads && nulls >= 2 && columns.iter().all(|c| c.state != null_flow::State::Data)
}

/// The dataflow behind [`projects_only_nulls`].
mod null_flow {
    use std::collections::HashMap;

    use sqlparser::ast::{
        Expr, Query, Select, SelectItem, SelectItemQualifiedWildcardKind, SetExpr, TableFactor,
        Value,
    };

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum State {
        /// Always NULL.
        Null,
        /// A constant (a non-NULL literal).
        Const,
        /// Anything else, including what the analysis cannot follow.
        Data,
    }

    #[derive(Debug, Clone)]
    pub struct Column {
        /// Lowercased output name; empty when the warehouse names it.
        pub name: String,
        pub state: State,
    }

    /// CTEs in scope, by lowercased name.
    #[derive(Debug, Clone, Default)]
    pub struct Scope {
        ctes: HashMap<String, Vec<Column>>,
    }

    /// One FROM relation: its alias (or name) and its columns when known.
    struct Relation {
        name: String,
        columns: Option<Vec<Column>>,
    }

    fn last_ident(name: &sqlparser::ast::ObjectName) -> String {
        name.0
            .last()
            .and_then(|p| p.as_ident())
            .map(|i| i.value.to_lowercase())
            .unwrap_or_default()
    }

    /// Output columns of `query`, and whether it reads any relation.
    pub fn query_columns(query: &Query, outer: &Scope) -> (Vec<Column>, bool) {
        let mut scope = outer.clone();
        if let Some(with) = &query.with {
            for cte in &with.cte_tables {
                let (mut cols, _) = query_columns(&cte.query, &scope);
                for (col, renamed) in cols.iter_mut().zip(&cte.alias.columns) {
                    col.name = renamed.name.value.to_lowercase();
                }
                scope.ctes.insert(cte.alias.name.value.to_lowercase(), cols);
            }
        }
        body_columns(&query.body, &scope)
    }

    fn body_columns(body: &SetExpr, scope: &Scope) -> (Vec<Column>, bool) {
        match body {
            SetExpr::Select(select) => select_columns(select, scope),
            SetExpr::Query(q) => query_columns(q, scope),
            SetExpr::SetOperation { left, right, .. } => {
                let (l, lr) = body_columns(left, scope);
                let (r, rr) = body_columns(right, scope);
                let cols = l
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let other = r.get(i).map_or(State::Data, |c| c.state);
                        let state = match (c.state, other) {
                            (State::Null, State::Null) => State::Null,
                            (State::Data, _) | (_, State::Data) => State::Data,
                            _ => State::Const,
                        };
                        Column {
                            name: c.name,
                            state,
                        }
                    })
                    .collect();
                (cols, lr || rr)
            }
            _ => (
                vec![Column {
                    name: String::new(),
                    state: State::Data,
                }],
                false,
            ),
        }
    }

    fn relation(factor: &TableFactor, scope: &Scope) -> Relation {
        match factor {
            TableFactor::Table { name, alias, .. } => {
                let table = last_ident(name);
                let columns = (name.0.len() == 1)
                    .then(|| scope.ctes.get(&table).cloned())
                    .flatten();
                Relation {
                    name: alias
                        .as_ref()
                        .map_or(table, |a| a.name.value.to_lowercase()),
                    columns,
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => Relation {
                name: alias
                    .as_ref()
                    .map(|a| a.name.value.to_lowercase())
                    .unwrap_or_default(),
                columns: Some(query_columns(subquery, scope).0),
            },
            _ => Relation {
                name: String::new(),
                columns: None,
            },
        }
    }

    fn select_columns(select: &Select, scope: &Scope) -> (Vec<Column>, bool) {
        let mut relations = Vec::new();
        for twj in &select.from {
            relations.push(relation(&twj.relation, scope));
            for join in &twj.joins {
                relations.push(relation(&join.relation, scope));
            }
        }
        let data = |name: String| Column {
            name,
            state: State::Data,
        };
        let mut out = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(e) => {
                    let name = match e {
                        Expr::Identifier(i) => i.value.to_lowercase(),
                        Expr::CompoundIdentifier(parts) => parts
                            .last()
                            .map(|i| i.value.to_lowercase())
                            .unwrap_or_default(),
                        _ => String::new(),
                    };
                    out.push(Column {
                        name,
                        state: classify(e, &relations),
                    });
                }
                SelectItem::ExprWithAlias { expr, alias } => out.push(Column {
                    name: alias.value.to_lowercase(),
                    state: classify(expr, &relations),
                }),
                SelectItem::Wildcard(_) => {
                    if relations.iter().all(|r| r.columns.is_some()) && !relations.is_empty() {
                        for r in &relations {
                            out.extend(r.columns.iter().flatten().cloned());
                        }
                    } else {
                        out.push(data(String::new()));
                    }
                }
                SelectItem::QualifiedWildcard(
                    SelectItemQualifiedWildcardKind::ObjectName(n),
                    _,
                ) => {
                    let q = last_ident(n);
                    match relations
                        .iter()
                        .find(|r| r.name == q)
                        .and_then(|r| r.columns.as_ref())
                    {
                        Some(cols) => out.extend(cols.iter().cloned()),
                        None => out.push(data(String::new())),
                    }
                }
                _ => out.push(data(String::new())),
            }
        }
        (out, !relations.is_empty())
    }

    fn classify(expr: &Expr, relations: &[Relation]) -> State {
        match expr {
            Expr::Value(v) if matches!(v.value, Value::Null) => State::Null,
            Expr::Value(_) => State::Const,
            Expr::Cast { expr, .. } | Expr::Nested(expr) => classify(expr, relations),
            Expr::Identifier(i) => lookup(None, &i.value, relations),
            Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
                lookup(Some(&parts[0].value), &parts[1].value, relations)
            }
            _ => State::Data,
        }
    }

    fn lookup(qualifier: Option<&str>, column: &str, relations: &[Relation]) -> State {
        let column = column.to_lowercase();
        let qualifier = qualifier.map(str::to_lowercase);
        let mut found = None;
        for r in relations {
            if qualifier.as_ref().is_some_and(|q| *q != r.name) {
                continue;
            }
            // A relation whose columns are unknown may hold the column.
            let Some(cols) = &r.columns else {
                return State::Data;
            };
            if let Some(c) = cols.iter().find(|c| c.name == column) {
                if found.is_some() {
                    return State::Data;
                }
                found = Some(c.state);
            }
        }
        found.unwrap_or(State::Data)
    }
}

/// How `rocky package` got the package's compiled SQL. Recorded in the
/// lockfile so `rocky package update` replays it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BuildMode {
    /// `dbt compile` only. Nothing is written to the warehouse. A package
    /// whose macros introspect upstream models is refused (E055).
    #[default]
    CompileOnly,
    /// `dbt run --empty` first (opt-in `--build-empty`): writes empty
    /// `rocky_package_build*` relations and runs package hooks.
    BuildEmpty,
    /// `--compiled <dir>`: a dbt project compiled elsewhere.
    Compiled,
}

impl BuildMode {
    pub fn as_str(self) -> &'static str {
        match self {
            BuildMode::CompileOnly => "compile-only",
            BuildMode::BuildEmpty => "build-empty",
            BuildMode::Compiled => "compiled",
        }
    }
}

/// Split `<namespace>/<name>[@<version-spec>]`. The spec is a comma-separated
/// requirement list (`>=1.0.0,<2.0.0`) or one exact version; empty means
/// "whatever the Hub resolves".
pub fn parse_package_spec(spec: &str) -> Result<(String, String), String> {
    let (hub, version) = match spec.split_once('@') {
        Some((h, v)) => (h.trim(), v.trim()),
        None => (spec.trim(), ""),
    };
    let valid = hub.split_once('/').is_some_and(|(ns, name)| {
        is_safe_package_name(ns) && is_safe_package_name(name) && !name.contains('/')
    });
    if !valid {
        return Err(format!(
            "`{spec}` is not a dbt Hub package; expected `<namespace>/<name>[@<version>]`, \
             e.g. `fivetran/stripe@>=1.0.0,<2.0.0`"
        ));
    }
    if version.contains(['\n', '"', '\'']) {
        return Err(format!(
            "version spec `{version}` contains a quote or newline"
        ));
    }
    Ok((hub.to_string(), version.to_string()))
}

/// `packages.yml` for the throwaway project.
pub fn render_packages_yml(hub: &str, version_spec: &str) -> String {
    let mut pkg = serde_yaml::Mapping::new();
    pkg.insert("package".into(), hub.into());
    let reqs: Vec<serde_yaml::Value> = version_spec
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(Into::into)
        .collect();
    if !reqs.is_empty() {
        pkg.insert("version".into(), serde_yaml::Value::Sequence(reqs));
    }
    let mut root = serde_yaml::Mapping::new();
    root.insert(
        "packages".into(),
        serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(pkg)]),
    );
    serde_yaml::to_string(&root).unwrap_or_default()
}

/// True when `s` carries Jinja template syntax.
pub fn has_jinja(s: &str) -> bool {
    s.contains("{{") || s.contains("{%") || s.contains("{#")
}

/// True when a var name looks like it holds a credential. Vars are stored in
/// clear text in `rocky-packages.lock`, which is committed.
pub fn secret_like_var(name: &str) -> bool {
    const WORDS: &[&str] = &[
        "token",
        "secret",
        "password",
        "passwd",
        "pwd",
        "key",
        "apikey",
        "credential",
        "credentials",
        "private",
    ];
    name.to_ascii_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|word| WORDS.contains(&word))
}

/// Refuse vars dbt would execute: dbt renders `dbt_project.yml`, and the
/// vars in it, through Jinja, so `{{ env_var('AWS_SECRET_ACCESS_KEY') }}` in
/// a value (from a flag or a lockfile someone else committed) would run.
pub fn validate_vars(vars: &BTreeMap<String, String>) -> Result<(), String> {
    for (k, v) in vars {
        if k.is_empty() || !k.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(format!("var key `{k}` must be letters, digits and `_`"));
        }
        if has_jinja(v) {
            return Err(format!(
                "var `{k}` contains Jinja template syntax (`{{{{`, `{{%` or `{{#`), which dbt \
                 would execute; pass a plain value"
            ));
        }
        serde_yaml::from_str::<serde_yaml::Value>(v)
            .map_err(|e| format!("var `{k}`: value is not valid YAML: {e}"))?;
    }
    Ok(())
}

/// Parse `--vars key=value` flags. Keys are identifiers; values keep their
/// text and are read as YAML when dbt sees them (`false`, `5`, `[a, b]`). A
/// name that looks like a credential is refused unless `allow_secret`.
pub fn parse_vars(
    flags: &[String],
    allow_secret: bool,
) -> Result<BTreeMap<String, String>, String> {
    let mut vars = BTreeMap::new();
    for flag in flags {
        let (k, v) = flag
            .split_once('=')
            .ok_or_else(|| format!("--vars `{flag}` is not `key=value`"))?;
        let k = k.trim();
        if !allow_secret && secret_like_var(k) {
            return Err(format!(
                "--vars `{k}` looks like a credential. Vars are stored in clear text in \
                 rocky-packages.lock; pass --allow-secret-var if it is not a secret"
            ));
        }
        vars.insert(k.to_string(), v.trim().to_string());
    }
    validate_vars(&vars).map_err(|e| format!("--vars: {e}"))?;
    Ok(vars)
}

/// `dbt_project.yml` for the throwaway project, carrying `vars` as dbt reads
/// them (each value parsed as YAML).
pub fn render_dbt_project_yml(
    name: &str,
    vars: &BTreeMap<String, String>,
) -> Result<String, String> {
    let mut root = serde_yaml::Mapping::new();
    root.insert("name".into(), name.into());
    root.insert("version".into(), "1.0.0".into());
    root.insert("config-version".into(), 2.into());
    root.insert("profile".into(), name.into());
    validate_vars(vars)?;
    if !vars.is_empty() {
        let mut map = serde_yaml::Mapping::new();
        for (k, v) in vars {
            let value: serde_yaml::Value = serde_yaml::from_str(v)
                .map_err(|e| format!("var `{k}`: value is not valid YAML: {e}"))?;
            map.insert(k.as_str().into(), value);
        }
        root.insert("vars".into(), serde_yaml::Value::Mapping(map));
    }
    serde_yaml::to_string(&root).map_err(|e| e.to_string())
}

/// A rendered `profiles.yml` plus the environment it reads secrets from.
/// Secrets never touch disk: the profile names an `env_var()` and the
/// caller sets it on the dbt process only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbtProfile {
    pub yaml: String,
    pub env: Vec<(String, String)>,
}

/// Rocky adapter types `rocky package add` can generate a dbt profile for.
pub const PROFILE_ADAPTERS: &[&str] =
    &["duckdb", "snowflake", "databricks", "bigquery", "postgres"];

/// The schema Rocky builds vendored models into when `--target-schema` is
/// not given: the schema a bare table name resolves to on a fresh
/// connection. `None` where there is no such default (BigQuery datasets).
pub fn default_target_schema(adapter_type: &str) -> Option<&'static str> {
    match adapter_type {
        "duckdb" => Some("main"),
        "postgres" => Some("public"),
        "snowflake" => Some("PUBLIC"),
        "databricks" => Some("default"),
        _ => None,
    }
}

/// Generate a one-target `profiles.yml` from a Rocky adapter block.
///
/// `dbt_schema` is the dbt target schema (where `dbt run --empty` builds its
/// empty relations). `duckdb_path` is the adapter's database file, already
/// resolved to an absolute path. Refuses adapters dbt has no profile mapping
/// for here.
pub fn render_profiles_yml(
    profile_name: &str,
    adapter: &rocky_core::config::AdapterConfig,
    dbt_schema: &str,
    duckdb_path: Option<&Path>,
) -> Result<DbtProfile, String> {
    use serde_yaml::{Mapping, Value};
    let mut out = Mapping::new();
    let mut env: Vec<(String, String)> = Vec::new();
    let mut secret_keys: BTreeSet<String> = BTreeSet::new();
    let absolute = |p: &str| -> Result<String, String> {
        std::path::absolute(p)
            .map(|a| a.display().to_string())
            .map_err(|e| format!("cannot resolve path `{p}`: {e}"))
    };
    let mut secret = |out: &mut Mapping, key: &str, value: &str, var: &str| {
        secret_keys.insert(key.to_string());
        env.push((var.to_string(), value.to_string()));
        out.insert(key.into(), format!("{{{{ env_var(\"{var}\") }}}}").into());
    };
    let need = |field: &str, v: Option<&String>| -> Result<String, String> {
        v.filter(|s| !s.is_empty()).cloned().ok_or_else(|| {
            format!(
                "the {} adapter has no `{field}`; dbt needs it to compile the package",
                adapter.adapter_type
            )
        })
    };
    out.insert("type".into(), adapter.adapter_type.as_str().into());
    out.insert("threads".into(), 4.into());
    match adapter.adapter_type.as_str() {
        "duckdb" => {
            let path = duckdb_path.ok_or_else(|| {
                "the duckdb adapter has no `path`. dbt must compile against the database that \
                 holds the package's source tables, and an in-memory database is empty; set \
                 `path` in the [adapter] block"
                    .to_string()
            })?;
            out.insert("path".into(), path.display().to_string().into());
            out.insert("schema".into(), dbt_schema.into());
        }
        "snowflake" => {
            out.insert(
                "account".into(),
                need("account", adapter.account.as_ref())?.into(),
            );
            out.insert(
                "user".into(),
                need("username", adapter.username.as_ref())?.into(),
            );
            out.insert(
                "database".into(),
                need("database", adapter.database.as_ref())?.into(),
            );
            if let Some(w) = &adapter.warehouse {
                out.insert("warehouse".into(), w.as_str().into());
            }
            if let Some(r) = &adapter.role {
                out.insert("role".into(), r.as_str().into());
            }
            out.insert("schema".into(), dbt_schema.into());
            if let Some(t) = &adapter.oauth_token {
                out.insert("authenticator".into(), "oauth".into());
                secret(
                    &mut out,
                    "token",
                    t.expose(),
                    "DBT_ENV_SECRET_ROCKY_SNOWFLAKE_TOKEN",
                );
            } else if let Some(k) = &adapter.private_key_path {
                // dbt runs in a temp directory; a relative path must stay
                // relative to where `rocky` was started.
                out.insert("private_key_path".into(), absolute(k)?.into());
            } else if let Some(p) = adapter.password.as_ref().or(adapter.pat.as_ref()) {
                secret(
                    &mut out,
                    "password",
                    p.expose(),
                    "DBT_ENV_SECRET_ROCKY_SNOWFLAKE_PASSWORD",
                );
            } else {
                return Err(
                    "the snowflake adapter has no oauth_token, private_key_path, password or pat"
                        .to_string(),
                );
            }
        }
        "databricks" => {
            let host = need("host", adapter.host.as_ref())?;
            let host = host
                .trim_start_matches("https://")
                .trim_start_matches("http://")
                .trim_end_matches('/')
                .to_string();
            out.insert("host".into(), host.into());
            out.insert(
                "http_path".into(),
                need("http_path", adapter.http_path.as_ref())?.into(),
            );
            if let Some(c) = &adapter.database {
                out.insert("catalog".into(), c.as_str().into());
            }
            out.insert("schema".into(), dbt_schema.into());
            if let Some(t) = &adapter.token {
                secret(
                    &mut out,
                    "token",
                    t.expose(),
                    "DBT_ENV_SECRET_ROCKY_DATABRICKS_TOKEN",
                );
            } else if let (Some(id), Some(cs)) = (&adapter.client_id, &adapter.client_secret) {
                out.insert("auth_type".into(), "oauth".into());
                out.insert("client_id".into(), id.as_str().into());
                secret(
                    &mut out,
                    "client_secret",
                    cs.expose(),
                    "DBT_ENV_SECRET_ROCKY_DATABRICKS_CLIENT_SECRET",
                );
            } else {
                return Err(
                    "the databricks adapter has no token or client_id + client_secret".to_string(),
                );
            }
        }
        "bigquery" => {
            out.insert(
                "project".into(),
                need("project_id", adapter.project_id.as_ref())?.into(),
            );
            out.insert("dataset".into(), dbt_schema.into());
            if let Some(l) = &adapter.location {
                out.insert("location".into(), l.as_str().into());
            }
            match adapter.extra.get("keyfile").and_then(|v| v.as_str()) {
                Some(keyfile) => {
                    out.insert("method".into(), "service-account".into());
                    out.insert("keyfile".into(), absolute(keyfile)?.into());
                }
                None => {
                    // Application Default Credentials, the same chain the
                    // Rocky BigQuery adapter uses.
                    out.insert("method".into(), "oauth".into());
                }
            }
        }
        "postgres" => {
            out.insert("host".into(), need("host", adapter.host.as_ref())?.into());
            let port = match adapter.extra.get("port") {
                Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(5432),
                Some(serde_json::Value::String(s)) => s.trim().parse().unwrap_or(5432),
                _ => 5432,
            };
            out.insert("port".into(), port.into());
            out.insert(
                "user".into(),
                need("username", adapter.username.as_ref())?.into(),
            );
            out.insert(
                "dbname".into(),
                need("database", adapter.database.as_ref())?.into(),
            );
            out.insert("schema".into(), dbt_schema.into());
            if let Some(p) = &adapter.password {
                secret(
                    &mut out,
                    "password",
                    p.expose(),
                    "DBT_ENV_SECRET_ROCKY_POSTGRES_PASSWORD",
                );
            } else {
                out.insert("password".into(), "".into());
            }
        }
        other => {
            return Err(format!(
                "no dbt profile mapping for adapter type `{other}`; `rocky package` supports {}. \
                 Compile the package yourself and pass `--compiled <dbt project dir>`",
                PROFILE_ADAPTERS.join(", ")
            ));
        }
    }
    // dbt renders profiles.yml through Jinja. Only the `env_var()` lookups
    // generated above may carry template syntax; a config value that does
    // would run as a template inside dbt.
    for (key, value) in &out {
        if let (Some(k), Some(v)) = (key.as_str(), value.as_str())
            && !secret_keys.contains(k)
            && has_jinja(v)
        {
            return Err(format!(
                "adapter field `{k}` contains Jinja template syntax, which dbt would execute; \
                 remove it"
            ));
        }
    }
    let mut outputs = Mapping::new();
    outputs.insert("rocky".into(), Value::Mapping(out));
    let mut profile = Mapping::new();
    profile.insert("target".into(), "rocky".into());
    profile.insert("outputs".into(), Value::Mapping(outputs));
    let mut root = Mapping::new();
    root.insert(profile_name.into(), Value::Mapping(profile));
    Ok(DbtProfile {
        yaml: serde_yaml::to_string(&root).map_err(|e| e.to_string())?,
        env,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::import::dbt_manifest::parse_manifest;

    /// A two-package manifest shaped like a compiled Fivetran package: a
    /// `stripe` package whose mart reads a staging model from a separate
    /// `stripe_source` package, a source with a var-resolved schema, generic
    /// tests (mappable and not), and an incremental model.
    fn fixture_manifest() -> serde_json::Value {
        serde_json::json!({
            "metadata": {
                "dbt_version": "1.12.5",
                "project_name": "rocky_vendor",
                "invocation_id": "inv-1"
            },
            "nodes": {
                "model.stripe_source.stg_stripe__charge": {
                    "unique_id": "model.stripe_source.stg_stripe__charge",
                    "name": "stg_stripe__charge",
                    "resource_type": "model",
                    "package_name": "stripe_source",
                    "compiled_code": "select id as charge_id, amount, status, customer_id from \"dev\".\"raw_stripe\".\"charge\"",
                    "raw_code": "select * from {{ source('stripe', 'charge') }}",
                    "depends_on": {"nodes": ["source.stripe_source.stripe.charge"], "macros": []},
                    "config": {"materialized": "table", "schema": "stg_stripe"},
                    "schema": "main_stg_stripe",
                    "database": "dev",
                    "relation_name": "\"dev\".\"main_stg_stripe\".\"stg_stripe__charge\""
                },
                "model.stripe_source.stg_stripe__customer": {
                    "unique_id": "model.stripe_source.stg_stripe__customer",
                    "name": "stg_stripe__customer",
                    "resource_type": "model",
                    "package_name": "stripe_source",
                    "compiled_code": "select id as customer_id, email from \"dev\".\"raw_stripe\".\"customer\"",
                    "raw_code": "select * from {{ source('stripe', 'customer') }}",
                    "depends_on": {"nodes": ["source.stripe_source.stripe.customer"], "macros": []},
                    "config": {"materialized": "table", "schema": "stg_stripe"},
                    "schema": "main_stg_stripe",
                    "database": "dev",
                    "relation_name": "\"dev\".\"main_stg_stripe\".\"stg_stripe__customer\""
                },
                "model.stripe.stripe__charges": {
                    "unique_id": "model.stripe.stripe__charges",
                    "name": "stripe__charges",
                    "resource_type": "model",
                    "package_name": "stripe",
                    "compiled_code": "select c.charge_id, c.amount, c.status, u.email from \"dev\".\"main_stg_stripe\".\"stg_stripe__charge\" c left join \"dev\".\"main_stg_stripe\".\"stg_stripe__customer\" u on c.customer_id = u.customer_id",
                    "raw_code": "select ... from {{ ref('stg_stripe__charge') }}",
                    "depends_on": {"nodes": ["model.stripe_source.stg_stripe__charge", "model.stripe_source.stg_stripe__customer"], "macros": []},
                    "config": {"materialized": "table", "schema": "stripe"},
                    "schema": "main_stripe",
                    "database": "dev",
                    "relation_name": "\"dev\".\"main_stripe\".\"stripe__charges\""
                },
                "model.other_pkg.unrelated": {
                    "unique_id": "model.other_pkg.unrelated",
                    "name": "unrelated",
                    "resource_type": "model",
                    "package_name": "other_pkg",
                    "compiled_code": "select 1 as x",
                    "raw_code": "select 1 as x",
                    "depends_on": {"nodes": [], "macros": []},
                    "config": {"materialized": "view"},
                    "schema": "main",
                    "database": "dev"
                },
                "test.stripe.not_null_stripe__charges_charge_id.abc": {
                    "unique_id": "test.stripe.not_null_stripe__charges_charge_id.abc",
                    "name": "not_null_stripe__charges_charge_id",
                    "resource_type": "test",
                    "package_name": "stripe",
                    "test_metadata": {"name": "not_null", "kwargs": {"column_name": "charge_id", "model": "{{ get_where_subquery(ref('stripe__charges')) }}"}, "namespace": null},
                    "attached_node": "model.stripe.stripe__charges",
                    "column_name": "charge_id",
                    "depends_on": {"nodes": ["model.stripe.stripe__charges"]},
                    "config": {"severity": "ERROR", "where": null}
                },
                "test.stripe.accepted_values_stripe__charges_status.def": {
                    "unique_id": "test.stripe.accepted_values_stripe__charges_status.def",
                    "name": "accepted_values_stripe__charges_status",
                    "resource_type": "test",
                    "package_name": "stripe",
                    "test_metadata": {"name": "accepted_values", "kwargs": {"column_name": "status", "values": ["succeeded", "failed"]}, "namespace": null},
                    "attached_node": "model.stripe.stripe__charges",
                    "column_name": "status",
                    "depends_on": {"nodes": ["model.stripe.stripe__charges"]},
                    "config": {"severity": "warn", "where": "amount > 0"}
                },
                "test.stripe_source.relationships_charge_customer.ghi": {
                    "unique_id": "test.stripe_source.relationships_charge_customer.ghi",
                    "name": "relationships_charge_customer",
                    "resource_type": "test",
                    "package_name": "stripe_source",
                    "test_metadata": {"name": "relationships", "kwargs": {"column_name": "customer_id", "to": "ref('stg_stripe__customer')", "field": "customer_id"}, "namespace": null},
                    "attached_node": "model.stripe_source.stg_stripe__charge",
                    "column_name": "customer_id",
                    "depends_on": {"nodes": ["model.stripe_source.stg_stripe__customer", "model.stripe_source.stg_stripe__charge"]},
                    "config": {"severity": "ERROR"}
                },
                "test.stripe.dbt_utils_unique_combination.jkl": {
                    "unique_id": "test.stripe.dbt_utils_unique_combination.jkl",
                    "name": "dbt_utils_unique_combination_of_columns_stripe__charges",
                    "resource_type": "test",
                    "package_name": "stripe",
                    "test_metadata": {"name": "unique_combination_of_columns", "kwargs": {"combination_of_columns": ["charge_id", "status"]}, "namespace": "dbt_utils"},
                    "attached_node": "model.stripe.stripe__charges",
                    "depends_on": {"nodes": ["model.stripe.stripe__charges"]},
                    "config": {"severity": "ERROR"}
                },
                "test.stripe_source.not_null_source_charge_id.mno": {
                    "unique_id": "test.stripe_source.not_null_source_charge_id.mno",
                    "name": "source_not_null_stripe_charge_id",
                    "resource_type": "test",
                    "package_name": "stripe_source",
                    "test_metadata": {"name": "not_null", "kwargs": {"column_name": "id"}, "namespace": null},
                    "attached_node": "source.stripe_source.stripe.charge",
                    "depends_on": {"nodes": ["source.stripe_source.stripe.charge"]},
                    "config": {"severity": "ERROR"}
                }
            },
            "sources": {
                "source.stripe_source.stripe.charge": {
                    "unique_id": "source.stripe_source.stripe.charge",
                    "name": "charge",
                    "source_name": "stripe",
                    "package_name": "stripe_source",
                    "database": "dev",
                    "schema": "raw_stripe",
                    "identifier": "charge"
                },
                "source.stripe_source.stripe.customer": {
                    "unique_id": "source.stripe_source.stripe.customer",
                    "name": "customer",
                    "source_name": "stripe",
                    "package_name": "stripe_source",
                    "database": "dev",
                    "schema": "raw_stripe",
                    "identifier": "customer"
                }
            }
        })
    }

    fn load_fixture() -> (DbtManifest, PackageManifestInfo, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, fixture_manifest().to_string()).unwrap();
        (
            parse_manifest(&path).unwrap(),
            parse_package_info(&path).unwrap(),
            dir,
        )
    }

    fn target() -> TargetConfig {
        TargetConfig {
            catalog: "dev".into(),
            schema: "main".into(),
            table: String::new(),
        }
    }

    fn import_json(json: &serde_json::Value) -> PackageImport {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json.to_string()).unwrap();
        let manifest = parse_manifest(&path).unwrap();
        let info = parse_package_info(&path).unwrap();
        import_package(&manifest, &info, "stripe", &target()).unwrap()
    }

    #[test]
    fn a_clean_import_has_no_blocking_problems() {
        assert_eq!(
            import_json(&fixture_manifest()).blocking,
            Vec::<String>::new()
        );
    }

    #[test]
    fn a_model_reading_a_refused_upstream_blocks_the_package() {
        let mut json = fixture_manifest();
        json["nodes"]["model.stripe_source.stg_stripe__charge"]["compiled_code"] =
            serde_json::Value::String(format!(
                "select\n*\n/* {INTROSPECTION_PLACEHOLDER} */\nfrom \"dev\".\"raw_stripe\".\"charge\""
            ));
        let import = import_json(&json);
        assert!(
            import.blocking.iter().any(|p| p
                .contains("`stripe__charges` reads `stg_stripe__charge`")
                && p.contains("failed to import")),
            "{:?}",
            import.blocking
        );
    }

    #[test]
    fn a_model_reading_a_dbt_seed_blocks_the_package() {
        let mut json = fixture_manifest();
        json["nodes"]["seed.stripe.country_codes"] = serde_json::json!({
            "unique_id": "seed.stripe.country_codes",
            "name": "country_codes",
            "resource_type": "seed",
            "package_name": "stripe",
            "depends_on": {"nodes": []},
            "config": {}
        });
        json["nodes"]["model.stripe.stripe__charges"]["depends_on"]["nodes"]
            .as_array_mut()
            .unwrap()
            .push("seed.stripe.country_codes".into());
        let import = import_json(&json);
        assert!(
            import
                .blocking
                .iter()
                .any(|p| p.contains("dbt seed `country_codes`")),
            "{:?}",
            import.blocking
        );
    }

    #[test]
    fn unparseable_vendored_sql_blocks_the_package() {
        let mut json = fixture_manifest();
        json["nodes"]["model.stripe.stripe__charges"]["compiled_code"] =
            serde_json::Value::String("select a,, from (((".to_string());
        let import = import_json(&json);
        assert!(
            import
                .blocking
                .iter()
                .any(|p| p.contains("`stripe__charges`") && p.contains("does not parse")),
            "{:?}",
            import.blocking
        );
    }

    #[test]
    fn selects_package_plus_upstream_dependency_package() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        let names: Vec<(&str, &str)> = import
            .models
            .iter()
            .map(|m| (m.model.name.as_str(), m.owner_package.as_str()))
            .collect();
        assert_eq!(
            names,
            vec![
                ("stg_stripe__charge", "stripe_source"),
                ("stg_stripe__customer", "stripe_source"),
                ("stripe__charges", "stripe"),
            ],
            "the unrelated package is not vendored; the source package is"
        );
    }

    #[test]
    fn upstream_refs_become_bare_names_and_models_share_one_schema() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        let mart = import
            .models
            .iter()
            .find(|m| m.model.name == "stripe__charges")
            .unwrap();
        assert!(
            mart.model.sql.contains("from stg_stripe__charge c"),
            "{}",
            mart.model.sql
        );
        assert!(
            !mart.model.sql.contains("main_stg_stripe"),
            "{}",
            mart.model.sql
        );
        assert_eq!(
            mart.model.config.target.schema, "main",
            "not dbt's per-folder `main_stripe`: bare reads must resolve"
        );
        assert_eq!(mart.model.config.target.catalog, "dev");
        assert_eq!(
            mart.model.config.depends_on,
            vec![
                "stg_stripe__charge".to_string(),
                "stg_stripe__customer".to_string()
            ]
        );
    }

    #[test]
    fn package_sources_are_declared_on_models_and_listed() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        let stg = import
            .models
            .iter()
            .find(|m| m.model.name == "stg_stripe__charge")
            .unwrap();
        assert_eq!(stg.model.config.sources.len(), 1);
        assert_eq!(stg.model.config.sources[0].schema, "raw_stripe");
        assert_eq!(stg.model.config.sources[0].table, "charge");
        assert!(
            stg.model.sql.contains("\"raw_stripe\".\"charge\""),
            "sources stay qualified"
        );
        let names: Vec<&str> = import.sources.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["stripe.charge", "stripe.customer"]);
    }

    #[test]
    fn numeric_accepted_values_become_a_numeric_expression() {
        let mut json = fixture_manifest();
        let t = &mut json["nodes"]["test.stripe.accepted_values_stripe__charges_status.def"];
        t["test_metadata"]["kwargs"]["column_name"] = "amount".into();
        t["test_metadata"]["kwargs"]["values"] = serde_json::json!([1, 2.5]);
        let import = import_json(&json);
        let charges = import
            .models
            .iter()
            .find(|m| m.model.name == "stripe__charges")
            .unwrap();
        let expr = charges
            .model
            .config
            .tests
            .iter()
            .find_map(|t| match &t.test_type {
                TestType::Expression { expression } => Some(expression.clone()),
                _ => None,
            })
            .expect("an expression test");
        assert_eq!(expr, "amount IS NULL OR amount IN (1, 2.5)");
    }

    #[test]
    fn a_test_filter_that_is_not_one_predicate_is_dropped() {
        let mut json = fixture_manifest();
        json["nodes"]["test.stripe.accepted_values_stripe__charges_status.def"]["config"]["where"] =
            "amount > 0; drop table x".into();
        let import = import_json(&json);
        assert!(
            import
                .tests_dropped
                .iter()
                .any(|d| d.test == "accepted_values_stripe__charges_status"),
            "{:?}",
            import.tests_dropped
        );
    }

    #[test]
    fn maps_canonical_tests_and_reports_the_rest() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        assert_eq!(
            import.tests_mapped, 3,
            "not_null + accepted_values + relationships"
        );
        let mart = import
            .models
            .iter()
            .find(|m| m.model.name == "stripe__charges")
            .unwrap();
        let av = mart
            .model
            .config
            .tests
            .iter()
            .find(|t| matches!(t.test_type, TestType::AcceptedValues { .. }))
            .unwrap();
        assert_eq!(av.severity, TestSeverity::Warning);
        assert_eq!(av.filter.as_deref(), Some("amount > 0"));
        let stg = import
            .models
            .iter()
            .find(|m| m.model.name == "stg_stripe__charge")
            .unwrap();
        match &stg.model.config.tests[0].test_type {
            TestType::Relationships {
                to_table,
                to_column,
            } => {
                assert_eq!(to_table, "dev.main.stg_stripe__customer");
                assert_eq!(to_column, "customer_id");
            }
            other => panic!("expected relationships, got {other:?}"),
        }
        let dropped: Vec<&str> = import
            .tests_dropped
            .iter()
            .map(|d| d.test.as_str())
            .collect();
        assert_eq!(
            dropped,
            vec![
                "dbt_utils_unique_combination_of_columns_stripe__charges",
                "source_not_null_stripe_charge_id"
            ]
        );
    }

    #[test]
    fn unknown_package_is_refused_and_names_the_known_ones() {
        let (manifest, info, _d) = load_fixture();
        let err = import_package(&manifest, &info, "hubspot", &target())
            .err()
            .unwrap();
        assert!(err.contains("no models for package 'hubspot'"), "{err}");
        assert!(err.contains("stripe"), "{err}");
    }

    #[test]
    fn collisions_with_project_and_other_packages_are_found() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        let def = |name: &str, owner: &str| ExistingModel {
            name: name.into(),
            owner: owner.into(),
        };
        let mut existing: BTreeMap<String, BTreeSet<ExistingModel>> = BTreeMap::new();
        // The package's own copy AND a project model: still a collision.
        existing.insert(
            "stripe__charges".to_string(),
            [
                def("stripe__charges", "stripe"),
                def("stripe__charges", "project"),
            ]
            .into(),
        );
        // Differs only by case: a collision.
        existing.insert(
            "stg_stripe__customer".to_string(),
            [def("STG_Stripe__Customer", "hubspot")].into(),
        );
        existing.insert(
            "stg_stripe__charge".to_string(),
            [def("stg_stripe__charge", "stripe")].into(),
        );
        let replacing: BTreeSet<String> = ["stripe".to_string()].into();
        let collisions = find_collisions(&import, &existing, &replacing);
        assert_eq!(
            collisions,
            vec![
                Collision {
                    model: "stg_stripe__customer".into(),
                    existing: "STG_Stripe__Customer".into(),
                    owner: "hubspot".into()
                },
                Collision {
                    model: "stripe__charges".into(),
                    existing: "stripe__charges".into(),
                    owner: "project".into()
                },
            ]
        );
    }

    #[test]
    fn target_collisions_within_the_package_and_with_existing_models_are_found() {
        let (manifest, info, _d) = load_fixture();
        let mut import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        assert!(find_target_collisions(&import, &BTreeMap::new(), &BTreeSet::new()).is_empty());
        // Two package models aliased onto one table.
        import.models[1].model.config.target.table = "Stg_Stripe__Charge".into();
        let mut existing: BTreeMap<String, BTreeSet<ExistingModel>> = BTreeMap::new();
        existing.insert(
            "dev.main.stripe__charges".into(),
            [ExistingModel {
                name: "my_charges".into(),
                owner: "project".into(),
            }]
            .into(),
        );
        let found = find_target_collisions(&import, &existing, &BTreeSet::new());
        let pairs: Vec<(&str, &str, &str)> = found
            .iter()
            .map(|c| (c.model.as_str(), c.existing.as_str(), c.owner.as_str()))
            .collect();
        assert!(
            pairs.contains(&("stripe__charges", "my_charges", "project")),
            "{found:?}"
        );
        assert!(
            found
                .iter()
                .any(|c| c.owner == "this package" && c.target == "dev.main.stg_stripe__charge"),
            "{found:?}"
        );
        // An existing model of the package being re-vendored does not collide.
        let replacing: BTreeSet<String> = ["project".to_string()].into();
        assert!(
            find_target_collisions(&import, &existing, &replacing)
                .iter()
                .all(|c| c.owner == "this package")
        );
    }

    #[test]
    fn package_models_differing_only_by_case_are_found() {
        let (manifest, info, _d) = load_fixture();
        let mut import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        assert!(case_duplicates(&import).is_empty());
        import.models[1].model.name = import.models[0].model.name.to_uppercase();
        let dups = case_duplicates(&import);
        assert_eq!(dups.len(), 1);
        assert!(dups[0].0.eq_ignore_ascii_case(&dups[0].1));
    }

    #[test]
    fn resolved_model_names_follow_the_sidecar_then_the_stem() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path();
        std::fs::write(p.join("a.sql"), "select 1").unwrap();
        std::fs::write(p.join("a.toml"), "name = \"orders\"\n").unwrap();
        std::fs::write(
            p.join("b.sql"),
            "---toml\nname = \"Customers\"\n---\nselect 1",
        )
        .unwrap();
        std::fs::write(p.join("c.sql"), "select 1").unwrap();
        std::fs::write(p.join("d.rocky"), "from x").unwrap();
        std::fs::write(p.join("d.toml"), "[strategy]\ntype = \"full_refresh\"\n").unwrap();
        assert_eq!(resolved_model_name(&p.join("a.sql")).unwrap(), "orders");
        assert_eq!(resolved_model_name(&p.join("b.sql")).unwrap(), "Customers");
        assert_eq!(resolved_model_name(&p.join("c.sql")).unwrap(), "c");
        assert_eq!(resolved_model_name(&p.join("d.rocky")).unwrap(), "d");
    }

    #[test]
    fn rendered_files_live_under_each_owning_package() {
        let (manifest, info, _d) = load_fixture();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        let files = render_package_files(&import).unwrap();
        let paths: Vec<&str> = files.keys().map(String::as_str).collect();
        assert!(paths.contains(&"models/packages/stripe/stripe__charges.sql"));
        assert!(paths.contains(&"models/packages/stripe_source/stg_stripe__charge.toml"));
        let sidecar = &files["models/packages/stripe_source/stg_stripe__charge.toml"];
        assert!(sidecar.contains("[[sources]]"), "{sidecar}");
        assert!(sidecar.contains("[[tests]]"), "{sidecar}");
        assert!(sidecar.contains("schema = \"main\""), "{sidecar}");
    }

    #[test]
    fn plan_update_protects_edited_files() {
        let old_clean = "select 1".to_string();
        let old_edited_base = "select 2".to_string();
        let removed_clean = "select 3".to_string();
        let removed_edited = "select 4".to_string();
        let same = "select 5".to_string();
        let mut locked = BTreeMap::new();
        locked.insert("a.sql".to_string(), content_hash(&old_clean));
        locked.insert("b.sql".to_string(), content_hash(&old_edited_base));
        locked.insert("c.sql".to_string(), content_hash(&removed_clean));
        locked.insert("d.sql".to_string(), content_hash(&removed_edited));
        locked.insert("e.sql".to_string(), content_hash(&same));
        locked.insert("f.sql".to_string(), content_hash("select 6"));

        let mut planned = BTreeMap::new();
        planned.insert("a.sql".to_string(), "select 1 -- v2".to_string());
        planned.insert("b.sql".to_string(), "select 2 -- v2".to_string());
        planned.insert("e.sql".to_string(), same.clone());
        planned.insert("f.sql".to_string(), "select 6".to_string());
        planned.insert("new.sql".to_string(), "select 7".to_string());

        let text = |p: &str| -> Option<String> {
            match p {
                "a.sql" => Some(old_clean.clone()),
                "b.sql" => Some("select 2 -- my edit".to_string()),
                "c.sql" => Some(removed_clean.clone()),
                "d.sql" => Some("select 4 -- my edit".to_string()),
                "e.sql" => Some(same.clone()),
                "f.sql" => Some("select 6 -- my edit".to_string()),
                _ => None,
            }
        };
        let disk = |p: &str| match text(p) {
            Some(t) => DiskFile::Hash(content_hash(&t)),
            None => DiskFile::Absent,
        };
        let plan = plan_update(&planned, &locked, &disk);
        assert_eq!(
            plan.write.keys().collect::<Vec<_>>(),
            vec!["a.sql", "new.sql"]
        );
        assert_eq!(plan.incoming.keys().collect::<Vec<_>>(), vec!["b.sql"]);
        assert_eq!(plan.delete, vec!["c.sql".to_string()]);
        assert_eq!(
            plan.kept_edited,
            vec!["d.sql".to_string(), "f.sql".to_string()]
        );
        assert_eq!(plan.unchanged, vec!["e.sql".to_string()]);
        assert_eq!(plan.lock_files["b.sql"], content_hash("select 2 -- v2"));
        assert!(!plan.lock_files.contains_key("c.sql"));
    }

    #[test]
    fn unlocked_file_on_disk_is_never_overwritten() {
        let mut planned = BTreeMap::new();
        planned.insert("x.sql".to_string(), "select 1".to_string());
        let plan = plan_update(&planned, &BTreeMap::new(), &|_| {
            DiskFile::Hash(content_hash("select 'mine'"))
        });
        assert!(plan.write.is_empty());
        assert_eq!(plan.incoming.keys().collect::<Vec<_>>(), vec!["x.sql"]);
    }

    #[test]
    fn unreadable_files_are_treated_as_edited() {
        let mut planned = BTreeMap::new();
        planned.insert("a.sql".to_string(), "select 2".to_string());
        let mut locked = BTreeMap::new();
        locked.insert("a.sql".to_string(), content_hash("select 1"));
        locked.insert("gone.sql".to_string(), content_hash("select 9"));
        let plan = plan_update(&planned, &locked, &|_| DiskFile::Unreadable);
        assert!(plan.write.is_empty());
        assert!(plan.delete.is_empty());
        assert_eq!(plan.incoming.keys().collect::<Vec<_>>(), vec!["a.sql"]);
        assert_eq!(plan.kept_edited, vec!["gone.sql".to_string()]);
    }

    #[test]
    fn a_removed_model_pair_is_kept_whole_when_one_half_is_edited() {
        let mut locked = BTreeMap::new();
        locked.insert("m.sql".to_string(), content_hash("select 1"));
        locked.insert("m.toml".to_string(), content_hash("[strategy]"));
        let disk = |p: &str| match p {
            "m.sql" => DiskFile::Hash(content_hash("select 1 -- mine")),
            _ => DiskFile::Hash(content_hash("[strategy]")),
        };
        let plan = plan_update(&BTreeMap::new(), &locked, &disk);
        assert!(plan.delete.is_empty(), "{plan:?}");
        assert_eq!(
            plan.kept_edited,
            vec!["m.sql".to_string(), "m.toml".to_string()]
        );
        assert!(plan.lock_files.contains_key("m.toml"));
    }

    #[test]
    fn crlf_checkouts_hash_like_the_written_file() {
        assert_eq!(
            hash_bytes(b"select 1\r\nfrom t\r\n"),
            content_hash("select 1\nfrom t\n")
        );
        assert_ne!(
            hash_bytes(b"select 1\rfrom t"),
            content_hash("select 1\nfrom t")
        );
        assert!(hash_bytes(&[0xff, 0xfe, 0x00]).starts_with("blake3:"));
    }

    #[test]
    fn lockfile_round_trips() {
        let mut lock = PackagesLock {
            version: LOCK_VERSION,
            packages: vec![],
        };
        let mut vars = BTreeMap::new();
        vars.insert("stripe_schema".to_string(), "raw_stripe".to_string());
        lock.upsert(LockedPackage {
            name: "stripe".into(),
            hub: "fivetran/stripe".into(),
            version_spec: ">=1.0.0,<2.0.0".into(),
            version: "1.10.1".into(),
            dbt_version: "1.12.5".into(),
            adapter: "duckdb".into(),
            adapter_name: "default".into(),
            target_schema: "main".into(),
            compiled_at: "2026-10-04T00:00:00Z".into(),
            vars_hash: vars_hash(&vars),
            vars,
            mode: BuildMode::BuildEmpty,
            includes: vec![],
            sources: vec![PackageSource {
                name: "stripe.charge".into(),
                catalog: "dev".into(),
                schema: "raw_stripe".into(),
                table: "charge".into(),
            }],
            files: [(
                "models/packages/stripe/a.sql".to_string(),
                content_hash("x"),
            )]
            .into_iter()
            .collect(),
        });
        let text = lock.render().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LOCKFILE_NAME);
        std::fs::write(&path, &text).unwrap();
        assert_eq!(PackagesLock::read(&path).unwrap(), lock);
        assert_eq!(
            lock.owner_of("models/packages/stripe/a.sql"),
            Some("stripe")
        );
        let absent = PackagesLock::read(&dir.path().join("nope.lock")).unwrap();
        assert!(absent.packages.is_empty());
    }

    #[test]
    fn vars_hash_is_order_independent_and_value_sensitive() {
        let a: BTreeMap<String, String> = [("b", "2"), ("a", "1")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let b: BTreeMap<String, String> = [("a", "1"), ("b", "2")]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(vars_hash(&a), vars_hash(&b));
        let mut c = b.clone();
        c.insert("a".into(), "3".into());
        assert_ne!(vars_hash(&a), vars_hash(&c));
    }

    #[test]
    fn package_lock_yml_resolution() {
        let text = "packages:\n  - name: stripe\n    package: fivetran/stripe\n    version: 1.10.1\n  - name: fivetran_utils\n    package: fivetran/fivetran_utils\n    version: 0.4.13\nsha1_hash: abc\n";
        assert_eq!(
            resolve_from_package_lock(text, "fivetran/stripe").unwrap(),
            ("stripe".to_string(), "1.10.1".to_string())
        );
        assert!(resolve_from_package_lock(text, "fivetran/hubspot").is_err());
        let old = "packages:\n  - package: fivetran/stripe\n    version: 1.10.1\n";
        assert!(
            resolve_from_package_lock(old, "fivetran/stripe")
                .unwrap_err()
                .contains("upgrade dbt")
        );
    }

    #[test]
    fn lock_paths_outside_the_packages_dir_are_refused() {
        assert!(is_vendored_path("models/packages/stripe/a.sql"));
        assert!(
            !is_vendored_path("models/packages/a.sql"),
            "needs a package dir"
        );
        assert!(!is_vendored_path("models/packages/../../etc/passwd"));
        assert!(!is_vendored_path("models/packages/stripe/../../x.sql"));
        assert!(!is_vendored_path("/etc/passwd"));
        assert!(!is_vendored_path("models/x.sql"));
        assert!(!is_vendored_path("models/packages/stripe//a.sql"));
        assert!(!is_vendored_path("models/packages/stripe/C:\\x"));
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LOCKFILE_NAME);
        std::fs::write(
            &path,
            "version = 1\n[[package]]\nname = \"stripe\"\nhub = \"fivetran/stripe\"\n\
             version = \"1\"\ndbt_version = \"1\"\nadapter = \"duckdb\"\ncompiled_at = \"\"\n\
             vars_hash = \"\"\n[package.files]\n\"models/packages/../../x\" = \"h\"\n",
        )
        .unwrap();
        let err = PackagesLock::read(&path).unwrap_err();
        assert!(err.contains("refusing"), "{err}");
    }

    #[test]
    fn package_names_are_path_safe() {
        assert!(is_safe_package_name("stripe"));
        assert!(is_safe_package_name("fivetran_log"));
        assert!(!is_safe_package_name("../x"));
        assert!(!is_safe_package_name("a/b"));
        assert!(!is_safe_package_name(""));
    }

    #[test]
    fn introspection_placeholder_models_are_refused() {
        let mut json = fixture_manifest();
        json["nodes"]["model.stripe.stripe__charges"]["compiled_code"] = serde_json::Value::String(
            "select\n*\n/* No columns were returned. Maybe the relation doesn't exist yet\nor all columns were excluded. */\nfrom \"dev\".\"main_stg_stripe\".\"stg_stripe__charge\"".to_string(),
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("manifest.json");
        std::fs::write(&path, json.to_string()).unwrap();
        let manifest = parse_manifest(&path).unwrap();
        let info = parse_package_info(&path).unwrap();
        let import = import_package(&manifest, &info, "stripe", &target()).unwrap();
        assert!(
            import
                .models
                .iter()
                .all(|m| m.model.name != "stripe__charges")
        );
        assert!(
            import
                .failed
                .iter()
                .any(|f| f.name == "stripe__charges" && f.reason.contains("--build-empty"))
        );
        assert_eq!(
            import.introspection_refused,
            vec!["stripe__charges".to_string()]
        );
        assert_eq!(import.models.len(), 2, "the clean upstreams still vendor");
    }

    #[test]
    fn all_null_projections_are_detected() {
        // fivetran/stripe 1.10.1 `stg_stripe__charge`, compiled without
        // `dbt run --empty`.
        let compiled = "with base as (select * from \"dev\".\"s\".\"stg_stripe__charge_tmp\"), \
                        fields as (select cast(null as timestamp) as _fivetran_synced, \
                        cast(null as integer) as amount, cast(null as TEXT) as id from base) \
                        select id, amount from fields";
        assert!(projects_only_nulls(compiled));
        assert!(projects_only_nulls("select null as a, (null) as b from t"));
        // Real columns, a single NULL, no FROM, and a filler UNION branch stay clean.
        assert!(!projects_only_nulls(
            "select cast(null as int) as a, id from t"
        ));
        assert!(!projects_only_nulls("select cast(null as int) as a from t"));
        assert!(!projects_only_nulls("select null as a, null as b"));
        assert!(!projects_only_nulls(
            "select a, b from t union all select null as a, null as b from u"
        ));
        assert!(!projects_only_nulls("not sql at all ((("));
    }

    /// The shape fivetran/stripe 1.10.1 really compiles to without
    /// `dbt run --empty`: an all-NULL `fields` CTE beside a constant
    /// `source_relation`, renamed by `final`, read by `select * from final`.
    #[test]
    fn the_fivetran_staging_shape_is_detected_through_the_cte_chain() {
        let compiled = "with base as (select * from \"dev\".\"s\".\"stg_stripe__charge_tmp\"), \
             fields as (select cast(null as integer) as amount, cast(null as TEXT) as id, \
             cast(null as timestamp) as created, cast('dev.stripe' as TEXT) as source_relation \
             from base), \
             final as (select id as charge_id, amount as amount, cast(created as timestamp) \
             as created_at, source_relation from fields where cast(livemode as boolean) = True) \
             select * from final";
        assert!(projects_only_nulls(compiled));
    }

    /// Red-team case: an all-NULL padding CTE is fine when the output also
    /// carries real columns.
    #[test]
    fn an_all_null_padding_cte_beside_real_columns_is_not_flagged() {
        let sql = "with base as (select id, name from raw.t), \
                   pad as (select cast(null as varchar) a, cast(null as varchar) b from base) \
                   select base.id, base.name, pad.a, pad.b from base cross join pad";
        assert!(!projects_only_nulls(sql));
        let unioned = "with pad as (select cast(null as varchar) a, cast(null as varchar) b \
                       from raw.t) select a, b from raw.u union all select a, b from pad";
        assert!(!projects_only_nulls(unioned));
    }

    #[test]
    fn both_star_placeholders_are_detected() {
        assert!(has_introspection_placeholder(
            "select /* no columns returned from star() macro */ from x"
        ));
        assert!(has_introspection_placeholder(&format!(
            "select * /* {INTROSPECTION_PLACEHOLDER} */ from x"
        )));
        assert!(!has_introspection_placeholder("select * from x"));
    }

    #[test]
    fn package_spec_parsing() {
        assert_eq!(
            parse_package_spec("fivetran/stripe").unwrap(),
            ("fivetran/stripe".to_string(), String::new())
        );
        assert_eq!(
            parse_package_spec("fivetran/stripe@>=1.0.0,<2.0.0").unwrap(),
            ("fivetran/stripe".to_string(), ">=1.0.0,<2.0.0".to_string())
        );
        assert!(parse_package_spec("stripe").is_err());
        assert!(parse_package_spec("fivetran/../x").is_err());
        assert!(parse_package_spec("a/b@1\"\nx").is_err());
        let yml = render_packages_yml("fivetran/stripe", ">=1.0.0, <2.0.0");
        let v: serde_yaml::Value = serde_yaml::from_str(&yml).unwrap();
        assert_eq!(v["packages"][0]["package"], "fivetran/stripe");
        assert_eq!(v["packages"][0]["version"][1], "<2.0.0");
        let latest: serde_yaml::Value =
            serde_yaml::from_str(&render_packages_yml("fivetran/stripe", "")).unwrap();
        assert!(latest["packages"][0].get("version").is_none());
    }

    #[test]
    fn vars_are_typed_as_yaml_in_dbt_project() {
        let vars = parse_vars(
            &[
                "stripe_schema=raw_stripe".to_string(),
                "stripe__using_invoices=false".to_string(),
                "stripe_sources=[a, b]".to_string(),
            ],
            false,
        )
        .unwrap();
        let yml = render_dbt_project_yml("rocky_package_build", &vars).unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&yml).unwrap();
        assert_eq!(v["vars"]["stripe_schema"], "raw_stripe");
        assert_eq!(v["vars"]["stripe__using_invoices"], false);
        assert_eq!(v["vars"]["stripe_sources"][1], "b");
        assert_eq!(v["profile"], "rocky_package_build");
        assert!(parse_vars(&["noequals".to_string()], false).is_err());
        assert!(parse_vars(&["bad key=1".to_string()], false).is_err());
    }

    #[test]
    fn jinja_in_vars_is_refused_from_flags_the_lockfile_and_the_render() {
        let injected = "x={{ env_var('AWS_SECRET_ACCESS_KEY') }}".to_string();
        let err = parse_vars(&[injected], false).unwrap_err();
        assert!(err.contains("Jinja"), "{err}");
        for v in ["{% raw %}", "{# c #}", "\"{{ 1 }}\""] {
            assert!(parse_vars(&[format!("x={v}")], false).is_err(), "{v}");
        }
        let mut vars = BTreeMap::new();
        vars.insert("x".to_string(), "{{ env_var('HOME') }}".to_string());
        assert!(render_dbt_project_yml("p", &vars).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(LOCKFILE_NAME);
        std::fs::write(
            &path,
            "version = 1\n[[package]]\nname = \"stripe\"\nhub = \"fivetran/stripe\"\n\
             version = \"1\"\ndbt_version = \"1\"\nadapter = \"duckdb\"\ncompiled_at = \"\"\n\
             vars_hash = \"\"\n[package.vars]\nx = \"{{ env_var('HOME') }}\"\n",
        )
        .unwrap();
        let err = PackagesLock::read(&path).unwrap_err();
        assert!(err.contains("Jinja"), "{err}");
    }

    #[test]
    fn credential_like_var_names_need_an_explicit_opt_in() {
        for name in [
            "api_key",
            "stripe_token",
            "db_password",
            "SECRET",
            "private_key_path",
        ] {
            let flag = format!("{name}=x");
            assert!(
                parse_vars(std::slice::from_ref(&flag), false).is_err(),
                "{name}"
            );
            assert!(parse_vars(&[flag], true).is_ok(), "{name}");
        }
        for name in [
            "stripe_schema",
            "stripe__using_invoices",
            "monkey",
            "tokens_table",
        ] {
            assert!(parse_vars(&[format!("{name}=x")], false).is_ok(), "{name}");
        }
    }

    #[test]
    fn jinja_in_an_adapter_field_is_refused_and_key_paths_are_absolute() {
        let a = adapter(
            "type = \"snowflake\"\naccount = \"{{ env_var('X') }}\"\nusername = \"u\"\ndatabase = \"DB\"\npassword = \"p\"\n",
        );
        let err = render_profiles_yml("p", &a, "S", None).unwrap_err();
        assert!(err.contains("account"), "{err}");
        let k = adapter(
            "type = \"snowflake\"\naccount = \"a\"\nusername = \"u\"\ndatabase = \"DB\"\nprivate_key_path = \"keys/rsa.p8\"\n",
        );
        let p = render_profiles_yml("p", &k, "S", None).unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&p.yaml).unwrap();
        let path = v["p"]["outputs"]["rocky"]["private_key_path"]
            .as_str()
            .unwrap();
        assert!(Path::new(path).is_absolute(), "{path}");
        assert!(path.ends_with("keys/rsa.p8"), "{path}");
    }

    fn adapter(toml_text: &str) -> rocky_core::config::AdapterConfig {
        toml::from_str(toml_text).unwrap()
    }

    #[test]
    fn duckdb_profile_points_at_the_rocky_database_file() {
        let a = adapter("type = \"duckdb\"\npath = \"dev.duckdb\"\n");
        let p = render_profiles_yml(
            "rocky_package_build",
            &a,
            "rocky_package_build",
            Some(Path::new("/abs/dev.duckdb")),
        )
        .unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&p.yaml).unwrap();
        let out = &v["rocky_package_build"]["outputs"]["rocky"];
        assert_eq!(out["type"], "duckdb");
        assert_eq!(out["path"], "/abs/dev.duckdb");
        assert_eq!(out["schema"], "rocky_package_build");
        assert!(p.env.is_empty());
        let mem = adapter("type = \"duckdb\"\n");
        assert!(
            render_profiles_yml("p", &mem, "s", None)
                .unwrap_err()
                .contains("path")
        );
    }

    #[test]
    fn secrets_go_through_env_vars_never_the_profile_text() {
        let a = adapter(
            "type = \"snowflake\"\naccount = \"xy1\"\nusername = \"u\"\ndatabase = \"DB\"\nwarehouse = \"WH\"\npassword = \"hunter2\"\n",
        );
        let p = render_profiles_yml("p", &a, "S", None).unwrap();
        assert!(!p.yaml.contains("hunter2"), "{}", p.yaml);
        assert!(
            p.yaml
                .contains("env_var(\"DBT_ENV_SECRET_ROCKY_SNOWFLAKE_PASSWORD\")"),
            "{}",
            p.yaml
        );
        assert_eq!(
            p.env,
            vec![(
                "DBT_ENV_SECRET_ROCKY_SNOWFLAKE_PASSWORD".to_string(),
                "hunter2".to_string()
            )]
        );

        let d = adapter(
            "type = \"databricks\"\nhost = \"https://adb.example.net/\"\nhttp_path = \"/sql/1\"\ntoken = \"dapi\"\n",
        );
        let p = render_profiles_yml("p", &d, "s", None).unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&p.yaml).unwrap();
        assert_eq!(v["p"]["outputs"]["rocky"]["host"], "adb.example.net");
        assert!(!p.yaml.contains("dapi"));

        let pg = adapter(
            "type = \"postgres\"\nhost = \"h\"\nusername = \"u\"\ndatabase = \"d\"\npassword = \"pw\"\n[extra]\nport = 6543\n",
        );
        let p = render_profiles_yml("p", &pg, "public", None).unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&p.yaml).unwrap();
        assert_eq!(v["p"]["outputs"]["rocky"]["port"], 6543);
        assert_eq!(v["p"]["outputs"]["rocky"]["dbname"], "d");

        let bq = adapter("type = \"bigquery\"\nproject_id = \"proj\"\n");
        let p = render_profiles_yml("p", &bq, "ds", None).unwrap();
        let v: serde_yaml::Value = serde_yaml::from_str(&p.yaml).unwrap();
        assert_eq!(v["p"]["outputs"]["rocky"]["method"], "oauth");
        assert_eq!(v["p"]["outputs"]["rocky"]["dataset"], "ds");
    }

    #[test]
    fn unsupported_adapters_are_refused_by_name() {
        let t = adapter("type = \"trino\"\nhost = \"h\"\n");
        let err = render_profiles_yml("p", &t, "s", None).unwrap_err();
        assert!(err.contains("`trino`"), "{err}");
        assert!(err.contains("--compiled"), "{err}");
        assert_eq!(default_target_schema("duckdb"), Some("main"));
        assert_eq!(default_target_schema("bigquery"), None);
    }
}
