//! dbt snapshots → Rocky `type = "snapshot"` models.
//!
//! dbt defines snapshots two ways, and both convert:
//!
//! - **Legacy blocks** (`snapshots/*.sql`): `{% snapshot name %}
//!   {{ config(...) }} select ... {% endsnapshot %}`.
//! - **YAML snapshots** (dbt 1.9+, `snapshots/*.yml`): a `snapshots:` list
//!   whose entries name a `relation` (`ref(...)` / `source(...)`) and a
//!   `config` block.
//!
//! The manifest path gets both from `manifest.json` snapshot nodes; this
//! module supplies the shared config conversion
//! ([`snapshot_strategy_from_dbt`]) plus the raw (no-manifest) scan.
//!
//! An imported snapshot keeps dbt's column names (`dbt_valid_from`,
//! `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`, `dbt_is_deleted`) and
//! writes no `is_current` column, so `rocky run` can continue a snapshot
//! table dbt already built.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};
use rocky_core::snapshot_model::{
    SnapshotCheckColsConfig, SnapshotStrategyKind, SnapshotUniqueKey,
};
use rocky_ir::{SnapshotFlagColumn, SnapshotHardDeletes, SnapshotMetaColumns};
use serde::Deserialize;

use super::dbt::{
    ImportFailure, ImportResult, ImportWarning, ImportedModel, WarningCategory,
    convert_jinja_to_sql, dbt_config_calls, quoted_literal_contents, split_literal_items,
};
use super::dbt_sources;

/// The snapshot-specific part of a dbt snapshot's config, from a manifest
/// node, a YAML `config:` block, or a legacy `{{ config(...) }}` call.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DbtSnapshotConfig {
    #[serde(default)]
    pub unique_key: Option<serde_json::Value>,
    #[serde(default)]
    pub strategy: Option<String>,
    #[serde(default)]
    pub updated_at: Option<String>,
    #[serde(default)]
    pub check_cols: Option<serde_json::Value>,
    #[serde(default)]
    pub hard_deletes: Option<String>,
    #[serde(default)]
    pub invalidate_hard_deletes: Option<bool>,
    #[serde(default)]
    pub snapshot_meta_column_names: Option<BTreeMap<String, Option<String>>>,
    #[serde(default)]
    pub dbt_valid_to_current: Option<String>,
    #[serde(default)]
    pub target_schema: Option<String>,
    #[serde(default)]
    pub target_database: Option<String>,
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(default)]
    pub database: Option<String>,
    #[serde(default)]
    pub alias: Option<String>,
}

/// dbt's metadata column names, the import default.
fn dbt_meta_columns() -> SnapshotMetaColumns {
    SnapshotMetaColumns {
        valid_from: "dbt_valid_from".to_string(),
        valid_to: "dbt_valid_to".to_string(),
        is_current: SnapshotFlagColumn::Enabled(false),
        scd_id: "dbt_scd_id".to_string(),
        updated_at: Some("dbt_updated_at".to_string()),
        is_deleted: "dbt_is_deleted".to_string(),
    }
}

/// Convert a dbt snapshot config to a Rocky snapshot strategy.
///
/// Errors (the snapshot is reported as failed, never silently changed): no
/// `unique_key`, a custom (macro) strategy, an unknown `hard_deletes` value,
/// or anything [`StrategyConfig::snapshot_lowered`] reports as E049.
pub fn snapshot_strategy_from_dbt(cfg: &DbtSnapshotConfig) -> Result<StrategyConfig, String> {
    let unique_key = match &cfg.unique_key {
        Some(serde_json::Value::String(k)) => SnapshotUniqueKey::One(k.clone()),
        Some(serde_json::Value::Array(items)) => {
            let keys: Option<Vec<String>> = items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect();
            match keys {
                Some(keys) if !keys.is_empty() => SnapshotUniqueKey::Many(keys),
                _ => return Err("snapshot `unique_key` must be a column or a list".to_string()),
            }
        }
        Some(_) => return Err("snapshot `unique_key` must be a column or a list".to_string()),
        None => return Err("snapshot has no `unique_key`".to_string()),
    };
    let strategy = match cfg.strategy.as_deref() {
        Some("timestamp") => Some(SnapshotStrategyKind::Timestamp),
        Some("check") => Some(SnapshotStrategyKind::Check),
        Some(other) => {
            return Err(format!(
                "snapshot strategy '{other}' is a custom strategy macro; Rocky supports \
                 `timestamp` and `check`"
            ));
        }
        None => None,
    };
    let check_cols = match &cfg.check_cols {
        None => None,
        Some(serde_json::Value::String(s)) => Some(SnapshotCheckColsConfig::Keyword(s.clone())),
        Some(serde_json::Value::Array(items)) => {
            let cols: Option<Vec<String>> = items
                .iter()
                .map(|v| v.as_str().map(str::to_string))
                .collect();
            Some(SnapshotCheckColsConfig::List(cols.ok_or_else(|| {
                "snapshot `check_cols` must be a list of columns or \"all\"".to_string()
            })?))
        }
        Some(_) => {
            return Err("snapshot `check_cols` must be a list of columns or \"all\"".to_string());
        }
    };
    let hard_deletes = match cfg.hard_deletes.as_deref() {
        None => None,
        Some("ignore") => Some(SnapshotHardDeletes::Ignore),
        Some("invalidate") => Some(SnapshotHardDeletes::Invalidate),
        Some("new_record") => Some(SnapshotHardDeletes::NewRecord),
        Some(other) => return Err(format!("unknown snapshot `hard_deletes = '{other}'`")),
    };

    let mut meta = dbt_meta_columns();
    if let Some(names) = &cfg.snapshot_meta_column_names {
        for (key, value) in names {
            let Some(value) = value.clone() else { continue };
            match key.as_str() {
                "dbt_valid_from" => meta.valid_from = value,
                "dbt_valid_to" => meta.valid_to = value,
                "dbt_scd_id" => meta.scd_id = value,
                "dbt_updated_at" => meta.updated_at = Some(value),
                "dbt_is_deleted" => meta.is_deleted = value,
                other => {
                    return Err(format!(
                        "unknown key '{other}' in `snapshot_meta_column_names`"
                    ));
                }
            }
        }
    }

    let strategy = StrategyConfig::Snapshot {
        unique_key: Some(unique_key),
        snapshot_strategy: strategy,
        updated_at: cfg.updated_at.clone(),
        check_cols,
        hard_deletes,
        invalidate_hard_deletes: cfg.invalidate_hard_deletes,
        snapshot_meta_column_names: Some(Box::new(meta)),
        valid_to_current: cfg.dbt_valid_to_current.clone(),
    };
    if let Some(lowered) = strategy.snapshot_lowered()
        && !lowered.problems.is_empty()
    {
        return Err(lowered.problems.join("; "));
    }
    Ok(strategy)
}

/// The informational note every converted snapshot carries.
pub fn snapshot_import_note(name: &str) -> ImportWarning {
    ImportWarning {
        model: name.to_string(),
        category: WarningCategory::MappedConstruct,
        message: "dbt snapshot imported as a `type = \"snapshot\"` model; it keeps dbt's \
                  metadata column names and runs in the model DAG"
            .to_string(),
        suggestion: Some(
            "new versions get Rocky's `dbt_scd_id` hash, not dbt's, and `dbt_is_deleted` is \
             written as BOOLEAN; check readers that depend on either"
                .to_string(),
        ),
    }
}

/// `catalog`, `schema`, `table` for a snapshot outside the manifest.
fn snapshot_target(name: &str, cfg: &DbtSnapshotConfig, default: &TargetConfig) -> TargetConfig {
    TargetConfig {
        catalog: cfg
            .target_database
            .clone()
            .or_else(|| cfg.database.clone())
            .unwrap_or_else(|| default.catalog.clone()),
        schema: cfg
            .target_schema
            .clone()
            .or_else(|| cfg.schema.clone())
            .unwrap_or_else(|| default.schema.clone()),
        table: cfg.alias.clone().unwrap_or_else(|| name.to_string()),
    }
}

fn imported_snapshot(
    name: &str,
    sql: &str,
    strategy: StrategyConfig,
    target: TargetConfig,
    sources: Vec<rocky_core::models::SourceConfig>,
    intent: Option<String>,
) -> ImportedModel {
    ImportedModel {
        name: name.to_string(),
        sql: sql.trim().to_string(),
        config: ModelConfig {
            name: name.to_string(),
            depends_on: vec![],
            strategy,
            target,
            sources,
            adapter: None,
            intent,
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
        unit_tests: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Raw (no-manifest) scan
// ---------------------------------------------------------------------------

/// The project's snapshot directories: `snapshot-paths` from
/// `dbt_project.yml`, default `snapshots`. Paths escaping the project root
/// are refused, like model paths.
/// Also reports whether `dbt_project.yml` declares project-level snapshot
/// config (a `snapshots:` block), which the raw path cannot apply.
fn snapshot_dirs(dbt_dir: &Path) -> Result<(Vec<PathBuf>, bool), String> {
    #[derive(Deserialize, Default)]
    struct Project {
        #[serde(default, rename = "snapshot-paths")]
        snapshot_paths: Option<Vec<PathBuf>>,
        #[serde(default)]
        snapshots: Option<serde_yaml::Value>,
    }
    let project = std::fs::read_to_string(dbt_dir.join("dbt_project.yml"))
        .ok()
        .and_then(|yml| serde_yaml::from_str::<Project>(&yml).ok())
        .unwrap_or_default();
    let has_project_config = project
        .snapshots
        .as_ref()
        .is_some_and(|v| !matches!(v, serde_yaml::Value::Null));
    let paths = project
        .snapshot_paths
        .unwrap_or_else(|| vec![PathBuf::from("snapshots")]);
    let dirs = paths
        .iter()
        .map(|p| super::dbt::safe_join_under(dbt_dir, p))
        .collect::<Result<_, _>>()?;
    Ok((dirs, has_project_config))
}

const PROJECT_SNAPSHOT_CONFIG_REFUSED: &str = "dbt_project.yml declares project-level \
    `snapshots:` config (e.g. `+target_schema`, `+hard_deletes`), which the raw import \
    cannot apply; import from a compiled manifest.json instead";

/// The raw path cannot see dbt's `generate_schema_name`, so say where the
/// snapshot will land.
fn raw_target_note(name: &str, target: &TargetConfig) -> ImportWarning {
    ImportWarning {
        model: name.to_string(),
        category: WarningCategory::MappedConstruct,
        message: format!(
            "snapshot targets {}.{}.{} as configured; dbt's `generate_schema_name` may have \
             built it elsewhere",
            target.catalog, target.schema, target.table
        ),
        suggestion: Some(
            "check the target before the first `rocky run`: a missing target starts a new \
             history instead of continuing dbt's"
                .to_string(),
        ),
    }
}

/// Scan the project's snapshot directories and convert every snapshot found
/// into `result`. A snapshot that cannot convert is recorded in
/// `result.failed` with the reason.
pub fn import_raw_snapshots(
    dbt_dir: &Path,
    default_target: &TargetConfig,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
) -> Result<(), String> {
    let (dirs, has_project_config) = snapshot_dirs(dbt_dir)?;
    let before = result.imported.len();
    for dir in dirs {
        if dir.is_dir() {
            visit(&dir, default_target, source_map, result, 0)?;
        }
    }
    // Project-level snapshot config would change what was just converted
    // (schema, hard deletes, strategy). Refuse rather than drop it silently.
    if has_project_config {
        let converted: Vec<ImportedModel> = result.imported.drain(before..).collect();
        for model in converted {
            result.warnings.retain(|w| w.model != model.name);
            result.failed.push(ImportFailure {
                name: model.name,
                reason: PROJECT_SNAPSHOT_CONFIG_REFUSED.to_string(),
            });
        }
    }
    Ok(())
}

fn visit(
    dir: &Path,
    default_target: &TargetConfig,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
    depth: usize,
) -> Result<(), String> {
    if depth > super::MAX_IMPORT_RECURSION_DEPTH {
        return Err(format!(
            "snapshot directory tree exceeds the maximum import depth of {} at {}",
            super::MAX_IMPORT_RECURSION_DEPTH,
            dir.display()
        ));
    }
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to read {}: {e}", dir.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| e.to_string())?;
    entries.sort_by_key(std::fs::DirEntry::path);
    for entry in entries {
        let path = entry.path();
        if super::is_traversable_subdir(&entry) {
            visit(&path, default_target, source_map, result, depth + 1)?;
            continue;
        }
        let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let content = match ext {
            "sql" | "yml" | "yaml" => std::fs::read_to_string(&path)
                .map_err(|e| format!("failed to read {}: {e}", path.display()))?,
            _ => continue,
        };
        if ext == "sql" {
            import_legacy_blocks(&content, default_target, source_map, result);
        } else {
            import_yaml_snapshots(&content, &path, default_target, source_map, result);
        }
    }
    Ok(())
}

/// One `{% snapshot name %} ... {% endsnapshot %}` block.
#[derive(Debug, PartialEq, Eq)]
pub struct LegacySnapshotBlock<'a> {
    pub name: &'a str,
    pub body: &'a str,
}

/// Find every legacy snapshot block in a `.sql` file. Whitespace-control
/// dashes (`{%-` / `-%}`) are accepted.
pub fn legacy_snapshot_blocks(content: &str) -> Vec<LegacySnapshotBlock<'_>> {
    let open = regex::Regex::new(r"\{%-?\s*snapshot\s+(\w+)\s*-?%\}").expect("static regex");
    let close = regex::Regex::new(r"\{%-?\s*endsnapshot\s*-?%\}").expect("static regex");
    let mut blocks = Vec::new();
    let mut offset = 0;
    while let Some(m) = open.captures(&content[offset..]) {
        let whole = m.get(0).expect("group 0");
        let name = m.get(1).expect("group 1").as_str();
        let body_start = offset + whole.end();
        let Some(end) = close.find(&content[body_start..]) else {
            break;
        };
        blocks.push(LegacySnapshotBlock {
            name,
            body: &content[body_start..body_start + end.start()],
        });
        offset = body_start + end.end();
    }
    blocks
}

/// Parse the literal arguments of a `config(...)` call into a JSON object.
/// `None` when an argument is not a literal the raw path can read.
pub fn literal_config_args(call: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut map = serde_json::Map::new();
    for arg in split_literal_items(call)? {
        let (key, value) = arg.split_once('=')?;
        map.insert(key.trim().to_string(), literal_value(value.trim())?);
    }
    Some(map)
}

fn literal_value(value: &str) -> Option<serde_json::Value> {
    if let Some(s) = quoted_literal_contents(value) {
        return Some(serde_json::Value::String(s.to_string()));
    }
    match value {
        "true" | "True" => return Some(serde_json::Value::Bool(true)),
        "false" | "False" => return Some(serde_json::Value::Bool(false)),
        "none" | "None" => return Some(serde_json::Value::Null),
        _ => {}
    }
    if let Some(inner) = value.strip_prefix('[').and_then(|s| s.strip_suffix(']')) {
        return split_literal_items(inner)?
            .into_iter()
            .map(|item| literal_value(item.trim()))
            .collect::<Option<Vec<_>>>()
            .map(serde_json::Value::Array);
    }
    if let Some(inner) = value.strip_prefix('{').and_then(|s| s.strip_suffix('}')) {
        let mut map = serde_json::Map::new();
        for item in split_literal_items(inner)? {
            let (k, v) = item.split_once(':')?;
            map.insert(
                quoted_literal_contents(k.trim())?.to_string(),
                literal_value(v.trim())?,
            );
        }
        return Some(serde_json::Value::Object(map));
    }
    None
}

/// Resolve `{{ source('a', 'b') }}` references against the project's
/// sources, warning on the unknown ones (as the model importer does).
fn snapshot_sources(
    name: &str,
    content: &str,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
) -> Vec<rocky_core::models::SourceConfig> {
    let source_re = regex::Regex::new(r#"source\s*\(\s*['"](\w+)['"]\s*,\s*['"](\w+)['"]\s*\)"#)
        .expect("static regex");
    let mut sources = Vec::new();
    for cap in source_re.captures_iter(content) {
        let key = (cap[1].to_string(), cap[2].to_string());
        match source_map.get(&key) {
            Some(mapping) => sources.push(mapping.source_config.clone()),
            None => result.warnings.push(ImportWarning {
                model: name.to_string(),
                category: WarningCategory::MissingSource,
                message: format!("source('{}', '{}') not found in sources.yml", key.0, key.1),
                suggestion: Some("add a sources.yml definition for this source".to_string()),
            }),
        }
    }
    sources
}

fn import_legacy_blocks(
    content: &str,
    default_target: &TargetConfig,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
) {
    for block in legacy_snapshot_blocks(content) {
        let name = block.name;
        let fail = |result: &mut ImportResult, reason: String| {
            result.failed.push(ImportFailure {
                name: name.to_string(),
                reason,
            });
        };
        if block.body.contains("{%") {
            fail(
                result,
                "snapshot body contains Jinja control flow the raw import cannot evaluate; \
                 import from a compiled manifest.json instead"
                    .to_string(),
            );
            continue;
        }
        let calls = dbt_config_calls(block.body);
        let Some(args) = calls.last().and_then(|call| literal_config_args(call)) else {
            fail(
                result,
                "snapshot has no `{{ config(...) }}` with literal arguments; import from a \
                 compiled manifest.json instead"
                    .to_string(),
            );
            continue;
        };
        let cfg: DbtSnapshotConfig = match serde_json::from_value(serde_json::Value::Object(args)) {
            Ok(cfg) => cfg,
            Err(e) => {
                fail(result, format!("snapshot config is not readable: {e}"));
                continue;
            }
        };
        let strategy = match snapshot_strategy_from_dbt(&cfg) {
            Ok(s) => s,
            Err(e) => {
                fail(result, e);
                continue;
            }
        };
        let target = snapshot_target(name, &cfg, default_target);
        let this_ref = format!("{}.{}.{}", target.catalog, target.schema, target.table);
        let sql = convert_jinja_to_sql(block.body, &this_ref);
        let sources = snapshot_sources(name, block.body, source_map, result);
        result.warnings.push(snapshot_import_note(name));
        result.warnings.push(raw_target_note(name, &target));
        result.imported.push(imported_snapshot(
            name, &sql, strategy, target, sources, None,
        ));
    }
}

#[derive(Deserialize)]
struct YamlSnapshotFile {
    #[serde(default)]
    snapshots: Vec<YamlSnapshot>,
}

#[derive(Deserialize)]
struct YamlSnapshot {
    name: String,
    #[serde(default)]
    relation: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    config: DbtSnapshotConfig,
}

fn import_yaml_snapshots(
    content: &str,
    path: &Path,
    default_target: &TargetConfig,
    source_map: &HashMap<(String, String), dbt_sources::RockySourceMapping>,
    result: &mut ImportResult,
) {
    // Only files with a `snapshots:` list are snapshot definitions; other
    // YAML (properties files) parse to an empty list and are skipped.
    let file: YamlSnapshotFile = match serde_yaml::from_str(content) {
        Ok(f) => f,
        Err(e) => {
            if content.contains("snapshots:") {
                result.warnings.push(ImportWarning {
                    model: "<project>".to_string(),
                    category: WarningCategory::UnsupportedMaterialization,
                    message: format!("could not parse snapshot YAML {}: {e}", path.display()),
                    suggestion: None,
                });
            }
            return;
        }
    };
    for snap in file.snapshots {
        let name = snap.name.as_str();
        let Some(relation) = snap.relation.as_deref() else {
            // A properties-only entry (docs / columns for a legacy snapshot).
            continue;
        };
        let relation = relation
            .trim()
            .trim_start_matches("{{")
            .trim_end_matches("}}")
            .trim();
        let is_call = |prefix: &str| {
            relation
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.trim_start().starts_with('('))
        };
        if !(is_call("ref") || is_call("source")) {
            result.failed.push(ImportFailure {
                name: name.to_string(),
                reason: format!(
                    "snapshot relation `{relation}` is not a ref() or source() the raw import can \
                     resolve"
                ),
            });
            continue;
        }
        let strategy = match snapshot_strategy_from_dbt(&snap.config) {
            Ok(s) => s,
            Err(e) => {
                result.failed.push(ImportFailure {
                    name: name.to_string(),
                    reason: e,
                });
                continue;
            }
        };
        let target = snapshot_target(name, &snap.config, default_target);
        let this_ref = format!("{}.{}.{}", target.catalog, target.schema, target.table);
        let from = convert_jinja_to_sql(&format!("{{{{ {relation} }}}}"), &this_ref);
        if from.contains("TODO") {
            result.failed.push(ImportFailure {
                name: name.to_string(),
                reason: format!("snapshot relation `{relation}` could not be resolved"),
            });
            continue;
        }
        let sources = snapshot_sources(name, relation, source_map, result);
        result.warnings.push(snapshot_import_note(name));
        result.warnings.push(raw_target_note(name, &target));
        result.imported.push(imported_snapshot(
            name,
            &format!("SELECT * FROM {from}"),
            strategy,
            target,
            sources,
            snap.description.filter(|d| !d.is_empty()),
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> TargetConfig {
        TargetConfig {
            catalog: "warehouse".into(),
            schema: "analytics".into(),
            table: "x".into(),
        }
    }

    fn empty_result() -> ImportResult {
        super::super::dbt::empty_import_result()
    }

    fn snapshot_fields(s: &StrategyConfig) -> rocky_ir::SnapshotSpec {
        let lowered = s.snapshot_lowered().expect("snapshot strategy");
        assert!(lowered.problems.is_empty(), "{:?}", lowered.problems);
        lowered.spec
    }

    #[test]
    fn legacy_block_converts_with_dbt_column_names() {
        let content = r#"
{% snapshot orders_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}
select * from {{ source('jaffle', 'orders') }}
{% endsnapshot %}
"#;
        let mut result = empty_result();
        import_legacy_blocks(content, &target(), &HashMap::new(), &mut result);
        assert!(
            result.failed.is_empty(),
            "{:?}",
            result.failed.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
        assert_eq!(result.imported.len(), 1);
        let m = &result.imported[0];
        assert_eq!(m.name, "orders_snapshot");
        assert_eq!(m.sql, "select * from jaffle.orders");
        assert_eq!(m.config.target.schema, "snapshots");
        let spec = snapshot_fields(&m.config.strategy);
        assert_eq!(spec.unique_key, vec![std::sync::Arc::from("id")]);
        assert_eq!(spec.hard_deletes, SnapshotHardDeletes::Invalidate);
        assert_eq!(spec.meta_columns.valid_from, "dbt_valid_from");
        assert_eq!(spec.meta_columns.is_current.name(), None);
    }

    #[test]
    fn legacy_check_block_with_composite_key_and_list() {
        let content = "{%- snapshot cust_snap -%}\n{{ config(unique_key=['id', 'region'], strategy='check', check_cols=['name', 'email'], hard_deletes='new_record') }}\nselect * from {{ ref('stg_customers') }}\n{%- endsnapshot -%}";
        let mut result = empty_result();
        import_legacy_blocks(content, &target(), &HashMap::new(), &mut result);
        assert_eq!(
            result.imported.len(),
            1,
            "{:?}",
            result.failed.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
        let m = &result.imported[0];
        assert_eq!(m.sql, "select * from stg_customers");
        assert_eq!(m.config.target.schema, "analytics");
        let spec = snapshot_fields(&m.config.strategy);
        assert_eq!(spec.unique_key.len(), 2);
        assert_eq!(spec.hard_deletes, SnapshotHardDeletes::NewRecord);
        assert!(matches!(
            spec.change,
            rocky_ir::SnapshotChangeStrategy::Check { check_cols: rocky_ir::SnapshotCheckColumns::Explicit(ref c), .. } if c.len() == 2
        ));
    }

    #[test]
    fn legacy_block_without_unique_key_fails_instead_of_dropping() {
        let content = "{% snapshot s %}{{ config(strategy='check', check_cols='all') }} select 1 {% endsnapshot %}";
        let mut result = empty_result();
        import_legacy_blocks(content, &target(), &HashMap::new(), &mut result);
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert!(result.failed[0].reason.contains("unique_key"));
    }

    #[test]
    fn yaml_snapshot_converts_with_meta_overrides() {
        let yaml = r#"
snapshots:
  - name: orders_snapshot
    relation: source('jaffle', 'orders')
    description: order history
    config:
      schema: snapshots
      unique_key: id
      strategy: timestamp
      updated_at: updated_at
      dbt_valid_to_current: "to_date('9999-12-31')"
      hard_deletes: new_record
      snapshot_meta_column_names:
        dbt_valid_from: start_date
        dbt_valid_to: end_date
  - name: docs_only
    description: properties for a legacy snapshot
"#;
        let mut result = empty_result();
        import_yaml_snapshots(
            yaml,
            Path::new("s.yml"),
            &target(),
            &HashMap::new(),
            &mut result,
        );
        assert!(
            result.failed.is_empty(),
            "{:?}",
            result.failed.iter().map(|f| &f.reason).collect::<Vec<_>>()
        );
        assert_eq!(result.imported.len(), 1);
        let m = &result.imported[0];
        assert_eq!(m.sql, "SELECT * FROM jaffle.orders");
        assert_eq!(m.config.intent.as_deref(), Some("order history"));
        let spec = snapshot_fields(&m.config.strategy);
        assert_eq!(spec.meta_columns.valid_from, "start_date");
        assert_eq!(spec.meta_columns.valid_to, "end_date");
        assert_eq!(spec.meta_columns.scd_id, "dbt_scd_id");
        assert_eq!(
            spec.valid_to_current.as_deref(),
            Some("to_date('9999-12-31')")
        );
    }

    #[test]
    fn project_level_snapshot_config_refuses_raw_import() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nsnapshots:\n  p:\n    +hard_deletes: invalidate\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("snapshots")).unwrap();
        std::fs::write(
            dir.path().join("snapshots/s.sql"),
            "{% snapshot s %}{{ config(unique_key='id', strategy='check', check_cols='all') }} select id from raw.t {% endsnapshot %}",
        )
        .unwrap();
        let mut result = empty_result();
        import_raw_snapshots(dir.path(), &target(), &HashMap::new(), &mut result).unwrap();
        assert!(result.imported.is_empty());
        assert_eq!(result.failed.len(), 1);
        assert!(result.failed[0].reason.contains("project-level"));
    }

    #[test]
    fn check_strategy_keeps_updated_at_for_valid_from() {
        let cfg = DbtSnapshotConfig {
            unique_key: Some(serde_json::json!("id")),
            strategy: Some("check".into()),
            check_cols: Some(serde_json::json!(["name"])),
            updated_at: Some("changed_at".into()),
            ..Default::default()
        };
        let spec = snapshot_fields(&snapshot_strategy_from_dbt(&cfg).unwrap());
        assert_eq!(spec.change.version_column(), Some("changed_at"));
    }

    #[test]
    fn expression_unique_key_fails_import() {
        let cfg = DbtSnapshotConfig {
            unique_key: Some(serde_json::json!("id || '-' || region")),
            strategy: Some("check".into()),
            check_cols: Some(serde_json::json!("all")),
            ..Default::default()
        };
        assert!(
            snapshot_strategy_from_dbt(&cfg)
                .unwrap_err()
                .contains("not a column name")
        );
    }

    #[test]
    fn custom_strategy_is_a_failure() {
        let cfg = DbtSnapshotConfig {
            unique_key: Some(serde_json::json!("id")),
            strategy: Some("my_custom".into()),
            ..Default::default()
        };
        assert!(
            snapshot_strategy_from_dbt(&cfg)
                .unwrap_err()
                .contains("custom")
        );
    }

    #[test]
    fn raw_scan_reads_snapshot_paths_and_emits_round_trippable_toml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("dbt_project.yml"),
            "name: p\nsnapshot-paths: ['snaps']\n",
        )
        .unwrap();
        std::fs::create_dir_all(dir.path().join("snaps")).unwrap();
        std::fs::write(
            dir.path().join("snaps/s.sql"),
            "{% snapshot s %}{{ config(unique_key='id', strategy='check', check_cols='all') }} select id, v from raw.t {% endsnapshot %}",
        )
        .unwrap();
        let mut result = empty_result();
        import_raw_snapshots(dir.path(), &target(), &HashMap::new(), &mut result).unwrap();
        assert_eq!(result.imported.len(), 1);
        let toml = super::super::emit::render_model_sidecar(&result.imported[0].config);
        let raw: rocky_core::models::RawModelConfig = toml::from_str(&toml).unwrap();
        let strategy = raw.strategy.expect("strategy emitted");
        assert_eq!(
            snapshot_fields(&strategy),
            snapshot_fields(&result.imported[0].config.strategy),
            "{toml}"
        );
    }
}
