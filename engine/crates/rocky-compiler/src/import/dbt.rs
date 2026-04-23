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

use std::collections::HashMap;
use std::path::Path;

use regex::Regex;

use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

use super::dbt_manifest::{self, DbtManifest, DbtManifestNode, UniqueKeyValue};
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
}

/// A warning produced during import.
#[derive(Debug, Clone)]
pub struct ImportWarning {
    pub model: String,
    pub category: WarningCategory,
    pub message: String,
    pub suggestion: Option<String>,
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
    /// Structured warnings.
    pub warnings: Vec<ImportWarning>,
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
}

/// A successfully imported model.
pub struct ImportedModel {
    pub name: String,
    pub sql: String,
    pub config: ModelConfig,
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
    };

    for node in manifest.nodes.values() {
        import_manifest_node(node, default_target, &mut result);
    }

    result
}

fn import_manifest_node(
    node: &DbtManifestNode,
    default_target: &TargetConfig,
    result: &mut ImportResult,
) {
    // Use compiled_code (Jinja resolved) if available, else raw_code
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
            convert_jinja_to_sql(&node.raw_code)
        }
    };

    // Map strategy from manifest config
    let (strategy, strategy_warnings) = manifest_config_to_strategy(&node.config, &node.name);
    result.warnings.extend(strategy_warnings);

    // Map dependencies
    let depends_on = dbt_manifest::depends_on_to_rocky(&node.depends_on.nodes);

    // Use description as intent
    let intent = node.description.clone();

    let schema = node
        .config
        .schema
        .as_deref()
        .unwrap_or(&default_target.schema);

    let catalog = if node.database.is_empty() {
        default_target.catalog.clone()
    } else {
        node.database.clone()
    };

    let config = ModelConfig {
        name: node.name.clone(),
        depends_on,
        strategy,
        target: TargetConfig {
            catalog,
            schema: schema.to_string(),
            table: node.name.clone(),
        },
        sources: vec![],
        adapter: None,
        intent,
        freshness: None,
        tests: vec![],
        format: None,
        format_options: None,
        classification: Default::default(),
    };

    result.imported.push(ImportedModel {
        name: node.name.clone(),
        sql: sql.trim().to_string(),
        config,
    });
}

fn manifest_config_to_strategy(
    config: &super::dbt_manifest::DbtNodeConfig,
    model_name: &str,
) -> (StrategyConfig, Vec<ImportWarning>) {
    let mut warnings = Vec::new();

    match config.materialized.as_str() {
        "incremental" => {
            if let Some(ref uk) = config.unique_key {
                let keys = match uk {
                    UniqueKeyValue::Single(s) => vec![s.clone()],
                    UniqueKeyValue::Multiple(v) => v.clone(),
                };
                (
                    StrategyConfig::Merge {
                        unique_key: keys,
                        update_columns: None,
                    },
                    warnings,
                )
            } else {
                (
                    StrategyConfig::Incremental {
                        timestamp_column: "updated_at".to_string(),
                    },
                    warnings,
                )
            }
        }
        "view" => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: "materialized='view' not supported — using full_refresh".to_string(),
                suggestion: Some(
                    "consider using materialized='table' or 'incremental' in Rocky".to_string(),
                ),
            });
            (StrategyConfig::FullRefresh, warnings)
        }
        "ephemeral" => {
            warnings.push(ImportWarning {
                model: model_name.to_string(),
                category: WarningCategory::UnsupportedMaterialization,
                message: "materialized='ephemeral' not supported — using full_refresh".to_string(),
                suggestion: Some("inline the SQL into downstream models".to_string()),
            });
            (StrategyConfig::FullRefresh, warnings)
        }
        _ => (StrategyConfig::FullRefresh, warnings),
    }
}

// ---------------------------------------------------------------------------
// Import from raw SQL files (regex path)
// ---------------------------------------------------------------------------

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

    // Determine model paths
    let model_dirs: Vec<std::path::PathBuf> = match &project_config {
        Some(cfg) => cfg.model_paths.iter().map(|p| dbt_dir.join(p)).collect(),
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
            )?;
        }
    }

    // Phase 2: Scan for model YAML test definitions and convert to contracts
    for dir in &model_dirs {
        if dir.exists() {
            if let Ok(model_yamls) = super::dbt_tests::parse_model_yamls(dir) {
                for (model_name, model_yaml) in &model_yamls {
                    let (checks, skipped) = super::dbt_tests::tests_to_contracts(model_yaml);
                    let total_tests: usize = model_yaml.columns.iter().map(|c| c.tests.len()).sum();
                    result.tests_found += total_tests;
                    result.tests_skipped += skipped;

                    for check in &checks {
                        if check.custom_sql.is_some() {
                            result.tests_converted_custom += 1;
                        } else {
                            result.tests_converted += 1;
                        }
                    }

                    // Set model description as intent if available and model was imported
                    if let Some(ref desc) = model_yaml.description {
                        if let Some(imported) =
                            result.imported.iter_mut().find(|m| &m.name == model_name)
                        {
                            if imported.config.intent.is_none() {
                                imported.config.intent = Some(desc.clone());
                            }
                        }
                    }
                }
            }
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
) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.is_dir() {
            visit_dbt_models(
                &path,
                models_root,
                default_target,
                project_config,
                source_map,
                result,
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
        let has_non_incr_blocks = {
            let block_re = Regex::new(r"\{%[^%]*%\}").unwrap();
            block_re.is_match(&content_processed)
        };
        if has_non_incr_blocks {
            warnings.push(ImportWarning {
                model: name.to_string(),
                category: WarningCategory::JinjaControlFlow,
                message: "contains Jinja control flow ({% if %}, {% for %}) — replaced with TODO comments".to_string(),
                suggestion: Some("consider using manifest.json import path for full Jinja resolution".to_string()),
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
        suggestion: None,
    }));

    // If is_incremental() was detected and config didn't already set incremental,
    // override the strategy
    if let Some(ref incr) = incr_result {
        if matches!(strategy, StrategyConfig::FullRefresh) {
            strategy = StrategyConfig::Incremental {
                timestamp_column: incr.timestamp_column.clone(),
            };
        }
    }

    // Apply project config inheritance
    let resolved_schema = if let Some(proj) = project_config {
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
                    warnings.push(ImportWarning {
                        model: name.to_string(),
                        category: WarningCategory::UnsupportedMaterialization,
                        message: "project config materialized='view' — using full_refresh"
                            .to_string(),
                        suggestion: None,
                    });
                }
                _ => {}
            }
        }
        resolved.schema
    } else {
        None
    };

    // Convert Jinja refs to plain SQL
    let sql = convert_jinja_to_sql(&content_processed);

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

    let schema = resolved_schema.as_deref().unwrap_or(&default_target.schema);

    let config = ModelConfig {
        name: name.to_string(),
        depends_on: vec![], // Auto-resolved by compiler
        strategy,
        target: TargetConfig {
            catalog: default_target.catalog.clone(),
            schema: schema.to_string(),
            table: name.to_string(),
        },
        sources: model_sources,
        adapter: None,
        intent: None,
        freshness: None,
        tests: vec![],
        format: None,
        format_options: None,
        classification: Default::default(),
    };

    Ok((
        ImportedModel {
            name: name.to_string(),
            sql,
            config,
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
fn extract_dbt_config(content: &str) -> (StrategyConfig, Vec<String>) {
    let mut warnings = Vec::new();

    let config_re = Regex::new(r"\{\{\s*config\s*\(([^)]*)\)\s*\}\}").unwrap();
    if let Some(captures) = config_re.captures(content) {
        let config_str = &captures[1];

        // Parse materialized
        let mat_re = Regex::new(r#"materialized\s*=\s*['"](\w+)['"]"#).unwrap();
        let materialized = mat_re
            .captures(config_str)
            .map(|c| c[1].to_string())
            .unwrap_or_else(|| "table".to_string());

        match materialized.as_str() {
            "incremental" => {
                // Extract unique_key for merge strategy
                let key_re = Regex::new(r#"unique_key\s*=\s*['"](\w+)['"]"#).unwrap();
                if let Some(key_cap) = key_re.captures(config_str) {
                    return (
                        StrategyConfig::Merge {
                            unique_key: vec![key_cap[1].to_string()],
                            update_columns: None,
                        },
                        warnings,
                    );
                }
                // Incremental without unique_key -> append
                let ts_re = Regex::new(r#"(?:incremental_strategy|timestamp)\s*=\s*['"](\w+)['"]"#)
                    .unwrap();
                let ts_col = ts_re
                    .captures(config_str)
                    .map(|c| c[1].to_string())
                    .unwrap_or_else(|| "updated_at".to_string());
                (
                    StrategyConfig::Incremental {
                        timestamp_column: ts_col,
                    },
                    warnings,
                )
            }
            "view" => {
                warnings.push(
                    "materialized='view' not supported in Rocky — using full_refresh".to_string(),
                );
                (StrategyConfig::FullRefresh, warnings)
            }
            "ephemeral" => {
                warnings.push(
                    "materialized='ephemeral' not supported — using full_refresh".to_string(),
                );
                (StrategyConfig::FullRefresh, warnings)
            }
            _ => (StrategyConfig::FullRefresh, warnings),
        }
    } else {
        (StrategyConfig::FullRefresh, warnings)
    }
}

// ---------------------------------------------------------------------------
// Jinja -> SQL conversion
// ---------------------------------------------------------------------------

/// Convert dbt Jinja expressions to plain SQL.
fn convert_jinja_to_sql(content: &str) -> String {
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

    // {{ this }} -> __this__ (placeholder, resolved at execution)
    let this_re = Regex::new(r"\{\{\s*this\s*\}\}").unwrap();
    sql = this_re.replace_all(&sql, "__this__").to_string();

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
        assert_eq!(convert_jinja_to_sql(input), "SELECT * FROM orders");
    }

    #[test]
    fn test_convert_source() {
        let input = "SELECT * FROM {{ source('raw', 'customers') }}";
        assert_eq!(convert_jinja_to_sql(input), "SELECT * FROM raw.customers");
    }

    #[test]
    fn test_convert_config_removed() {
        let input = "{{ config(materialized='table') }}\nSELECT 1";
        assert_eq!(convert_jinja_to_sql(input), "SELECT 1");
    }

    #[test]
    fn test_convert_this() {
        let input = "SELECT * FROM {{ this }}";
        assert_eq!(convert_jinja_to_sql(input), "SELECT * FROM __this__");
    }

    #[test]
    fn test_convert_unsupported_jinja() {
        let input = "{% if some_condition %}WHERE id > 0{% endif %}";
        let result = convert_jinja_to_sql(input);
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
    fn test_extract_config_view_warning() {
        let input = "{{ config(materialized='view') }}";
        let (strategy, warnings) = extract_dbt_config(input);
        assert!(matches!(strategy, StrategyConfig::FullRefresh));
        assert!(!warnings.is_empty());
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
        let result = convert_jinja_to_sql(input);
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
    fn test_import_from_manifest_view_warning() {
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

        assert!(result.warnings.iter().any(|w| {
            w.category == WarningCategory::UnsupportedMaterialization && w.message.contains("view")
        }));
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
}
