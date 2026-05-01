//! Scope resolution for catalog/project-aware Rocky CLI commands.
//!
//! Shared by `rocky compact --measure-dedup`, `rocky compact --catalog`,
//! and `rocky archive --catalog`. The resolvers walk the pipeline config
//! (replication source discovery or transformation model files) to
//! produce the set of fully-qualified `catalog.schema.table` names
//! Rocky manages — without touching the warehouse.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

use crate::registry::{AdapterRegistry, resolve_pipeline};

/// Resolve the set of table names that Rocky manages for a given pipeline.
///
/// Returns `Ok(Some(set))` with lowercase fully-qualified table names
/// (`catalog.schema.table`) when the pipeline type supports resolution,
/// or `Ok(None)` when resolution is not possible (unsupported pipeline
/// type, no discovery adapter configured, etc.). Callers should fall
/// back to scanning all warehouse tables when `None` is returned.
///
/// ## Pipeline types
///
/// - **Replication**: discovers connectors via the discovery adapter,
///   parses each schema through the source `schema_pattern`, resolves
///   target catalog/schema from the target templates, and collects
///   `catalog.schema.table` for every discovered table.
/// - **Transformation**: loads model files from the models directory
///   (same walk as `rocky list models`) and extracts the `target`
///   coordinates from each model's TOML sidecar.
/// - **Other types** (quality, snapshot, load): not yet supported —
///   returns `Ok(None)`.
pub(crate) async fn resolve_managed_tables(
    config: &rocky_core::config::RockyConfig,
    pipeline_name: &str,
    pipeline: &rocky_core::config::PipelineConfig,
    registry: &AdapterRegistry,
    config_path: &Path,
) -> Result<Option<HashSet<String>>> {
    match pipeline {
        rocky_core::config::PipelineConfig::Replication(repl) => {
            resolve_replication_managed_tables(repl, registry, config).await
        }
        rocky_core::config::PipelineConfig::Transformation(tx) => {
            resolve_transformation_managed_tables(tx, config_path)
        }
        _ => {
            tracing::info!(
                pipeline = pipeline_name,
                pipeline_type = pipeline.pipeline_type_str(),
                "managed-table resolution not supported for this pipeline type"
            );
            Ok(None)
        }
    }
}

/// Resolve managed tables for a replication pipeline by discovering
/// connectors and resolving each table through the schema pattern
/// templates.
pub(crate) async fn resolve_replication_managed_tables(
    repl: &rocky_core::config::ReplicationPipelineConfig,
    registry: &AdapterRegistry,
    _config: &rocky_core::config::RockyConfig,
) -> Result<Option<HashSet<String>>> {
    let pattern = repl
        .schema_pattern()
        .map_err(|e| anyhow!("failed to parse schema pattern: {e}"))?;

    // Discovery adapter is required to enumerate source connectors.
    let disc_config = match repl.source.discovery.as_ref() {
        Some(d) => d,
        None => {
            tracing::warn!(
                "no discovery adapter configured for this replication pipeline; \
                 cannot resolve managed tables"
            );
            return Ok(None);
        }
    };
    let discovery_adapter = registry.discovery_adapter(&disc_config.adapter)?;

    let connectors = discovery_adapter
        .discover(&pattern.prefix)
        .await
        .map_err(|e| anyhow!("discovery failed: {e}"))?
        .connectors;

    let target_sep = repl
        .target
        .separator
        .as_deref()
        .unwrap_or(&pattern.separator);

    let mut managed = HashSet::new();
    for conn in &connectors {
        let parsed = match pattern.parse(&conn.schema) {
            Ok(p) => p,
            Err(_) => continue,
        };

        let target_catalog = parsed.resolve_template(&repl.target.catalog_template, target_sep);
        let target_schema = parsed.resolve_template(&repl.target.schema_template, target_sep);

        for table in &conn.tables {
            let full_name = format!("{}.{}.{}", target_catalog, target_schema, table.name);
            managed.insert(full_name.to_lowercase());
        }
    }

    tracing::info!(
        managed_count = managed.len(),
        "resolved managed tables from replication pipeline"
    );
    Ok(Some(managed))
}

/// Resolve managed tables for a transformation pipeline by loading
/// model files and extracting their target coordinates.
pub(crate) fn resolve_transformation_managed_tables(
    tx: &rocky_core::config::TransformationPipelineConfig,
    config_path: &Path,
) -> Result<Option<HashSet<String>>> {
    // Models directory is relative to the config file's parent.
    let project_root = config_path.parent().unwrap_or(Path::new("."));

    // The `models` field is a glob like "models/**" — extract the base
    // directory (everything before any glob wildcard).
    let models_base = tx
        .models
        .split(&['*', '?', '['][..])
        .next()
        .unwrap_or("models");
    let models_dir = project_root.join(models_base.trim_end_matches('/'));

    if !models_dir.exists() {
        tracing::warn!(
            models_dir = %models_dir.display(),
            "models directory does not exist; cannot resolve managed tables"
        );
        return Ok(None);
    }

    // Load models the same way `rocky list models` does: top-level +
    // immediate subdirectories.
    let mut all_models = rocky_core::models::load_models_from_dir(&models_dir).context(format!(
        "failed to load models from {}",
        models_dir.display()
    ))?;

    if let Ok(entries) = std::fs::read_dir(&models_dir) {
        for entry in entries.flatten() {
            if entry.path().is_dir() {
                if let Ok(sub) = rocky_core::models::load_models_from_dir(&entry.path()) {
                    all_models.extend(sub);
                }
            }
        }
    }

    let mut managed = HashSet::new();
    for model in &all_models {
        let full_name = format!(
            "{}.{}.{}",
            model.config.target.catalog, model.config.target.schema, model.config.target.table
        );
        managed.insert(full_name.to_lowercase());
    }

    tracing::info!(
        managed_count = managed.len(),
        "resolved managed tables from transformation pipeline"
    );
    Ok(Some(managed))
}

/// Extract the distinct set of catalog identifiers from a managed-table
/// set built by `resolve_managed_tables`.
///
/// The set is keyed on fully-qualified lowercase names (`catalog.schema.table`)
/// assembled via `format!("{}.{}.{}", ...)` in the resolver, so splitting
/// on the first `.` yields the catalog component. Names without a dot
/// are skipped (defensive — the resolver always emits three components).
pub(crate) fn managed_catalog_set(managed: &HashSet<String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for full in managed {
        if let Some((catalog, _rest)) = full.split_once('.') {
            if seen.insert(catalog.to_string()) {
                out.push(catalog.to_string());
            }
        }
    }
    out.sort();
    out
}

/// Result of resolving the set of Rocky-managed tables in a single catalog.
pub(crate) struct CatalogScope {
    /// Catalog identifier as supplied to `--catalog`, lowercased to match
    /// the resolver's output.
    pub catalog: String,
    /// Sorted list of fully-qualified `catalog.schema.table` names.
    pub tables: Vec<String>,
}

/// Resolve the list of Rocky-managed tables in `catalog`.
///
/// Loads `rocky.toml`, picks the single pipeline (or first), runs the
/// managed-table resolver, and filters to entries whose catalog component
/// matches `catalog` (case-insensitive). Errors helpfully when:
///
/// - The pipeline type doesn't support managed-table resolution
///   (quality, snapshot, load).
/// - Resolution succeeded but no managed tables matched the catalog —
///   the error lists every catalog Rocky knows about so the user can
///   spot a typo.
///
/// This helper backs `rocky compact --catalog` and `rocky archive
/// --catalog`. It does not touch the warehouse — both commands generate
/// SQL only, so the resolver's config-derived view of managed tables is
/// authoritative.
pub(crate) async fn resolve_managed_tables_in_catalog(
    config_path: &Path,
    catalog: &str,
) -> Result<CatalogScope> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

    let (pipeline_name, pipeline) = resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline for catalog-scoped command")?;

    let managed =
        match resolve_managed_tables(&rocky_cfg, pipeline_name, pipeline, &registry, config_path)
            .await?
        {
            Some(set) if !set.is_empty() => set,
            Some(_) => {
                bail!(
                    "pipeline '{pipeline_name}' resolved zero managed tables; \
                     `--catalog` requires at least one declared model or discovered source"
                );
            }
            None => {
                bail!(
                    "pipeline '{pipeline_name}' (type: {}) does not support `--catalog` scope. \
                     Supported pipeline types: replication, transformation.",
                    pipeline.pipeline_type_str()
                );
            }
        };

    let (needle, tables) = filter_managed_to_catalog(&managed, catalog)?;

    Ok(CatalogScope {
        catalog: needle,
        tables,
    })
}

/// Filter a managed-table set to only entries in `catalog` (case-insensitive).
///
/// Returns `(lowercased_catalog, sorted_fqns)` on hit. Errors with a
/// helpful "available catalogs: …" listing on miss so consumer typos
/// don't silently turn into no-ops. Pure — no warehouse or filesystem
/// access — so it's the easy unit-test target for the catalog filter.
pub(crate) fn filter_managed_to_catalog(
    managed: &HashSet<String>,
    catalog: &str,
) -> Result<(String, Vec<String>)> {
    let needle = catalog.to_lowercase();
    let mut tables: Vec<String> = managed
        .iter()
        .filter(|fqn| {
            fqn.split_once('.')
                .map(|(cat, _)| cat == needle)
                .unwrap_or(false)
        })
        .cloned()
        .collect();

    if tables.is_empty() {
        let catalogs = managed_catalog_set(managed);
        let available = if catalogs.is_empty() {
            "(none)".to_string()
        } else {
            catalogs.join(", ")
        };
        bail!(
            "no Rocky-managed tables found in catalog '{catalog}'. \
             Available catalogs: {available}"
        );
    }
    tables.sort();
    Ok((needle, tables))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_catalog_set_extracts_distinct_catalogs() {
        let mut managed = HashSet::new();
        managed.insert("acme.raw__shopify.orders".to_string());
        managed.insert("acme.raw__shopify.events".to_string());
        managed.insert("beta.raw__stripe.charges".to_string());
        managed.insert("acme.raw__stripe.customers".to_string());

        let catalogs = managed_catalog_set(&managed);
        assert_eq!(catalogs, vec!["acme".to_string(), "beta".to_string()]);
    }

    #[test]
    fn managed_catalog_set_skips_names_without_catalog_prefix() {
        let mut managed = HashSet::new();
        managed.insert("acme.schema.table".to_string());
        managed.insert("schema_only".to_string()); // defensive: no `.`

        let catalogs = managed_catalog_set(&managed);
        assert_eq!(catalogs, vec!["acme".to_string()]);
    }

    #[test]
    fn filter_managed_to_catalog_filters_to_catalog() {
        let mut managed = HashSet::new();
        managed.insert("acme.raw__shopify.orders".to_string());
        managed.insert("acme.raw__stripe.events".to_string());
        managed.insert("beta.raw__shopify.orders".to_string());

        let (needle, tables) = filter_managed_to_catalog(&managed, "acme").unwrap();
        assert_eq!(needle, "acme");
        assert_eq!(
            tables,
            vec![
                "acme.raw__shopify.orders".to_string(),
                "acme.raw__stripe.events".to_string(),
            ]
        );
    }

    #[test]
    fn filter_managed_to_catalog_is_case_insensitive() {
        // Resolver always lowercases; user input may not. The filter
        // must match regardless of input case.
        let mut managed = HashSet::new();
        managed.insert("warehouse.staging.orders".to_string());
        managed.insert("warehouse.marts.customers".to_string());

        let (needle, tables) = filter_managed_to_catalog(&managed, "WAREHOUSE").unwrap();
        assert_eq!(needle, "warehouse");
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn filter_managed_to_catalog_lists_available_on_miss() {
        // Empty match must not silently succeed — the consumer would
        // ship a nightly job that quietly does nothing. Error must list
        // available catalogs so a typo is obvious.
        let mut managed = HashSet::new();
        managed.insert("acme.raw__shopify.orders".to_string());
        managed.insert("beta.raw__shopify.orders".to_string());

        let err = filter_managed_to_catalog(&managed, "gamma").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'gamma'"), "error mentions the input: {msg}");
        assert!(msg.contains("acme"), "error lists acme: {msg}");
        assert!(msg.contains("beta"), "error lists beta: {msg}");
    }

    #[test]
    fn filter_managed_to_catalog_handles_empty_managed_set() {
        let managed = HashSet::new();
        let err = filter_managed_to_catalog(&managed, "acme").unwrap_err();
        assert!(err.to_string().contains("(none)"));
    }

    /// Test that `resolve_transformation_managed_tables` correctly
    /// extracts target table names from model sidecar files.
    #[test]
    fn resolve_transformation_managed_tables_extracts_targets() {
        let dir = tempfile::tempdir().unwrap();
        let models_dir = dir.path().join("models");
        std::fs::create_dir(&models_dir).unwrap();

        // Model A: target = warehouse.staging.orders
        std::fs::write(
            models_dir.join("orders.toml"),
            r#"
name = "orders"
[target]
catalog = "warehouse"
schema = "staging"
table = "orders"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("orders.sql"), "SELECT 1").unwrap();

        // Model B: target = warehouse.marts.customers
        std::fs::write(
            models_dir.join("customers.toml"),
            r#"
name = "customers"
[target]
catalog = "warehouse"
schema = "marts"
table = "customers"
"#,
        )
        .unwrap();
        std::fs::write(models_dir.join("customers.sql"), "SELECT 1").unwrap();

        let tx = rocky_core::config::TransformationPipelineConfig {
            models: "models/**".to_string(),
            target: rocky_core::config::TransformationTargetConfig {
                adapter: "default".to_string(),
                governance: Default::default(),
            },
            checks: Default::default(),
            execution: Default::default(),
            depends_on: vec![],
        };

        let config_path = dir.path().join("rocky.toml");
        let result = resolve_transformation_managed_tables(&tx, &config_path).unwrap();
        let managed = result.expect("should resolve some managed tables");

        assert_eq!(managed.len(), 2);
        assert!(managed.contains("warehouse.staging.orders"));
        assert!(managed.contains("warehouse.marts.customers"));
    }
}
