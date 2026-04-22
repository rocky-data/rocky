use std::path::Path;

use anyhow::{Context, Result};
use tracing::{debug, warn};

use crate::output::*;
use crate::registry;

use super::parsed_to_json_map;

/// Execute `rocky discover`.
pub async fn discover(
    config_path: &Path,
    pipeline_name: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
    let pattern = pipeline.schema_pattern()?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;

    let connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        anyhow::bail!("no discovery adapter configured for this pipeline")
    };

    // Source-existence filter: exclude tables that the discovery adapter
    // reports (e.g. Fivetran "enabled") but that don't actually exist in
    // the source warehouse. This keeps the discover output accurate so
    // downstream orchestrators (Dagster) don't plan assets for ghost tables.
    let source_table_sets =
        build_source_table_sets(&adapter_registry, &pipeline.source, &connectors).await;

    let mut output_sources = Vec::new();
    let mut excluded_tables: Vec<ExcludedTableOutput> = Vec::new();
    for conn in &connectors {
        let parsed = match pattern.parse(&conn.schema) {
            Ok(p) => p,
            Err(e) => {
                warn!(schema = conn.schema, error = %e, "skipping source with unparseable schema");
                continue;
            }
        };

        let components = parsed_to_json_map(&parsed);

        let tables: Vec<TableOutput> = if let Some(existing) = source_table_sets.get(&conn.schema) {
            let mut included = Vec::new();
            let mut skipped = 0usize;
            for t in &conn.tables {
                if existing.contains(&t.name.to_lowercase()) {
                    included.push(TableOutput {
                        name: t.name.clone(),
                        row_count: t.row_count,
                    });
                } else {
                    debug!(
                        table = t.name.as_str(),
                        schema = conn.schema.as_str(),
                        "discover: table not in source, excluding"
                    );
                    // Build the same asset key shape RunOutput uses so
                    // downstream consumers can reconcile excluded entries
                    // against asset graph keys.
                    let mut asset_key = vec![conn.source_type.clone()];
                    for v in components.values() {
                        match v {
                            serde_json::Value::String(s) => asset_key.push(s.clone()),
                            serde_json::Value::Array(arr) => {
                                for item in arr {
                                    if let Some(s) = item.as_str() {
                                        asset_key.push(s.to_string());
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    asset_key.push(t.name.clone());
                    excluded_tables.push(ExcludedTableOutput {
                        asset_key,
                        source_schema: conn.schema.clone(),
                        table_name: t.name.clone(),
                        reason: "missing_from_source".to_string(),
                    });
                    skipped += 1;
                }
            }
            if skipped > 0 {
                warn!(
                    schema = conn.schema.as_str(),
                    skipped,
                    remaining = included.len(),
                    "discover: excluded tables missing from source — see `excluded_tables` in JSON output for details"
                );
            }
            included
        } else {
            // No source table set available — include all (fallback)
            conn.tables
                .iter()
                .map(|t| TableOutput {
                    name: t.name.clone(),
                    row_count: t.row_count,
                })
                .collect()
        };

        output_sources.push(SourceOutput {
            id: conn.id.clone(),
            components,
            source_type: conn.source_type.clone(),
            last_sync_at: conn.last_sync_at,
            tables,
            metadata: conn.metadata.clone(),
        });
    }

    let checks_output = ChecksConfigOutput::from_engine(&pipeline.checks);
    let output = DiscoverOutput::new(output_sources)
        .with_checks(checks_output)
        .with_excluded_tables(excluded_tables);
    if output_json {
        print_json(&output)?;
    } else {
        for s in &output.sources {
            let desc = s
                .components
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join(" ");
            println!("{} | {} | {} tables", s.id, desc, s.tables.len());
        }
    }
    Ok(())
}

/// Build a mapping of schema → existing table names by querying the
/// warehouse adapter's `list_tables` method. Returns an empty map
/// (graceful degradation) if:
/// - The source has no catalog configured
/// - No warehouse adapter is registered for the source adapter name
/// - The adapter doesn't support `list_tables` (default trait impl errors)
async fn build_source_table_sets(
    adapter_registry: &registry::AdapterRegistry,
    source_config: &rocky_core::config::PipelineSourceConfig,
    connectors: &[rocky_core::source::DiscoveredConnector],
) -> std::collections::HashMap<String, std::collections::HashSet<String>> {
    let source_catalog = match source_config.catalog.as_deref() {
        Some(c) if !c.is_empty() => c,
        _ => return Default::default(),
    };

    let adapter = match adapter_registry.warehouse_adapter(&source_config.adapter) {
        Ok(a) => a,
        Err(_) => {
            warn!(
                adapter = source_config.adapter.as_str(),
                "discover: no warehouse adapter registered for source — table-existence filtering disabled"
            );
            return Default::default();
        }
    };

    // Deduplicate schema names before querying
    let schemas: Vec<String> = {
        let mut seen = std::collections::HashSet::new();
        connectors
            .iter()
            .filter_map(|c| {
                if seen.insert(c.schema.clone()) {
                    Some(c.schema.clone())
                } else {
                    None
                }
            })
            .collect()
    };

    let mut map: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();

    for schema in &schemas {
        match adapter.list_tables(source_catalog, schema).await {
            Ok(tables) => {
                let set: std::collections::HashSet<String> =
                    tables.into_iter().map(|t| t.to_lowercase()).collect();
                map.insert(schema.clone(), set);
            }
            Err(e) => {
                warn!(
                    schema = schema.as_str(),
                    error = %e,
                    "discover: failed to list source tables, including all for this schema"
                );
            }
        }
    }

    map
}
