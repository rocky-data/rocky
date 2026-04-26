use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info, warn};

use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
use rocky_core::state::StateStore;

use crate::output::*;
use crate::registry;

use super::parsed_to_json_map;

/// Execute `rocky discover`.
pub async fn discover(
    config_path: &Path,
    pipeline_name: Option<&str>,
    state_path: &Path,
    with_schemas: bool,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    // Two contradictory signals — user explicitly asked for cache warm-up
    // via `--with-schemas` but also disabled the cache in `rocky.toml`.
    // Erroring is the trust-aligned call: silently skipping would leave
    // the user guessing why `schemas_cached=0`. Design doc §5.4 pairs
    // with the explicit opt-out of `[cache.schemas] enabled = false`.
    if with_schemas && !rocky_cfg.cache.schemas.enabled {
        anyhow::bail!(
            "`--with-schemas` conflicts with `[cache.schemas] enabled = false` in {}; \
             remove the flag or set `enabled = true` to warm the cache",
            config_path.display()
        );
    }

    let (_name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
    let pattern = pipeline.schema_pattern()?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;

    let discovery_result = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        anyhow::bail!("no discovery adapter configured for this pipeline")
    };

    let connectors = &discovery_result.connectors;
    let failed_sources: Vec<FailedSourceOutput> = discovery_result
        .failed
        .iter()
        .cloned()
        .map(FailedSourceOutput::from_engine)
        .collect();
    if !failed_sources.is_empty() {
        warn!(
            count = failed_sources.len(),
            "discover: source(s) failed metadata fetch — surfacing in `failed_sources`; \
             downstream consumers must NOT treat them as deletions"
        );
    }

    // Source-existence filter: exclude tables that the discovery adapter
    // reports (e.g. Fivetran "enabled") but that don't actually exist in
    // the source warehouse. This keeps the discover output accurate so
    // downstream orchestrators (Dagster) don't plan assets for ghost tables.
    let source_table_sets =
        build_source_table_sets(&adapter_registry, &pipeline.source, connectors).await;

    let mut output_sources = Vec::new();
    let mut excluded_tables: Vec<ExcludedTableOutput> = Vec::new();
    for conn in connectors {
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

    // Cache warm-up (Arc 7 wave 2 wave-2 PR 3, design doc §4.2 route B).
    // Only runs when `--with-schemas` is set; config gating already happened
    // at the top of this function. Errors inside the warm-up are logged and
    // counted as misses — a bad source shouldn't abort the whole discovery.
    let schemas_cached = if with_schemas {
        warm_schema_cache(&adapter_registry, &pipeline.source, connectors, state_path).await?
    } else {
        0
    };

    let checks_output = ChecksConfigOutput::from_engine(&pipeline.checks);
    let output = DiscoverOutput::new(output_sources)
        .with_checks(checks_output)
        .with_excluded_tables(excluded_tables)
        .with_failed_sources(failed_sources)
        .with_schemas_cached(schemas_cached);
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
        if with_schemas {
            println!("schemas cached: {schemas_cached}");
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

/// Populate the Arc 7 wave 2 wave-2 schema cache for every discovered source.
///
/// For each unique `(catalog, schema)` pair reachable via the source's
/// warehouse `BatchCheckAdapter`, issues a single `batch_describe_schema`
/// round-trip and persists one `SchemaCacheEntry` per returned table via
/// [`StateStore::write_schema_cache_entry`]. Returns the count of entries
/// actually written (partial successes roll in — one failing schema does not
/// invalidate writes from another).
///
/// Error-to-warn rules:
/// - missing `source.catalog` → warn once, return 0 (no keys possible).
/// - `batch_check_adapter` returns `None` (adapter is DuckDB/etc.) →
///   warn once, return 0.
/// - per-schema `batch_describe_schema` failure → warn and continue.
/// - per-entry `write_schema_cache_entry` failure → warn and continue.
/// - state store won't open → bail (the user asked for writes; a broken
///   state dir is a hard fail, not a silent no-op).
async fn warm_schema_cache(
    adapter_registry: &registry::AdapterRegistry,
    source_config: &rocky_core::config::PipelineSourceConfig,
    connectors: &[rocky_core::source::DiscoveredConnector],
    state_path: &Path,
) -> Result<usize> {
    let source_catalog = match source_config.catalog.as_deref() {
        Some(c) if !c.is_empty() => c.to_string(),
        _ => {
            warn!(
                "discover --with-schemas: source.catalog not configured — \
                 cannot key cache entries, skipping"
            );
            return Ok(0);
        }
    };

    let Some(batch_adapter) = adapter_registry.batch_check_adapter(&source_config.adapter) else {
        warn!(
            adapter = source_config.adapter.as_str(),
            "discover --with-schemas: no batch-describe-capable adapter \
             registered for this source (e.g. DuckDB has no batched describe) \
             — cache warm-up skipped"
        );
        return Ok(0);
    };

    let schemas = dedup_schemas(connectors);

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    warm_schema_cache_inner(batch_adapter.as_ref(), &store, &source_catalog, &schemas).await
}

/// Dedup the schema names reported by discovered connectors.
///
/// Mirrors the dedup in `build_source_table_sets` — Fivetran commonly hands
/// back two connectors against the same `src__*` schema, and a `BatchCheck`
/// `information_schema` round-trip keyed on `<catalog, schema>` is wasted
/// work when repeated.
fn dedup_schemas(connectors: &[rocky_core::source::DiscoveredConnector]) -> Vec<String> {
    let mut seen = HashSet::new();
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
}

/// Inner warm-up loop — split out so unit tests can drive it with a stub
/// [`rocky_core::traits::BatchCheckAdapter`].
///
/// Per-schema `batch_describe_schema` errors log a warn and are counted as
/// zero writes; per-entry `write_schema_cache_entry` errors also warn and
/// skip. Both paths preserve the "one bad source shouldn't abort warm-up"
/// contract the design doc §4.2 gives.
async fn warm_schema_cache_inner(
    batch_adapter: &dyn rocky_core::traits::BatchCheckAdapter,
    store: &StateStore,
    source_catalog: &str,
    schemas: &[String],
) -> Result<usize> {
    let cached_at = Utc::now();
    let mut written: usize = 0;

    for schema in schemas {
        let describe = batch_adapter
            .batch_describe_schema(source_catalog, schema)
            .await;
        let tables = match describe {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    catalog = source_catalog,
                    schema = schema.as_str(),
                    error = %e,
                    "discover --with-schemas: batch_describe_schema failed, \
                     skipping schema"
                );
                continue;
            }
        };

        for (table_name, column_infos) in tables {
            let key = schema_cache_key(source_catalog, schema, &table_name);
            let entry = SchemaCacheEntry {
                columns: column_infos
                    .into_iter()
                    .map(|c| StoredColumn {
                        name: c.name,
                        data_type: c.data_type,
                        nullable: c.nullable,
                    })
                    .collect(),
                cached_at,
            };
            match store.write_schema_cache_entry(&key, &entry) {
                Ok(()) => {
                    debug!(
                        key = key.as_str(),
                        "discover --with-schemas: wrote cache entry"
                    );
                    written += 1;
                }
                Err(e) => {
                    warn!(
                        key = key.as_str(),
                        error = %e,
                        "discover --with-schemas: failed to write cache entry, skipping"
                    );
                }
            }
        }
    }

    info!(
        schemas = schemas.len(),
        entries_written = written,
        "discover --with-schemas: cache warm-up complete"
    );

    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use rocky_core::ir::ColumnInfo;
    use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
    use rocky_core::traits::{AdapterError, AdapterResult, BatchCheckAdapter};

    /// Alias for the describe-response payload — keeps the stub's field
    /// type under clippy's `type_complexity` limit.
    type DescribeResponse = AdapterResult<HashMap<String, Vec<ColumnInfo>>>;

    /// Stub adapter whose `batch_describe_schema` replays a pre-seeded
    /// `(catalog, schema)`-keyed map. Any unknown key returns an error so
    /// tests can cover the "one bad source doesn't abort warm-up" case.
    struct StubBatchAdapter {
        responses: HashMap<(String, String), DescribeResponse>,
        calls: Mutex<Vec<(String, String)>>,
    }

    impl StubBatchAdapter {
        fn new() -> Self {
            Self {
                responses: HashMap::new(),
                calls: Mutex::new(Vec::new()),
            }
        }

        fn with_ok(
            mut self,
            catalog: &str,
            schema: &str,
            cols: HashMap<String, Vec<ColumnInfo>>,
        ) -> Self {
            self.responses
                .insert((catalog.to_string(), schema.to_string()), Ok(cols));
            self
        }

        fn with_err(mut self, catalog: &str, schema: &str, message: &str) -> Self {
            self.responses.insert(
                (catalog.to_string(), schema.to_string()),
                Err(AdapterError::msg(message)),
            );
            self
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl BatchCheckAdapter for StubBatchAdapter {
        async fn batch_row_counts(
            &self,
            _tables: &[rocky_core::ir::TableRef],
        ) -> AdapterResult<Vec<rocky_core::traits::RowCountResult>> {
            Err(AdapterError::msg("not needed in tests"))
        }

        async fn batch_freshness(
            &self,
            _tables: &[rocky_core::ir::TableRef],
            _timestamp_col: &str,
        ) -> AdapterResult<Vec<rocky_core::traits::FreshnessResult>> {
            Err(AdapterError::msg("not needed in tests"))
        }

        async fn batch_describe_schema(
            &self,
            catalog: &str,
            schema: &str,
        ) -> AdapterResult<HashMap<String, Vec<ColumnInfo>>> {
            self.calls
                .lock()
                .unwrap()
                .push((catalog.to_string(), schema.to_string()));
            match self
                .responses
                .get(&(catalog.to_string(), schema.to_string()))
            {
                Some(Ok(map)) => Ok(map.clone()),
                Some(Err(e)) => Err(AdapterError::msg(format!("{e}"))),
                None => Err(AdapterError::msg("stub: unknown (catalog, schema) pair")),
            }
        }
    }

    fn col(name: &str, ty: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: ty.to_string(),
            nullable,
        }
    }

    fn discovered(schema: &str) -> DiscoveredConnector {
        DiscoveredConnector {
            id: format!("conn_{schema}"),
            schema: schema.to_string(),
            source_type: "shopify".to_string(),
            last_sync_at: None,
            tables: vec![DiscoveredTable {
                name: "orders".to_string(),
                row_count: None,
            }],
            metadata: Default::default(),
        }
    }

    #[test]
    fn dedup_schemas_drops_repeat_connector_schemas() {
        let connectors = vec![
            discovered("raw__shopify"),
            discovered("raw__shopify"),
            discovered("raw__netsuite"),
        ];
        let schemas = dedup_schemas(&connectors);
        assert_eq!(schemas, vec!["raw__shopify", "raw__netsuite"]);
    }

    #[tokio::test]
    async fn warm_inner_writes_one_entry_per_table() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::open(&tmp.path().join("state.redb")).unwrap();

        let mut cols = HashMap::new();
        cols.insert(
            "orders".to_string(),
            vec![col("id", "BIGINT", false), col("email", "STRING", true)],
        );
        cols.insert(
            "order_items".to_string(),
            vec![col("order_id", "BIGINT", false)],
        );
        let adapter = StubBatchAdapter::new().with_ok("prod", "raw__shopify", cols);

        let written =
            warm_schema_cache_inner(&adapter, &store, "prod", &["raw__shopify".to_string()])
                .await
                .unwrap();
        assert_eq!(written, 2);
        assert_eq!(adapter.call_count(), 1);

        let orders_key = schema_cache_key("prod", "raw__shopify", "orders");
        let entry = store.read_schema_cache_entry(&orders_key).unwrap().unwrap();
        assert_eq!(entry.columns.len(), 2);
        assert_eq!(entry.columns[0].name, "id");
        assert_eq!(entry.columns[0].data_type, "BIGINT");
        assert!(!entry.columns[0].nullable);

        let items_key = schema_cache_key("prod", "raw__shopify", "order_items");
        assert!(store.read_schema_cache_entry(&items_key).unwrap().is_some());
    }

    #[tokio::test]
    async fn warm_inner_continues_past_schema_describe_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::open(&tmp.path().join("state.redb")).unwrap();

        let mut good = HashMap::new();
        good.insert("orders".to_string(), vec![col("id", "BIGINT", false)]);
        let adapter = StubBatchAdapter::new()
            .with_err("prod", "raw__broken", "Databricks: permission denied")
            .with_ok("prod", "raw__good", good);

        let schemas = vec!["raw__broken".to_string(), "raw__good".to_string()];
        let written = warm_schema_cache_inner(&adapter, &store, "prod", &schemas)
            .await
            .unwrap();
        assert_eq!(written, 1);
        assert_eq!(adapter.call_count(), 2);

        let bad_key = schema_cache_key("prod", "raw__broken", "anything");
        assert!(store.read_schema_cache_entry(&bad_key).unwrap().is_none());
        let ok_key = schema_cache_key("prod", "raw__good", "orders");
        assert!(store.read_schema_cache_entry(&ok_key).unwrap().is_some());
    }

    #[tokio::test]
    async fn warm_inner_returns_zero_for_empty_schema_list() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::open(&tmp.path().join("state.redb")).unwrap();
        let adapter = StubBatchAdapter::new();

        let written = warm_schema_cache_inner(&adapter, &store, "prod", &[])
            .await
            .unwrap();
        assert_eq!(written, 0);
        assert_eq!(adapter.call_count(), 0);
        assert!(store.list_schema_cache().unwrap().is_empty());
    }

    #[tokio::test]
    async fn warm_inner_lowercases_key_components() {
        let tmp = tempfile::tempdir().unwrap();
        let store = StateStore::open(&tmp.path().join("state.redb")).unwrap();

        let mut cols = HashMap::new();
        cols.insert("Orders".to_string(), vec![col("ID", "BIGINT", false)]);
        let adapter = StubBatchAdapter::new().with_ok("PROD", "RAW__Shopify", cols);

        let written =
            warm_schema_cache_inner(&adapter, &store, "PROD", &["RAW__Shopify".to_string()])
                .await
                .unwrap();
        assert_eq!(written, 1);

        // Keys are stored lowercase (design §4.1). The adapter call itself
        // preserves casing — it's the key composition that lowercases.
        let key = schema_cache_key("PROD", "RAW__Shopify", "Orders");
        assert_eq!(key, "prod.raw__shopify.orders");
        assert!(store.read_schema_cache_entry(&key).unwrap().is_some());
    }
}
