use std::collections::HashSet;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;
use tracing::{debug, info, warn};

use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
use rocky_core::state::StateStore;
use rocky_fivetran::client::FivetranClient;
use rocky_fivetran::envelope::{FivetranStateEnvelope, envelope_hash};
use rocky_fivetran::ratelimit::hash_account_id;

use crate::output::*;
use crate::registry;

use super::parsed_to_json_map;

/// Execute `rocky discover`.
pub async fn discover(
    config_path: &Path,
    pipeline_name: Option<&str>,
    state_path: &Path,
    with_schemas: bool,
    emit_fivetran_state_to: Option<&Path>,
    no_cache: bool,
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

    let (name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
    let pattern = pipeline.schema_pattern()?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;

    let discovery_result = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        anyhow::bail!(
            "{}",
            registry::missing_discovery_config_message(&rocky_cfg, config_path, name)
        )
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

    // Schema-cache warm-up. Only runs when `--with-schemas` is set;
    // config gating already happened at the top of this function.
    // Errors inside the warm-up are logged and counted as misses — a
    // bad source shouldn't abort the whole discovery.
    let schemas_cached = if with_schemas {
        warm_schema_cache(&adapter_registry, &pipeline.source, connectors, state_path).await?
    } else {
        0
    };

    // FR-C — `--emit-fivetran-state-to <PATH>` writes a canonical
    // envelope per Fivetran adapter to disk. Walks the whole config's
    // `[adapter]` table rather than the current pipeline's source so
    // multi-destination projects emit one envelope per destination.
    if let Some(emit_path) = emit_fivetran_state_to {
        emit_fivetran_state(&rocky_cfg, emit_path, no_cache).await?;
    }

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

/// Populate the schema cache for every discovered source.
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

/// FR-C — fetch and emit a canonical envelope per Fivetran adapter
/// declared in the resolved config.
///
/// Walks `rocky_cfg.adapters` filtered by `adapter_type == "fivetran"`
/// (rather than just the current pipeline's source) so multi-destination
/// projects produce one envelope per destination. Builds a fresh
/// [`FivetranClient`] per adapter — the registry is keyed by adapter
/// type via trait-object discovery, not by concrete client instance,
/// so we'd lose the typed `fetch_envelope` surface going through it.
///
/// The write is idempotent: see [`write_envelope_idempotent`] for the
/// hash-sentinel + tmp + rename details.
///
/// `force_refresh` is wired to the `rocky discover --no-cache` flag.
/// When `true`, the persistent state cache layer is skipped on read
/// (envelope comes straight from the API) but still written back so
/// the next call sees fresh data.
async fn emit_fivetran_state(
    rocky_cfg: &rocky_core::config::RockyConfig,
    emit_path: &Path,
    force_refresh: bool,
) -> Result<()> {
    let fivetran_adapters: Vec<(&String, &rocky_core::config::AdapterConfig)> = rocky_cfg
        .adapters
        .iter()
        .filter(|(_, cfg)| cfg.adapter_type == "fivetran")
        .collect();

    if fivetran_adapters.is_empty() {
        warn!(
            path = %emit_path.display(),
            "discover --emit-fivetran-state-to: no Fivetran adapters in config — nothing to emit"
        );
        return Ok(());
    }

    let multi_destination = fivetran_adapters.len() > 1;

    for (name, adapter_cfg) in fivetran_adapters {
        let api_key = adapter_cfg
            .api_key
            .as_ref()
            .map(rocky_core::redacted::RedactedString::expose)
            .with_context(|| format!("adapters.{name}: api_key required for fivetran"))?;
        let api_secret = adapter_cfg
            .api_secret
            .as_ref()
            .map(rocky_core::redacted::RedactedString::expose)
            .with_context(|| format!("adapters.{name}: api_secret required for fivetran"))?;
        let destination_id = adapter_cfg
            .destination_id
            .as_deref()
            .with_context(|| format!("adapters.{name}: destination_id required for fivetran"))?;

        let mut client = FivetranClient::with_retry(
            api_key.to_string(),
            api_secret.to_string(),
            adapter_cfg.retry.clone(),
        );

        // FR-A — instantiate the configured state cache backend. If
        // the adapter has no `[adapter.<name>.cache]` block this
        // resolves to a NoCache backend that's transparent on
        // read/write, so a project that hasn't opted in stays on the
        // pre-FR-A behavior.
        if let Some(cache_cfg) = adapter_cfg.cache.as_ref() {
            let state_cache = rocky_fivetran::state_cache::build_state_cache(cache_cfg)
                .with_context(|| {
                    format!("adapters.{name}: failed to build [adapter.{name}.cache] backend")
                })?;
            client = client.with_state_cache(state_cache);
        }

        // FR-B Phase 2 + Layer 1 + Layer 3 — coordination backends.
        // Each factory returns the default no-op backend when the
        // block is absent, so a project that hasn't opted in stays
        // on the per-host file / NoLock / AlwaysClosed behavior.
        let budget =
            rocky_fivetran::ratelimit::build_ratelimit_budget(adapter_cfg.ratelimit.as_ref())
                .with_context(|| {
                    format!("adapters.{name}: failed to build [adapter.{name}.ratelimit] backend")
                })?;
        client = client.with_ratelimit_budget(budget);

        let (stampede, stampede_tunables) =
            rocky_fivetran::stampede::build_stampede_lock(adapter_cfg.stampede.as_ref())
                .with_context(|| {
                    format!("adapters.{name}: failed to build [adapter.{name}.stampede] backend")
                })?;
        client = client
            .with_stampede_lock(stampede)
            .with_stampede_lock_ttl(stampede_tunables.lock_ttl)
            .with_stampede_poll_timeout(stampede_tunables.poll_timeout);

        let breaker = rocky_fivetran::circuit_breaker::build_circuit_breaker(
            adapter_cfg.circuit_breaker.as_ref(),
        )
        .with_context(|| {
            format!("adapters.{name}: failed to build [adapter.{name}.circuit_breaker] backend")
        })?;
        client = client.with_circuit_breaker(breaker);

        let envelope = client
            .fetch_envelope(destination_id, force_refresh)
            .await
            .with_context(|| {
                format!("fetching Fivetran envelope for adapter '{name}' (destination '{destination_id}')")
            })?;

        let output_path = if multi_destination {
            multi_destination_path(emit_path, api_key, destination_id)
        } else {
            emit_path.to_path_buf()
        };

        write_envelope_idempotent(&envelope, &output_path)
            .with_context(|| format!("writing envelope to {}", output_path.display()))?;
    }

    Ok(())
}

/// Build the per-destination envelope path used in multi-adapter
/// configs — `<PATH>.<account_hash>.<destination_id>.json`.
///
/// `account_hash` mirrors the per-host rate-limit budget's
/// [`hash_account_id`](rocky_fivetran::ratelimit::hash_account_id) so
/// the same scoping is used end-to-end. `destination_id` shows up
/// verbatim in the filename — Fivetran destination ids are
/// snake-case ASCII tokens, safe for path use.
fn multi_destination_path(base: &Path, api_key: &str, destination_id: &str) -> PathBuf {
    let account_hash = hash_account_id(api_key);
    let mut filename = base
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "fivetran-state".to_string());
    // Strip the trailing `.json` if present so the per-destination
    // segments land before the extension: `state.<hash>.<id>.json`
    // rather than `state.json.<hash>.<id>.json`.
    if let Some(stripped) = filename.strip_suffix(".json") {
        filename = stripped.to_string();
    }
    filename.push('.');
    filename.push_str(&account_hash);
    filename.push('.');
    filename.push_str(destination_id);
    filename.push_str(".json");
    match base.parent() {
        Some(parent) => parent.join(filename),
        None => PathBuf::from(filename),
    }
}

/// Write the envelope to `path` idempotently via the hash-sentinel +
/// tmp + rename dance documented on the `--emit-fivetran-state-to`
/// CLI help.
///
/// Algorithm:
///   1. Hash the envelope via [`envelope_hash`] and hex-encode.
///   2. If `<path>.blake3` exists and its trimmed content equals the
///      new hex hash, log INFO and return — both files are left
///      unchanged so downstream `stat(2)` watchers see a stable mtime.
///   3. Otherwise, write the canonical pretty-JSON to `<path>.tmp`,
///      the hex hash + newline to `<path>.blake3.tmp`, fsync both,
///      then atomic-rename each into place.
fn write_envelope_idempotent(envelope: &FivetranStateEnvelope, path: &Path) -> Result<()> {
    let new_hash = envelope_hash(envelope);
    let new_hash_hex = hex_encode(&new_hash);

    let hash_sidecar = blake3_sidecar_path(path);

    if let Ok(existing) = std::fs::read_to_string(&hash_sidecar)
        && existing.trim() == new_hash_hex
    {
        info!(
            path = %path.display(),
            "emit-state: {} unchanged (hash match)",
            path.display()
        );
        return Ok(());
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating parent directory {}", parent.display()))?;
    }

    let json_bytes = serde_json::to_vec_pretty(envelope)
        .context("serializing envelope to pretty JSON for emit")?;

    let json_tmp = path.with_extension(temp_extension(path, "tmp"));
    let hash_tmp = hash_sidecar.with_extension(temp_extension(&hash_sidecar, "tmp"));

    write_and_fsync(&json_tmp, &json_bytes)
        .with_context(|| format!("writing tmp envelope at {}", json_tmp.display()))?;
    let mut hash_payload = new_hash_hex.clone();
    hash_payload.push('\n');
    write_and_fsync(&hash_tmp, hash_payload.as_bytes())
        .with_context(|| format!("writing tmp hash sidecar at {}", hash_tmp.display()))?;

    std::fs::rename(&json_tmp, path)
        .with_context(|| format!("renaming {} -> {}", json_tmp.display(), path.display()))?;
    std::fs::rename(&hash_tmp, &hash_sidecar).with_context(|| {
        format!(
            "renaming {} -> {}",
            hash_tmp.display(),
            hash_sidecar.display()
        )
    })?;

    info!(
        path = %path.display(),
        sidecar = %hash_sidecar.display(),
        "emit-state: {} written ({} bytes)",
        path.display(),
        json_bytes.len()
    );
    Ok(())
}

/// Resolve the `<path>.blake3` sidecar path the idempotent writer
/// pins next to the JSON output.
fn blake3_sidecar_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(".blake3");
    PathBuf::from(s)
}

/// Compute a `.tmp` filename adjacent to `path`. We append rather
/// than swap the extension because the envelope filename in
/// multi-adapter mode already has multiple dots (e.g.
/// `state.<hash>.<id>.json`) and `path.with_extension("tmp")` would
/// stomp the trailing `.json` segment.
fn temp_extension(path: &Path, suffix: &str) -> String {
    let current = path.extension().and_then(|s| s.to_str()).unwrap_or("");
    if current.is_empty() {
        suffix.to_string()
    } else {
        format!("{current}.{suffix}")
    }
}

/// Write `bytes` to `path` and call `sync_all` before returning so
/// the subsequent rename is observed by a fresh open.
fn write_and_fsync(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    let mut f = File::create(path)?;
    f.write_all(bytes)?;
    f.flush()?;
    f.sync_all()?;
    Ok(())
}

/// Lowercase-hex encode 32 bytes. Pulling in `hex` would be a new
/// workspace dep for ~12 lines of code; the existing
/// [`fmt::Write`](std::fmt::Write)-into-`String` pattern matches what
/// `rocky-fivetran::ratelimit::hash_account_id` does for the same
/// reason.
fn hex_encode(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(64);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use rocky_core::source::{DiscoveredConnector, DiscoveredTable};
    use rocky_core::traits::{AdapterError, AdapterResult, BatchCheckAdapter};
    use rocky_ir::ColumnInfo;

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
            _tables: &[rocky_ir::TableRef],
        ) -> AdapterResult<Vec<rocky_core::traits::RowCountResult>> {
            Err(AdapterError::msg("not needed in tests"))
        }

        async fn batch_freshness(
            &self,
            _tables: &[rocky_ir::TableRef],
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

    // -----------------------------------------------------------------
    // FR-C — `--emit-fivetran-state-to` helpers
    // -----------------------------------------------------------------

    use chrono::DateTime;
    use rocky_fivetran::envelope::{
        EnvelopeVersion, FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
        FivetranSchemaConfig, FivetranStateEnvelope,
    };
    use std::collections::BTreeMap;

    fn sample_envelope(connector_id: &str) -> FivetranStateEnvelope {
        let mut schemas = BTreeMap::new();
        schemas.insert(
            connector_id.to_string(),
            FivetranSchemaConfig {
                schemas: BTreeMap::new(),
            },
        );
        FivetranStateEnvelope::from_parts(
            DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
            FivetranDestination {
                id: "dest_smoke".into(),
                region: Some("us-east-1".into()),
                time_zone: Some("UTC".into()),
                service: Some("snowflake".into()),
                setup_status: Some("connected".into()),
            },
            vec![FivetranConnectorSummary {
                id: connector_id.into(),
                name: format!("{connector_id}_name"),
                schema: "src__acme__na__shopify".into(),
                service: "shopify".into(),
                status: FivetranConnectorStatus {
                    setup_state: "connected".into(),
                    sync_state: "scheduled".into(),
                },
                paused: false,
                succeeded_at: None,
                failed_at: None,
                group_id: Some("group_abc".into()),
            }],
            schemas,
        )
    }

    /// First write produces a JSON file at `path` and a `.blake3`
    /// sidecar with the lowercase hex hash. Both files are present
    /// and the JSON parses back to the same envelope.
    #[test]
    fn write_envelope_idempotent_creates_both_files() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let env = sample_envelope("conn_a");
        write_envelope_idempotent(&env, &path).unwrap();

        assert!(path.exists(), "JSON file should exist");
        let sidecar = blake3_sidecar_path(&path);
        assert!(sidecar.exists(), "blake3 sidecar should exist");

        let on_disk: FivetranStateEnvelope =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(on_disk, env);

        let hash = std::fs::read_to_string(&sidecar).unwrap();
        assert_eq!(hash.trim().len(), 64, "blake3 hash hex length is 64");
        assert!(hash.ends_with('\n'), "sidecar must end with newline");
    }

    /// Re-writing the *same* envelope is a no-op — the JSON file's
    /// mtime must NOT advance because the idempotent writer skips the
    /// rename when the hash matches.
    #[test]
    fn write_envelope_idempotent_skips_unchanged() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let env = sample_envelope("conn_a");
        write_envelope_idempotent(&env, &path).unwrap();
        let mtime_before = std::fs::metadata(&path).unwrap().modified().unwrap();

        // Sleep ~50ms to ensure any filesystem mtime resolution can
        // distinguish a real second write from a no-op. On macOS HFS
        // mtimes have second-granularity, but tmpfs is finer. Keeping
        // the sleep small to not bloat CI.
        std::thread::sleep(std::time::Duration::from_millis(50));

        // Re-write the same envelope but with a different fetched_at
        // — the hash excludes fetched_at, so the writer must skip.
        let mut env_b = env.clone();
        env_b.fetched_at = DateTime::<chrono::Utc>::from_timestamp(2_000_000_000, 0).unwrap();
        write_envelope_idempotent(&env_b, &path).unwrap();

        let mtime_after = std::fs::metadata(&path).unwrap().modified().unwrap();
        assert_eq!(
            mtime_before, mtime_after,
            "no-op write must leave mtime unchanged"
        );
    }

    /// Re-writing a *different* envelope updates both files and a
    /// fresh hash lands in the sidecar.
    #[test]
    fn write_envelope_idempotent_updates_on_change() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("state.json");
        let env_a = sample_envelope("conn_a");
        write_envelope_idempotent(&env_a, &path).unwrap();
        let hash_a = std::fs::read_to_string(blake3_sidecar_path(&path))
            .unwrap()
            .trim()
            .to_string();

        let env_b = sample_envelope("conn_b");
        write_envelope_idempotent(&env_b, &path).unwrap();
        let hash_b = std::fs::read_to_string(blake3_sidecar_path(&path))
            .unwrap()
            .trim()
            .to_string();

        assert_ne!(hash_a, hash_b, "hash must update on real change");
        let on_disk: FivetranStateEnvelope =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(on_disk.connectors[0].id, "conn_b");
    }

    #[test]
    fn multi_destination_path_strips_json_suffix() {
        let base = Path::new("/tmp/state.json");
        let p = multi_destination_path(base, "secret-key", "dest_xyz");
        // Account hash is 16 hex chars per `hash_account_id`.
        let s = p.to_string_lossy();
        assert!(s.ends_with(".dest_xyz.json"), "got {s}");
        assert!(s.contains("/tmp/state."), "must keep stem and dirname");
        assert!(
            !s.contains("state.json.dest"),
            "must strip the .json before injecting"
        );
    }

    #[test]
    fn multi_destination_path_handles_extensionless_base() {
        let base = Path::new("/tmp/state");
        let p = multi_destination_path(base, "key", "dest_x");
        let s = p.to_string_lossy();
        assert!(s.ends_with(".dest_x.json"));
        assert!(s.starts_with("/tmp/state."));
    }

    #[test]
    fn envelope_pinned_version_serializes_as_1_0() {
        // Guard against accidental serde renames on EnvelopeVersion
        // — downstream consumers parse this string before doing full
        // deserialization.
        let env = sample_envelope("conn_a");
        let v = serde_json::to_value(&env).unwrap();
        assert_eq!(v["version"], "1.0");
        assert_eq!(env.version, EnvelopeVersion::V1_0);
    }
}
