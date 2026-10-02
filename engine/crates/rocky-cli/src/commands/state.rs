use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::config::load_rocky_config;
use rocky_core::config::{PipelineConfig, ReplicationPipelineConfig, resolve_table_override};
use rocky_core::retention::StateRetentionConfig;
use rocky_core::state::StateStore;
use rocky_core::state_sync::{FinalizeDurability, RemoteStateSession, StateAuthority};
use rocky_ir::{TableRef, WatermarkState};

use crate::registry::AdapterRegistry;

/// Rebuild incremental cursors from the physical targets recorded in the
/// pipeline's checkpoints. The warehouse queries finish before any
/// state write; a failed query leaves every cursor unchanged.
pub async fn state_reconcile_watermark(
    config_path: &Path,
    state_path: &Path,
    pipeline_name: &str,
    selected: &[String],
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    let config = load_rocky_config(config_path)?;
    let pipeline = match config.pipelines.get(pipeline_name) {
        Some(PipelineConfig::Replication(pipeline)) => pipeline,
        _ => anyhow::bail!("'{pipeline_name}' is not a replication pipeline"),
    };
    let mut session = RemoteStateSession::new(
        &config.state,
        state_path,
        FinalizeDurability::Durable,
        config.cache.schemas.replicate,
    );
    let authority = session.acquire().await?;
    let result: Result<ReconcileWatermarkOutput> = async {
        anyhow::ensure!(
            authority == StateAuthority::Authoritative,
            "watermark repair requires authoritative existing state"
        );
        let store = StateStore::open(state_path)?;
        let adapter = config
            .adapters
            .get(&pipeline.target.adapter)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "target adapter '{}' is not configured",
                    pipeline.target.adapter
                )
            })?;
        let planned =
            store.recorded_pipeline_targets(pipeline_name, &adapter.endpoint_identity())?;
        anyhow::ensure!(
            !planned.is_empty(),
            "no recorded physical targets for pipeline '{pipeline_name}'"
        );
        let targets: Vec<String> = if selected.is_empty() {
            planned
        } else {
            for key in selected {
                anyhow::ensure!(
                    planned.contains(key),
                    "target '{key}' is not in the pipeline's recorded plans"
                );
            }
            selected.to_vec()
        };
        let registry = AdapterRegistry::from_config(&config)?;
        let columns = repair_timestamp_columns(pipeline, &registry, &targets).await?;
        let warehouse = registry.warehouse_adapter(&pipeline.target.adapter)?;
        let mut watermarks = Vec::new();
        for key in targets {
            let components: Vec<&str> = key.split('.').collect();
            anyhow::ensure!(
                components.len() == 3 && components[1..].iter().all(|s| !s.is_empty()),
                "target '{key}' is not a catalog.schema.table name"
            );
            anyhow::ensure!(
                !components[0].is_empty()
                    || config
                        .adapters
                        .get(&pipeline.target.adapter)
                        .is_some_and(|adapter| adapter.adapter_type == "duckdb"),
                "target '{key}' has no pinned catalog; repair requires a physical catalog"
            );
            let target = TableRef {
                catalog: components[0].to_string(),
                schema: components[1].to_string(),
                table: components[2].to_string(),
            };
            let column = columns.get(&key).ok_or_else(|| {
                anyhow::anyhow!("target '{key}' has no resolved timestamp column")
            })?;
            let target_max = super::run::query_target_max_timestamp(
                warehouse.as_ref(),
                warehouse.dialect(),
                &target,
                column,
            )
            .await?;
            let previous = store.get_watermark(&key)?.map(|wm| wm.last_value);
            watermarks.push(ReconciledWatermark {
                table: key,
                previous,
                target_max,
                watermark: target_max,
            });
        }
        if !dry_run {
            let updates: Vec<(String, Option<WatermarkState>)> = watermarks
                .iter()
                .map(|item| {
                    (
                        item.table.clone(),
                        item.watermark.map(|last_value| WatermarkState {
                            last_value,
                            updated_at: chrono::Utc::now(),
                        }),
                    )
                })
                .collect();
            store.batch_reconcile_watermarks(&updates)?;
        }
        Ok(ReconcileWatermarkOutput {
            version: env!("CARGO_PKG_VERSION").to_string(),
            command: "state-reconcile-watermark".to_string(),
            pipeline: pipeline_name.to_string(),
            dry_run,
            watermarks,
        })
    }
    .await;
    let output = match result {
        Ok(output) => output,
        Err(error) => {
            session
                .abandon("watermark repair failed before finalize")
                .await;
            return Err(error);
        }
    };
    if dry_run {
        session.abandon("dry-run").await;
    } else {
        session.finalize().await?;
    }
    if output_json {
        print_json(&output)?;
    } else {
        for item in &output.watermarks {
            let prefix = if item.watermark.is_none() {
                if dry_run { "would clear" } else { "cleared" }
            } else if dry_run {
                "would set"
            } else {
                "set"
            };
            println!(
                "{prefix} {} = {} (target MAX: {})",
                item.table,
                item.watermark
                    .map_or_else(|| "NULL".to_string(), |value| value.to_string()),
                item.target_max
                    .map_or_else(|| "NULL".to_string(), |value| value.to_string())
            );
        }
    }
    Ok(())
}

/// Resolve against the same discovered connector and rendered target names
/// used by replication. A historical target without a current source can be
/// repaired only when its table-level override is unambiguous.
async fn repair_timestamp_columns(
    pipeline: &ReplicationPipelineConfig,
    registry: &AdapterRegistry,
    targets: &[String],
) -> Result<std::collections::HashMap<String, String>> {
    let pattern = pipeline.schema_pattern()?;
    let mut columns = std::collections::HashMap::new();
    let needs_connector = targets.iter().any(|key| {
        let table = key.rsplit('.').next().unwrap_or("");
        pipeline.table_overrides.iter().any(|rule| {
            rule.timestamp_column.is_some()
                && rule.match_.connector.is_some()
                && rule.match_.matches_table(table)
        })
    });
    if needs_connector {
        let discovery = pipeline.source.discovery.as_ref().ok_or_else(|| {
            anyhow::anyhow!("connector-specific timestamp overrides require a discovery adapter")
        })?;
        let discovered = registry
            .discovery_adapter(&discovery.adapter)?
            .discover(&pattern.prefix)
            .await?;
        anyhow::ensure!(
            discovered.failed.is_empty(),
            "source discovery was incomplete; cannot resolve timestamp overrides safely"
        );
        let separator = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        for connector in discovered.connectors {
            let Ok(parsed) = pattern.parse(&connector.schema) else {
                continue;
            };
            let catalog = parsed.resolve_template(&pipeline.target.catalog_template, separator);
            let schema = parsed.resolve_template(&pipeline.target.schema_template, separator);
            for table in connector.tables {
                let key = format!("{catalog}.{schema}.{}", table.name);
                if !targets.contains(&key) {
                    continue;
                }
                let resolved = resolve_table_override(
                    &pipeline.table_overrides,
                    &connector.id,
                    &connector.schema,
                    &table.name,
                );
                let column = resolved
                    .timestamp_column
                    .unwrap_or_else(|| pipeline.timestamp_column.clone());
                if let Some(previous) = columns.insert(key.clone(), column.clone()) {
                    anyhow::ensure!(
                        previous == column,
                        "target '{key}' has conflicting timestamp overrides"
                    );
                }
            }
        }
    }
    for key in targets {
        if columns.contains_key(key) {
            continue;
        }
        let table = key.rsplit('.').next().unwrap_or("");
        anyhow::ensure!(
            !pipeline
                .table_overrides
                .iter()
                .any(|rule| rule.timestamp_column.is_some()
                    && rule.match_.connector.is_some()
                    && rule.match_.matches_table(table)),
            "target '{key}' has a connector-specific timestamp override but its source was not discovered"
        );
        let resolved = resolve_table_override(&pipeline.table_overrides, "", "", table);
        columns.insert(
            key.clone(),
            resolved
                .timestamp_column
                .unwrap_or_else(|| pipeline.timestamp_column.clone()),
        );
    }
    Ok(columns)
}

use crate::output::*;

/// Execute `rocky state show`.
///
/// Reports the watermark set plus both schema versions (the version this
/// binary supports and the version stamped in the on-disk file). The schema
/// versions are read via [`StateStore::peek_schema_version`] so they are
/// reported even when the on-disk store is **forward-incompatible** (newer
/// than this binary) — that is exactly when an operator most needs the
/// numbers. A forward-incompatible store still prints its versions with an
/// empty watermark list and a warning, rather than failing outright.
pub fn state_show(state_path: &Path, output_json: bool) -> Result<()> {
    let schema_version_on_disk =
        StateStore::peek_schema_version(state_path).map_err(|e| anyhow::anyhow!("{e}"))?;

    let entries: Vec<WatermarkEntry> = match StateStore::open_read_only_or_empty(state_path) {
        Ok(store) => store
            .list_watermarks()
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .into_iter()
            .map(|(table, wm)| WatermarkEntry {
                table,
                last_value: wm.last_value,
                updated_at: wm.updated_at,
            })
            .collect(),
        // Forward-incompatible store: still report the version fields (the
        // point of this command for an orchestrator startup hook) but skip the
        // watermark read — this binary can't safely parse the newer layout.
        Err(rocky_core::state::StateError::SchemaMismatch {
            found, expected, ..
        }) => {
            eprintln!(
                "warning: on-disk state schema v{found} is newer than this binary supports \
                 (v{expected}); watermarks are not shown. Upgrade rocky to read this state."
            );
            Vec::new()
        }
        Err(e) => {
            return Err(anyhow::anyhow!("{e}").context(format!(
                "failed to open state store at {}",
                state_path.display()
            )));
        }
    };

    if output_json {
        print_json(&StateOutput::new(entries, schema_version_on_disk))?;
    } else {
        println!(
            "schema version: binary supports v{}, on disk {}",
            rocky_core::state::current_schema_version(),
            schema_version_on_disk
                .map(|v| format!("v{v}"))
                .unwrap_or_else(|| "none".to_string()),
        );
        for e in &entries {
            println!("{} | {} | {}", e.table, e.last_value, e.updated_at);
        }
        if entries.is_empty() {
            println!("No watermarks stored.");
        }
    }
    Ok(())
}

/// Execute `rocky state clear-schema-cache`.
///
/// The explicit-flush path for the DESCRIBE cache. Counterpart to the
/// TTL auto-eviction baked into the read path: users who want a cache
/// refresh *now* (e.g. after a manual warehouse DDL change, or during
/// strict-CI debugging) use this command.
///
/// Behaviour:
/// - No prompt (the cache is cheap to rebuild via the next `rocky run`
///   or `rocky discover --with-schemas` — explicit opt-in is sufficient).
/// - `dry_run = true` reports what *would* be removed without touching
///   redb. Useful for automation scripts that want to assert emptiness
///   before a scheduled flush.
/// - Does NOT fail when the state store is missing (fresh clone, nothing
///   to flush) — emits an empty result and exits zero. Failing here would
///   be user-hostile for the "make sure the cache is clear before this
///   CI run" use case on an ephemeral runner.
pub fn state_clear_schema_cache(state_path: &Path, dry_run: bool, output_json: bool) -> Result<()> {
    // Missing state.redb → nothing to flush. Report 0 and exit cleanly so
    // CI pipelines that run `rocky state clear-schema-cache` unconditionally
    // before a build don't fail on a fresh runner.
    if !state_path.exists() {
        return emit_result(0, dry_run, output_json);
    }

    let store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    // Gather the current key set up front. `list_schema_cache` already
    // tolerates an empty table, so an uninitialised cache returns
    // `entries_deleted = 0` without ever reaching the delete loop.
    let entries = store
        .list_schema_cache()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let count = entries.len();

    if !dry_run {
        for (key, _entry) in &entries {
            store
                .delete_schema_cache_entry(key)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    emit_result(count, dry_run, output_json)
}

/// Execute `rocky state retention sweep`.
///
/// Loads the [`StateRetentionConfig`] from the project's `[state.retention]`
/// block (falling back to the defaults — `max_age_days = 365`,
/// `min_runs_kept = 100`, `applies_to = [history, lineage, audit]` —
/// when the section is absent), then sweeps run history, DAG snapshots,
/// and quality snapshots accordingly.
///
/// Behaviour:
/// - No `rocky.toml` at `config_path` → the defaults apply. That is the init
///   flow, and the only case that sweeps without reading a policy.
/// - A `rocky.toml` that is present but cannot be loaded → **refuses**. The
///   sweep deletes rows and nothing puts them back, so a config that might
///   have said `max_age_days = 3650` must never be skipped in silence.
/// - Missing `state.redb` → emits a zero-count report and exits cleanly,
///   matching `state clear-schema-cache` (CI-safe on ephemeral runners).
/// - `dry_run = true` runs the planner but skips every write transaction
///   so the store is left untouched. The reported counts match what an
///   apply run would produce, modulo concurrent writers.
pub fn state_retention_sweep(
    config_path: &Path,
    state_path: &Path,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    // Read the policy from rocky.toml so the same sweep semantics apply
    // whether the operator runs the command manually or it's wired into a
    // future scheduled hook.
    //
    // The loader IS the probe. A `config_path.exists()` gate in front of it
    // was a second source of truth that disagreed with the loader: `exists()`
    // is `metadata(..).is_ok()`, which FOLLOWS a symlink, so a `rocky.toml`
    // symlink whose target is gone answered `false`. The project's
    // `[state.retention]` was never read, the defaults applied, and the sweep
    // deleted run history the config said to keep — exit 0, no warning,
    // irreversible (#1729). `ConfigError::FileNotFound` is the one signal that
    // means "this project has no config" (#1668); every other error is a
    // config that IS there and could not be read, and the sweep refuses on it.
    let policy = match load_rocky_config(config_path) {
        Ok(cfg) => cfg.state.retention,
        // The init flow: no rocky.toml at all. Sweep with the defaults.
        Err(rocky_core::config::ConfigError::FileNotFound { .. }) => {
            StateRetentionConfig::default()
        }
        // A config is there and could not be loaded — a dangling symlink, a
        // directory, a parse error, an unresolved `${VAR}`. Fail closed: this
        // sweep is irreversible.
        Err(e) => {
            return Err(anyhow::Error::new(e))
                .with_context(|| format!("loading rocky config at {}", config_path.display()));
        }
    };

    if !state_path.exists() {
        emit_sweep_report(&Default::default(), &policy, dry_run, output_json)?;
        return Ok(());
    }

    let store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    let report = if dry_run {
        store
            .sweep_retention_dry_run(&policy)
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        store
            .sweep_retention(&policy)
            .map_err(|e| anyhow::anyhow!("{e}"))?
    };

    emit_sweep_report(&report, &policy, dry_run, output_json)
}

fn emit_sweep_report(
    report: &rocky_core::retention::SweepReport,
    policy: &StateRetentionConfig,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    if output_json {
        print_json(&RetentionSweepOutput::new(report, policy, dry_run))?;
    } else {
        let prefix = if dry_run {
            "[dry-run] would remove"
        } else {
            "Removed"
        };
        println!(
            "{prefix} {} run records, {} dag snapshots, {} quality snapshots ({} ms)",
            report.runs_deleted, report.lineage_deleted, report.audit_deleted, report.duration_ms,
        );
        println!(
            "Kept: {} runs, {} dag snapshots, {} quality snapshots",
            report.runs_kept, report.lineage_kept, report.audit_kept,
        );
    }
    Ok(())
}

/// Execute `rocky state schedule pause|resume <pipeline>` — the human side of
/// the runtime schedule hold (the agent side is the `pause_schedule` MCP tool,
/// which is pause-only on purpose: resuming re-enables autonomous runs).
///
/// Refuses a pipeline with no `[schedule]` block: the hold must attach to
/// something the reconciler consults, never a stray cursor.
pub fn state_schedule_hold(
    config_path: &Path,
    state_path: &Path,
    pipeline: &str,
    paused: bool,
    output_json: bool,
) -> Result<()> {
    let config = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config {}", config_path.display()))?;
    let has_schedule = config
        .pipelines
        .get(pipeline)
        .map(|p| p.schedule().is_some())
        .unwrap_or(false);
    if !has_schedule {
        anyhow::bail!(
            "pipeline '{pipeline}' has no [schedule] block (or does not exist) — the hold \
             attaches to scheduled pipelines only"
        );
    }
    let store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;
    let changed = store
        .set_schedule_paused(pipeline, paused)
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if output_json {
        print_json(&crate::output::ScheduleHoldOutput::new(
            pipeline, paused, changed, state_path,
        ))?;
    } else {
        let verb = if paused { "Paused" } else { "Resumed" };
        let note = if changed {
            ""
        } else {
            " (already in that state)"
        };
        // The acted-on store is part of the answer: a scheduler is controlled
        // by this hold only if it reads the SAME state file.
        println!(
            "{verb} schedule for '{pipeline}'{note} (state: {})",
            state_path.display()
        );
    }
    Ok(())
}

fn emit_result(count: usize, dry_run: bool, output_json: bool) -> Result<()> {
    if output_json {
        print_json(&ClearSchemaCacheOutput::new(count, dry_run))?;
    } else if dry_run {
        println!(
            "[dry-run] would remove {count} schema cache entr{}",
            if count == 1 { "y" } else { "ies" }
        );
    } else {
        println!(
            "Removed {count} schema cache entr{}",
            if count == 1 { "y" } else { "ies" }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use tempfile::TempDir;

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn reconcile_watermark_repairs_stale_cursor_and_dry_run_does_not_write() {
        use chrono::TimeZone;
        use rocky_core::state::{ResumeScope, ResumeTarget, TableProgress, TableStatus};
        use rocky_core::traits::WarehouseAdapter;
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("warehouse.duckdb");
        let warehouse = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db_path).unwrap();
        warehouse
            .execute_statement("CREATE SCHEMA tgt")
            .await
            .unwrap();
        warehouse
            .execute_statement("CREATE TABLE tgt.events (id BIGINT, ts TIMESTAMP)")
            .await
            .unwrap();
        warehouse.execute_statement("INSERT INTO tgt.events VALUES (1, '2026-09-01 00:00:00'), (2, '2026-09-02 00:00:00')").await.unwrap();
        drop(warehouse);
        let config_path = tmp.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"
[state]
backend = "local"
[pipeline.p]
type = "replication"
strategy = "incremental"
timestamp_column = "ts"
[pipeline.p.source.discovery]
adapter = "default"
[pipeline.p.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]
[pipeline.p.target]
adapter = "default"
catalog_template = ""
schema_template = "tgt"
[[pipeline.p.table_overrides]]
timestamp_column = "other_ts"
[pipeline.p.table_overrides.match]
table = "other"
"#,
                db_path.display()
            ),
        )
        .unwrap();
        let state_path = tmp.path().join("state.redb");
        let stale = Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap();
        let config = load_rocky_config(&config_path).unwrap();
        let store = StateStore::open(&state_path).unwrap();
        store
            .init_run_progress(
                "old",
                &[".tgt.events".into()],
                Some(&ResumeScope {
                    pipeline: "p".into(),
                    filter: None,
                    target: Some(ResumeTarget {
                        adapter: "default".into(),
                        catalog_template: String::new(),
                        schema_template: Some("tgt".into()),
                        separator_role: None,
                        endpoint: config.adapters["default"].endpoint_identity(),
                        shadow: None,
                    }),
                    source: None,
                }),
            )
            .unwrap();
        store
            .record_table_progress(
                "old",
                &TableProgress {
                    index: 0,
                    table_key: ".tgt.events".into(),
                    asset_key: vec!["events".into()],
                    status: TableStatus::Success,
                    error: None,
                    duration_ms: 1,
                    completed_at: Utc::now(),
                },
            )
            .unwrap();
        store
            .set_watermark(
                ".tgt.events",
                &WatermarkState {
                    last_value: stale,
                    updated_at: stale,
                },
            )
            .unwrap();
        drop(store);
        state_reconcile_watermark(&config_path, &state_path, "p", &[], true, false)
            .await
            .unwrap();
        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(
            store
                .get_watermark(".tgt.events")
                .unwrap()
                .unwrap()
                .last_value,
            stale
        );
        drop(store);
        state_reconcile_watermark(&config_path, &state_path, "p", &[], false, false)
            .await
            .unwrap();
        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(
            store
                .get_watermark(".tgt.events")
                .unwrap()
                .unwrap()
                .last_value,
            Utc.with_ymd_and_hms(2026, 9, 2, 0, 0, 0).unwrap()
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn reconcile_watermark_clears_empty_target_and_resolves_selected_overrides() {
        use chrono::TimeZone;
        use rocky_core::state::{ResumeScope, ResumeTarget};
        use rocky_core::traits::WarehouseAdapter;
        let tmp = TempDir::new().unwrap();
        let db_path = tmp.path().join("warehouse.duckdb");
        let warehouse = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db_path).unwrap();
        warehouse
            .execute_statement("CREATE SCHEMA tgt")
            .await
            .unwrap();
        warehouse
            .execute_statement("CREATE TABLE tgt.events (ts TIMESTAMP)")
            .await
            .unwrap();
        warehouse
            .execute_statement("CREATE SCHEMA src__x")
            .await
            .unwrap();
        for table in ["events", "special", "empty"] {
            warehouse
                .execute_statement(&format!("CREATE TABLE src__x.{table} (ts TIMESTAMP) "))
                .await
                .unwrap();
        }
        warehouse
            .execute_statement("INSERT INTO tgt.events VALUES (TIMESTAMP '2026-09-01 00:00:00')")
            .await
            .unwrap();
        warehouse
            .execute_statement("CREATE TABLE tgt.special (ts TIMESTAMP, override_ts TIMESTAMP)")
            .await
            .unwrap();
        warehouse.execute_statement("INSERT INTO tgt.special VALUES (TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP '2026-09-05 00:00:00')").await.unwrap();
        warehouse
            .execute_statement("CREATE TABLE tgt.empty (ts TIMESTAMP)")
            .await
            .unwrap();
        drop(warehouse);
        let config_path = tmp.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{}"
[state]
backend = "local"
[pipeline.p]
type = "replication"
strategy = "incremental"
timestamp_column = "ts"
[pipeline.p.source.discovery]
adapter = "default"
[pipeline.p.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["source"]
[pipeline.p.target]
adapter = "default"
catalog_template = ""
schema_template = "tgt"
[[pipeline.p.table_overrides]]
timestamp_column = "override_ts"
[pipeline.p.table_overrides.match]
connector = "src__x"
table = "special"
"#,
                db_path.display()
            ),
        )
        .unwrap();
        let config = load_rocky_config(&config_path).unwrap();
        let state_path = tmp.path().join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        let keys = [
            ".tgt.events".to_string(),
            ".tgt.special".to_string(),
            ".tgt.empty".to_string(),
        ];
        store
            .init_run_progress(
                "old",
                &keys,
                Some(&ResumeScope {
                    pipeline: "p".into(),
                    filter: None,
                    target: Some(ResumeTarget {
                        adapter: "default".into(),
                        catalog_template: String::new(),
                        schema_template: Some("tgt".into()),
                        separator_role: None,
                        endpoint: config.adapters["default"].endpoint_identity(),
                        shadow: None,
                    }),
                    source: None,
                }),
            )
            .unwrap();
        let stale = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        for key in &keys {
            store
                .set_watermark(
                    key,
                    &WatermarkState {
                        last_value: stale,
                        updated_at: stale,
                    },
                )
                .unwrap();
        }
        drop(store);
        state_reconcile_watermark(
            &config_path,
            &state_path,
            "p",
            &[keys[0].clone()],
            false,
            false,
        )
        .await
        .unwrap();
        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(
            store.get_watermark(&keys[0]).unwrap().unwrap().last_value,
            Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap()
        );
        drop(store);
        state_reconcile_watermark(
            &config_path,
            &state_path,
            "p",
            &[keys[1].clone(), keys[2].clone()],
            false,
            false,
        )
        .await
        .unwrap();
        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(
            store.get_watermark(&keys[1]).unwrap().unwrap().last_value,
            Utc.with_ymd_and_hms(2026, 9, 5, 0, 0, 0).unwrap()
        );
        assert!(
            store.get_watermark(&keys[2]).unwrap().is_none(),
            "NULL MAX must clear a stale cursor"
        );
    }

    fn seed_cache(store: &StateStore, catalog: &str, schema: &str, table: &str) {
        let entry = SchemaCacheEntry {
            columns: vec![StoredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            cached_at: Utc::now(),
        };
        store
            .write_schema_cache_entry(&schema_cache_key(catalog, schema, table), &entry)
            .unwrap();
    }

    #[test]
    fn clear_removes_all_entries() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "c", "staging", "orders");
            seed_cache(&store, "c", "staging", "customers");
            seed_cache(&store, "other", "staging", "events");
            assert_eq!(store.list_schema_cache().unwrap().len(), 3);
        }

        state_clear_schema_cache(&path, false, false).unwrap();

        let store = StateStore::open(&path).unwrap();
        assert!(
            store.list_schema_cache().unwrap().is_empty(),
            "all entries should be deleted"
        );
    }

    #[test]
    fn dry_run_reports_but_does_not_delete() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "c", "staging", "orders");
            seed_cache(&store, "c", "staging", "customers");
        }

        state_clear_schema_cache(&path, true, false).unwrap();

        let store = StateStore::open(&path).unwrap();
        assert_eq!(
            store.list_schema_cache().unwrap().len(),
            2,
            "dry-run must leave entries intact"
        );
    }

    #[test]
    fn missing_state_path_is_not_an_error() {
        // Ephemeral CI runners don't have `.rocky-state.redb` before the
        // first `rocky run`. The clear command should succeed (nothing to
        // flush) rather than erroring — otherwise "flush before CI" is
        // impossible to automate.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        assert!(!path.exists());

        state_clear_schema_cache(&path, false, false).unwrap();
        state_clear_schema_cache(&path, true, false).unwrap();

        // Must not have been created as a side effect.
        assert!(!path.exists(), "clear must not create state.redb");
    }

    #[test]
    fn state_output_json_carries_both_schema_versions() {
        // Acceptance (D): `rocky state show --output json` must carry both the
        // supported and on-disk schema versions as structured fields, so an
        // orchestrator startup hook never has to parse a human error string.
        let out = StateOutput::new(Vec::new(), Some(9));
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(
            json["schema_version_supported"],
            serde_json::json!(rocky_core::state::current_schema_version())
        );
        assert_eq!(json["schema_version_on_disk"], serde_json::json!(9));

        // A missing on-disk version serializes to null (not omitted).
        let out_none = StateOutput::new(Vec::new(), None);
        let json_none = serde_json::to_value(&out_none).unwrap();
        assert!(json_none["schema_version_on_disk"].is_null());
    }

    #[test]
    fn state_show_reports_versions_on_a_fresh_store() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        drop(StateStore::open(&path).unwrap());
        // Both the human and JSON render paths succeed with the new fields.
        state_show(&path, false).unwrap();
        state_show(&path, true).unwrap();
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(rocky_core::state::current_schema_version())
        );
    }

    #[test]
    fn clear_on_empty_cache_returns_zero() {
        // `rocky run` has opened state.redb but nothing has written to
        // SCHEMA_CACHE yet (PR 2 write tap not exercised). Clearing should
        // be a no-op that reports 0.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let _store = StateStore::open(&path).unwrap();
        }
        // File exists but the table is empty.
        assert!(path.exists());
        state_clear_schema_cache(&path, false, false).unwrap();
    }
}

#[cfg(test)]
mod retention_sweep_config_tests {
    //! `rocky state retention sweep` deletes rows and nothing puts them back.
    //! What decides how many it deletes is `[state.retention]` in `rocky.toml`,
    //! so how the command decides whether that file is THERE is a data-loss
    //! surface (#1729).

    use super::*;
    use rocky_core::state::{RunRecord, RunStatus, RunTrigger, StateStore};

    /// One aged run record. `started_at` is what the sweep buckets on.
    fn aged_run(id: &str, days_old: i64) -> RunRecord {
        let at = chrono::Utc::now() - chrono::Duration::days(days_old);
        RunRecord {
            check_gate_failed: false,
            verify_after_failed: false,
            run_id: id.to_string(),
            started_at: at,
            finished_at: at,
            status: RunStatus::Success,
            models_executed: Vec::new(),
            trigger: RunTrigger::Manual,
            config_hash: "c".to_string(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            rocky_branch: None,
            run_scope: Some(rocky_core::state::RunScope::Production),
        }
    }

    /// A store holding `n` runs, every one older than the 365-day default.
    /// `min_runs_kept` defaults to 100, so `n` must exceed it for the default
    /// policy to delete anything at all — otherwise a green test would prove
    /// only that the store was empty.
    fn store_with_aged_runs(state_path: &std::path::Path, n: usize) {
        let store = StateStore::open(state_path).unwrap();
        for i in 0..n {
            store
                .record_run(&aged_run(&format!("r{i}"), 500 + i as i64))
                .unwrap();
        }
        assert_eq!(
            store.list_runs(1000).unwrap().len(),
            n,
            "precondition: the store really holds the aged rows"
        );
    }

    fn run_count(state_path: &std::path::Path) -> usize {
        StateStore::open_read_only(state_path)
            .unwrap()
            .list_runs(1000)
            .unwrap()
            .len()
    }

    const KEEP_EVERYTHING: &str = "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
                                   [state.retention]\nmax_age_days = 3650\n";

    /// The #1729 data-loss case. `rocky.toml` is a symlink whose target is
    /// gone, and it declares `max_age_days = 3650`. `config_path.exists()` is
    /// `metadata(..).is_ok()`, which FOLLOWS the link, so the probe answered
    /// `false`, the 3650 was never read, the 365-day defaults applied, and the
    /// sweep deleted run history — exit 0, no warning, irreversible.
    ///
    /// The sweep must refuse, name the config path AND the link target, and
    /// leave every row where it was.
    #[cfg(unix)]
    #[test]
    fn a_dangling_config_symlink_refuses_the_sweep_and_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("shared-prod.toml");
        std::fs::write(&target, KEEP_EVERYTHING).unwrap();
        let config = tmp.path().join("rocky.toml");
        std::os::unix::fs::symlink(&target, &config).unwrap();
        std::fs::remove_file(&target).unwrap();
        assert!(
            !config.exists(),
            "precondition: the probe this replaces reports the config as absent"
        );

        // A real store with real rows past the default cutoff — otherwise the
        // `!state_path.exists()` early return below would carry the test.
        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        let err = state_retention_sweep(&config, &state_path, false, false)
            .expect_err("a config that is present but unreadable must refuse the sweep");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(&config.display().to_string())
                && rendered.contains(&target.display().to_string()),
            "the refusal must name the config path and the link target it could not \
             resolve, got: {rendered}"
        );

        assert_eq!(
            run_count(&state_path),
            105,
            "a refused sweep must delete nothing; on the defaults it would have dropped 5"
        );
    }

    /// #1817: the #1729 case one directory up. `rocky.toml` itself is fine —
    /// it is the project directory that is a symlink to nowhere. The config
    /// read reports `NotFound`, the LEAF discriminator agreed (a leaf under a
    /// broken parent stats exactly like a path nobody created), the loader
    /// returned `FileNotFound`, and the sweep ran on the defaults: irreversible
    /// deletion, exit 0, from a config that was never absent. The ancestor
    /// walk turns that into the same refusal the leaf case gets.
    #[cfg(unix)]
    #[test]
    fn a_dangling_ancestor_above_the_config_refuses_the_sweep_and_deletes_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let proj = tmp.path().join("proj");
        std::os::unix::fs::symlink(tmp.path().join("gone"), &proj).unwrap();
        let config = proj.join("rocky.toml");
        assert!(
            std::fs::symlink_metadata(&config).is_err(),
            "precondition: the leaf-only probe reports the config absent"
        );

        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        let err = state_retention_sweep(&config, &state_path, false, false)
            .expect_err("a config under a dangling ancestor is not an absent config");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(&proj.display().to_string())
                && rendered.contains("cannot be resolved"),
            "the refusal must name the ancestor the operator has to fix, got: {rendered}"
        );
        assert_eq!(
            run_count(&state_path),
            105,
            "a refused sweep deletes nothing; on the defaults it would have dropped 5"
        );
    }

    /// The control that proves the assertion above is about the refusal and
    /// not about an inert sweep: with the SAME store and the SAME `rocky.toml`
    /// resolving, the sweep runs and the defaults delete.
    #[cfg(unix)]
    #[test]
    fn the_same_store_loses_rows_when_the_config_reads_as_absent() {
        let tmp = tempfile::tempdir().unwrap();
        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        // No rocky.toml at all — the init flow, and the one case that sweeps
        // on the defaults. `min_runs_kept = 100` keeps 100 of the 105.
        let absent = tmp.path().join("rocky.toml");
        state_retention_sweep(&absent, &state_path, false, false)
            .expect("no rocky.toml is the documented init flow and must still sweep");
        assert_eq!(
            run_count(&state_path),
            100,
            "the defaults delete past 365 days, keeping min_runs_kept = 100"
        );
    }

    /// The other honest-failure control: a `rocky.toml` that is plainly there
    /// and readable is unchanged — its `max_age_days = 3650` is read and every
    /// aged row survives.
    #[test]
    fn a_real_config_is_read_and_its_policy_applies() {
        let tmp = tempfile::tempdir().unwrap();
        let config = tmp.path().join("rocky.toml");
        std::fs::write(&config, KEEP_EVERYTHING).unwrap();

        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        state_retention_sweep(&config, &state_path, false, false)
            .expect("a readable config must sweep as before");
        assert_eq!(
            run_count(&state_path),
            105,
            "max_age_days = 3650 must keep every row the defaults would have dropped"
        );
    }

    /// A `rocky.toml` symlink that RESOLVES is an ordinary config: followed,
    /// read, and its policy applied. The fix discriminates on presence, never
    /// on whether the path happens to be a link.
    #[cfg(unix)]
    #[test]
    fn a_resolvable_config_symlink_still_loads() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("shared-prod.toml");
        std::fs::write(&target, KEEP_EVERYTHING).unwrap();
        let config = tmp.path().join("rocky.toml");
        std::os::unix::fs::symlink(&target, &config).unwrap();

        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        state_retention_sweep(&config, &state_path, false, false)
            .expect("a symlink that resolves must sweep as before");
        assert_eq!(run_count(&state_path), 105, "the linked policy must apply");
    }

    /// A config that is present and MALFORMED already refused before this
    /// change (`exists()` was true, the loader errored). Pinned so the new
    /// match arm cannot quietly turn it into "absent, sweep on defaults".
    #[test]
    fn a_malformed_config_still_refuses() {
        let tmp = tempfile::tempdir().unwrap();
        let config = tmp.path().join("rocky.toml");
        std::fs::write(&config, "this is not = = valid toml\n").unwrap();

        let state_path = tmp.path().join("state.redb");
        store_with_aged_runs(&state_path, 105);

        state_retention_sweep(&config, &state_path, false, false)
            .expect_err("a config that does not parse must refuse, as it always did");
        assert_eq!(run_count(&state_path), 105, "and delete nothing");
    }
}

#[cfg(test)]
mod schedule_hold_tests {
    use super::*;

    fn project(tmp: &tempfile::TempDir) -> std::path::PathBuf {
        let config = tmp.path().join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.p.schedule]\ncron = \"* * * * *\"\ntimezone = \"UTC\"\n",
        )
        .unwrap();
        config
    }

    /// The human round-trip: pause sets the durable flag (changed=true), a
    /// repeat is idempotent (changed=false), resume clears it — and the flag
    /// is exactly what the reconciler consults (pinned separately in
    /// rocky-core's demand tests).
    #[test]
    fn pause_and_resume_round_trip_through_the_store() {
        let tmp = tempfile::tempdir().unwrap();
        let config = project(&tmp);
        let state_path = tmp.path().join("state.redb");

        state_schedule_hold(&config, &state_path, "p", true, true).unwrap();
        let store = StateStore::open_read_only(&state_path).unwrap();
        assert!(store.get_schedule_state("p").unwrap().unwrap().paused);
        drop(store);

        // Idempotent repeat.
        state_schedule_hold(&config, &state_path, "p", true, true).unwrap();

        state_schedule_hold(&config, &state_path, "p", false, true).unwrap();
        let store = StateStore::open_read_only(&state_path).unwrap();
        assert!(!store.get_schedule_state("p").unwrap().unwrap().paused);
    }

    /// A pipeline with no [schedule] block is refused — the hold must attach
    /// to something the reconciler consults, never a stray cursor.
    #[test]
    fn a_pipeline_without_a_schedule_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let config = tmp.path().join("rocky.toml");
        std::fs::write(
            &config,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let state_path = tmp.path().join("state.redb");
        let err = state_schedule_hold(&config, &state_path, "p", true, true)
            .expect_err("no [schedule] must refuse");
        assert!(
            format!("{err:#}").contains("no [schedule] block"),
            "{err:#}"
        );
        assert!(!state_path.exists(), "a refusal must not create the store");
    }
}
