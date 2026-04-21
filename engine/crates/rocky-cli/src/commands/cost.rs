//! `rocky cost <run_id|latest>` — historical per-run cost attribution.
//!
//! Trust-system Arc 2 wave 2. The first wave (engine v1.11.0) shipped
//! per-run cost on [`crate::output::RunOutput::cost_summary`] and the
//! declarative `[budget]` block; this command adds the *historical*
//! surface on top of the embedded state store's
//! [`rocky_core::state::RunRecord`].
//!
//! The command re-derives per-model cost via
//! [`rocky_core::cost::compute_observed_cost_usd`] — the same formula
//! `rocky run` uses for the live summary — so the two surfaces stay
//! consistent without sharing storage. The historical path has one
//! genuine advantage: [`rocky_core::state::ModelExecution::bytes_scanned`]
//! is persisted, so BigQuery cost can be computed here even though the
//! live `rocky run` path reports `None` for BQ today.
//!
//! Adapter type is resolved by loading `rocky.toml` at the configured
//! path. If the config can't be read the command still succeeds and
//! emits `adapter_type: None` / `cost_usd: None`; durations and byte
//! counts are still useful on their own.
//!
//! Re-execution with pinned inputs is an Arc-1 follow-up; this command
//! is inspection-only.
//!
//! # Out of scope (this PR)
//!
//! - Surfacing cost on `rocky replay --output json`.
//! - Per-model `[budget]` blocks (first-wave per-pipeline budgets already
//!   shipped).
//! - Adapter-reported `bytes_scanned` plumbing for the live run path.
//! - PR cost-projection GitHub Action.

use std::path::Path;

use anyhow::{Context, Result};
use tracing::warn;

use rocky_core::config::load_rocky_config;
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger, StateStore};

use crate::output::{CostOutput, PerModelCostHistorical};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn status_str(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
    }
}

fn trigger_str(trigger: &RunTrigger) -> &'static str {
    match trigger {
        RunTrigger::Manual => "manual",
        RunTrigger::Sensor => "sensor",
        RunTrigger::Schedule => "schedule",
        RunTrigger::Ci => "ci",
    }
}

/// Resolve `target` (either a literal `run_id` or the alias `"latest"`)
/// against the state store. Mirrors [`crate::commands::run_replay`]'s
/// convention so the two commands accept the same targets.
fn resolve(store: &StateStore, target: &str) -> Result<RunRecord> {
    if target == "latest" {
        let runs = store.list_runs(1)?;
        return runs
            .into_iter()
            .next()
            .context("no runs recorded yet — nothing to report cost for");
    }
    store
        .get_run(target)?
        .with_context(|| format!("no run with id '{target}' in the state store"))
}

/// Resolve the billed-warehouse type for a run's config.
///
/// Picks `adapter.default` when present, otherwise the first-declared
/// adapter. Returns `None` for adapters that aren't billed warehouses
/// (e.g. `fivetran`, `airbyte`) — upstream `compute_observed_cost_usd`
/// handles the cost-is-None path gracefully.
///
/// Takes the adapter-type string map directly (rather than the full
/// [`rocky_core::config::AdapterConfig`] map) so tests can drive it
/// without building the large credential-heavy struct.
fn resolve_warehouse_type_from_types(
    adapter_types: &[(String, String)],
) -> Option<(String, WarehouseType)> {
    // Prefer `default` for determinism; fall back to first-declared.
    let preferred = adapter_types
        .iter()
        .find(|(k, _)| k == "default")
        .or_else(|| adapter_types.first())?;
    let wh = WarehouseType::from_adapter_type(&preferred.1)?;
    Some((preferred.1.clone(), wh))
}

/// Thin wrapper that extracts `(name, adapter_type)` pairs from a
/// loaded [`rocky_core::config::RockyConfig`] and delegates to
/// [`resolve_warehouse_type_from_types`].
fn resolve_warehouse_type(
    adapters: &indexmap::IndexMap<String, rocky_core::config::AdapterConfig>,
) -> Option<(String, WarehouseType)> {
    let pairs: Vec<(String, String)> = adapters
        .iter()
        .map(|(k, v)| (k.clone(), v.adapter_type.clone()))
        .collect();
    resolve_warehouse_type_from_types(&pairs)
}

/// Build the per-model + totals rollup from a recorded run. Pure: takes
/// stored executions + cost inputs and produces the output struct. The
/// CLI wrapper in [`run_cost`] feeds this with state-loaded + config-loaded
/// values, while unit tests can drive it with hand-crafted inputs.
fn build_output(
    record: &RunRecord,
    adapter_info: Option<&(String, WarehouseType, f64, f64)>,
    model_filter: Option<&str>,
) -> CostOutput {
    let executions: Vec<&ModelExecution> = record
        .models_executed
        .iter()
        .filter(|m| match model_filter {
            Some(name) => m.model_name == name,
            None => true,
        })
        .collect();

    let mut per_model = Vec::with_capacity(executions.len());
    let mut total_cost = 0.0;
    let mut any_cost = false;
    let mut total_duration_ms: u64 = 0;
    let mut total_bytes_scanned: u64 = 0;
    let mut any_bytes_scanned = false;
    let mut total_bytes_written: u64 = 0;
    let mut any_bytes_written = false;

    for exec in &executions {
        let cost = adapter_info.and_then(|(_, wh, dbu_per_hour, cost_per_dbu)| {
            compute_observed_cost_usd(
                *wh,
                exec.bytes_scanned,
                exec.duration_ms,
                *dbu_per_hour,
                *cost_per_dbu,
            )
        });
        if let Some(c) = cost {
            total_cost += c;
            any_cost = true;
        }
        total_duration_ms = total_duration_ms.saturating_add(exec.duration_ms);
        if let Some(b) = exec.bytes_scanned {
            total_bytes_scanned = total_bytes_scanned.saturating_add(b);
            any_bytes_scanned = true;
        }
        if let Some(b) = exec.bytes_written {
            total_bytes_written = total_bytes_written.saturating_add(b);
            any_bytes_written = true;
        }
        per_model.push(PerModelCostHistorical {
            model_name: exec.model_name.clone(),
            status: exec.status.clone(),
            duration_ms: exec.duration_ms,
            rows_affected: exec.rows_affected,
            bytes_scanned: exec.bytes_scanned,
            bytes_written: exec.bytes_written,
            cost_usd: cost,
        });
    }

    let adapter_type = adapter_info.map(|(name, _, _, _)| name.clone());
    let total_cost_usd = if any_cost { Some(total_cost) } else { None };
    let total_bytes_scanned_out = if any_bytes_scanned {
        Some(total_bytes_scanned)
    } else {
        None
    };
    let total_bytes_written_out = if any_bytes_written {
        Some(total_bytes_written)
    } else {
        None
    };

    let duration_ms = (record.finished_at - record.started_at)
        .num_milliseconds()
        .max(0) as u64;

    CostOutput {
        version: VERSION.to_string(),
        command: "cost".to_string(),
        run_id: record.run_id.clone(),
        status: status_str(&record.status).to_string(),
        trigger: trigger_str(&record.trigger).to_string(),
        started_at: record.started_at.to_rfc3339(),
        finished_at: record.finished_at.to_rfc3339(),
        duration_ms,
        adapter_type,
        total_cost_usd,
        total_duration_ms,
        total_bytes_scanned: total_bytes_scanned_out,
        total_bytes_written: total_bytes_written_out,
        per_model,
    }
}

fn print_table(output: &CostOutput) {
    println!("run: {}", output.run_id);
    println!("status: {}", output.status);
    println!("trigger: {}", output.trigger);
    println!("started_at: {}", output.started_at);
    println!("finished_at: {}", output.finished_at);
    println!("duration_ms: {}", output.duration_ms);
    match &output.adapter_type {
        Some(t) => println!("adapter_type: {t}"),
        None => {
            println!("adapter_type: (unavailable — config not loaded or not a billed warehouse)")
        }
    }
    println!();
    println!("models ({}):", output.per_model.len());
    // Align headers roughly; keep it simple (no external table crate).
    println!(
        "  {:<32}  {:>10}  {:>12}  {:>14}  {:>14}  {:>12}  status",
        "model", "duration", "rows", "bytes_scan", "bytes_write", "cost_usd"
    );
    for m in &output.per_model {
        let rows = m
            .rows_affected
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let bs = m
            .bytes_scanned
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let bw = m
            .bytes_written
            .map(|v| v.to_string())
            .unwrap_or_else(|| "-".to_string());
        let cost = m
            .cost_usd
            .map(|v| format!("${v:.6}"))
            .unwrap_or_else(|| "-".to_string());
        println!(
            "  {:<32}  {:>10}  {:>12}  {:>14}  {:>14}  {:>12}  {}",
            m.model_name, m.duration_ms, rows, bs, bw, cost, m.status
        );
    }
    println!();
    println!("total_duration_ms: {}", output.total_duration_ms);
    if let Some(b) = output.total_bytes_scanned {
        println!("total_bytes_scanned: {b}");
    }
    if let Some(b) = output.total_bytes_written {
        println!("total_bytes_written: {b}");
    }
    match output.total_cost_usd {
        Some(c) => println!("total_cost_usd: ${c:.6}"),
        None => println!("total_cost_usd: -"),
    }
}

/// Execute `rocky cost <run_id|latest>`.
///
/// Loads the run from the state store, loads `rocky.toml` to resolve the
/// billed-warehouse type (degrading gracefully when the config can't be
/// read), and emits the rollup as JSON or a human table.
pub fn run_cost(
    state_path: &Path,
    config_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;

    // Load config best-effort: the record itself is enough to emit
    // durations/bytes; the only thing we lose without a config is the
    // cost formula's parameters.
    let adapter_info: Option<(String, WarehouseType, f64, f64)> =
        match load_rocky_config(config_path) {
            Ok(cfg) => {
                let dbu_per_hour = warehouse_size_to_dbu_per_hour(&cfg.cost.warehouse_size);
                let cost_per_dbu = cfg.cost.compute_cost_per_dbu;
                resolve_warehouse_type(&cfg.adapters)
                    .map(|(name, wh)| (name, wh, dbu_per_hour, cost_per_dbu))
            }
            Err(err) => {
                warn!(
                    "failed to load config at {} — cost figures will be omitted: {err}",
                    config_path.display()
                );
                None
            }
        };

    let output = build_output(&record, adapter_info.as_ref(), model_filter);

    if model_filter.is_some() && output.per_model.is_empty() {
        anyhow::bail!(
            "run '{}' did not execute model '{}'",
            record.run_id,
            model_filter.unwrap_or("")
        );
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_table(&output);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{TimeZone, Utc};
    use tempfile::TempDir;

    fn sample_exec(
        name: &str,
        status: &str,
        duration_ms: u64,
        bytes_scanned: Option<u64>,
        bytes_written: Option<u64>,
    ) -> ModelExecution {
        let now = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        ModelExecution {
            model_name: name.to_string(),
            started_at: now,
            finished_at: now + chrono::Duration::milliseconds(duration_ms as i64),
            duration_ms,
            rows_affected: Some(100),
            status: status.to_string(),
            sql_hash: format!("hash_{name}"),
            bytes_scanned,
            bytes_written,
        }
    }

    fn sample_run(run_id: &str, models: Vec<ModelExecution>) -> RunRecord {
        let started = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        let finished = started + chrono::Duration::milliseconds(10_000);
        RunRecord {
            run_id: run_id.to_string(),
            started_at: started,
            finished_at: finished,
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
        }
    }

    #[test]
    fn rollup_sums_per_model_cost_and_duration_databricks() {
        // 60s on Medium (24 DBU/hr) @ $0.40/DBU ≈ 60/3600 * 24 * 0.40 = $0.16.
        let record = sample_run(
            "run-1",
            vec![
                sample_exec("orders", "success", 30_000, Some(1_000_000), Some(500_000)),
                sample_exec("customers", "success", 30_000, Some(2_000_000), None),
            ],
        );

        let dbu_per_hour = warehouse_size_to_dbu_per_hour("Medium");
        let cost_per_dbu = 0.40;
        let adapter = (
            "databricks".to_string(),
            WarehouseType::Databricks,
            dbu_per_hour,
            cost_per_dbu,
        );

        let out = build_output(&record, Some(&adapter), None);

        assert_eq!(out.command, "cost");
        assert_eq!(out.run_id, "run-1");
        assert_eq!(out.adapter_type.as_deref(), Some("databricks"));
        assert_eq!(out.per_model.len(), 2);
        assert_eq!(out.total_duration_ms, 60_000);
        assert_eq!(out.total_bytes_scanned, Some(3_000_000));
        assert_eq!(out.total_bytes_written, Some(500_000));

        // Total cost is the sum of per-model costs (strict).
        let sum_per_model: f64 = out
            .per_model
            .iter()
            .map(|m| m.cost_usd.unwrap_or(0.0))
            .sum();
        let total = out.total_cost_usd.expect("total_cost_usd should be Some");
        assert!((total - sum_per_model).abs() < 1e-12);

        // And it matches the closed-form expectation.
        let expected = (60_000f64 / 3_600_000.0) * dbu_per_hour * cost_per_dbu;
        assert!((total - expected).abs() < 1e-9);
    }

    #[test]
    fn rollup_without_adapter_still_reports_bytes_and_duration() {
        let record = sample_run(
            "run-2",
            vec![sample_exec(
                "orders",
                "success",
                5_000,
                Some(1_024),
                Some(2_048),
            )],
        );

        let out = build_output(&record, None, None);

        assert_eq!(out.adapter_type, None);
        assert_eq!(out.total_duration_ms, 5_000);
        assert_eq!(out.total_bytes_scanned, Some(1_024));
        assert_eq!(out.total_bytes_written, Some(2_048));
        assert_eq!(out.total_cost_usd, None);
        assert_eq!(out.per_model[0].cost_usd, None);
        // Per-model bytes/duration/status are preserved.
        assert_eq!(out.per_model[0].duration_ms, 5_000);
        assert_eq!(out.per_model[0].bytes_scanned, Some(1_024));
        assert_eq!(out.per_model[0].status, "success");
    }

    #[test]
    fn rollup_bigquery_uses_bytes_scanned() {
        // BigQuery: cost = bytes_scanned / 1e12 * $6.25. State store
        // persists bytes_scanned, so the historical command can compute
        // real BQ cost even though `rocky run`'s live summary can't yet.
        let record = sample_run(
            "run-bq",
            vec![sample_exec(
                "events",
                "success",
                1_000,
                Some(1_000_000_000_000), // 1 TB
                None,
            )],
        );

        let adapter = ("bigquery".to_string(), WarehouseType::BigQuery, 0.0, 0.0);
        let out = build_output(&record, Some(&adapter), None);

        let cost = out.per_model[0].cost_usd.expect("BQ cost should be Some");
        assert!((cost - 6.25).abs() < 1e-9);
    }

    #[test]
    fn rollup_applies_model_filter() {
        let record = sample_run(
            "run-3",
            vec![
                sample_exec("orders", "success", 1_000, None, None),
                sample_exec("customers", "success", 2_000, None, None),
            ],
        );

        let out = build_output(&record, None, Some("orders"));
        assert_eq!(out.per_model.len(), 1);
        assert_eq!(out.per_model[0].model_name, "orders");
        assert_eq!(out.total_duration_ms, 1_000);
    }

    #[test]
    fn resolve_by_run_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        store
            .record_run(&sample_run(
                "run-1",
                vec![sample_exec("m", "success", 1, None, None)],
            ))
            .unwrap();

        let resolved = resolve(&store, "run-1").unwrap();
        assert_eq!(resolved.run_id, "run-1");
    }

    #[test]
    fn resolve_latest_picks_most_recent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        store
            .record_run(&sample_run(
                "old",
                vec![sample_exec("m", "success", 1, None, None)],
            ))
            .unwrap();
        // Brief gap so the second run's started_at actually sorts after.
        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .record_run(&sample_run(
                "new",
                vec![sample_exec("m", "success", 1, None, None)],
            ))
            .unwrap();

        let resolved = resolve(&store, "latest").unwrap();
        assert_eq!(resolved.run_id, "new");
    }

    #[test]
    fn resolve_missing_run_surfaces_id_in_error() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let err = resolve(&store, "does-not-exist").unwrap_err();
        assert!(err.to_string().contains("does-not-exist"));
    }

    #[test]
    fn resolve_latest_empty_store_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let err = resolve(&store, "latest").unwrap_err();
        assert!(err.to_string().to_lowercase().contains("no runs"));
    }

    #[test]
    fn resolve_warehouse_type_prefers_default() {
        // First-declared is snowflake, but `default` is databricks —
        // `default` wins for determinism.
        let pairs = vec![
            ("prod".to_string(), "snowflake".to_string()),
            ("default".to_string(), "databricks".to_string()),
        ];
        let (name, wh) = resolve_warehouse_type_from_types(&pairs).unwrap();
        assert_eq!(name, "databricks");
        assert_eq!(wh, WarehouseType::Databricks);
    }

    #[test]
    fn resolve_warehouse_type_falls_back_to_first() {
        let pairs = vec![("prod".to_string(), "duckdb".to_string())];
        let (name, wh) = resolve_warehouse_type_from_types(&pairs).unwrap();
        assert_eq!(name, "duckdb");
        assert_eq!(wh, WarehouseType::DuckDb);
    }

    #[test]
    fn resolve_warehouse_type_none_for_unbilled_source() {
        let pairs = vec![("default".to_string(), "fivetran".to_string())];
        assert!(resolve_warehouse_type_from_types(&pairs).is_none());
    }

    #[test]
    fn resolve_warehouse_type_none_for_empty_adapters() {
        let pairs: Vec<(String, String)> = Vec::new();
        assert!(resolve_warehouse_type_from_types(&pairs).is_none());
    }
}
