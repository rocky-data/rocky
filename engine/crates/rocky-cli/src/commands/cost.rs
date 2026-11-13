//! `rocky cost <run_id|latest>` — historical per-run cost attribution.
//!
//! The earlier per-run cost surface (engine v1.11.0) shipped
//! [`crate::output::RunOutput::cost_summary`] and the declarative
//! `[budget]` block; this command adds the *historical* surface on top
//! of the embedded state store's [`rocky_core::state::RunRecord`].
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
//! path. If the config is absent the command still succeeds and emits
//! `adapter_type: None` / `cost_usd: None`; durations and byte counts
//! are still useful on their own. A config that is there but does not
//! load is an error: a wrong price is worse than no price.
//!
//! Re-execution with pinned inputs is a follow-up; this command is
//! inspection-only.
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
use chrono::{DateTime, Utc};
use rocky_core::config::load_optional_project_config;
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::cost_split;
use rocky_core::state::{
    ModelExecution, RunRecord, RunStatus, RunTrigger, StateStore, StoredBytesBasis, UnrecordedScope,
};

use crate::output::{
    AllocatedUpstreamCost, CostAmount, CostGroup, CostNotMeasured, CostOutput, CostUnit,
    MoneyFigure, MoneyPart, MoneyStatus, PerModelCostHistorical, ProductBudgetBreachEvent,
    ProductBudgetCheck, ProductBudgetLimitType, ProductBudgetStatus, ProductBudgetVerdict,
    ProductCostMonth, ProductMonthCost, ProductMonthCostOutput, RunProductBudget,
    SharedNotChargedCost,
};
use rocky_core::config::ProductBudgetConfig;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Sentinel bucket key for executions with no recorded tenant under
/// `--by tenant`. A literal angle-bracketed string so it can never
/// collide with a real tenant name (schema-pattern components are
/// validated identifiers).
const UNATTRIBUTED: &str = "<unattributed>";

/// Dimension to roll per-model cost up by, selected via `--by`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostGroupBy {
    /// One group per tenant (the discover-time `{tenant}` component);
    /// models with no recorded tenant collect in an `"<unattributed>"`
    /// bucket.
    Tenant,
    /// One group per model name.
    Model,
}

impl CostGroupBy {
    /// Parse the `--by` flag value. Accepts `tenant` / `model`
    /// case-insensitively.
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "tenant" => Ok(Self::Tenant),
            "model" => Ok(Self::Model),
            other => {
                anyhow::bail!("unknown --by dimension '{other}' — expected 'tenant' or 'model'")
            }
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Tenant => "tenant",
            Self::Model => "model",
        }
    }
}

fn status_str(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
        RunStatus::SkippedIdempotent => "skipped_idempotent",
        RunStatus::SkippedInFlight => "skipped_in_flight",
    }
}

fn trigger_str(trigger: &RunTrigger) -> &'static str {
    match trigger {
        RunTrigger::Manual => "manual",
        RunTrigger::Sensor => "sensor",
        RunTrigger::Schedule => "schedule",
        RunTrigger::Ci => "ci",
        RunTrigger::Webhook => "webhook",
    }
}

/// Resolve `target` (either a literal `run_id` or the alias `"latest"`)
/// against the state store. Mirrors [`crate::commands::run_replay`]'s
/// convention so the two commands accept the same targets.
///
/// `latest` is the newest production run (#2201): a shadow or branch run's
/// cost is not production spend. A run recorded before runs carried a scope
/// is eligible, as it was before; [`CostOutput::run_scope`] reports it as
/// `unrecorded`. An explicit run id is honoured whatever its scope.
fn resolve(store: &StateStore, target: &str) -> Result<RunRecord> {
    if target == "latest" {
        let runs =
            store.list_runs_matching(1, |r| r.counts_as_production(UnrecordedScope::Count))?;
        return runs.into_iter().next().context(
            "no production runs recorded yet — nothing to report cost for. \
             Pass a run id to report a shadow or branch run",
        );
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
    group_by: Option<CostGroupBy>,
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
            tenant: exec.tenant.clone(),
            status: exec.status.clone(),
            duration_ms: exec.duration_ms,
            rows_affected: exec.rows_affected,
            bytes_scanned: exec.bytes_scanned,
            bytes_written: exec.bytes_written,
            cost_usd: cost,
            stored_bytes: exec.stored_bytes.map(|s| s.bytes),
            stored_bytes_basis: exec.stored_bytes.map(|s| s.basis),
            upstream_reads: exec.upstream_reads.clone(),
        });
    }

    let groups = group_by.map(|by| build_groups(&per_model, by));

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
        groups,
        run_scope: crate::output::RunScopeKind::of(record),
    }
}

/// Roll the per-model rows up by the requested dimension into a stable,
/// deterministically-ordered list of [`CostGroup`]s.
///
/// Determinism: groups are sorted by `key`, with the `"<unattributed>"`
/// tenant bucket sorting naturally (its angle brackets sort before
/// alphanumerics). This keeps `--by` output diff-stable across runs and
/// across the (arbitrary) execution order in the stored record.
fn build_groups(per_model: &[PerModelCostHistorical], by: CostGroupBy) -> Vec<CostGroup> {
    use std::collections::BTreeMap;

    // Accumulator per key. BTreeMap gives sorted, deterministic output.
    struct Acc {
        model_count: u64,
        total_cost: f64,
        any_cost: bool,
        total_duration_ms: u64,
        total_bytes_scanned: u64,
        any_bytes_scanned: bool,
        total_bytes_written: u64,
        any_bytes_written: bool,
    }

    let mut acc: BTreeMap<String, Acc> = BTreeMap::new();
    for m in per_model {
        let key = match by {
            CostGroupBy::Tenant => m.tenant.clone().unwrap_or_else(|| UNATTRIBUTED.to_string()),
            CostGroupBy::Model => m.model_name.clone(),
        };
        let entry = acc.entry(key).or_insert(Acc {
            model_count: 0,
            total_cost: 0.0,
            any_cost: false,
            total_duration_ms: 0,
            total_bytes_scanned: 0,
            any_bytes_scanned: false,
            total_bytes_written: 0,
            any_bytes_written: false,
        });
        entry.model_count += 1;
        if let Some(c) = m.cost_usd {
            entry.total_cost += c;
            entry.any_cost = true;
        }
        entry.total_duration_ms = entry.total_duration_ms.saturating_add(m.duration_ms);
        if let Some(b) = m.bytes_scanned {
            entry.total_bytes_scanned = entry.total_bytes_scanned.saturating_add(b);
            entry.any_bytes_scanned = true;
        }
        if let Some(b) = m.bytes_written {
            entry.total_bytes_written = entry.total_bytes_written.saturating_add(b);
            entry.any_bytes_written = true;
        }
    }

    acc.into_iter()
        .map(|(key, a)| CostGroup {
            dimension: by.as_str().to_string(),
            key,
            model_count: a.model_count,
            total_cost_usd: a.any_cost.then_some(a.total_cost),
            total_duration_ms: a.total_duration_ms,
            total_bytes_scanned: a.any_bytes_scanned.then_some(a.total_bytes_scanned),
            total_bytes_written: a.any_bytes_written.then_some(a.total_bytes_written),
        })
        .collect()
}

/// One `stored_bytes` table cell: the bytes, marked `(est)` when the
/// figure is an estimate, or `-` when unknown.
fn stored_bytes_cell(bytes: Option<u64>, basis: Option<StoredBytesBasis>) -> String {
    match (bytes, basis) {
        (Some(b), Some(StoredBytesBasis::DataFiles)) => b.to_string(),
        (Some(b), Some(StoredBytesBasis::AllocatedBlocks)) => format!("{b} (est)"),
        (Some(_), None) | (None, _) => "-".to_string(),
    }
}

fn print_table(output: &CostOutput) {
    println!("run: {}", output.run_id);
    println!("status: {}", output.status);
    println!("trigger: {}", output.trigger);
    println!("started_at: {}", output.started_at);
    println!("finished_at: {}", output.finished_at);
    println!("duration_ms: {}", output.duration_ms);
    println!("run_scope: {}", output.run_scope.as_str());
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
        "  {:<32}  {:>10}  {:>12}  {:>14}  {:>14}  {:>16}  {:>12}  status",
        "model", "duration", "rows", "bytes_scan", "bytes_write", "stored_bytes", "cost_usd"
    );
    for m in &output.per_model {
        let stored = stored_bytes_cell(m.stored_bytes, m.stored_bytes_basis);
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
            "  {:<32}  {:>10}  {:>12}  {:>14}  {:>14}  {:>16}  {:>12}  {}",
            m.model_name, m.duration_ms, rows, bs, bw, stored, cost, m.status
        );
    }
    if output
        .per_model
        .iter()
        .any(|m| m.stored_bytes_basis == Some(StoredBytesBasis::AllocatedBlocks))
    {
        println!(
            "  (est) = estimate: storage blocks x block size; overstates a small table, \
             because a partly filled block counts in full"
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

    if let Some(groups) = &output.groups {
        println!();
        let dimension = groups
            .first()
            .map(|g| g.dimension.as_str())
            .unwrap_or("group");
        println!("by {dimension} ({}):", groups.len());
        println!(
            "  {:<24}  {:>8}  {:>12}  {:>14}  {:>14}  {:>12}",
            dimension, "models", "duration", "bytes_scan", "bytes_write", "cost_usd"
        );
        for g in groups {
            let bs = g
                .total_bytes_scanned
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let bw = g
                .total_bytes_written
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            let cost = g
                .total_cost_usd
                .map(|v| format!("${v:.6}"))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "  {:<24}  {:>8}  {:>12}  {:>14}  {:>14}  {:>12}",
                g.key, g.model_count, g.total_duration_ms, bs, bw, cost
            );
        }
    }
}

/// The billed-warehouse pricing parameters from `rocky.toml`:
/// `(adapter name, warehouse type, DBU/hour, $/DBU)`.
///
/// ```text
///   no rocky.toml            -> Ok(None)   durations and bytes only
///   rocky.toml does not load -> Err        naming the file (#1702)
///   loads, non-billed source -> Ok(None)   Fivetran / Airbyte, unchanged
/// ```
///
/// This used to be a `match` that logged a warning and returned `None` on ANY
/// load error, so a broken config produced the same rollup as a config-less
/// project and as a genuinely non-billed one. Three different situations, one
/// indistinguishable answer.
///
/// The loader is the credential-tolerant one: `rocky cost` reads the state
/// store and prices from the run record, and opens no warehouse connection.
/// Adapters are read for their `type` only.
pub(crate) fn adapter_pricing(
    config_path: &Path,
) -> Result<Option<(String, WarehouseType, f64, f64)>> {
    let Some(cfg) = load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?
    else {
        return Ok(None);
    };
    let dbu_per_hour = warehouse_size_to_dbu_per_hour(&cfg.cost.warehouse_size);
    let cost_per_dbu = cfg.cost.compute_cost_per_dbu_or_default();
    Ok(resolve_warehouse_type(&cfg.adapters)
        .map(|(name, wh)| (name, wh, dbu_per_hour, cost_per_dbu)))
}

/// Execute `rocky cost <run_id|latest>`.
///
/// Loads the run from the state store, loads `rocky.toml` to resolve the
/// billed-warehouse type, and emits the rollup as JSON or a human table.
///
/// ```text
///   no rocky.toml            -> durations and bytes only, no dollar figures
///   rocky.toml does not load -> Err, naming the file (#1702)
///   unset ${VAR} in adapter
///     connection fields      -> loads; the cost figures render (#1536)
/// ```
///
/// It used to swallow every load error into "no dollar figures", so a broken
/// config and a config-less project produced the same rollup. `rocky cost`
/// reads the state store and prices from the run record — it opens no
/// warehouse connection — so the loader is the credential-tolerant one.
/// The typed payload behind `rocky cost`: one recorded run's per-model cost
/// attribution.
///
/// This is the one producer of [`CostOutput`]. The CLI renders it as JSON
/// or as a table; a server route can serve it unchanged. The store is
/// opened read-only; the config is read for adapter pricing and its absence
/// degrades to `adapter_type: None`, as the module doc says. A
/// `model_filter` that names a model the run did not execute is an error,
/// not an empty rollup.
pub fn compute_cost(
    state_path: &Path,
    config_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    group_by: Option<CostGroupBy>,
) -> Result<CostOutput> {
    let store = StateStore::open_read_only_or_empty(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;

    let adapter_info = adapter_pricing(config_path)?;

    let output = build_output(&record, adapter_info.as_ref(), model_filter, group_by);

    if let Some(name) = model_filter
        && output.per_model.is_empty()
    {
        anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
    }

    Ok(output)
}

/// Execute `rocky cost`: one run (`target`), or one month split by product
/// (`month` with `by = "product"`).
pub fn run_cost(
    state_path: &Path,
    config_path: &Path,
    target: Option<&str>,
    model_filter: Option<&str>,
    by: Option<&str>,
    month: Option<&str>,
    json: bool,
) -> Result<()> {
    if let Some(month) = month {
        anyhow::ensure!(
            target.is_none() && model_filter.is_none(),
            "--month reports a whole month; it cannot be combined with a run id or --model"
        );
        anyhow::ensure!(
            by.is_some_and(|by| by.eq_ignore_ascii_case("product")),
            "--month needs --by product"
        );
        return run_month_cost(state_path, config_path, month, json);
    }
    if by.is_some_and(|by| by.eq_ignore_ascii_case("product")) {
        anyhow::bail!("--by product needs --month YYYY-MM");
    }
    let target = target.context(
        "pass a run id or `latest`, or report a month with `--month YYYY-MM --by product`",
    )?;
    let group_by = by.map(CostGroupBy::parse).transpose()?;
    let output = compute_cost(state_path, config_path, target, model_filter, group_by)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_table(&output);
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky cost --month YYYY-MM --by product`
// ---------------------------------------------------------------------------

/// Parse `--month` as `YYYY-MM`.
pub(crate) fn parse_month(raw: &str) -> Result<(i32, u32)> {
    let bad = || anyhow::anyhow!("--month '{raw}' is not a month — expected YYYY-MM, e.g. 2026-10");
    let (year, month) = raw.split_once('-').ok_or_else(bad)?;
    if year.len() != 4 || month.len() != 2 {
        return Err(bad());
    }
    let year: i32 = year.parse().map_err(|_| bad())?;
    let month: u32 = month.parse().map_err(|_| bad())?;
    if !(1..=12).contains(&month) {
        return Err(bad());
    }
    Ok((year, month))
}

/// Whether run history reaches back far enough to cover `window_start`, as
/// far as the retention policy can tell.
///
/// Retention keeps rows younger than `max_age_days`, plus the newest
/// `min_runs_kept`. A month that began longer ago than that may have lost
/// runs, and the few rows `min_runs_kept` leaves behind look like a quiet
/// month. The scan starts one day before the month (a run can start before
/// the month and have executions inside it), so that day counts.
fn history_is_complete(
    retention: &rocky_core::retention::StateRetentionConfig,
    window_start: DateTime<Utc>,
    now: DateTime<Utc>,
) -> bool {
    use rocky_core::retention::StateRetentionDomain;
    if !retention.applies_to_domain(StateRetentionDomain::History) {
        return true;
    }
    let oldest_kept = now - chrono::Duration::days(i64::from(retention.max_age_days));
    window_start - chrono::Duration::days(1) >= oldest_kept
}

/// Model name to the models that declare it as a dependency, from the
/// compiled model graph of every transformation pipeline. A graph that does
/// not load adds a warning and leaves its models without declared children:
/// the report then cannot tell a reader that recorded nothing from a model
/// that is not a reader.
fn declared_children(
    cfg: Option<&rocky_core::config::RockyConfig>,
    config_path: &Path,
    warnings: &mut Vec<String>,
) -> std::collections::BTreeMap<String, std::collections::BTreeSet<String>> {
    let mut children: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        std::collections::BTreeMap::new();
    let Some(cfg) = cfg else {
        return children;
    };
    for (name, pipeline) in &cfg.pipelines {
        let Some(tx) = pipeline.as_transformation() else {
            continue;
        };
        let dir = match crate::models_loader::locate_models_dir(&tx.models, config_path) {
            Ok(crate::models_loader::ModelsDir::Present(dir)) => dir,
            // `rocky run` fails on a missing models directory unless the
            // pipeline sets `allow_empty_models = true`, so the report says
            // why this pipeline adds no model.
            Ok(crate::models_loader::ModelsDir::Absent(dir)) => {
                if !tx.allow_empty_models {
                    warnings.push(crate::models_loader::missing_models_dir_warning(
                        &dir,
                        name,
                        "its declared dependencies are unknown",
                    ));
                }
                continue;
            }
            Err(error) => {
                warnings.push(format!(
                    "pipeline {name}: its models directory cannot be resolved ({error:#}); \
                     its declared dependencies are unknown"
                ));
                continue;
            }
        };
        let (models, errors) =
            crate::models_loader::load_project_models_partial(&dir, Some(&cfg.freshness));
        for error in errors {
            warnings.push(format!(
                "pipeline {name}: some models did not load ({error:#}); their declared \
                 dependencies are unknown"
            ));
        }
        match crate::selection::project_from_models(
            models,
            &rocky_core::run_vars::RunVars::default(),
        ) {
            Ok(project) => {
                for node in &project.dag_nodes {
                    for dependency in &node.depends_on {
                        children
                            .entry(dependency.clone())
                            .or_default()
                            .insert(node.name.clone());
                    }
                }
            }
            Err(error) => warnings.push(format!(
                "pipeline {name}: the model graph does not resolve ({error:#}); its declared \
                 dependencies are unknown"
            )),
        }
    }
    children
}

/// Narrow a figure to the JSON width. [`check_fits_u64`] has already refused
/// any report with a larger figure, so the fallback is never taken.
fn to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Refuse a report whose figures do not fit the 64-bit JSON integers, rather
/// than print a clipped number that no longer adds up.
fn check_fits_u64(split: &cost_split::MonthSplit) -> Result<()> {
    let limit = u128::from(u64::MAX);
    let mut largest = split
        .measured_total
        .values()
        .chain(split.shadow_branch.values())
        .chain(split.unallocated.values())
        .copied()
        .max()
        .unwrap_or(0);
    for row in split.products.values() {
        for value in row
            .built
            .values()
            .chain(row.kept.values())
            .chain(row.owner_residual.values())
        {
            largest = largest.max(*value);
        }
        for a in &row.allocated_upstream {
            largest = largest.max(a.amount).max(a.of_total);
        }
        for s in &row.shared_not_charged {
            largest = largest.max(s.amount);
        }
    }
    anyhow::ensure!(
        largest <= limit,
        "a figure in this month is larger than 2^64 - 1 and cannot be reported exactly"
    );
    Ok(())
}

fn amounts_out(amounts: &cost_split::Amounts) -> Vec<CostAmount> {
    amounts
        .iter()
        .filter(|(_, value)| **value > 0)
        .map(|(unit, value)| CostAmount {
            unit: (*unit).into(),
            amount: to_u64(*value),
        })
        .collect()
}

/// Bytes in one GB, for `storage_cost_per_gb_month`. The same GiB that
/// `rocky optimize` divides stored bytes by.
const BYTES_PER_GB: f64 = 1_073_741_824.0;

/// Milliseconds in one machine-hour, for `compute_cost_per_machine_hour`.
const MS_PER_MACHINE_HOUR: f64 = 3_600_000.0;

/// Tokens in one million, for `ai_cost_per_million_tokens`.
const TOKENS_PER_MILLION: f64 = 1_000_000.0;

/// The `[cost]` key that prices a money figure.
fn price_key(figure: MoneyFigure) -> &'static str {
    match figure {
        MoneyFigure::Compute => "compute_cost_per_machine_hour",
        MoneyFigure::StorageDataFiles | MoneyFigure::StorageAllocatedBlocks => {
            "storage_cost_per_gb_month"
        }
        MoneyFigure::Egress => "cross_region_transfer_cost_per_gb",
        MoneyFigure::AiTokens => "ai_cost_per_million_tokens",
    }
}

/// The `[cost]` keys a `no_price` total is missing, deduplicated, in figure
/// order.
fn missing_price_keys(parts: &[MoneyPart]) -> Vec<&'static str> {
    let mut keys: Vec<&'static str> = Vec::new();
    for part in parts.iter().filter(|p| p.status == MoneyStatus::NoPrice) {
        let key = price_key(part.figure);
        if !keys.contains(&key) {
            keys.push(key);
        }
    }
    keys
}

/// The prices the monthly report may use: only those the user set in
/// `[cost]`. It reads the raw fields, never the estimate accessors that fall
/// back to Rocky's shipped defaults, and never the per-run cost formula.
#[derive(Debug, Clone, Copy)]
struct MonthPricing {
    /// USD per machine-hour of compute.
    compute_per_machine_hour: Option<f64>,
    /// USD per GB-month of storage.
    storage_per_gb_month: Option<f64>,
    /// Hours in the month being reported: a GB-month is one GB stored for
    /// all of them.
    hours_in_month: f64,
    /// When false, every amount is partial and gets no money.
    history_complete: bool,
    /// Whether the month measured each storage basis at all. A basis the
    /// month never measured is left out of every money total.
    data_files_measured: bool,
    allocated_blocks_measured: bool,
    /// USD per GB (2^30 bytes) of cross-region transfer.
    egress_per_gb: Option<f64>,
    /// USD per million AI tokens, input and output alike.
    ai_per_million_tokens: Option<f64>,
    /// Whether any production execution this month reported egress. When
    /// none did, egress is `not_measured` everywhere.
    egress_measured: bool,
    /// Whether this month has a recorded AI call with token counts. When it
    /// has none, AI tokens are `not_measured` everywhere.
    ai_measured: bool,
    /// Whether the state store started recording AI calls after the month
    /// began (or never did). Then the AI part is `partial` and stays out of
    /// every total.
    ai_partial: bool,
}

impl MonthPricing {
    fn new(
        cost: Option<&rocky_core::config::CostSection>,
        window: &cost_split::MonthWindow,
        history_complete: bool,
        measured_total: &[CostAmount],
        egress_reported: bool,
        ai_measured: bool,
        ai_usage_since: Option<DateTime<Utc>>,
    ) -> Self {
        let measured = |unit: CostUnit| measured_total.iter().any(|a| a.unit == unit);
        Self {
            compute_per_machine_hour: cost.and_then(|c| c.compute_cost_per_machine_hour),
            storage_per_gb_month: cost.and_then(|c| c.storage_cost_per_gb_month),
            hours_in_month: (window.end - window.start).num_seconds() as f64 / 3600.0,
            history_complete,
            data_files_measured: measured(CostUnit::StorageByteHoursDataFiles),
            allocated_blocks_measured: measured(CostUnit::StorageByteHoursAllocatedBlocks),
            egress_per_gb: cost.and_then(|c| c.cross_region_transfer_cost_per_gb),
            ai_per_million_tokens: cost.and_then(|c| c.ai_cost_per_million_tokens),
            // Taken from the split, not inferred from the sums: a reported
            // zero adds nothing to a sum but is still measured.
            egress_measured: egress_reported,
            ai_measured,
            ai_partial: ai_usage_since.is_none_or(|since| since > window.start),
        }
    }

    /// Price one measured amount. A zero amount is $0 with or without a
    /// price; a non-zero amount needs a price the user set. In a partial
    /// month a part that could be priced is `partial` and has no money.
    fn part(&self, figure: MoneyFigure, amount: u64, price: Option<f64>, per: f64) -> MoneyPart {
        let (status, usd) = match price {
            _ if amount == 0 => (MoneyStatus::Priced, Some(0.0)),
            None => (MoneyStatus::NoPrice, None),
            Some(price) => (MoneyStatus::Priced, Some(amount as f64 / per * price)),
        };
        let (status, usd) = if status == MoneyStatus::Priced && !self.history_complete {
            (MoneyStatus::Partial, None)
        } else {
            (status, usd)
        };
        MoneyPart {
            figure,
            status,
            usd,
        }
    }

    /// Price a set of amounts. Returns the total, its status and the parts.
    ///
    /// The total exists only when every part in it is priced. A partial
    /// month has no total. Egress is in the total when the month measured
    /// it, and `not_measured` (outside the total) otherwise. AI tokens are
    /// in the total when the month has recorded calls and the store recorded
    /// the whole month. When the store started recording after the month
    /// began (or never did), the AI part is `partial` and outside the total,
    /// with or without recorded calls. With the whole month recorded and no
    /// calls it is `not_measured`.
    fn price(&self, amounts: &[CostAmount]) -> (Option<f64>, MoneyStatus, Vec<MoneyPart>) {
        let amount_of = |unit: CostUnit| {
            amounts
                .iter()
                .find(|a| a.unit == unit)
                .map_or(0, |a| a.amount)
        };
        let mut parts = vec![self.part(
            MoneyFigure::Compute,
            amount_of(CostUnit::ComputeMs),
            self.compute_per_machine_hour,
            MS_PER_MACHINE_HOUR,
        )];
        let byte_hours_per_gb_month = BYTES_PER_GB * self.hours_in_month;
        for (figure, unit, present) in [
            (
                MoneyFigure::StorageDataFiles,
                CostUnit::StorageByteHoursDataFiles,
                self.data_files_measured,
            ),
            (
                MoneyFigure::StorageAllocatedBlocks,
                CostUnit::StorageByteHoursAllocatedBlocks,
                self.allocated_blocks_measured,
            ),
        ] {
            if present {
                parts.push(self.part(
                    figure,
                    amount_of(unit),
                    self.storage_per_gb_month,
                    byte_hours_per_gb_month,
                ));
            }
        }
        if self.egress_measured {
            parts.push(self.part(
                MoneyFigure::Egress,
                amount_of(CostUnit::EgressBytes),
                self.egress_per_gb,
                BYTES_PER_GB,
            ));
        }
        if self.ai_measured && !self.ai_partial {
            let tokens = amount_of(CostUnit::AiInputTokens)
                .saturating_add(amount_of(CostUnit::AiOutputTokens));
            parts.push(self.part(
                MoneyFigure::AiTokens,
                tokens,
                self.ai_per_million_tokens,
                TOKENS_PER_MILLION,
            ));
        }
        // A partial AI part holds the whole total back when the month has
        // recorded calls: a total that leaves out spend the store never saw
        // would understate. With no recorded calls the AI part is only
        // listed, and the rest of the month is priced.
        let (usd, status) = if !self.history_complete || (self.ai_measured && self.ai_partial) {
            (None, MoneyStatus::Partial)
        } else if parts.iter().all(|p| p.status == MoneyStatus::Priced) {
            (
                Some(parts.iter().filter_map(|p| p.usd).sum()),
                MoneyStatus::Priced,
            )
        } else {
            (None, MoneyStatus::NoPrice)
        };
        if !self.egress_measured {
            parts.push(MoneyPart {
                figure: MoneyFigure::Egress,
                status: MoneyStatus::NotMeasured,
                usd: None,
            });
        }
        // A month that began before this store started recording has AI
        // calls the store never saw, with or without calls recorded since.
        if self.ai_partial {
            parts.push(MoneyPart {
                figure: MoneyFigure::AiTokens,
                status: MoneyStatus::Partial,
                usd: None,
            });
        } else if !self.ai_measured {
            parts.push(MoneyPart {
                figure: MoneyFigure::AiTokens,
                status: MoneyStatus::NotMeasured,
                usd: None,
            });
        }
        (usd, status, parts)
    }

    /// A floor on the money of `parts`: the sum of the priced parts. Costs
    /// are never negative, so the real figure is at least this.
    fn priced_floor(parts: &[MoneyPart]) -> f64 {
        parts
            .iter()
            .filter(|p| p.status == MoneyStatus::Priced)
            .filter_map(|p| p.usd)
            .sum()
    }
}

fn product_out(
    product: &str,
    output_model: &str,
    row: &cost_split::ProductSplit,
    pricing: &MonthPricing,
) -> ProductMonthCost {
    let charged = charged_amounts(row);
    let (money, money_status, money_parts) = pricing.price(&charged);
    ProductMonthCost {
        product: product.to_string(),
        output_model: output_model.to_string(),
        built: amounts_out(&row.built),
        kept: amounts_out(&row.kept),
        owner_residual: amounts_out(&row.owner_residual),
        allocated_upstream: row
            .allocated_upstream
            .iter()
            .map(|a| AllocatedUpstreamCost {
                table: a.table.clone(),
                unit: a.unit.into(),
                basis: a.basis.into(),
                amount: to_u64(a.amount),
                of_total: to_u64(a.of_total),
                low_confidence: a.low_confidence,
                provisional: a.provisional,
            })
            .collect(),
        shared_not_charged: row
            .shared_not_charged
            .iter()
            .map(|s| SharedNotChargedCost {
                table: s.table.clone(),
                reader: s.reader.clone(),
                unit: s.unit.into(),
                basis: s.basis.into(),
                amount: to_u64(s.amount),
            })
            .collect(),
        charged,
        money,
        money_status,
        money_parts,
        budget: None,
        breach_events: Vec::new(),
        breach_events_skipped: None,
    }
}

/// Everything one month's report reads, loaded once: the config, the month
/// window, the run scan and the carry-in. Every split of the month (the
/// report itself and each replay step of the budgets) is computed from this,
/// so the budgets add no scan of the state store.
struct MonthInputs {
    cfg: Option<rocky_core::config::RockyConfig>,
    /// `YYYY-MM`.
    label: String,
    timezone: String,
    window: cost_split::MonthWindow,
    history_complete: bool,
    owners: std::collections::BTreeMap<String, String>,
    children: std::collections::BTreeMap<String, std::collections::BTreeSet<String>>,
    /// The scanned runs, ordered by when their record was written
    /// (`finished_at`, then `run_id`).
    runs: Vec<RunRecord>,
    carry_in: Vec<ModelExecution>,
    /// Every AI call this state store recorded: its table and its undrained
    /// spool, deduplicated. The split keeps those inside the month.
    ai_usage: Vec<rocky_core::ai_usage::AiUsageRecord>,
    /// When the store started recording AI calls. `None` when it never did.
    ai_usage_since: Option<DateTime<Utc>>,
    /// AI usage records the store could not read: spool lines and table rows
    /// that did not parse, plus those a drain dropped earlier.
    ai_records_dropped: u64,
    warnings: Vec<String>,
}

/// A warning when the default state file's AI usage spool holds calls this
/// report does not read.
///
/// `rocky lsp` and the MCP server spool next to the default state file
/// (`<models>/.rocky-state.redb`). They take no `--state-path` or
/// `--state-namespace`, so when this report reads another store those calls
/// land where it never looks. Their count is unknown here, so the report says
/// so and is not silent.
fn stray_default_spool_warning(store: &StateStore, config_path: &Path) -> Option<String> {
    let used = store.ai_usage_spool_path()?;
    let models_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new(""))
        .join("models");
    let default = rocky_core::ai_usage::spool_path(
        &rocky_core::state::resolve_state_path(None, &models_dir).path,
    );
    let non_empty = std::fs::metadata(&default).is_ok_and(|m| m.len() > 0);
    if !non_empty {
        return None;
    }
    let same = match (std::fs::canonicalize(used), std::fs::canonicalize(&default)) {
        (Ok(a), Ok(b)) => a == b,
        _ => used == default,
    };
    if same {
        return None;
    }
    Some(format!(
        "AI calls from `rocky lsp` or the MCP server are spooled at {}, but this report reads \
         {}; those calls are not counted. `rocky lsp` and MCP always use the default state \
         file, not --state-path or --state-namespace",
        default.display(),
        used.display()
    ))
}

fn load_month_inputs(
    store: &StateStore,
    config_path: &Path,
    month: &str,
    now: DateTime<Utc>,
) -> Result<MonthInputs> {
    let (year, month_number) = parse_month(month)?;
    let cfg = load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let timezone = cfg
        .as_ref()
        .map_or_else(|| "UTC".to_string(), |c| c.schedule.timezone.clone());
    let window = cost_split::month_bounds_in_zone(year, month_number, &timezone).map_err(
        |error| match error {
            cost_split::MonthBoundsError::UnknownTimeZone => {
                anyhow::anyhow!("[schedule].timezone '{timezone}' is not an IANA time zone name")
            }
            cost_split::MonthBoundsError::NoSuchMonth => {
                anyhow::anyhow!("--month '{month}' is outside the range of dates Rocky handles")
            }
        },
    )?;
    let retention = cfg
        .as_ref()
        .map(|c| c.state.retention.clone())
        .unwrap_or_default();
    let history_complete = history_is_complete(&retention, window.start, now);

    let mut warnings = Vec::new();
    let root = crate::commands::product::product_command_root(config_path);
    let (owners, problems) = crate::commands::product::product_owners_in(&root);
    // A spec that did not load owns no model in this report: its cost is not
    // charged to the product.
    warnings.extend(problems);
    let children = declared_children(cfg.as_ref(), config_path, &mut warnings);

    // A run can start before the month and hold executions inside it, so the
    // scan reaches one day back. Anything older is the known limit.
    let mut runs =
        store.list_runs_matching_since(window.start - chrono::Duration::days(1), |_| true)?;
    runs.sort_by(|a, b| {
        a.finished_at
            .cmp(&b.finished_at)
            .then_with(|| a.run_id.cmp(&b.run_id))
    });

    // Models whose last build before the month is needed: every owned model,
    // every model run in the scan, and every model the graph names, so a table
    // built before the month and only read in it still has its storage.
    let mut wanted: std::collections::BTreeSet<String> = owners.keys().cloned().collect();
    for (parent, kids) in &children {
        wanted.insert(parent.clone());
        wanted.extend(kids.iter().cloned());
    }
    for run in &runs {
        for exec in &run.models_executed {
            wanted.insert(exec.rocky_model_name().to_string());
        }
    }
    let carry_in = store.last_measured_builds_before(&wanted, window.start, |r| {
        r.counts_as_production(UnrecordedScope::Count)
    })?;
    let carry_in: Vec<ModelExecution> = carry_in.into_values().collect();

    // AI calls recorded in this state store only: a call on another machine
    // wrote to that machine's store.
    let ai = store.ai_usage_including_spool()?;
    if let Some(warning) = stray_default_spool_warning(store, config_path) {
        warnings.push(warning);
    }
    let ai_records_dropped = store
        .ai_usage_dropped()?
        .saturating_add(ai.malformed as u64);
    if ai_records_dropped > 0 {
        warnings.push(format!(
            "{ai_records_dropped} AI usage record(s) did not parse and are not counted"
        ));
    }
    let ai_usage_since = store.ai_usage_since()?;

    Ok(MonthInputs {
        cfg,
        label: format!("{year:04}-{month_number:02}"),
        timezone,
        window,
        history_complete,
        owners,
        children,
        runs,
        carry_in,
        ai_usage: ai.records,
        ai_usage_since,
        ai_records_dropped,
        warnings,
    })
}

impl MonthInputs {
    /// Split the month over `runs` (a prefix of [`Self::runs`] for a replay
    /// step, or all of them), with storage accrued to `as_of`. A split that
    /// breaks its own invariant is an error, never printed.
    fn split(&self, runs: &[RunRecord], as_of: DateTime<Utc>) -> Result<cost_split::MonthSplit> {
        let split = cost_split::split_month(&cost_split::SplitInput {
            window: self.window,
            as_of,
            runs,
            carry_in: &self.carry_in,
            history_complete: self.history_complete,
            owners: &self.owners,
            declared_children: &self.children,
            default_catalog: None,
            ai_usage: &self.ai_usage,
        });
        let broken = split.invariant_violations();
        anyhow::ensure!(
            broken.is_empty(),
            "the monthly split does not add up for {} — this is a bug in Rocky, not in your data",
            broken
                .iter()
                .map(|u| u.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        check_fits_u64(&split)?;
        Ok(split)
    }

    fn pricing(
        &self,
        split: &cost_split::MonthSplit,
        measured_total: &[CostAmount],
    ) -> MonthPricing {
        MonthPricing::new(
            self.cfg.as_ref().map(|c| &c.cost),
            &self.window,
            self.history_complete,
            measured_total,
            split.egress_reported,
            split.ai_measured,
            self.ai_usage_since,
        )
    }

    fn budgets(&self) -> Option<&std::collections::BTreeMap<String, ProductBudgetConfig>> {
        self.cfg
            .as_ref()
            .map(|c| &c.product_budget)
            .filter(|b| !b.is_empty())
    }

    /// The executions the full replay splits: for each replay step, every
    /// execution in the runs up to it. This, not the number of runs, is what
    /// the replay costs.
    fn replay_work(&self) -> usize {
        let mut prefix = 0usize;
        let mut work = 0usize;
        for run in &self.runs {
            prefix = prefix.saturating_add(run.models_executed.len());
            if self.is_step(run) {
                work = work.saturating_add(prefix);
            }
        }
        work
    }

    /// Whether `run` is a replay step: a production run with an execution
    /// that started inside the month.
    fn is_step(&self, run: &RunRecord) -> bool {
        run.counts_as_production(UnrecordedScope::Count)
            && run
                .models_executed
                .iter()
                .any(|exec| self.window.contains(exec.started_at))
    }
}

/// Milliseconds in one hour, for `max_compute_hours`.
const MS_PER_HOUR: f64 = 3_600_000.0;

fn budget_check(
    limit_type: ProductBudgetLimitType,
    unit: Option<CostUnit>,
    limit: f64,
    actual: Option<f64>,
    unknown: Option<&str>,
) -> ProductBudgetCheck {
    let (verdict, reason) = match (actual, unknown) {
        (_, Some(reason)) => (ProductBudgetVerdict::NotEvaluated, Some(reason.to_string())),
        (None, None) => (
            ProductBudgetVerdict::NotEvaluated,
            Some("the figure is not known".to_string()),
        ),
        (Some(actual), None) if actual > limit => (ProductBudgetVerdict::Breached, None),
        (Some(_), None) => (ProductBudgetVerdict::InBudget, None),
    };
    ProductBudgetCheck {
        limit_type,
        unit,
        limit,
        actual,
        verdict,
        reason,
    }
}

const HISTORY_INCOMPLETE: &str =
    "run history does not cover the whole month: state retention may have swept runs it needs";

/// Check one product's budget against what it is charged this month.
///
/// `charged` is the product's `kept + allocated_upstream` (see
/// [`product_out`]); `pricing` also says which storage bases the month
/// measured at all; `production_ran` is whether any production run had an
/// execution in the month. The verdict is never `in_budget` unless every limit was
/// checked.
fn evaluate_budget(
    budget: &ProductBudgetConfig,
    charged: &[CostAmount],
    pricing: &MonthPricing,
    production_ran: bool,
) -> ProductBudgetStatus {
    let amount_of = |unit: CostUnit| {
        charged
            .iter()
            .find(|a| a.unit == unit)
            .map_or(0, |a| a.amount)
    };
    let history = (!pricing.history_complete).then_some(HISTORY_INCOMPLETE);
    let mut checks = Vec::new();
    if let Some(limit) = budget.max_compute_hours {
        let unknown = history.or_else(|| {
            (!production_ran).then_some("no production run recorded model time this month")
        });
        checks.push(budget_check(
            ProductBudgetLimitType::MaxComputeHours,
            None,
            limit,
            Some(amount_of(CostUnit::ComputeMs) as f64 / MS_PER_HOUR),
            unknown,
        ));
    }
    if let Some(limit) = budget.max_storage_gb_month {
        let byte_hours_per_gb_month = BYTES_PER_GB * pricing.hours_in_month;
        let mut any = false;
        for (unit, measured) in [
            (
                CostUnit::StorageByteHoursDataFiles,
                pricing.data_files_measured,
            ),
            (
                CostUnit::StorageByteHoursAllocatedBlocks,
                pricing.allocated_blocks_measured,
            ),
        ] {
            if !measured {
                continue;
            }
            any = true;
            checks.push(budget_check(
                ProductBudgetLimitType::MaxStorageGbMonth,
                Some(unit),
                limit,
                Some(amount_of(unit) as f64 / byte_hours_per_gb_month),
                history,
            ));
        }
        if !any {
            checks.push(budget_check(
                ProductBudgetLimitType::MaxStorageGbMonth,
                None,
                limit,
                None,
                Some("no stored size was measured this month"),
            ));
        }
    }
    if let Some(limit) = budget.max_usd {
        checks.push(max_usd_check(limit, charged, pricing));
    }
    let mut reasons = Vec::new();
    let verdict = if let Some(reason) = history {
        reasons.push(reason.to_string());
        ProductBudgetVerdict::NotEvaluated
    } else if checks.is_empty() {
        reasons.push("the budget sets no limit".to_string());
        ProductBudgetVerdict::NotEvaluated
    } else if checks
        .iter()
        .any(|c| c.verdict == ProductBudgetVerdict::Breached)
    {
        ProductBudgetVerdict::Breached
    } else if checks
        .iter()
        .any(|c| c.verdict == ProductBudgetVerdict::NotEvaluated)
    {
        ProductBudgetVerdict::NotEvaluated
    } else {
        ProductBudgetVerdict::InBudget
    };
    ProductBudgetStatus {
        verdict,
        on_breach: budget.on_breach,
        checks,
        reasons,
    }
}

/// Check a `max_usd` limit against `charged`.
///
/// With every figure priced, the total is tested. Otherwise the priced
/// parts alone are a floor (costs are never negative): a floor over the
/// limit is already a breach, and `actual` is that floor. Anything else is
/// `not_evaluated`, naming what is missing. A `not_measured` part is outside
/// the total either way, as it is in the report.
fn max_usd_check(limit: f64, charged: &[CostAmount], pricing: &MonthPricing) -> ProductBudgetCheck {
    let (usd, status, parts) = pricing.price(charged);
    // A budget is held back by partial AI only when the month recorded AI
    // calls. A month that began before recording but has none is reported
    // partial, and its budget is judged on what the store recorded: the
    // alternative, no verdict for every month a store is first upgraded in,
    // would silence every `max_usd` budget.
    let ai_partial = pricing.ai_measured && pricing.ai_partial;
    let missing = match status {
        MoneyStatus::Priced => {
            return budget_check(ProductBudgetLimitType::MaxUsd, None, limit, usd, None);
        }
        MoneyStatus::Partial | MoneyStatus::NotMeasured if !pricing.history_complete => {
            return budget_check(
                ProductBudgetLimitType::MaxUsd,
                None,
                limit,
                None,
                Some(HISTORY_INCOMPLETE),
            );
        }
        MoneyStatus::Partial | MoneyStatus::NotMeasured | MoneyStatus::NoPrice => {
            let mut missing: Vec<String> = Vec::new();
            let keys = missing_price_keys(&parts);
            if !keys.is_empty() {
                missing.push(format!(
                    "a charged figure has no price in [cost] ({})",
                    keys.join(", ")
                ));
            }
            if ai_partial {
                missing.push(AI_RECORDING_STARTED_LATE.to_string());
            }
            missing.join("; ")
        }
    };
    let floor = MonthPricing::priced_floor(&parts);
    if floor > limit {
        return ProductBudgetCheck {
            limit_type: ProductBudgetLimitType::MaxUsd,
            unit: None,
            limit,
            actual: Some(floor),
            verdict: ProductBudgetVerdict::Breached,
            reason: Some(format!(
                "the priced figures alone exceed the limit; the full figure is higher: {missing}"
            )),
        };
    }
    budget_check(
        ProductBudgetLimitType::MaxUsd,
        None,
        limit,
        None,
        Some(&missing),
    )
}

const AI_RECORDING_STARTED_LATE: &str = "this state store started recording AI calls after the \
     month began, so the month's AI spend is partial";

/// The charged amounts of one product row, as the report prints them.
fn charged_amounts(row: &cost_split::ProductSplit) -> Vec<CostAmount> {
    let charged: cost_split::Amounts = cost_split::Unit::ALL
        .into_iter()
        .map(|unit| (unit, row.charged(unit)))
        .collect();
    amounts_out(&charged)
}

/// Every budgeted product's verdict for one split of the month.
fn evaluate_budgets(
    inputs: &MonthInputs,
    budgets: &std::collections::BTreeMap<String, ProductBudgetConfig>,
    split: &cost_split::MonthSplit,
    production_ran: bool,
) -> std::collections::BTreeMap<String, ProductBudgetStatus> {
    let measured_total = amounts_out(&split.measured_total);
    let pricing = inputs.pricing(split, &measured_total);
    budgets
        .iter()
        .filter_map(|(product, budget)| {
            let row = split.products.get(product)?;
            Some((
                product.clone(),
                evaluate_budget(budget, &charged_amounts(row), &pricing, production_ran),
            ))
        })
        .collect()
}

/// Every budgeted product's verdict as of the replay step at `index` of
/// [`MonthInputs::runs`]: the month split over the runs whose records were
/// written by the time that run's was, with storage accrued to then.
fn step_verdicts(
    inputs: &MonthInputs,
    budgets: &std::collections::BTreeMap<String, ProductBudgetConfig>,
    index: usize,
) -> Result<std::collections::BTreeMap<String, ProductBudgetStatus>> {
    let run = &inputs.runs[index];
    let split = inputs.split(&inputs.runs[..=index], run.finished_at)?;
    Ok(evaluate_budgets(inputs, budgets, &split, true))
}

/// The event a breached step records: the run, and the limits it exceeded.
fn breach_event(run: &RunRecord, status: &ProductBudgetStatus) -> ProductBudgetBreachEvent {
    ProductBudgetBreachEvent {
        run_id: run.run_id.clone(),
        at: run.finished_at.to_rfc3339(),
        breached: status
            .checks
            .iter()
            .filter(|c| c.verdict == ProductBudgetVerdict::Breached)
            .cloned()
            .collect(),
    }
}

/// The breach events of every budgeted product this month.
///
/// The prior verdict is not stored. Instead the month is split again after
/// each production run, as of the moment that run's record was written, over
/// the runs whose records existed by then, and the budget is checked each
/// time. An event is a step where the product is `breached` and its last
/// evaluated verdict before it was not (or there was none). A step that is
/// `not_evaluated` is skipped over: it neither ends a breach nor starts one.
fn replay_breach_events(
    inputs: &MonthInputs,
    budgets: &std::collections::BTreeMap<String, ProductBudgetConfig>,
) -> Result<std::collections::BTreeMap<String, Vec<ProductBudgetBreachEvent>>> {
    let mut events: std::collections::BTreeMap<String, Vec<ProductBudgetBreachEvent>> =
        std::collections::BTreeMap::new();
    // With history incomplete every step is `not_evaluated`.
    if !inputs.history_complete {
        return Ok(events);
    }
    let mut last: std::collections::BTreeMap<String, ProductBudgetVerdict> =
        std::collections::BTreeMap::new();
    for (index, run) in inputs.runs.iter().enumerate() {
        if !inputs.is_step(run) {
            continue;
        }
        for (product, status) in step_verdicts(inputs, budgets, index)? {
            match status.verdict {
                ProductBudgetVerdict::NotEvaluated => {}
                ProductBudgetVerdict::InBudget => {
                    last.insert(product, ProductBudgetVerdict::InBudget);
                }
                ProductBudgetVerdict::Breached => {
                    let previous = last.insert(product.clone(), ProductBudgetVerdict::Breached);
                    if previous == Some(ProductBudgetVerdict::Breached) {
                        continue;
                    }
                    events
                        .entry(product)
                        .or_default()
                        .push(breach_event(run, &status));
                }
            }
        }
    }
    Ok(events)
}

/// The events the replay step at `index` records, by product: the same rule
/// as [`replay_breach_events`], without replaying the whole month.
///
/// Splits the month at `index`, then, for each product breached there, walks
/// back over earlier steps only until it finds that product's last evaluated
/// verdict. Usually that is the step before, so the end-of-run check costs
/// two splits, not one per run of the month.
fn events_at_step(
    inputs: &MonthInputs,
    budgets: &std::collections::BTreeMap<String, ProductBudgetConfig>,
    index: usize,
    walk_back_cap: usize,
) -> Result<StepCheck> {
    let mut events = std::collections::BTreeMap::new();
    let run = &inputs.runs[index];
    let verdicts = step_verdicts(inputs, budgets, index)?;
    if !inputs.history_complete || !inputs.is_step(run) {
        return Ok(StepCheck {
            verdicts,
            events,
            earlier_splits: 0,
        });
    }
    let mut pending: std::collections::BTreeMap<String, ProductBudgetStatus> = verdicts
        .iter()
        .filter(|(_, status)| status.verdict == ProductBudgetVerdict::Breached)
        .map(|(product, status)| (product.clone(), status.clone()))
        .collect();
    let mut earlier_splits = 0;
    for earlier in (0..index).rev() {
        if pending.is_empty() || earlier_splits == walk_back_cap {
            break;
        }
        if !inputs.is_step(&inputs.runs[earlier]) {
            continue;
        }
        earlier_splits += 1;
        let before = step_verdicts(inputs, budgets, earlier)?;
        pending.retain(|product, status| {
            match before.get(product).map(|s| s.verdict) {
                // Already over: still out, no new event.
                Some(ProductBudgetVerdict::Breached) => false,
                Some(ProductBudgetVerdict::InBudget) => {
                    events.insert(product.clone(), breach_event(run, status));
                    false
                }
                Some(ProductBudgetVerdict::NotEvaluated) | None => true,
            }
        });
    }
    // No evaluated verdict before this step, within the cap: this is the
    // product's first.
    for (product, status) in pending {
        events.insert(product, breach_event(run, &status));
    }
    Ok(StepCheck {
        verdicts,
        events,
        earlier_splits,
    })
}

/// What [`events_at_step`] found at one replay step.
struct StepCheck {
    /// Every budgeted product's verdict as of the step.
    verdicts: std::collections::BTreeMap<String, ProductBudgetStatus>,
    /// The events the step records, by product.
    events: std::collections::BTreeMap<String, ProductBudgetBreachEvent>,
    /// How many earlier steps it split to find the last verdicts.
    earlier_splits: usize,
}

/// The most replay work for which the monthly report (`rocky cost --month`,
/// and `cost_month` on `rocky product status`) lists breach events, counted
/// as executions split (see `MonthInputs::replay_work`). Listing them splits
/// the month once per production run, so the work grows with the square of
/// the runs and with the executions in each. This cap is 750 runs of 21
/// executions each (about 3 s in a release build); a month of hourly runs of
/// about 20 models fits. Above it, the report skips the replay and says so.
pub(crate) const MAX_REPLAY_WORK: usize = 750 * 751 / 2 * 21;

/// The most earlier runs the end-of-run check splits to find a breached
/// product's last verdict. Earlier runs are `not_evaluated` only while the
/// month has measured nothing a limit needs, so the walk almost always stops
/// at the run before. Past the cap, the breach counts as the product's first.
const MAX_WALK_BACK_STEPS: usize = 8;

/// The typed payload behind `rocky cost --month YYYY-MM --by product`.
///
/// Splits one month of production cost across the products in `products/`,
/// in physical units (see [`rocky_core::cost_split`]), priced in USD from the
/// prices set in `[cost]` (see `MonthPricing`). It reads run history
/// only; the month is cut in the project's `[schedule].timezone`, and each
/// execution belongs to the month of its own start. `now` is passed in so a
/// caller can pin it: storage accrues to the earlier of `now` and the end of
/// the month.
///
/// Each product with a `[product_budget.<name>]` also gets its budget,
/// checked as of `now`, and the month's breach events (see
/// `replay_breach_events`).
///
/// A split that breaks its own invariant is an error, never printed.
pub fn compute_month_cost(
    state_path: &Path,
    config_path: &Path,
    month: &str,
    now: DateTime<Utc>,
) -> Result<ProductMonthCostOutput> {
    let store = StateStore::open_read_only_or_empty(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    compute_month_cost_in(&store, config_path, month, now, MAX_REPLAY_WORK)
}

/// [`compute_month_cost`] over a store the caller already holds open, listing
/// breach events only when the month has at most `replay_cap` production runs.
fn compute_month_cost_in(
    store: &StateStore,
    config_path: &Path,
    month: &str,
    now: DateTime<Utc>,
    replay_cap: usize,
) -> Result<ProductMonthCostOutput> {
    let mut inputs = load_month_inputs(store, config_path, month, now)?;
    let split = inputs.split(&inputs.runs, now)?;
    let measured_total = amounts_out(&split.measured_total);
    let unallocated = amounts_out(&split.unallocated);
    let pricing = inputs.pricing(&split, &measured_total);
    let (measured_total_money, measured_total_money_status, measured_total_money_parts) =
        pricing.price(&measured_total);
    let (unallocated_money, unallocated_money_status, unallocated_money_parts) =
        pricing.price(&unallocated);

    let mut budget_status = std::collections::BTreeMap::new();
    let mut breach_events = std::collections::BTreeMap::new();
    let mut budget_warnings = Vec::new();
    let mut events_skipped: Option<String> = None;
    if let Some(budgets) = inputs.budgets() {
        for product in budgets.keys() {
            if !split.products.contains_key(product) {
                budget_warnings.push(format!(
                    "[product_budget.{product}] names no product that owns a model; \
                     it is not checked"
                ));
            }
        }
        let steps = inputs.runs.iter().filter(|run| inputs.is_step(run)).count();
        budget_status = evaluate_budgets(&inputs, budgets, &split, steps > 0);
        let work = inputs.replay_work();
        if work <= replay_cap {
            breach_events = replay_breach_events(&inputs, budgets)?;
        } else if inputs.history_complete {
            // With history incomplete there are no events to list anyway.
            let reason = format!(
                "breach events are not listed: replaying this month's {steps} production \
                 runs would split {work} executions, more than the {replay_cap} the report \
                 replays"
            );
            budget_warnings.push(reason.clone());
            events_skipped = Some(reason);
        }
    }
    inputs.warnings.extend(budget_warnings);

    let model_of: std::collections::BTreeMap<&str, &str> = inputs
        .owners
        .iter()
        .map(|(model, product)| (product.as_str(), model.as_str()))
        .collect();
    let products = split
        .products
        .iter()
        .map(|(product, row)| {
            let mut out = product_out(
                product,
                model_of.get(product.as_str()).copied().unwrap_or(""),
                row,
                &pricing,
            );
            out.budget = budget_status.remove(product);
            out.breach_events = breach_events.remove(product).unwrap_or_default();
            if out.budget.is_some() {
                out.breach_events_skipped.clone_from(&events_skipped);
            }
            out
        })
        .collect();

    Ok(ProductMonthCostOutput {
        version: VERSION.to_string(),
        command: "cost_by_product".to_string(),
        month: inputs.label,
        timezone: inputs.timezone,
        month_start: inputs.window.start.to_rfc3339(),
        month_end: inputs.window.end.to_rfc3339(),
        as_of: now.to_rfc3339(),
        history_complete: inputs.history_complete,
        products,
        unallocated,
        shadow_branch: amounts_out(&split.shadow_branch),
        measured_total,
        measured_total_money,
        measured_total_money_status,
        measured_total_money_parts,
        unallocated_money,
        unallocated_money_status,
        unallocated_money_parts,
        not_measured: CostNotMeasured {
            failed_executions: split.not_measured.failed_executions,
            storage_unknown_tables: split.not_measured.storage_unknown_tables,
            unattributed_read_tables: split.not_measured.unattributed_read_tables,
            egress_unreported_executions: split.not_measured.egress_unreported_executions,
            ai_calls_without_usage: split.not_measured.ai_calls_without_usage,
            ai_records_dropped: inputs.ai_records_dropped,
        },
        warnings: inputs.warnings,
    })
}

/// The month, `YYYY-MM`, that contains `at` in `timezone`.
fn month_containing(at: DateTime<Utc>, timezone: &str) -> Option<String> {
    use chrono::Datelike;
    // The local month is the UTC month or one next to it.
    let utc = at.date_naive();
    let (year, month) = (utc.year(), utc.month());
    let candidates = [
        if month == 1 {
            (year - 1, 12)
        } else {
            (year, month - 1)
        },
        (year, month),
        if month == 12 {
            (year + 1, 1)
        } else {
            (year, month + 1)
        },
    ];
    candidates.into_iter().find_map(|(y, m)| {
        cost_split::month_bounds_in_zone(y, m, timezone)
            .ok()
            .filter(|window| window.contains(at))
            .map(|_| format!("{y:04}-{m:02}"))
    })
}

/// The project's report time zone: `[schedule].timezone`, or UTC with no
/// config.
fn project_timezone(config_path: &Path) -> Result<String> {
    let cfg = load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    Ok(cfg.map_or_else(|| "UTC".to_string(), |c| c.schedule.timezone))
}

/// The month, `YYYY-MM`, that contains `now` in the project's
/// `[schedule].timezone` (UTC with no config). The month `rocky product
/// status` reports, and the default month of `GET /api/v1/cost/month`.
///
/// # Errors
///
/// Returns an error when the config cannot be read, or when no month
/// contains `now` in its time zone.
pub(crate) fn current_month(config_path: &Path, now: DateTime<Utc>) -> Result<String> {
    let timezone = project_timezone(config_path)?;
    month_containing(now, &timezone)
        .with_context(|| format!("no month contains {now} in time zone '{timezone}'"))
}

/// The product's current month, for `rocky product status`.
///
/// # Errors
///
/// Returns an error when the config, the state store or the month split
/// cannot be read.
pub fn product_cost_month(
    state_path: &Path,
    config_path: &Path,
    product: &str,
    now: DateTime<Utc>,
) -> Result<Option<ProductCostMonth>> {
    let month = current_month(config_path, now)?;
    let output = compute_month_cost(state_path, config_path, &month, now)?;
    Ok(output
        .products
        .into_iter()
        .find(|p| p.product == product)
        .map(|cost| ProductCostMonth {
            month: output.month,
            timezone: output.timezone,
            as_of: output.as_of,
            history_complete: output.history_complete,
            cost,
        }))
}

/// Check every `[product_budget.<name>]` at the end of a production run.
///
/// Reads the run's record back from `store`, so it must run after the
/// record is written. Checks each month the run's
/// executions can fall in (the month of its start and of its finish, in the
/// project's `[schedule].timezone`). Returns nothing when no product budget
/// is set, the record is not there, or the run is not a production run.
/// `breach_event` is set on an entry only when this run is the step that
/// moved the product over.
///
/// # Errors
///
/// Returns an error when the config, the state store or a month split cannot
/// be read. The caller only warns: a budget never fails a run.
pub fn product_budgets_after_run(
    store: &StateStore,
    config_path: &Path,
    run_id: &str,
    now: DateTime<Utc>,
) -> Result<Vec<RunProductBudget>> {
    let Some(cfg) = load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?
    else {
        return Ok(Vec::new());
    };
    if cfg.product_budget.is_empty() {
        return Ok(Vec::new());
    }
    let Some(record) = store.get_run(run_id)? else {
        tracing::warn!(
            run_id,
            "monthly product budgets not checked: this run's record is not in the state store"
        );
        return Ok(Vec::new());
    };
    if !record.counts_as_production(UnrecordedScope::Count) {
        return Ok(Vec::new());
    }
    let timezone = cfg.schedule.timezone.clone();
    let mut months: Vec<String> = [record.started_at, record.finished_at]
        .into_iter()
        .filter_map(|at| month_containing(at, &timezone))
        .collect();
    months.dedup();

    let mut out = Vec::new();
    for month in months {
        let inputs = load_month_inputs(store, config_path, &month, now)?;
        let Some(budgets) = inputs.budgets() else {
            continue;
        };
        // The verdict is the one as of this run's finish, from the same split
        // that decides its event. A run the scan does not hold (it started
        // more than a day before the month) gets no entry for the month.
        let Some(index) = inputs.runs.iter().position(|run| run.run_id == run_id) else {
            continue;
        };
        let StepCheck {
            verdicts,
            mut events,
            earlier_splits,
        } = events_at_step(&inputs, budgets, index, MAX_WALK_BACK_STEPS)?;
        tracing::debug!(
            run_id,
            month = %inputs.label,
            earlier_splits,
            "checked monthly product budgets"
        );
        for (product, budget) in verdicts {
            out.push(RunProductBudget {
                breach_event: events.remove(&product),
                product,
                month: inputs.label.clone(),
                budget,
            });
        }
    }
    Ok(out)
}

fn amount_cell(amounts: &[CostAmount], unit: CostUnit) -> String {
    amounts
        .iter()
        .find(|a| a.unit == unit)
        .map_or_else(|| "-".to_string(), |a| a.amount.to_string())
}

fn money_cell(usd: Option<f64>) -> String {
    usd.map_or_else(|| "-".to_string(), |usd| format!("{usd:.6}"))
}

fn money_status_label(status: MoneyStatus) -> &'static str {
    match status {
        MoneyStatus::Priced => "priced",
        MoneyStatus::NoPrice => "no price",
        MoneyStatus::NotMeasured => "not measured",
        MoneyStatus::Partial => "partial",
    }
}

fn print_month_table(output: &ProductMonthCostOutput) {
    println!(
        "month: {} ({}, {} to {})",
        output.month, output.timezone, output.month_start, output.month_end
    );
    println!("as_of: {}", output.as_of);
    if !output.history_complete {
        println!(
            "history_complete: false — state retention may have swept runs this month needs; \
             every figure below is partial"
        );
    }
    for warning in &output.warnings {
        println!("warning: {warning}");
    }
    println!();
    println!("model time, ms (built is a memo: it is not summed)");
    println!(
        "  {:<24}  {:>12}  {:>12}  {:>12}  {:>12}",
        "product", "built", "kept", "from others", "charged"
    );
    let from_others = |p: &ProductMonthCost| -> u64 {
        p.allocated_upstream
            .iter()
            .filter(|a| a.unit == CostUnit::ComputeMs)
            .map(|a| a.amount)
            .sum()
    };
    for p in &output.products {
        println!(
            "  {:<24}  {:>12}  {:>12}  {:>12}  {:>12}",
            p.product,
            amount_cell(&p.built, CostUnit::ComputeMs),
            amount_cell(&p.kept, CostUnit::ComputeMs),
            from_others(p),
            amount_cell(&p.charged, CostUnit::ComputeMs),
        );
    }
    println!(
        "  {:<24}  {:>12}  {:>12}  {:>12}  {:>12}",
        "<unallocated>",
        "",
        "",
        "",
        amount_cell(&output.unallocated, CostUnit::ComputeMs)
    );
    println!(
        "  {:<24}  {:>12}  {:>12}  {:>12}  {:>12}",
        "measured total",
        "",
        "",
        "",
        amount_cell(&output.measured_total, CostUnit::ComputeMs)
    );
    println!(
        "  shadow and branch (outside the total): {}",
        amount_cell(&output.shadow_branch, CostUnit::ComputeMs)
    );
    for (unit, label) in [
        (CostUnit::StorageByteHoursDataFiles, "data files"),
        (
            CostUnit::StorageByteHoursAllocatedBlocks,
            "allocated blocks, an estimate",
        ),
    ] {
        if !output.measured_total.iter().any(|a| a.unit == unit) {
            continue;
        }
        println!();
        println!("storage, byte-hours ({label})");
        println!("  {:<24}  {:>16}  {:>16}", "product", "kept", "charged");
        for p in &output.products {
            println!(
                "  {:<24}  {:>16}  {:>16}",
                p.product,
                amount_cell(&p.kept, unit),
                amount_cell(&p.charged, unit)
            );
        }
        println!(
            "  {:<24}  {:>16}  {:>16}",
            "<unallocated>",
            "",
            amount_cell(&output.unallocated, unit)
        );
        println!(
            "  {:<24}  {:>16}  {:>16}",
            "measured total",
            "",
            amount_cell(&output.measured_total, unit)
        );
    }
    for (unit, label) in [
        (
            CostUnit::EgressBytes,
            "egress, bytes (as adapters reported it)",
        ),
        (
            CostUnit::AiInputTokens,
            "AI input tokens (calls recorded in this state store)",
        ),
        (
            CostUnit::AiOutputTokens,
            "AI output tokens (calls recorded in this state store)",
        ),
    ] {
        if !output.measured_total.iter().any(|a| a.unit == unit) {
            continue;
        }
        println!();
        println!("{label}");
        println!("  {:<24}  {:>16}", "product", "charged");
        for p in &output.products {
            println!("  {:<24}  {:>16}", p.product, amount_cell(&p.charged, unit));
        }
        println!(
            "  {:<24}  {:>16}",
            "<unallocated>",
            amount_cell(&output.unallocated, unit)
        );
        println!(
            "  {:<24}  {:>16}",
            "measured total",
            amount_cell(&output.measured_total, unit)
        );
    }
    let part_status = |figure: MoneyFigure| {
        output
            .measured_total_money_parts
            .iter()
            .find(|p| p.figure == figure)
            .map_or(MoneyStatus::NotMeasured, |p| p.status)
    };
    let mut outside = Vec::new();
    if part_status(MoneyFigure::Egress) == MoneyStatus::NotMeasured {
        outside.push("egress is not measured");
    }
    match part_status(MoneyFigure::AiTokens) {
        MoneyStatus::NotMeasured => outside.push("no AI call was recorded"),
        MoneyStatus::Partial if output.history_complete => {
            outside.push("AI tokens are partial: recording began after the month began");
        }
        MoneyStatus::Priced | MoneyStatus::NoPrice | MoneyStatus::Partial => {}
    }
    println!();
    if outside.is_empty() {
        println!("money, USD (only from prices set in [cost])");
    } else {
        println!(
            "money, USD (only from prices set in [cost]; {}, and not in the total)",
            outside.join("; ")
        );
    }
    println!("  {:<24}  {:>14}  {:<12}", "product", "charged", "status");
    for p in &output.products {
        println!(
            "  {:<24}  {:>14}  {:<12}",
            p.product,
            money_cell(p.money),
            money_status_label(p.money_status)
        );
    }
    println!(
        "  {:<24}  {:>14}  {:<12}",
        "<unallocated>",
        money_cell(output.unallocated_money),
        money_status_label(output.unallocated_money_status)
    );
    println!(
        "  {:<24}  {:>14}  {:<12}",
        "measured total",
        money_cell(output.measured_total_money),
        money_status_label(output.measured_total_money_status)
    );
    if output.measured_total_money_status == MoneyStatus::NoPrice {
        println!(
            "  to price this view, set [cost] {}; compute_cost_per_dbu does not price it, and 0 is a valid price",
            missing_price_keys(&output.measured_total_money_parts).join(", ")
        );
    }
    println!();
    println!(
        "not measured: {} failed executions with no recorded time, {} tables with unknown early storage",
        output.not_measured.failed_executions, output.not_measured.storage_unknown_tables
    );
    println!(
        "{} tables were read that name no model with a recorded build; their cost is not split",
        output.not_measured.unattributed_read_tables
    );
    if output.not_measured.egress_unreported_executions > 0 {
        println!(
            "{} production executions reported no egress; the egress figure is a floor",
            output.not_measured.egress_unreported_executions
        );
    }
    if output.not_measured.ai_calls_without_usage > 0 {
        println!(
            "{} AI calls reported no token counts and are not counted",
            output.not_measured.ai_calls_without_usage
        );
    }
    if output.not_measured.ai_records_dropped > 0 {
        println!(
            "{} AI usage records could not be read and are not counted; the AI figure is a floor",
            output.not_measured.ai_records_dropped
        );
    }
    if output
        .products
        .iter()
        .flat_map(|p| &p.allocated_upstream)
        .any(|a| a.provisional)
    {
        println!("some shares are provisional: a later reader can still change them");
    }
}

/// Execute `rocky cost --month YYYY-MM --by product`.
pub fn run_month_cost(
    state_path: &Path,
    config_path: &Path,
    month: &str,
    json: bool,
) -> Result<()> {
    let output = compute_month_cost(state_path, config_path, month, Utc::now())?;
    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_month_table(&output);
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    use chrono::{TimeZone, Utc};
    use tempfile::TempDir;

    /// A pipeline whose models directory does not exist adds a warning to
    /// the month report, unless it sets `allow_empty_models = true`.
    #[test]
    fn a_missing_models_dir_warns_unless_allowed() {
        for allow in [false, true] {
            let dir = TempDir::new().unwrap();
            let config_path = dir.path().join("rocky.toml");
            std::fs::write(
                &config_path,
                format!(
                    "[adapter]\ntype = \"duckdb\"\npath = \"w.duckdb\"\n\n\
                     [pipeline.silver]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                     allow_empty_models = {allow}\n\n\
                     [pipeline.silver.target.governance]\nauto_create_schemas = true\n"
                ),
            )
            .unwrap();
            let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
            let mut warnings = Vec::new();
            let children = declared_children(Some(&cfg), &config_path, &mut warnings);
            assert!(children.is_empty());
            if allow {
                assert!(warnings.is_empty(), "{warnings:?}");
            } else {
                assert_eq!(warnings.len(), 1, "{warnings:?}");
                assert!(warnings[0].contains("does not exist"), "{warnings:?}");
                assert!(warnings[0].contains("pipeline 'silver'"), "{warnings:?}");
                assert!(
                    warnings[0].contains("allow_empty_models = true"),
                    "{warnings:?}"
                );
            }
        }
    }

    fn sample_exec(
        name: &str,
        status: &str,
        duration_ms: u64,
        bytes_scanned: Option<u64>,
        bytes_written: Option<u64>,
    ) -> ModelExecution {
        sample_exec_tenant(
            name,
            status,
            duration_ms,
            bytes_scanned,
            bytes_written,
            None,
        )
    }

    fn sample_exec_tenant(
        name: &str,
        status: &str,
        duration_ms: u64,
        bytes_scanned: Option<u64>,
        bytes_written: Option<u64>,
        tenant: Option<&str>,
    ) -> ModelExecution {
        let now = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        ModelExecution {
            row_count: None,
            upstream_reads: None,
            model_name: name.to_string(),
            started_at: now,
            finished_at: now + chrono::Duration::milliseconds(duration_ms as i64),
            duration_ms,
            rows_affected: Some(100),
            status: status.to_string(),
            sql_hash: format!("hash_{name}"),
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned,
            bytes_written,
            egress_bytes: None,
            tenant: tenant.map(str::to_string),
            recipe_hash: None,
            input_hash: None,
            input_proof_class: None,
            env_hash: None,
            hash_scheme: None,
            output_column_hashes: None,
            attempts: Vec::new(),
            output_version: None,
            stored_bytes: None,
            output_target: None,
            due_at: None,
            wait_for_upstream_ms: None,
        }
    }

    fn sample_run(run_id: &str, models: Vec<ModelExecution>) -> RunRecord {
        sample_run_at(
            run_id,
            Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap(),
            models,
        )
    }

    fn sample_run_at(
        run_id: &str,
        started: chrono::DateTime<Utc>,
        models: Vec<ModelExecution>,
    ) -> RunRecord {
        let finished = started + chrono::Duration::milliseconds(10_000);
        RunRecord {
            run_id: run_id.to_string(),
            started_at: started,
            finished_at: finished,
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "cost-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            check_gate_failed: false,
            verify_after_failed: false,
            rocky_branch: None,
            run_scope: Some(rocky_core::state::RunScope::Production),
            queued_ms: None,
            source_volumes: Vec::new(),
            output_volumes: Vec::new(),
            hold: None,
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

        let out = build_output(&record, Some(&adapter), None, None);

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

        let out = build_output(&record, None, None, None);

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

    /// Recorded stored bytes reach the per-model row with their basis; an
    /// unknown stays unknown and is omitted from JSON.
    #[test]
    fn per_model_reports_recorded_stored_bytes_with_basis() {
        let mut measured = sample_exec("orders", "success", 10, None, None);
        measured.stored_bytes = Some(rocky_core::state::StoredBytes {
            bytes: 524_288,
            basis: StoredBytesBasis::AllocatedBlocks,
        });
        let unknown = sample_exec("v", "success", 10, None, None);
        let record = sample_run("run-sb", vec![measured, unknown]);

        let out = build_output(&record, None, None, None);

        assert_eq!(out.per_model[0].stored_bytes, Some(524_288));
        assert_eq!(
            out.per_model[0].stored_bytes_basis,
            Some(StoredBytesBasis::AllocatedBlocks)
        );
        assert_eq!(out.per_model[1].stored_bytes, None);
        assert_eq!(out.per_model[1].stored_bytes_basis, None);
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["per_model"][0]["stored_bytes"], 524_288);
        assert_eq!(
            json["per_model"][0]["stored_bytes_basis"],
            "allocated_blocks"
        );
        assert!(json["per_model"][1].get("stored_bytes").is_none());
        assert_eq!(
            stored_bytes_cell(Some(524_288), Some(StoredBytesBasis::AllocatedBlocks)),
            "524288 (est)"
        );
        assert_eq!(
            stored_bytes_cell(Some(10), Some(StoredBytesBasis::DataFiles)),
            "10"
        );
        assert_eq!(stored_bytes_cell(None, None), "-");
    }

    /// Recorded upstream reads reach the per-model row; an unmeasured model
    /// has no `upstream_reads` key.
    #[test]
    fn per_model_reports_recorded_upstream_reads() {
        let mut measured = sample_exec("orders", "success", 10, None, None);
        measured.upstream_reads = Some(rocky_core::state::UpstreamReads {
            reads: vec![rocky_core::state::UpstreamRead {
                table: "memory.raw.events".into(),
                rows_scanned: 5000,
                projected_columns: vec!["id".into(), "amount".into()],
                est_bytes: Some(80_000),
            }],
            unattributed_scans: 0,
        });
        let unknown = sample_exec("v", "success", 10, None, None);
        let record = sample_run("run-ur", vec![measured, unknown]);

        let out = build_output(&record, None, None, None);

        let json = serde_json::to_value(&out).unwrap();
        let read = &json["per_model"][0]["upstream_reads"]["reads"][0];
        assert_eq!(read["table"], "memory.raw.events");
        assert_eq!(read["rows_scanned"], 5000);
        assert_eq!(read["est_bytes"], 80_000);
        assert!(json["per_model"][1].get("upstream_reads").is_none());
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
        let out = build_output(&record, Some(&adapter), None, None);

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

        let out = build_output(&record, None, Some("orders"), None);
        assert_eq!(out.per_model.len(), 1);
        assert_eq!(out.per_model[0].model_name, "orders");
        assert_eq!(out.total_duration_ms, 1_000);
    }

    #[test]
    fn no_grouping_leaves_groups_none() {
        let record = sample_run(
            "run-flat",
            vec![sample_exec("orders", "success", 1_000, None, None)],
        );
        let out = build_output(&record, None, None, None);
        assert!(out.groups.is_none(), "default cost output has no groups");
        // per_model is always present regardless of grouping.
        assert_eq!(out.per_model.len(), 1);
    }

    #[test]
    fn group_by_tenant_buckets_and_sums() {
        // Two tenants + one unattributed model. 60s on Medium @ $0.40/DBU.
        let record = sample_run(
            "run-tenant",
            vec![
                sample_exec_tenant("orders", "success", 30_000, Some(10), None, Some("acme")),
                sample_exec_tenant("returns", "success", 10_000, Some(5), None, Some("acme")),
                sample_exec_tenant("orders", "success", 20_000, Some(7), None, Some("globex")),
                // No tenant → lands in the "<unattributed>" bucket.
                sample_exec("internal_metrics", "success", 5_000, Some(3), None),
            ],
        );

        let dbu_per_hour = warehouse_size_to_dbu_per_hour("Medium");
        let adapter = (
            "databricks".to_string(),
            WarehouseType::Databricks,
            dbu_per_hour,
            0.40,
        );

        let out = build_output(&record, Some(&adapter), None, Some(CostGroupBy::Tenant));
        let groups = out.groups.expect("groups present under --by tenant");

        // Deterministic ordering: "<unattributed>" (angle bracket sorts
        // first), then "acme", then "globex".
        assert_eq!(
            groups.iter().map(|g| g.key.as_str()).collect::<Vec<_>>(),
            vec![UNATTRIBUTED, "acme", "globex"]
        );

        let acme = groups.iter().find(|g| g.key == "acme").unwrap();
        assert_eq!(acme.dimension, "tenant");
        assert_eq!(acme.model_count, 2);
        assert_eq!(acme.total_duration_ms, 40_000);
        assert_eq!(acme.total_bytes_scanned, Some(15));

        // Group cost equals the sum of its members' per-model costs.
        let acme_members_cost: f64 = out
            .per_model
            .iter()
            .filter(|m| m.tenant.as_deref() == Some("acme"))
            .map(|m| m.cost_usd.unwrap_or(0.0))
            .sum();
        assert!((acme.total_cost_usd.unwrap() - acme_members_cost).abs() < 1e-12);

        let unattr = groups.iter().find(|g| g.key == UNATTRIBUTED).unwrap();
        assert_eq!(unattr.model_count, 1);
        assert_eq!(unattr.total_duration_ms, 5_000);
    }

    #[test]
    fn group_by_model_collapses_repeated_model_names() {
        // Same model name across two executions collapses into one group.
        let record = sample_run(
            "run-model",
            vec![
                sample_exec("orders", "success", 1_000, Some(10), None),
                sample_exec("orders", "success", 2_000, Some(20), None),
                sample_exec("customers", "success", 500, None, None),
            ],
        );

        let out = build_output(&record, None, None, Some(CostGroupBy::Model));
        let groups = out.groups.expect("groups present under --by model");

        assert_eq!(groups.len(), 2);
        let orders = groups.iter().find(|g| g.key == "orders").unwrap();
        assert_eq!(orders.dimension, "model");
        assert_eq!(orders.model_count, 2);
        assert_eq!(orders.total_duration_ms, 3_000);
        assert_eq!(orders.total_bytes_scanned, Some(30));
    }

    #[test]
    fn group_by_parse_rejects_unknown_dimension() {
        assert!(CostGroupBy::parse("tenant").is_ok());
        assert!(CostGroupBy::parse("MODEL").is_ok());
        assert!(CostGroupBy::parse("region").is_err());
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

    /// The seam serves what the store recorded; `run_cost --output json` is
    /// `compute_cost` plus one `println!`, so this pins the producer both
    /// callers share. An absent config degrades to no adapter rather than
    /// failing the rollup; a config that is there but does not parse is an
    /// error, because a wrong price is worse than no price.
    #[test]
    fn compute_cost_reads_the_recorded_run() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            store
                .record_run(&sample_run(
                    "run-1",
                    vec![
                        sample_exec("m", "success", 10, Some(100), None),
                        sample_exec("n", "success", 5, None, None),
                    ],
                ))
                .unwrap();
        }
        let missing_config = dir.path().join("rocky.toml");

        let out = compute_cost(&path, &missing_config, "latest", None, None).unwrap();
        assert_eq!(out.command, "cost");
        assert_eq!(out.run_id, "run-1");
        assert_eq!(out.adapter_type, None, "no config, no adapter");
        assert_eq!(
            out.per_model
                .iter()
                .map(|m| m.model_name.as_str())
                .collect::<Vec<_>>(),
            vec!["m", "n"]
        );

        let one = compute_cost(&path, &missing_config, "run-1", Some("n"), None).unwrap();
        assert_eq!(one.per_model.len(), 1);

        let err = compute_cost(&path, &missing_config, "run-1", Some("zzz"), None).unwrap_err();
        assert!(
            err.to_string().contains("did not execute model 'zzz'"),
            "{err}"
        );

        let broken_config = dir.path().join("broken.toml");
        std::fs::write(&broken_config, "[adapter\ntype = ").unwrap();
        let err = compute_cost(&path, &broken_config, "latest", None, None).unwrap_err();
        assert!(
            format!("{err:#}").contains("failed to load config"),
            "a present config that does not parse is an error, not no adapter: {err:#}"
        );
    }

    /// `latest` is the run that STARTED last. The newer run sorts LAST by
    /// id, and the store iterates in key order, so neither key order nor
    /// table order can stand in for its timestamp: only a descending sort
    /// on `started_at` picks it.
    #[test]
    fn resolve_latest_picks_most_recent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let noon = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        store
            .record_run(&sample_run_at(
                "z-newer",
                noon + chrono::Duration::hours(1),
                vec![sample_exec("m", "success", 1, None, None)],
            ))
            .unwrap();
        store
            .record_run(&sample_run_at(
                "a-older",
                noon,
                vec![sample_exec("m", "success", 1, None, None)],
            ))
            .unwrap();

        let resolved = resolve(&store, "latest").unwrap();
        assert_eq!(resolved.run_id, "z-newer");
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
        assert!(
            err.to_string()
                .to_lowercase()
                .contains("no production runs")
        );
    }

    /// #2201: `latest` skips a newer shadow or branch run, reports a
    /// pre-#2200 run as `unrecorded`, and an explicit id still reaches a
    /// shadow run.
    #[test]
    fn resolve_latest_skips_shadow_and_branch_runs() {
        use rocky_core::state::RunScope;
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        let noon = Utc.with_ymd_and_hms(2026, 4, 21, 12, 0, 0).unwrap();
        let mut legacy = sample_run_at("legacy", noon, vec![]);
        legacy.run_scope = None;
        store.record_run(&legacy).unwrap();
        let mut shadow = sample_run_at("shadow", noon + chrono::Duration::hours(1), vec![]);
        shadow.run_scope = Some(RunScope::Shadow { schema: None });
        store.record_run(&shadow).unwrap();
        let mut branch = sample_run_at("branch", noon + chrono::Duration::hours(2), vec![]);
        branch.run_scope = Some(RunScope::Branch { name: "b".into() });
        store.record_run(&branch).unwrap();

        let latest = resolve(&store, "latest").unwrap();
        assert_eq!(latest.run_id, "legacy");
        let out = build_output(&latest, None, None, None);
        assert_eq!(out.run_scope, crate::output::RunScopeKind::Unrecorded);

        let named = resolve(&store, "shadow").unwrap();
        assert_eq!(
            build_output(&named, None, None, None).run_scope,
            crate::output::RunScopeKind::Shadow
        );

        // Only non-production runs: `latest` refuses rather than reporting one.
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        store.record_run(&shadow).unwrap();
        let err = resolve(&store, "latest").unwrap_err();
        assert!(err.to_string().contains("no production runs"), "{err}");
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
    // ------------------------------------------------------------------
    // The config leg (#1702). `rocky cost` used to swallow every load error
    // into "no dollar figures", which is also what a config-less project and
    // a non-billed source show.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1702: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// Loads under the credential-TOLERANT loader (#1536), refuses under the
    /// strict one: `${...}` is unset and sits in an adapter connection field.
    const UNSET_CREDENTIAL_CONFIG_1702: &str = "[adapters.wh]\ntype = \"databricks\"\n\
         host = \"${ROCKY_T_1702_UNSET}\"\n";

    /// A state store holding one run, so `run_cost` reaches the config leg.
    fn seed_one_run(dir: &TempDir) -> std::path::PathBuf {
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        store
            .record_run(&sample_run(
                "run-1",
                vec![sample_exec("m", "success", 1000, Some(10), None)],
            ))
            .unwrap();
        path
    }

    /// The CALLER refuses: `rocky cost latest` on a present-but-unloadable
    /// `rocky.toml` errors, naming the file. Restoring the swallow makes this
    /// fail — it returns `Ok` and prints a rollup with no dollar figures.
    #[test]
    fn cost_refuses_a_present_but_unloadable_config() {
        let dir = TempDir::new().unwrap();
        let state_path = seed_one_run(&dir);
        let cfg = dir.path().join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1702).unwrap();

        let err = run_cost(&state_path, &cfg, Some("latest"), None, None, None, true)
            .expect_err("a present but unloadable rocky.toml must refuse `rocky cost`");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains(&cfg.display().to_string()),
            "the refusal must name the config file: {rendered}"
        );
        assert!(
            rendered.contains("discovery-only") || rendered.contains("kind = \"discovery\""),
            "the loader's own error must survive: {rendered}"
        );
    }

    /// Honest failure (a): with NO `rocky.toml` the rollup still renders —
    /// durations and bytes come from the run record. The second half is the
    /// discriminator, so this cannot pass by the config leg being dropped.
    #[test]
    fn cost_still_reports_without_any_config() {
        let dir = TempDir::new().unwrap();
        let state_path = seed_one_run(&dir);
        let cfg = dir.path().join("rocky.toml");
        assert!(!cfg.exists());

        assert!(
            adapter_pricing(&cfg)
                .expect("absent is not invalid")
                .is_none(),
            "no config → no pricing parameters, as before"
        );
        run_cost(&state_path, &cfg, Some("latest"), None, None, None, true)
            .expect("a missing rocky.toml must not refuse `rocky cost`");

        std::fs::write(&cfg, BROKEN_CONFIG_1702).unwrap();
        assert!(
            adapter_pricing(&cfg).is_err(),
            "a present-but-broken config in the same project must refuse"
        );
    }

    /// Honest failure (b): a config holding an unset `${VAR}` in an adapter
    /// connection field now PRICES the run. Under the strict loader it fell
    /// into the same "no dollar figures" hole as a broken config.
    #[test]
    fn cost_prices_a_run_with_an_unset_credential_var() {
        let dir = TempDir::new().unwrap();
        let cfg = dir.path().join("rocky.toml");
        std::fs::write(&cfg, UNSET_CREDENTIAL_CONFIG_1702).unwrap();

        assert!(
            rocky_core::config::load_rocky_config(&cfg).is_err(),
            "fixture must fail the STRICT loader, or this proves nothing"
        );

        let pricing = adapter_pricing(&cfg)
            .expect("an unset ${VAR} in adapter credentials must not refuse")
            .expect("the billed warehouse must resolve");
        assert_eq!(pricing.0, "databricks");
        assert_eq!(pricing.1, WarehouseType::Databricks);

        // The claim is that the run is PRICED, so assert the dollar figure,
        // not just that the parameters were read.
        let record = sample_run(
            "run-1",
            vec![sample_exec("m", "success", 60_000, Some(1_000), None)],
        );
        let out = build_output(&record, Some(&pricing), None, None);
        let cost = out
            .total_cost_usd
            .expect("the rollup must carry a dollar figure");
        assert!(cost > 0.0, "priced run must cost something: {cost}");
    }

    // ----- `--month YYYY-MM --by product` -----

    use crate::commands::product::tests::api_fixture_project;
    use rocky_core::state::{RecordedTarget, UpstreamRead, UpstreamReads};

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, 0).unwrap()
    }

    /// An execution that started at `start`, ran `ms`, and wrote
    /// `main.<name>`.
    fn exec_at(name: &str, start: DateTime<Utc>, ms: u64) -> ModelExecution {
        let mut e = sample_exec(name, "success", ms, None, None);
        e.started_at = start;
        e.finished_at = start + chrono::Duration::milliseconds(ms as i64);
        e.output_target = Some(RecordedTarget {
            model: name.to_string(),
            catalog: String::new(),
            schema: "main".to_string(),
            table: name.to_string(),
        });
        e
    }

    fn reading(mut e: ModelExecution, table: &str, est_bytes: u64) -> ModelExecution {
        e.upstream_reads = Some(UpstreamReads {
            reads: vec![UpstreamRead {
                table: format!("memory.main.{table}"),
                rows_scanned: 10,
                projected_columns: Vec::new(),
                est_bytes: Some(est_bytes),
            }],
            unattributed_scans: 0,
        });
        e
    }

    fn amount(amounts: &[CostAmount], unit: CostUnit) -> u64 {
        amounts
            .iter()
            .find(|a| a.unit == unit)
            .map_or(0, |a| a.amount)
    }

    #[test]
    fn month_cost_charges_the_product_that_reads_an_unowned_table() {
        let dir = TempDir::new().unwrap();
        let (_root, config, state_path) = api_fixture_project(dir.path());
        {
            let store = StateStore::open(&state_path).unwrap();
            let start = at(2026, 10, 5, 2, 0);
            store
                .record_run(&sample_run_at(
                    "run-1",
                    start,
                    vec![
                        exec_at("raw", start, 1_000),
                        reading(
                            exec_at("revenue_daily", start + chrono::Duration::minutes(1), 400),
                            "raw",
                            10,
                        ),
                    ],
                ))
                .unwrap();
        }
        let out =
            compute_month_cost(&state_path, &config, "2026-10", at(2026, 11, 2, 0, 0)).unwrap();
        assert_eq!(out.command, "cost_by_product");
        assert_eq!(out.month, "2026-10");
        assert_eq!(out.timezone, "UTC");
        assert!(out.history_complete);
        assert_eq!(out.products.len(), 1);
        let product = &out.products[0];
        assert_eq!(product.product, "revenue_daily");
        assert_eq!(amount(&product.built, CostUnit::ComputeMs), 400);
        assert_eq!(amount(&product.kept, CostUnit::ComputeMs), 400);
        assert_eq!(product.allocated_upstream.len(), 1);
        assert_eq!(product.allocated_upstream[0].table, "raw");
        assert_eq!(product.allocated_upstream[0].amount, 1_000);
        assert_eq!(amount(&product.charged, CostUnit::ComputeMs), 1_400);
        assert_eq!(amount(&out.measured_total, CostUnit::ComputeMs), 1_400);
        assert!(out.unallocated.is_empty());

        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["command"], "cost_by_product");
        assert_eq!(json["products"][0]["charged"][0]["unit"], "compute_ms");
    }

    /// Two stores hold the same in-month rows. One keeps a year of history.
    /// In the other, retention swept to the newest three runs: the month then
    /// looks quiet, and the report must say its history is not complete
    /// rather than print the smaller number as if it were the month.
    #[test]
    fn month_cost_says_so_when_retention_may_have_swept_the_month() {
        let record_march = |state_path: &Path| {
            let store = StateStore::open(state_path).unwrap();
            for (day, id) in [(5, "a"), (10, "b"), (20, "c"), (28, "d")] {
                let start = at(2025, 3, day, 2, 0);
                store
                    .record_run(&sample_run_at(
                        id,
                        start,
                        vec![exec_at("revenue_daily", start, 100)],
                    ))
                    .unwrap();
            }
            store
        };
        let now = at(2025, 4, 25, 0, 0);

        let full_dir = TempDir::new().unwrap();
        let (_root, full_config, full_state) = api_fixture_project(full_dir.path());
        drop(record_march(&full_state));
        let full = compute_month_cost(&full_state, &full_config, "2025-03", now).unwrap();
        assert!(full.history_complete, "365 days reach back to March");
        assert_eq!(amount(&full.measured_total, CostUnit::ComputeMs), 400);

        let swept_dir = TempDir::new().unwrap();
        let (_root, swept_config, swept_state) = api_fixture_project(swept_dir.path());
        let mut config_text = std::fs::read_to_string(&swept_config).unwrap();
        config_text.push_str("\n[state.retention]\nmax_age_days = 20\nmin_runs_kept = 3\n");
        std::fs::write(&swept_config, config_text).unwrap();
        {
            let store = record_march(&swept_state);
            let cfg = load_optional_project_config(Some(&swept_config))
                .unwrap()
                .unwrap();
            store.sweep_retention(&cfg.state.retention).unwrap();
            assert_eq!(
                store.list_runs(100).unwrap().len(),
                3,
                "the sweep leaves the newest three runs"
            );
        }
        let swept = compute_month_cost(&swept_state, &swept_config, "2025-03", now).unwrap();
        assert!(
            !swept.history_complete,
            "20 days of history cannot cover a month that began 55 days ago"
        );
        assert_eq!(
            amount(&swept.measured_total, CostUnit::ComputeMs),
            300,
            "three runs survive and look like a quiet month"
        );
    }

    #[test]
    fn two_specs_naming_one_output_model_warn_instead_of_dropping_one() {
        let dir = TempDir::new().unwrap();
        let (root, _config, _state) = api_fixture_project(dir.path());
        let other = "[product]\nname = \"other\"\nintent = \"a second owner\"\n\n\
                     [product.source]\ntables = [\"c.s.t\"]\n\n\
                     [product.output]\nmodel = \"revenue_daily\"\ngrain = [\"id\"]\n\
                     columns = [ { name = \"id\", type = \"Int64\", nullable = false } ]\n\n\
                     [product.trust]\nagent = \"propose_only\"\n";
        std::fs::write(root.join("products/other.toml"), other).unwrap();
        let (owners, problems) = crate::commands::product::product_owners_in(&root);
        assert_eq!(owners.len(), 1, "one owner keeps the model");
        assert!(
            problems
                .iter()
                .any(|p| p.contains("both name output model revenue_daily")),
            "{problems:?}"
        );
    }

    /// A table built before the month and only read in it still has its
    /// storage in the report, because the model graph names it.
    #[test]
    fn month_cost_finds_the_carry_in_of_a_table_only_the_graph_names() {
        use rocky_core::state::{StoredBytes, StoredBytesBasis};
        let dir = TempDir::new().unwrap();
        let (root, config, state_path) = api_fixture_project(dir.path());
        let sidecar = |deps: &str, table: &str| {
            format!(
                "{deps}[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"main\"\ntable = \"{table}\"\n"
            )
        };
        let models = root.join("models");
        std::fs::write(models.join("raw_t.sql"), "SELECT 1 AS id\n").unwrap();
        std::fs::write(models.join("raw_t.toml"), sidecar("", "raw_t")).unwrap();
        std::fs::write(models.join("revenue_daily.sql"), "SELECT id FROM raw_t\n").unwrap();
        std::fs::write(
            models.join("revenue_daily.toml"),
            sidecar("depends_on = [\"raw_t\"]\n\n", "revenue_daily"),
        )
        .unwrap();
        {
            let store = StateStore::open(&state_path).unwrap();
            let mut raw = exec_at("raw_t", at(2026, 9, 20, 0, 0), 5);
            raw.stored_bytes = Some(StoredBytes {
                bytes: 3_600_000,
                basis: StoredBytesBasis::DataFiles,
            });
            store
                .record_run(&sample_run_at("sep", at(2026, 9, 20, 0, 0), vec![raw]))
                .unwrap();
            let start = at(2026, 10, 5, 2, 0);
            store
                .record_run(&sample_run_at(
                    "oct",
                    start,
                    vec![reading(exec_at("revenue_daily", start, 10), "raw_t", 10)],
                ))
                .unwrap();
        }
        let out =
            compute_month_cost(&state_path, &config, "2026-10", at(2026, 11, 2, 0, 0)).unwrap();
        assert!(out.warnings.is_empty(), "{:?}", out.warnings);
        // 31 days * 24 h * 3,600,000 bytes.
        assert_eq!(
            amount(&out.measured_total, CostUnit::StorageByteHoursDataFiles),
            744 * 3_600_000
        );
        assert_eq!(out.not_measured.unattributed_read_tables, 0);
    }

    #[test]
    fn month_cost_arguments_are_checked() {
        let dir = TempDir::new().unwrap();
        let state = dir.path().join("state.redb");
        let cfg = dir.path().join("rocky.toml");
        let go = |target, by, month| run_cost(&state, &cfg, target, None, by, month, true);

        let msg = |r: Result<()>| format!("{:#}", r.unwrap_err());
        assert!(msg(go(None, None, Some("2026-10"))).contains("--month needs --by product"));
        assert!(
            msg(go(Some("latest"), Some("product"), Some("2026-10")))
                .contains("cannot be combined")
        );
        assert!(msg(go(None, Some("product"), None)).contains("--by product needs --month"));
        assert!(msg(go(None, None, None)).contains("pass a run id"));
        assert!(msg(go(None, Some("product"), Some("2026-13"))).contains("YYYY-MM"));
        assert!(msg(go(None, Some("product"), Some("26-10"))).contains("YYYY-MM"));
    }

    #[test]
    fn a_figure_wider_than_u64_is_refused() {
        let unit = cost_split::Unit::ALL[0];
        let mut split = cost_split::MonthSplit::default();
        split.measured_total.insert(unit, u128::from(u64::MAX));
        assert!(check_fits_u64(&split).is_ok());
        split.unallocated.insert(unit, u128::from(u64::MAX) + 1);
        assert!(check_fits_u64(&split).is_err());
    }

    // ----- money in the monthly report -----

    use std::path::PathBuf;

    /// October 2026: `raw` (no product) runs 1,000 ms and `revenue_daily`
    /// reads it for 400 ms, then keeps 1 GiB of data files stored from
    /// 2026-10-16 to the end of the month.
    fn money_project(dir: &Path, cost: &str) -> (PathBuf, PathBuf) {
        use rocky_core::state::{StoredBytes, StoredBytesBasis};
        let (_root, config, state_path) = api_fixture_project(dir);
        let mut text = std::fs::read_to_string(&config).unwrap();
        text.push_str(cost);
        std::fs::write(&config, text).unwrap();
        let store = StateStore::open(&state_path).unwrap();
        let start = at(2026, 10, 16, 0, 0);
        let mut product = reading(
            exec_at("revenue_daily", start + chrono::Duration::minutes(1), 400),
            "raw",
            10,
        );
        product.stored_bytes = Some(StoredBytes {
            bytes: 1 << 30,
            basis: StoredBytesBasis::DataFiles,
        });
        store
            .record_run(&sample_run_at(
                "run-1",
                start,
                vec![exec_at("raw", start, 1_000), product],
            ))
            .unwrap();
        (config, state_path)
    }

    fn month(config: &Path, state_path: &Path) -> ProductMonthCostOutput {
        compute_month_cost(state_path, config, "2026-10", at(2026, 11, 2, 0, 0)).unwrap()
    }

    fn part(parts: &[MoneyPart], figure: MoneyFigure) -> &MoneyPart {
        parts
            .iter()
            .find(|p| p.figure == figure)
            .unwrap_or_else(|| panic!("no {figure:?} part in {parts:?}"))
    }

    #[test]
    fn month_money_is_priced_from_the_user_prices() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            "\n[cost]\ncompute_cost_per_machine_hour = 3.6\nstorage_cost_per_gb_month = 7.44\n",
        );
        let out = month(&config, &state);
        let p = &out.products[0];
        assert_eq!(p.money_status, MoneyStatus::Priced);
        // 1,400 ms at $3.60 per machine-hour.
        let compute = part(&p.money_parts, MoneyFigure::Compute);
        assert_eq!(compute.status, MoneyStatus::Priced);
        assert!((compute.usd.unwrap() - 0.0014).abs() < 1e-12, "{compute:?}");
        // Byte-hours over (1 GiB x 744 hours), at $7.44 per GB-month.
        let byte_hours = amount(&p.charged, CostUnit::StorageByteHoursDataFiles);
        assert!(byte_hours > 0, "the fixture must store data");
        let storage = part(&p.money_parts, MoneyFigure::StorageDataFiles);
        let expected = byte_hours as f64 / (1_073_741_824.0 * 744.0) * 7.44;
        assert!(
            (storage.usd.unwrap() - expected).abs() < 1e-12,
            "{storage:?}"
        );
        assert!(
            (p.money.unwrap() - (0.0014 + expected)).abs() < 1e-12,
            "{:?}",
            p.money
        );
        // The month never measured allocated blocks, so it has no part.
        assert!(
            !p.money_parts
                .iter()
                .any(|part| part.figure == MoneyFigure::StorageAllocatedBlocks)
        );
        assert_eq!(out.measured_total_money_status, MoneyStatus::Priced);
        assert!((out.measured_total_money.unwrap() - p.money.unwrap()).abs() < 1e-12);
        // Nothing is unallocated: $0, priced.
        assert_eq!(out.unallocated_money_status, MoneyStatus::Priced);
        assert_eq!(out.unallocated_money, Some(0.0));
    }

    #[test]
    fn month_money_without_prices_is_no_price_not_the_shipped_default() {
        let dir = TempDir::new().unwrap();
        // `compute_cost_per_dbu` is a DBU price, not a machine-hour price,
        // and no shipped default stands in for either.
        let (config, state) = money_project(dir.path(), "\n[cost]\ncompute_cost_per_dbu = 0.40\n");
        let out = month(&config, &state);
        let p = &out.products[0];
        assert_eq!(p.money_status, MoneyStatus::NoPrice);
        assert_eq!(p.money, None);
        for figure in [MoneyFigure::Compute, MoneyFigure::StorageDataFiles] {
            let part = part(&p.money_parts, figure);
            assert_eq!(part.status, MoneyStatus::NoPrice, "{part:?}");
            assert_eq!(part.usd, None);
        }
        assert_eq!(out.measured_total_money_status, MoneyStatus::NoPrice);
        assert_eq!(out.measured_total_money, None);
    }

    /// `MonthPricing` on its own: both storage bases are priced and added,
    /// the GB-month spreads over the hours of the month being reported
    /// (November has 720), a zero amount is $0 without a price, and a
    /// missing compute price holds back the total even when storage is
    /// priced.
    #[test]
    fn month_pricing_prices_each_basis_over_the_hours_of_the_month() {
        let window = cost_split::month_bounds_in_zone(2026, 11, "UTC").unwrap();
        let gib_month = 1_073_741_824_u64 * 720;
        let measured = [
            CostAmount {
                unit: CostUnit::ComputeMs,
                amount: 3_600_000,
            },
            CostAmount {
                unit: CostUnit::StorageByteHoursDataFiles,
                amount: gib_month,
            },
            CostAmount {
                unit: CostUnit::StorageByteHoursAllocatedBlocks,
                amount: 2 * gib_month,
            },
        ];
        let cost = |toml_text: &str| -> rocky_core::config::CostSection {
            toml::from_str(toml_text).unwrap()
        };

        let both = cost("compute_cost_per_machine_hour = 2.0\nstorage_cost_per_gb_month = 0.5\n");
        let pricing = MonthPricing::new(Some(&both), &window, true, &measured, false, false, None);
        let (usd, status, parts) = pricing.price(&measured);
        assert_eq!(status, MoneyStatus::Priced);
        let usd_of = |figure| part(&parts, figure).usd.unwrap();
        assert!((usd_of(MoneyFigure::Compute) - 2.0).abs() < 1e-12);
        assert!((usd_of(MoneyFigure::StorageDataFiles) - 0.5).abs() < 1e-12);
        assert!((usd_of(MoneyFigure::StorageAllocatedBlocks) - 1.0).abs() < 1e-12);
        assert!((usd.unwrap() - 3.5).abs() < 1e-12, "{usd:?}");

        // Storage priced, compute not: no total.
        let storage_only = cost("storage_cost_per_gb_month = 0.5\n");
        let pricing = MonthPricing::new(
            Some(&storage_only),
            &window,
            true,
            &measured,
            false,
            false,
            None,
        );
        let (usd, status, parts) = pricing.price(&measured);
        assert_eq!(status, MoneyStatus::NoPrice);
        assert_eq!(usd, None);
        assert_eq!(
            part(&parts, MoneyFigure::Compute).status,
            MoneyStatus::NoPrice
        );
        assert_eq!(
            part(&parts, MoneyFigure::StorageDataFiles).status,
            MoneyStatus::Priced
        );

        // No prices at all, and nothing charged: $0, priced. With no config
        // the same holds.
        for section in [Some(cost("")), None] {
            let pricing = MonthPricing::new(
                section.as_ref(),
                &window,
                true,
                &measured,
                false,
                false,
                None,
            );
            let (usd, status, parts) = pricing.price(&[]);
            assert_eq!(status, MoneyStatus::Priced);
            assert_eq!(usd, Some(0.0));
            assert_eq!(parts.len(), 5, "{parts:?}");
            // The same config with a non-zero amount is no_price.
            let (usd, status, _) = pricing.price(&measured);
            assert_eq!((usd, status), (None, MoneyStatus::NoPrice));
        }
    }

    #[test]
    fn month_money_with_one_price_missing_has_no_total() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            "\n[cost]\ncompute_cost_per_machine_hour = 3.6\n",
        );
        let out = month(&config, &state);
        let p = &out.products[0];
        assert_eq!(p.money_status, MoneyStatus::NoPrice);
        assert_eq!(p.money, None, "a total without storage would understate");
        let compute = part(&p.money_parts, MoneyFigure::Compute);
        assert_eq!(compute.status, MoneyStatus::Priced);
        assert!(compute.usd.is_some());
        let storage = part(&p.money_parts, MoneyFigure::StorageDataFiles);
        assert_eq!(storage.status, MoneyStatus::NoPrice);
    }

    #[test]
    fn month_money_lists_egress_and_ai_tokens_as_not_measured() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            "\n[cost]\ncompute_cost_per_machine_hour = 3.6\nstorage_cost_per_gb_month = 1.0\n\
             cross_region_transfer_cost_per_gb = 0.02\nai_cost_per_million_tokens = 3.0\n",
        );
        // A store that recorded the whole month, with no AI call and no
        // egress reported: both are not measured.
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        let out = month(&config, &state);
        for parts in [
            &out.products[0].money_parts,
            &out.measured_total_money_parts,
            &out.unallocated_money_parts,
        ] {
            for figure in [MoneyFigure::Egress, MoneyFigure::AiTokens] {
                let part = part(parts, figure);
                assert_eq!(part.status, MoneyStatus::NotMeasured);
                assert_eq!(part.usd, None, "not measured is never $0");
            }
        }
        // Not measured figures do not hold back a priced total.
        assert_eq!(out.products[0].money_status, MoneyStatus::Priced);
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["products"][0]["money_status"], "priced");
        let egress = json["products"][0]["money_parts"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["figure"] == "egress")
            .unwrap()
            .clone();
        assert_eq!(egress["status"], "not_measured");
        assert!(egress["usd"].is_null());
    }

    // ----- AI tokens and egress in the monthly report -----

    use rocky_core::ai_usage::AiUsageRecord;

    const ALL_PRICES: &str = "\n[cost]\ncompute_cost_per_machine_hour = 3.6\n\
         storage_cost_per_gb_month = 7.44\ncross_region_transfer_cost_per_gb = 0.5\n\
         ai_cost_per_million_tokens = 2.0\n";

    fn stamp_ai_since(state_path: &Path, since: DateTime<Utc>) {
        StateStore::open(state_path)
            .unwrap()
            .stamp_ai_usage_since(since)
            .unwrap();
    }

    fn ai_call(
        id: &str,
        when: DateTime<Utc>,
        model: Option<&str>,
        tokens: Option<(u64, u64)>,
    ) -> AiUsageRecord {
        AiUsageRecord {
            id: id.to_string(),
            at: when,
            command: "ai_explain".to_string(),
            model: model.map(str::to_string),
            llm_model: "claude-sonnet-4-6".to_string(),
            input_tokens: tokens.map(|t| t.0),
            output_tokens: tokens.map(|t| t.1),
        }
    }

    fn record_ai(state_path: &Path, calls: &[AiUsageRecord]) {
        StateStore::open(state_path)
            .unwrap()
            .record_ai_usage(calls)
            .unwrap();
    }

    /// Calls in October: two about the owned model `revenue_daily`, one of
    /// them with no usage; one about unowned `raw`; one about no model. One
    /// call in September and one in November are outside the month.
    fn october_ai_calls() -> Vec<AiUsageRecord> {
        let oct = |d| at(2026, 10, d, 9, 0);
        vec![
            ai_call("owned", oct(3), Some("revenue_daily"), Some((100, 50))),
            ai_call("owned-no-usage", oct(4), Some("revenue_daily"), None),
            ai_call("unowned", oct(5), Some("raw"), Some((1_000, 500))),
            ai_call("no-model", oct(6), None, Some((10, 5))),
            ai_call(
                "sept",
                at(2026, 9, 30, 23, 0),
                Some("revenue_daily"),
                Some((7, 7)),
            ),
            ai_call(
                "nov",
                at(2026, 11, 1, 1, 0),
                Some("revenue_daily"),
                Some((9, 9)),
            ),
        ]
    }

    fn amounts_of(parts: &[CostAmount]) -> (u64, u64) {
        (
            amount(parts, CostUnit::AiInputTokens),
            amount(parts, CostUnit::AiOutputTokens),
        )
    }

    /// An owned model's calls go to its product, every other call to
    /// unallocated, a call without usage is only counted, and the money is
    /// the blended price over both token units.
    #[test]
    fn ai_tokens_are_charged_to_the_owning_product_else_unallocated() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);

        let product = budget_of(&out, "revenue_daily");
        assert_eq!(amounts_of(&product.charged), (100, 50));
        assert_eq!(amounts_of(&out.unallocated), (1_010, 505));
        assert_eq!(amounts_of(&out.measured_total), (1_110, 555));
        assert_eq!(out.not_measured.ai_calls_without_usage, 1);

        let money = |parts: &[MoneyPart]| {
            let part = part(parts, MoneyFigure::AiTokens);
            assert_eq!(part.status, MoneyStatus::Priced);
            part.usd.unwrap()
        };
        assert!((money(&product.money_parts) - 150.0 / 1e6 * 2.0).abs() < 1e-12);
        assert!((money(&out.unallocated_money_parts) - 1_515.0 / 1e6 * 2.0).abs() < 1e-12);
        assert!((money(&out.measured_total_money_parts) - 1_665.0 / 1e6 * 2.0).abs() < 1e-12);
        // The AI money is inside the total.
        assert_eq!(out.measured_total_money_status, MoneyStatus::Priced);
        let parts_sum: f64 = out
            .measured_total_money_parts
            .iter()
            .filter(|p| p.status == MoneyStatus::Priced)
            .filter_map(|p| p.usd)
            .sum();
        assert!((out.measured_total_money.unwrap() - parts_sum).abs() < 1e-12);

        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["not_measured"]["ai_calls_without_usage"], 1);
        let units: Vec<&str> = json["measured_total"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["unit"].as_str().unwrap())
            .collect();
        assert!(units.contains(&"ai_input_tokens"), "{units:?}");
        assert!(units.contains(&"ai_output_tokens"), "{units:?}");
    }

    /// The report reads the spool next to the state file as well as the
    /// table, and a call in both counts once.
    #[test]
    fn the_report_reads_the_spool_and_counts_a_call_once() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        let calls = october_ai_calls();
        record_ai(&state, &calls[..1]);
        // `owned` is in both; `unowned` only in the spool.
        rocky_core::ai_usage::append_to_spool(
            &rocky_core::ai_usage::spool_path(&state),
            &[calls[0].clone(), calls[2].clone()],
        )
        .unwrap();
        let out = month(&config, &state);
        assert_eq!(
            amounts_of(&budget_of(&out, "revenue_daily").charged),
            (100, 50)
        );
        assert_eq!(amounts_of(&out.unallocated), (1_000, 500));
    }

    /// Recorded AI usage with no price is `no_price`, and the figure that
    /// holds the total back is named by its `[cost]` key. A price of 0 is a
    /// valid explicit price.
    #[test]
    fn recorded_ai_usage_with_no_price_gives_no_price_and_zero_is_a_price() {
        let priced_but_ai = "\n[cost]\ncompute_cost_per_machine_hour = 3.6\n\
             storage_cost_per_gb_month = 7.44\n";
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), priced_but_ai);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);
        let product = budget_of(&out, "revenue_daily");
        assert_eq!(product.money_status, MoneyStatus::NoPrice);
        assert_eq!(product.money, None, "a total without AI would understate");
        assert_eq!(
            part(&product.money_parts, MoneyFigure::AiTokens).status,
            MoneyStatus::NoPrice
        );
        assert_eq!(out.measured_total_money_status, MoneyStatus::NoPrice);
        assert_eq!(
            missing_price_keys(&out.measured_total_money_parts),
            ["ai_cost_per_million_tokens"]
        );
        // The other figures are still priced.
        assert_eq!(
            part(&product.money_parts, MoneyFigure::Compute).status,
            MoneyStatus::Priced
        );

        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            &format!("{priced_but_ai}ai_cost_per_million_tokens = 0\n"),
        );
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);
        let product = budget_of(&out, "revenue_daily");
        assert_eq!(product.money_status, MoneyStatus::Priced);
        let ai = part(&product.money_parts, MoneyFigure::AiTokens);
        assert_eq!((ai.status, ai.usd), (MoneyStatus::Priced, Some(0.0)));
    }

    /// A month that began before the store started recording AI calls has
    /// calls the store never saw: its AI part is partial, outside the total,
    /// with or without recorded calls. Beginning exactly with the month is
    /// complete.
    #[test]
    fn a_month_before_ai_recording_began_reports_ai_as_partial() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        // The store was created just now: October is before the stamp.
        let out = month(&config, &state);
        for parts in [
            &out.products[0].money_parts,
            &out.measured_total_money_parts,
            &out.unallocated_money_parts,
        ] {
            let ai = part(parts, MoneyFigure::AiTokens);
            assert_eq!(ai.status, MoneyStatus::Partial, "no calls recorded");
            assert_eq!(ai.usd, None);
        }
        // Partial AI stays outside the total: the rest still prices.
        assert_eq!(out.products[0].money_status, MoneyStatus::Priced);

        // With calls recorded in the month the amounts show, the money does
        // not.
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);
        let product = budget_of(&out, "revenue_daily");
        assert_eq!(amounts_of(&product.charged), (100, 50));
        let ai = part(&product.money_parts, MoneyFigure::AiTokens);
        assert_eq!((ai.status, ai.usd), (MoneyStatus::Partial, None));
        // Recorded calls with a partial AI part: no total, on every row. A
        // total that left the unseen spend out would understate it, and the
        // `max_usd` check already refuses to judge on it.
        assert_eq!(product.money_status, MoneyStatus::Partial);
        assert_eq!(product.money, None);
        assert_eq!(out.measured_total_money_status, MoneyStatus::Partial);
        assert_eq!(out.measured_total_money, None);
        assert_eq!(out.unallocated_money_status, MoneyStatus::Partial);
        assert_eq!(out.unallocated_money, None);
        // The priced parts are still listed.
        assert_eq!(
            part(&product.money_parts, MoneyFigure::Compute).status,
            MoneyStatus::Priced
        );

        // Recording began exactly at the start of the month: complete.
        stamp_ai_since(&state, at(2026, 10, 1, 0, 0));
        let out = month(&config, &state);
        assert_eq!(
            part(&out.products[0].money_parts, MoneyFigure::AiTokens).status,
            MoneyStatus::Priced
        );
        assert_eq!(out.products[0].money_status, MoneyStatus::Priced);
        // One second later: partial.
        stamp_ai_since(&state, at(2026, 10, 1, 0, 0) + chrono::Duration::seconds(1));
        let out = month(&config, &state);
        assert_eq!(
            part(&out.products[0].money_parts, MoneyFigure::AiTokens).status,
            MoneyStatus::Partial
        );
    }

    /// Run 2 adds an execution that reports 2 GiB of egress. The other three
    /// production executions report none.
    fn add_egress_run(state_path: &Path) {
        let start = at(2026, 10, 20, 0, 0);
        let mut raw = exec_at("raw", start, 1_000);
        raw.egress_bytes = Some(2 * (1 << 30));
        record(
            state_path,
            &[sample_run_at(
                "run-2",
                start,
                vec![
                    raw,
                    reading(
                        exec_at("revenue_daily", start + chrono::Duration::minutes(1), 400),
                        "raw",
                        10,
                    ),
                ],
            )],
        );
    }

    /// Some executions report egress and some do not: the reported sum is
    /// priced, and the rest are counted.
    #[test]
    fn partially_reported_egress_prices_the_reported_sum_and_counts_the_rest() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        add_egress_run(&state);
        let out = month(&config, &state);
        assert_eq!(amount(&out.measured_total, CostUnit::EgressBytes), 2 << 30);
        assert_eq!(out.not_measured.egress_unreported_executions, 3);
        let egress = part(&out.measured_total_money_parts, MoneyFigure::Egress);
        assert_eq!(egress.status, MoneyStatus::Priced);
        assert!((egress.usd.unwrap() - 1.0).abs() < 1e-12, "2 GiB * 0.5");
        // The egress money is inside the total.
        let parts_sum: f64 = out
            .measured_total_money_parts
            .iter()
            .filter_map(|p| p.usd)
            .sum();
        assert!((out.measured_total_money.unwrap() - parts_sum).abs() < 1e-12);
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["not_measured"]["egress_unreported_executions"], 3);
        // Every byte reported is somewhere: on a product or unallocated.
        let on_products: u64 = out
            .products
            .iter()
            .map(|p| amount(&p.charged, CostUnit::EgressBytes))
            .sum();
        assert_eq!(
            on_products + amount(&out.unallocated, CostUnit::EgressBytes),
            2 << 30
        );
    }

    /// Reported egress with no price is `no_price` and names the key; 0 is a
    /// valid price. Nothing reported is `not_measured` with nothing counted.
    #[test]
    fn egress_with_no_price_gives_no_price_and_zero_is_a_price() {
        let no_egress_price = "\n[cost]\ncompute_cost_per_machine_hour = 3.6\n\
             storage_cost_per_gb_month = 7.44\nai_cost_per_million_tokens = 2.0\n";
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), no_egress_price);
        add_egress_run(&state);
        let out = month(&config, &state);
        assert_eq!(out.measured_total_money_status, MoneyStatus::NoPrice);
        assert_eq!(out.measured_total_money, None);
        assert_eq!(
            missing_price_keys(&out.measured_total_money_parts),
            ["cross_region_transfer_cost_per_gb"]
        );

        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            &format!("{no_egress_price}cross_region_transfer_cost_per_gb = 0\n"),
        );
        add_egress_run(&state);
        let out = month(&config, &state);
        assert_eq!(out.measured_total_money_status, MoneyStatus::Priced);
        let egress = part(&out.measured_total_money_parts, MoneyFigure::Egress);
        assert_eq!(
            (egress.status, egress.usd),
            (MoneyStatus::Priced, Some(0.0))
        );

        // No execution reports egress: not measured, and no execution is
        // counted as unreported.
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        let out = month(&config, &state);
        assert_eq!(out.not_measured.egress_unreported_executions, 0);
        assert_eq!(
            part(&out.measured_total_money_parts, MoneyFigure::Egress).status,
            MoneyStatus::NotMeasured
        );
    }

    /// A reported zero is measured: priced at $0, not `not_measured`.
    /// Mutation: infer the flags from the sums and both read `not_measured`.
    #[test]
    fn a_reported_zero_of_egress_or_ai_tokens_is_priced_at_zero() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        let start = at(2026, 10, 20, 0, 0);
        let mut raw = exec_at("raw", start, 1_000);
        raw.egress_bytes = Some(0);
        record(&state, &[sample_run_at("run-2", start, vec![raw])]);
        record_ai(
            &state,
            &[ai_call("zero", at(2026, 10, 3, 9, 0), None, Some((0, 0)))],
        );
        let out = month(&config, &state);
        for figure in [MoneyFigure::Egress, MoneyFigure::AiTokens] {
            let p = part(&out.measured_total_money_parts, figure);
            assert_eq!(
                (p.status, p.usd),
                (MoneyStatus::Priced, Some(0.0)),
                "{figure:?}"
            );
        }
    }

    /// With recorded calls and a partial AI part the `max_usd` check and the
    /// reported total agree: neither judges on the incomplete figure.
    #[test]
    fn the_total_and_the_max_usd_check_agree_on_partial_ai() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);
        assert_eq!(out.measured_total_money_status, MoneyStatus::Partial);
        let inputs = load_month_inputs(
            &StateStore::open_read_only(&state).unwrap(),
            &config,
            "2026-10",
            at(2026, 11, 2, 0, 0),
        )
        .unwrap();
        let split = inputs.split(&inputs.runs, at(2026, 11, 2, 0, 0)).unwrap();
        let measured = amounts_out(&split.measured_total);
        let pricing = inputs.pricing(&split, &measured);
        let check = max_usd_check(1e9, &measured, &pricing);
        assert_eq!(check.verdict, ProductBudgetVerdict::NotEvaluated);
        assert!(
            check
                .reason
                .as_deref()
                .is_some_and(|r| r.contains("started recording AI calls")),
            "{check:?}"
        );
    }

    /// Spooled records that no parse can read are counted in `not_measured`
    /// and warned about; the persisted count survives the spool being
    /// emptied. Mutation: drop the field and the loss is silent.
    #[test]
    fn unreadable_ai_records_are_counted_in_not_measured() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        let spool = rocky_core::ai_usage::spool_path(&state);
        rocky_core::ai_usage::append_to_spool(&spool, &october_ai_calls()[..1]).unwrap();
        std::io::Write::write_all(
            &mut std::fs::OpenOptions::new()
                .append(true)
                .open(&spool)
                .unwrap(),
            b"not json\n\xff\n",
        )
        .unwrap();
        // Not yet drained: counted from the spool read.
        let out = month(&config, &state);
        assert_eq!(out.not_measured.ai_records_dropped, 2);
        assert!(out.warnings.iter().any(|w| w.contains("did not parse")));
        // A drain drops them for good; the persisted count keeps them.
        StateStore::open(&state)
            .unwrap()
            .drain_ai_usage_spool()
            .unwrap();
        let out = month(&config, &state);
        assert_eq!(out.not_measured.ai_records_dropped, 2);
        let json = serde_json::to_value(&out).unwrap();
        assert_eq!(json["not_measured"]["ai_records_dropped"], 2);
    }

    /// `rocky lsp` and MCP spool next to the default state file. A report on
    /// another store says so. Mutation: drop the check and no warning.
    #[test]
    fn a_report_on_another_store_warns_about_the_default_spool() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        let models = config.parent().unwrap().join("models");
        std::fs::create_dir_all(&models).unwrap();
        let default_state = models.join(".rocky-state.redb");
        let spool = rocky_core::ai_usage::spool_path(&default_state);
        rocky_core::ai_usage::append_to_spool(&spool, &october_ai_calls()[..1]).unwrap();
        let out = month(&config, &state);
        assert!(
            out.warnings
                .iter()
                .any(|w| w.contains("rocky lsp") && w.contains("not counted")),
            "{:?}",
            out.warnings
        );
        // Reading the default store itself reads that spool: no warning.
        drop(StateStore::open(&default_state).unwrap());
        let out = month(&config, &default_state);
        assert!(
            !out.warnings.iter().any(|w| w.contains("rocky lsp")),
            "{:?}",
            out.warnings
        );
    }

    /// The month table prints the new figures and the counters.
    #[test]
    fn the_month_table_prints_ai_tokens_egress_and_the_counters() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(dir.path(), ALL_PRICES);
        stamp_ai_since(&state, at(2026, 9, 1, 0, 0));
        add_egress_run(&state);
        record_ai(&state, &october_ai_calls());
        let out = month(&config, &state);
        // Prints without panicking; the content is the output fields above.
        print_month_table(&out);
        assert!(out.not_measured.ai_calls_without_usage > 0);
        assert!(out.not_measured.egress_unreported_executions > 0);
    }

    #[test]
    fn a_partial_month_has_no_money_total() {
        let dir = TempDir::new().unwrap();
        let (config, state) = money_project(
            dir.path(),
            "\n[cost]\ncompute_cost_per_machine_hour = 3.6\nstorage_cost_per_gb_month = 1.0\n\
             \n[state.retention]\nmax_age_days = 10\nmin_runs_kept = 3\n",
        );
        let out = month(&config, &state);
        assert!(!out.history_complete, "10 days cannot cover October");
        let p = &out.products[0];
        assert!(amount(&p.charged, CostUnit::ComputeMs) > 0);
        assert_eq!(p.money_status, MoneyStatus::Partial);
        assert_eq!(p.money, None);
        assert_eq!(
            part(&p.money_parts, MoneyFigure::Compute).status,
            MoneyStatus::Partial
        );
        assert_eq!(part(&p.money_parts, MoneyFigure::Compute).usd, None);
        assert_eq!(out.measured_total_money_status, MoneyStatus::Partial);
        assert_eq!(out.measured_total_money, None);
        assert_eq!(out.unallocated_money, None);
    }

    // ----- monthly product budgets -----

    /// A second product, `ltv`, owning the model `ltv`.
    const LTV_SPEC: &str = "[product]\nname = \"ltv\"\nintent = \"lifetime value\"\n\n\
                            [product.source]\ntables = [\"c.s.t\"]\n\n\
                            [product.output]\nmodel = \"ltv\"\ngrain = [\"id\"]\n\
                            columns = [ { name = \"id\", type = \"Int64\", nullable = false } ]\n\n\
                            [product.trust]\nagent = \"propose_only\"\n";

    /// The fixture project (product `revenue_daily` owns model
    /// `revenue_daily`), plus product `ltv` and `extra` appended to the
    /// config. Returns `(config, state_path)`.
    pub(crate) fn budget_project(dir: &Path, extra: &str) -> (PathBuf, PathBuf) {
        let (root, config, state_path) = api_fixture_project(dir);
        std::fs::write(root.join("products/ltv.toml"), LTV_SPEC).unwrap();
        let mut text = std::fs::read_to_string(&config).unwrap();
        text.push('\n');
        text.push_str(extra);
        std::fs::write(&config, text).unwrap();
        (config, state_path)
    }

    fn record(state_path: &Path, runs: &[RunRecord]) {
        let store = StateStore::open(state_path).unwrap();
        for run in runs {
            store.record_run(run).unwrap();
        }
    }

    fn budget_of<'a>(out: &'a ProductMonthCostOutput, product: &str) -> &'a ProductMonthCost {
        out.products
            .iter()
            .find(|p| p.product == product)
            .unwrap_or_else(|| panic!("no product {product}"))
    }

    fn verdict(out: &ProductMonthCostOutput, product: &str) -> ProductBudgetVerdict {
        budget_of(out, product)
            .budget
            .as_ref()
            .expect("the product has a budget")
            .verdict
    }

    fn event_runs(out: &ProductMonthCostOutput, product: &str) -> Vec<String> {
        budget_of(out, product)
            .breach_events
            .iter()
            .map(|e| e.run_id.clone())
            .collect()
    }

    /// 400 ms of `revenue_daily` reading 1,000 ms of unowned `raw`: charged
    /// 1,400 ms, about 0.000389 hours.
    pub(crate) fn one_charged_run(state_path: &Path) {
        let start = at(2026, 10, 5, 2, 0);
        record(
            state_path,
            &[sample_run_at(
                "run-1",
                start,
                vec![
                    exec_at("raw", start, 1_000),
                    reading(
                        exec_at("revenue_daily", start + chrono::Duration::minutes(1), 400),
                        "raw",
                        10,
                    ),
                ],
            )],
        );
    }

    fn one_run_month(budget: &str) -> ProductMonthCostOutput {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(
            dir.path(),
            &format!("[product_budget.revenue_daily]\n{budget}"),
        );
        one_charged_run(&state);
        month(&config, &state)
    }

    const HOURS_1400_MS: f64 = 1_400.0 / 3_600_000.0;

    #[test]
    fn a_budget_above_the_charge_is_in_budget() {
        let out = one_run_month("max_compute_hours = 1\n");
        let product = budget_of(&out, "revenue_daily");
        let budget = product.budget.as_ref().unwrap();
        assert_eq!(budget.verdict, ProductBudgetVerdict::InBudget);
        assert_eq!(budget.checks.len(), 1);
        let check = &budget.checks[0];
        assert_eq!(check.limit_type, ProductBudgetLimitType::MaxComputeHours);
        assert!((check.actual.unwrap() - HOURS_1400_MS).abs() < 1e-12);
        assert!(product.breach_events.is_empty());
        // The product without a budget has none in the output.
        assert!(budget_of(&out, "ltv").budget.is_none());
    }

    /// The budget tests the charged figure, `kept + allocated_upstream`
    /// (1,400 ms), not `built` (400 ms).
    #[test]
    fn a_budget_below_the_charged_figure_is_breached() {
        let between = (400.0 + 1_000.0) / 2.0 / 3_600_000.0;
        let out = one_run_month(&format!("max_compute_hours = {between}\n"));
        assert_eq!(
            verdict(&out, "revenue_daily"),
            ProductBudgetVerdict::Breached
        );
        assert_eq!(event_runs(&out, "revenue_daily"), ["run-1"]);
        let event = &budget_of(&out, "revenue_daily").breach_events[0];
        assert_eq!(event.breached.len(), 1);
        assert_eq!(
            event.breached[0].limit_type,
            ProductBudgetLimitType::MaxComputeHours
        );
    }

    /// Equal to the limit is in budget: only more than the limit breaches.
    #[test]
    fn a_charge_equal_to_the_limit_is_in_budget() {
        let out = one_run_month(&format!("max_compute_hours = {HOURS_1400_MS}\n"));
        assert_eq!(
            verdict(&out, "revenue_daily"),
            ProductBudgetVerdict::InBudget
        );
    }

    #[test]
    fn max_usd_without_a_price_is_not_evaluated() {
        let out = one_run_month("max_compute_hours = 1\nmax_usd = 100\n");
        let budget = budget_of(&out, "revenue_daily").budget.clone().unwrap();
        assert_eq!(budget.verdict, ProductBudgetVerdict::NotEvaluated);
        let usd = budget
            .checks
            .iter()
            .find(|c| c.limit_type == ProductBudgetLimitType::MaxUsd)
            .unwrap();
        assert_eq!(usd.verdict, ProductBudgetVerdict::NotEvaluated);
        assert_eq!(usd.actual, None);
        assert!(usd.reason.as_deref().unwrap().contains("no price"));
    }

    /// `max_usd` uses the same money as the report: `charged` priced from
    /// `[cost]`. 1,400 ms at $3.60 per machine-hour is $0.0014.
    #[test]
    fn max_usd_tests_the_priced_charge() {
        let budget = "max_usd = 0.001\n\n[cost]\ncompute_cost_per_machine_hour = 3.6\n";
        let out = one_run_month(budget);
        let product = budget_of(&out, "revenue_daily");
        let check = &product.budget.as_ref().unwrap().checks[0];
        assert_eq!(check.actual, product.money);
        assert!((check.actual.unwrap() - 0.0014).abs() < 1e-12);
        assert_eq!(check.verdict, ProductBudgetVerdict::Breached);
    }

    /// A limit that is exceeded is a breach even when another limit cannot
    /// be checked: a missing price never hides a known overrun.
    #[test]
    fn a_known_breach_is_not_hidden_by_an_unpriced_limit() {
        let out = one_run_month("max_compute_hours = 0\nmax_usd = 100\n");
        assert_eq!(
            verdict(&out, "revenue_daily"),
            ProductBudgetVerdict::Breached
        );
    }

    #[test]
    fn a_storage_limit_with_no_storage_measured_is_not_evaluated() {
        let out = one_run_month("max_storage_gb_month = 1\n");
        let budget = budget_of(&out, "revenue_daily").budget.clone().unwrap();
        assert_eq!(budget.verdict, ProductBudgetVerdict::NotEvaluated);
        assert!(
            budget.checks[0]
                .reason
                .as_deref()
                .unwrap()
                .contains("no stored size")
        );
    }

    #[test]
    fn a_budget_with_no_limit_is_not_evaluated() {
        let out = one_run_month("on_breach = \"warn\"\n");
        let budget = budget_of(&out, "revenue_daily").budget.clone().unwrap();
        assert_eq!(budget.verdict, ProductBudgetVerdict::NotEvaluated);
        assert!(budget.checks.is_empty());
    }

    /// With history incomplete the verdict is `not_evaluated`, even for a
    /// charge far over the limit, and no event is recorded.
    #[test]
    fn incomplete_history_is_not_evaluated_and_records_no_event() {
        let out = one_run_month(
            "max_compute_hours = 0\n\n[state.retention]\nmax_age_days = 10\nmin_runs_kept = 3\n",
        );
        assert!(!out.history_complete);
        let product = budget_of(&out, "revenue_daily");
        let budget = product.budget.as_ref().unwrap();
        assert_eq!(budget.verdict, ProductBudgetVerdict::NotEvaluated);
        assert!(!budget.reasons.is_empty());
        assert!(product.breach_events.is_empty());
    }

    #[test]
    fn a_month_with_no_production_run_is_not_evaluated() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(
            dir.path(),
            "[product_budget.revenue_daily]\nmax_compute_hours = 1\n",
        );
        let out = month(&config, &state);
        assert_eq!(
            verdict(&out, "revenue_daily"),
            ProductBudgetVerdict::NotEvaluated
        );
    }

    #[test]
    fn a_budget_naming_no_product_is_a_warning() {
        let out = one_run_month("max_compute_hours = 1\n\n[product_budget.nope]\nmax_usd = 1\n");
        assert!(
            out.warnings
                .iter()
                .any(|w| w.contains("[product_budget.nope]")),
            "{:?}",
            out.warnings
        );
    }

    /// Product `revenue_daily` builds its table (out), `ltv` reads that
    /// version and pays for it (back in), then `revenue_daily` rebuilds with
    /// no reader (out again): two events, and the first is not withdrawn.
    fn out_in_out(state_path: &Path) {
        let first = at(2026, 10, 5, 2, 0);
        let second = at(2026, 10, 6, 2, 0);
        let third = at(2026, 10, 7, 2, 0);
        record(
            state_path,
            &[
                sample_run_at("run-1", first, vec![exec_at("revenue_daily", first, 400)]),
                sample_run_at(
                    "run-2",
                    second,
                    vec![reading(exec_at("ltv", second, 50), "revenue_daily", 10)],
                ),
                sample_run_at("run-3", third, vec![exec_at("revenue_daily", third, 400)]),
            ],
        );
    }

    /// 300 ms: under one 400 ms build, over nothing.
    const OUT_IN_OUT_BUDGET: &str =
        "[product_budget.revenue_daily]\nmax_compute_hours = 0.00008333333333333333\n";

    #[test]
    fn out_in_out_is_two_breach_events() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        out_in_out(&state);
        let out = month(&config, &state);
        assert_eq!(event_runs(&out, "revenue_daily"), ["run-1", "run-3"]);
        assert_eq!(
            verdict(&out, "revenue_daily"),
            ProductBudgetVerdict::Breached
        );
    }

    /// Still over at the next run is not a new event.
    #[test]
    fn staying_over_records_no_second_event() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        let first = at(2026, 10, 5, 2, 0);
        let second = at(2026, 10, 6, 2, 0);
        record(
            &state,
            &[
                sample_run_at("run-1", first, vec![exec_at("revenue_daily", first, 400)]),
                sample_run_at("run-2", second, vec![exec_at("raw", second, 5)]),
            ],
        );
        let out = month(&config, &state);
        assert_eq!(event_runs(&out, "revenue_daily"), ["run-1"]);
    }

    /// A product whose models did not run can still cross: its table's
    /// storage accrues between runs, so another pipeline's run is the step
    /// that finds it over. 1 GiB from 1 October against 0.5 GB-month crosses
    /// past the middle of the month.
    #[test]
    fn a_product_that_did_not_run_can_still_cross() {
        use rocky_core::state::{StoredBytes, StoredBytesBasis};
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(
            dir.path(),
            "[product_budget.revenue_daily]\nmax_storage_gb_month = 0.5\n",
        );
        let first = at(2026, 10, 1, 0, 0);
        let mut build = exec_at("revenue_daily", first, 10);
        build.stored_bytes = Some(StoredBytes {
            bytes: 1 << 30,
            basis: StoredBytesBasis::DataFiles,
        });
        let tenth = at(2026, 10, 10, 0, 0);
        let twentieth = at(2026, 10, 20, 0, 0);
        record(
            &state,
            &[
                sample_run_at("build", first, vec![build]),
                sample_run_at("other-10", tenth, vec![exec_at("raw", tenth, 5)]),
                sample_run_at("other-20", twentieth, vec![exec_at("raw", twentieth, 5)]),
            ],
        );
        let out = month(&config, &state);
        assert_eq!(event_runs(&out, "revenue_daily"), ["other-20"]);
        let check = &budget_of(&out, "revenue_daily").breach_events[0].breached[0];
        assert_eq!(check.unit, Some(CostUnit::StorageByteHoursDataFiles));
        assert!(check.actual.unwrap() > 0.5);
    }

    /// The end-of-run check reports an event only for the run that made the
    /// transition: the hook fires once, at that run, and never again for
    /// the same breach.
    #[test]
    fn the_run_check_reports_only_the_transition_at_this_run() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        out_in_out(&state);
        let fourth = at(2026, 10, 8, 2, 0);
        record(
            &state,
            &[sample_run_at(
                "run-4",
                fourth,
                vec![exec_at("raw", fourth, 5)],
            )],
        );
        let store = StateStore::open_read_only_or_empty(&state).unwrap();
        let now = at(2026, 10, 9, 0, 0);
        let at_run = |run_id: &str| {
            let budgets = product_budgets_after_run(&store, &config, run_id, now).unwrap();
            assert_eq!(budgets.len(), 1, "{budgets:?}");
            assert_eq!(budgets[0].product, "revenue_daily");
            assert_eq!(budgets[0].month, "2026-10");
            budgets[0].breach_event.as_ref().map(|e| e.run_id.clone())
        };
        assert_eq!(at_run("run-1").as_deref(), Some("run-1"));
        assert_eq!(at_run("run-2"), None, "run-2 moved it back in");
        assert_eq!(at_run("run-3").as_deref(), Some("run-3"));
        assert_eq!(at_run("run-4"), None, "still over is not a new breach");
        assert!(
            product_budgets_after_run(&store, &config, "no-such-run", now)
                .unwrap()
                .is_empty()
        );
    }

    /// The end-of-run check walks back only as far as it must, and agrees
    /// with the full replay at every step, including a breach found at a run
    /// of another pipeline.
    #[test]
    fn the_run_check_agrees_with_the_full_replay_at_every_step() {
        use rocky_core::state::{StoredBytes, StoredBytesBasis};
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(
            dir.path(),
            "[product_budget.revenue_daily]\nmax_storage_gb_month = 0.5\nmax_usd = 1\n",
        );
        let first = at(2026, 10, 1, 0, 0);
        let mut build = exec_at("revenue_daily", first, 10);
        build.stored_bytes = Some(StoredBytes {
            bytes: 1 << 30,
            basis: StoredBytesBasis::DataFiles,
        });
        let mut runs = vec![sample_run_at("build", first, vec![build])];
        for day in [5, 10, 15, 20, 25] {
            let start = at(2026, 10, day, 0, 0);
            runs.push(sample_run_at(
                &format!("day-{day}"),
                start,
                vec![exec_at("raw", start, 5)],
            ));
        }
        record(&state, &runs);
        let replayed = event_runs(&month(&config, &state), "revenue_daily");
        assert_eq!(
            replayed,
            ["day-20"],
            "the unpriced max_usd does not hide it"
        );
        let store = StateStore::open_read_only_or_empty(&state).unwrap();
        let mut found = Vec::new();
        for run in &runs {
            let after =
                product_budgets_after_run(&store, &config, &run.run_id, at(2026, 11, 2, 0, 0))
                    .unwrap();
            found.extend(
                after
                    .into_iter()
                    .filter_map(|b| b.breach_event.map(|e| e.run_id)),
            );
        }
        assert_eq!(found, replayed);
    }

    /// A shadow run is not a production run: no check, no event.
    #[test]
    fn a_shadow_run_checks_no_budget() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        let start = at(2026, 10, 5, 2, 0);
        let mut shadow = sample_run_at(
            "shadow-1",
            start,
            vec![exec_at("revenue_daily", start, 400)],
        );
        shadow.run_scope = Some(rocky_core::state::RunScope::Shadow { schema: None });
        record(&state, &[shadow]);
        {
            let store = StateStore::open_read_only_or_empty(&state).unwrap();
            let budgets =
                product_budgets_after_run(&store, &config, "shadow-1", at(2026, 10, 6, 0, 0))
                    .unwrap();
            assert!(budgets.is_empty(), "{budgets:?}");
        }
        let out = month(&config, &state);
        assert!(event_runs(&out, "revenue_daily").is_empty());
    }

    /// `rocky product status` carries the current month only for a product
    /// that has a budget, with its verdict.
    #[test]
    fn product_status_gains_cost_month_only_with_a_budget() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        out_in_out(&state);
        let root = config.parent().unwrap();
        let now = at(2026, 10, 9, 0, 0);
        let status = |product: &str| {
            let mut out =
                crate::commands::product::product_status_in(root, Some(&state), product).unwrap();
            crate::commands::product::attach_cost_month(&mut out, &config, &state, now);
            out
        };
        let budgeted = status("revenue_daily");
        assert_eq!(budgeted.cost_month_error, None);
        let cost_month = budgeted
            .cost_month
            .expect("a budgeted product has cost_month");
        assert_eq!(cost_month.month, "2026-10");
        assert_eq!(
            cost_month.cost.budget.map(|b| b.verdict),
            Some(ProductBudgetVerdict::Breached)
        );
        assert_eq!(cost_month.cost.breach_events.len(), 2);
        let unbudgeted = status("ltv");
        assert!(unbudgeted.cost_month.is_none());
        assert!(unbudgeted.cost_month_error.is_none());
    }

    #[test]
    fn month_containing_cuts_in_the_project_time_zone() {
        // 23:30 UTC on 31 October is still October in UTC, and already
        // 1 November in Tokyo (UTC+9).
        let late = at(2026, 10, 31, 23, 30);
        assert_eq!(month_containing(late, "UTC").as_deref(), Some("2026-10"));
        assert_eq!(
            month_containing(late, "Asia/Tokyo").as_deref(),
            Some("2026-11")
        );
        let early = at(2026, 11, 1, 2, 0);
        assert_eq!(
            month_containing(early, "America/New_York").as_deref(),
            Some("2026-10")
        );
        assert_eq!(month_containing(early, "Not/AZone"), None);
    }

    /// The report lists breach events only up to the replay cap, counted as
    /// executions split: three runs of one execution each split 1 + 2 + 3 =
    /// 6. At the cap it replays; one over it skips the replay, says why with
    /// the counts, and still checks the budget.
    #[test]
    fn the_report_replays_breach_events_only_up_to_the_cap() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        out_in_out(&state);
        let store = StateStore::open_read_only_or_empty(&state).unwrap();
        let now = at(2026, 11, 2, 0, 0);
        let report = |cap| compute_month_cost_in(&store, &config, "2026-10", now, cap).unwrap();

        let at_cap = report(6);
        assert_eq!(event_runs(&at_cap, "revenue_daily"), ["run-1", "run-3"]);
        assert_eq!(
            budget_of(&at_cap, "revenue_daily").breach_events_skipped,
            None
        );

        let over = report(5);
        let product = budget_of(&over, "revenue_daily");
        assert!(product.breach_events.is_empty());
        let reason = product.breach_events_skipped.as_deref().expect("a reason");
        assert!(reason.contains("3 production runs"), "{reason}");
        assert!(reason.contains("split 6 executions"), "{reason}");
        assert!(reason.contains("the 5 the report replays"), "{reason}");
        assert!(
            over.warnings.iter().any(|w| w == reason),
            "{:?}",
            over.warnings
        );
        assert_eq!(
            verdict(&over, "revenue_daily"),
            ProductBudgetVerdict::Breached
        );
        // A product without a budget carries no reason.
        assert_eq!(budget_of(&over, "ltv").breach_events_skipped, None);
        // The shipped cap: hourly runs of 20 executions over a 31-day month fit.
        const { assert!(MAX_REPLAY_WORK >= 20 * (31 * 24) * (31 * 24 + 1) / 2) };
    }

    /// The end-of-run check splits at most `MAX_WALK_BACK_STEPS` earlier runs,
    /// so its cost does not grow with the runs of the month. Five runs that
    /// measure no storage are `not_evaluated` for a storage limit; the sixth
    /// builds the table and is over.
    #[test]
    fn the_end_of_run_walk_back_is_bounded() {
        use rocky_core::state::{StoredBytes, StoredBytesBasis};
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(
            dir.path(),
            "[product_budget.revenue_daily]\nmax_storage_gb_month = 0\n",
        );
        let mut runs = Vec::new();
        for day in 1..=5 {
            let start = at(2026, 10, day, 0, 0);
            runs.push(sample_run_at(
                &format!("raw-{day}"),
                start,
                vec![exec_at("raw", start, 5)],
            ));
        }
        let start = at(2026, 10, 6, 0, 0);
        let mut build = exec_at("revenue_daily", start, 10);
        build.stored_bytes = Some(StoredBytes {
            bytes: 1 << 30,
            basis: StoredBytesBasis::DataFiles,
        });
        runs.push(sample_run_at("build", start, vec![build]));
        record(&state, &runs);
        let store = StateStore::open_read_only_or_empty(&state).unwrap();
        let inputs = load_month_inputs(&store, &config, "2026-10", at(2026, 11, 2, 0, 0)).unwrap();
        let budgets = inputs.budgets().unwrap();
        let last = inputs.runs.len() - 1;
        let bounded = events_at_step(&inputs, budgets, last, 2).unwrap();
        assert_eq!(bounded.earlier_splits, 2);
        let open = events_at_step(&inputs, budgets, last, usize::MAX).unwrap();
        assert_eq!(open.earlier_splits, 5);
        // Both find the same first breach, at the build.
        for check in [&bounded, &open] {
            assert_eq!(
                check.events.get("revenue_daily").map(|e| e.run_id.as_str()),
                Some("build")
            );
        }
        let report = compute_month_cost_in(
            &store,
            &config,
            "2026-10",
            at(2026, 11, 2, 0, 0),
            MAX_REPLAY_WORK,
        )
        .unwrap();
        assert_eq!(event_runs(&report, "revenue_daily"), ["build"]);
        const { assert!(MAX_WALK_BACK_STEPS <= 8) };
    }

    /// The end-of-run verdict is the replay's verdict at that run: the same
    /// split, as of the run's finish.
    #[test]
    fn the_end_of_run_verdict_is_the_replay_step_verdict() {
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        out_in_out(&state);
        let store = StateStore::open_read_only_or_empty(&state).unwrap();
        let now = at(2026, 11, 2, 0, 0);
        let inputs = load_month_inputs(&store, &config, "2026-10", now).unwrap();
        let budgets = inputs.budgets().unwrap();
        for (index, run) in inputs.runs.iter().enumerate() {
            let replayed = step_verdicts(&inputs, budgets, index).unwrap();
            let after = product_budgets_after_run(&store, &config, &run.run_id, now).unwrap();
            assert_eq!(after.len(), 1);
            assert_eq!(after[0].budget, replayed["revenue_daily"], "{}", run.run_id);
        }
    }

    /// Timing probe, not a check (run it with `--ignored` in a release
    /// build): `ROCKY_PERF_RUNS` production runs spread
    /// over October, each building `revenue_daily` and 20 readers of it.
    #[test]
    #[ignore]
    fn perf_probe_month_shape() {
        let n: i64 = std::env::var("ROCKY_PERF_RUNS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(720);
        let dir = TempDir::new().unwrap();
        let (config, state) = budget_project(dir.path(), OUT_IN_OUT_BUDGET);
        let spacing = 31 * 24 * 60 * 60 / n;
        let mut runs = Vec::new();
        for i in 0..n {
            let start = at(2026, 10, 1, 0, 0) + chrono::Duration::seconds(i * spacing);
            let models = (0..20)
                .map(|m| reading(exec_at(&format!("m{m}"), start, 50), "revenue_daily", 10))
                .chain(std::iter::once(exec_at("revenue_daily", start, 400)))
                .collect();
            runs.push(sample_run_at(&format!("run-{i:05}"), start, models));
        }
        record(&state, &runs);
        let last = format!("run-{:05}", n - 1);
        let now = at(2026, 11, 2, 0, 0);
        let t = std::time::Instant::now();
        {
            let store = StateStore::open_read_only_or_empty(&state).unwrap();
            let after = product_budgets_after_run(&store, &config, &last, now).unwrap();
            assert_eq!(after.len(), 1);
        }
        let end_of_run = t.elapsed();
        {
            let store = StateStore::open_read_only_or_empty(&state).unwrap();
            let t = std::time::Instant::now();
            let inputs = load_month_inputs(&store, &config, "2026-10", now).unwrap();
            let load = t.elapsed();
            let t = std::time::Instant::now();
            let _ = inputs.split(&inputs.runs, now).unwrap();
            let split = t.elapsed();
            let budgets = inputs.budgets().unwrap();
            let t = std::time::Instant::now();
            let step = events_at_step(&inputs, budgets, inputs.runs.len() - 1, MAX_WALK_BACK_STEPS)
                .unwrap();
            eprintln!(
                "PERF parts load={load:?} one_split={split:?} step_check={:?} earlier_splits={}",
                t.elapsed(),
                step.earlier_splits
            );
        }
        let skip_report = std::env::var("ROCKY_PERF_SKIP_REPORT").is_ok();
        let report = if skip_report {
            None
        } else {
            let t = std::time::Instant::now();
            let out = month(&config, &state);
            Some((
                t.elapsed(),
                budget_of(&out, "revenue_daily").breach_events.len(),
            ))
        };
        eprintln!("PERF runs={n} end_of_run={end_of_run:?} report={report:?}");
    }
}
