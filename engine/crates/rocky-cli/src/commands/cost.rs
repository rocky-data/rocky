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
    PerModelCostHistorical, ProductMonthCost, ProductMonthCostOutput, SharedNotChargedCost,
};

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
    let cost_per_dbu = cfg.cost.compute_cost_per_dbu;
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
fn parse_month(raw: &str) -> Result<(i32, u32)> {
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
        let dir = match crate::models_loader::resolve_models_dir(&tx.models, config_path) {
            Ok(Some(dir)) => dir,
            Ok(None) => continue,
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

fn to_u64(value: u128) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
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

fn product_out(
    product: &str,
    output_model: &str,
    row: &cost_split::ProductSplit,
) -> ProductMonthCost {
    let charged: cost_split::Amounts = cost_split::Unit::ALL
        .into_iter()
        .map(|unit| (unit, row.charged(unit)))
        .collect();
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
        charged: amounts_out(&charged),
    }
}

/// The typed payload behind `rocky cost --month YYYY-MM --by product`.
///
/// Splits one month of production cost across the products in `products/`,
/// in physical units (see [`rocky_core::cost_split`]). It reads run history
/// only; the month is cut in the project's `[schedule].timezone`, and each
/// execution belongs to the month of its own start. `now` is passed in so a
/// caller can pin it: storage accrues to the earlier of `now` and the end of
/// the month.
///
/// A split that breaks its own invariant is an error, never printed.
pub fn compute_month_cost(
    state_path: &Path,
    config_path: &Path,
    month: &str,
    now: DateTime<Utc>,
) -> Result<ProductMonthCostOutput> {
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
    warnings.extend(problems.into_iter().map(|p| {
        format!("{p}: the model it owns has no owner in this report, so its cost is not charged to the product")
    }));
    let children = declared_children(cfg.as_ref(), config_path, &mut warnings);

    let store = StateStore::open_read_only_or_empty(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    // A run can start before the month and hold executions inside it, so the
    // scan reaches one day back. Anything older is the known limit.
    let runs =
        store.list_runs_matching_since(window.start - chrono::Duration::days(1), |_| true)?;

    let mut wanted: std::collections::BTreeSet<String> = owners.keys().cloned().collect();
    for run in &runs {
        for exec in &run.models_executed {
            wanted.insert(exec.rocky_model_name().to_string());
        }
    }
    let carry_in = store.last_measured_builds_before(&wanted, window.start, |r| {
        r.counts_as_production(UnrecordedScope::Count)
    })?;
    let carry_in: Vec<ModelExecution> = carry_in.into_values().collect();

    let split = cost_split::split_month(&cost_split::SplitInput {
        window,
        as_of: now,
        runs: &runs,
        carry_in: &carry_in,
        history_complete,
        owners: &owners,
        declared_children: &children,
        default_catalog: None,
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

    let model_of: std::collections::BTreeMap<&str, &str> = owners
        .iter()
        .map(|(model, product)| (product.as_str(), model.as_str()))
        .collect();
    let products = split
        .products
        .iter()
        .map(|(product, row)| {
            product_out(
                product,
                model_of.get(product.as_str()).copied().unwrap_or(""),
                row,
            )
        })
        .collect();

    Ok(ProductMonthCostOutput {
        version: VERSION.to_string(),
        command: "cost_by_product".to_string(),
        month: format!("{year:04}-{month_number:02}"),
        timezone,
        month_start: window.start.to_rfc3339(),
        month_end: window.end.to_rfc3339(),
        as_of: now.to_rfc3339(),
        history_complete,
        products,
        unallocated: amounts_out(&split.unallocated),
        shadow_branch: amounts_out(&split.shadow_branch),
        measured_total: amounts_out(&split.measured_total),
        not_measured: CostNotMeasured {
            failed_executions: split.not_measured.failed_executions,
            storage_unknown_tables: split.not_measured.storage_unknown_tables,
        },
        warnings,
    })
}

fn amount_cell(amounts: &[CostAmount], unit: CostUnit) -> String {
    amounts
        .iter()
        .find(|a| a.unit == unit)
        .map_or_else(|| "-".to_string(), |a| a.amount.to_string())
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
    println!();
    println!(
        "not measured: {} failed executions with no recorded time, {} tables with unknown early storage",
        output.not_measured.failed_executions, output.not_measured.storage_unknown_tables
    );
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
}
