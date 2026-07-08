//! `rocky brief` — the governor's estate digest.
//!
//! A typed projection of the state store and the policy-decision ledger over
//! a time window. It answers the human-oversight question for an
//! agent-operated estate — *what happened, and what needs me?* — without a
//! dashboard: agent activity by principal, the ranked queue of decisions
//! still awaiting review, runs, drift, freshness, quality, and cost.
//!
//! Composition is **template-first**: every section is an independent typed
//! query over data already recorded by `rocky run` / `rocky apply`, rendered
//! with no narration layer. Each section fails closed — a query that returns
//! nothing renders as `no_data`, and a signal that is not persisted at all
//! renders as `unavailable` with a note, never as a smoothed-over "all
//! clear". Every line item carries a ledger citation (`run_id`, `plan_id`, or
//! the composite `decision_ref`) so a governor can drill from the digest into
//! `rocky audit` / `rocky replay` and check it.
//!
//! Markdown mode (`--output` anything but `json`) renders the same data as a
//! Slack/email-ready document — the payload a webhook hook posts. JSON mode
//! is the machine surface for a Dagster asset or the governor's own agent.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};

use rocky_core::config::{PolicyEffect, PolicyPrincipal, load_rocky_config};
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::state::{
    DagChange, PolicyDecisionRecord, QualitySnapshot, RunRecord, RunStatus, RunTrigger, StateStore,
};

use crate::output::{
    BriefAgentActivitySection, BriefBudgetStatus, BriefCostSection, BriefDecisionEntry,
    BriefDriftEntry, BriefDriftSection, BriefEscalationsSection, BriefFailedModel,
    BriefFreshnessEntry, BriefFreshnessSection, BriefOutput, BriefPrincipalActivity,
    BriefQualityEntry, BriefQualitySection, BriefRunCost, BriefRunEntry, BriefRunsSection,
    BriefSinceMode, SectionAvailability, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Upper bound on the run/decision history the digest scans. Windowing then
/// prunes to the requested span; the cap only guards against an unbounded
/// scan on a very long-lived state store.
const MAX_HISTORY_SCAN: usize = 10_000;

/// How `--since` was requested. Parsed from the (clap-validated) flag value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BriefSince {
    /// Everything since the previous `--since last` digest (the stored
    /// cursor). Advances the cursor on success.
    Last,
    /// A rolling 24-hour window ending now.
    Hours24,
    /// A rolling 7-day window ending now.
    Days7,
}

impl BriefSince {
    /// Parse the `--since` flag. clap already constrains the value, so an
    /// unrecognised token falls back to the digest default (`last`) rather
    /// than erroring.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "24h" => Self::Hours24,
            "7d" => Self::Days7,
            _ => Self::Last,
        }
    }

    fn mode(self) -> BriefSinceMode {
        match self {
            Self::Last => BriefSinceMode::Last,
            Self::Hours24 => BriefSinceMode::Hours24,
            Self::Days7 => BriefSinceMode::Days7,
        }
    }
}

/// Execute `rocky brief`.
///
/// `json` selects the machine surface; any other output format renders the
/// Markdown digest. The digest reads the state store at `state_path` and
/// loads `rocky.toml` at `config_path` best-effort (only the cost section
/// depends on the config, and it degrades gracefully when it can't be read).
pub fn run_brief(
    state_path: &Path,
    config_path: &Path,
    since: BriefSince,
    json: bool,
) -> Result<()> {
    let now = Utc::now();

    // Fail closed on an absent state store: there is no history to project.
    if !state_path.exists() {
        let output = empty_brief(
            now,
            since,
            None,
            "state store not found — no runs, decisions, or metrics have been recorded yet",
        );
        return emit(&output, json);
    }

    // `--since last` advances the digest cursor, so it needs write access;
    // the relative windows are read-only.
    let store = match since {
        BriefSince::Last => StateStore::open(state_path),
        _ => StateStore::open_read_only(state_path),
    }
    .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let since_ts: Option<DateTime<Utc>> = match since {
        BriefSince::Last => store
            .get_last_brief_at()
            .context("failed to read the brief cursor")?,
        BriefSince::Hours24 => Some(now - Duration::hours(24)),
        BriefSince::Days7 => Some(now - Duration::days(7)),
    };

    let decisions = store
        .list_policy_decisions()
        .context("failed to read the policy-decision ledger")?;
    let runs = store
        .list_runs(MAX_HISTORY_SCAN)
        .context("failed to read the run ledger")?;

    let windowed_runs: Vec<&RunRecord> = runs
        .iter()
        .filter(|r| in_window(r.started_at, since_ts))
        .collect();
    let mut windowed_decisions: Vec<&PolicyDecisionRecord> = decisions
        .iter()
        .filter(|d| in_window(d.timestamp, since_ts))
        .collect();
    // Newest first for every decision-derived section.
    windowed_decisions.sort_by_key(|d| Reverse(d.timestamp));

    let agent_activity = build_agent_activity(&windowed_decisions);
    let escalations = build_escalations(&windowed_decisions);
    let runs_section = build_runs(&windowed_runs);
    let drift = build_drift(&store, since_ts);
    let (freshness, quality) = build_freshness_and_quality(&store, &windowed_runs, since_ts);
    let cost = build_cost(config_path, &windowed_runs);

    let output = BriefOutput {
        version: VERSION.to_string(),
        command: "brief".to_string(),
        generated_at: now.to_rfc3339(),
        since_mode: since.mode(),
        since_timestamp: since_ts.map(|t| t.to_rfc3339()),
        agent_activity,
        escalations,
        runs: runs_section,
        drift,
        freshness,
        quality,
        cost,
    };

    // Advance the cursor only after a successful render, and only for the
    // digest mode — relative windows never touch it.
    if let BriefSince::Last = since {
        store
            .set_last_brief_at(now)
            .context("failed to advance the brief cursor")?;
    }

    emit(&output, json)
}

/// True when `ts` falls at or after the window's lower bound. An unbounded
/// window (a first-ever `--since last`) admits everything.
fn in_window(ts: DateTime<Utc>, since_ts: Option<DateTime<Utc>>) -> bool {
    since_ts.is_none_or(|lo| ts >= lo)
}

// ---------------------------------------------------------------------------
// Section builders
// ---------------------------------------------------------------------------

fn decision_ref(d: &PolicyDecisionRecord) -> String {
    format!("{}|{}|{}", d.timestamp.to_rfc3339(), d.plan_id, d.model)
}

fn decision_entry(d: &PolicyDecisionRecord) -> BriefDecisionEntry {
    BriefDecisionEntry {
        timestamp: d.timestamp.to_rfc3339(),
        decision_ref: decision_ref(d),
        plan_id: d.plan_id.clone(),
        principal: d.principal,
        capability: d.capability,
        model: d.model.clone(),
        effect: d.effect,
        rule_id: d.rule_id,
        reason: d.reason.clone(),
    }
}

fn build_agent_activity(decisions: &[&PolicyDecisionRecord]) -> BriefAgentActivitySection {
    if decisions.is_empty() {
        return BriefAgentActivitySection {
            availability: SectionAvailability::NoData,
            note: Some("no policy decisions recorded in the window".to_string()),
            total: 0,
            allow: 0,
            require_review: 0,
            deny: 0,
            by_principal: Vec::new(),
            decisions: Vec::new(),
        };
    }

    let (mut allow, mut require_review, mut deny) = (0u64, 0u64, 0u64);
    // Per-principal counts `[total, allow, review, deny]`, keyed by a rank
    // so the digest lists human before agent deterministically.
    let mut per_principal: BTreeMap<u8, [u64; 4]> = BTreeMap::new();
    for d in decisions {
        let bucket = per_principal
            .entry(principal_rank(d.principal))
            .or_default();
        bucket[0] += 1;
        match d.effect {
            PolicyEffect::Allow => {
                allow += 1;
                bucket[1] += 1;
            }
            PolicyEffect::RequireReview => {
                require_review += 1;
                bucket[2] += 1;
            }
            PolicyEffect::Deny => {
                deny += 1;
                bucket[3] += 1;
            }
        }
    }

    let by_principal = per_principal
        .into_iter()
        .map(|(rank, counts)| BriefPrincipalActivity {
            principal: principal_from_rank(rank),
            total: counts[0],
            allow: counts[1],
            require_review: counts[2],
            deny: counts[3],
        })
        .collect();

    BriefAgentActivitySection {
        availability: SectionAvailability::Available,
        note: None,
        total: decisions.len() as u64,
        allow,
        require_review,
        deny,
        by_principal,
        decisions: decisions.iter().map(|d| decision_entry(d)).collect(),
    }
}

fn build_escalations(decisions: &[&PolicyDecisionRecord]) -> BriefEscalationsSection {
    let pending: Vec<BriefDecisionEntry> = decisions
        .iter()
        .filter(|d| matches!(d.effect, PolicyEffect::RequireReview))
        .map(|d| decision_entry(d))
        .collect();

    if pending.is_empty() {
        return BriefEscalationsSection {
            availability: SectionAvailability::NoData,
            note: Some("no decisions awaiting review in the window".to_string()),
            total: 0,
            ranking: "recency".to_string(),
            pending,
        };
    }

    BriefEscalationsSection {
        availability: SectionAvailability::Available,
        note: None,
        total: pending.len() as u64,
        ranking: "recency".to_string(),
        pending,
    }
}

fn build_runs(runs: &[&RunRecord]) -> BriefRunsSection {
    if runs.is_empty() {
        return BriefRunsSection {
            availability: SectionAvailability::NoData,
            note: Some("no runs recorded in the window".to_string()),
            total: 0,
            succeeded: 0,
            partial_failure: 0,
            failed: 0,
            attention: Vec::new(),
        };
    }

    let (mut succeeded, mut partial_failure, mut failed) = (0u64, 0u64, 0u64);
    let mut attention: Vec<&RunRecord> = Vec::new();
    for r in runs {
        match r.status {
            RunStatus::Success => succeeded += 1,
            RunStatus::PartialFailure => {
                partial_failure += 1;
                attention.push(r);
            }
            RunStatus::Failure => {
                failed += 1;
                attention.push(r);
            }
            RunStatus::SkippedIdempotent | RunStatus::SkippedInFlight => {}
        }
    }
    // Newest first.
    attention.sort_by_key(|r| Reverse(r.started_at));

    let attention = attention
        .into_iter()
        .map(|r| BriefRunEntry {
            run_id: r.run_id.clone(),
            status: run_status_str(r.status).to_string(),
            trigger: run_trigger_str(&r.trigger).to_string(),
            started_at: r.started_at.to_rfc3339(),
            finished_at: r.finished_at.to_rfc3339(),
            failed_models: r
                .models_executed
                .iter()
                .filter(|m| m.status != "success")
                .map(|m| BriefFailedModel {
                    model_name: m.model_name.clone(),
                    status: m.status.clone(),
                })
                .collect(),
        })
        .collect();

    BriefRunsSection {
        availability: SectionAvailability::Available,
        note: None,
        total: runs.len() as u64,
        succeeded,
        partial_failure,
        failed,
        attention,
    }
}

fn build_drift(store: &StateStore, since_ts: Option<DateTime<Utc>>) -> BriefDriftSection {
    // Schema drift is surfaced at run time via the `drift_detected` hook but
    // is not persisted to the state store today, so the DAG-snapshot table is
    // the only durable drift signal — and nothing writes it in the run path.
    // Fail closed to `unavailable` rather than fabricate an all-clear.
    match store.get_latest_dag_snapshot() {
        Ok(Some(snapshot))
            if in_window(snapshot.timestamp, since_ts) && !snapshot.changes.is_empty() =>
        {
            let events = snapshot
                .changes
                .iter()
                .map(|c| BriefDriftEntry {
                    timestamp: snapshot.timestamp.to_rfc3339(),
                    graph_hash: snapshot.graph_hash.clone(),
                    change: render_dag_change(c),
                })
                .collect();
            BriefDriftSection {
                availability: SectionAvailability::Available,
                note: None,
                events,
            }
        }
        Ok(Some(_)) => BriefDriftSection {
            availability: SectionAvailability::NoData,
            note: Some("no schema-drift changes recorded in the window".to_string()),
            events: Vec::new(),
        },
        Ok(None) | Err(_) => BriefDriftSection {
            availability: SectionAvailability::Unavailable,
            note: Some(
                "schema drift is surfaced at run time via the drift_detected hook but is not \
                 persisted to the state store — no durable drift signal to report"
                    .to_string(),
            ),
            events: Vec::new(),
        },
    }
}

fn build_freshness_and_quality(
    store: &StateStore,
    runs: &[&RunRecord],
    since_ts: Option<DateTime<Utc>>,
) -> (BriefFreshnessSection, BriefQualitySection) {
    // Distinct model names seen in the window, in a stable order.
    let mut model_names: Vec<String> = runs
        .iter()
        .flat_map(|r| r.models_executed.iter().map(|m| m.model_name.clone()))
        .collect();
    model_names.sort();
    model_names.dedup();

    // Latest quality snapshot per model, if any is recorded and in window.
    let mut snapshots: Vec<QualitySnapshot> = Vec::new();
    let mut query_failed = false;
    for name in &model_names {
        match store.get_quality_trend(name, 1) {
            Ok(mut trend) => {
                if let Some(snap) = trend.pop()
                    && in_window(snap.timestamp, since_ts)
                {
                    snapshots.push(snap);
                }
            }
            Err(_) => query_failed = true,
        }
    }

    if query_failed && snapshots.is_empty() {
        let note = "quality snapshots could not be read from the state store".to_string();
        return (
            BriefFreshnessSection {
                availability: SectionAvailability::Unavailable,
                note: Some(note.clone()),
                models: Vec::new(),
            },
            BriefQualitySection {
                availability: SectionAvailability::Unavailable,
                note: Some(note),
                models: Vec::new(),
            },
        );
    }

    // Freshness: only models whose snapshot recorded a lag.
    let mut freshness: Vec<BriefFreshnessEntry> = snapshots
        .iter()
        .filter_map(|s| {
            s.metrics
                .freshness_lag_seconds
                .map(|lag| BriefFreshnessEntry {
                    model_name: s.model_name.clone(),
                    run_id: s.run_id.clone(),
                    freshness_lag_seconds: lag,
                    observed_at: s.timestamp.to_rfc3339(),
                })
        })
        .collect();
    // Worst (stalest) first.
    freshness.sort_by_key(|f| Reverse(f.freshness_lag_seconds));

    let freshness_section = if freshness.is_empty() {
        BriefFreshnessSection {
            availability: SectionAvailability::NoData,
            note: Some(
                "no freshness metrics recorded in the window (freshness lag is captured only \
                 when a quality snapshot records it)"
                    .to_string(),
            ),
            models: Vec::new(),
        }
    } else {
        BriefFreshnessSection {
            availability: SectionAvailability::Available,
            note: None,
            models: freshness,
        }
    };

    // Quality: every model with a snapshot.
    let mut quality: Vec<BriefQualityEntry> = snapshots
        .iter()
        .map(|s| BriefQualityEntry {
            model_name: s.model_name.clone(),
            run_id: s.run_id.clone(),
            observed_at: s.timestamp.to_rfc3339(),
            row_count: s.metrics.row_count,
            max_null_rate: s.metrics.null_rates.values().copied().reduce(f64::max),
        })
        .collect();
    quality.sort_by_key(|q| Reverse(q.observed_at.clone()));

    let quality_section = if quality.is_empty() {
        BriefQualitySection {
            availability: SectionAvailability::NoData,
            note: Some("no quality snapshots recorded in the window".to_string()),
            models: Vec::new(),
        }
    } else {
        BriefQualitySection {
            availability: SectionAvailability::Available,
            note: None,
            models: quality,
        }
    };

    (freshness_section, quality_section)
}

fn build_cost(config_path: &Path, runs: &[&RunRecord]) -> BriefCostSection {
    if runs.is_empty() {
        return BriefCostSection {
            availability: SectionAvailability::NoData,
            note: Some("no runs recorded in the window".to_string()),
            adapter_type: None,
            run_count: 0,
            total_cost_usd: None,
            total_duration_ms: 0,
            total_bytes_scanned: None,
            per_run: Vec::new(),
            budget: None,
        };
    }

    // Load config best-effort — a missing config only costs the dollar
    // figures; durations and bytes stand on their own.
    let cfg = load_rocky_config(config_path).ok();
    let adapter_info: Option<(String, WarehouseType, f64, f64)> = cfg.as_ref().and_then(|c| {
        let dbu_per_hour = warehouse_size_to_dbu_per_hour(&c.cost.warehouse_size);
        let cost_per_dbu = c.cost.compute_cost_per_dbu;
        resolve_warehouse_type(c).map(|(name, wh)| (name, wh, dbu_per_hour, cost_per_dbu))
    });

    let mut per_run: Vec<BriefRunCost> = Vec::with_capacity(runs.len());
    let mut total_cost = 0.0;
    let mut any_cost = false;
    let mut total_duration_ms: u64 = 0;
    let mut total_bytes: u64 = 0;
    let mut any_bytes = false;

    for r in runs {
        let mut run_cost = 0.0;
        let mut run_any_cost = false;
        let mut run_bytes: u64 = 0;
        let mut run_any_bytes = false;
        for exec in &r.models_executed {
            if let Some((_, wh, dbu, per_dbu)) = adapter_info.as_ref()
                && let Some(c) = compute_observed_cost_usd(
                    *wh,
                    exec.bytes_scanned,
                    exec.duration_ms,
                    *dbu,
                    *per_dbu,
                )
            {
                run_cost += c;
                run_any_cost = true;
            }
            if let Some(b) = exec.bytes_scanned {
                run_bytes = run_bytes.saturating_add(b);
                run_any_bytes = true;
            }
        }
        let duration_ms = (r.finished_at - r.started_at).num_milliseconds().max(0) as u64;
        total_duration_ms = total_duration_ms.saturating_add(duration_ms);
        if run_any_cost {
            total_cost += run_cost;
            any_cost = true;
        }
        if run_any_bytes {
            total_bytes = total_bytes.saturating_add(run_bytes);
            any_bytes = true;
        }
        per_run.push(BriefRunCost {
            run_id: r.run_id.clone(),
            cost_usd: run_any_cost.then_some(run_cost),
            duration_ms,
            bytes_scanned: run_any_bytes.then_some(run_bytes),
        });
    }

    // Priciest first (by cost, then duration), for the exception view.
    per_run.sort_by(|a, b| {
        b.cost_usd
            .unwrap_or(0.0)
            .total_cmp(&a.cost_usd.unwrap_or(0.0))
            .then(b.duration_ms.cmp(&a.duration_ms))
    });

    let budget = cfg
        .as_ref()
        .and_then(|c| c.budget.max_usd)
        .map(|ceiling| build_budget(ceiling, &per_run));

    let note = if adapter_info.is_none() {
        Some(
            "cost not computed — config not loaded or the adapter is not a billed warehouse; \
             durations and bytes are still reported"
                .to_string(),
        )
    } else if !any_cost {
        Some("adapter reported no billed bytes/duration cost for these runs".to_string())
    } else {
        None
    };

    BriefCostSection {
        availability: SectionAvailability::Available,
        note,
        adapter_type: adapter_info.map(|(name, _, _, _)| name),
        run_count: runs.len() as u64,
        total_cost_usd: any_cost.then_some(total_cost),
        total_duration_ms,
        total_bytes_scanned: any_bytes.then_some(total_bytes),
        per_run,
        budget,
    }
}

fn build_budget(ceiling: f64, per_run: &[BriefRunCost]) -> BriefBudgetStatus {
    let runs_over_budget = per_run
        .iter()
        .filter(|r| r.cost_usd.is_some_and(|c| c > ceiling))
        .count() as u64;
    // `per_run` is already sorted priciest first.
    let worst = per_run.iter().find(|r| r.cost_usd.is_some());
    BriefBudgetStatus {
        max_usd_per_run: ceiling,
        runs_over_budget,
        worst_run_id: worst.map(|r| r.run_id.clone()),
        worst_run_cost_usd: worst.and_then(|r| r.cost_usd),
    }
}

/// Resolve the billed-warehouse type for the project's adapters. Prefers the
/// `default` adapter for determinism, then the first declared. Returns `None`
/// for non-billed sources (Fivetran, Airbyte). Mirrors the resolution
/// `rocky cost` uses so the two surfaces agree.
fn resolve_warehouse_type(
    cfg: &rocky_core::config::RockyConfig,
) -> Option<(String, WarehouseType)> {
    let preferred = cfg
        .adapters
        .iter()
        .find(|(k, _)| k.as_str() == "default")
        .or_else(|| cfg.adapters.iter().next())?;
    let wh = WarehouseType::from_adapter_type(&preferred.1.adapter_type)?;
    Some((preferred.1.adapter_type.clone(), wh))
}

// ---------------------------------------------------------------------------
// Small stringifiers
// ---------------------------------------------------------------------------

fn run_status_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
        RunStatus::SkippedIdempotent => "skipped_idempotent",
        RunStatus::SkippedInFlight => "skipped_in_flight",
    }
}

fn run_trigger_str(trigger: &RunTrigger) -> &'static str {
    match trigger {
        RunTrigger::Manual => "manual",
        RunTrigger::Sensor => "sensor",
        RunTrigger::Schedule => "schedule",
        RunTrigger::Ci => "ci",
    }
}

fn principal_rank(p: PolicyPrincipal) -> u8 {
    match p {
        PolicyPrincipal::Human => 0,
        PolicyPrincipal::Agent => 1,
    }
}

fn principal_from_rank(rank: u8) -> PolicyPrincipal {
    match rank {
        0 => PolicyPrincipal::Human,
        _ => PolicyPrincipal::Agent,
    }
}

/// Serialise a small serde enum to its wire spelling for Markdown output.
fn plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

fn render_dag_change(change: &DagChange) -> String {
    match change {
        DagChange::ModelAdded(m) => format!("model added: {m}"),
        DagChange::ModelRemoved(m) => format!("model removed: {m}"),
        DagChange::ColumnAdded { model, column } => format!("column added: {model}.{column}"),
        DagChange::ColumnRemoved { model, column } => format!("column removed: {model}.{column}"),
        DagChange::ColumnTypeChanged {
            model,
            column,
            from,
            to,
        } => format!("column type changed: {model}.{column} {from} → {to}"),
        DagChange::DependencyAdded { from, to } => format!("dependency added: {from} → {to}"),
        DagChange::DependencyRemoved { from, to } => format!("dependency removed: {from} → {to}"),
    }
}

/// Short prefix of a 64-hex id for Markdown citations (full value stays in
/// JSON). Ids shorter than the prefix render verbatim.
fn short(id: &str) -> String {
    if id.len() > 12 {
        format!("{}…", &id[..12])
    } else {
        id.to_string()
    }
}

// ---------------------------------------------------------------------------
// Emit
// ---------------------------------------------------------------------------

fn emit(output: &BriefOutput, json: bool) -> Result<()> {
    if json {
        print_json(output)
    } else {
        print!("{}", render_markdown(output));
        Ok(())
    }
}

/// A fully fail-closed digest for the no-state-store case: every section
/// `unavailable` with the same explanatory note.
fn empty_brief(
    now: DateTime<Utc>,
    since: BriefSince,
    since_ts: Option<DateTime<Utc>>,
    note: &str,
) -> BriefOutput {
    let note = note.to_string();
    BriefOutput {
        version: VERSION.to_string(),
        command: "brief".to_string(),
        generated_at: now.to_rfc3339(),
        since_mode: since.mode(),
        since_timestamp: since_ts.map(|t| t.to_rfc3339()),
        agent_activity: BriefAgentActivitySection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            total: 0,
            allow: 0,
            require_review: 0,
            deny: 0,
            by_principal: Vec::new(),
            decisions: Vec::new(),
        },
        escalations: BriefEscalationsSection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            total: 0,
            ranking: "recency".to_string(),
            pending: Vec::new(),
        },
        runs: BriefRunsSection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            total: 0,
            succeeded: 0,
            partial_failure: 0,
            failed: 0,
            attention: Vec::new(),
        },
        drift: BriefDriftSection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            events: Vec::new(),
        },
        freshness: BriefFreshnessSection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            models: Vec::new(),
        },
        quality: BriefQualitySection {
            availability: SectionAvailability::Unavailable,
            note: Some(note.clone()),
            models: Vec::new(),
        },
        cost: BriefCostSection {
            availability: SectionAvailability::Unavailable,
            note: Some(note),
            adapter_type: None,
            run_count: 0,
            total_cost_usd: None,
            total_duration_ms: 0,
            total_bytes_scanned: None,
            per_run: Vec::new(),
            budget: None,
        },
    }
}

/// Render the digest as a Slack/email-ready Markdown document.
fn render_markdown(out: &BriefOutput) -> String {
    let mut s = String::new();
    s.push_str("# Rocky estate brief\n\n");
    let window = match &out.since_timestamp {
        Some(from) => format!("{from} → {}", out.generated_at),
        None => format!("all history → {}", out.generated_at),
    };
    s.push_str(&format!(
        "_Window: {window}  (mode: {})_\n",
        plain(&out.since_mode)
    ));

    // Needs you — escalations first: this is the point of the digest.
    s.push_str("\n## Needs you\n");
    match out.escalations.availability {
        SectionAvailability::Available => {
            s.push_str(&format!(
                "{} decision(s) awaiting review (ranked by {}):\n",
                out.escalations.total, out.escalations.ranking
            ));
            for d in &out.escalations.pending {
                s.push_str(&format!(
                    "- {}/{} `{}` — {} (plan {}, {}) [decision {}]\n",
                    plain(&d.principal),
                    plain(&d.capability),
                    d.model,
                    d.reason,
                    short(&d.plan_id),
                    rule_label(d.rule_id),
                    short(&d.decision_ref),
                ));
            }
        }
        _ => s.push_str(&section_status(
            &out.escalations.availability,
            &out.escalations.note,
        )),
    }

    // Agent activity.
    s.push_str("\n## Agent activity\n");
    match out.agent_activity.availability {
        SectionAvailability::Available => {
            s.push_str(&format!(
                "{} decision(s): {} allow · {} review · {} deny\n",
                out.agent_activity.total,
                out.agent_activity.allow,
                out.agent_activity.require_review,
                out.agent_activity.deny,
            ));
            for p in &out.agent_activity.by_principal {
                s.push_str(&format!(
                    "- {}: {} ({} allow, {} review, {} deny)\n",
                    plain(&p.principal),
                    p.total,
                    p.allow,
                    p.require_review,
                    p.deny,
                ));
            }
            s.push_str("\nDecisions:\n");
            for d in &out.agent_activity.decisions {
                s.push_str(&format!(
                    "- {}  {}/{} `{}` {} ({}) — plan {}\n",
                    d.timestamp,
                    plain(&d.principal),
                    plain(&d.capability),
                    d.model,
                    plain(&d.effect).to_uppercase(),
                    rule_label(d.rule_id),
                    short(&d.plan_id),
                ));
            }
        }
        _ => s.push_str(&section_status(
            &out.agent_activity.availability,
            &out.agent_activity.note,
        )),
    }

    // Runs.
    s.push_str("\n## Runs\n");
    match out.runs.availability {
        SectionAvailability::Available => {
            s.push_str(&format!(
                "{} run(s): {} ok · {} partial · {} failed\n",
                out.runs.total, out.runs.succeeded, out.runs.partial_failure, out.runs.failed,
            ));
            if out.runs.attention.is_empty() {
                s.push_str("No runs need attention.\n");
            } else {
                s.push_str("\nNeeds attention:\n");
                for r in &out.runs.attention {
                    let failed: Vec<String> = r
                        .failed_models
                        .iter()
                        .map(|m| format!("{} ({})", m.model_name, m.status))
                        .collect();
                    let failed = if failed.is_empty() {
                        "—".to_string()
                    } else {
                        failed.join(", ")
                    };
                    s.push_str(&format!(
                        "- {}  {}  (started {}) — failed: {} [run {}]\n",
                        r.run_id, r.status, r.started_at, failed, r.run_id,
                    ));
                }
            }
        }
        _ => s.push_str(&section_status(&out.runs.availability, &out.runs.note)),
    }

    // Drift.
    s.push_str("\n## Drift\n");
    match out.drift.availability {
        SectionAvailability::Available => {
            for e in &out.drift.events {
                s.push_str(&format!(
                    "- {}  {} [graph {}]\n",
                    e.timestamp,
                    e.change,
                    short(&e.graph_hash)
                ));
            }
        }
        _ => s.push_str(&section_status(&out.drift.availability, &out.drift.note)),
    }

    // Freshness.
    s.push_str("\n## Freshness\n");
    match out.freshness.availability {
        SectionAvailability::Available => {
            for f in &out.freshness.models {
                s.push_str(&format!(
                    "- {}  lag {}s (observed {}) [run {}]\n",
                    f.model_name, f.freshness_lag_seconds, f.observed_at, f.run_id,
                ));
            }
        }
        _ => s.push_str(&section_status(
            &out.freshness.availability,
            &out.freshness.note,
        )),
    }

    // Quality.
    s.push_str("\n## Quality\n");
    match out.quality.availability {
        SectionAvailability::Available => {
            for q in &out.quality.models {
                let nulls = q
                    .max_null_rate
                    .map(|r| format!(", max null {:.1}%", r * 100.0))
                    .unwrap_or_default();
                s.push_str(&format!(
                    "- {}  {} rows{} (observed {}) [run {}]\n",
                    q.model_name, q.row_count, nulls, q.observed_at, q.run_id,
                ));
            }
        }
        _ => s.push_str(&section_status(
            &out.quality.availability,
            &out.quality.note,
        )),
    }

    // Cost.
    s.push_str("\n## Cost\n");
    match out.cost.availability {
        SectionAvailability::Available => {
            let total = out
                .cost
                .total_cost_usd
                .map(|c| format!("${c:.6}"))
                .unwrap_or_else(|| "n/a".to_string());
            s.push_str(&format!(
                "{} run(s), total {} / {}ms\n",
                out.cost.run_count, total, out.cost.total_duration_ms,
            ));
            if let Some(a) = &out.cost.adapter_type {
                s.push_str(&format!("adapter: {a}\n"));
            }
            if let Some(b) = &out.cost.budget {
                s.push_str(&format!(
                    "budget: ${:.2}/run ceiling — {} run(s) over budget\n",
                    b.max_usd_per_run, b.runs_over_budget,
                ));
            }
            for r in &out.cost.per_run {
                let cost = r
                    .cost_usd
                    .map(|c| format!("${c:.6}"))
                    .unwrap_or_else(|| "n/a".to_string());
                s.push_str(&format!(
                    "- {}  {} / {}ms [run {}]\n",
                    r.run_id, cost, r.duration_ms, r.run_id,
                ));
            }
            if let Some(note) = &out.cost.note {
                s.push_str(&format!("_{note}_\n"));
            }
        }
        _ => s.push_str(&section_status(&out.cost.availability, &out.cost.note)),
    }

    s
}

fn rule_label(rule_id: Option<usize>) -> String {
    rule_id.map_or_else(|| "default posture".to_string(), |r| format!("rule {r}"))
}

/// Markdown line for a non-available section: the marker plus any note.
fn section_status(availability: &SectionAvailability, note: &Option<String>) -> String {
    let marker = match availability {
        SectionAvailability::Available => "available",
        SectionAvailability::NoData => "no data in window",
        SectionAvailability::Unavailable => "unavailable",
    };
    match note {
        Some(n) => format!("_{marker}: {n}_\n"),
        None => format!("_{marker}_\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use rocky_core::config::PolicyCapability;
    use rocky_core::state::ModelExecution;

    fn ts(h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 7, h, 0, 0).unwrap()
    }

    fn decision(
        h: u32,
        principal: PolicyPrincipal,
        effect: PolicyEffect,
        model: &str,
        rule_id: Option<usize>,
    ) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: ts(h),
            plan_id: format!("plan-{model}-{h}"),
            principal,
            capability: PolicyCapability::Apply,
            model: model.to_string(),
            effect,
            rule_id,
            reason: "test".to_string(),
        }
    }

    fn exec(name: &str, status: &str) -> ModelExecution {
        ModelExecution {
            model_name: name.to_string(),
            started_at: ts(1),
            finished_at: ts(1),
            duration_ms: 10,
            rows_affected: Some(1),
            status: status.to_string(),
            sql_hash: "h".to_string(),
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            recipe_hash: None,
            input_hash: None,
            input_proof_class: None,
            env_hash: None,
            hash_scheme: None,
            output_column_hashes: None,
            attempts: Vec::new(),
        }
    }

    fn run(id: &str, status: RunStatus, models: Vec<ModelExecution>) -> RunRecord {
        RunRecord {
            run_id: id.to_string(),
            started_at: ts(2),
            finished_at: ts(3),
            status,
            models_executed: models,
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
        }
    }

    #[test]
    fn in_window_admits_all_when_unbounded() {
        assert!(in_window(ts(1), None));
        assert!(in_window(ts(1), Some(ts(1))));
        assert!(!in_window(ts(1), Some(ts(2))));
    }

    #[test]
    fn agent_activity_counts_by_effect_and_principal() {
        let d = [
            decision(
                9,
                PolicyPrincipal::Agent,
                PolicyEffect::Deny,
                "fct_orders",
                Some(0),
            ),
            decision(
                10,
                PolicyPrincipal::Agent,
                PolicyEffect::Allow,
                "bronze",
                Some(1),
            ),
            decision(
                11,
                PolicyPrincipal::Agent,
                PolicyEffect::RequireReview,
                "dim_customer",
                None,
            ),
            decision(
                12,
                PolicyPrincipal::Human,
                PolicyEffect::Allow,
                "x",
                Some(2),
            ),
        ];
        let refs: Vec<&PolicyDecisionRecord> = d.iter().collect();
        let section = build_agent_activity(&refs);
        assert_eq!(section.availability, SectionAvailability::Available);
        assert_eq!(section.total, 4);
        assert_eq!(section.allow, 2);
        assert_eq!(section.require_review, 1);
        assert_eq!(section.deny, 1);
        // Two principals, human before agent.
        assert_eq!(section.by_principal.len(), 2);
        assert_eq!(section.by_principal[0].principal, PolicyPrincipal::Human);
        assert_eq!(section.by_principal[1].principal, PolicyPrincipal::Agent);
        let agent = &section.by_principal[1];
        assert_eq!(agent.total, 3);
        assert_eq!(agent.allow, 1);
        assert_eq!(agent.require_review, 1);
        assert_eq!(agent.deny, 1);
    }

    #[test]
    fn agent_activity_empty_is_no_data() {
        let section = build_agent_activity(&[]);
        assert_eq!(section.availability, SectionAvailability::NoData);
        assert!(section.note.is_some());
    }

    #[test]
    fn escalations_only_require_review() {
        let d = [
            decision(
                9,
                PolicyPrincipal::Agent,
                PolicyEffect::Deny,
                "fct_orders",
                Some(0),
            ),
            decision(
                11,
                PolicyPrincipal::Agent,
                PolicyEffect::RequireReview,
                "dim_customer",
                None,
            ),
        ];
        let refs: Vec<&PolicyDecisionRecord> = d.iter().collect();
        let section = build_escalations(&refs);
        assert_eq!(section.availability, SectionAvailability::Available);
        assert_eq!(section.total, 1);
        assert_eq!(section.pending[0].model, "dim_customer");
        // The pending entry carries its ledger citations.
        assert!(section.pending[0].decision_ref.contains("dim_customer"));
        assert!(section.pending[0].plan_id.contains("dim_customer"));
    }

    #[test]
    fn runs_surface_failed_models_with_run_id() {
        let ok = run("run-ok", RunStatus::Success, vec![exec("a", "success")]);
        let partial = run(
            "run-partial",
            RunStatus::PartialFailure,
            vec![exec("good", "success"), exec("bad", "failed")],
        );
        let refs = [&ok, &partial];
        let section = build_runs(&refs);
        assert_eq!(section.availability, SectionAvailability::Available);
        assert_eq!(section.total, 2);
        assert_eq!(section.succeeded, 1);
        assert_eq!(section.partial_failure, 1);
        // Only the partial run needs attention, and it cites its run_id +
        // the failed model.
        assert_eq!(section.attention.len(), 1);
        assert_eq!(section.attention[0].run_id, "run-partial");
        assert_eq!(section.attention[0].failed_models.len(), 1);
        assert_eq!(section.attention[0].failed_models[0].model_name, "bad");
    }

    #[test]
    fn runs_empty_is_no_data() {
        let section = build_runs(&[]);
        assert_eq!(section.availability, SectionAvailability::NoData);
    }

    #[test]
    fn markdown_leads_with_needs_you_and_cites() {
        let d = [decision(
            11,
            PolicyPrincipal::Agent,
            PolicyEffect::RequireReview,
            "dim_customer",
            Some(3),
        )];
        let refs: Vec<&PolicyDecisionRecord> = d.iter().collect();
        let out = BriefOutput {
            version: "0".to_string(),
            command: "brief".to_string(),
            generated_at: ts(12).to_rfc3339(),
            since_mode: BriefSinceMode::Hours24,
            since_timestamp: Some(ts(1).to_rfc3339()),
            agent_activity: build_agent_activity(&refs),
            escalations: build_escalations(&refs),
            runs: build_runs(&[]),
            drift: BriefDriftSection {
                availability: SectionAvailability::Unavailable,
                note: Some("not wired".to_string()),
                events: Vec::new(),
            },
            freshness: BriefFreshnessSection {
                availability: SectionAvailability::NoData,
                note: None,
                models: Vec::new(),
            },
            quality: BriefQualitySection {
                availability: SectionAvailability::NoData,
                note: None,
                models: Vec::new(),
            },
            cost: BriefCostSection {
                availability: SectionAvailability::NoData,
                note: None,
                adapter_type: None,
                run_count: 0,
                total_cost_usd: None,
                total_duration_ms: 0,
                total_bytes_scanned: None,
                per_run: Vec::new(),
                budget: None,
            },
        };
        let md = render_markdown(&out);
        assert!(md.starts_with("# Rocky estate brief"));
        assert!(md.contains("## Needs you"));
        assert!(md.contains("dim_customer"));
        assert!(md.contains("rule 3"));
        // Drift fails closed, not silently.
        assert!(md.contains("unavailable: not wired"));
    }
}
