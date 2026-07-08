//! `rocky audit` — query the agent-policy decision ledger.
//!
//! Two modes:
//!
//! - Bare `rocky audit` reads the [`POLICY_DECISIONS`](rocky_core::state) table
//!   and renders every recorded policy decision (oldest first). Each row is one
//!   evaluation the policy plane made at a mutating enforcement seam
//!   (`rocky apply` / promote) — reads are never recorded, so this is the trail
//!   of *governed mutations*, not inspection.
//! - `rocky audit --for <table|run|plan>` assembles the **custody chain** for a
//!   single subject: who proposed the change and what the policy plane decided,
//!   what the plan changed, which runs materialized it, what a post-apply
//!   verification found, and what sits downstream in its blast radius. It is a
//!   one-query drill-down for the "alert → full chain" path. Every link fails
//!   closed: a link whose signal is genuinely not recorded renders
//!   `unavailable` with a note rather than a fabricated value.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Duration, Utc};
use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::config::PolicyEffect;
use rocky_core::state::{PolicyDecisionRecord, RunRecord, StateStore};

use crate::output::{
    AuditChainBlastRadius, AuditChainDecisions, AuditChainPlan, AuditChainRuns, AuditChainVerify,
    AuditDecisionEntry, AuditForOutput, AuditOutput, AuditPlanChange, AuditRunEntry,
    AuditScorecardOutput, AuditSubjectKind, ScorecardDimension, ScorecardGroup,
    ScorecardUnavailableMetric, SectionAvailability, print_json,
};
use crate::plan_store::read_plan;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Upper bound on run/decision history scanned when assembling a chain.
const MAX_HISTORY_SCAN: usize = 10_000;

/// Execute `rocky audit`.
///
/// Opens the state store read-only and lists the policy-decision ledger. An
/// absent state file is not an error — it renders an empty ledger (no plan has
/// been applied against a `[policy]` block yet).
pub fn run_audit(state_path: &Path, output_json: bool) -> Result<()> {
    let decisions = if state_path.exists() {
        let store = StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        store
            .list_policy_decisions()
            .context("failed to read the policy-decision ledger")?
    } else {
        Vec::new()
    };

    let entries: Vec<AuditDecisionEntry> = decisions.into_iter().map(to_decision_entry).collect();

    let output = AuditOutput {
        version: VERSION.to_string(),
        command: "audit".to_string(),
        decisions: entries,
    };

    if output_json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }
    Ok(())
}

/// Map a ledger record to its serializable entry.
fn to_decision_entry(d: PolicyDecisionRecord) -> AuditDecisionEntry {
    AuditDecisionEntry {
        timestamp: d.timestamp.to_rfc3339(),
        plan_id: d.plan_id,
        principal: d.principal,
        capability: d.capability,
        model: d.model,
        effect: d.effect,
        rule_id: d.rule_id,
        reason: d.reason,
    }
}

/// Render the ledger as a compact human-readable table.
fn render_text(out: &AuditOutput) {
    if out.decisions.is_empty() {
        println!("policy audit: no decisions recorded");
        return;
    }
    println!("policy audit: {} decision(s)", out.decisions.len());
    for d in &out.decisions {
        let principal = serde_plain(&d.principal);
        let capability = serde_plain(&d.capability);
        let effect = serde_plain(&d.effect);
        let rule = d
            .rule_id
            .map(|r| format!("rule {r}"))
            .unwrap_or_else(|| "default".to_string());
        println!(
            "  {} {principal}/{capability} {} {} [{effect} via {rule}] — {}",
            d.timestamp, d.model, d.plan_id, d.reason,
        );
    }
}

/// Serialize a small serde enum to its wire spelling for text output.
fn serde_plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// `rocky audit --for` — the custody chain
// ---------------------------------------------------------------------------

/// Execute `rocky audit --for <selector>`.
///
/// Resolves the selector to a plan, run, or model, then assembles the custody
/// chain from the ledger, the plan file, the run ledger, and a fresh compile
/// (for the downstream blast radius). Reads only — no mutation.
pub fn run_audit_for(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    selector: &str,
    output_json: bool,
) -> Result<()> {
    let root = std::env::current_dir().context("failed to get current working directory")?;

    let (decisions, runs): (Vec<PolicyDecisionRecord>, Vec<RunRecord>) = if state_path.exists() {
        let store = StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        let d = store
            .list_policy_decisions()
            .context("failed to read the policy-decision ledger")?;
        let r = store.list_runs(MAX_HISTORY_SCAN).unwrap_or_default();
        (d, r)
    } else {
        (Vec::new(), Vec::new())
    };

    // Resolve the subject kind (plan file > run id > model name).
    let is_hex64 = selector.len() == 64 && selector.chars().all(|c| c.is_ascii_hexdigit());
    let plan_on_disk = is_hex64 && plan_file_path(&root, selector).exists();
    let run_match = runs.iter().any(|r| r.run_id == selector);
    let kind = if plan_on_disk {
        AuditSubjectKind::Plan
    } else if run_match {
        AuditSubjectKind::Run
    } else {
        AuditSubjectKind::Model
    };

    // The set of models the subject touches — drives the runs join and the
    // blast-radius computation.
    let subject_run: Option<&RunRecord> = match kind {
        AuditSubjectKind::Run => runs.iter().find(|r| r.run_id == selector),
        _ => None,
    };

    let decisions_link = build_decisions_link(kind, selector, &decisions);
    let plan_link = build_plan_link(kind, selector, &decisions_link, &root);
    let runs_link = build_runs_link(kind, selector, subject_run, &runs);
    let verify_link = build_verify_link();

    // Subject models for the blast radius: the model itself, the run's executed
    // models, or the plan's changed models.
    let subject_models: Vec<String> = match kind {
        AuditSubjectKind::Model => vec![selector.to_string()],
        AuditSubjectKind::Run => subject_run
            .map(|r| {
                let mut m: Vec<String> = r
                    .models_executed
                    .iter()
                    .map(|e| e.model_name.clone())
                    .collect();
                m.sort();
                m.dedup();
                m
            })
            .unwrap_or_default(),
        AuditSubjectKind::Plan => plan_link.changes.iter().map(|c| c.model.clone()).collect(),
    };
    let blast_link = build_blast_radius_link(config_path, state_path, models_dir, &subject_models);

    let resolved = match kind {
        AuditSubjectKind::Plan | AuditSubjectKind::Run => true,
        AuditSubjectKind::Model => {
            !decisions_link.entries.is_empty()
                || !runs_link.runs.is_empty()
                || blast_link.availability == SectionAvailability::Available
        }
    };

    let output = AuditForOutput {
        version: VERSION.to_string(),
        command: "audit".to_string(),
        subject: selector.to_string(),
        subject_kind: kind,
        resolved,
        decisions: decisions_link,
        plan: plan_link,
        runs: runs_link,
        verify_after: verify_link,
        blast_radius: blast_link,
    };

    if output_json {
        print_json(&output)?;
    } else {
        render_chain_text(&output);
    }
    Ok(())
}

/// Path a plan file would occupy — existence-checked without the
/// dir-creating side effects of [`read_plan`].
fn plan_file_path(root: &Path, plan_id: &str) -> PathBuf {
    root.join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.json"))
}

fn build_decisions_link(
    kind: AuditSubjectKind,
    selector: &str,
    decisions: &[PolicyDecisionRecord],
) -> AuditChainDecisions {
    // A run is not keyed to any policy decision — the ledger keys on plan_id,
    // and a `RunRecord` carries no plan_id — so a run selector cannot join back
    // to a decision. Fail closed rather than fabricate a link.
    if kind == AuditSubjectKind::Run {
        return AuditChainDecisions {
            availability: SectionAvailability::Unavailable,
            note: Some(
                "the run ledger is not keyed to policy decisions (a run records no plan_id), \
                 so a run selector cannot be joined to a decision"
                    .to_string(),
            ),
            total: 0,
            entries: Vec::new(),
        };
    }

    let mut matched: Vec<&PolicyDecisionRecord> = decisions
        .iter()
        .filter(|d| match kind {
            AuditSubjectKind::Plan => d.plan_id == selector,
            _ => d.model == selector,
        })
        .collect();
    // Newest first.
    matched.sort_by_key(|d| std::cmp::Reverse(d.timestamp));

    if matched.is_empty() {
        return AuditChainDecisions {
            availability: SectionAvailability::NoData,
            note: Some("no policy decisions recorded for this subject".to_string()),
            total: 0,
            entries: Vec::new(),
        };
    }

    let entries: Vec<AuditDecisionEntry> = matched
        .into_iter()
        .cloned()
        .map(to_decision_entry)
        .collect();
    AuditChainDecisions {
        availability: SectionAvailability::Available,
        note: None,
        total: entries.len() as u64,
        entries,
    }
}

fn build_plan_link(
    kind: AuditSubjectKind,
    selector: &str,
    decisions_link: &AuditChainDecisions,
    root: &Path,
) -> AuditChainPlan {
    // Which plan governs the subject?
    let plan_id: Option<String> = match kind {
        AuditSubjectKind::Plan => Some(selector.to_string()),
        // The newest decision on the model names the plan that governed it.
        AuditSubjectKind::Model => decisions_link.entries.first().map(|e| e.plan_id.clone()),
        AuditSubjectKind::Run => None,
    };

    let Some(plan_id) = plan_id else {
        let note = match kind {
            AuditSubjectKind::Run => {
                "the run ledger is not keyed to a plan, so no plan diff can be linked to a run"
                    .to_string()
            }
            _ => "no decision names a governing plan for this subject".to_string(),
        };
        return AuditChainPlan {
            availability: SectionAvailability::Unavailable,
            note: Some(note),
            plan_id: None,
            principal: None,
            kind: None,
            diff_available: false,
            changes: Vec::new(),
        };
    };

    if !plan_file_path(root, &plan_id).exists() {
        return AuditChainPlan {
            availability: SectionAvailability::Unavailable,
            note: Some(format!(
                "plan '{plan_id}' is no longer on disk (its .rocky/plans file was removed), \
                 so the recorded change classification cannot be read"
            )),
            plan_id: Some(plan_id),
            principal: None,
            kind: None,
            diff_available: false,
            changes: Vec::new(),
        };
    }

    let plan = match read_plan(root, &plan_id) {
        Ok(p) => p,
        Err(e) => {
            return AuditChainPlan {
                availability: SectionAvailability::Unavailable,
                note: Some(format!("plan '{plan_id}' could not be read: {e}")),
                plan_id: Some(plan_id),
                principal: None,
                kind: None,
                diff_available: false,
                changes: Vec::new(),
            };
        }
    };

    let embedded = plan.embedded_capabilities();
    let mut changes: Vec<AuditPlanChange> = embedded
        .changed
        .iter()
        .map(|(model, capability)| AuditPlanChange {
            model: model.clone(),
            capability: *capability,
        })
        .collect();
    changes.sort_by(|a, b| a.model.cmp(&b.model));

    let note = if !embedded.diff_available {
        Some(
            "the base↔head change classification was unavailable when the plan was authored — \
             every planned model was treated as a breaking change (fail-closed)"
                .to_string(),
        )
    } else {
        None
    };

    AuditChainPlan {
        availability: SectionAvailability::Available,
        note,
        plan_id: Some(plan_id),
        principal: Some(plan.resolved_principal()),
        kind: Some(plan.kind.to_string()),
        diff_available: embedded.diff_available,
        changes,
    }
}

fn build_runs_link(
    kind: AuditSubjectKind,
    selector: &str,
    subject_run: Option<&RunRecord>,
    runs: &[RunRecord],
) -> AuditChainRuns {
    let matched: Vec<&RunRecord> = match kind {
        AuditSubjectKind::Run => subject_run.into_iter().collect(),
        AuditSubjectKind::Model => {
            let mut m: Vec<&RunRecord> = runs
                .iter()
                .filter(|r| r.models_executed.iter().any(|e| e.model_name == selector))
                .collect();
            m.sort_by_key(|r| std::cmp::Reverse(r.started_at));
            m
        }
        // A plan is not linked to a run in the ledger.
        AuditSubjectKind::Plan => Vec::new(),
    };

    if kind == AuditSubjectKind::Plan {
        return AuditChainRuns {
            availability: SectionAvailability::Unavailable,
            note: Some(
                "the run ledger is not keyed to a plan, so runs cannot be joined to a plan_id"
                    .to_string(),
            ),
            total: 0,
            runs: Vec::new(),
        };
    }

    if matched.is_empty() {
        return AuditChainRuns {
            availability: SectionAvailability::NoData,
            note: Some("no runs recorded for this subject".to_string()),
            total: 0,
            runs: Vec::new(),
        };
    }

    let entries: Vec<AuditRunEntry> = matched
        .into_iter()
        .map(|r| AuditRunEntry {
            run_id: r.run_id.clone(),
            status: run_status_str(r.status).to_string(),
            started_at: r.started_at.to_rfc3339(),
            finished_at: r.finished_at.to_rfc3339(),
            triggering_identity: r.triggering_identity.clone(),
        })
        .collect();

    AuditChainRuns {
        availability: SectionAvailability::Available,
        note: None,
        total: entries.len() as u64,
        runs: entries,
    }
}

/// The verify-after link is always unavailable: post-apply verification
/// outcomes are not persisted to the state store today.
fn build_verify_link() -> AuditChainVerify {
    AuditChainVerify {
        availability: SectionAvailability::Unavailable,
        note: Some(
            "post-apply verification outcomes are not persisted to the state store, so the \
             verify-after result for this subject is not recorded"
                .to_string(),
        ),
    }
}

fn build_blast_radius_link(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    subject_models: &[String],
) -> AuditChainBlastRadius {
    if subject_models.is_empty() {
        return AuditChainBlastRadius {
            availability: SectionAvailability::NoData,
            note: Some("no subject models to compute a blast radius for".to_string()),
            model: None,
            direct: Vec::new(),
            transitive: Vec::new(),
            total: 0,
        };
    }

    let result = match compile_project(config_path, state_path, models_dir) {
        Ok(r) => r,
        Err(e) => {
            return AuditChainBlastRadius {
                availability: SectionAvailability::Unavailable,
                note: Some(format!(
                    "models did not compile, so the downstream blast radius could not be computed: {e}"
                )),
                model: None,
                direct: Vec::new(),
                transitive: Vec::new(),
                total: 0,
            };
        }
    };

    let mut direct: BTreeSet<String> = BTreeSet::new();
    let mut transitive: BTreeSet<String> = BTreeSet::new();
    let mut any_present = false;
    for model in subject_models {
        if let Some((d, t)) = blast_radius_of(&result, model) {
            any_present = true;
            direct.extend(d);
            transitive.extend(t);
        }
    }

    // Don't count the subject models themselves as their own blast radius.
    for m in subject_models {
        direct.remove(m);
        transitive.remove(m);
    }

    let model = if subject_models.len() == 1 {
        Some(subject_models[0].clone())
    } else {
        None
    };

    if !any_present {
        return AuditChainBlastRadius {
            availability: SectionAvailability::NoData,
            note: Some(
                "the subject was not found in the compiled graph (it may have been removed), \
                 so no downstream blast radius could be computed"
                    .to_string(),
            ),
            model,
            direct: Vec::new(),
            transitive: Vec::new(),
            total: 0,
        };
    }

    let transitive: Vec<String> = transitive.into_iter().collect();
    AuditChainBlastRadius {
        availability: SectionAvailability::Available,
        note: None,
        model,
        direct: direct.into_iter().collect(),
        total: transitive.len() as u64,
        transitive,
    }
}

fn run_status_str(status: rocky_core::state::RunStatus) -> &'static str {
    use rocky_core::state::RunStatus;
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
        RunStatus::SkippedIdempotent => "skipped_idempotent",
        RunStatus::SkippedInFlight => "skipped_in_flight",
    }
}

/// Compile the project so downstream lineage carries real types. Seeds the
/// compile with cached source schemas like `rocky lineage` does; degrades to an
/// empty map on config / cache failure.
pub(crate) fn compile_project(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
) -> Result<compile::CompileResult> {
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas,
        source_column_info: std::collections::HashMap::new(),
        ..Default::default()
    };
    compile::compile(&config).context("failed to compile models for blast-radius analysis")
}

/// The `(direct, transitive)` downstream model sets for `model`, or `None`
/// when the model is absent from the compiled graph. Both lists are sorted and
/// deduplicated; neither includes `model` itself.
pub(crate) fn blast_radius_of(
    result: &compile::CompileResult,
    model: &str,
) -> Option<(Vec<String>, Vec<String>)> {
    let schema = result.semantic_graph.model_schema(model)?;

    let mut direct: Vec<String> = schema.downstream.clone();
    direct.sort();
    direct.dedup();

    // BFS the transitive downstream closure.
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut queue: VecDeque<String> = schema.downstream.iter().cloned().collect();
    while let Some(m) = queue.pop_front() {
        if !seen.insert(m.clone()) {
            continue;
        }
        if let Some(s) = result.semantic_graph.model_schema(&m) {
            for d in &s.downstream {
                if !seen.contains(d) {
                    queue.push_back(d.clone());
                }
            }
        }
    }
    seen.remove(model);
    let transitive: Vec<String> = seen.into_iter().collect();
    Some((direct, transitive))
}

/// Render the custody chain as a concise human-readable report.
fn render_chain_text(out: &AuditForOutput) {
    let kind = serde_plain(&out.subject_kind);
    println!("custody chain for {kind} '{}'", out.subject);
    if !out.resolved {
        println!("  (nothing in the ledger, run history, or graph references this subject)");
    }

    // Decisions.
    println!("\ndecision — who proposed, what policy decided:");
    match out.decisions.availability {
        SectionAvailability::Available => {
            for d in &out.decisions.entries {
                let rule = d
                    .rule_id
                    .map(|r| format!("rule {r}"))
                    .unwrap_or_else(|| "default posture".to_string());
                println!(
                    "  {} {}/{} {} → {} via {} — {}",
                    d.timestamp,
                    serde_plain(&d.principal),
                    serde_plain(&d.capability),
                    d.model,
                    serde_plain(&d.effect).to_uppercase(),
                    rule,
                    d.reason,
                );
            }
        }
        _ => println!(
            "  {}",
            status_line(&out.decisions.availability, &out.decisions.note)
        ),
    }

    // Plan diff.
    println!("\nplan — what the plan changed:");
    match out.plan.availability {
        SectionAvailability::Available => {
            if let Some(pid) = &out.plan.plan_id {
                let principal = out
                    .plan
                    .principal
                    .as_ref()
                    .map(serde_plain)
                    .unwrap_or_default();
                let kind = out.plan.kind.clone().unwrap_or_default();
                println!("  plan {pid} ({kind}, authored by {principal})");
            }
            if out.plan.changes.is_empty() {
                println!("  (no per-model change classification recorded)");
            }
            for c in &out.plan.changes {
                println!("  {} → {}", c.model, serde_plain(&c.capability));
            }
            if let Some(note) = &out.plan.note {
                println!("  note: {note}");
            }
        }
        _ => println!("  {}", status_line(&out.plan.availability, &out.plan.note)),
    }

    // Runs.
    println!("\nruns — what materialized it:");
    match out.runs.availability {
        SectionAvailability::Available => {
            for r in &out.runs.runs {
                let who = r
                    .triggering_identity
                    .clone()
                    .unwrap_or_else(|| "unknown".to_string());
                println!(
                    "  {} {} (started {}, by {})",
                    r.run_id, r.status, r.started_at, who
                );
            }
        }
        _ => println!("  {}", status_line(&out.runs.availability, &out.runs.note)),
    }

    // Verify-after.
    println!("\nverify-after — what verification found:");
    println!(
        "  {}",
        status_line(&out.verify_after.availability, &out.verify_after.note)
    );

    // Blast radius.
    println!("\nblast radius — what it touches downstream:");
    match out.blast_radius.availability {
        SectionAvailability::Available => {
            println!(
                "  {} downstream model(s) transitively; direct: {}",
                out.blast_radius.total,
                if out.blast_radius.direct.is_empty() {
                    "—".to_string()
                } else {
                    out.blast_radius.direct.join(", ")
                },
            );
            if !out.blast_radius.transitive.is_empty() {
                println!("  transitive: {}", out.blast_radius.transitive.join(", "));
            }
        }
        _ => println!(
            "  {}",
            status_line(&out.blast_radius.availability, &out.blast_radius.note)
        ),
    }
}

/// One-line status for a non-available chain link.
fn status_line(availability: &SectionAvailability, note: &Option<String>) -> String {
    let marker = match availability {
        SectionAvailability::Available => "available",
        SectionAvailability::NoData => "no data",
        SectionAvailability::Unavailable => "not recorded",
    };
    match note {
        Some(n) => format!("{marker}: {n}"),
        None => marker.to_string(),
    }
}

// ---------------------------------------------------------------------------
// `rocky audit --scorecard` — the decisions-by-group trust scorecard
// ---------------------------------------------------------------------------

/// Execute `rocky audit --scorecard --by <dimension> [--window <span>]`.
///
/// Aggregates the persisted policy-decision ledger into a decisions-by-group
/// trust scorecard: per group (principal / rule / scope) the acceptance,
/// denial, and escalation rates over a `--window`. Reads only.
///
/// Fails closed like the estate brief: a ledger that cannot be read renders the
/// whole scorecard `unavailable` with a note (exit 0) rather than propagating a
/// hard error, and metrics the ledger does not persist are declared
/// `unavailable` rather than fabricated. A malformed `--window`, by contrast,
/// is a usage error and is surfaced as one.
pub fn run_audit_scorecard(
    state_path: &Path,
    by: ScorecardDimension,
    window: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let now = Utc::now();
    let window_label = window.unwrap_or("all").to_string();
    let window_start = parse_window(window, now)?;

    let output = match load_decisions_for_scorecard(state_path) {
        Ok(decisions) => build_scorecard(by, &window_label, window_start, &decisions),
        Err(err) => AuditScorecardOutput {
            version: VERSION.to_string(),
            command: "audit".to_string(),
            by,
            window: window_label,
            window_start: window_start.map(|t| t.to_rfc3339()),
            availability: SectionAvailability::Unavailable,
            note: Some(format!(
                "the policy-decision ledger could not be read, so no scorecard could be \
                 composed: {err}"
            )),
            total_decisions: 0,
            groups: Vec::new(),
            unavailable_metrics: scorecard_unavailable_metrics(),
        },
    };

    if output_json {
        print_json(&output)?;
    } else {
        render_scorecard_text(&output);
    }
    Ok(())
}

/// Read the decision ledger for a scorecard. An absent state file is not an
/// error — it yields an empty ledger (renders as `no_data`). An open/read
/// failure is surfaced so the caller can fail the section closed.
fn load_decisions_for_scorecard(state_path: &Path) -> Result<Vec<PolicyDecisionRecord>> {
    if !state_path.exists() {
        return Ok(Vec::new());
    }
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    store
        .list_policy_decisions()
        .context("failed to read the policy-decision ledger")
}

/// Parse the `--window` flag into an inclusive lower bound.
///
/// `None` and `all` mean all-time (no bound). Otherwise the value is a
/// `<N><unit>` duration with `unit` in `{d, h}` (days / hours), e.g. `30d` or
/// `24h`. A malformed *or out-of-range* value is a usage error, not a
/// fail-closed section and never a panic — an absurd magnitude (`100000000d`)
/// bails with the same clean error as a syntactic one, never overflowing the
/// `TimeDelta` / `DateTime` subtraction.
fn parse_window(window: Option<&str>, now: DateTime<Utc>) -> Result<Option<DateTime<Utc>>> {
    let Some(raw) = window else {
        return Ok(None);
    };
    let raw = raw.trim();
    if raw.eq_ignore_ascii_case("all") {
        return Ok(None);
    }

    let malformed = || {
        format!("invalid --window '{raw}': expected 'all' or a '<N>d' / '<N>h' duration (e.g. 30d)")
    };

    // Guard the byte slice below against a non-ASCII last char splitting a
    // UTF-8 boundary; a duration is always ASCII.
    if !raw.is_ascii() || raw.len() < 2 {
        bail!(malformed());
    }
    let (magnitude, unit) = raw.split_at(raw.len() - 1);
    let n: i64 = match magnitude.parse::<i64>() {
        Ok(n) if n >= 0 => n,
        _ => bail!(malformed()),
    };

    // Build the span with the checked constructors: `Duration::days` /
    // `Duration::hours` panic on `TimeDelta` overflow, and subtracting an
    // over-large span from `now` panics on `DateTime` underflow. An
    // i64-parseable but absurd magnitude must degrade to the usage error, not
    // crash.
    let span = match unit {
        "d" | "D" => Duration::try_days(n),
        "h" | "H" => Duration::try_hours(n),
        _ => bail!(malformed()),
    };
    let Some(span) = span else {
        bail!(malformed());
    };
    match now.checked_sub_signed(span) {
        Some(lower_bound) => Ok(Some(lower_bound)),
        None => bail!(malformed()),
    }
}

/// The ledger key that cites a single decision (`timestamp|plan_id|model`) —
/// the same composite the brief and review queue use.
fn scorecard_decision_ref(d: &PolicyDecisionRecord) -> String {
    format!("{}|{}|{}", d.timestamp.to_rfc3339(), d.plan_id, d.model)
}

/// The group label for a decision under a grouping dimension.
///
/// `Scope` groups by the concrete `model` the decision was about — the ledger
/// records the matched model, not the rule's scope predicates, so the model is
/// the honest stand-in for "scope."
fn scorecard_group_key(by: ScorecardDimension, d: &PolicyDecisionRecord) -> String {
    match by {
        ScorecardDimension::Principal => serde_plain(&d.principal),
        ScorecardDimension::Rule => d
            .rule_id
            .map(|r| format!("rule {r}"))
            .unwrap_or_else(|| "default posture".to_string()),
        ScorecardDimension::Scope => d.model.clone(),
    }
}

/// Running per-group counts while aggregating the ledger.
#[derive(Default)]
struct GroupAccumulator {
    allow: u64,
    require_review: u64,
    deny: u64,
    /// Citations backing the counts, in insertion order (newest first, since
    /// the ledger is sorted newest-first before aggregation).
    decision_refs: Vec<String>,
}

impl GroupAccumulator {
    fn record(&mut self, d: &PolicyDecisionRecord) {
        match d.effect {
            PolicyEffect::Allow => self.allow += 1,
            PolicyEffect::RequireReview => self.require_review += 1,
            PolicyEffect::Deny => self.deny += 1,
        }
        self.decision_refs.push(scorecard_decision_ref(d));
    }

    fn into_group(self, key: String) -> ScorecardGroup {
        let total = self.allow + self.require_review + self.deny;
        let rate = |n: u64| {
            if total == 0 {
                0.0
            } else {
                n as f64 / total as f64
            }
        };
        ScorecardGroup {
            key,
            total,
            allow: self.allow,
            require_review: self.require_review,
            deny: self.deny,
            acceptance_rate: rate(self.allow),
            denial_rate: rate(self.deny),
            review_rate: rate(self.require_review),
            decision_refs: self.decision_refs,
        }
    }
}

/// Compose the scorecard from a decision ledger. Pure over its inputs so the
/// aggregation is unit-testable without a state store.
fn build_scorecard(
    by: ScorecardDimension,
    window_label: &str,
    window_start: Option<DateTime<Utc>>,
    decisions: &[PolicyDecisionRecord],
) -> AuditScorecardOutput {
    // Window filter, then newest-first so each group's citations lead with the
    // most recent decision. The whole ledger is aggregated (no scan cap): the
    // store already materialized every row, and truncating would bias the
    // rates — a scorecard must summarize exactly what is persisted.
    let mut windowed: Vec<&PolicyDecisionRecord> = decisions
        .iter()
        .filter(|d| window_start.is_none_or(|lo| d.timestamp >= lo))
        .collect();
    windowed.sort_by_key(|d| std::cmp::Reverse(d.timestamp));

    let total_decisions = windowed.len() as u64;

    let mut accumulators: BTreeMap<String, GroupAccumulator> = BTreeMap::new();
    for d in &windowed {
        accumulators
            .entry(scorecard_group_key(by, d))
            .or_default()
            .record(d);
    }

    let mut groups: Vec<ScorecardGroup> = accumulators
        .into_iter()
        .map(|(key, acc)| acc.into_group(key))
        .collect();
    // Busiest group first; deterministic tie-break on the label.
    groups.sort_by(|a, b| b.total.cmp(&a.total).then_with(|| a.key.cmp(&b.key)));

    let (availability, note) = if total_decisions == 0 {
        (
            SectionAvailability::NoData,
            Some("no policy decisions fall in the window".to_string()),
        )
    } else {
        (SectionAvailability::Available, None)
    };

    AuditScorecardOutput {
        version: VERSION.to_string(),
        command: "audit".to_string(),
        by,
        window: window_label.to_string(),
        window_start: window_start.map(|t| t.to_rfc3339()),
        availability,
        note,
        total_decisions,
        groups,
        unavailable_metrics: scorecard_unavailable_metrics(),
    }
}

/// The scorecard metrics the ledger does not persist, each rendered
/// `unavailable` with the reason. Declared once — never faked into a number.
fn scorecard_unavailable_metrics() -> Vec<ScorecardUnavailableMetric> {
    vec![
        ScorecardUnavailableMetric {
            metric: "verify_after_pass_rate".to_string(),
            availability: SectionAvailability::Unavailable,
            note: "post-apply verification outcomes are not persisted to the state store, so a \
                   verify-after pass rate cannot be computed"
                .to_string(),
        },
        ScorecardUnavailableMetric {
            metric: "reverts".to_string(),
            availability: SectionAvailability::Unavailable,
            note: "reverts / rollbacks of an applied change are not recorded to the ledger, so a \
                   revert count cannot be computed"
                .to_string(),
        },
        ScorecardUnavailableMetric {
            metric: "escalation_latency".to_string(),
            availability: SectionAvailability::Unavailable,
            note: "the resolution of a require_review escalation is not recorded to the ledger \
                   (an approval leaves only an on-disk plan marker, a denial records nothing), so \
                   escalation latency cannot be computed"
                .to_string(),
        },
    ]
}

/// Render the scorecard as a concise human-readable report.
fn render_scorecard_text(out: &AuditScorecardOutput) {
    let by = serde_plain(&out.by);
    println!("policy scorecard by {by} ({} window)", out.window);

    match out.availability {
        SectionAvailability::Available => {
            println!(
                "  {} decision(s) across {} group(s)",
                out.total_decisions,
                out.groups.len()
            );
            for g in &out.groups {
                println!(
                    "  {}: {} decision(s) — accept {:.1}% / review {:.1}% / deny {:.1}% \
                     [allow {}, review {}, deny {}]",
                    g.key,
                    g.total,
                    g.acceptance_rate * 100.0,
                    g.review_rate * 100.0,
                    g.denial_rate * 100.0,
                    g.allow,
                    g.require_review,
                    g.deny,
                );
            }
        }
        _ => println!("  {}", status_line(&out.availability, &out.note)),
    }

    // The honesty footer: what the ledger cannot support, stated plainly.
    println!("\nnot recorded — uncomputable from the persisted ledger:");
    for m in &out.unavailable_metrics {
        println!("  {} — {}", m.metric, m.note);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use chrono::{TimeZone, Utc};
    use rocky_compiler::compile::{self, CompilerConfig};
    use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
    use tempfile::TempDir;

    use super::*;

    fn decision(
        secs: u32,
        plan_id: &str,
        model: &str,
        effect: PolicyEffect,
    ) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: Utc.with_ymd_and_hms(2026, 7, 7, 0, 0, secs).unwrap(),
            plan_id: plan_id.to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: model.to_string(),
            effect,
            rule_id: Some(0),
            reason: "test".to_string(),
        }
    }

    fn write_model(dir: &Path, name: &str, sql: &str) {
        fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    #[test]
    fn blast_radius_transitive_closure() {
        let dir = TempDir::new().unwrap();
        let models_dir = dir.path();
        // a -> b -> c, plus a -> d (fans out). c and d are leaves.
        write_model(models_dir, "a", "SELECT id FROM source.raw.t");
        write_model(models_dir, "b", "SELECT id FROM a");
        write_model(models_dir, "c", "SELECT id FROM b");
        write_model(models_dir, "d", "SELECT id FROM a");

        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            ..Default::default()
        };
        let result = compile::compile(&config).unwrap();

        let (direct, transitive) = blast_radius_of(&result, "a").expect("a is in the graph");
        assert_eq!(direct, vec!["b".to_string(), "d".to_string()]);
        assert_eq!(
            transitive,
            vec!["b".to_string(), "c".to_string(), "d".to_string()]
        );
        // A leaf has an empty blast radius.
        let (ld, lt) = blast_radius_of(&result, "c").expect("c is in the graph");
        assert!(ld.is_empty() && lt.is_empty());
        // An unknown model is absent from the graph.
        assert!(blast_radius_of(&result, "nope").is_none());
    }

    #[test]
    fn decisions_link_scopes_by_model_for_a_model_subject() {
        let decisions = vec![
            decision(1, "planA", "fct_orders", PolicyEffect::Deny),
            decision(2, "planB", "dim_customer", PolicyEffect::Allow),
            decision(3, "planC", "fct_orders", PolicyEffect::RequireReview),
        ];
        let link = build_decisions_link(AuditSubjectKind::Model, "fct_orders", &decisions);
        assert_eq!(link.availability, SectionAvailability::Available);
        assert_eq!(link.total, 2);
        // Newest first.
        assert_eq!(link.entries[0].plan_id, "planC");
        assert_eq!(link.entries[1].plan_id, "planA");
    }

    #[test]
    fn decisions_link_scopes_by_plan_for_a_plan_subject() {
        let decisions = vec![
            decision(1, "planA", "fct_orders", PolicyEffect::Deny),
            decision(2, "planA", "dim_customer", PolicyEffect::Allow),
            decision(3, "planB", "raw_events", PolicyEffect::Allow),
        ];
        let link = build_decisions_link(AuditSubjectKind::Plan, "planA", &decisions);
        assert_eq!(link.total, 2);
        assert!(link.entries.iter().all(|e| e.plan_id == "planA"));
    }

    #[test]
    fn decisions_link_for_a_run_is_unavailable_not_fabricated() {
        let decisions = vec![decision(1, "planA", "fct_orders", PolicyEffect::Deny)];
        let link = build_decisions_link(AuditSubjectKind::Run, "run-123", &decisions);
        assert_eq!(link.availability, SectionAvailability::Unavailable);
        assert!(link.entries.is_empty());
        assert!(link.note.unwrap().contains("not keyed"));
    }

    #[test]
    fn decisions_link_no_match_is_no_data() {
        let decisions = vec![decision(1, "planA", "fct_orders", PolicyEffect::Deny)];
        let link = build_decisions_link(AuditSubjectKind::Model, "unseen", &decisions);
        assert_eq!(link.availability, SectionAvailability::NoData);
        assert_eq!(link.total, 0);
    }

    #[test]
    fn verify_after_is_always_unavailable() {
        let link = build_verify_link();
        assert_eq!(link.availability, SectionAvailability::Unavailable);
        assert!(link.note.unwrap().contains("not persisted"));
    }

    #[test]
    fn blast_radius_link_with_no_subject_models_is_no_data() {
        let link = build_blast_radius_link(
            Path::new("rocky.toml"),
            Path::new(".rocky/state.redb"),
            Path::new("models"),
            &[],
        );
        assert_eq!(link.availability, SectionAvailability::NoData);
        assert_eq!(link.total, 0);
    }

    // ---------- scorecard ----------

    /// Full-control decision builder: principal, rule, effect, model, and a
    /// second offset in a fixed day so timestamps order deterministically.
    fn sc_decision(
        secs: u32,
        plan_id: &str,
        model: &str,
        principal: PolicyPrincipal,
        rule_id: Option<usize>,
        effect: PolicyEffect,
    ) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: Utc.with_ymd_and_hms(2026, 7, 7, 0, 0, secs).unwrap(),
            plan_id: plan_id.to_string(),
            principal,
            capability: PolicyCapability::Apply,
            model: model.to_string(),
            effect,
            rule_id,
            reason: "test".to_string(),
        }
    }

    /// The seed used by the hand-computed-truth tests below. Ten decisions:
    ///
    /// agent: 3 allow, 2 require_review, 2 deny  (7 total)
    /// human: 3 allow                            (3 total)
    ///
    /// by rule: rule 0 → 4 (agent), rule 1 → 3 (agent), default posture →
    /// 3 (human, rule_id None).
    fn seed() -> Vec<PolicyDecisionRecord> {
        use PolicyEffect::{Allow, Deny, RequireReview};
        use PolicyPrincipal::{Agent, Human};
        vec![
            sc_decision(1, "p1", "bronze_a", Agent, Some(0), Allow),
            sc_decision(2, "p1", "bronze_b", Agent, Some(0), Allow),
            sc_decision(3, "p2", "silver_a", Agent, Some(0), RequireReview),
            sc_decision(4, "p2", "silver_b", Agent, Some(0), Deny),
            sc_decision(5, "p3", "bronze_a", Agent, Some(1), Allow),
            sc_decision(6, "p3", "silver_a", Agent, Some(1), RequireReview),
            sc_decision(7, "p3", "gold_a", Agent, Some(1), Deny),
            sc_decision(8, "p4", "bronze_a", Human, None, Allow),
            sc_decision(9, "p4", "silver_a", Human, None, Allow),
            sc_decision(10, "p5", "gold_a", Human, None, Allow),
        ]
    }

    fn group<'a>(out: &'a AuditScorecardOutput, key: &str) -> &'a ScorecardGroup {
        out.groups
            .iter()
            .find(|g| g.key == key)
            .unwrap_or_else(|| panic!("group '{key}' not found in {:?}", out.groups))
    }

    #[test]
    fn scorecard_by_principal_matches_hand_computed_truth() {
        let out = build_scorecard(ScorecardDimension::Principal, "all", None, &seed());
        assert_eq!(out.availability, SectionAvailability::Available);
        assert_eq!(out.total_decisions, 10);
        assert_eq!(out.groups.len(), 2);

        let agent = group(&out, "agent");
        assert_eq!(
            (agent.total, agent.allow, agent.require_review, agent.deny),
            (7, 3, 2, 2)
        );
        assert!((agent.acceptance_rate - 3.0 / 7.0).abs() < 1e-9);
        assert!((agent.denial_rate - 2.0 / 7.0).abs() < 1e-9);
        assert!((agent.review_rate - 2.0 / 7.0).abs() < 1e-9);
        // The three rates partition the group.
        assert!((agent.acceptance_rate + agent.review_rate + agent.denial_rate - 1.0).abs() < 1e-9);

        let human = group(&out, "human");
        assert_eq!(
            (human.total, human.allow, human.require_review, human.deny),
            (3, 3, 0, 0)
        );
        assert!((human.acceptance_rate - 1.0).abs() < 1e-9);
        assert_eq!(human.denial_rate, 0.0);
    }

    #[test]
    fn scorecard_by_rule_groups_by_rule_id_and_default_posture() {
        let out = build_scorecard(ScorecardDimension::Rule, "all", None, &seed());
        assert_eq!(out.groups.len(), 3);

        let r0 = group(&out, "rule 0");
        assert_eq!(
            (r0.total, r0.allow, r0.require_review, r0.deny),
            (4, 2, 1, 1)
        );

        let r1 = group(&out, "rule 1");
        assert_eq!(
            (r1.total, r1.allow, r1.require_review, r1.deny),
            (3, 1, 1, 1)
        );

        // rule_id None renders as the default posture, not a fabricated index.
        let default = group(&out, "default posture");
        assert_eq!((default.total, default.allow), (3, 3));
    }

    #[test]
    fn scorecard_by_scope_groups_by_model() {
        let out = build_scorecard(ScorecardDimension::Scope, "all", None, &seed());
        // bronze_a = secs 1, 5, 8 → 3 rows, all allow.
        let bronze_a = group(&out, "bronze_a");
        assert_eq!((bronze_a.total, bronze_a.allow), (3, 3));
        // silver_a = secs 3 (review), 6 (review), 9 (allow) → 3 rows.
        let silver_a = group(&out, "silver_a");
        assert_eq!(
            (silver_a.total, silver_a.allow, silver_a.require_review),
            (3, 1, 2)
        );
        // gold_a = secs 7 (deny), 10 (allow).
        let gold_a = group(&out, "gold_a");
        assert_eq!((gold_a.total, gold_a.allow, gold_a.deny), (2, 1, 1));
    }

    #[test]
    fn scorecard_groups_ranked_by_total_desc() {
        let out = build_scorecard(ScorecardDimension::Principal, "all", None, &seed());
        // agent (7) outranks human (3).
        assert_eq!(out.groups[0].key, "agent");
        assert_eq!(out.groups[1].key, "human");
    }

    #[test]
    fn scorecard_decision_refs_are_complete_and_newest_first() {
        let out = build_scorecard(ScorecardDimension::Principal, "all", None, &seed());
        let agent = group(&out, "agent");
        // One ref per decision — a governor can recount from these.
        assert_eq!(agent.decision_refs.len() as u64, agent.total);
        // Newest first: the agent's newest decision is at secs=7 (p3/gold_a).
        assert!(agent.decision_refs[0].contains("|p3|gold_a"));
        // Every ref is a full ledger key (timestamp|plan_id|model).
        for r in &agent.decision_refs {
            assert_eq!(r.matches('|').count(), 2, "ref not a full ledger key: {r}");
        }
    }

    #[test]
    fn scorecard_window_excludes_out_of_window_decisions() {
        // A lower bound at secs=5 keeps only decisions 5..=10 (6 rows).
        let lo = Utc.with_ymd_and_hms(2026, 7, 7, 0, 0, 5).unwrap();
        let out = build_scorecard(
            ScorecardDimension::Principal,
            "5s-cutoff",
            Some(lo),
            &seed(),
        );
        assert_eq!(out.total_decisions, 6);
        // agent within window: secs 5 (allow), 6 (review), 7 (deny) → 3.
        let agent = group(&out, "agent");
        assert_eq!(
            (agent.total, agent.allow, agent.require_review, agent.deny),
            (3, 1, 1, 1)
        );
        // human within window: secs 8, 9, 10 → 3 allow.
        assert_eq!(group(&out, "human").total, 3);
    }

    #[test]
    fn scorecard_empty_ledger_is_no_data_not_unavailable() {
        let out = build_scorecard(ScorecardDimension::Principal, "all", None, &[]);
        assert_eq!(out.availability, SectionAvailability::NoData);
        assert_eq!(out.total_decisions, 0);
        assert!(out.groups.is_empty());
        // The honesty footer is present even with no data.
        assert_eq!(out.unavailable_metrics.len(), 3);
    }

    #[test]
    fn scorecard_unavailable_metrics_are_the_three_unpersisted_and_all_unavailable() {
        let out = build_scorecard(ScorecardDimension::Principal, "all", None, &seed());
        let names: Vec<&str> = out
            .unavailable_metrics
            .iter()
            .map(|m| m.metric.as_str())
            .collect();
        assert_eq!(
            names,
            vec!["verify_after_pass_rate", "reverts", "escalation_latency"]
        );
        // None is faked into a number — all render unavailable with a reason.
        for m in &out.unavailable_metrics {
            assert_eq!(m.availability, SectionAvailability::Unavailable);
            assert!(!m.note.is_empty());
        }
    }

    #[test]
    fn parse_window_accepts_all_and_durations_and_rejects_garbage() {
        let now = Utc.with_ymd_and_hms(2026, 7, 8, 12, 0, 0).unwrap();
        assert_eq!(parse_window(None, now).unwrap(), None);
        assert_eq!(parse_window(Some("all"), now).unwrap(), None);
        assert_eq!(parse_window(Some("ALL"), now).unwrap(), None);
        assert_eq!(
            parse_window(Some("30d"), now).unwrap(),
            Some(now - Duration::days(30))
        );
        assert_eq!(
            parse_window(Some("24h"), now).unwrap(),
            Some(now - Duration::hours(24))
        );
        // Malformed values are usage errors, not silently-defaulted windows.
        assert!(parse_window(Some("30"), now).is_err());
        assert!(parse_window(Some("d"), now).is_err());
        assert!(parse_window(Some("30w"), now).is_err());
        assert!(parse_window(Some("-5d"), now).is_err());
        assert!(parse_window(Some("thirtyd"), now).is_err());
        // i64-parseable but out-of-range magnitudes must degrade to the usage
        // error, never panic on TimeDelta / DateTime overflow (a fat-fingered
        // extra-zeros window like 100000000d ≈ 273k years).
        assert!(parse_window(Some("100000000d"), now).is_err());
        assert!(parse_window(Some("200000000000d"), now).is_err());
        assert!(parse_window(Some("100000000000h"), now).is_err());
        // An i64-overflowing magnitude was already rejected at the parse step.
        assert!(parse_window(Some("9999999999999999999999d"), now).is_err());
    }

    #[test]
    fn scorecard_group_key_maps_each_dimension() {
        let d = sc_decision(
            1,
            "p1",
            "fct_orders",
            PolicyPrincipal::Agent,
            Some(3),
            PolicyEffect::Deny,
        );
        assert_eq!(
            scorecard_group_key(ScorecardDimension::Principal, &d),
            "agent"
        );
        assert_eq!(scorecard_group_key(ScorecardDimension::Rule, &d), "rule 3");
        assert_eq!(
            scorecard_group_key(ScorecardDimension::Scope, &d),
            "fct_orders"
        );
        // A default-posture decision (rule_id None) never fabricates an index.
        let dd = sc_decision(
            2,
            "p1",
            "m",
            PolicyPrincipal::Human,
            None,
            PolicyEffect::Allow,
        );
        assert_eq!(
            scorecard_group_key(ScorecardDimension::Rule, &dd),
            "default posture"
        );
    }
}
