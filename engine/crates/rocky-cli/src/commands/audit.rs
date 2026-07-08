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

use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::state::{PolicyDecisionRecord, RunRecord, StateStore};

use crate::output::{
    AuditChainBlastRadius, AuditChainDecisions, AuditChainPlan, AuditChainRuns, AuditChainVerify,
    AuditDecisionEntry, AuditForOutput, AuditOutput, AuditPlanChange, AuditRunEntry,
    AuditSubjectKind, SectionAvailability, print_json,
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
}
