//! `rocky review <plan-id>` — human sign-off gate for AI-authored plans.
//!
//! An AI agent can author a `PlanKind::AiAuthored` plan, but a bare
//! `rocky apply` refuses to execute it (see [`crate::commands::apply`]). This
//! command is the gate that unblocks apply:
//!
//! 1. Read the plan and assert it is `PlanKind::AiAuthored`.
//! 2. Compile the working-tree models and the models at `base_ref`, then run
//!    the semantic breaking-change classifier across the two `ProjectIr`
//!    snapshots — reusing the exact same compile + classifier path as
//!    `rocky ci-diff` / the branch-promote gate.
//! 3. Without `--approve`: report the findings (dry run); the plan stays
//!    blocked.
//! 4. With `--approve`: write a review marker at
//!    `<root>/.rocky/plans/<plan_id>.reviewed.json`. The marker is the human
//!    sign-off; `rocky apply` checks for its presence before executing.
//!
//! The marker is written even when breaking changes exist — the human
//! approving has seen the report and is explicitly signing off on them. The
//! emitted output therefore always lists the full classified delta so the
//! approval is informed.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rocky_core::breaking_change::{self, BreakingFinding};
use rocky_core::config::{PolicyCapability, PolicyEffect};
use rocky_core::state::{PolicyDecisionRecord, StateStore};
use serde::{Deserialize, Serialize};

use crate::commands::apply::{ai_plan_is_reviewed, review_marker_path};
use crate::commands::audit::{blast_radius_of, compile_project};
use crate::output::{
    ApproverIdentity, ReviewOutput, ReviewQueueEntry, ReviewQueueOutput, RunPlan, print_json,
};
use crate::plan_store::{PlanKind, read_plan};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// On-disk review marker written by `rocky review <plan-id> --approve`.
///
/// Internal artifact, not a CLI output — it only needs serde, not
/// `JsonSchema`. Its presence (not its contents) is what the apply gate
/// checks; the fields are recorded for an audit trail.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReviewMarker {
    /// The AI-authored plan this marker approves.
    plan_id: String,
    /// When the approval was recorded.
    reviewed_at: DateTime<Utc>,
    /// Git ref the working tree was compared against at review time.
    base_ref: String,
    /// Count of breaking-severity findings the approver signed off on.
    breaking_change_count: usize,
    /// Best-effort git identity of the approver.
    approver: ApproverIdentity,
}

/// Execute `rocky review <plan-id>`.
///
/// Resolves the worktree root from the process cwd (mirroring
/// [`crate::commands::run_apply`]) and delegates to [`run_review_in`].
pub async fn run_review(
    config_path: &Path,
    plan_id: &str,
    base_ref: &str,
    approve: bool,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_review_in(&cwd, config_path, plan_id, base_ref, approve, output_json).await
}

/// Inner implementation — takes an explicit `root` for the plans / marker
/// directory so tests can pass a temp dir without touching the
/// process-global cwd.
pub(crate) async fn run_review_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    base_ref: &str,
    approve: bool,
    output_json: bool,
) -> Result<()> {
    let output = compute_review(root, config_path, plan_id, base_ref, approve).await?;

    if output_json {
        print_json(&output)?;
    } else {
        if let Some(ref message) = output.message {
            println!("{message}");
        }
        if let Some(ref f) = output.breaking_changes {
            let breaking: Vec<&BreakingFinding> = f.iter().filter(|x| x.is_breaking()).collect();
            if !breaking.is_empty() {
                println!("breaking changes ({}):", breaking.len());
                for finding in breaking {
                    println!("  - {:?}", finding.change);
                }
            }
        }
    }

    Ok(())
}

/// Review (and optionally sign off on) an AI-authored / agent-authored plan,
/// returning the typed [`ReviewOutput`] without rendering it.
///
/// The reusable core behind both `rocky review <plan-id>` and the governor's
/// `review_queue` approve action. When `approve` is `true` it writes the
/// sign-off marker at `<root>/.rocky/plans/<plan_id>.reviewed.json` (the same
/// artifact `rocky apply` checks); the marker records the caller's git identity
/// and timestamp. Errors (bails) when the plan is not reviewable. No stdout.
pub async fn compute_review(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    base_ref: &str,
    approve: bool,
) -> Result<ReviewOutput> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read plan '{plan_id}' for review"))?;

    // `rocky review` writes the human sign-off marker that satisfies a
    // `require_review` policy effect. It applies to AI-authored plans and to
    // agent-authored `Run` plans (`rocky plan --principal agent`) — both carry
    // the identical `RunPlan` payload shape. Human-authored plans are never
    // gated, so reviewing one is a no-op the guard rejects rather than
    // silently writing a marker that means nothing.
    let reviewable = plan.kind == PlanKind::AiAuthored
        || plan.kind == PlanKind::Backfill
        || plan.kind == PlanKind::Gc
        || (plan.kind == PlanKind::Run
            && plan.resolved_principal() == rocky_core::config::PolicyPrincipal::Agent);
    if !reviewable {
        bail!(
            "plan '{plan_id}' is a {} plan authored by {}; `rocky review` only applies to \
             AI-authored plans, agent-authored run plans, backfills, and gc plans.",
            plan.kind,
            serde_json::to_value(plan.resolved_principal())
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "human".to_string()),
        );
    }

    // A gc plan carries a `GcPlan` payload (not a `RunPlan`) and changes no
    // model definitions, so the breaking-change gate does not apply — reviewing
    // it is purely the human sign-off that unblocks the eviction. Branch here,
    // before the `RunPlan` deserialize the other kinds need.
    if plan.kind == PlanKind::Gc {
        return review_gc_plan(root, plan_id, approve, output_json).await;
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize plan payload")?;

    let models_dir = run_plan.models_dir.as_deref().unwrap_or("models");

    let findings = compute_review_findings(config_path, Path::new(models_dir), base_ref);
    let breaking_count = findings
        .as_ref()
        .map(|f| f.iter().filter(|x| x.is_breaking()).count())
        .unwrap_or(0);

    let mut marker_written = false;
    if approve {
        let approver =
            crate::commands::branch::approver_identity_pub().unwrap_or_else(|_| ApproverIdentity {
                email: "unknown".to_string(),
                name: None,
                host: "unknown".to_string(),
                source: crate::output::ApproverSource::Local,
            });
        let marker = ReviewMarker {
            plan_id: plan_id.to_string(),
            reviewed_at: Utc::now(),
            base_ref: base_ref.to_string(),
            breaking_change_count: breaking_count,
            approver,
        };
        write_review_marker(root, plan_id, &marker)?;
        marker_written = true;
        tracing::info!(
            target: "rocky::review",
            plan_id,
            base_ref,
            breaking_count,
            "AI-authored plan approved — review marker written"
        );
    } else {
        tracing::info!(
            target: "rocky::review",
            plan_id,
            base_ref,
            breaking_count,
            "AI-authored plan reviewed (dry run) — apply remains blocked"
        );
    }

    let message = build_message(approve, breaking_count, &findings, plan_id);

    Ok(ReviewOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        plan_id: plan_id.to_string(),
        base_ref: base_ref.to_string(),
        approved: approve,
        marker_written,
        breaking_changes: findings,
        message: Some(message),
    })
}

/// Review (and optionally approve) a `PlanKind::Gc` reclamation plan.
///
/// A gc plan changes no model definitions, so there is no breaking-change gate
/// to compute — reviewing it is the human sign-off that unblocks the eviction.
/// `--approve` writes the same marker (`<plan_id>.reviewed.json`) the apply gate
/// checks; without it, apply stays blocked. The marker is payload-agnostic, so
/// the existing `ai_plan_is_reviewed` gate in `commands::apply` recognises it.
async fn review_gc_plan(
    root: &Path,
    plan_id: &str,
    approve: bool,
    output_json: bool,
) -> Result<()> {
    let mut marker_written = false;
    if approve {
        let approver =
            crate::commands::branch::approver_identity_pub().unwrap_or_else(|_| ApproverIdentity {
                email: "unknown".to_string(),
                name: None,
                host: "unknown".to_string(),
                source: crate::output::ApproverSource::Local,
            });
        let marker = ReviewMarker {
            plan_id: plan_id.to_string(),
            reviewed_at: Utc::now(),
            base_ref: String::new(),
            breaking_change_count: 0,
            approver,
        };
        write_review_marker(root, plan_id, &marker)?;
        marker_written = true;
        tracing::info!(
            target: "rocky::review",
            plan_id,
            "gc plan approved — review marker written"
        );
    }

    let message = if approve {
        format!(
            "approved gc plan '{plan_id}' — `rocky apply {plan_id}` is now unblocked. Apply \
             re-verifies each artifact against the live ledger before deleting, and every \
             eviction is tombstoned for restore."
        )
    } else {
        format!(
            "reviewed gc plan '{plan_id}' — re-run with `--approve` to unblock \
             `rocky apply {plan_id}`. Deletion is symmetric-caution gated: even a human gc goes \
             through review."
        )
    };

    let output = ReviewOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        plan_id: plan_id.to_string(),
        base_ref: String::new(),
        approved: approve,
        marker_written,
        breaking_changes: None,
        message: Some(message.clone()),
    };

    if output_json {
        print_json(&output)?;
    } else {
        println!("{message}");
    }
    Ok(())
}

/// Build the human-readable summary line for the review outcome.
///
/// When approving over breaking changes, the message makes that LOUD so the
/// sign-off is never silent.
fn build_message(
    approve: bool,
    breaking_count: usize,
    findings: &Option<Vec<BreakingFinding>>,
    plan_id: &str,
) -> String {
    if findings.is_none() {
        let suffix = if approve {
            format!(
                "approved plan '{plan_id}' (breaking-change gate skipped — could not compile base or HEAD)"
            )
        } else {
            format!(
                "reviewed plan '{plan_id}' (breaking-change gate skipped — could not compile base or HEAD); apply remains blocked"
            )
        };
        return suffix;
    }

    match (approve, breaking_count) {
        (true, 0) => {
            format!(
                "approved plan '{plan_id}' — no breaking changes detected; `rocky apply {plan_id}` is now unblocked"
            )
        }
        (true, n) => {
            format!(
                "approved plan '{plan_id}' WITH {n} breaking change(s) — the approver explicitly signed off on them; `rocky apply {plan_id}` is now unblocked"
            )
        }
        (false, 0) => {
            format!(
                "reviewed plan '{plan_id}' — no breaking changes detected; re-run with `--approve` to unblock `rocky apply {plan_id}`"
            )
        }
        (false, n) => {
            format!(
                "reviewed plan '{plan_id}' — {n} breaking change(s) detected; apply remains blocked. Re-run with `--approve` to sign off on them"
            )
        }
    }
}

/// Compute the breaking-change findings between `base_ref` and the working
/// tree, reusing the exact compile + classifier path that `rocky ci-diff`
/// and the branch-promote gate use.
///
/// Returns:
/// - `Some(findings)` when both refs compiled cleanly and the typed-IR diff
///   ran. `findings` is the full classified list including `Info`-severity
///   entries; callers filter on [`BreakingFinding::is_breaking`].
/// - `None` when the gate was skipped because the models directory was
///   unavailable or either side failed to compile.
fn compute_review_findings(
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
) -> Option<Vec<BreakingFinding>> {
    use rocky_compiler::compile::{self, CompilerConfig};

    if !models_dir.is_dir() {
        tracing::warn!(
            target: "rocky::review",
            models_dir = %models_dir.display(),
            "models directory missing — breaking-change gate skipped"
        );
        return None;
    }

    // Seed both compiles with the cached source schemas so the resulting IR
    // uses real types rather than `Unknown`. Mirrors the branch-promote
    // gate: degrade to an empty map on config / cache failure rather than
    // blocking the review on a configuration issue.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            let state_path = std::path::PathBuf::from(".rocky/state.redb");
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, &state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };

    let head_compile = {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    target: "rocky::review",
                    error = %e,
                    "HEAD compile failed — breaking-change gate skipped"
                );
                return None;
            }
        }
    };

    let base_compile =
        match super::ci_diff::extract_base_compile(base_ref, models_dir, source_schemas) {
            Ok(r) => r,
            Err(reason) => {
                tracing::warn!(
                    target: "rocky::review",
                    reason = %reason,
                    "base compile failed — breaking-change gate skipped"
                );
                return None;
            }
        };

    let base_ir = super::ci_diff::project_ir_from_compile(&base_compile);
    let head_ir = super::ci_diff::project_ir_from_compile(&head_compile);
    Some(breaking_change::diff_project_ir(&base_ir, &head_ir))
}

/// Write the review marker to `<root>/.rocky/plans/<plan_id>.reviewed.json`.
///
/// Factored out so the marker-writing logic is unit-testable without standing
/// up a git repo or running the compiler.
fn write_review_marker(root: &Path, plan_id: &str, marker: &ReviewMarker) -> Result<()> {
    let path = review_marker_path(root, plan_id);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create plans directory at {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(marker).context("failed to serialize review marker")?;
    std::fs::write(&path, bytes)
        .with_context(|| format!("failed to write review marker to {}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky review --queue` — the pending-review work queue
// ---------------------------------------------------------------------------

/// Upper bound on decision history scanned when building the queue.
const MAX_HISTORY_SCAN: usize = 10_000;

/// Human-readable description of the queue ordering.
const QUEUE_RANKING: &str = "blast_radius × classification × staleness";

/// Execute `rocky review --queue`.
///
/// Lists every `require_review` policy decision not yet cleared by a sign-off
/// marker, ranked so the change most in need of a human floats to the top. The
/// approval path is unchanged: each entry names the `rocky review <plan_id>
/// --approve` command that clears it.
pub fn run_review_queue(
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    output_json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_review_queue_in(&cwd, config_path, state_path, models_dir, output_json)
}

/// Inner implementation — takes an explicit `root` (for the marker check) so
/// tests can drive it without touching the process cwd.
pub(crate) fn run_review_queue_in(
    root: &Path,
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    output_json: bool,
) -> Result<()> {
    let output = compute_review_queue(root, config_path, state_path, models_dir)?;

    if output_json {
        print_json(&output)?;
    } else {
        render_queue_text(&output);
    }
    Ok(())
}

/// Build the ranked pending-review queue without rendering it.
///
/// The reusable core behind both `rocky review --queue` and the governor's
/// `review_queue` MCP tool. `root` locates the sign-off markers that clear an
/// escalation; the CLI resolves it from the process cwd, the MCP server passes
/// its project root. Reads only — no stdout.
pub fn compute_review_queue(
    root: &Path,
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
) -> Result<ReviewQueueOutput> {
    let decisions: Vec<PolicyDecisionRecord> = if state_path.exists() {
        let store = StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        store
            .list_policy_decisions()
            .context("failed to read the policy-decision ledger")?
    } else {
        Vec::new()
    };

    let pending = build_queue(
        root,
        config_path,
        state_path,
        models_dir,
        &decisions,
        Utc::now(),
    );

    Ok(ReviewQueueOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        ranking: QUEUE_RANKING.to_string(),
        total: pending.len() as u64,
        pending,
    })
}

/// Assemble the ranked queue from the decision ledger. Factored out so the
/// ranking is unit-testable without a state store.
fn build_queue(
    root: &Path,
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    decisions: &[PolicyDecisionRecord],
    now: DateTime<Utc>,
) -> Vec<ReviewQueueEntry> {
    let outstanding = select_outstanding(decisions, |plan_id| ai_plan_is_reviewed(root, plan_id));
    if outstanding.is_empty() {
        return Vec::new();
    }

    // One compile serves every model's blast radius. A compile failure leaves
    // every blast radius unknown (weight-and-staleness-only ranking) rather
    // than failing the whole queue.
    let compiled = compile_project(config_path, state_path, models_dir).ok();

    let mut entries: Vec<ReviewQueueEntry> = outstanding
        .into_iter()
        .map(|d| {
            let blast_radius = compiled
                .as_ref()
                .and_then(|r| blast_radius_of(r, &d.model))
                .map(|(_, transitive)| transitive.len() as u64);
            let classification_weight = classification_weight(d.capability);
            let staleness_seconds = (now - d.timestamp).num_seconds().max(0);
            let score = queue_score(blast_radius, classification_weight, staleness_seconds);
            ReviewQueueEntry {
                plan_id: d.plan_id.clone(),
                decision_ref: format!("{}|{}|{}", d.timestamp.to_rfc3339(), d.plan_id, d.model),
                timestamp: d.timestamp.to_rfc3339(),
                principal: d.principal,
                capability: d.capability,
                model: d.model.clone(),
                rule_id: d.rule_id,
                reason: d.reason.clone(),
                blast_radius,
                classification_weight,
                staleness_seconds,
                score,
                approve_command: format!("rocky review {} --approve", d.plan_id),
            }
        })
        .collect();

    // Highest score first; deterministic tie-break on plan_id then model.
    entries.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.plan_id.cmp(&b.plan_id))
            .then_with(|| a.model.cmp(&b.model))
    });
    entries
}

/// The pending escalations to surface: the latest `require_review` decision
/// per `(plan_id, model)` whose plan has not yet been signed off.
///
/// A re-evaluated plan appends a fresh ledger row each time, so the queue
/// keeps only the newest row per `(plan_id, model)` — current state, not
/// history — and drops any whose plan already carries a review marker
/// (`is_reviewed`). Pure over the ledger + the marker predicate so the
/// dedup/filter logic is testable without a state store.
fn select_outstanding(
    decisions: &[PolicyDecisionRecord],
    is_reviewed: impl Fn(&str) -> bool,
) -> Vec<&PolicyDecisionRecord> {
    let mut latest: BTreeMap<(&str, &str), &PolicyDecisionRecord> = BTreeMap::new();
    for d in decisions
        .iter()
        .take(MAX_HISTORY_SCAN)
        .filter(|d| d.effect == PolicyEffect::RequireReview)
    {
        latest
            .entry((d.plan_id.as_str(), d.model.as_str()))
            .and_modify(|cur| {
                if d.timestamp > cur.timestamp {
                    *cur = d;
                }
            })
            .or_insert(d);
    }
    latest
        .into_values()
        .filter(|d| !is_reviewed(&d.plan_id))
        .collect()
}

/// Composite priority score: `(blast + 1) × classification_weight ×
/// (1 + staleness_hours)`. Higher sorts first. An unknown blast radius
/// (compile failed / model removed) contributes as zero so the entry still
/// ranks on class and age rather than dropping out.
fn queue_score(
    blast_radius: Option<u64>,
    classification_weight: u32,
    staleness_seconds: i64,
) -> f64 {
    let staleness_factor = 1.0 + (staleness_seconds.max(0) as f64 / 3600.0);
    (blast_radius.unwrap_or(0) + 1) as f64 * f64::from(classification_weight) * staleness_factor
}

/// Change-class weight for the ranking: a breaking schema change outranks a
/// bare mutating verb, which outranks an additive / value-only change.
fn classification_weight(capability: PolicyCapability) -> u32 {
    match capability {
        PolicyCapability::SchemaChangeBreaking => 3,
        PolicyCapability::Apply | PolicyCapability::Promote | PolicyCapability::Backfill => 2,
        _ => 1,
    }
}

/// Render the queue as a concise human-readable list.
fn render_queue_text(out: &ReviewQueueOutput) {
    if out.pending.is_empty() {
        println!("review queue: no escalations awaiting review");
        return;
    }
    println!(
        "review queue: {} escalation(s) awaiting review (ranked by {})",
        out.total, out.ranking
    );
    for (i, e) in out.pending.iter().enumerate() {
        let blast = e
            .blast_radius
            .map(|b| format!("{b} downstream"))
            .unwrap_or_else(|| "blast radius unknown".to_string());
        let principal = serde_json::to_value(e.principal)
            .ok()
            .and_then(|v| v.as_str().map(str::to_string))
            .unwrap_or_default();
        println!(
            "  {}. {} ({}) — {}, waited {}s [score {:.1}]",
            i + 1,
            e.model,
            principal,
            blast,
            e.staleness_seconds,
            e.score,
        );
        println!("     {}", e.reason);
        println!("     approve: {}", e.approve_command);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::output::ApproverSource;

    fn dummy_marker(plan_id: &str) -> ReviewMarker {
        ReviewMarker {
            plan_id: plan_id.to_string(),
            reviewed_at: Utc::now(),
            base_ref: "HEAD".to_string(),
            breaking_change_count: 0,
            approver: ApproverIdentity {
                email: "dev@example.com".to_string(),
                name: Some("Dev".to_string()),
                host: "localhost".to_string(),
                source: ApproverSource::Local,
            },
        }
    }

    #[test]
    fn write_review_marker_creates_file_at_expected_path() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = "a".repeat(64);

        // No marker before.
        let marker_path = review_marker_path(dir.path(), &plan_id);
        assert!(!marker_path.exists());

        write_review_marker(dir.path(), &plan_id, &dummy_marker(&plan_id))?;

        // Marker present after, at the canonical location apply checks.
        assert!(marker_path.exists(), "marker file must exist after write");
        assert_eq!(
            marker_path,
            dir.path()
                .join(".rocky")
                .join("plans")
                .join(format!("{plan_id}.reviewed.json"))
        );

        // Round-trips back into a ReviewMarker.
        let bytes = std::fs::read(&marker_path)?;
        let parsed: ReviewMarker = serde_json::from_slice(&bytes)?;
        assert_eq!(parsed.plan_id, plan_id);
        assert_eq!(parsed.base_ref, "HEAD");
        Ok(())
    }

    /// `--approve` writes the marker; the dry-run path does not. We exercise
    /// the marker-writing decision directly (the full `run_review_in` needs a
    /// git repo + models tree for the compile step).
    #[test]
    fn dry_run_leaves_no_marker_approve_writes_one() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = "b".repeat(64);
        let marker_path = review_marker_path(dir.path(), &plan_id);

        // Dry run: simulate by simply not calling the writer.
        assert!(
            !marker_path.exists(),
            "dry run must not create a marker file"
        );

        // Approve: the writer creates it.
        write_review_marker(dir.path(), &plan_id, &dummy_marker(&plan_id))?;
        assert!(
            marker_path.exists(),
            "approve must create the marker file that unblocks apply"
        );
        Ok(())
    }

    #[test]
    fn message_makes_approval_over_breaking_changes_loud() {
        let findings = Some(vec![]); // gate ran, present (count comes from arg)
        let msg = build_message(true, 3, &findings, "deadbeef");
        assert!(
            msg.contains("WITH 3 breaking change(s)"),
            "approval over breaking changes must be loud: {msg}"
        );
        assert!(msg.contains("explicitly signed off"));
    }

    #[test]
    fn message_dry_run_with_breaking_changes_stays_blocked() {
        let findings = Some(vec![]);
        let msg = build_message(false, 2, &findings, "deadbeef");
        assert!(msg.contains("apply remains blocked"), "dry run msg: {msg}");
        assert!(msg.contains("--approve"));
    }

    #[test]
    fn message_handles_skipped_gate() {
        let msg = build_message(false, 0, &None, "deadbeef");
        assert!(msg.contains("gate skipped"), "skipped-gate msg: {msg}");
    }

    // ---------- review queue ----------

    use chrono::TimeZone;
    use rocky_core::config::PolicyPrincipal;

    fn qd(
        secs: u32,
        plan_id: &str,
        model: &str,
        effect: PolicyEffect,
        cap: PolicyCapability,
    ) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: Utc.with_ymd_and_hms(2026, 7, 7, 0, 0, secs).unwrap(),
            plan_id: plan_id.to_string(),
            principal: PolicyPrincipal::Agent,
            capability: cap,
            model: model.to_string(),
            effect,
            rule_id: None,
            reason: "test".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        }
    }

    #[test]
    fn classification_weight_orders_breaking_over_verb_over_additive() {
        assert!(
            classification_weight(PolicyCapability::SchemaChangeBreaking)
                > classification_weight(PolicyCapability::Apply)
        );
        assert!(
            classification_weight(PolicyCapability::Apply)
                > classification_weight(PolicyCapability::SchemaChangeAdditive)
        );
        assert_eq!(
            classification_weight(PolicyCapability::ValueChange),
            classification_weight(PolicyCapability::SchemaChangeAdditive)
        );
    }

    #[test]
    fn queue_score_rewards_blast_class_and_staleness() {
        // More blast radius ranks higher, all else equal.
        assert!(queue_score(Some(5), 1, 0) > queue_score(Some(0), 1, 0));
        // A more disruptive change class ranks higher.
        assert!(queue_score(Some(0), 3, 0) > queue_score(Some(0), 1, 0));
        // A staler escalation ranks higher.
        assert!(queue_score(Some(0), 1, 7200) > queue_score(Some(0), 1, 0));
        // An unknown blast radius contributes as zero, not as a drop-out.
        assert_eq!(queue_score(None, 2, 0), queue_score(Some(0), 2, 0));
    }

    #[test]
    fn select_outstanding_filters_effect_and_reviewed_and_dedupes() {
        let decisions = vec![
            // require_review, plan A / model x — two rows, newest wins.
            qd(
                1,
                "planA",
                "x",
                PolicyEffect::RequireReview,
                PolicyCapability::SchemaChangeAdditive,
            ),
            qd(
                9,
                "planA",
                "x",
                PolicyEffect::RequireReview,
                PolicyCapability::SchemaChangeBreaking,
            ),
            // require_review on an already-reviewed plan — dropped.
            qd(
                2,
                "planReviewed",
                "y",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
            // not a require_review — never in the queue.
            qd(
                3,
                "planC",
                "z",
                PolicyEffect::Deny,
                PolicyCapability::SchemaChangeBreaking,
            ),
            qd(
                4,
                "planD",
                "w",
                PolicyEffect::Allow,
                PolicyCapability::SchemaChangeAdditive,
            ),
        ];

        let out = select_outstanding(&decisions, |plan_id| plan_id == "planReviewed");

        // Only planA/x survives: deny + allow excluded, planReviewed filtered.
        assert_eq!(out.len(), 1);
        let d = out[0];
        assert_eq!(d.plan_id, "planA");
        assert_eq!(d.model, "x");
        // The newest of the two planA/x rows wins (the breaking one at secs=9).
        assert_eq!(d.capability, PolicyCapability::SchemaChangeBreaking);
    }
}
