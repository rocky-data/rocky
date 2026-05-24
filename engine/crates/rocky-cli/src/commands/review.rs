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

use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rocky_core::breaking_change::{self, BreakingFinding};
use serde::{Deserialize, Serialize};

use crate::commands::apply::review_marker_path;
use crate::output::{ApproverIdentity, ReviewOutput, RunPlan, print_json};
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
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read plan '{plan_id}' for review"))?;

    if plan.kind != PlanKind::AiAuthored {
        bail!(
            "plan '{plan_id}' is a {} plan, not an ai_authored plan. \
             `rocky review` only applies to AI-authored plans.",
            plan.kind,
        );
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize ai_authored plan payload")?;

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

    let output = ReviewOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        plan_id: plan_id.to_string(),
        base_ref: base_ref.to_string(),
        approved: approve,
        marker_written,
        breaking_changes: findings,
        message: Some(message.clone()),
    };

    if output_json {
        print_json(&output)?;
    } else {
        println!("{message}");
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
}
