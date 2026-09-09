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
//!    `<root>/.rocky/plans/<plan_id>.reviewed.json`. `rocky apply` checks the
//!    marker's CONTENTS before executing, not just its presence: it must parse
//!    and name that exact plan. The marker is unsigned, so it records that an
//!    approval was made on this machine, not who made it.
//!
//! The marker is written even when breaking changes exist: approving over a
//! reported break is allowed.
//!
//! The marker is ALSO written when the classifier could not run for a
//! RECOVERABLE reason: a missing models directory, or either tree failing to
//! compile. `compute_review_findings` returns `Ok(None)` there,
//! `breaking_change_count` falls back to 0 and `--approve` still writes the
//! marker. So the count on a marker is not evidence that a delta was computed,
//! and the emitted output does not always carry one. The approver identity
//! falls back to `unknown` when the git identity cannot be read.
//!
//! One case is NOT recoverable and refuses instead (#1680): a `rocky.toml`
//! that is PRESENT and does not load. The schema cache the classifier types
//! against is gated on that config, so an unloadable one turns a real type
//! change into `Unknown`-vs-`Unknown` and reports zero breaking changes.
//! Approving on that is weaker informed approval than the loaded path, so
//! `compute_review_findings` returns `Err` and `compute_review` propagates it
//! before the `--approve` branch — **no marker is written**. A project with no
//! `rocky.toml` at all is unchanged; so is an unset `${VAR}` in an adapter's
//! connection fields, which this path now tolerates (#1536) where the strict
//! loader it used before silently cooled the cache.
//!
//! The marker-only kinds (gc / restore / compact / archive) short-circuit in
//! `compute_review_marker_only` before any config read, so they still approve
//! on an unloadable config. There is no breaking-change gate on them to
//! degrade — reviewing is purely the human sign-off.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rocky_core::breaking_change::{self, BreakingFinding};
use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};
use rocky_core::state::{PolicyDecisionRecord, StateStore};
use serde::{Deserialize, Serialize};

use crate::commands::apply::{ai_plan_is_reviewed, review_marker_path};
use crate::commands::audit::{blast_radius_union, compile_project_with_schemas, plan_file_path};
use crate::output::{
    ApproverIdentity, ReviewOutput, ReviewQueueEntry, ReviewQueueOutput, RunPlan, print_json,
};
use crate::plan_store::{PlanKind, read_plan};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// On-disk review marker written by `rocky review <plan-id> --approve`.
///
/// Internal artifact, not a CLI output — it only needs serde, not
/// `JsonSchema` (deliberate: the typed status surface is
/// [`crate::output::ReviewStatusOutput`], projected from this). The apply
/// gate checks it via [`review_marker_state`]: the marker must exist, parse,
/// AND name the plan being applied — a truncated or mispasted marker never
/// approves.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReviewMarker {
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

/// What the on-disk review marker for a plan amounts to.
///
/// The single oracle behind the apply gates, the review queue's
/// reviewed-filter, and `rocky review --status` — so "approved" means the
/// same thing everywhere: the marker exists, parses as a [`ReviewMarker`],
/// and its `plan_id` matches the plan being asked about.
#[derive(Debug)]
pub(crate) enum ReviewMarkerState {
    /// No marker on disk — the plan awaits review.
    Absent,
    /// A well-formed marker naming this exact plan.
    Approved(ReviewMarker),
    /// A marker file exists but is unreadable, unparseable, or names a
    /// DIFFERENT plan. Never approves; the apply gates refuse it with a
    /// distinct error rather than reading it as "not reviewed".
    Invalid { reason: String },
}

/// Read and classify the review marker for `plan_id` under `root`.
pub(crate) fn review_marker_state(root: &Path, plan_id: &str) -> ReviewMarkerState {
    let path = review_marker_path(root, plan_id);
    let bytes = match std::fs::read(&path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return ReviewMarkerState::Absent,
        Err(e) => {
            return ReviewMarkerState::Invalid {
                reason: format!("the marker at {} is unreadable: {e}", path.display()),
            };
        }
        Ok(bytes) => bytes,
    };
    match serde_json::from_slice::<ReviewMarker>(&bytes) {
        Err(e) => ReviewMarkerState::Invalid {
            reason: format!(
                "the marker at {} does not parse as a review marker: {e}",
                path.display()
            ),
        },
        Ok(marker) if marker.plan_id != plan_id => ReviewMarkerState::Invalid {
            reason: format!(
                "the marker at {} approves plan '{}', not '{plan_id}'",
                path.display(),
                marker.plan_id
            ),
        },
        Ok(marker) => ReviewMarkerState::Approved(marker),
    }
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
/// Whether `rocky review` — and `GET /api/v1/review/{plan_id}` — has anything
/// to say about this plan.
///
/// Review applies to the kinds an apply gates on `require_review`: AI-authored
/// plans, agent-authored run plans, backfills, and the marker-only kinds (gc,
/// restore, compact, archive). A human-authored run plan is never gated, so
/// reviewing one would write a marker that means nothing.
///
/// Public because the HTTP route needs the same answer *before* it calls
/// [`compute_review`], to refuse with `409` rather than read its own refusal
/// out of an error message — a rule stated twice drifts, and a rule matched by
/// text drifts silently.
pub fn plan_is_reviewable(plan: &crate::plan_store::PersistedPlan) -> bool {
    matches!(
        plan.kind,
        PlanKind::AiAuthored
            | PlanKind::Backfill
            | PlanKind::Gc
            | PlanKind::Restore
            | PlanKind::Compact
            | PlanKind::Archive
    ) || (plan.kind == PlanKind::Run
        && plan.resolved_principal() == rocky_core::config::PolicyPrincipal::Agent)
}

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
    // the identical `RunPlan` payload shape — and to the marker-only kinds
    // (gc, restore, compact, archive), whose apply gates on `require_review`
    // the same way. Human-authored plans are never gated, so reviewing one is a
    // no-op the guard rejects rather than silently writing a marker that means
    // nothing.
    if !plan_is_reviewable(&plan) {
        bail!(
            "plan '{plan_id}' is a {} plan authored by {}; `rocky review` only applies to \
             AI-authored plans, agent-authored run plans, backfills, gc plans, restore plans, \
             and compact / archive plans.",
            plan.kind,
            serde_json::to_value(plan.resolved_principal())
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| "human".to_string()),
        );
    }

    // The marker-only kinds (gc / restore / compact / archive) carry their own
    // payload shape (not a `RunPlan`) and change no model definitions, so the
    // breaking-change gate does not apply — reviewing is purely the human
    // sign-off that unblocks the apply. Branch here, before the `RunPlan`
    // deserialize the other kinds need.
    if matches!(
        plan.kind,
        PlanKind::Gc | PlanKind::Restore | PlanKind::Compact | PlanKind::Archive
    ) {
        return compute_review_marker_only(root, plan_id, approve, &plan.kind).await;
    }

    let run_plan: RunPlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize plan payload")?;

    // Resolve the plan's models directory against the project `root`, never
    // the process cwd — the governor's MCP server reviews from a cwd that is
    // not the project, and a cwd-relative path would silently skip the
    // breaking-change gate there.
    let (models_dir, state_path) = review_gate_paths(root, run_plan.models_dir.as_deref());

    // `?` here is the whole point of the change: a present-but-unloadable
    // `rocky.toml` propagates BEFORE the `--approve` branch below, so no marker
    // is written and no zero count is recorded.
    let findings = compute_review_findings(config_path, &models_dir, &state_path, base_ref)?;
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

/// Review (and optionally approve) a `PlanKind::Gc` reclamation plan or a
/// `PlanKind::Restore` restoration plan — and, since the compact/archive apply
/// policy gate landed, `PlanKind::Compact` / `PlanKind::Archive` maintenance
/// plans.
///
/// None of these change model definitions, so there is no breaking-change gate
/// to compute — reviewing is the human sign-off that unblocks the apply.
/// `--approve` writes the same marker (`<plan_id>.reviewed.json`) the apply gate
/// checks; without it, apply stays blocked. The marker is payload-agnostic, so
/// the existing `ai_plan_is_reviewed` gate in `commands::apply` recognises it —
/// which is exactly what clears a `require_review` verdict on a compact/archive
/// apply.
async fn compute_review_marker_only(
    root: &Path,
    plan_id: &str,
    approve: bool,
    kind: &PlanKind,
) -> Result<ReviewOutput> {
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
            kind = %kind,
            "marker-only plan approved — review marker written"
        );
    }

    let message = match (kind, approve) {
        (PlanKind::Restore, true) => format!(
            "approved restore plan '{plan_id}' — `rocky apply {plan_id}` is now unblocked. Apply \
             rebuilds each artifact from its recorded recipe and asserts the recomputed blake3 \
             equals the tombstoned hash before any write becomes visible."
        ),
        (PlanKind::Restore, false) => format!(
            "reviewed restore plan '{plan_id}' — re-run with `--approve` to unblock \
             `rocky apply {plan_id}`. Restoration is symmetric-caution gated: even a human \
             restore goes through review."
        ),
        (PlanKind::Compact, true) => format!(
            "approved compact plan '{plan_id}' — `rocky apply {plan_id}` is now unblocked. \
             Apply runs the OPTIMIZE / VACUUM maintenance SQL against the target."
        ),
        (PlanKind::Compact, false) => format!(
            "reviewed compact plan '{plan_id}' — re-run with `--approve` to unblock \
             `rocky apply {plan_id}`."
        ),
        (PlanKind::Archive, true) => format!(
            "approved archive plan '{plan_id}' — `rocky apply {plan_id}` is now unblocked. \
             Apply runs the DELETE + VACUUM SQL against the target (a destructive operation)."
        ),
        (PlanKind::Archive, false) => format!(
            "reviewed archive plan '{plan_id}' — re-run with `--approve` to unblock \
             `rocky apply {plan_id}`. Archive is destructive (DELETE + VACUUM)."
        ),
        (_, true) => format!(
            "approved gc plan '{plan_id}' — `rocky apply {plan_id}` is now unblocked. Apply \
             re-verifies each artifact against the live ledger before evicting (a tombstone + \
             retired ledger row; no physical byte deletion), and every eviction is tombstoned \
             for restore."
        ),
        (_, false) => format!(
            "reviewed gc plan '{plan_id}' — re-run with `--approve` to unblock \
             `rocky apply {plan_id}`. Deletion is symmetric-caution gated: even a human gc goes \
             through review."
        ),
    };

    Ok(ReviewOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        plan_id: plan_id.to_string(),
        base_ref: String::new(),
        approved: approve,
        marker_written,
        breaking_changes: None,
        message: Some(message),
    })
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

/// Resolve the paths the review's breaking-change gate reads, anchored at the
/// project `root` rather than the process cwd.
///
/// - `models_dir`: the plan's recorded models directory (default `models`),
///   joined onto `root` (an already-absolute recorded path is used verbatim —
///   `Path::join` replaces on absolute).
/// - `state_path`: the schema-cache state store, resolved with the same
///   [`rocky_core::state::resolve_state_path`] defaulting the CLI and the MCP
///   server use (`<models_dir>/.rocky-state.redb` et al.) — not a hardcoded
///   cwd-relative file.
fn review_gate_paths(root: &Path, plan_models_dir: Option<&str>) -> (PathBuf, PathBuf) {
    let models_dir = root.join(plan_models_dir.unwrap_or("models"));
    let state_path = rocky_core::state::resolve_state_path(None, &models_dir).path;
    (models_dir, state_path)
}

/// Compute the breaking-change findings between `base_ref` and the working
/// tree, reusing the exact compile + classifier path that `rocky ci-diff`
/// and the branch-promote gate use.
///
/// Returns:
/// - `Ok(Some(findings))` when both refs compiled cleanly and the typed-IR diff
///   ran. `findings` is the full classified list including `Info`-severity
///   entries; callers filter on [`BreakingFinding::is_breaking`].
/// - `Ok(None)` when the gate was skipped because the models directory was
///   unavailable or either side failed to compile. Those are recoverable
///   conditions the approver can see; the marker is still written.
/// - `Err` when a `rocky.toml` is PRESENT and does not load (#1680). That is
///   not a skip: the schema cache the classifier types against is gated on the
///   config, so an unloadable config silently downgrades a type change to "no
///   breaking changes" — and `--approve` used to record that as a signed-off
///   zero. Refusing here is what keeps the marker's count honest, because the
///   caller propagates before any marker is written.
fn compute_review_findings(
    config_path: &Path,
    models_dir: &Path,
    state_path: &Path,
    base_ref: &str,
) -> Result<Option<Vec<BreakingFinding>>> {
    use rocky_compiler::compile::{self, CompilerConfig};

    // The config is read BEFORE the models-dir check so a broken `rocky.toml`
    // refuses whether or not the project also has a models directory — the
    // refusal must not depend on which degradation is hit first.
    //
    // Credential-TOLERANT (`load_optional_project_config`): `rocky review`
    // compiles and diffs, it opens no warehouse connection, so an unset
    // `${DATABRICKS_HOST}` must not block a review (#1536). Under the strict
    // loader this site used before, that unset variable failed the load and
    // silently cooled the cache — the review then classified against `Unknown`
    // leaf types.
    let loaded_cfg = rocky_core::config::load_optional_project_config(Some(config_path))
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    if !models_dir.is_dir() {
        tracing::warn!(
            target: "rocky::review",
            models_dir = %models_dir.display(),
            "models directory missing — breaking-change gate skipped"
        );
        return Ok(None);
    }

    // Seed both compiles with the cached source schemas so the resulting IR
    // uses real types rather than `Unknown`. A project with NO `rocky.toml`
    // still degrades to an empty map, and so does a cold or unreadable cache:
    // those cost only type precision and are not the approver's to fix.
    let source_schemas = match loaded_cfg {
        Some(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        None => std::collections::HashMap::new(),
    };

    let head_compile = {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
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
                return Ok(None);
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
                return Ok(None);
            }
        };

    let base_ir = super::ci_diff::project_ir_from_compile(&base_compile);
    let head_ir = super::ci_diff::project_ir_from_compile(&head_compile);
    Ok(Some(breaking_change::diff_project_ir(&base_ir, &head_ir)))
}

/// Write the review marker to `<root>/.rocky/plans/<plan_id>.reviewed.json`.
///
/// Staged write: the bytes go to a `.tmp` sibling in the same directory and
/// land via an atomic rename, so a crash mid-write leaves NO marker (a
/// half-written marker would otherwise exist-but-not-parse, and the marker is
/// the human sign-off that unblocks `rocky apply`). A stale `.tmp` from a
/// crashed writer is overwritten by the next approval and never read by the
/// gate (only the exact `.reviewed.json` path is).
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
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, bytes)
        .with_context(|| format!("failed to stage review marker at {}", tmp.display()))?;
    std::fs::rename(&tmp, &path).with_context(|| {
        format!(
            "failed to move the staged review marker into place at {}",
            path.display()
        )
    })?;
    Ok(())
}

/// Test-support: write a WELL-FORMED review marker naming `plan_id`, exactly
/// as `rocky review --approve` would. Since the apply gate parses and matches
/// the marker (FF-WP1), test fixtures can no longer plant a bare `{}` — they
/// go through this so "approved" in a test means what it means in production.
#[cfg(test)]
pub(crate) fn write_test_review_marker(root: &Path, plan_id: &str) {
    let marker = ReviewMarker {
        plan_id: plan_id.to_string(),
        reviewed_at: Utc::now(),
        base_ref: "HEAD".to_string(),
        breaking_change_count: 0,
        approver: ApproverIdentity {
            email: "dev@example.com".to_string(),
            name: Some("Dev".to_string()),
            host: "localhost".to_string(),
            source: crate::output::ApproverSource::Local,
        },
    };
    write_review_marker(root, plan_id, &marker).expect("test marker write");
}

/// Compute the typed `rocky review <plan-id> --status` payload — the runner's
/// marker oracle.
///
/// Reads the plan (integrity-checked — a tampered plan file errors here, it
/// does not report a status) and classifies the marker via
/// [`review_marker_state`]. An absent marker is `reviewed: false`; a marker
/// that is malformed or names a different plan is an ERROR (the same distinct
/// refusal the apply gate raises), never a silent `false` a polling runner
/// would wait on forever.
pub fn compute_review_status(
    root: &Path,
    plan_id: &str,
) -> Result<crate::output::ReviewStatusOutput> {
    let plan = read_plan(root, plan_id)
        .with_context(|| format!("failed to read plan '{plan_id}' for status"))?;
    let product_id = plan
        .payload
        .get("product_id")
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let spec_digest = plan
        .payload
        .get("spec_digest")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let (reviewed, reviewed_at, approver, breaking_change_count) =
        match review_marker_state(root, plan_id) {
            ReviewMarkerState::Absent => (false, None, None, None),
            ReviewMarkerState::Approved(marker) => (
                true,
                Some(marker.reviewed_at),
                Some(marker.approver),
                Some(marker.breaking_change_count as u64),
            ),
            ReviewMarkerState::Invalid { reason } => bail!(
                "review marker for plan '{plan_id}' is invalid: {reason}. A truncated or \
                 mispasted marker never approves — re-approve with \
                 `rocky review {plan_id} --approve` to rewrite it atomically."
            ),
        };

    Ok(crate::output::ReviewStatusOutput {
        version: VERSION.to_string(),
        command: "review_status".to_string(),
        plan_id: plan_id.to_string(),
        kind: plan.kind.to_string(),
        reviewed,
        reviewed_at,
        approver,
        breaking_change_count,
        product_id,
        spec_digest,
    })
}

/// Execute `rocky review <plan-id> --status`.
pub fn run_review_status(config_path: &Path, plan_id: &str, output_json: bool) -> Result<()> {
    let _ = config_path; // status is plan+marker only; kept for CLI symmetry.
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    let output = compute_review_status(&cwd, plan_id)?;
    if output_json {
        crate::output::print_json(&output)?;
    } else {
        let state = if output.reviewed {
            format!(
                "approved{}",
                output
                    .reviewed_at
                    .map(|t| format!(" at {t}"))
                    .unwrap_or_default()
            )
        } else {
            "pending review".to_string()
        };
        println!("plan {} ({}): {state}", output.plan_id, output.kind);
        if let (Some(product), Some(digest)) = (&output.product_id, &output.spec_digest) {
            println!("  product: {product} @ {digest}");
        }
    }
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
/// escalation and the plan files that make one approvable; the CLI resolves it
/// from the process cwd, the MCP server passes its project root. Reads only —
/// no stdout.
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

    let (pending, excluded_non_plan_rows) = build_queue(
        root,
        config_path,
        state_path,
        models_dir,
        &decisions,
        Utc::now(),
        MAX_HISTORY_SCAN,
    )?;

    Ok(ReviewQueueOutput {
        version: VERSION.to_string(),
        command: "review".to_string(),
        ranking: QUEUE_RANKING.to_string(),
        total: pending.len() as u64,
        excluded_non_plan_rows,
        pending,
    })
}

/// Assemble the ranked queue from the decision ledger, scanning at most the
/// `max_scan` **newest** ledger rows. Returns the ranked entries plus the
/// count of outstanding escalations excluded because no plan file backs them.
/// Factored out (with an injectable scan cap) so the ranking and the cap
/// semantics are unit-testable without a state store.
///
/// Refuses when `rocky.toml` is present and does not load (#1702). An absent
/// `rocky.toml` is still fine — it ranks against a cold schema cache, as it
/// always did.
#[allow(clippy::too_many_arguments)]
fn build_queue(
    root: &Path,
    config_path: &Path,
    state_path: &Path,
    models_dir: &Path,
    decisions: &[PolicyDecisionRecord],
    now: DateTime<Utc>,
    max_scan: usize,
) -> Result<(Vec<ReviewQueueEntry>, u64)> {
    // Read the config FIRST — before the outstanding-selection short-circuit
    // below, and before the compile. `rocky review --queue` refuses a
    // present-but-unloadable `rocky.toml`, full stop; the refusal must not
    // depend on whether the ledger happens to hold a pending row today. The
    // cost is one read-only schema-cache scan on an otherwise-empty queue,
    // which is the price of a rule with no exceptions.
    //
    // The loader is the credential-tolerant one (`load_optional_project_config`
    // under `load_project_source_schemas`): `rocky review --queue` reads the
    // state store and compiles offline, and opens no warehouse connection.
    let source_schemas =
        crate::source_schemas::load_project_source_schemas(config_path, state_path, None)?;

    // `list_policy_decisions` yields oldest-first; scan the NEWEST `max_scan`
    // rows (matching `list_runs`' newest-first convention) so a long-lived
    // ledger never ages genuinely-pending escalations out of the queue.
    let (outstanding, excluded_non_plan) = select_outstanding(
        decisions.iter().rev().take(max_scan),
        |plan_id| ai_plan_is_reviewed(root, plan_id),
        |plan_id| plan_file_path(root, plan_id).exists(),
    );
    if outstanding.is_empty() {
        return Ok((Vec::new(), excluded_non_plan));
    }

    // One compile serves every model's blast radius. A COMPILE failure still
    // leaves every blast radius unknown (weight-and-staleness-only ranking)
    // rather than failing the whole queue — that degrade is deliberate and
    // unchanged. Only the config leg above refuses, and it already ran.
    let compiled = compile_project_with_schemas(source_schemas, models_dir).ok();

    // Whether the samples route would get past its own front door for this
    // project: the strict config loader, the adapter registry, and pipeline
    // resolution with no `pipeline` parameter (the UI sends none). The queue
    // itself tolerates an unset credential and an absent config on purpose;
    // the route does not, and a queue that offered a read there would be
    // advertising an answer the route cannot give (#1815, review round
    // three). Decided once; it is the project's, not a row's.
    let route_front_door_open = rocky_core::config::load_rocky_config(config_path)
        .ok()
        .filter(|cfg| crate::registry::AdapterRegistry::from_config(cfg).is_ok())
        .is_some_and(|cfg| crate::registry::resolve_pipeline(&cfg, None).is_ok());

    let mut entries: Vec<ReviewQueueEntry> = outstanding
        .into_iter()
        .map(|d| {
            // One derivation serves both the ranking and the entry's `models`,
            // so what a consumer is told the row stands for is exactly what
            // the blast radius was computed over.
            let models = queue_graph_keys(d, compiled.as_ref());
            // What the samples route would read for this row, decided by the
            // route's own admission (`preview_rows::admit_model`) and not by
            // any consumer's reading of a name. `models` ranks and audits;
            // only this licenses an offer to read (#1815).
            let preview_model = match (models.as_slice(), compiled.as_ref()) {
                ([only], Some(result)) if route_front_door_open => {
                    crate::commands::preview_rows::admit_model(result, models_dir, only)
                        .ok()
                        .map(|_| only.clone())
                }
                _ => None,
            };
            // Deduplicated union, all-or-nothing: an absent member makes the
            // whole answer unknown rather than a partial count dressed as a
            // measurement, and no members at all is unknown too. See
            // `blast_radius_union` for why union and not `max`, and why the
            // plan's own models stay in.
            let blast_radius = compiled
                .as_ref()
                .and_then(|r| blast_radius_union(r, models.iter().map(String::as_str)))
                .map(|reached| reached.len() as u64);
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
                models,
                preview_model,
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
    Ok((entries, excluded_non_plan))
}

/// The pending escalations to surface: the latest `require_review` decision
/// per `(plan_id, model)` whose plan has not yet been signed off **and whose
/// plan actually exists to be approved**.
///
/// A re-evaluated plan appends a fresh ledger row each time, so the queue
/// keeps only the newest row per `(plan_id, model)` — current state, not
/// history — and drops any whose plan already carries a review marker
/// (`is_reviewed`).
///
/// Decision-only custody rows never map to a persisted plan: the MCP draft
/// tools file refusals under `draft:*` / `draft-contract:*` / `draft-check:*`
/// ids and the drift auto-apply path files evaluations under
/// `autoapply:<run_id>`. Nothing can approve those (`rocky review` bails at
/// `read_plan`), so surfacing them would pollute the queue with permanently
/// unclearable entries. The `plan_exists` probe drops them from the *queue*
/// while they remain in the *ledger* (the audit history); the count of
/// dropped rows is returned so the queue can say so.
///
/// Pure over the ledger + the two predicates so the dedup/filter logic is
/// testable without a state store. Accepts any iteration order — the dedup
/// keeps the newest row per key regardless.
pub(crate) fn select_outstanding<'a>(
    decisions: impl IntoIterator<Item = &'a PolicyDecisionRecord>,
    is_reviewed: impl Fn(&str) -> bool,
    plan_exists: impl Fn(&str) -> bool,
) -> (Vec<&'a PolicyDecisionRecord>, u64) {
    let mut latest: BTreeMap<(&str, &str), &PolicyDecisionRecord> = BTreeMap::new();
    // A `deny` row is left out of the latest-row pick, so it neither queues
    // nor supersedes. It cannot supersede: the fail-closed path records a
    // `deny` when the ledger snapshot could not be read — an operational
    // refusal, not a policy decision about the plan — and the row cannot say
    // which kind it is. Letting it supersede hid an escalation the policy
    // still required until someone retried the mutation (#1815, review round
    // three). A superseded-by-deny escalation stays approvable, which is what
    // it was before; approval then meets the deny at apply, loudly.
    for d in decisions
        .into_iter()
        .filter(|d| d.effect != PolicyEffect::Deny)
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
    let mut excluded_non_plan: u64 = 0;
    let outstanding = latest
        .into_values()
        // The LATEST of `require_review` and `allow` per key decides. A
        // `require_review` that a later `allow` for the same (plan, model)
        // superseded is history: policy loosened, the plan re-ran, approving
        // it is moot. Filtering to `require_review` BEFORE picking the
        // latest kept such rows approvable (#1815, review round two).
        .filter(|d| d.effect == PolicyEffect::RequireReview)
        .filter(|d| !is_reviewed(&d.plan_id))
        .filter(|d| {
            if plan_exists(&d.plan_id) {
                true
            } else {
                excluded_non_plan += 1;
                false
            }
        })
        .collect();
    (outstanding, excluded_non_plan)
}

/// The graph keys a queue row stands for — what the ranking resolves and what
/// the entry reports as `models`, from one derivation so the two cannot drift.
///
/// The recorded set when there is one: producers write it when they know
/// their subject is a compiled model (the apply-time gate, since #1815) or
/// which models a plan-level escalation covers (#1766). Otherwise the row's
/// bare `model` is a key only if the compiled graph has it. Nothing about the
/// string itself can say: the same field holds a compiled model's name, a
/// replication target's table name (gated under bare `apply`, never a model)
/// and, on a pre-v28 plan-level row, a label — and a model name may carry a
/// dot (`v2.fct_orders`), so an identifier check fails both ways. Two rules
/// were tried and rejected on the record: the capability (the gate records
/// ordinary rows with the plan's capability) and identifier syntax (admits a
/// replication target, rejects a dotted name).
///
/// No keys means unknown: a subject no graph can name, a model since removed,
/// a compile that failed, or a row from before its producer recorded the set.
/// `graph_keys` is deliberately not used here: it also yields the label, on
/// purpose, so `audit --for` can match it.
///
/// **The producer's word stands.** A row whose producer recorded its set
/// (`keys_recorded`) is taken as written, an empty set included: the gate
/// wrote "no compiled model" for a replication target, and resolving its
/// bare name against the graph anyway turned a target called `orders` into
/// the compiled model `orders` and offered its rows (#1815, review round
/// seven). Only a row that never said — pre-v29 — is resolved by the graph.
fn queue_graph_keys(
    d: &PolicyDecisionRecord,
    compiled: Option<&rocky_compiler::compile::CompileResult>,
) -> Vec<String> {
    if !d.models.is_empty() || d.keys_recorded {
        return d.models.clone();
    }
    let in_graph = compiled.is_some_and(|r| r.semantic_graph.model_schema(&d.model).is_some());
    if in_graph {
        vec![d.model.clone()]
    } else {
        Vec::new()
    }
}

/// Record the "this plan awaits review" escalation for an unconditionally
/// review-gated plan (gc / backfill / restore) at **plan creation**.
///
/// Those plans never pass through `evaluate_apply_policy` before their apply
/// bails on the missing review marker, so without this row the decision-driven
/// review queue (and the governor's MCP approve path) would never list them —
/// even though `compute_review` was built to approve them. One plan-level row
/// (a representative `model` summary, not one row per affected model) keeps
/// the ledger lean.
///
/// `model_summary` is the human label — `"backfill: 3 model(s)"` — and is what
/// the queue and the `decision_ref` display. `models` carries the graph keys
/// behind it. The two are separate on purpose: the summary is not a model name,
/// so a ranking or a `--for <model>` match that reads it finds nothing (#1766).
/// Pass every model the plan touches; passing an empty slice records the row
/// with no keys, which reads back as "blast radius unknown" — honest, but it
/// forfeits the ranking, so do it only when the set genuinely cannot be
/// resolved.
///
/// Best-effort like every other ledger write: the review-marker gate at apply
/// is the safety boundary, the ledger is the trail — a locked or unreadable
/// state store must not fail plan creation.
pub(crate) fn record_plan_review_escalation(
    state_path: &Path,
    plan_id: &str,
    principal: PolicyPrincipal,
    capability: PolicyCapability,
    model_summary: &str,
    models: Vec<String>,
    reason: &str,
) {
    let record = PolicyDecisionRecord {
        // The plan-level writer names its set on purpose.
        keys_recorded: true,
        models,
        timestamp: Utc::now(),
        plan_id: plan_id.to_string(),
        principal,
        capability,
        model: model_summary.to_string(),
        effect: PolicyEffect::RequireReview,
        rule_id: None,
        reason: reason.to_string(),
        verify_after: Vec::new(),
        auto_apply: None,
    };
    let written = StateStore::open(state_path).and_then(|s| s.record_policy_decision(&record));
    if let Err(e) = written {
        tracing::warn!(
            target: "rocky::review",
            plan_id,
            error = %e,
            "failed to record the plan's review escalation to the ledger (continuing) — \
             the plan is still review-gated at apply, but the review queue will not list it"
        );
    }
}

/// Composite priority score: `(blast + 1) × classification_weight ×
/// (1 + staleness_hours)`. Higher sorts first. An unknown blast radius
/// contributes as zero so the entry still ranks on class and age rather than
/// dropping out.
///
/// **What "unknown" covers**, stated in full because an earlier version of this
/// comment named only the first two and was therefore false: the project failed
/// to compile; the model is gone from the graph; the row is a pre-v28
/// plan-level escalation, which recorded no model set and whose `model` is a
/// label no graph resolves; or the row named models and **any one of them** is
/// absent. That last case is all-or-nothing on purpose — a partly resolvable
/// plan is unknown, not a small number. Zero is the ranking's honest floor for
/// every one of those, and it is not a measured radius of zero, which is
/// `Some(0)` and means the plan really does reach nothing.
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
        render_excluded_note(out.excluded_non_plan_rows);
        return;
    }
    println!(
        "review queue: {} escalation(s) awaiting review (ranked by {})",
        out.total, out.ranking
    );
    render_excluded_note(out.excluded_non_plan_rows);
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
        // The label above is display text; on a plan-level row the names are
        // in `models`, and the JSON carries them, so the text does too.
        if e.models.as_slice() != std::slice::from_ref(&e.model) {
            let listed = if e.models.is_empty() {
                "none the compiled graph can name".to_string()
            } else {
                e.models.join(", ")
            };
            println!("     models: {listed}");
        }
        println!("     approve: {}", e.approve_command);
    }
}

/// One-line footnote for custody rows the queue excluded because no plan file
/// backs them (nothing to approve). Silent when there are none.
fn render_excluded_note(excluded: u64) {
    if excluded > 0 {
        println!(
            "  ({excluded} decision-only custody row(s) excluded — no persisted plan to approve; \
             they remain in `rocky audit`)"
        );
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

    /// FF-WP1 ⟦RTL-6⟧ marker atomicity: the crash window between the tmp
    /// write and the rename leaves NO marker — the on-disk residue of that
    /// crash (a staged `.tmp`, no `.reviewed.json`) reads as Absent, so apply
    /// still refuses. And once renamed, the marker is whole by construction.
    #[test]
    fn a_crashed_staged_write_leaves_no_marker() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = "c".repeat(64);
        let marker_path = review_marker_path(dir.path(), &plan_id);

        // The exact bytes write_review_marker stages, parked at the exact tmp
        // path it uses — the disk state of a crash between write and rename.
        let tmp = marker_path.with_extension("json.tmp");
        std::fs::create_dir_all(marker_path.parent().unwrap())?;
        std::fs::write(&tmp, serde_json::to_vec_pretty(&dummy_marker(&plan_id))?)?;

        assert!(!marker_path.exists(), "no marker may exist mid-crash");
        assert!(
            matches!(
                review_marker_state(dir.path(), &plan_id),
                ReviewMarkerState::Absent
            ),
            "the staged tmp is never read as an approval"
        );
        assert!(
            !ai_plan_is_reviewed(dir.path(), &plan_id),
            "apply's gate still refuses"
        );

        // The completed write lands atomically over the same path.
        write_review_marker(dir.path(), &plan_id, &dummy_marker(&plan_id))?;
        assert!(marker_path.exists());
        assert!(
            matches!(
                review_marker_state(dir.path(), &plan_id),
                ReviewMarkerState::Approved(_)
            ),
            "the renamed marker approves"
        );
        Ok(())
    }

    /// FF-WP1 ⟦RTL-6⟧ parse-and-match: a marker that is malformed, or that
    /// names a DIFFERENT plan, is Invalid — never Approved, never Absent.
    #[test]
    fn malformed_or_mismatched_markers_are_invalid_not_approved() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plan_id = "d".repeat(64);
        let marker_path = review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker_path.parent().unwrap())?;

        // Truncated / malformed bytes.
        std::fs::write(&marker_path, b"{\"plan_id\": \"dddd")?;
        let state = review_marker_state(dir.path(), &plan_id);
        assert!(
            matches!(&state, ReviewMarkerState::Invalid { reason } if reason.contains("parse")),
            "malformed marker must be Invalid: {state:?}"
        );
        assert!(!ai_plan_is_reviewed(dir.path(), &plan_id));

        // Well-formed marker approving a DIFFERENT plan, copied to this
        // plan's marker path (the mispaste / copy attack).
        let other_id = "e".repeat(64);
        std::fs::write(
            &marker_path,
            serde_json::to_vec_pretty(&dummy_marker(&other_id))?,
        )?;
        let state = review_marker_state(dir.path(), &plan_id);
        assert!(
            matches!(&state, ReviewMarkerState::Invalid { reason } if reason.contains(&other_id)),
            "a mismatched marker must be Invalid and name the plan it actually approves: {state:?}"
        );
        assert!(
            !ai_plan_is_reviewed(dir.path(), &plan_id),
            "a copied marker from another plan never approves this one"
        );
        Ok(())
    }

    /// FF-WP1: `rocky review --status` — the runner's typed marker oracle.
    /// Pending → reviewed:false with the product binding surfaced from the
    /// plan payload; approved → the marker's identity fields; a malformed
    /// marker is an ERROR, never a silent false.
    #[test]
    fn review_status_reports_pending_then_approved_and_errors_on_invalid() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = serde_json::json!({
            "parallel": 1,
            "product_id": "product:revenue_daily",
            "spec_digest": "sha256:abc",
        });
        let plan_id = crate::plan_store::write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;

        // Pending.
        let status = compute_review_status(dir.path(), &plan_id)?;
        assert_eq!(status.command, "review_status");
        assert_eq!(status.plan_id, plan_id);
        assert_eq!(status.kind, "ai_authored");
        assert!(!status.reviewed);
        assert!(status.reviewed_at.is_none());
        assert!(status.approver.is_none());
        assert_eq!(status.product_id.as_deref(), Some("product:revenue_daily"));
        assert_eq!(status.spec_digest.as_deref(), Some("sha256:abc"));

        // Approved.
        write_review_marker(dir.path(), &plan_id, &dummy_marker(&plan_id))?;
        let status = compute_review_status(dir.path(), &plan_id)?;
        assert!(status.reviewed);
        assert!(status.reviewed_at.is_some());
        assert_eq!(
            status.approver.as_ref().map(|a| a.email.as_str()),
            Some("dev@example.com")
        );
        assert_eq!(status.breaking_change_count, Some(0));

        // Invalid marker → hard error (a polling runner must not wait on a
        // silent false), naming the marker problem.
        std::fs::write(review_marker_path(dir.path(), &plan_id), b"garbage")?;
        let err = compute_review_status(dir.path(), &plan_id)
            .expect_err("an invalid marker is an error, not a status");
        assert!(
            format!("{err:#}").contains("invalid"),
            "the error says the marker is invalid: {err:#}"
        );

        // And an unknown plan errors at the integrity-checked read.
        assert!(compute_review_status(dir.path(), &"f".repeat(64)).is_err());
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
            keys_recorded: false,
            models: Vec::new(),
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

        let (out, excluded) =
            select_outstanding(&decisions, |plan_id| plan_id == "planReviewed", |_| true);

        // Only planA/x survives: deny + allow excluded, planReviewed filtered.
        assert_eq!(out.len(), 1);
        assert_eq!(excluded, 0);
        let d = out[0];
        assert_eq!(d.plan_id, "planA");
        assert_eq!(d.model, "x");
        // The newest of the two planA/x rows wins (the breaking one at secs=9).
        assert_eq!(d.capability, PolicyCapability::SchemaChangeBreaking);
    }

    /// The latest of `require_review` and `allow` per (plan, model) decides.
    /// A `require_review` followed by an `allow` (policy loosened, plan
    /// re-run) is moot and used to stay in the queue, because the effect
    /// filter ran before the latest-row pick. A later `deny` does NOT
    /// supersede: a deny may be the fail-closed refusal of an unreadable
    /// ledger, which says nothing about the plan, and the row cannot tell
    /// the two apart — so the escalation stays, as it always did. The
    /// reverse orders queue: the newest row is the escalation.
    #[test]
    fn a_later_allow_supersedes_an_older_require_review_but_a_deny_does_not() {
        let decisions = vec![
            qd(
                1,
                "planA",
                "x",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
            qd(
                5,
                "planA",
                "x",
                PolicyEffect::Allow,
                PolicyCapability::Apply,
            ),
            qd(
                1,
                "planB",
                "y",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
            qd(5, "planB", "y", PolicyEffect::Deny, PolicyCapability::Apply),
            qd(
                1,
                "planC",
                "z",
                PolicyEffect::Allow,
                PolicyCapability::Apply,
            ),
            qd(
                5,
                "planC",
                "z",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
            qd(1, "planD", "w", PolicyEffect::Deny, PolicyCapability::Apply),
            qd(
                5,
                "planD",
                "w",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
        ];
        let (out, excluded) = select_outstanding(&decisions, |_| false, |_| true);
        assert_eq!(excluded, 0);
        let mut plans: Vec<&str> = out.iter().map(|d| d.plan_id.as_str()).collect();
        plans.sort_unstable();
        assert_eq!(
            plans,
            vec!["planB", "planC", "planD"],
            "an allow supersedes; a deny does not; new escalations queue"
        );
    }

    /// FIX: decision-only custody rows (`draft:*`, `autoapply:*`, …) whose
    /// plan_id resolves to no persisted plan must not render as approvable
    /// queue items — `rocky review --approve` bails at `read_plan` for them,
    /// so they would sit pending forever. They stay in the ledger; the queue
    /// counts them out.
    #[test]
    fn select_outstanding_excludes_planless_custody_rows_and_counts_them() {
        let decisions = vec![
            qd(
                1,
                "draft:orders_daily",
                "orders_daily",
                PolicyEffect::RequireReview,
                PolicyCapability::SchemaChangeBreaking,
            ),
            qd(
                2,
                "draft-contract:orders_daily",
                "orders_daily",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
            qd(
                3,
                "autoapply:run-abc",
                "raw_events",
                PolicyEffect::RequireReview,
                PolicyCapability::SchemaChangeAdditive,
            ),
            // A real planned escalation — its plan file exists.
            qd(
                4,
                "planReal",
                "dim_customer",
                PolicyEffect::RequireReview,
                PolicyCapability::Apply,
            ),
        ];

        let (out, excluded) =
            select_outstanding(&decisions, |_| false, |plan_id| plan_id == "planReal");

        assert_eq!(out.len(), 1, "only the plan-backed escalation surfaces");
        assert_eq!(out[0].plan_id, "planReal");
        assert_eq!(excluded, 3, "the three custody-only rows are counted out");
    }

    /// FIX: the scan cap must keep the NEWEST rows. `list_policy_decisions`
    /// returns oldest-first, so a naive head-`take` would silently age the
    /// newest genuinely-pending escalations out of a >cap ledger.
    #[test]
    fn queue_scan_cap_keeps_newest_rows() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        // Every row references a real plan file so the existence filter is not
        // what excludes anything here.
        let plans_dir = root.join(".rocky").join("plans");
        std::fs::create_dir_all(&plans_dir).unwrap();
        std::fs::write(plans_dir.join("planX.json"), "{}").unwrap();

        // Oldest-first ledger, one model per row (m1 oldest … m5 newest).
        let decisions: Vec<PolicyDecisionRecord> = (1..=5)
            .map(|i| {
                qd(
                    i,
                    "planX",
                    &format!("m{i}"),
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                )
            })
            .collect();

        let now = Utc.with_ymd_and_hms(2026, 7, 7, 1, 0, 0).unwrap();
        let (entries, excluded) = build_queue(
            root,
            &root.join("rocky.toml"),
            &root.join("state.redb"),
            &root.join("models"),
            &decisions,
            now,
            2, // injected cap — production passes MAX_HISTORY_SCAN
        )
        .expect("no rocky.toml is written here, so the config leg must not refuse");

        assert_eq!(excluded, 0);
        assert_eq!(entries.len(), 2, "only the capped scan window surfaces");
        let models: Vec<&str> = entries.iter().map(|e| e.model.as_str()).collect();
        assert!(
            models.contains(&"m5"),
            "the newest pending row must appear: {models:?}"
        );
        assert!(
            models.contains(&"m4"),
            "the second-newest pending row must appear: {models:?}"
        );
        assert!(
            !models.contains(&"m1"),
            "rows beyond the cap are the OLDEST, not the newest: {models:?}"
        );
    }

    /// FIX: the review gate's paths anchor at the project root, not the
    /// process cwd — a governor MCP approve runs from an unrelated cwd.
    #[test]
    fn review_gate_paths_resolve_against_root_not_cwd() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("models")).unwrap();

        let (models_dir, state_path) = review_gate_paths(root, Some("models"));
        assert_eq!(models_dir, root.join("models"));
        assert!(
            models_dir.is_dir(),
            "root-joined models dir must be found regardless of cwd"
        );
        // The schema-cache state path follows the CLI/MCP default
        // (`<models_dir>/.rocky-state.redb`), not the old hardcoded
        // cwd-relative `.rocky/state.redb`.
        assert_eq!(state_path, root.join("models").join(".rocky-state.redb"));

        // An absolute recorded models_dir is used verbatim.
        let (abs_dir, _) = review_gate_paths(
            Path::new("/somewhere/else"),
            Some(root.join("models").to_str().unwrap()),
        );
        assert_eq!(abs_dir, root.join("models"));

        // Default when the plan recorded none.
        let (default_dir, _) = review_gate_paths(root, None);
        assert_eq!(default_dir, root.join("models"));
    }

    /// FIX: an approved plan's later apply-time re-evaluation rows (same
    /// plan_id) must not resurrect it in the queue — the marker filter clears
    /// every row of a reviewed plan, whatever model the row names.
    #[test]
    fn approved_plan_does_not_relist_after_apply_records_more_rows() {
        let decisions = vec![
            qd(
                1,
                "planB",
                "a",
                PolicyEffect::RequireReview,
                PolicyCapability::Backfill,
            ),
            // Post-approval apply evaluated policy again and recorded fresh
            // rows under the same plan.
            qd(
                5,
                "planB",
                "a",
                PolicyEffect::RequireReview,
                PolicyCapability::Backfill,
            ),
            qd(
                6,
                "planB",
                "b",
                PolicyEffect::RequireReview,
                PolicyCapability::Backfill,
            ),
        ];
        let (out, excluded) = select_outstanding(&decisions, |pid| pid == "planB", |_| true);
        assert!(out.is_empty(), "a reviewed plan never re-lists: {out:?}");
        assert_eq!(excluded, 0);
    }

    // ------------------------------------------------------------------
    // #1680 — the priority case.
    //
    // `rocky review <plan> --approve` on a PRESENT-but-unloadable
    // `rocky.toml` used to fold every config error into an empty schema map,
    // classify a real type change as `Unknown`-vs-`Unknown`, find nothing,
    // and then write a marker recording `breaking_change_count = 0`. That is
    // a signed-off zero for a gate that never ran.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1680: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// Loads under the credential-TOLERANT loader (#1536), refuses under the
    /// strict one this site used before.
    const UNSET_CREDENTIAL_CONFIG_1680: &str = "[adapters.wh]\ntype = \"databricks\"\n\
         host = \"${ROCKY_T_1680_REVIEW_UNSET}\"\n";

    /// Build a reviewable AI-authored plan in `root`, with a real models tree
    /// so the refusal cannot be confused with the missing-models-dir skip.
    fn seed_reviewable_plan(root: &Path) -> anyhow::Result<String> {
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir)?;
        std::fs::write(models_dir.join("m.sql"), "SELECT id FROM src.raw.t")?;
        let payload = serde_json::json!({ "parallel": 1, "models_dir": "models" });
        crate::plan_store::write_plan(root, PlanKind::AiAuthored, &payload)
    }

    /// FAIL-BEFORE: with a broken config, `--approve` must refuse and write NO
    /// marker. On unmodified production code this returns `Ok`, reports
    /// "no breaking changes", and leaves a marker on disk.
    #[tokio::test]
    async fn review_approve_refuses_an_unloadable_config_and_writes_no_marker() -> anyhow::Result<()>
    {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let plan_id = seed_reviewable_plan(root)?;
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, BROKEN_CONFIG_1680)?;

        let marker_path = review_marker_path(root, &plan_id);
        assert!(!marker_path.exists(), "no marker before the approve");

        let err = compute_review(root, &config_path, &plan_id, "HEAD", true)
            .await
            .expect_err("--approve on an unloadable rocky.toml must refuse");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("failed to load config from") && rendered.contains("rocky.toml"),
            "the refusal must name the config file, got: {rendered}"
        );

        // The load-bearing half: nothing was recorded.
        assert!(
            !marker_path.exists(),
            "a refused review must leave NO approval marker on disk"
        );
        assert!(
            !ai_plan_is_reviewed(root, &plan_id),
            "a refused review must not unblock `rocky apply`"
        );
        Ok(())
    }

    /// The dry run refuses too — the report it would print carries the same
    /// bogus zero, so `--approve` is not the only surface that must not lie.
    #[tokio::test]
    async fn review_dry_run_refuses_an_unloadable_config() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let plan_id = seed_reviewable_plan(root)?;
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, BROKEN_CONFIG_1680)?;

        assert!(
            compute_review(root, &config_path, &plan_id, "HEAD", false)
                .await
                .is_err(),
            "the dry run must refuse an unloadable config too"
        );
        Ok(())
    }

    /// HONEST FAILURE (a): a project with NO `rocky.toml` reviews exactly as
    /// before — the gate is skipped for want of a git base, and `--approve`
    /// still writes the marker.
    #[tokio::test]
    async fn review_approve_still_works_without_any_config() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let plan_id = seed_reviewable_plan(root)?;
        let config_path = root.join("rocky.toml");
        assert!(!config_path.exists());

        // Discriminate: the gate itself must SKIP on an absent config and
        // REFUSE on a broken one. Without this the assertion below would pass
        // for the wrong reason — the marker is written here anyway, because
        // the base compile has no git repo to read.
        let (models_dir, state_path) = review_gate_paths(root, Some("models"));
        assert!(
            compute_review_findings(&config_path, &models_dir, &state_path, "HEAD").is_ok(),
            "an absent rocky.toml must skip the gate, never refuse it"
        );
        let broken = root.join("broken.toml");
        std::fs::write(&broken, BROKEN_CONFIG_1680)?;
        assert!(
            compute_review_findings(&broken, &models_dir, &state_path, "HEAD").is_err(),
            "a present-but-broken rocky.toml must refuse the gate"
        );

        let out = compute_review(root, &config_path, &plan_id, "HEAD", true)
            .await
            .expect("a missing rocky.toml must not refuse a review");
        assert!(out.marker_written, "the marker must still be written");
        assert!(review_marker_path(root, &plan_id).exists());
        Ok(())
    }

    /// HONEST FAILURE (b): a VALID config whose adapter connection field holds
    /// an unset `${VAR}` must not refuse. This site used the STRICT loader, so
    /// that config used to fail the load and silently cool the schema cache —
    /// the classifier then typed every source leaf as `Unknown`. The tolerant
    /// loader accepts it, and the review proceeds.
    #[tokio::test]
    async fn review_approve_tolerates_an_unset_credential_var() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let plan_id = seed_reviewable_plan(root)?;
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, UNSET_CREDENTIAL_CONFIG_1680)?;

        // Pin that the loader swap is what decides this, in both directions.
        assert!(
            rocky_core::config::load_rocky_config(&config_path).is_err(),
            "fixture must fail the STRICT loader, or this proves nothing"
        );
        assert!(
            rocky_core::config::load_optional_project_config(Some(&config_path))
                .expect("the tolerant loader must accept an unset adapter credential")
                .is_some()
        );

        let out = compute_review(root, &config_path, &plan_id, "HEAD", true)
            .await
            .expect("an unset credential var must not refuse a review");
        assert!(out.marker_written, "the marker must still be written");
        Ok(())
    }

    /// The marker-only kinds short-circuit before any config read, so they
    /// still approve on a broken config. Deliberate: they change no model
    /// definitions, so there is no breaking-change gate to degrade. Pinned so
    /// the carve-out is a decision on the record, not an oversight.
    #[tokio::test]
    async fn marker_only_kinds_still_approve_on_an_unloadable_config() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let root = dir.path();
        let plan_id =
            crate::plan_store::write_plan(root, PlanKind::Gc, &serde_json::json!({ "a": 1 }))?;
        let config_path = root.join("rocky.toml");
        std::fs::write(&config_path, BROKEN_CONFIG_1680)?;

        let out = compute_review(root, &config_path, &plan_id, "HEAD", true)
            .await
            .expect("a gc plan carries no breaking-change gate to degrade");
        assert!(out.marker_written);
        Ok(())
    }

    // ------------------------------------------------------------------
    // `rocky review --queue` — the config leg (#1702)
    //
    // `build_queue` called `compile_project(..).ok()`, which re-swallowed the
    // refusal #1667 added inside `audit::compile_project`. A broken
    // `rocky.toml` therefore ranked EVERY escalation with "blast radius
    // unknown" — the same ranking a healthy project with no downstream models
    // produces. These tests drive `compute_review_queue`, the caller shared by
    // the CLI, `GET /api/v1/review/queue` and the governor's `review_queue`
    // MCP tool, so a `.ok()` put back at the call site fails them.
    // ------------------------------------------------------------------

    /// Parses as TOML, fails a validator: `fivetran` is discovery-only and
    /// needs `kind = "discovery"`. Present-and-broken, never absent.
    const BROKEN_CONFIG_1702: &str =
        "[adapter.ft]\ntype = \"fivetran\"\napi_key = \"k\"\napi_secret = \"s\"\n";

    /// Loads under the credential-TOLERANT loader (#1536), refuses under the
    /// strict one: `${...}` is unset and sits in an adapter connection field.
    const UNSET_CREDENTIAL_CONFIG_1702: &str = "[adapters.wh]\ntype = \"databricks\"\n\
         host = \"${ROCKY_T_1702_UNSET}\"\n";

    /// One outstanding `require_review` escalation, backed by a plan file so
    /// `select_outstanding` keeps it. Returns the state-store path.
    fn seed_pending_escalation(root: &Path) -> std::path::PathBuf {
        let plans = root.join(".rocky").join("plans");
        std::fs::create_dir_all(&plans).unwrap();
        std::fs::write(plans.join("planQ.json"), "{}").unwrap();

        let state_path = root.join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        store
            .record_policy_decision(&qd(
                1,
                "planQ",
                "fct_orders",
                PolicyEffect::RequireReview,
                PolicyCapability::SchemaChangeBreaking,
            ))
            .unwrap();
        state_path
    }

    // --- #1766: a plan-level escalation's blast radius ------------------
    //
    // `record_plan_review_escalation` puts a human SUMMARY in `model`
    // ("backfill: 3 model(s)", "gc: 9 artifact(s) across 4 model(s)",
    // "restore: orders (a1b2c3...)"). None of those is a graph key, so the
    // ranking's `blast_radius_of` lookup missed on every plan-level row and
    // the whole class ranked on class and age alone. The `models` set is what
    // it looks up now.

    /// A four-model graph on disk: `a -> b -> c` and `a -> d`.
    ///
    /// Transitive downstream counts: a=3, b=1, c=0, d=0.
    fn write_blast_graph(models_dir: &Path) {
        std::fs::create_dir_all(models_dir).unwrap();
        for (name, sql) in [
            ("a", "SELECT id FROM source.raw.t"),
            ("b", "SELECT id FROM a"),
            ("c", "SELECT id FROM b"),
            ("d", "SELECT id FROM a"),
        ] {
            std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
            std::fs::write(
                models_dir.join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
    }

    /// Back a plan id with a file so `select_outstanding` keeps its row.
    fn touch_plan_file(root: &Path, plan_id: &str) {
        let plans = root.join(".rocky").join("plans");
        std::fs::create_dir_all(&plans).unwrap();
        std::fs::write(plans.join(format!("{plan_id}.json")), "{}").unwrap();
    }

    /// **The fixture the plan asked for.** Two pending gc plans, ranked.
    ///
    /// This is the case nobody had checked: backfill's collision was
    /// documented, gc's was assumed to be "the same code shape" without
    /// anyone running it. It is the same USER-VISIBLE consequence — two gc
    /// plans, one that would delete a leaf and one that would delete a model
    /// three others read from, arriving in the queue indistinguishable.
    ///
    /// The discriminator is deliberate: `gc_aaa` (the harmless one) sorts
    /// FIRST on the plan-id tie-break, so under the old lookup — where both
    /// radii are unknown and both scores equal — it leads the queue. It only
    /// drops to second if the ranking actually resolved `gc_bbb`'s models.
    /// Reverting `graph_keys()` to `&d.model` makes this test fail.
    #[test]
    fn two_pending_gc_plans_rank_by_what_they_would_delete_not_by_age() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");

        // gc_aaa evicts from `c`, a leaf: nothing downstream.
        touch_plan_file(root, "gc_aaa");
        record_plan_review_escalation(
            &state_path,
            "gc_aaa",
            PolicyPrincipal::Human,
            PolicyCapability::Gc,
            "gc: 1 artifact(s) across 1 model(s)",
            vec!["c".to_string()],
            "gc plan awaits review",
        );
        // gc_bbb evicts from `b`, which `c` reads from.
        touch_plan_file(root, "gc_bbb");
        record_plan_review_escalation(
            &state_path,
            "gc_bbb",
            PolicyPrincipal::Human,
            PolicyCapability::Gc,
            "gc: 1 artifact(s) across 1 model(s)",
            vec!["b".to_string()],
            "gc plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        assert_eq!(out.total, 2);
        assert_eq!(
            out.pending[0].plan_id, "gc_bbb",
            "the gc plan with something downstream must outrank the leaf one; \
             it does not sort first on id, so only the blast radius can put it there"
        );
        assert_eq!(out.pending[0].blast_radius, Some(1));
        assert_eq!(out.pending[1].plan_id, "gc_aaa");
        assert_eq!(
            out.pending[1].blast_radius,
            Some(0),
            "a leaf's radius is a MEASURED zero, which must not read as unknown"
        );
        // The label is untouched, so the drill keys and the display are too.
        assert_eq!(out.pending[0].model, "gc: 1 artifact(s) across 1 model(s)");
        assert!(
            out.pending[0]
                .decision_ref
                .ends_with("|gc: 1 artifact(s) across 1 model(s)"),
            "the decision_ref still ends in the label: {}",
            out.pending[0].decision_ref
        );
    }

    /// A backfill's radius is the union over its models, and the order the
    /// set arrives in cannot change it.
    ///
    /// The seed `a` reaches `b`, `c` and `d`; the leaves reach nothing. The
    /// union is the same three whichever way the list is written, which is
    /// what a set-valued answer has to guarantee.
    #[test]
    fn a_backfill_ranks_on_the_union_of_its_models_whatever_the_order() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");

        for (plan_id, models) in [
            ("bf_widest_last", vec!["c", "d", "a"]),
            ("bf_widest_first", vec!["a", "c", "d"]),
        ] {
            touch_plan_file(root, plan_id);
            record_plan_review_escalation(
                &state_path,
                plan_id,
                PolicyPrincipal::Agent,
                PolicyCapability::Backfill,
                "backfill: 3 model(s)",
                models.into_iter().map(str::to_string).collect(),
                "backfill plan awaits review",
            );
        }

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        assert_eq!(out.total, 2);
        for e in &out.pending {
            assert_eq!(
                e.blast_radius,
                Some(3),
                "{} must rank on `a` (b, c, d downstream), not on a leaf",
                e.plan_id
            );
        }
    }

    /// **Union, not `max`.** Two disjoint roots reach twice as far as one.
    ///
    /// `blast_radius_of` takes a single name, so an aggregation rule had to be
    /// chosen, and `max` was the wrong one: it discards every closure but the
    /// biggest. A plan over `a` and `x` — three downstream models each, no
    /// overlap — reaches six models, and `max` reported three, the same number
    /// as a plan over `a` alone. The two plans were indistinguishable in the
    /// ranking, which is the defect #1766 exists to remove.
    ///
    /// Restoring `max` makes this fail. `sum` would pass here and fail
    /// `overlapping_closures_are_counted_once` below; only the deduplicated
    /// union passes both.
    #[test]
    fn two_disjoint_roots_reach_further_than_one() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        // Two unrelated trees: a -> {b, c, d} and x -> {y, z, w}.
        for (name, sql) in [
            ("a", "SELECT id FROM source.raw.t"),
            ("b", "SELECT id FROM a"),
            ("c", "SELECT id FROM b"),
            ("d", "SELECT id FROM a"),
            ("x", "SELECT id FROM source.raw.u"),
            ("y", "SELECT id FROM x"),
            ("z", "SELECT id FROM y"),
            ("w", "SELECT id FROM x"),
        ] {
            std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
            std::fs::write(
                models_dir.join(format!("{name}.toml")),
                format!(
                    "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
                ),
            )
            .unwrap();
        }
        let state_path = root.join("state.redb");

        // `bf_one` sorts FIRST on the plan-id tie-break, so under `max` — where
        // both score 3 — it led the queue. Only a wider measured radius on
        // `bf_two` can move it.
        touch_plan_file(root, "bf_one");
        record_plan_review_escalation(
            &state_path,
            "bf_one",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 1 model(s)",
            vec!["a".to_string()],
            "backfill plan awaits review",
        );
        touch_plan_file(root, "bf_two");
        record_plan_review_escalation(
            &state_path,
            "bf_two",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 2 model(s)",
            vec!["a".to_string(), "x".to_string()],
            "backfill plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        assert_eq!(out.total, 2);
        let radius = |plan: &str| {
            out.pending
                .iter()
                .find(|e| e.plan_id == plan)
                .unwrap_or_else(|| panic!("{plan} must be in the queue"))
                .blast_radius
        };
        assert_eq!(radius("bf_one"), Some(3), "a alone reaches b, c, d");
        assert_eq!(
            radius("bf_two"),
            Some(6),
            "a and x are disjoint, so the plan reaches all six — `max` said 3"
        );
        assert_eq!(
            out.pending[0].plan_id, "bf_two",
            "the wider plan leads; it does not sort first on id, so only the \
             radius can put it there"
        );
    }

    /// The union counts each reached model **once**, so an overlap is not
    /// double-counted.
    ///
    /// This is the half that rules out `sum`. `a` reaches `b`, `c`, `d`; `b`
    /// reaches `c`. A plan over both reaches three distinct models, not four.
    #[test]
    fn overlapping_closures_are_counted_once() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");

        touch_plan_file(root, "bf_overlap");
        record_plan_review_escalation(
            &state_path,
            "bf_overlap",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 2 model(s)",
            vec!["a".to_string(), "b".to_string()],
            "backfill plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        assert_eq!(
            out.pending[0].blast_radius,
            Some(3),
            "a reaches b, c, d and b reaches c — three distinct models, not four"
        );
    }

    /// **A partly resolvable plan is unknown, not a small number.**
    ///
    /// The sharpest case in the whole fix, and the one the first version got
    /// wrong. `c` is a live leaf, so `blast_radius_of("c")` returns
    /// `Some(empty)` — a real, measured zero. `deleted_since` is absent and
    /// returns `None`. Under a `filter_map` + `max` the absent member was
    /// simply dropped, leaving `Some(0)`, and the queue rendered
    /// "0 downstream" for a plan half of which could not be looked up at all.
    ///
    /// A measured zero and an unknown are different answers. Only one of them
    /// is true here, and it is not the one that reads as a measurement.
    ///
    /// The second half is the discriminator: the SAME leaf alone really does
    /// measure zero, so this test cannot pass by making every leaf unknown.
    #[test]
    fn a_partly_resolvable_plan_is_unknown_not_a_measured_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");

        // A live leaf PLUS a model the graph no longer has.
        touch_plan_file(root, "gc_partial");
        record_plan_review_escalation(
            &state_path,
            "gc_partial",
            PolicyPrincipal::Human,
            PolicyCapability::Gc,
            "gc: 2 artifact(s) across 2 model(s)",
            vec!["c".to_string(), "deleted_since".to_string()],
            "gc plan awaits review",
        );
        // The same leaf on its own.
        touch_plan_file(root, "gc_leaf_only");
        record_plan_review_escalation(
            &state_path,
            "gc_leaf_only",
            PolicyPrincipal::Human,
            PolicyCapability::Gc,
            "gc: 1 artifact(s) across 1 model(s)",
            vec!["c".to_string()],
            "gc plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        let radius = |plan: &str| {
            out.pending
                .iter()
                .find(|e| e.plan_id == plan)
                .unwrap_or_else(|| panic!("{plan} must be in the queue"))
                .blast_radius
        };
        assert_eq!(
            radius("gc_partial"),
            None,
            "one member of this plan cannot be resolved, so the radius is \
             unknown — never a zero that reads as a measurement"
        );
        assert_eq!(
            radius("gc_leaf_only"),
            Some(0),
            "the same leaf alone IS a measured zero, so unknown is not simply \
             what every leaf now returns"
        );
    }

    /// **The empty set is reachable, and it must rank as unknown.**
    ///
    /// Three ways to get here, and all of them end in the same honest answer:
    /// a pre-v28 row (no `models` key in the blob at all), a row whose models
    /// were all deleted from the graph since, and a caller that resolved no
    /// model set to record. None of them may produce a measured `Some(0)` —
    /// that would claim "nothing downstream" about a plan nobody could look
    /// up, and rank it above a real leaf on the tie-break.
    #[test]
    fn a_plan_level_row_with_no_resolvable_model_ranks_unknown_not_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");

        // (1) Empty set — a pre-v28 row reads back exactly like this.
        touch_plan_file(root, "p_empty");
        record_plan_review_escalation(
            &state_path,
            "p_empty",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 2 model(s)",
            Vec::new(),
            "backfill plan awaits review",
        );
        // (2) A set naming only models the graph does not have.
        touch_plan_file(root, "p_gone");
        record_plan_review_escalation(
            &state_path,
            "p_gone",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 1 model(s)",
            vec!["deleted_since".to_string()],
            "backfill plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue must rank");
        assert_eq!(
            out.total, 2,
            "both rows still LIST — unknown is not a drop-out"
        );
        for e in &out.pending {
            assert_eq!(
                e.blast_radius, None,
                "{} resolves no model, so its radius is unknown, never a measured zero",
                e.plan_id
            );
        }
        // And the entry says which it is. The pre-v28 row's `model` is a label
        // that resolves nowhere, and the entry does not hand it over as if it
        // were a name: `models` is empty, which is the documented "unknown".
        let by_plan = |plan: &str| {
            out.pending
                .iter()
                .find(|e| e.plan_id == plan)
                .unwrap_or_else(|| panic!("{plan} must be listed"))
        };
        assert!(
            by_plan("p_empty").models.is_empty(),
            "a pre-v28 plan-level row reports no keys, never its label as one"
        );
        assert_eq!(by_plan("p_empty").model, "backfill: 2 model(s)");
        assert_eq!(by_plan("p_gone").models, vec!["deleted_since".to_string()]);
    }

    /// The entry's `models` is the set the ranking resolved, from the same
    /// derivation, so a consumer that wants a model NAME reads it from there
    /// and never parses the label. Three shapes, one rule each:
    ///
    /// - an ordinary evaluation row: `models` is the one graph key, which is
    ///   `model` itself;
    /// - a plan-level row with its set: `models` is that set, and `model`
    ///   stays the label;
    /// - a plan-level row without one (covered above): `models` is empty.
    ///
    /// The UI used a `^[a-zA-Z0-9_]+$` regex on `model` to decide whether it
    /// could sample it, which accepted `"backfill_3_models"` as a name and
    /// would have sampled a real model of that name (#1815). Restoring the
    /// label as the ordinary row's key, dropping the set from a plan-level
    /// row, or deciding by capability or by identifier syntax instead of by
    /// the compiled graph, makes this fail: the dotted model below is a real
    /// graph key an identifier check rejects, and the replication target is
    /// an identifier no graph has.
    #[test]
    fn the_entry_reports_the_graph_keys_the_ranking_resolved() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let config_path = write_single_pipeline_config(root);
        let state_path = root.join("state.redb");

        // An ordinary evaluation row: `model` is the graph key, the set empty.
        touch_plan_file(root, "ordinary");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    1,
                    "ordinary",
                    "a",
                    PolicyEffect::RequireReview,
                    PolicyCapability::SchemaChangeBreaking,
                ))
                .unwrap();
        }
        // A model whose name carries a dot. Rocky loads it as a model, the
        // graph indexes it by that exact string, and an identifier check
        // rejects it — so the graph, not the syntax, must decide.
        std::fs::write(models_dir.join("v2.fct_orders.sql"), "SELECT id FROM a").unwrap();
        std::fs::write(
            models_dir.join("v2.fct_orders.toml"),
            "name = \"v2.fct_orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"fct_orders\"\n",
        )
        .unwrap();
        touch_plan_file(root, "dotted");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    4,
                    "dotted",
                    "v2.fct_orders",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                ))
                .unwrap();
        }
        // A replication target: gated under bare `apply` by TABLE name, which
        // is an identifier and is not a model. It must not be handed over as
        // one — the UI would offer to sample it, and a compiled model that
        // happened to share the name would be what got sampled.
        touch_plan_file(root, "replication");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    3,
                    "replication",
                    "raw_orders",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                ))
                .unwrap();
        }
        // The apply-time gate's row for a gc plan: the plan's capability, a
        // real model name, no set. A capability-based rule called this
        // plan-level and threw the name away; the name is the key.
        touch_plan_file(root, "gc_applied");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    2,
                    "gc_applied",
                    "b",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Gc,
                ))
                .unwrap();
        }
        // A plan-level row with its set; its `model` is a label that would
        // pass the identifier regex the UI used to trust.
        touch_plan_file(root, "bf");
        record_plan_review_escalation(
            &state_path,
            "bf",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill_2_models",
            vec!["b".to_string(), "d".to_string()],
            "backfill plan awaits review",
        );

        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("the queue must rank");
        let by_plan = |plan: &str| {
            out.pending
                .iter()
                .find(|e| e.plan_id == plan)
                .unwrap_or_else(|| panic!("{plan} must be listed"))
        };
        let ordinary = by_plan("ordinary");
        assert_eq!(ordinary.model, "a");
        assert_eq!(ordinary.models, vec!["a".to_string()]);
        // b, c, d and the dotted model below all sit downstream of `a`.
        assert_eq!(ordinary.blast_radius, Some(4), "ranked on `a`, the one key");

        let gc_applied = by_plan("gc_applied");
        assert_eq!(gc_applied.models, vec!["b".to_string()]);
        assert_eq!(
            gc_applied.blast_radius,
            Some(1),
            "a gc row over a real name ranks on it"
        );

        let dotted = by_plan("dotted");
        assert_eq!(dotted.models, vec!["v2.fct_orders".to_string()]);
        assert_eq!(
            dotted.blast_radius,
            Some(0),
            "a leaf the graph has: a measured zero"
        );

        let replication = by_plan("replication");
        assert!(
            replication.models.is_empty(),
            "a replication target is not a model, whatever its name looks like"
        );
        assert_eq!(replication.blast_radius, None);

        let bf = by_plan("bf");
        assert_eq!(bf.model, "backfill_2_models", "the label is untouched");
        assert_eq!(bf.models, vec!["b".to_string(), "d".to_string()]);
        // `b` reaches `c`; `d` is a leaf. Ranked on the label it would be
        // unknown (`None`), since no graph has a model called that.
        assert_eq!(
            bf.blast_radius,
            Some(1),
            "ranked on the set, not on the label"
        );

        // A graph key is not a licence to read: the samples route refuses a
        // dotted name, so the queue offers none — while `a` is readable, a
        // target is not a model, and two models are not one to read.
        assert_eq!(ordinary.preview_model.as_deref(), Some("a"));
        assert_eq!(dotted.preview_model, None);
        assert_eq!(replication.preview_model, None);
        assert_eq!(bf.preview_model, None);
    }

    /// `preview_model` follows the samples route's own admission, not the
    /// graph's: a model a restore plan recorded that is gone from the current
    /// project, and a model that no longer compiles, are graph keys the route
    /// would refuse, so no offer is made. Deciding from `models` alone makes
    /// this fail.
    #[test]
    fn the_entry_offers_a_read_only_where_the_samples_route_would_answer() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let config_path = write_single_pipeline_config(root);
        // A model whose SQL does not parse: in the project, with an error.
        std::fs::write(models_dir.join("shaky.sql"), "SELECT FROM WHERE").unwrap();
        std::fs::write(
            models_dir.join("shaky.toml"),
            "name = \"shaky\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"shaky\"\n",
        )
        .unwrap();
        let state_path = root.join("state.redb");

        // A restore plan's recorded set names a tombstoned model the current
        // project no longer has.
        touch_plan_file(root, "restore_gone");
        record_plan_review_escalation(
            &state_path,
            "restore_gone",
            PolicyPrincipal::Human,
            PolicyCapability::Restore,
            "restore: gone (abc123…)",
            vec!["gone".to_string()],
            "restore plan awaits review",
        );
        touch_plan_file(root, "shaky_plan");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    1,
                    "shaky_plan",
                    "shaky",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                ))
                .unwrap();
        }

        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("a model that fails to parse degrades the ranking, it does not refuse");
        let by_plan = |plan: &str| out.pending.iter().find(|e| e.plan_id == plan).unwrap();
        assert_eq!(by_plan("restore_gone").models, vec!["gone".to_string()]);
        assert_eq!(
            by_plan("restore_gone").preview_model,
            None,
            "not in the current project"
        );
        assert_eq!(
            by_plan("shaky_plan").preview_model,
            None,
            "the route refuses compile errors"
        );
    }

    /// The samples route has a front door the queue must not promise past:
    /// the strict config loader, the adapter registry, and pipeline
    /// resolution with no name. With two pipelines the UI (which sends no
    /// `pipeline`) would get `pipeline_error` on every click; with no config
    /// at all, `config_error`. The queue still ranks — its own tolerance is
    /// deliberate and pinned elsewhere — but offers no read. Dropping the
    /// front-door check makes this fail.
    #[test]
    fn the_entry_offers_no_read_where_the_routes_front_door_is_shut() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");
        touch_plan_file(root, "ordinary");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    1,
                    "ordinary",
                    "a",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                ))
                .unwrap();
        }

        // No config at all: the queue ranks, the route would refuse.
        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("the queue ranks without a config");
        assert_eq!(
            out.pending[0].models,
            vec!["a".to_string()],
            "still a graph key"
        );
        assert_eq!(
            out.pending[0].preview_model, None,
            "no config, no front door"
        );

        // Two pipelines: the route cannot pick one without a name.
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.q]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.q.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("the queue ranks with two pipelines");
        assert_eq!(
            out.pending[0].preview_model, None,
            "two pipelines, no name: shut"
        );

        // One pipeline: open, and `a` is readable.
        write_single_pipeline_config(root);
        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("the queue ranks with one pipeline");
        assert_eq!(out.pending[0].preview_model.as_deref(), Some("a"));
    }

    /// A config the samples route's front door accepts: the strict loader,
    /// the adapter registry, and pipeline resolution with no name. The queue
    /// tolerates more than the route does, so a queue that offers a read
    /// without this would be advertising an answer the route cannot give.
    fn write_single_pipeline_config(root: &Path) -> std::path::PathBuf {
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n",
        )
        .unwrap();
        config_path
    }

    /// `blast_radius_union` over no names is `None`, not `Some(∅)`: an empty
    /// union would report as a measured zero for a row that named nothing.
    /// The queue reaches this through a pre-v28 plan-level row, and the test
    /// above pins the queue's answer; this one pins the function's, so the
    /// guard cannot quietly move to one caller.
    /// **The producer binds the contract.** This drives the real apply-time
    /// gate, not a hand-built row: with a policy that escalates every agent
    /// apply, a touched set holding a compiled model and a replication-target
    /// name records two rows under ONE plan. The compiled model's row carries
    /// its own key; the target's carries none, because the gate could not
    /// name a model for it. The queue then reports exactly that, and lists
    /// both rows — one per (plan, model), never one per plan.
    ///
    /// Restoring `models: Vec::new()` in the gate's record makes the first
    /// assertion fail; making the queue accept any identifier makes the
    /// target's assertion fail.
    #[test]
    fn the_apply_gate_records_a_compiled_models_key_and_no_key_for_a_target() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        let state_path = root.join("state.redb");
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
             [policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n",
        )
        .unwrap();

        touch_plan_file(root, "plan_e2e");
        let touched: BTreeMap<String, PolicyCapability> = [
            ("a".to_string(), PolicyCapability::Apply),
            ("raw_orders".to_string(), PolicyCapability::Apply),
        ]
        .into_iter()
        .collect();
        let gate = crate::commands::apply::evaluate_apply_policy(
            &config_path,
            "plan_e2e",
            PolicyPrincipal::Agent,
            &touched,
            &models_dir,
            &state_path,
            &[],
        );
        assert!(
            matches!(
                gate,
                crate::commands::apply::PolicyGate::RequireReview { .. }
            ),
            "the default posture escalates: {gate:?}"
        );

        // What the gate wrote.
        let rows = StateStore::open(&state_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        let row = |model: &str| {
            rows.iter()
                .find(|r| r.plan_id == "plan_e2e" && r.model == model)
                .unwrap_or_else(|| panic!("a row for {model}"))
        };
        assert_eq!(
            row("a").models,
            vec!["a".to_string()],
            "a compiled model records its key"
        );
        assert!(
            row("raw_orders").models.is_empty(),
            "a target is not a model"
        );

        // What the queue says about it.
        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("the queue must rank");
        let entries: Vec<_> = out
            .pending
            .iter()
            .filter(|e| e.plan_id == "plan_e2e")
            .collect();
        assert_eq!(entries.len(), 2, "one row per (plan, model), both listed");
        let entry = |model: &str| entries.iter().find(|e| e.model == model).unwrap();
        assert_eq!(entry("a").models, vec!["a".to_string()]);
        assert_eq!(entry("a").blast_radius, Some(3));
        assert!(entry("raw_orders").models.is_empty());
        assert_eq!(entry("raw_orders").blast_radius, None);
    }

    /// **The queue must not undo the gate's word.** A replication target
    /// called `orders`, gated beside a compiled model also called `orders`:
    /// the gate records `keys_recorded` with an empty set — "this is no
    /// model" — and the queue used to resolve the bare name against the graph
    /// anyway, hand over the compiled model as the key, and offer its rows
    /// (#1815, review round seven). Dropping the `keys_recorded` check makes
    /// this fail.
    #[test]
    fn the_queue_keeps_a_gates_no_model_word_even_when_a_model_shares_the_name() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        // A compiled model that happens to share the target's name.
        std::fs::write(models_dir.join("orders.sql"), "SELECT id FROM a").unwrap();
        std::fs::write(
            models_dir.join("orders.toml"),
            "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"orders\"\n",
        )
        .unwrap();
        let state_path = root.join("state.redb");
        let config_path = root.join("rocky.toml");
        std::fs::write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.p.target.governance]\nauto_create_schemas = true\n\n\
             [policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n",
        )
        .unwrap();
        touch_plan_file(root, "repl");
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        {
            // The real replication gate path, through the held-store evaluator.
            let ledger = StateStore::open(&state_path).unwrap();
            let touched: BTreeMap<String, PolicyCapability> =
                [("orders".to_string(), PolicyCapability::Apply)]
                    .into_iter()
                    .collect();
            let _ = crate::commands::apply::evaluate_apply_policy_with_store(
                cfg.policy.as_ref(),
                "repl",
                PolicyPrincipal::Agent,
                &touched,
                &models_dir,
                None,
                &ledger,
                &[],
                crate::commands::apply::GateSubjects::ReplicationTargets,
            );
            let rows = ledger.list_policy_decisions().unwrap();
            let row = rows
                .iter()
                .find(|r| r.plan_id == "repl")
                .expect("the target's row");
            assert!(
                row.keys_recorded && row.models.is_empty(),
                "the gate's word: no model"
            );
        }

        let out = compute_review_queue(root, &config_path, &state_path, &models_dir)
            .expect("the queue must rank");
        let entry = out
            .pending
            .iter()
            .find(|e| e.plan_id == "repl")
            .expect("listed");
        assert!(
            entry.models.is_empty(),
            "the queue keeps the gate's word: {:?}",
            entry.models
        );
        assert_eq!(
            entry.preview_model, None,
            "and offers no read of the same-named model"
        );
        assert_eq!(entry.blast_radius, None);
    }

    /// When the compile fails, a bare `model` cannot be checked against any
    /// graph, so it is unknown — but a set the producer recorded stands,
    /// because the producer knew. The ranking is unchanged either way (every
    /// radius is already unknown under a failed compile); what this pins is
    /// that the entry does not invent a key it cannot vouch for, and does not
    /// drop one it was given.
    #[test]
    fn a_failed_compile_keeps_recorded_keys_and_vouches_for_no_bare_name() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let models_dir = root.join("models");
        write_blast_graph(&models_dir);
        // A model that does not parse breaks the compile.
        std::fs::write(models_dir.join("broken.sql"), "SELECT FROM WHERE").unwrap();
        std::fs::write(
            models_dir.join("broken.toml"),
            "name = \"broken\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"broken\"\n",
        )
        .unwrap();
        let state_path = root.join("state.redb");

        touch_plan_file(root, "bare");
        {
            let store = StateStore::open(&state_path).unwrap();
            store
                .record_policy_decision(&qd(
                    1,
                    "bare",
                    "a",
                    PolicyEffect::RequireReview,
                    PolicyCapability::Apply,
                ))
                .unwrap();
        }
        touch_plan_file(root, "recorded");
        record_plan_review_escalation(
            &state_path,
            "recorded",
            PolicyPrincipal::Agent,
            PolicyCapability::Backfill,
            "backfill: 2 model(s)",
            vec!["a".to_string(), "b".to_string()],
            "backfill plan awaits review",
        );

        let out = compute_review_queue(root, &root.join("rocky.toml"), &state_path, &models_dir)
            .expect("a failed compile degrades the ranking, it does not refuse the queue");
        let by_plan = |plan: &str| out.pending.iter().find(|e| e.plan_id == plan).unwrap();
        assert!(
            by_plan("bare").models.is_empty(),
            "no graph to check the bare name against"
        );
        assert_eq!(
            by_plan("recorded").models,
            vec!["a".to_string(), "b".to_string()]
        );
        for e in &out.pending {
            assert_eq!(
                e.blast_radius, None,
                "{}: nothing resolves without a compile",
                e.plan_id
            );
        }
    }

    #[test]
    fn a_union_over_no_subjects_is_unknown_not_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        write_blast_graph(&models_dir);
        let compiled = compile_project_with_schemas(Default::default(), &models_dir)
            .expect("the blast graph compiles");
        assert_eq!(blast_radius_union(&compiled, std::iter::empty()), None);
        assert_eq!(
            blast_radius_union(&compiled, ["c"]).map(|s| s.len()),
            Some(0),
            "a live leaf alone really is a measured zero — the discriminator"
        );
    }

    /// A present-but-unloadable `rocky.toml` refuses the queue, and the error
    /// NAMES the file. Restoring `compile_project(..).ok()` makes this fail:
    /// the queue returns `Ok` with one entry whose `blast_radius` is `None`.
    #[test]
    fn review_queue_refuses_a_present_but_unloadable_config() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let state_path = seed_pending_escalation(root);
        let cfg = root.join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1702).unwrap();

        let err = compute_review_queue(root, &cfg, &state_path, &root.join("models"))
            .expect_err("a present but unloadable rocky.toml must refuse `rocky review --queue`");
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

    /// The refusal does not depend on the ledger holding a pending row today.
    /// The config is read BEFORE the outstanding-selection short-circuit, so
    /// `rocky review --queue` on a broken config refuses whether or not there
    /// is anything to rank. Moving the read below that short-circuit makes
    /// this fail while the test above still passes.
    #[test]
    fn review_queue_refuses_an_unloadable_config_with_an_empty_ledger() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let cfg = root.join("rocky.toml");
        std::fs::write(&cfg, BROKEN_CONFIG_1702).unwrap();

        let err = compute_review_queue(root, &cfg, &root.join("state.redb"), &root.join("models"))
            .expect_err("an empty queue must not excuse an unloadable config");
        assert!(
            format!("{err:#}").contains(&cfg.display().to_string()),
            "{err:#}"
        );
    }

    /// Honest failure (a): absent is not invalid. With NO `rocky.toml` the
    /// queue ranks exactly as it always did — a cold schema cache, so the
    /// blast radius is unknown and the entry still lists.
    ///
    /// The second half is the discriminator: it pins that a present-but-broken
    /// config in the SAME shape does refuse, so this guard cannot pass just
    /// because the config leg was removed altogether.
    #[test]
    fn review_queue_still_ranks_without_any_config() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let state_path = seed_pending_escalation(root);
        let cfg = root.join("rocky.toml");
        assert!(!cfg.exists());

        let out = compute_review_queue(root, &cfg, &state_path, &root.join("models"))
            .expect("a missing rocky.toml must not refuse the queue");
        assert_eq!(out.total, 1);
        assert_eq!(out.pending[0].model, "fct_orders");
        assert_eq!(
            out.pending[0].blast_radius, None,
            "no models dir and a cold cache — the blast radius is unknown, as before"
        );

        std::fs::write(&cfg, BROKEN_CONFIG_1702).unwrap();
        assert!(
            compute_review_queue(root, &cfg, &state_path, &root.join("models")).is_err(),
            "a present-but-broken config in the same project must refuse"
        );
    }

    /// Honest failure (b): a config that LOADS but holds an unset `${VAR}` in
    /// an adapter connection field must not refuse. `rocky review --queue`
    /// opens no warehouse connection, so it uses the credential-tolerant
    /// loader and reads the schema cache the strict loader would have skipped.
    #[test]
    fn review_queue_tolerates_an_unset_credential_var() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let state_path = seed_pending_escalation(root);
        let cfg = root.join("rocky.toml");
        std::fs::write(&cfg, UNSET_CREDENTIAL_CONFIG_1702).unwrap();

        // The loader choice is what makes the difference — pin both directions.
        assert!(
            rocky_core::config::load_rocky_config(&cfg).is_err(),
            "fixture must fail the STRICT loader, or this proves nothing"
        );
        assert!(
            rocky_core::config::load_optional_project_config(Some(&cfg))
                .expect("the tolerant loader must accept an unset adapter credential")
                .is_some()
        );

        let out = compute_review_queue(root, &cfg, &state_path, &root.join("models"))
            .expect("an unset ${VAR} in adapter credentials must not refuse the queue");
        assert_eq!(out.total, 1);
    }
}
