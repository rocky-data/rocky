//! `rocky audit` — query the agent-policy decision ledger.
//!
//! Reads the [`POLICY_DECISIONS`](rocky_core::state) table and renders every
//! recorded policy decision (oldest first) as text (default) or JSON. Each row
//! is one evaluation the F3 policy plane made at a mutating enforcement seam
//! (`rocky apply` / promote) — reads are never recorded, so this is the trail
//! of *governed mutations*, not inspection.

use std::path::Path;

use anyhow::{Context, Result};
use rocky_core::state::StateStore;

use crate::output::{AuditDecisionEntry, AuditOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

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

    let entries: Vec<AuditDecisionEntry> = decisions
        .into_iter()
        .map(|d| AuditDecisionEntry {
            timestamp: d.timestamp.to_rfc3339(),
            plan_id: d.plan_id,
            principal: d.principal,
            capability: d.capability,
            model: d.model,
            effect: d.effect,
            rule_id: d.rule_id,
            reason: d.reason,
        })
        .collect();

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
