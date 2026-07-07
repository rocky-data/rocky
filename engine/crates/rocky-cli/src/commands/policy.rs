//! `rocky policy check` — explain a policy decision.
//!
//! Explain-mode only (v0): loads the project's `[policy]` block, compiles
//! the project to read the target model's attributes, evaluates the
//! `(principal, capability, model)` triple against the policy, and prints
//! the resolved effect + winning rule + reason. It does **not** gate any
//! real command.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result};
use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::config::{ConfigError, PolicyCapability, PolicyConfig, PolicyPrincipal};
use rocky_core::policy::{self, ModelAttributes};

use crate::output::{PolicyCheckOutput, PolicyModelAttributes, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky policy check`.
///
/// Resolves the effect the policy plane would yield for
/// `(principal, capability, model)` and renders it as text (default) or
/// JSON (`--output json`).
pub fn run_policy_check(
    config_path: &Path,
    models_dir: &Path,
    principal: PolicyPrincipal,
    capability: PolicyCapability,
    model_name: &str,
    json: bool,
) -> Result<()> {
    // Load the `[policy]` block. A missing rocky.toml falls back to the
    // default posture (agents on mutating actions require review, humans
    // are never gated); a *malformed* config (including an invalid
    // `[policy]`) surfaces the error rather than silently defaulting.
    let policy = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => cfg.policy.unwrap_or_else(PolicyConfig::default_posture),
        Err(ConfigError::FileNotFound { .. }) => PolicyConfig::default_posture(),
        Err(e) => return Err(e).context("loading rocky.toml for [policy]"),
    };

    // Compile the project to read the target model's attributes.
    let compiler = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let result = compile::compile(&compiler)
        .with_context(|| format!("compiling models in {}", models_dir.display()))?;

    let model = result
        .project
        .models
        .iter()
        .find(|m| m.config.name == model_name)
        .with_context(|| format!("model '{model_name}' not found in {}", models_dir.display()))?;

    // Build the matcher's view of the model. `classifications` collapses the
    // per-column classification map to the distinct set of values; `layer`
    // is read from the model's `layer` tag (v0 convention); `contracted` is
    // the presence of a sibling `.contract.toml` (best-effort v0).
    let classifications: BTreeSet<String> = model.config.classification.values().cloned().collect();
    let layer = model.config.tags.get("layer").cloned();
    let contracted = model.contract_path.is_some();
    let downstreams = result
        .project
        .models
        .iter()
        .filter(|m| m.config.depends_on.iter().any(|d| d == model_name))
        .count() as u64;

    let attrs = ModelAttributes {
        name: model.config.name.clone(),
        tags: model.config.tags.clone(),
        classifications,
        layer,
        contracted,
        downstreams,
    };

    let decision = policy::evaluate(&policy, principal, capability, &attrs);

    let output = PolicyCheckOutput {
        version: VERSION.to_string(),
        command: "policy_check".to_string(),
        principal,
        capability,
        model: model_name.to_string(),
        effect: decision.effect,
        matched_rule: decision.matched_rule,
        reason: decision.reason,
        model_attributes: PolicyModelAttributes {
            tags: attrs.tags,
            classifications: attrs.classifications.into_iter().collect(),
            layer: attrs.layer,
            contracted: attrs.contracted,
            downstreams: attrs.downstreams,
        },
    };

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    Ok(())
}

/// Render the decision as a compact human-readable block.
fn render_text(out: &PolicyCheckOutput) {
    let principal = serde_plain(&out.principal);
    let capability = serde_plain(&out.capability);
    let effect = serde_plain(&out.effect);
    println!("policy check: {principal} / {capability} / {}", out.model);
    println!("  effect: {effect}");
    match out.matched_rule {
        Some(idx) => println!("  matched: rule {idx}"),
        None => println!("  matched: (none)"),
    }
    println!("  reason: {}", out.reason);
    let attrs = &out.model_attributes;
    let classifications = if attrs.classifications.is_empty() {
        "(none)".to_string()
    } else {
        attrs.classifications.join(", ")
    };
    println!(
        "  model: contracted={} layer={} classifications=[{}] downstreams={}",
        attrs.contracted,
        attrs.layer.as_deref().unwrap_or("(none)"),
        classifications,
        attrs.downstreams,
    );
}

/// Serialize a small serde enum to its wire spelling for text output.
fn serde_plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}
