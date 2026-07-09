//! `rocky policy` — explain a policy decision (`check`) and assert policy
//! behaviour (`test`).
//!
//! `check` is explain-mode only (v0): loads the project's `[policy]` block,
//! compiles the project to read the target model's attributes, evaluates the
//! `(principal, capability, model)` triple against the policy, and prints
//! the resolved effect + winning rule + reason. It does **not** gate any
//! real command.
//!
//! `test` is the CI safety net: it runs the project's `[[policy.tests]]`
//! scenarios through the *same* evaluator and fails (non-zero exit) if any
//! resolved effect differs from what the scenario expected — so a policy edit
//! cannot silently open a hole.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{Context, Result, bail};
use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::config::{ConfigError, PolicyCapability, PolicyConfig, PolicyPrincipal};
use rocky_core::policy::{self, ModelAttributes};

use crate::output::{
    PolicyCheckOutput, PolicyModelAttributes, PolicyTestOutput, PolicyTestResult, print_json,
};

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
    // Transitive blast radius for the `max_downstreams` ceiling. `None` when
    // the model is absent from the compiled graph (the ceiling fails closed).
    let reachable_downstreams = crate::commands::audit::blast_radius_of(&result, model_name)
        .map(|(_direct, transitive)| transitive.len() as u64);

    let attrs = ModelAttributes {
        name: model.config.name.clone(),
        tags: model.config.tags.clone(),
        classifications,
        layer,
        contracted,
        downstreams,
        reachable_downstreams,
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
            reachable_downstreams: attrs.reachable_downstreams,
        },
    };

    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }

    Ok(())
}

/// Execute `rocky policy test`.
///
/// Loads the project's `[policy]` block and its `[[policy.tests]]` scenarios,
/// runs every scenario through [`policy::evaluate`], and reports the pass/fail
/// verdict per scenario (actual vs expected effect, plus the deciding rule and
/// reason on a failure). Returns an error — a non-zero exit for CI — when any
/// scenario's resolved effect differs from its expectation.
///
/// A missing `rocky.toml`, an absent `[policy]` block, or zero scenarios are
/// each treated as a hard error rather than a silent pass: a policy-test run
/// that asserts nothing would defeat the guardrail it exists to be.
pub fn run_policy_test(config_path: &Path, json: bool) -> Result<()> {
    let config = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => cfg,
        Err(ConfigError::FileNotFound { .. }) => bail!(
            "no rocky.toml found at {} — `rocky policy test` needs a [policy] block with \
             [[policy.tests]] scenarios",
            config_path.display()
        ),
        Err(e) => return Err(e).context("loading rocky.toml for [policy]"),
    };

    let Some(policy) = config.policy else {
        bail!(
            "no [policy] block in {} — nothing to test",
            config_path.display()
        );
    };

    if policy.tests.is_empty() {
        bail!(
            "no [[policy.tests]] scenarios in {} — `rocky policy test` has nothing to assert",
            config_path.display()
        );
    }

    let mut results = Vec::with_capacity(policy.tests.len());
    for test in &policy.tests {
        // Build the evaluator's input verbatim from the scenario — the same
        // `ModelAttributes` shape a real enforcement seam constructs. No
        // derivation, no compile: the scenario *is* the input.
        let attrs = ModelAttributes {
            name: test.model.clone(),
            tags: test.tags.clone(),
            classifications: test.classifications.iter().cloned().collect(),
            layer: test.layer.clone(),
            contracted: test.contracted,
            downstreams: test.downstreams,
            reachable_downstreams: test.reachable_downstreams,
        };
        let decision = policy::evaluate(&policy, test.principal, test.capability, &attrs);
        results.push(PolicyTestResult {
            name: test.name.clone(),
            passed: decision.effect == test.expect,
            principal: test.principal,
            capability: test.capability,
            model: test.model.clone(),
            expected: test.expect,
            actual: decision.effect,
            matched_rule: decision.matched_rule,
            reason: decision.reason,
        });
    }

    let total = results.len();
    let passed = results.iter().filter(|r| r.passed).count();
    let failed = total - passed;

    let output = PolicyTestOutput {
        version: VERSION.to_string(),
        command: "policy_test".to_string(),
        total,
        passed,
        failed,
        results,
    };

    if json {
        print_json(&output)?;
    } else {
        render_test_text(&output);
    }

    if failed > 0 {
        bail!("{failed} of {total} policy scenario(s) failed");
    }

    Ok(())
}

/// Render the scenario results as a compact pass/fail report.
fn render_test_text(out: &PolicyTestOutput) {
    println!("policy test: {} scenario(s)", out.total);
    for result in &out.results {
        let verdict = if result.passed { "PASS" } else { "FAIL" };
        println!("  [{verdict}] {}", result.name);
        if !result.passed {
            let principal = serde_plain(&result.principal);
            let capability = serde_plain(&result.capability);
            let expected = serde_plain(&result.expected);
            let actual = serde_plain(&result.actual);
            let model = if result.model.is_empty() {
                "(unnamed)"
            } else {
                result.model.as_str()
            };
            println!("         {principal} / {capability} / {model}");
            println!("         expected {expected}, got {actual}");
            match result.matched_rule {
                Some(idx) => println!("         matched: rule {idx}"),
                None => println!("         matched: (none)"),
            }
            println!("         reason: {}", result.reason);
        }
    }
    println!("  {} passed, {} failed", out.passed, out.failed);
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
    let reachable = attrs
        .reachable_downstreams
        .map(|n| n.to_string())
        .unwrap_or_else(|| "(uncomputable)".to_string());
    println!(
        "  model: contracted={} layer={} classifications=[{}] downstreams={} blast_radius={}",
        attrs.contracted,
        attrs.layer.as_deref().unwrap_or("(none)"),
        classifications,
        attrs.downstreams,
        reachable,
    );
}

/// Serialize a small serde enum to its wire spelling for text output.
fn serde_plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    /// Write `body` to a `rocky.toml` in a fresh temp dir and return its path
    /// (kept alive by the returned `TempDir`).
    fn config_with(body: &str) -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("rocky.toml");
        fs::write(&path, body).unwrap();
        (dir, path)
    }

    const POLICY: &str = r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { contracted = true }
effect = "deny"

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { tags = { layer = "bronze" }, max_downstreams = 5 }
effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "schema_change.additive"
scope = { tags = { layer = "bronze" } }
effect = "allow"
"#;

    #[test]
    fn all_scenarios_pass_returns_ok() {
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"contracted apply is denied\"
principal = \"agent\"
capability = \"apply\"
contracted = true
expect = \"deny\"

[[policy.tests]]
name = \"human is ungated\"
principal = \"human\"
capability = \"apply\"
contracted = true
expect = \"allow\"

[[policy.tests]]
name = \"unmatched agent falls to default posture\"
principal = \"agent\"
capability = \"promote\"
model = \"stg_orders\"
expect = \"require_review\"
"
        );
        let (_dir, path) = config_with(&body);
        run_policy_test(&path, true).expect("all scenarios should pass");
    }

    #[test]
    fn a_failing_scenario_errors() {
        // The contracted-apply rule denies, but the scenario wrongly expects
        // allow — the runner must fail (non-zero exit for CI).
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"wrongly expects allow on a denied action\"
principal = \"agent\"
capability = \"apply\"
contracted = true
expect = \"allow\"
"
        );
        let (_dir, path) = config_with(&body);
        let err = run_policy_test(&path, true).unwrap_err();
        assert!(err.to_string().contains("1 of 1 policy scenario"));
    }

    #[test]
    fn sticky_ceiling_breach_is_catchable() {
        // A broad ungated `allow` (rule 2) dominates the ceilinged sibling
        // (rule 1) on specificity, but the sticky safety cap must still degrade
        // the final effect when the blast radius exceeds the ceiling. A
        // scenario asserting `allow` here MUST fail — this is the exact
        // false-allow class the cap exists to prevent.
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"oversized blast radius is capped to require_review\"
principal = \"agent\"
capability = \"schema_change.additive\"
tags = {{ layer = \"bronze\" }}
reachable_downstreams = 99
expect = \"require_review\"

[[policy.tests]]
name = \"uncomputable blast radius fails closed\"
principal = \"agent\"
capability = \"schema_change.additive\"
tags = {{ layer = \"bronze\" }}
expect = \"require_review\"

[[policy.tests]]
name = \"within-ceiling stays allow\"
principal = \"agent\"
capability = \"schema_change.additive\"
tags = {{ layer = \"bronze\" }}
reachable_downstreams = 3
expect = \"allow\"
"
        );
        let (_dir, path) = config_with(&body);
        run_policy_test(&path, true).expect("ceiling scenarios should all pass");
    }

    #[test]
    fn dominant_allow_cannot_mask_a_ceilinged_sibling() {
        // The false-allow class the sticky safety cap exists to prevent: a
        // broad ungated `allow` (constraints {Tags, Models}) strictly dominates
        // a ceilinged sibling ({Tags}) on specificity, so the ceilinged rule is
        // filtered out of the non-dominated tier. The sticky cap — not
        // most-restrictive selection — is what still degrades the dominant
        // `allow` when the sibling's ceiling is breached. A scenario pins that:
        // were the cap broken, the resolved effect would be `allow` and this
        // assertion (expecting require_review) would fail.
        let body = "
[policy]
version = 1

[[policy.rules]]
principal = \"agent\"
capability = \"schema_change.additive\"
scope = { tags = { layer = \"bronze\" }, models = [\"stg_*\"] }
effect = \"allow\"

[[policy.rules]]
principal = \"agent\"
capability = \"schema_change.additive\"
scope = { tags = { layer = \"bronze\" }, max_downstreams = 5 }
effect = \"allow\"

[[policy.tests]]
name = \"dominant broad allow cannot mask a ceilinged sibling\"
principal = \"agent\"
capability = \"schema_change.additive\"
model = \"stg_orders\"
tags = { layer = \"bronze\" }
reachable_downstreams = 99
expect = \"require_review\"
";
        let (_dir, path) = config_with(body);
        run_policy_test(&path, true).expect("sticky cap must keep the scenario green");
    }

    #[test]
    fn missing_config_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.toml");
        let err = run_policy_test(&path, true).unwrap_err();
        assert!(err.to_string().contains("no rocky.toml"));
    }

    #[test]
    fn no_scenarios_errors() {
        let (_dir, path) = config_with(POLICY);
        let err = run_policy_test(&path, true).unwrap_err();
        assert!(err.to_string().contains("nothing to assert"));
    }

    #[test]
    fn no_policy_block_errors() {
        let (_dir, path) = config_with("");
        let err = run_policy_test(&path, true).unwrap_err();
        assert!(err.to_string().contains("no [policy] block"));
    }

    #[test]
    fn typo_in_scenario_key_is_rejected() {
        // `deny_unknown_fields` on PolicyTest turns a mistyped assertion key
        // into a parse error rather than a silently-ignored false green.
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"typo\"
principal = \"agent\"
capability = \"apply\"
contracted = true
expcet = \"deny\"
"
        );
        let (_dir, path) = config_with(&body);
        assert!(run_policy_test(&path, true).is_err());
    }
}
