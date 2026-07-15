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
use rocky_core::config::{
    ConfigError, PolicyCapability, PolicyConfig, PolicyEffect, PolicyPrincipal, StateBackend,
    StateConfig, StateUploadFailureMode,
};
use rocky_core::policy::{self, ModelAttributes};
use rocky_core::state::{PolicyDecisionRecord, StateStore};

use crate::output::{
    PolicyCheckOutput, PolicyFreezeEntry, PolicyFreezeOutput, PolicyModelAttributes,
    PolicyTestOutput, PolicyTestResult, print_json,
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
        // Reconcile `layer` ↔ `tags["layer"]` symmetrically. At a live
        // enforcement seam a model's `layer` attribute IS its `layer` tag, so
        // the evaluator sees the two in lockstep. A scenario that sets only one
        // must therefore back-fill the other, or a rule scoped on the *other*
        // spelling would mispredict:
        //   - `layer = "gold"` alone must match a `scope.tags = { layer = "gold" }`
        //     rule (back-fill the tag).
        //   - `tags = { layer = "gold" }` alone must match a `scope.layer = "gold"`
        //     rule (back-fill the attribute — this was already handled).
        // A scenario that sets BOTH to different values is contradictory (no
        // live model can present that) and is rejected at load with a clear
        // error rather than silently picking one.
        let mut tags = test.tags.clone();
        let layer = match (test.layer.as_deref(), tags.get("layer").cloned()) {
            (Some(explicit), Some(tag)) if explicit != tag => {
                bail!(
                    "policy test '{}' is inconsistent: layer = \"{explicit}\" but \
                     tags.layer = \"{tag}\". At a live seam a model's layer IS its `layer` tag, \
                     so these cannot differ — set one, or make them equal.",
                    test.name
                );
            }
            (Some(explicit), _) => {
                // Back-fill the tag so a `scope.tags = { layer = ... }` rule matches.
                tags.entry("layer".to_string())
                    .or_insert_with(|| explicit.to_string());
                Some(explicit.to_string())
            }
            (None, Some(tag)) => Some(tag),
            (None, None) => None,
        };
        // Build the evaluator's input from the reconciled scenario — the same
        // `ModelAttributes` a real enforcement seam constructs, with no compile
        // step.
        let attrs = ModelAttributes {
            name: test.model.clone(),
            tags,
            classifications: test.classifications.iter().cloned().collect(),
            layer,
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
    // Same static-vs-dynamic divergence note `rocky policy check` prints:
    // scenarios evaluate the STATIC `[policy]` config, but a live enforcement
    // seam additionally projects the ledger (active freezes, autonomy-budget
    // burn), which can only tighten a scenario's resolved effect.
    println!(
        "  note: scenarios evaluate the static [policy] config; live seams (apply/promote) also \
         project active freezes and autonomy-budget burn, which can only tighten these effects"
    );
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
    // This is the static base effect. The dynamic breakers — autonomy-budget
    // burn and active policy freezes — are ledger-derived and applied at the
    // mutating enforcement seam (apply / promote); they can only tighten this
    // effect. See `rocky brief` for the current budget/freeze state.
    println!(
        "  note: base effect only; autonomy-budget burn and active freezes apply at enforcement \
         (apply/promote) and can only tighten it"
    );
}

/// Serialize a small serde enum to its wire spelling for text output.
fn serde_plain<T: serde::Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_default()
}

/// Run a state-sync future to completion from a **synchronous** command entry
/// point.
///
/// `rocky policy freeze` is dispatched synchronously from within the CLI's async
/// runtime, but `state_sync::{download,upload}_state` are async. We cannot make
/// this function async without changing its (out-of-crate) call signature, and
/// we cannot `block_on` the ambient runtime from one of its own worker threads
/// (tokio's re-entrancy guard panics). So we drive the future on a dedicated OS
/// thread with its own single-threaded runtime — correct whether or not there is
/// an ambient runtime, and regardless of its flavor.
///
/// Exposed to the sibling `apply` module so the SYNC promote gate
/// ([`crate::commands::apply::gate_promote_plan`]) — reached from two async
/// entry points — can pull remote state before it reads the freeze ledger,
/// without threading an async download through each caller.
pub(crate) fn block_on_state_sync<F>(fut: F) -> Result<(), rocky_core::state_sync::StateSyncError>
where
    F: std::future::Future<Output = Result<(), rocky_core::state_sync::StateSyncError>> + Send,
{
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(e) => return Err(rocky_core::state_sync::StateSyncError::Io(e)),
                };
                rt.block_on(fut)
            })
            .join()
            .unwrap_or_else(|_| {
                Err(rocky_core::state_sync::StateSyncError::Io(
                    std::io::Error::other("state-sync worker thread panicked"),
                ))
            })
    })
}

/// Execute `rocky policy freeze` — the kill switch.
///
/// Flips every matched `(principal, scope)` to `deny` by recording a **freeze
/// decision** into the existing policy-decision ledger. No config file is
/// rewritten and no new table is created: the enforcement seam reads the ledger
/// and an active freeze forces `deny`. Freezing is always allowed.
///
/// `principal = None` freezes both principals (`agent` + `human`); `scope =
/// None` freezes every model (`any`). The inverse (`lift = true`) records an
/// unfreeze that supersedes a matching freeze — the normal way to lift one.
///
/// # Remote `[state]` integrity (S1, #1089)
///
/// The policy-decision ledger is one of the tables `rocky run` wholesale
/// **downloads at start and uploads at end** when `[state]` is a remote backend.
/// Left unsynced, a freeze recorded here would be silently reverted by the next
/// run's start-download. So when the backend is remote we wrap the ledger write
/// with a seam-scoped sync: **download-before-open** (pull the authoritative
/// remote ledger, overwriting the local file, so we record on top of it) and
/// **upload-after-commit** (push the freeze back). A remote-backend freeze
/// therefore requires the backend to be reachable: a download failure aborts the
/// command rather than recording a freeze that would be clobbered — for a kill
/// switch, failing loudly beats appearing to engage and then silently
/// disengaging.
///
/// KNOWN LIMITATION (concurrency): this closes only the *sequential*
/// between-runs clobber. The remote state object is a whole-file blob with **no
/// compare-and-swap**, so a concurrent writer — e.g. a `rocky run` whose
/// start-download completed *before* this freeze — can still overwrite the
/// freeze on its own end-upload (a cross-pod / concurrent-writer lost update).
/// This does NOT make concurrent writes safe; it narrows the window, not closes
/// it.
pub fn run_policy_freeze(
    config_path: &Path,
    state_path: &Path,
    principal: Option<PolicyPrincipal>,
    scope: Option<String>,
    lift: bool,
    json: bool,
) -> Result<()> {
    let scope = scope.unwrap_or_else(|| "any".to_string());
    policy::validate_scope_selector(&scope).map_err(|e| anyhow::anyhow!(e))?;

    let principals = match principal {
        Some(p) => vec![p],
        None => vec![PolicyPrincipal::Agent, PolicyPrincipal::Human],
    };

    // A freeze recorded with no `[policy]` block is INERT: every enforcement
    // seam short-circuits to `NotConfigured` before it reads the ledger, so the
    // freeze binds nothing. Recording it is still useful (it takes effect the
    // moment a `[policy]` block is added), so this is a loud warning — stderr +
    // an output note — not an error; the exit code stays 0.
    let notes = freeze_enforcement_notes(config_path, lift);
    for note in &notes {
        eprintln!("warning: {note}");
    }

    // Resolve the `[state]` backend. A missing/unreadable config degrades to the
    // Local default (no remote sync) — freeze still records locally, matching
    // the tolerant posture of `freeze_enforcement_notes` above.
    let state_cfg = rocky_core::config::load_rocky_config(config_path)
        .map(|cfg| cfg.state)
        .unwrap_or_default();
    let remote_state = !matches!(state_cfg.backend, StateBackend::Local);

    // SEAM-SCOPED SYNC — download half. Pull the authoritative remote ledger
    // (overwriting the local file) BEFORE opening the store, so the freeze is
    // recorded on top of other pods' decisions rather than over an empty local.
    if remote_state {
        block_on_state_sync(rocky_core::state_sync::download_state(
            &state_cfg, state_path,
        ))
        .with_context(|| {
            "failed to download remote state before recording the policy freeze; \
                 a remote-backend freeze requires the state backend to be reachable"
        })?;
    }

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let now = chrono::Utc::now();
    let prefix = if lift {
        policy::UNFREEZE_PLAN_PREFIX
    } else {
        policy::FREEZE_PLAN_PREFIX
    };

    let mut entries = Vec::new();
    for p in principals {
        let principal_label = serde_plain(&p);
        // The plan_id carries the principal so a "freeze all" (two records at
        // the same timestamp + scope) does not collide on the ledger key
        // `(timestamp, plan_id, model)`.
        let plan_id = format!("{prefix}{principal_label}:{}", now.to_rfc3339());
        let effect = if lift {
            PolicyEffect::Allow
        } else {
            PolicyEffect::Deny
        };
        let reason = if lift {
            format!("policy unfreeze: lifted freeze for {principal_label} on scope '{scope}'")
        } else {
            format!("policy freeze: {principal_label} actions on scope '{scope}' frozen to deny")
        };
        let record = PolicyDecisionRecord {
            timestamp: now,
            plan_id: plan_id.clone(),
            principal: p,
            capability: PolicyCapability::Apply,
            model: scope.clone(),
            effect,
            rule_id: None,
            reason: reason.clone(),
            verify_after: Vec::new(),
            // A freeze/unfreeze is a policy-change decision, not a drift
            // auto-apply, so it carries no auto-apply custody.
            auto_apply: None,
        };
        store
            .record_policy_decision(&record)
            .context("failed to record the freeze decision to the ledger")?;
        entries.push(PolicyFreezeEntry {
            principal: p,
            effect,
            decision_ref: format!("{}|{plan_id}|{scope}", now.to_rfc3339()),
            plan_id,
            reason,
        });
    }

    // SEAM-SCOPED SYNC — upload half, FAIL-CLOSED. Push the freeze back to the
    // remote backend so the next `rocky run`'s start-download inherits it instead
    // of reverting it. Drop the store first to release the advisory lock and
    // flush the file. Durability is the whole point of a kill switch, so the
    // upload is forced to `Fail` regardless of the configured `on_upload_failure`
    // (default `skip`): a freeze that commits locally but never reaches the
    // remote — while the command reports success — would leave every other pod
    // unfrozen. A failed upload aborts (finding 5).
    drop(store);
    if remote_state {
        let upload_cfg = StateConfig {
            on_upload_failure: StateUploadFailureMode::Fail,
            ..state_cfg.clone()
        };
        block_on_state_sync(rocky_core::state_sync::upload_state(
            &upload_cfg,
            state_path,
        ))
        .with_context(|| "failed to upload remote state after recording the policy freeze")?;
    }

    let output = PolicyFreezeOutput {
        version: VERSION.to_string(),
        command: if lift {
            "policy_unfreeze".to_string()
        } else {
            "policy_freeze".to_string()
        },
        lifted: lift,
        scope,
        recorded_at: now.to_rfc3339(),
        entries,
        notes,
    };

    if json {
        print_json(&output)?;
    } else {
        render_freeze_text(&output);
    }
    Ok(())
}

/// Build the enforcement-status notes for a `freeze` / `unfreeze` against
/// `config_path`.
///
/// Returns a single "recorded but NOT enforced" warning exactly when the
/// project has no enforceable `[policy]` block (missing block, missing config,
/// or a config that fails to load) — the case where the freeze binds nothing
/// at any seam until a `[policy]` block is added. Empty when the freeze is
/// enforceable.
fn freeze_enforcement_notes(config_path: &Path, lift: bool) -> Vec<String> {
    let policy_configured = matches!(
        rocky_core::config::load_rocky_config(config_path),
        Ok(cfg) if cfg.policy.is_some()
    );
    if policy_configured {
        return Vec::new();
    }
    let verb = if lift { "unfreeze" } else { "freeze" };
    vec![format!(
        "{verb} recorded but NOT enforced: no [policy] block configured in {}. \
         Every enforcement seam short-circuits before reading the ledger until a [policy] \
         block exists; the freeze takes effect the moment one is added.",
        config_path.display()
    )]
}

fn render_freeze_text(out: &PolicyFreezeOutput) {
    let verb = if out.lifted { "unfreeze" } else { "freeze" };
    println!(
        "policy {verb}: scope '{}' ({} rule set(s))",
        out.scope,
        out.entries.len()
    );
    for e in &out.entries {
        println!(
            "  {} -> {} [{}]",
            serde_plain(&e.principal),
            serde_plain(&e.effect),
            e.decision_ref,
        );
    }
    if out.lifted {
        println!("  the matching freeze is lifted; agents resume their authored policy effect");
    } else {
        println!(
            "  frozen — matched actions now DENY at enforcement; lift with `rocky policy unfreeze` \
             (same --principal/--scope) or a policy-change PR"
        );
    }
    for note in &out.notes {
        println!("  ! {note}");
    }
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

    /// A config with an adapter + pipeline but NO `[policy]` block — the shape
    /// the freeze-inert warning fires on.
    const NO_POLICY_BODY: &str = r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target.governance]
auto_create_schemas = true
"#;

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
    fn layer_is_derived_from_the_layer_tag_like_a_real_seam() {
        // At a live seam `attrs.layer` comes from the model's `layer` tag, so a
        // `scope.layer` rule must match a scenario that sets `tags.layer` even
        // without an explicit `layer` field. Were the runner to take `layer`
        // verbatim (leaving it `None`), the rule would miss, the scenario would
        // resolve to the default posture, and the test would mispredict
        // production. Both scenarios below must pass.
        let body = "
[policy]
version = 1
default_agent_effect = \"deny\"

[[policy.rules]]
principal = \"agent\"
capability = \"apply\"
scope = { layer = \"gold\" }
effect = \"allow\"

[[policy.tests]]
name = \"layer derived from the tag matches a scope.layer rule\"
principal = \"agent\"
capability = \"apply\"
model = \"fct_revenue\"
tags = { layer = \"gold\" }
expect = \"allow\"

[[policy.tests]]
name = \"a non-gold layer does not match and falls to default\"
principal = \"agent\"
capability = \"apply\"
model = \"stg_orders\"
tags = { layer = \"bronze\" }
expect = \"deny\"
";
        let (_dir, path) = config_with(body);
        run_policy_test(&path, true).expect("layer-derivation scenarios must pass");
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

    /// 🔴 FIX 4 regression: a freeze recorded against a config with NO
    /// `[policy]` block is inert at every seam, so the command must surface a
    /// loud "recorded but NOT enforced" note. Pre-fix `freeze`/`unfreeze`
    /// succeeded silently with no signal the freeze binds nothing.
    #[test]
    fn freeze_without_policy_block_is_flagged_inert() {
        let (_dir, path) = config_with(NO_POLICY_BODY);
        let notes = freeze_enforcement_notes(&path, false);
        assert_eq!(notes.len(), 1, "a freeze with no [policy] block must warn");
        assert!(
            notes[0].contains("recorded but NOT enforced"),
            "note must say the freeze is not enforced: {}",
            notes[0]
        );
        // Unfreeze carries the same warning with its own verb.
        let unfreeze_notes = freeze_enforcement_notes(&path, true);
        assert!(unfreeze_notes[0].contains("unfreeze recorded but NOT enforced"));
    }

    /// A missing config file is treated as "no [policy] block" — the freeze
    /// still records (elsewhere) but is flagged inert.
    #[test]
    fn freeze_with_missing_config_is_flagged_inert() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("does-not-exist.toml");
        assert_eq!(freeze_enforcement_notes(&path, false).len(), 1);
    }

    /// With a `[policy]` block present, a freeze is enforceable → no note.
    #[test]
    fn freeze_with_policy_block_has_no_note() {
        let (_dir, path) = config_with(POLICY);
        assert!(
            freeze_enforcement_notes(&path, false).is_empty(),
            "an enforceable freeze must carry no inert-warning note"
        );
    }

    /// End-to-end: `run_policy_freeze` records the freeze AND returns Ok even
    /// with no `[policy]` block (recording stays useful; exit 0), and the freeze
    /// row lands in the ledger.
    #[test]
    fn run_policy_freeze_records_and_succeeds_without_policy_block() {
        let (dir, path) = config_with(NO_POLICY_BODY);
        let state = dir.path().join("state.redb");
        run_policy_freeze(
            &path,
            &state,
            Some(PolicyPrincipal::Agent),
            None,
            false,
            true,
        )
        .expect("freeze must record and exit 0 even with no [policy] block");
        let store = StateStore::open(&state).unwrap();
        let freezes = policy::active_freezes(&store.list_policy_decisions().unwrap());
        assert_eq!(
            freezes.len(),
            1,
            "the freeze must be recorded in the ledger"
        );
        assert_eq!(freezes[0].principal, PolicyPrincipal::Agent);
    }

    /// 🔴 FIX 7 regression: a scenario that sets an explicit `layer` (and no
    /// `tags.layer`) must match a rule scoped `tags = { layer = ... }`. At a
    /// live seam `attrs.tags["layer"]` and `attrs.layer` are the same value, so
    /// the scenario must back-fill the tag. Pre-fix the explicit `layer` did
    /// NOT populate `tags["layer"]`, so the tag-scoped rule missed, the
    /// scenario fell to the default posture, and this assertion failed.
    #[test]
    fn explicit_layer_backfills_the_layer_tag_for_tag_scoped_rules() {
        let body = "
[policy]
version = 1
default_agent_effect = \"deny\"

[[policy.rules]]
principal = \"agent\"
capability = \"apply\"
scope = { tags = { layer = \"gold\" } }
effect = \"allow\"

[[policy.tests]]
name = \"explicit layer matches a tags.layer rule\"
principal = \"agent\"
capability = \"apply\"
model = \"fct_revenue\"
layer = \"gold\"
expect = \"allow\"
";
        let (_dir, path) = config_with(body);
        run_policy_test(&path, true)
            .expect("an explicit layer must back-fill tags.layer and match the tag-scoped rule");
    }

    /// 🔴 FIX 7 regression: a scenario that sets BOTH `layer` and
    /// `tags.layer` to *different* values is contradictory (no live model can
    /// present that) and must fail the scenario load with a clear error rather
    /// than silently picking one.
    #[test]
    fn inconsistent_layer_and_tag_layer_is_rejected() {
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"contradictory layer\"
principal = \"agent\"
capability = \"apply\"
model = \"fct_revenue\"
layer = \"gold\"
tags = {{ layer = \"silver\" }}
expect = \"allow\"
"
        );
        let (_dir, path) = config_with(&body);
        let err = run_policy_test(&path, true).unwrap_err();
        assert!(
            err.to_string().contains("inconsistent"),
            "must reject the contradictory layer/tag pair: {err}"
        );
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

    /// S1 (#1089): with a REMOTE `[state]` backend, the freeze wraps the ledger
    /// write with a seam-scoped sync whose **download-before-open** half runs
    /// first. A deliberately-misconfigured remote backend (`s3`, no bucket)
    /// makes that download fail fast with `MissingConfig`, aborting the command
    /// BEFORE the ledger is opened/written. This proves the download-before
    /// half is wired and fatal: without it, freeze would record locally and
    /// return Ok (only to be clobbered by the next run's start-download).
    ///
    /// (A faithful remote round-trip proving the *upload-after* half isn't
    /// reachable from `rocky-cli`: the in-memory object-store seam is private to
    /// `rocky-core`'s test build. The upload half is wired identically and
    /// exercised by `rocky-core`'s `state_sync` round-trip tests.)
    #[test]
    fn freeze_remote_backend_download_before_is_wired_and_fatal() {
        let body = format!("{POLICY}\n[state]\nbackend = \"s3\"\n");
        let (dir, path) = config_with(&body);
        let state = dir.path().join("state.redb");

        let err = run_policy_freeze(
            &path,
            &state,
            Some(PolicyPrincipal::Agent),
            None,
            false,
            true,
        )
        .expect_err("a remote-backend freeze must abort when the backend is unreachable");
        assert!(
            err.to_string().contains("download remote state"),
            "download-before-open must be wired and fatal on a remote backend: {err}"
        );
        assert!(
            !state.exists(),
            "no local state should be written when the download-before-open aborts"
        );
    }

    /// S1 (#1089): the local (default) backend skips the remote-sync seam
    /// entirely — the freeze records and exits Ok with no remote round-trip.
    /// Paired with the test above (remote → attempted+fatal), this pins the
    /// `!matches!(backend, Local)` guard branching both ways.
    #[test]
    fn freeze_local_backend_records_without_remote_sync() {
        let body = format!("{POLICY}\n[state]\nbackend = \"local\"\n");
        let (dir, path) = config_with(&body);
        let state = dir.path().join("state.redb");

        run_policy_freeze(
            &path,
            &state,
            Some(PolicyPrincipal::Agent),
            None,
            false,
            true,
        )
        .expect("a local-backend freeze must record without any remote round-trip");
        let store = StateStore::open(&state).unwrap();
        let freezes = policy::active_freezes(&store.list_policy_decisions().unwrap());
        assert_eq!(freezes.len(), 1, "the freeze must be recorded locally");
    }
}
