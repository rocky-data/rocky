//! `rocky policy` — explain a policy decision (`check`) and assert policy
//! behaviour (`test`).
//!
//! `check` is a read-only explain surface: loads the project's `[policy]`
//! block, compiles the project to read the target model's attributes,
//! evaluates the `(principal, capability, model)` triple against the
//! policy, and prints the resolved effect + winning rule + reason. The
//! same evaluator is enforced at `apply`, `promote`, and the MCP write
//! tools, but `check` reports the *static* base decision only — the live
//! seams additionally project active freezes and autonomy-budget burn,
//! which can only tighten the effect (an `allow` here can still be
//! reviewed or denied at apply time; a `deny` stays a deny).
//!
//! `test` is the CI safety net: it runs the project's `[[policy.tests]]`
//! scenarios through the *same* evaluator and fails (non-zero exit) if any
//! resolved effect differs from what the scenario expected — so a policy edit
//! cannot silently open a hole.

use std::collections::BTreeSet;
use std::io::{self, Write};
use std::path::Path;

use anyhow::{Context, Result, bail};
use rocky_compiler::compile::{self, CompilerConfig};
use rocky_core::config::{
    ConfigError, PolicyCapability, PolicyConfig, PolicyEffect, PolicyPrincipal, StateBackend,
    StateConfig,
};
use rocky_core::freeze_marker::{
    self, ActiveMarkerFreeze, FreezeMarker, FreezeMarkerError, UnfreezeMarker,
};
use rocky_core::path_presence::{PathPresence, classify_not_found};
use rocky_core::policy::{self, ActiveFreeze, ModelAttributes};
use rocky_core::state::{PolicyDecisionRecord, StateStore};
use rocky_core::state_sync::StateSyncError;

use crate::output::{
    PolicyAutonomyBudgetOutput, PolicyCheckOutput, PolicyFreezeEntry, PolicyFreezeInForce,
    PolicyFreezeOutput, PolicyFreezeSources, PolicyModelAttributes, PolicyRuleEntry,
    PolicyRuleScopeOutput, PolicyRulesOutput, PolicyTestOutput, PolicyTestResult, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The decision `rocky policy check` reports: the effect the policy plane
/// would yield for `(principal, capability, model)`, with the matched rule,
/// the reason and the model's attributes. Pure compute, no printing;
/// [`run_policy_check`] renders it.
pub fn compute_policy_check(
    config_path: &Path,
    models_dir: &Path,
    principal: PolicyPrincipal,
    capability: PolicyCapability,
    model_name: &str,
) -> Result<PolicyCheckOutput> {
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

    Ok(output)
}

/// `rocky policy check`: [`compute_policy_check`], rendered as JSON or
/// text.
pub fn run_policy_check(
    config_path: &Path,
    models_dir: &Path,
    principal: PolicyPrincipal,
    capability: PolicyCapability,
    model_name: &str,
    json: bool,
) -> Result<()> {
    let output = compute_policy_check(config_path, models_dir, principal, capability, model_name)?;
    if json {
        print_json(&output)?;
    } else {
        render_text(&output);
    }
    Ok(())
}

/// The report `rocky policy test` prints.
///
/// Loads the project's `[policy]` block and its `[[policy.tests]]` scenarios,
/// runs every scenario through [`policy::evaluate`], and reports the pass/fail
/// verdict per scenario (actual vs expected effect, plus the deciding rule and
/// reason on a failure). A failing scenario is a row in the report, not an
/// error: [`run_policy_test`] prints the report and then exits non-zero when
/// `failed > 0`, so CI sees which scenario broke.
///
/// A missing `rocky.toml`, an absent `[policy]` block, or zero scenarios are
/// each treated as a hard error rather than a silent pass: a policy-test run
/// that asserts nothing would defeat the guardrail it exists to be.
pub fn compute_policy_test(config_path: &Path) -> Result<PolicyTestOutput> {
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

    Ok(output)
}

/// `rocky policy test`: [`compute_policy_test`], rendered, then a
/// non-zero exit when any scenario failed. The report is printed first so
/// a failing CI run still shows which scenario broke.
pub fn run_policy_test(config_path: &Path, json: bool) -> Result<()> {
    let output = compute_policy_test(config_path)?;
    if json {
        print_json(&output)?;
    } else {
        render_test_text(&output);
    }

    if output.failed > 0 {
        bail!(
            "{} of {} policy scenario(s) failed",
            output.failed,
            output.total
        );
    }

    Ok(())
}

/// The decision ledger's part of `rocky policy show`.
///
/// `source` is one of `"read"`, `"absent"` or `"local_mirror"`. Absent is
/// proven, not assumed: a store path that is missing is absence, a store path
/// that is there but cannot be read (a dangling link, an unreadable ancestor)
/// is an error.
#[derive(Debug)]
pub struct PolicyShowLedger {
    pub source: &'static str,
    pub freezes: Vec<ActiveFreeze>,
}

/// Read the freezes from the decision ledger, fail-closed.
///
/// `remote_backend` is `true` when `[state]` is not the local backend. There the
/// authoritative ledger lives remotely, and a governed apply downloads it before
/// it gates. This is a read-only producer and that download REPLACES the local
/// ledger file, so it must not run here. The read therefore reports
/// `"local_mirror"`: what it returns came from a mirror that may be stale or
/// empty, and a cross-pod freeze can be missing from it. Claiming `"absent"`
/// there would assert a proven absence that was never proven.
pub fn read_policy_show_ledger(
    state_path: &Path,
    remote_backend: bool,
) -> Result<PolicyShowLedger> {
    let read_label = if remote_backend {
        "local_mirror"
    } else {
        "read"
    };
    let absent_label = if remote_backend {
        "local_mirror"
    } else {
        "absent"
    };
    // `metadata` follows links: a dangling link is NotFound here, and the
    // classifier then reports it present-but-unreadable. An open on that path
    // would create the target through the link and report an empty ledger.
    match std::fs::metadata(state_path) {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            match classify_not_found(state_path) {
                PathPresence::Absent => {
                    return Ok(PolicyShowLedger {
                        source: absent_label,
                        freezes: Vec::new(),
                    });
                }
                PathPresence::Present { detail } => bail!(
                    "the state store at {} cannot be read ({detail}); refusing to report the \
                 policy plane without its freezes",
                    state_path.display()
                ),
            }
        }
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!(
                "the state store at {} cannot be read; refusing to report the policy plane \
                 without its freezes",
                state_path.display()
            )));
        }
    }
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let decisions = store
        .list_policy_decisions()
        .context("reading the policy decision ledger")?;
    Ok(PolicyShowLedger {
        source: read_label,
        freezes: policy::active_freezes(&decisions),
    })
}

/// What `rocky policy show` found when it looked for durable freeze markers.
///
/// Three outcomes, not two: "the backend keeps none" and "nothing enforces, so
/// we did not look" are different facts and a reader needs to tell them apart.
#[derive(Debug)]
pub enum PolicyShowMarkers {
    /// No `[policy]` block. The enforcement gate returns `NotConfigured`
    /// before it reads either freeze source, so neither did we.
    NotConsulted,
    /// The `[state]` backend has no durable object tier, so there are no
    /// markers to read.
    NotConfigured,
    /// The markers the durable tier holds.
    Read(Vec<ActiveMarkerFreeze>),
}

/// Read the durable freeze markers, fail-closed.
///
/// Gated exactly the way the apply gate's `marker_gate_provider` is gated: on
/// whether a durable object tier resolves, and NOT on `freeze_marker_writes`.
/// That flag gates writes only. A marker written while it was on stays enforced
/// after it is turned off, so a reader that honoured the flag would drop a live
/// freeze out of the document while an apply still denied on it.
///
/// A tier that is configured but will not resolve is an error, never an empty
/// list: reporting the ledger alone would read as a complete policy plane while
/// markers the fleet is enforcing stayed invisible.
///
/// The caller decides whether to call this at all; see [`compute_policy_show`].
pub async fn load_policy_show_markers(state_cfg: &StateConfig) -> Result<PolicyShowMarkers> {
    let provider = rocky_core::state_sync::durable_tier_provider(state_cfg)
        .context("resolving the durable object tier for freeze markers")?;
    let Some(provider) = provider else {
        return Ok(PolicyShowMarkers::NotConfigured);
    };
    let markers = freeze_marker::load_active_marker_freezes(&provider)
        .await
        .context(
            "reading the durable freeze markers; refusing to report the policy plane without them",
        )?;
    Ok(PolicyShowMarkers::Read(markers))
}

/// What `rocky policy show` needs from `rocky.toml`: the `[policy]` block
/// when there is one, and the `[state]` backend that says where freeze
/// markers live. A missing file is the default posture on a local backend;
/// a file that does not load is an error.
pub fn policy_show_config(config_path: &Path) -> Result<(Option<PolicyConfig>, StateConfig)> {
    match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => Ok((cfg.policy, cfg.state)),
        Err(ConfigError::FileNotFound { .. }) => Ok((None, StateConfig::default())),
        Err(e) => Err(anyhow::Error::new(e).context("loading rocky.toml for [policy]")),
    }
}

/// Build the report from its three inputs. Pure: the same inputs give the
/// same bytes whether the CLI or a route assembled them.
pub fn assemble_policy_show(
    policy: Option<&PolicyConfig>,
    ledger: PolicyShowLedger,
    markers: PolicyShowMarkers,
) -> PolicyRulesOutput {
    let PolicyShowLedger {
        source: ledger_source,
        freezes: ledger_freezes,
    } = ledger;
    let configured = policy.is_some();
    let default_posture;
    let policy = match policy {
        Some(p) => p,
        None => {
            default_posture = PolicyConfig::default_posture();
            &default_posture
        }
    };
    let rules = policy
        .rules
        .iter()
        .enumerate()
        .map(|(id, r)| PolicyRuleEntry {
            id,
            principal: r.principal,
            capability: r.capability,
            effect: r.effect,
            scope: PolicyRuleScopeOutput {
                any: r.scope.any,
                models: r.scope.models.clone(),
                tags: r.scope.tags.clone(),
                classifications: r.scope.classifications.clone(),
                exclude_classifications: r.scope.exclude_classifications.clone(),
                contracted: r.scope.contracted,
                layer: r.scope.layer.clone(),
                max_downstreams: r.scope.max_downstreams,
            },
            verify_after: r.verify_after.clone(),
            autonomy_budget: r
                .autonomy_budget
                .as_ref()
                .map(|b| PolicyAutonomyBudgetOutput {
                    failures: b.failures,
                    window: b.window.clone(),
                }),
        })
        .collect();
    let mut freezes: Vec<PolicyFreezeInForce> = ledger_freezes
        .into_iter()
        .map(|f| PolicyFreezeInForce {
            source: "ledger".to_string(),
            principal: Some(f.principal),
            scope: f.scope,
            reason: f.reason,
            since: Some(f.frozen_at),
            plan_id: Some(f.plan_id),
            freeze_id: None,
        })
        .collect();
    let markers_source = match &markers {
        PolicyShowMarkers::NotConsulted => "not_consulted",
        PolicyShowMarkers::NotConfigured => "not_configured",
        PolicyShowMarkers::Read(_) => "read",
    };
    if let PolicyShowMarkers::Read(markers) = markers {
        freezes.extend(markers.into_iter().map(|m| PolicyFreezeInForce {
            source: "marker".to_string(),
            principal: m.principal,
            scope: m.scope,
            reason: m.reason,
            since: m.created_at,
            plan_id: None,
            freeze_id: Some(m.freeze_id),
        }));
    }
    PolicyRulesOutput {
        version: VERSION.to_string(),
        command: "policy_show".to_string(),
        configured,
        policy_version: policy.version,
        default_agent_effect: policy.default_agent_effect,
        rules,
        freezes,
        freeze_sources: PolicyFreezeSources {
            ledger: ledger_source.to_string(),
            markers: markers_source.to_string(),
        },
    }
}

/// The report `rocky policy show` prints and `GET /api/v1/policy` serves:
/// [`policy_show_config`], [`read_policy_show_ledger`],
/// [`load_policy_show_markers`], then [`assemble_policy_show`]. Pure
/// compute, no printing; [`run_policy_show`] renders it.
pub async fn compute_policy_show(
    config_path: &Path,
    state_path: &Path,
) -> Result<PolicyRulesOutput> {
    let (policy, state_cfg) = policy_show_config(config_path)?;
    let (ledger, markers) = if policy.is_some() {
        let remote = policy_show_remote_backend(&state_cfg);
        (
            read_policy_show_ledger(state_path, remote)?,
            load_policy_show_markers(&state_cfg).await?,
        )
    } else {
        (
            policy_show_unconsulted_ledger(),
            PolicyShowMarkers::NotConsulted,
        )
    };
    Ok(assemble_policy_show(policy.as_ref(), ledger, markers))
}

/// `true` when the authoritative ledger lives on a remote `[state]` backend.
///
/// The same condition the apply gate's `remote_state_backend_for_gate` uses, so
/// the two agree on when a local read is only a mirror. Deliberately "not the
/// local backend" rather than "a durable object tier resolves": Valkey has no
/// object tier but its ledger is still remote, and calling a Valkey project's
/// local file authoritative is exactly the error this guards.
pub fn policy_show_remote_backend(state_cfg: &StateConfig) -> bool {
    !matches!(state_cfg.backend, StateBackend::Local)
}

/// The ledger half of a plane with no `[policy]` block.
///
/// With no policy the enforcement gate returns `NotConfigured` before it reads
/// any freeze source, so nothing is in force whatever either source holds.
/// Reading them anyway and listing what came back would put freezes in a
/// document whose own `configured` field says nothing enforces.
pub fn policy_show_unconsulted_ledger() -> PolicyShowLedger {
    PolicyShowLedger {
        source: "not_consulted",
        freezes: Vec::new(),
    }
}

/// `rocky policy show`: [`compute_policy_show`], rendered as JSON or text.
pub async fn run_policy_show(config_path: &Path, state_path: &Path, json: bool) -> Result<()> {
    let output = compute_policy_show(config_path, state_path).await?;
    if json {
        print_json(&output)?;
    } else {
        render_show_text(&mut io::stdout().lock(), &output)?;
    }
    Ok(())
}

fn scope_text(scope: &PolicyRuleScopeOutput) -> String {
    let mut parts: Vec<String> = Vec::new();
    if scope.any {
        parts.push("any".to_string());
    }
    if !scope.models.is_empty() {
        parts.push(format!("models={}", scope.models.join(",")));
    }
    for (k, v) in &scope.tags {
        parts.push(format!("tags.{k}={v}"));
    }
    if !scope.classifications.is_empty() {
        parts.push(format!(
            "classifications={}",
            scope.classifications.join(",")
        ));
    }
    if !scope.exclude_classifications.is_empty() {
        parts.push(format!(
            "exclude_classifications={}",
            scope.exclude_classifications.join(",")
        ));
    }
    if let Some(c) = scope.contracted {
        parts.push(format!("contracted={c}"));
    }
    if let Some(l) = &scope.layer {
        parts.push(format!("layer={l}"));
    }
    if let Some(n) = scope.max_downstreams {
        parts.push(format!("max_downstreams={n}"));
    }
    if parts.is_empty() {
        "(unscoped)".to_string()
    } else {
        parts.join(" ")
    }
}

/// How a freeze's principal reads in the text output.
///
/// `principal` is absent ONLY on a marker whose body could not be read, which
/// the loader widens to scope `any` and to both principals so the freeze fails
/// closed. Printing a plain `both` there would read as a marker that
/// deliberately froze both, which is a different and much less alarming fact.
///
/// This is a function rather than a line inside the `println!` so a test can
/// assert it. Round two of the review pointed out that the wording was
/// unreachable from any test, so reverting it would have gone unnoticed.
fn freeze_principal_text(f: &PolicyFreezeInForce) -> String {
    match f.principal {
        Some(p) => serde_plain(&p),
        None => "both (marker body unreadable)".to_string(),
    }
}

/// Render the policy plane as a compact human-readable block.
///
/// Writes to a sink rather than calling `println!` so a test can assert the
/// whole block. #1874 lost two lines this way in two review rounds — a
/// freeze's audit `plan_id`, and the wording that separates an unreadable
/// marker from a deliberate both-principal freeze — because the byte-parity
/// test pins JSON and nothing read the text at all. Rendering into a
/// `Vec<u8>` reaches the branches a binary test cannot set up cheaply: an
/// unreadable marker, a `local_mirror` ledger, and a plane with no `[policy]`
/// block.
fn render_show_text<W: Write>(w: &mut W, out: &PolicyRulesOutput) -> io::Result<()> {
    if out.configured {
        writeln!(w, "policy: [policy] version {}", out.policy_version)?;
    } else {
        writeln!(
            w,
            "policy: default posture (no [policy] block in rocky.toml)"
        )?;
    }
    writeln!(
        w,
        "default agent effect: {}",
        serde_plain(&out.default_agent_effect)
    )?;
    writeln!(w, "rules: {}", out.rules.len())?;
    for rule in &out.rules {
        write!(
            w,
            "  #{}  {}  {}  {}  {}",
            rule.id,
            serde_plain(&rule.principal),
            serde_plain(&rule.capability),
            serde_plain(&rule.effect),
            scope_text(&rule.scope)
        )?;
        if let Some(b) = &rule.autonomy_budget {
            write!(w, "  budget={}/{}", b.failures, b.window)?;
        }
        if !rule.verify_after.is_empty() {
            write!(w, "  verify_after={}", rule.verify_after.join(","))?;
        }
        writeln!(w)?;
    }
    // "recorded" not "in force": with no `[policy]` block the gate returns
    // NotConfigured before it reads a freeze source, so the list would be a
    // claim the engine does not honour. The sources line below says which were
    // consulted at all.
    let heading = if out.configured {
        "freezes in force"
    } else {
        "freezes in force (none: no [policy] block, so nothing is enforced)"
    };
    writeln!(w, "{}: {}", heading, out.freezes.len())?;
    for f in &out.freezes {
        let principal = freeze_principal_text(f);
        let since = f
            .since
            .map(|t| t.to_rfc3339())
            .unwrap_or_else(|| "-".to_string());
        write!(
            w,
            "  {}  {}  {}  since {}  reason: {}",
            f.source, principal, f.scope, since, f.reason
        )?;
        if let Some(id) = &f.freeze_id {
            write!(w, "  id={id}")?;
        }
        if let Some(plan) = &f.plan_id {
            write!(w, "  plan={plan}")?;
        }
        writeln!(w)?;
    }
    writeln!(
        w,
        "freeze sources: ledger {}, markers {}",
        out.freeze_sources.ledger, out.freeze_sources.markers
    )?;
    if out.freeze_sources.ledger == "local_mirror" {
        writeln!(
            w,
            "  note: [state] is a remote backend. The ledger above is the local mirror; the \
remote authority was not downloaded, so a freeze recorded by another pod may be missing."
        )?;
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
pub(crate) fn block_on_state_sync<T, F>(fut: F) -> Result<T, rocky_core::state_sync::StateSyncError>
where
    T: Send,
    F: std::future::Future<Output = Result<T, rocky_core::state_sync::StateSyncError>> + Send,
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

/// Map a freeze-marker write error into the state-sync error type
/// [`block_on_state_sync`] is typed over, preserving the object-store variant
/// verbatim so transport causes stay legible in the fail-closed context.
fn marker_error_to_sync(e: FreezeMarkerError) -> StateSyncError {
    match e {
        FreezeMarkerError::ObjectStore(inner) => StateSyncError::ObjectStore(inner),
        other => StateSyncError::Io(std::io::Error::other(other.to_string())),
    }
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
/// # Durable freeze markers — the un-erasable enforcement half
///
/// With effective blob CAS, this command replays its exact ledger rows on a
/// fresh shared `state.redb` blob and generation after a conflict. When CAS is
/// off or unsupported, the legacy half-seam remains last-writer-wins. The
/// enforcement truth that survives either mode — and the still-unconditional
/// gc/apply blob writers during the partial #1228 rollout — is a separate
/// **add-wins marker set** beside the state file (see
/// [`rocky_core::freeze_marker`]): `freeze` writes one create-once object
/// `<prefix>/freeze/<freeze_id>.json` per principal, `unfreeze` writes
/// `<prefix>/unfreeze/<unfreeze_id>.json` naming the exact `freeze_id`s it
/// lifts, and enforcement projects the order-independent active set from the
/// durable object tier. Marker **writes** are gated by `[state]
/// freeze_marker_writes` so a fleet can be upgraded to marker readers
/// everywhere before the first marker is written; reading and enforcement are
/// always on wherever a durable object tier exists. These marker keys never
/// bump the shared blob generation, so blob CAS does not serialize them or
/// close a marker-LIST-to-mutation window.
pub fn run_policy_freeze(
    config_path: &Path,
    state_path: &Path,
    principal: Option<PolicyPrincipal>,
    scope: Option<String>,
    reason: Option<String>,
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
    let mut notes = freeze_enforcement_notes(config_path, lift);
    for note in &notes {
        eprintln!("warning: {note}");
    }

    // Resolve the `[state]` backend (finding 7, fail-loud). Only a genuinely
    // ABSENT config selects the Local default (no remote sync) — a freeze
    // recorded with no config file is a legitimate local-only record. ANY OTHER
    // config error (malformed TOML, an unknown key tripping `deny_unknown_fields`,
    // a missing env var — including the very `[state]` block that would make this
    // a remote freeze) must ABORT: `unwrap_or_default()` would otherwise silently
    // pick the Local backend, record the freeze LOCAL-only, exit 0 with no upload,
    // and let a later remote download overwrite it. Mirrors the `[gc]
    // physical_delete` fail-loud pattern.
    // `[cache.schemas] replicate` rides alongside the `[state]` backend so the
    // freeze seam replicates exactly the table set every other leg does (#1620).
    let mut replicate_schema_cache = false;
    let state_cfg = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            replicate_schema_cache = cfg.cache.schemas.replicate;
            cfg.state
        }
        Err(ConfigError::FileNotFound { .. }) => StateConfig::default(),
        Err(e) => {
            return Err(anyhow::Error::new(e).context(format!(
                "refusing to record the policy freeze: {} failed to load, so the [state] backend \
                 cannot be resolved — a malformed config must not silently record a LOCAL-only \
                 freeze that a later remote download would overwrite (fail-closed). Fix the config \
                 and re-run.",
                config_path.display()
            )));
        }
    };
    let remote_state = !matches!(state_cfg.backend, StateBackend::Local);

    // Durable marker writes (two-phase rollout, write side): resolved up
    // front so a `freeze_marker_writes` flag pointing at a tier that cannot
    // hold markers aborts BEFORE anything is recorded. Config validation
    // already rejects the local/valkey-only combinations at load; the
    // re-check here is defensive. See docs/adr/ADR-CONCURRENCY.md (D3).
    let marker_provider = if remote_state && state_cfg.freeze_marker_writes {
        match rocky_core::state_sync::durable_tier_provider(&state_cfg)
            .context("resolving the durable object tier for freeze markers")?
        {
            Some(provider) => Some(provider),
            None => {
                bail!(
                    "[state] freeze_marker_writes = true requires a backend with a durable \
                     object tier (s3, gcs, or tiered); backend = \"{}\" cannot store durable \
                     freeze markers",
                    state_cfg.backend
                );
            }
        }
    } else {
        None
    };
    let seam_cas = rocky_core::state_sync::cas_effective(&state_cfg);

    // SEAM-SCOPED SYNC — download half. Pull the authoritative remote ledger
    // (overwriting the local file) BEFORE opening the store, so the freeze is
    // recorded on top of other pods' decisions rather than over an empty local.
    //
    // Effective CAS moves this download into each LedgerSeamSession attempt so
    // the installed bytes and generation are captured together. The legacy
    // half-seam remains unchanged when CAS is off or unsupported.
    if remote_state && !seam_cas {
        // WP-01 PR-B (2b): the session half-seam owns the download shape; a
        // successful download of either usable variant means the local ledger
        // now mirrors remote truth; failure still `?`-bails fail-closed
        // (unchanged).
        let _authority =
            block_on_state_sync(rocky_core::state_sync::RemoteStateSession::download_only(
                &state_cfg,
                state_path,
                replicate_schema_cache,
            ))
            .with_context(|| {
                "failed to download remote state before recording the policy freeze; \
                 a remote-backend freeze requires the state backend to be reachable"
            })?;
    }

    // Preserve the legacy open-before-timestamp ordering when CAS is inert.
    // Under effective CAS, LedgerSeamSession opens and drops a fresh store in
    // every attempt.
    let legacy_store =
        if seam_cas {
            None
        } else {
            Some(StateStore::open(state_path).with_context(|| {
                format!("failed to open state store at {}", state_path.display())
            })?)
        };

    let now = chrono::Utc::now();
    let prefix = if lift {
        policy::UNFREEZE_PLAN_PREFIX
    } else {
        policy::FREEZE_PLAN_PREFIX
    };

    let mut entries = Vec::new();
    let mut records = Vec::new();
    let mut freeze_markers: Vec<FreezeMarker> = Vec::new();
    for p in principals.iter().copied() {
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
        // The operator-supplied `--reason` is shared by the ledger row and
        // the durable marker; the synthesized description is the fallback.
        let reason = reason.clone().unwrap_or_else(|| {
            if lift {
                format!("policy unfreeze: lifted freeze for {principal_label} on scope '{scope}'")
            } else {
                format!(
                    "policy freeze: {principal_label} actions on scope '{scope}' frozen to deny"
                )
            }
        });
        // One durable marker per principal, mirroring the per-principal
        // ledger rows: fresh UUID, same `now`, same resolved reason. Minted
        // here, written after the store is dropped (see below).
        if !lift && marker_provider.is_some() {
            freeze_markers.push(FreezeMarker {
                freeze_id: uuid::Uuid::new_v4().to_string(),
                principal: p,
                scope: scope.clone(),
                reason: reason.clone(),
                created_at: now,
            });
        }
        let record = PolicyDecisionRecord {
            keys_recorded: false,
            models: Vec::new(),
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
        if let Some(store) = &legacy_store {
            store
                .record_policy_decision(&record)
                .context("failed to record the freeze decision to the ledger")?;
        }
        records.push(record);
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
    drop(legacy_store);

    // Durable freeze markers are written BEFORE the ledger upload (engage
    // early): if the blob upload then fails, the un-erasable marker is
    // already durable — the kill switch engaged even though the command exits
    // non-zero. Over-enforcement is the monotone-safe direction; the inverse
    // (unfreeze) writes its marker AFTER the upload, below.
    if !lift && let Some(provider) = &marker_provider {
        for marker in &freeze_markers {
            let key = freeze_marker::freeze_marker_key(&marker.freeze_id);
            block_on_state_sync(async {
                freeze_marker::write_freeze_marker(provider, marker)
                    .await
                    .map_err(marker_error_to_sync)
            })
            .with_context(|| {
                format!("failed to write durable freeze marker '{key}' (fail-closed)")
            })?;
            notes.push(format!(
                "durable freeze marker written: {key} ({})",
                serde_plain(&marker.principal)
            ));
        }
    }

    if seam_cas {
        // Freeze is always allowed and has no dynamic authorization or
        // external proof to refresh. Its complete replayable transition is the
        // exact set of pre-constructed ledger records above. The marker is
        // deliberately outside this closure because its create-once UUID
        // cannot be replayed.
        let session = rocky_core::state_sync::LedgerSeamSession::new(
            &state_cfg,
            state_path,
            replicate_schema_cache,
        );
        let expected_record_count = records.len();
        let committed_record_count =
            block_on_state_sync(session.execute(move |fresh_store, _fresh_base| {
                let records = records.clone();
                Box::pin(async move {
                    for record in &records {
                        fresh_store.record_policy_decision(record)?;
                    }
                    Ok(records.len())
                })
            }))
            .with_context(
                || "failed to commit the policy freeze ledger transition to shared remote state",
            )?;
        debug_assert_eq!(committed_record_count, expected_record_count);
    } else if remote_state {
        // WP-01 PR-B (2b): the half-seam owns the forced-`Fail` durability
        // policy (previously a local `StateConfig` clone here).
        block_on_state_sync(
            rocky_core::state_sync::RemoteStateSession::upload_only_fail_closed(
                &state_cfg,
                state_path,
                "policy freeze",
                replicate_schema_cache,
            ),
        )
        .with_context(|| "failed to upload remote state after recording the policy freeze")?;
    }

    // The unfreeze marker lands only once the superseding audit row is
    // durably visible (lift late) — until then other pods keep denying,
    // which is the safe direction (over-restriction).
    if lift && let Some(provider) = &marker_provider {
        // A kill-switch lift must not guess: a LIST/GET transport failure
        // aborts the command rather than lifting blind.
        let active = block_on_state_sync(async {
            freeze_marker::load_active_marker_freezes(provider)
                .await
                .map_err(StateSyncError::from)
        })
        .context(
            "failed to list durable freeze markers before lifting; an unfreeze must name the \
             exact markers it lifts (fail-closed)",
        )?;

        // Selection mirrors the ledger's `(principal, scope-string)` keying:
        // exact selector-string equality, not selector-overlap semantics. An
        // unreadable marker body (conservatively active for both principals
        // on scope "any") is lifted only by the broadest possible explicit
        // lift — no `--principal`, scope "any" — the deliberate escape hatch
        // for a corrupt marker.
        let covers_both_principals = principals.len() == 2;
        let lifted_ids: Vec<String> = active
            .iter()
            .filter(|m| match m.principal {
                Some(p) => principals.contains(&p) && m.scope == scope,
                None => covers_both_principals && scope == "any",
            })
            .map(|m| m.freeze_id.clone())
            .collect();

        if lifted_ids.is_empty() {
            // Writing a tombstone that lifts nothing would be noise.
            notes.push("no matching active freeze markers to lift".to_string());
        } else {
            let marker = UnfreezeMarker {
                unfreeze_id: uuid::Uuid::new_v4().to_string(),
                lifts: lifted_ids,
                principal: (principals.len() == 1).then_some(principals[0]),
                // Last use of the `--reason` argument: moved, not cloned.
                reason,
                created_at: now,
            };
            let key = freeze_marker::unfreeze_marker_key(&marker.unfreeze_id);
            block_on_state_sync(async {
                freeze_marker::write_unfreeze_marker(provider, &marker)
                    .await
                    .map_err(marker_error_to_sync)
            })
            .with_context(|| {
                format!("failed to write durable unfreeze marker '{key}' (fail-closed)")
            })?;
            notes.push(format!(
                "durable unfreeze marker written: {key} lifting [{}]",
                marker.lifts.join(", ")
            ));
        }
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

    /// `rocky policy check` compiles first and requires the target model
    /// to exist — the property that makes it the POST-draft confirmation
    /// only, never a cold-start oracle (the product posture verifier
    /// evaluates a synthetic post-image for exactly this reason).
    #[test]
    fn check_requires_the_model_to_exist() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();
        fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
        fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n",
        )
        .unwrap();
        let err = run_policy_check(
            &config,
            &models,
            rocky_core::config::PolicyPrincipal::Agent,
            rocky_core::config::PolicyCapability::Apply,
            "missing_model",
            false,
        )
        .expect_err("a nonexistent model must refuse");
        assert!(
            format!("{err:#}").contains("not found"),
            "the refusal names the missing model: {err:#}"
        );
    }

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

    /// Finding 7: a malformed config makes the `[state]` backend unresolvable.
    /// The freeze must ABORT (fail-loud) rather than silently pick the Local
    /// default and record a local-only freeze that a later remote download would
    /// overwrite. (A genuinely ABSENT config still selects Local — covered by
    /// `run_policy_freeze_records_and_succeeds_without_policy_block`.)
    #[test]
    fn run_policy_freeze_aborts_on_malformed_config() {
        // A present-but-invalid config: `load_rocky_config` returns a
        // non-FileNotFound error (unterminated string → TOML parse error).
        let (dir, path) = config_with("x = \"unterminated\n");
        let state = dir.path().join("state.redb");
        let err = run_policy_freeze(
            &path,
            &state,
            Some(PolicyPrincipal::Agent),
            None,
            None,
            false,
            true,
        )
        .expect_err("a malformed config must abort the freeze, not record it local-only");
        assert!(
            err.to_string().contains("failed to load"),
            "the abort must cite the config-load / [state]-resolution failure, got: {err}"
        );
        // The abort happens before the store is opened, so nothing is recorded.
        assert!(
            !state.exists(),
            "a malformed-config freeze must not create/record a local-only freeze"
        );
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
            None,
            false,
            true,
        )
        .expect("a local-backend freeze must record without any remote round-trip");
        let store = StateStore::open(&state).unwrap();
        let freezes = policy::active_freezes(&store.list_policy_decisions().unwrap());
        assert_eq!(freezes.len(), 1, "the freeze must be recorded locally");
    }

    /// The real policy-freeze command writes its create-once marker outside
    /// the CAS transition. Two injected state-blob conflicts force three full
    /// ledger attempts; the exact marker key must still see one create call.
    #[test]
    fn freeze_marker_is_written_once_across_two_cas_conflicts() {
        use rocky_core::fault_store::PutKind;
        use rocky_core::test_harness::CrossPodHarness;

        let _serial = rocky_core::state_sync::remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        let body = format!(
            "{POLICY}
[state]
backend = \"s3\"
s3_bucket = \"test\"
concurrency_control = \"cas\"
freeze_marker_writes = true
on_upload_failure = \"skip\"

[state.retry]
max_retries = 0
"
        );
        let (_dir, path) = config_with(&body);
        let object_key = format!(
            "v{}/state.redb",
            rocky_core::state::current_schema_version()
        );
        harness.faults.arm_precondition_failures(&object_key, 2);

        run_policy_freeze(
            &path,
            &harness.pod_a.state_path,
            Some(PolicyPrincipal::Agent),
            Some("any".to_string()),
            Some("two-conflict freeze".to_string()),
            false,
            true,
        )
        .expect("the third full transition attempt must commit");

        let provider = harness.provider.clone();
        let marker_keys = block_on_state_sync(async move {
            provider
                .list(rocky_core::freeze_marker::FREEZE_MARKER_PREFIX)
                .await
                .map_err(StateSyncError::ObjectStore)
        })
        .unwrap();
        assert_eq!(marker_keys.len(), 1, "one principal creates one marker");
        assert_eq!(
            harness.faults.put_count(&marker_keys[0], PutKind::Create),
            1,
            "the exact freeze-marker UUID must be written once, never replayed"
        );
        assert_eq!(
            harness.faults.put_count(&object_key, PutKind::Create),
            3,
            "two conflicts must force exactly three state-blob CAS attempts"
        );
        assert_eq!(
            harness
                .faults
                .put_count(&object_key, PutKind::Unconditional),
            0,
            "the CAS path must not issue an unconditional state-blob upload"
        );
    }

    /// Exhaustion propagates the typed conflict through the CLI layer, so the
    /// command exits nonzero even with `on_upload_failure = "skip"`. The
    /// existing remote winner remains the only ledger row, while the
    /// already-created freeze marker stays engaged.
    #[test]
    fn freeze_cas_exhaustion_is_nonzero_and_preserves_remote_winner() {
        use rocky_core::fault_store::PutKind;
        use rocky_core::test_harness::CrossPodHarness;

        let _serial = rocky_core::state_sync::remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        let winner = PolicyDecisionRecord {
            keys_recorded: false,
            models: Vec::new(),
            timestamp: chrono::Utc::now(),
            plan_id: "existing-run-winner".to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: "winner".to_string(),
            effect: PolicyEffect::Allow,
            rule_id: None,
            reason: "remote winner".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        };
        {
            let store = harness.open_store(&harness.pod_b);
            store.record_policy_decision(&winner).unwrap();
        }
        block_on_state_sync(harness.upload(&harness.pod_b)).unwrap();

        let body = format!(
            "{POLICY}
[state]
backend = \"s3\"
s3_bucket = \"test\"
concurrency_control = \"cas\"
freeze_marker_writes = true
on_upload_failure = \"skip\"

[state.retry]
max_retries = 0
"
        );
        let (_dir, path) = config_with(&body);
        let object_key = format!(
            "v{}/state.redb",
            rocky_core::state::current_schema_version()
        );
        harness.faults.arm_precondition_failures(&object_key, 3);

        let err = run_policy_freeze(
            &path,
            &harness.pod_a.state_path,
            Some(PolicyPrincipal::Agent),
            Some("any".to_string()),
            Some("exhausted freeze".to_string()),
            false,
            true,
        )
        .expect_err("three conflicts must make the command fail nonzero");
        assert!(
            err.chain().any(|cause| matches!(
                cause.downcast_ref::<StateSyncError>(),
                Some(StateSyncError::LedgerSeamConflict { attempts: 3, .. })
            )),
            "the typed exhausted-conflict error must survive anyhow context: {err:#}"
        );
        let local_rows = StateStore::open(&harness.pod_a.state_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert_eq!(
            local_rows,
            vec![winner.clone()],
            "the local state must contain only the remote winner; the losing freeze row must \
             not remain visible"
        );
        assert_eq!(harness.faults.put_count(&object_key, PutKind::Update), 3);
        assert_eq!(
            harness
                .faults
                .put_count(&object_key, PutKind::Unconditional),
            1,
            "only the explicit winner seed is unconditional; exhaustion adds no fallback put"
        );

        let provider = harness.provider.clone();
        let marker_keys = block_on_state_sync(async move {
            provider
                .list(rocky_core::freeze_marker::FREEZE_MARKER_PREFIX)
                .await
                .map_err(StateSyncError::ObjectStore)
        })
        .unwrap();
        assert_eq!(
            marker_keys.len(),
            1,
            "the marker remains engaged on failure"
        );
        assert_eq!(
            harness.faults.put_count(&marker_keys[0], PutKind::Create),
            1
        );

        harness.faults.clear();
        let verify_dir = TempDir::new().unwrap();
        let verify_path = verify_dir.path().join("verify.redb");
        let _authority = block_on_state_sync(rocky_core::state_sync::download_state(
            &harness.pod_b.cfg,
            &verify_path,
            false,
        ))
        .unwrap();
        let rows = StateStore::open(&verify_path)
            .unwrap()
            .list_policy_decisions()
            .unwrap();
        assert!(rows.iter().any(|row| row.plan_id == "existing-run-winner"));
        assert!(
            !rows.iter().any(|row| row.reason == "exhausted freeze"),
            "the losing freeze ledger row must not overwrite the remote winner"
        );
    }

    /// Unfreeze keeps the inverse ordering: its ledger row must win CAS before
    /// the unfreeze marker is created. Exhausting the ledger transition leaves
    /// the original freeze marker active and writes no lift object.
    #[test]
    fn unfreeze_marker_is_not_written_before_ledger_cas_wins() {
        use rocky_core::test_harness::CrossPodHarness;

        let _serial = rocky_core::state_sync::remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        let body = format!(
            "{POLICY}
[state]
backend = \"s3\"
s3_bucket = \"test\"
concurrency_control = \"cas\"
freeze_marker_writes = true

[state.retry]
max_retries = 0
"
        );
        let (_dir, path) = config_with(&body);
        run_policy_freeze(
            &path,
            &harness.pod_a.state_path,
            Some(PolicyPrincipal::Agent),
            Some("any".to_string()),
            Some("freeze before failed lift".to_string()),
            false,
            true,
        )
        .unwrap();

        let object_key = format!(
            "v{}/state.redb",
            rocky_core::state::current_schema_version()
        );
        harness.faults.arm_precondition_failures(&object_key, 3);
        run_policy_freeze(
            &path,
            &harness.pod_a.state_path,
            Some(PolicyPrincipal::Agent),
            Some("any".to_string()),
            Some("must not lift early".to_string()),
            true,
            true,
        )
        .expect_err("the unfreeze ledger transition must exhaust before marker creation");

        let provider = harness.provider.clone();
        let (freeze_keys, unfreeze_keys) = block_on_state_sync(async move {
            let freezes = provider
                .list(rocky_core::freeze_marker::FREEZE_MARKER_PREFIX)
                .await
                .map_err(StateSyncError::ObjectStore)?;
            let unfreezes = provider
                .list(rocky_core::freeze_marker::UNFREEZE_MARKER_PREFIX)
                .await
                .map_err(StateSyncError::ObjectStore)?;
            Ok((freezes, unfreezes))
        })
        .unwrap();
        assert_eq!(freeze_keys.len(), 1, "the original freeze stays engaged");
        assert!(
            unfreeze_keys.is_empty(),
            "no unfreeze marker may land before the superseding ledger row wins CAS"
        );
    }

    /// `run_policy_check --output json` is `compute_policy_check` plus one
    /// `print_json`; this pins the producer both callers share.
    #[test]
    fn compute_policy_check_serves_the_decision() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();
        fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
        fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n",
        )
        .unwrap();

        let out = compute_policy_check(
            &config,
            &models,
            rocky_core::config::PolicyPrincipal::Agent,
            rocky_core::config::PolicyCapability::Apply,
            "orders",
        )
        .unwrap();
        assert_eq!(out.command, "policy_check");
        assert_eq!(out.model, "orders");
        assert!(!out.model_attributes.contracted);
        assert_eq!(
            out.effect,
            PolicyEffect::RequireReview,
            "an uncontracted agent apply matches no rule and falls to the default posture"
        );
        assert_eq!(out.matched_rule, None);
    }

    /// `run_policy_test --output json` is `compute_policy_test` plus one
    /// `print_json` and the exit code. The seam reports every scenario, pass
    /// and fail alike, and never exits.
    #[test]
    fn compute_policy_test_reports_every_scenario() {
        let body = format!(
            "{POLICY}
[[policy.tests]]
name = \"contracted apply is denied\"
principal = \"agent\"
capability = \"apply\"
contracted = true
expect = \"deny\"

[[policy.tests]]
name = \"wrong on purpose\"
principal = \"agent\"
capability = \"apply\"
contracted = true
expect = \"allow\"
"
        );
        let (_dir, path) = config_with(&body);

        let out = compute_policy_test(&path).unwrap();
        assert_eq!(out.command, "policy_test");
        assert_eq!((out.total, out.passed, out.failed), (2, 1, 1));
        assert!(out.results[0].passed);
        assert!(!out.results[1].passed);
        assert_eq!(out.results[1].name, "wrong on purpose");
        assert_eq!(out.results[1].actual, PolicyEffect::Deny);
    }

    /// No `[policy]` block and no state store yet: the default posture, no
    /// rules, no freezes, and the sources say why. Both read `not_consulted`,
    /// because the enforcement gate returns `NotConfigured` before it reads
    /// either source — so reporting "absent" would answer a question this
    /// producer never asked. An honest empty, not a silent one.
    #[tokio::test]
    async fn compute_policy_show_reports_the_default_posture_without_a_policy_block() {
        let (dir, config) = config_with(NO_POLICY_BODY);
        let state_path = dir.path().join("state.redb");

        let out = compute_policy_show(&config, &state_path).await.unwrap();
        assert_eq!(out.command, "policy_show");
        assert!(!out.configured);
        assert_eq!(out.policy_version, 1);
        assert_eq!(out.default_agent_effect, PolicyEffect::RequireReview);
        assert!(out.rules.is_empty());
        assert!(out.freezes.is_empty());
        assert_eq!(out.freeze_sources.ledger, "not_consulted");
        assert_eq!(out.freeze_sources.markers, "not_consulted");
    }

    /// The rules come out in file order with their position as `id`, and a
    /// freeze recorded through `rocky policy freeze` is in force from the
    /// ledger. `run_policy_show --output json` is `compute_policy_show` plus
    /// one `print_json`, so this pins the producer both callers share.
    #[tokio::test]
    async fn compute_policy_show_lists_rules_by_position_and_the_freezes_in_force() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let state_path = dir.path().join("state.redb");
        run_policy_freeze(
            &config,
            &state_path,
            Some(PolicyPrincipal::Agent),
            Some("model=fct_*".to_string()),
            Some("incident 42".to_string()),
            false,
            true,
        )
        .unwrap();

        let out = compute_policy_show(&config, &state_path).await.unwrap();
        assert!(out.configured);
        assert_eq!(out.policy_version, 1);
        assert_eq!(
            out.rules.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(out.rules[0].effect, PolicyEffect::Deny);
        assert_eq!(out.rules[0].scope.contracted, Some(true));
        assert_eq!(
            out.rules[1].scope.tags.get("layer").map(String::as_str),
            Some("bronze")
        );
        assert_eq!(out.rules[1].scope.max_downstreams, Some(5));
        assert_eq!(out.rules[2].scope.max_downstreams, None);

        assert_eq!(out.freezes.len(), 1, "{:?}", out.freezes);
        let f = &out.freezes[0];
        assert_eq!(f.source, "ledger");
        assert_eq!(f.principal, Some(PolicyPrincipal::Agent));
        assert_eq!(f.scope, "model=fct_*");
        assert_eq!(f.reason, "incident 42");
        assert!(f.since.is_some());
        assert!(f.plan_id.is_some());
        assert!(f.freeze_id.is_none());
        assert_eq!(out.freeze_sources.ledger, "read");
        assert_eq!(out.freeze_sources.markers, "not_configured");

        // Lifting it takes it out of force: the report reads the ledger's
        // projection, not its raw rows.
        run_policy_freeze(
            &config,
            &state_path,
            Some(PolicyPrincipal::Agent),
            Some("model=fct_*".to_string()),
            None,
            true,
            true,
        )
        .unwrap();
        let out = compute_policy_show(&config, &state_path).await.unwrap();
        assert!(out.freezes.is_empty(), "{:?}", out.freezes);
    }

    /// A rule's `id` is the number `rocky policy check` reports as
    /// `matched_rule`: the two commands agree on which rule won.
    #[test]
    fn a_rule_id_is_the_matched_rule_policy_check_reports() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let models = dir.path().join("models");
        fs::create_dir_all(&models).unwrap();
        fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").unwrap();
        fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"orders\"\n\
             [tags]\nlayer = \"bronze\"\n",
        )
        .unwrap();
        let check = compute_policy_check(
            &config,
            &models,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            "orders",
        )
        .unwrap();
        let matched = check
            .matched_rule
            .expect("a bronze additive change matches a rule");

        let (_, state_cfg) = policy_show_config(&config).unwrap();
        let ledger = read_policy_show_ledger(&dir.path().join("state.redb"), false).unwrap();
        let (policy, _) = policy_show_config(&config).unwrap();
        let show = assemble_policy_show(policy.as_ref(), ledger, PolicyShowMarkers::NotConfigured);
        assert!(matches!(state_cfg.backend, StateBackend::Local));
        let rule = &show.rules[matched];
        assert_eq!(rule.id, matched);
        assert_eq!(rule.capability, PolicyCapability::SchemaChangeAdditive);
        assert_eq!(rule.effect, check.effect);
    }

    /// A state store path that is there but cannot be read is an error, not
    /// an empty freeze list: an empty list would say "nothing is frozen" for
    /// a plane whose freezes could not be read at all.
    #[cfg(unix)]
    #[test]
    fn read_policy_show_ledger_refuses_a_dangling_state_store_link() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        std::os::unix::fs::symlink(dir.path().join("gone.redb"), &state_path).unwrap();

        let err = read_policy_show_ledger(&state_path, false).unwrap_err();
        assert!(
            format!("{err:#}").contains("cannot be read"),
            "the refusal says the store is unreadable, not absent: {err:#}"
        );
    }

    /// A durable marker is a freeze in force with `source` `"marker"` and its
    /// id; the ledger's entries come first and `freeze_sources.markers` says
    /// the markers were read.
    ///
    /// The marker here carries no principal, which the loader produces ONLY
    /// for a marker whose body it could not read: it widens such a marker to
    /// scope `any` and to both principals so the freeze fails closed. It is
    /// not a marker that deliberately froze both, and the reason text says so.
    #[test]
    fn assemble_policy_show_maps_markers_after_the_ledger() {
        let policy = PolicyConfig::default_posture();
        let frozen_at = chrono::Utc::now();
        let ledger = PolicyShowLedger {
            source: "read",
            freezes: vec![ActiveFreeze {
                principal: PolicyPrincipal::Agent,
                scope: "model=fct_*".to_string(),
                frozen_at,
                plan_id: "freeze-1".to_string(),
                reason: "from the ledger".to_string(),
            }],
        };
        // Shaped exactly as `project_active` builds an unreadable marker:
        // principal None, scope widened to `any`, and a reason that names the
        // unreadable body. A hand-built fixture that dropped the reason would
        // hide the very thing this asserts.
        let markers = vec![ActiveMarkerFreeze {
            freeze_id: "marker-1".to_string(),
            principal: None,
            scope: "any".to_string(),
            reason: "unreadable freeze marker body (expected value at line 1)".to_string(),
            created_at: None,
        }];

        let out = assemble_policy_show(Some(&policy), ledger, PolicyShowMarkers::Read(markers));
        assert_eq!(out.freeze_sources.markers, "read");
        assert_eq!(out.freezes.len(), 2);
        assert_eq!(out.freezes[0].source, "ledger");
        assert_eq!(out.freezes[0].plan_id.as_deref(), Some("freeze-1"));
        assert_eq!(out.freezes[0].since, Some(frozen_at));
        let m = &out.freezes[1];
        assert_eq!(m.source, "marker");
        assert_eq!(m.freeze_id.as_deref(), Some("marker-1"));
        assert_eq!(
            m.principal, None,
            "an unreadable marker body names no principal; the loader widens it to both"
        );
        assert_eq!(m.scope, "any", "an unreadable body widens the scope");
        assert!(
            m.reason.contains("unreadable freeze marker body"),
            "the reason must say the body was unreadable, not imply a deliberate freeze: {}",
            m.reason
        );
        assert_eq!(m.since, None);
        assert_eq!(m.plan_id, None);
    }

    /// `freeze_marker_writes = true` on a backend whose durable tier cannot be
    /// resolved is a refusal, not an empty marker list. Without this the policy
    /// plane would report the ledger's freezes alone and read as complete,
    /// while durable markers the fleet is enforcing stayed invisible. No
    /// network: the bucket is missing, so the tier never resolves.
    #[tokio::test]
    async fn a_marker_tier_that_cannot_be_resolved_refuses_the_policy_plane() {
        let body = format!(
            "{NO_POLICY_BODY}\n{POLICY}\n[state]\nbackend = \"s3\"\nfreeze_marker_writes = true\n"
        );
        let (dir, config) = config_with(&body);
        let (policy, state_cfg) = policy_show_config(&config).unwrap();
        assert!(
            policy.is_some(),
            "the producer only consults a freeze source when a [policy] block exists, so this \
             case needs one"
        );

        let err = load_policy_show_markers(&state_cfg).await.unwrap_err();
        let text = format!("{err:#}");
        assert!(
            text.contains("state.s3_bucket"),
            "the refusal must name the missing key, got: {text}"
        );

        // The whole producer refuses too, so no caller sees a partial plane.
        let state_path = dir.path().join("state.redb");
        assert!(
            compute_policy_show(&config, &state_path).await.is_err(),
            "compute_policy_show must not report a plane whose markers it could not read"
        );
    }

    /// A local backend has no durable object tier, so there are no markers to
    /// read and the reader says `NotConfigured` rather than erroring.
    ///
    /// The read is NOT gated on `freeze_marker_writes`. That flag gates writes:
    /// a marker written while it was on stays enforced after it is turned off,
    /// and the apply gate resolves the tier without consulting the flag. A
    /// reader that honoured it would drop a live freeze out of the document
    /// while an apply still denied on it. So the flag changes nothing here, in
    /// either direction, and both assertions below must hold.
    #[tokio::test]
    async fn a_local_backend_reports_markers_as_not_configured() {
        let (_dir, config) = config_with(NO_POLICY_BODY);
        let (_policy, state_cfg) = policy_show_config(&config).unwrap();
        assert!(matches!(state_cfg.backend, StateBackend::Local));
        assert!(matches!(
            load_policy_show_markers(&state_cfg).await.unwrap(),
            PolicyShowMarkers::NotConfigured
        ));

        let flagged = StateConfig {
            freeze_marker_writes: true,
            ..StateConfig::default()
        };
        assert!(
            matches!(
                load_policy_show_markers(&flagged).await.unwrap(),
                PolicyShowMarkers::NotConfigured
            ),
            "the write flag must not change what a reader sees"
        );
    }

    /// On a remote `[state]` backend the local file is a MIRROR, not the
    /// authority. A governed apply downloads the remote ledger before it
    /// gates; this producer must not, because that download replaces the local
    /// ledger and this is a read-only route. So it must not claim `"absent"`
    /// either — absence there was never proven, it was never looked for. A
    /// screen that read `"absent"` as "nothing is frozen" would contradict an
    /// apply that denies on another pod's freeze.
    #[test]
    fn a_remote_backend_reports_the_ledger_as_a_local_mirror_not_as_absent() {
        let dir = TempDir::new().unwrap();
        let missing = dir.path().join("state.redb");

        let local = read_policy_show_ledger(&missing, false).unwrap();
        assert_eq!(local.source, "absent", "a local backend proves absence");

        let remote = read_policy_show_ledger(&missing, true).unwrap();
        assert_eq!(
            remote.source, "local_mirror",
            "a remote backend never proves absence from the local file alone"
        );
        assert!(remote.freezes.is_empty());
    }

    /// `policy_show_remote_backend` must agree with the apply gate's
    /// `remote_state_backend_for_gate`: every backend that is not Local keeps
    /// its authority remotely. Valkey has no durable OBJECT tier but its
    /// ledger is still remote, so a tier-based test would wrongly call a
    /// Valkey project's local file authoritative.
    #[test]
    fn every_backend_but_local_keeps_its_ledger_authority_remotely() {
        for backend in [
            StateBackend::S3,
            StateBackend::Gcs,
            StateBackend::Valkey,
            StateBackend::Tiered,
        ] {
            let cfg = StateConfig {
                backend,
                ..StateConfig::default()
            };
            assert!(
                policy_show_remote_backend(&cfg),
                "{backend} keeps its ledger remotely"
            );
        }
        assert!(!policy_show_remote_backend(&StateConfig::default()));
    }

    /// With no `[policy]` block the enforcement gate returns `NotConfigured`
    /// BEFORE it reads a freeze source, so nothing is in force whatever the
    /// ledger holds. The document must not list a freeze the engine would not
    /// honour: both sources report `not_consulted` and the list is empty, even
    /// though this project has a freeze recorded in its ledger.
    #[tokio::test]
    async fn a_recorded_freeze_is_not_in_force_without_a_policy_block() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let state_path = dir.path().join("state.redb");
        run_policy_freeze(
            &config,
            &state_path,
            Some(PolicyPrincipal::Agent),
            Some("model=fct_*".to_string()),
            Some("incident 42".to_string()),
            false,
            true,
        )
        .unwrap();

        // Same state store, same recorded freeze, but the config no longer has
        // a [policy] block: only the plane changed.
        let (_dir2, no_policy) = config_with(NO_POLICY_BODY);
        let out = compute_policy_show(&no_policy, &state_path).await.unwrap();

        assert!(!out.configured);
        assert!(
            out.freezes.is_empty(),
            "a freeze the gate never reads is not in force: {:?}",
            out.freezes
        );
        assert_eq!(out.freeze_sources.ledger, "not_consulted");
        assert_eq!(out.freeze_sources.markers, "not_consulted");

        // And with the block restored, the same store reports it in force.
        let back = compute_policy_show(&config, &state_path).await.unwrap();
        assert_eq!(back.freezes.len(), 1, "the freeze itself never moved");
        assert_eq!(back.freeze_sources.ledger, "read");
    }

    /// `verify_after` is a first-class rule field that decides whether a
    /// mutation this rule governs is gated on named post-apply checks. Two
    /// rules that differ only here govern differently, so a document that
    /// omitted it would render them identically. `conditions` is deliberately
    /// absent: it decides nothing and can hold a resolved `${VAR}`.
    #[tokio::test]
    async fn rules_carry_verify_after_and_never_carry_conditions() {
        let body = format!(
            "{NO_POLICY_BODY}\n{POLICY}\n\n[[policy.rules]]\nprincipal = \"agent\"\n\
             capability = \"apply\"\nscope = {{ any = true }}\neffect = \"allow\"\n\
             verify_after = [\"row_count\", \"freshness\"]\n\
             conditions = {{ token = \"super-secret-value\" }}\n"
        );
        let (dir, config) = config_with(&body);
        let state_path = dir.path().join("state.redb");

        let out = compute_policy_show(&config, &state_path).await.unwrap();
        let last = out.rules.last().expect("the appended rule");
        assert_eq!(last.verify_after, vec!["row_count", "freshness"]);

        let json = serde_json::to_string(&out).unwrap();
        assert!(
            !json.contains("conditions"),
            "the document must not carry a rule's conditions"
        );
        assert!(
            !json.contains("super-secret-value"),
            "an authored condition can hold a resolved ${{VAR}}; it must not reach the output"
        );
    }

    /// A marker must be READ even when `freeze_marker_writes` is false.
    ///
    /// The round-two review called the first version of this test vacuous, and
    /// it was right: it used a local backend, where the OLD gate returned no
    /// markers either, so restoring the old condition would still have passed.
    /// This one needs a REMOTE backend with a marker actually present, because
    /// that is the only shape where the two conditions disagree. The flag
    /// gates writes; a marker written while it was on stays enforced after it
    /// is turned off, and the apply gate reads it regardless.
    #[tokio::test]
    async fn a_marker_is_read_even_when_the_write_flag_is_off() {
        let _serial = rocky_core::state_sync::remote_testing::serial_guard();
        let _harness = rocky_core::test_harness::CrossPodHarness::new_s3_like();

        let body = format!(
            "{NO_POLICY_BODY}\n{POLICY}\n[state]\nbackend = \"s3\"\ns3_bucket = \"test\"\n\
             freeze_marker_writes = false\n"
        );
        let (_dir, config) = config_with(&body);
        let (_policy, state_cfg) = policy_show_config(&config).unwrap();
        assert!(
            !state_cfg.freeze_marker_writes,
            "this test is only meaningful with the write flag OFF"
        );

        let provider = rocky_core::state_sync::durable_tier_provider(&state_cfg)
            .unwrap()
            .expect("an s3 backend resolves a durable tier");
        rocky_core::freeze_marker::write_freeze_marker(
            &provider,
            &rocky_core::freeze_marker::FreezeMarker {
                freeze_id: "marker-kept".to_string(),
                principal: PolicyPrincipal::Agent,
                scope: "any".to_string(),
                reason: "written while the flag was on".to_string(),
                created_at: chrono::Utc::now(),
            },
        )
        .await
        .unwrap();

        match load_policy_show_markers(&state_cfg).await.unwrap() {
            PolicyShowMarkers::Read(markers) => {
                assert_eq!(markers.len(), 1, "the marker is still enforced, so read it");
                assert_eq!(markers[0].freeze_id, "marker-kept");
            }
            other => panic!(
                "the write flag must not stop a reader from seeing an enforced marker, got \
                 {other:?}"
            ),
        }
    }

    /// The PRESENT-file half of the local-mirror rule.
    ///
    /// The first version of this only covered a missing file, which left the
    /// `read_label` path unproved: a remote backend that found a populated
    /// local store could still have said `"read"` and no test would have
    /// noticed. A mirror that HAS rows is exactly the dangerous case, because
    /// a non-empty list is the one a reader is most likely to trust.
    #[tokio::test]
    async fn a_populated_local_store_still_reads_as_a_mirror_on_a_remote_backend() {
        let (dir, config) = config_with(&format!("{NO_POLICY_BODY}\n{POLICY}"));
        let state_path = dir.path().join("state.redb");
        run_policy_freeze(
            &config,
            &state_path,
            Some(PolicyPrincipal::Agent),
            Some("model=fct_*".to_string()),
            Some("incident 42".to_string()),
            false,
            true,
        )
        .unwrap();

        let local = read_policy_show_ledger(&state_path, false).unwrap();
        assert_eq!(local.source, "read");
        assert_eq!(local.freezes.len(), 1);

        let mirrored = read_policy_show_ledger(&state_path, true).unwrap();
        assert_eq!(
            mirrored.source, "local_mirror",
            "a populated local store on a remote backend is still only a mirror"
        );
        assert_eq!(
            mirrored.freezes.len(),
            1,
            "the rows it did see are still reported; only the completeness claim changes"
        );
    }

    /// The text must not call an unreadable marker a deliberate both-principal
    /// freeze. Round two found this wording had no test at all: it lived
    /// inside a `println!`, so reverting it to a plain `both` would have
    /// passed everything. Now it is a function, and this is that test.
    #[test]
    fn an_unreadable_marker_does_not_read_as_a_deliberate_both_principal_freeze() {
        let unreadable = PolicyFreezeInForce {
            source: "marker".to_string(),
            principal: None,
            scope: "any".to_string(),
            reason: "unreadable freeze marker body (expected value at line 1)".to_string(),
            since: None,
            plan_id: None,
            freeze_id: Some("marker-1".to_string()),
        };
        let text = freeze_principal_text(&unreadable);
        assert_ne!(
            text, "both",
            "a bare `both` hides why the principal is absent"
        );
        assert!(
            text.contains("unreadable"),
            "the principal column must say the body was unreadable, got: {text}"
        );

        let deliberate = PolicyFreezeInForce {
            principal: Some(PolicyPrincipal::Agent),
            ..unreadable
        };
        assert_eq!(freeze_principal_text(&deliberate), "agent");
    }

    /// #1879. The text renderer, rendered.
    fn show_text(out: &PolicyRulesOutput) -> String {
        let mut buf = Vec::new();
        render_show_text(&mut buf, out).expect("a Vec sink never fails");
        String::from_utf8(buf).expect("the renderer writes UTF-8")
    }

    /// A plane with a `[policy]` block, one rule and one ledger freeze.
    /// Every optional field is set, so a dropped one shows up as a missing
    /// column rather than as an equal-but-shorter line.
    fn populated_plane() -> PolicyRulesOutput {
        PolicyRulesOutput {
            version: "1".to_string(),
            command: "policy_show".to_string(),
            configured: true,
            policy_version: 3,
            default_agent_effect: PolicyEffect::RequireReview,
            rules: vec![PolicyRuleEntry {
                id: 0,
                principal: PolicyPrincipal::Agent,
                capability: PolicyCapability::Apply,
                effect: PolicyEffect::Deny,
                scope: PolicyRuleScopeOutput {
                    any: false,
                    models: vec!["orders".to_string()],
                    tags: [("tier".to_string(), "gold".to_string())]
                        .into_iter()
                        .collect(),
                    classifications: Vec::new(),
                    exclude_classifications: Vec::new(),
                    contracted: None,
                    layer: None,
                    max_downstreams: None,
                },
                verify_after: vec!["freshness".to_string(), "row_count".to_string()],
                autonomy_budget: Some(PolicyAutonomyBudgetOutput {
                    failures: 3,
                    window: "24h".to_string(),
                }),
            }],
            freezes: vec![PolicyFreezeInForce {
                source: "ledger".to_string(),
                principal: Some(PolicyPrincipal::Human),
                scope: "any".to_string(),
                reason: "incident 4412".to_string(),
                since: Some(
                    chrono::DateTime::parse_from_rfc3339("2026-09-01T12:00:00Z")
                        .unwrap()
                        .with_timezone(&chrono::Utc),
                ),
                plan_id: Some("plan-9".to_string()),
                freeze_id: Some("fz-1".to_string()),
            }],
            freeze_sources: PolicyFreezeSources {
                ledger: "read".to_string(),
                markers: "read".to_string(),
            },
        }
    }

    /// #1879. The whole block, asserted as bytes.
    ///
    /// Round one of the #1874 review found the text dropped a freeze's audit
    /// `plan_id`. The byte-parity test pins JSON, so nothing noticed, and a
    /// substring assertion would not have either — the line was present and
    /// merely shorter. This compares the entire rendering, which is the only
    /// assertion shape a dropped column cannot survive.
    #[test]
    fn the_whole_block_carries_every_authored_field() {
        let rendered = show_text(&populated_plane());
        assert_eq!(
            rendered,
            "policy: [policy] version 3\n\
             default agent effect: require_review\n\
             rules: 1\n\
             \x20 #0  agent  apply  deny  models=orders tags.tier=gold  budget=3/24h  \
             verify_after=freshness,row_count\n\
             freezes in force: 1\n\
             \x20 ledger  human  any  since 2026-09-01T12:00:00+00:00  reason: incident 4412  \
             id=fz-1  plan=plan-9\n\
             freeze sources: ledger read, markers read\n",
        );
    }

    /// #1879 + round two of #1874. An unreadable marker widens to both
    /// principals so it fails closed; the text must not present that as a
    /// marker that deliberately froze both.
    ///
    /// [`an_unreadable_marker_does_not_read_as_a_deliberate_both_principal_freeze`]
    /// pins the label. This pins that the renderer actually puts it on the
    /// line — the two are separable, and only one of them was covered.
    #[test]
    fn an_unreadable_marker_says_so_in_the_rendered_block() {
        let mut plane = populated_plane();
        plane.rules.clear();
        plane.freezes = vec![PolicyFreezeInForce {
            source: "marker".to_string(),
            principal: None,
            scope: "any".to_string(),
            reason: "unreadable freeze marker body (expected value at line 1)".to_string(),
            since: None,
            plan_id: None,
            freeze_id: Some("marker-1".to_string()),
        }];

        let rendered = show_text(&plane);
        assert_eq!(
            rendered,
            "policy: [policy] version 3\n\
             default agent effect: require_review\n\
             rules: 0\n\
             freezes in force: 1\n\
             \x20 marker  both (marker body unreadable)  any  since -  reason: unreadable \
             freeze marker body (expected value at line 1)  id=marker-1\n\
             freeze sources: ledger read, markers read\n",
        );
    }

    /// #1879. A remote `[state]` backend makes the freeze list non-exhaustive,
    /// and the note is the only place the text says so. It sits behind a
    /// string comparison on `freeze_sources.ledger`, which no binary test
    /// reaches without standing up a remote backend.
    #[test]
    fn a_local_mirror_ledger_renders_the_completeness_note() {
        let mut plane = populated_plane();
        plane.rules.clear();
        plane.freezes.clear();
        plane.freeze_sources = PolicyFreezeSources {
            ledger: "local_mirror".to_string(),
            markers: "read".to_string(),
        };

        let rendered = show_text(&plane);
        assert_eq!(
            rendered,
            "policy: [policy] version 3\n\
             default agent effect: require_review\n\
             rules: 0\n\
             freezes in force: 0\n\
             freeze sources: ledger local_mirror, markers read\n\
             \x20 note: [state] is a remote backend. The ledger above is the local mirror; \
             the remote authority was not downloaded, so a freeze recorded by another pod \
             may be missing.\n",
        );

        let read = show_text(&populated_plane());
        assert!(
            !read.contains("note:"),
            "a fully-read ledger must not carry the incompleteness note: {read}"
        );
    }

    /// #1879. With no `[policy]` block the gate answers `NotConfigured`
    /// before it reads a freeze source, so a bare "freezes in force: 0" would
    /// read as a plane that was checked and found clean. The heading says
    /// nothing is enforced instead.
    #[test]
    fn a_plane_with_no_policy_block_says_nothing_is_enforced() {
        let mut plane = populated_plane();
        plane.configured = false;
        plane.rules.clear();
        plane.freezes.clear();
        plane.freeze_sources = PolicyFreezeSources {
            ledger: "not_consulted".to_string(),
            markers: "not_consulted".to_string(),
        };

        let rendered = show_text(&plane);
        assert_eq!(
            rendered,
            "policy: default posture (no [policy] block in rocky.toml)\n\
             default agent effect: require_review\n\
             rules: 0\n\
             freezes in force (none: no [policy] block, so nothing is enforced): 0\n\
             freeze sources: ledger not_consulted, markers not_consulted\n",
        );
    }
}
