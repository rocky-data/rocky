//! `rocky product <compile|verify|status|approve>` — the deterministic
//! product-spec verbs.
//!
//! A product spec (`products/<name>.toml`) declares WHAT a data product
//! must be; these verbs parse it, verify the trust posture it requires,
//! lower it onto engine primitives, and record a human's approval of a
//! spec revision. They are gate-side code: this module is a sibling of
//! the other command modules and reaches engine internals directly — the
//! policy verdicts here come from `rocky_core::policy::evaluate`, the
//! same evaluator every enforcement path runs, never from a mirror.
//!
//! # The verbs
//!
//! - **verify** — the frozen `propose_only` posture (D5), fail-closed:
//!   a `[policy]` block must exist, `default_agent_effect` must be
//!   `require_review`, agent propose must resolve `allow` through an
//!   explicit budget-free rule scoped to EXACTLY this product's output
//!   model, and agent apply must resolve `require_review` or `deny`.
//!   A posture failure prints a paste-ready block. Then classification
//!   tags must resolve (a spec-compile ERROR where the engine's W004
//!   only warns) and product identities must not collide.
//! - **compile** — verify, then run the next lowering phase through the
//!   staged commit protocol (`rocky_core::product::commit`): Phase A
//!   before drafting, Phase B once the drafted sidecar exists.
//! - **status** — read-only report: spec identity, committed manifest,
//!   artifact byte-verification, approval record + snapshot integrity,
//!   fulfillment state. Status never mutates — it reports a pending
//!   staging journal rather than resolving it.
//! - **approve** — the authority transition: write the immutable
//!   digest-addressed snapshot file FIRST, then ONE state-store write
//!   transaction that CASes the approval record, CASes the fulfillment
//!   state, and appends the journal row
//!   ([`rocky_core::state::StateStore::product_approval_cas`]).
//!
//! # Exit codes
//!
//! `verify` exits 0 on `pass`, 1 on `needs_input`, 2 on `fail` — after
//! printing the full (JSON or text) report, so orchestrators parse the
//! payload and branch on the code. The other verbs exit 0 on success and
//! 1 on any refusal (the refusal code rides in the error message).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rocky_core::config::{MaskEntry, PolicyCapability, PolicyEffect, PolicyPrincipal, RockyConfig};
use rocky_core::fulfill::{
    FulfillJournalRow, FulfillState, FulfillStateRecord, ProductApprovalRecord,
};
use rocky_core::policy::{self, ModelAttributes};
use rocky_core::product::commit::{
    RecoveryAction, ReopenOutcome, committed_manifest, recover_generation, reopen_for_drafting,
    run_phase_a, run_phase_b,
};
use rocky_core::product::lowering::{manifest_rel, sidecar_rel, sql_rel, state_dir_rel};
use rocky_core::product::manifest::{
    MANIFEST_FILENAME, Manifest, ManifestPhase, content_digest, verify_artifact_hashes,
};
use rocky_core::product::spec::{ParsedSpec, SpecRejected, SpecResult, parse_spec_file};
use rocky_core::state::StateStore;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::output::print_json;

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The spec file of a product, project-root-relative.
pub fn spec_rel(product_name: &str) -> String {
    format!("products/{product_name}.toml")
}

/// The immutable approval snapshot of one spec revision,
/// project-root-relative.
///
/// Digest-addressed by the HEX half of the digest only: the `sha256:`
/// prefix carries a colon, which Windows refuses in file names, and the
/// scheme is already pinned by the record's `spec_digest`.
pub fn approval_snapshot_rel(product_name: &str, spec_digest: &str) -> String {
    let hex = spec_digest.strip_prefix("sha256:").unwrap_or(spec_digest);
    format!("{}/approved-{hex}.toml", state_dir_rel(product_name))
}

/// Parse `products/<name>.toml` and pin the file to the product it names.
///
/// The verb takes a product NAME and derives the path, so a spec file
/// whose `product.name` disagrees with its own file name is refused: the
/// generated-artifact headers, the state directory, and the approval
/// records all key on the name, and a mismatch would split that identity.
pub(crate) fn load_spec(root: &Path, product_name: &str) -> SpecResult<ParsedSpec> {
    let rel = spec_rel(product_name);
    let parsed = parse_spec_file(&root.join(&rel))?;
    if parsed.product().name != product_name {
        return Err(SpecRejected::new(
            "product-name-mismatch",
            format!(
                "{rel} declares product.name = '{}' — the file must be named after its \
                 product (products/<name>.toml), or every identity surface (state dir, \
                 approvals, generated headers) splits",
                parsed.product().name
            ),
        ));
    }
    Ok(parsed)
}

/// The project root: the directory that holds the config file.
///
/// Every `rocky product` command reads `products/` under it, a run stamps
/// delivery deadlines from it, and `rocky validate` and `rocky serve`
/// (including `GET /api/v1/schedule`) read it too, so none of them disagree about which
/// products exist. A scheduled child run starts with no working directory of
/// its own, so the root must never be the working directory. A bare config
/// name (`rocky.toml`) has an empty parent and maps to `.`.
pub(crate) fn product_command_root(config_path: &Path) -> std::path::PathBuf {
    match config_path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => std::path::PathBuf::from("."),
    }
}

/// Every delivery deadline the project's products declare.
///
/// Reads `products/*.toml` under `root`. Best effort, because a run records
/// delivery timing as advice: a spec that does not parse, or a directory that
/// cannot be read, contributes no deadline and logs a warning. `rocky
/// validate` and `rocky product status` are where a broken spec is refused.
pub(crate) fn product_deadlines_in(root: &Path) -> Vec<rocky_core::delivery::ProductDeadline> {
    loadable_specs_in(root, "delivery deadline")
        .into_iter()
        .filter_map(|parsed| {
            parsed
                .product()
                .deliver_by()
                .map(|deliver_by| rocky_core::delivery::ProductDeadline {
                    product: parsed.product().name.clone(),
                    output_model: parsed.output_model().to_string(),
                    deliver_by,
                })
        })
        .collect()
}

/// One product's declared volume range, keyed for `rocky run`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProductVolume {
    /// The product that declares the range.
    pub product: String,
    /// The product's output model.
    pub output_model: String,
    /// The declared range and severity.
    pub volume: rocky_core::product::spec::VolumeSpec,
}

/// Every volume range the project's products declare.
///
/// Best effort, like [`product_deadlines_in`]: a spec that does not parse
/// contributes no range and logs a warning.
pub(crate) fn product_volumes_in(root: &Path) -> Vec<ProductVolume> {
    loadable_specs_in(root, "volume range")
        .into_iter()
        .filter_map(|parsed| {
            parsed
                .product()
                .output
                .volume
                .clone()
                .map(|volume| ProductVolume {
                    product: parsed.product().name.clone(),
                    output_model: parsed.output_model().to_string(),
                    volume,
                })
        })
        .collect()
}

/// Every `products/*.toml` under `root` that parses, in file-name order.
/// `what` names the declaration a skipped spec loses, for the warning.
fn loadable_specs_in(root: &Path, what: &str) -> Vec<ParsedSpec> {
    let dir = root.join("products");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Vec::new(),
        Err(err) => {
            tracing::warn!(
                error = %err,
                dir = %dir.display(),
                "cannot read the products directory; no product {what} is recorded"
            );
            return Vec::new();
        }
    };
    let mut stems: Vec<String> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("toml"))
        .filter_map(|path| {
            path.file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_string)
        })
        .collect();
    stems.sort();
    let mut specs = Vec::new();
    for stem in stems {
        match load_spec(root, &stem) {
            Ok(parsed) => specs.push(parsed),
            Err(reject) => tracing::warn!(
                product = %stem,
                "product spec does not load ({reject}); its {what} is not recorded"
            ),
        }
    }
    specs
}

// ---------------------------------------------------------------------------
// The posture verification (D5 checks 1 + 2 against the real evaluator)
// ---------------------------------------------------------------------------

/// The corrected `propose_only` posture from FF-DESIGN D5, ready to
/// paste. The authoring-lane allow is scoped to the one output model;
/// apply stays a human decision.
pub fn paste_block(output_model: &str) -> String {
    format!(
        r#"[policy]
version = 1
default_agent_effect = "require_review"

# Authoring lane: the agent may draft and propose WITHIN the product's scope…
[[policy.rules]]
principal = "agent"
capability = "propose"
scope = {{ models = ["{output_model}"] }}
effect = "allow"

# …but applying stays a human decision (explicit, though the default already covers it).
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = {{ models = ["{output_model}"] }}
effect = "require_review"
"#
    )
}

/// Verification verdict, ordered by severity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum VerifyStatus {
    /// The frozen posture holds and every check passed.
    Pass,
    /// A human must edit configuration (the paste block says how).
    NeedsInput,
    /// A check failed outright (e.g. agent apply resolves `allow`).
    Fail,
}

/// One verification outcome, before it is wrapped into the output.
#[derive(Debug, Clone)]
pub(crate) struct PostureResult {
    pub status: VerifyStatus,
    pub reason: String,
    pub paste_block: Option<String>,
    pub propose_effect: Option<PolicyEffect>,
    pub apply_effect: Option<PolicyEffect>,
}

impl PostureResult {
    fn needs_input(reason: String, block: &str) -> Self {
        Self {
            status: VerifyStatus::NeedsInput,
            reason,
            paste_block: Some(block.to_string()),
            propose_effect: None,
            apply_effect: None,
        }
    }
}

/// The model attributes the lowering is ABOUT to create — the synthetic
/// post-image the posture is evaluated against.
///
/// Evaluating the post-image is the point: a gate reading the pre-write
/// attributes can be evaded by the very change under review. Phase A
/// writes the sibling `.contract.toml`, so the post-image is contracted;
/// the sidecar merge stamps `tags.product` and the spec's
/// classifications.
///
/// `reachable_downstreams` is `None` — unknown, not zero. "A brand-new
/// model has no dependents" is an assumption, not a proof: an existing
/// model can already reference this name, and resumes over prior state
/// exist. The verifier never computes the dependency graph, so
/// reachability stays unproved and any `max_downstreams` ceiling fails
/// closed (`allow` degrades to `require_review`), exactly as the engine
/// degrades an uncomputable blast radius.
pub(crate) fn synthetic_post_image(parsed: &ParsedSpec) -> ModelAttributes {
    let product = parsed.product();
    ModelAttributes {
        name: parsed.output_model().to_string(),
        tags: [("product".to_string(), product.name.clone())]
            .into_iter()
            .collect(),
        classifications: product.output.classifications.values().cloned().collect(),
        layer: None,
        contracted: true,
        downstreams: 0,
        reachable_downstreams: None,
    }
}

/// True iff the scope is EXACTLY the frozen posture's: the literal
/// output-model name and nothing else. `models` is compared as a literal
/// list (model names carry no glob characters, so a broader glob such as
/// `"revenue_*"` or `"*"` can never equal the name), and every other
/// predicate must be absent — a rule that reaches further than this one
/// product is not the frozen posture, however it happens to resolve.
fn is_exact_product_scope(scope: &rocky_core::config::PolicyScope, output_model: &str) -> bool {
    !scope.any
        && scope.models == [output_model]
        && scope.tags.is_empty()
        && scope.classifications.is_empty()
        && scope.exclude_classifications.is_empty()
        && scope.contracted.is_none()
        && scope.layer.is_none()
        && scope.max_downstreams.is_none()
}

/// D5 checks 1 + 2, fail-closed, cold-start capable — against the REAL
/// evaluator ([`policy::evaluate`]), never a mirror.
///
/// The pass condition is EXACTLY the frozen `propose_only` posture — not
/// any policy that happens to resolve safely for this product:
///
/// 1. `default_agent_effect = "require_review"` (the safe default).
/// 2. Agent propose resolves `allow` AND the winning rule is an explicit
///    allow scoped precisely to the output model (no `any`, no broader
///    glob, no attribute predicates) AND that rule carries no
///    `autonomy_budget`: the live ledger-aware engine degrades a
///    budgeted allow to `require_review` once the budget is exhausted,
///    so a static verification cannot prove the live posture of a
///    budgeted rule — the frozen D5 rule carries none.
/// 3. Agent apply resolves `require_review` or `deny`.
///
/// A globally-permissive policy whose resolution is coincidentally safe
/// (a permissive default with a scoped apply-review rule, or an
/// `any = true` propose-allow) is REJECTED: authority that reaches past
/// the product is global agent authority whether or not this product
/// feels it.
///
/// Check 1's existence test reads the parsed config directly, never
/// `rocky policy check`: with no `[policy]` block, ENFORCEMENT resolves
/// `NotConfigured` and allows agents everything, while `policy check`
/// *predicts* the safe default — the prediction is stricter than the
/// enforcement, so only the block's existence is trustworthy.
///
/// The Python prototype's check 3 (engine confirmation by subprocess)
/// dissolves here: this IS the engine, evaluating with its own
/// `policy::evaluate`. There is no second implementation to disagree
/// with.
pub(crate) fn verify_policy_posture(config_path: &Path, parsed: &ParsedSpec) -> PostureResult {
    let output_model = parsed.output_model();
    let block = paste_block(output_model);

    // The loader IS the presence probe. An `is_file()` gate in front of it
    // reported "rocky.toml not found" for a config that was plainly there:
    // `is_file()` follows a symlink, so a dangling `rocky.toml` answered
    // false (#1729). The direction was always safe — this returns
    // `needs_input` either way — but the reason it printed was untrue, and an
    // operator cannot fix a broken symlink they are told does not exist.
    let config = match rocky_core::config::load_rocky_config(config_path) {
        Ok(config) => config,
        // Nothing at the path. The config has to be created.
        Err(rocky_core::config::ConfigError::FileNotFound { .. }) => {
            return PostureResult::needs_input(
                format!(
                    "rocky.toml not found at {} — fix the config, then re-run",
                    config_path.display()
                ),
                &block,
            );
        }
        // An entry IS at the path and could not be read — a dangling symlink
        // most often. The message already names the path and the link target.
        Err(err @ rocky_core::config::ConfigError::UnreadableFile { .. }) => {
            return PostureResult::needs_input(
                format!("{err} — fix the config, then re-run"),
                &block,
            );
        }
        // The read itself failed for a reason that is not absence — a
        // directory at the path, a permission denial. Say so; it does not
        // reach the parser, so "does not parse" would be untrue.
        Err(rocky_core::config::ConfigError::ReadFile(io_error)) => {
            return PostureResult::needs_input(
                format!(
                    "rocky.toml at {} could not be read: {io_error} — fix the config, then \
                     re-run",
                    config_path.display()
                ),
                &block,
            );
        }
        Err(err) => {
            // A malformed `[policy]` hard-fails the engine's own config
            // load (serde deny_unknown_fields, unsigned integer types), so
            // nothing downstream may proceed on a guess. This is where the
            // prototype's strict-parsing mirror dissolves: the refusal IS
            // the engine's serde.
            return PostureResult::needs_input(
                format!(
                    "{} does not parse under the engine's config schema: {err:#} — fix the \
                     config, then re-run",
                    config_path.display()
                ),
                &block,
            );
        }
    };
    let Some(policy) = config.policy.clone() else {
        return PostureResult::needs_input(
            "rocky.toml has no [policy] block. With no block, ENFORCEMENT allows agents \
             everything (NotConfigured) even though `rocky policy check` predicts review — \
             so the block must exist before any agent runs. Paste the block below into \
             rocky.toml."
                .to_string(),
            &block,
        );
    };
    let problems: Vec<String> = rocky_core::config::validate_policy(&config)
        .into_iter()
        .map(|problem| problem.to_string())
        .collect();
    if !problems.is_empty() {
        return PostureResult::needs_input(
            format!("[policy] is present but invalid: {}", problems.join("; ")),
            &block,
        );
    }

    let attrs = synthetic_post_image(parsed);
    let propose = policy::evaluate(
        &policy,
        PolicyPrincipal::Agent,
        PolicyCapability::Propose,
        &attrs,
    );
    let apply = policy::evaluate(
        &policy,
        PolicyPrincipal::Agent,
        PolicyCapability::Apply,
        &attrs,
    );

    if policy.default_agent_effect != PolicyEffect::RequireReview {
        return PostureResult {
            status: VerifyStatus::NeedsInput,
            reason: format!(
                "default_agent_effect = '{:?}' is not the frozen propose_only posture \
                 (require_review). A permissive default is global agent authority even when \
                 this product's rules happen to resolve safely — no broader posture is \
                 accepted. Use the block below.",
                policy.default_agent_effect
            ),
            paste_block: Some(block),
            propose_effect: Some(propose.effect),
            apply_effect: Some(apply.effect),
        };
    }
    if propose.effect != PolicyEffect::Allow {
        return PostureResult {
            status: VerifyStatus::NeedsInput,
            reason: format!(
                "agent propose for '{output_model}' resolves to {:?} ({}). Drafting would \
                 stall: the draft tools are Propose-gated and return policy_review_required \
                 STOP instructions under require_review. Add the scoped authoring-lane allow \
                 below.",
                propose.effect, propose.reason
            ),
            paste_block: Some(block),
            propose_effect: Some(propose.effect),
            apply_effect: Some(apply.effect),
        };
    }
    let winning_rule = propose.matched_rule.and_then(|idx| policy.rules.get(idx));
    let exact = winning_rule.is_some_and(|rule| is_exact_product_scope(&rule.scope, output_model));
    if !exact {
        let scope_text = match (propose.matched_rule, winning_rule) {
            (Some(idx), Some(rule)) => format!(
                "rule {idx} with scope {}",
                serde_json::to_string(&rule.scope).unwrap_or_else(|_| "<unprintable>".to_string())
            ),
            _ => "the default posture".to_string(),
        };
        return PostureResult {
            status: VerifyStatus::NeedsInput,
            reason: format!(
                "agent propose for '{output_model}' resolves to allow via {scope_text}, \
                 which is broader than the frozen posture. The authoring-lane allow must be \
                 scoped to exactly this product's output model (models = ['{output_model}'] \
                 — no any, no glob, no attribute predicates); authority that reaches past \
                 the product is rejected. Use the block below."
            ),
            paste_block: Some(block),
            propose_effect: Some(propose.effect),
            apply_effect: Some(apply.effect),
        };
    }
    if let Some(budget) = winning_rule.and_then(|rule| rule.autonomy_budget.as_ref()) {
        return PostureResult {
            status: VerifyStatus::NeedsInput,
            reason: format!(
                "agent propose for '{output_model}' resolves to allow via rule {}, but that \
                 rule carries autonomy_budget = {{ failures = {}, window = '{}' }}. A \
                 budgeted allow is not the frozen posture: the live engine degrades it to \
                 require_review once the budget is exhausted, so this static verification \
                 cannot prove the posture that will actually run. Remove the budget and use \
                 the block below.",
                propose
                    .matched_rule
                    .map(|idx| idx.to_string())
                    .unwrap_or_default(),
                budget.failures,
                budget.window,
            ),
            paste_block: Some(block),
            propose_effect: Some(propose.effect),
            apply_effect: Some(apply.effect),
        };
    }
    if apply.effect == PolicyEffect::Allow {
        let rule_text = apply
            .matched_rule
            .map(|idx| format!("rule {idx}"))
            .unwrap_or_else(|| "the default posture".to_string());
        return PostureResult {
            status: VerifyStatus::Fail,
            reason: format!(
                "agent APPLY for '{output_model}' resolves to allow via {rule_text} ({}). \
                 propose_only requires apply to stay a human decision — remove or narrow \
                 that rule.",
                apply.reason
            ),
            paste_block: None,
            propose_effect: Some(propose.effect),
            apply_effect: Some(apply.effect),
        };
    }
    PostureResult {
        status: VerifyStatus::Pass,
        reason: format!(
            "propose → allow ({}); apply → {:?} ({})",
            propose.reason, apply.effect, apply.reason
        ),
        paste_block: None,
        propose_effect: Some(propose.effect),
        apply_effect: Some(apply.effect),
    }
}

// ---------------------------------------------------------------------------
// Classification-tag resolution (spec-compile error where W004 only warns)
// ---------------------------------------------------------------------------

/// Tags with a masking strategy or an explicit unmasked allowance.
///
/// Mirrors W004's completeness rule: a tag resolves if it appears as a
/// top-level `[mask]` strategy, as a key inside ANY `[mask.<env>]`
/// override table (not gated on the active env), or in
/// `[classifications].allow_unmasked`.
fn resolved_classification_tags(config: &RockyConfig) -> BTreeSet<String> {
    let mut resolved: BTreeSet<String> = BTreeSet::new();
    for (key, entry) in &config.mask {
        match entry {
            MaskEntry::Strategy(_) => {
                resolved.insert(key.clone());
            }
            MaskEntry::EnvOverride(overrides) => {
                resolved.extend(overrides.keys().cloned());
            }
        }
    }
    resolved.extend(config.classifications.allow_unmasked.iter().cloned());
    resolved
}

/// REJECT the spec unless every classification tag resolves.
///
/// The engine only warns (W004); the product verbs make an unresolved
/// tag a spec-compile error. Stated plainly: this closes tag RESOLUTION
/// only — masking APPLICATION is warehouse-dependent (Databricks-only
/// today), so a resolved tag is a declared intent, not an enforcement
/// guarantee.
pub(crate) fn check_classifications(config: &RockyConfig, parsed: &ParsedSpec) -> SpecResult<()> {
    let declared: BTreeSet<&String> = parsed.product().output.classifications.values().collect();
    if declared.is_empty() {
        return Ok(());
    }
    let resolved = resolved_classification_tags(config);
    let unresolved: Vec<&str> = declared
        .iter()
        .filter(|tag| !resolved.contains(tag.as_str()))
        .map(|tag| tag.as_str())
        .collect();
    if unresolved.is_empty() {
        return Ok(());
    }
    Err(SpecRejected::new(
        "classification-unresolved",
        format!(
            "classification tag(s) {} resolve to no [mask] / [mask.<env>] strategy and are \
             not in [classifications].allow_unmasked. Add a strategy or the explicit \
             allowance. (Resolution only: masking application is warehouse-dependent — \
             Databricks-only today.)",
            unresolved.join(", ")
        ),
    ))
}

// ---------------------------------------------------------------------------
// Collision checks against other products' state dirs
// ---------------------------------------------------------------------------

/// REJECT on identity collisions with existing fulfillment state.
///
/// - Duplicate product name: this product's state dir already carries a
///   committed manifest recorded from a DIFFERENT spec file.
/// - Duplicate output model: ANOTHER product's committed manifest
///   already claims the same `output.model`.
pub(crate) fn check_product_collisions(
    project_root: &Path,
    parsed: &ParsedSpec,
    spec_path: &str,
) -> SpecResult<()> {
    let product_name = &parsed.product().name;
    if let Some(own) = committed_manifest(project_root, product_name)?
        && own.spec_path != spec_path
    {
        return Err(SpecRejected::new(
            "duplicate-product-name",
            format!(
                "product name '{product_name}' is already claimed by the spec at '{}' \
                 (state dir {}); two spec files must not share a name",
                own.spec_path,
                state_dir_rel(product_name)
            ),
        ));
    }
    // A scan that cannot LIST the other products cannot clear this one. Every
    // fold below used to answer "no collision" for a reading nobody took:
    // `read_dir` failing for any reason returned `Ok(())`; `.flatten()`
    // dropped an entry that errored; `is_dir()` dropped a state dir that
    // could not be stat-ed; `is_file()` dropped a manifest that could not be.
    // `rocky product verify` then reported PASS and compile proceeded with
    // duplicate output ownership (#1817). Only a PROVEN absence — no
    // fulfillment dir yet, no manifest in a peer — is a clean answer.
    let fulfillment_root = project_root.join(".rocky").join("fulfillment");
    let unreadable = |path: &Path, why: &str| {
        SpecRejected::new(
            "fulfillment-dir-unreadable",
            format!(
                "{} cannot be read: {why}. The collision check cannot see the other \
                 products, so it cannot clear this one.",
                path.display()
            ),
        )
    };
    let entries = match std::fs::read_dir(&fulfillment_root) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            match rocky_core::path_presence::classify_not_found(&fulfillment_root) {
                rocky_core::path_presence::PathPresence::Absent => return Ok(()),
                rocky_core::path_presence::PathPresence::Present { detail } => {
                    return Err(unreadable(&fulfillment_root, &detail));
                }
            }
        }
        Err(err) => return Err(unreadable(&fulfillment_root, &err.to_string())),
    };
    let mut state_dirs: Vec<PathBuf> = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|err| unreadable(&fulfillment_root, &err.to_string()))?;
        let path = entry.path();
        // A state dir may legitimately be a symlink (an operator relocating
        // storage), so the directory test follows it — but a link that does
        // not resolve, or an entry that cannot be stat-ed, is not "not a
        // directory": it is a peer this scan cannot see. One exception, and
        // it is a PROVEN one: an unrelated entry (`.DS_Store`, an editor swap
        // file) that vanished between the listing and the stat. Nothing is
        // there now, so it can conceal nothing, and refusing on it would fail
        // `rocky product verify` on a race the scan has no stake in.
        let is_dir = match std::fs::metadata(&path) {
            Ok(metadata) => metadata.is_dir(),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match rocky_core::path_presence::classify_not_found(&path) {
                    rocky_core::path_presence::PathPresence::Absent => continue,
                    rocky_core::path_presence::PathPresence::Present { detail } => {
                        return Err(unreadable(&path, &detail));
                    }
                }
            }
            Err(err) => return Err(unreadable(&path, &err.to_string())),
        };
        if is_dir {
            state_dirs.push(path);
        }
    }
    state_dirs.sort();
    for state_dir in state_dirs {
        let dir_name = state_dir
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        if dir_name == *product_name {
            continue;
        }
        let other_manifest_path = state_dir.join(MANIFEST_FILENAME);
        let manifest_unreadable = |why: &str| {
            SpecRejected::new(
                "manifest-unreadable",
                format!("{} is unreadable: {why}", other_manifest_path.display()),
            )
        };
        let raw = match std::fs::read(&other_manifest_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                match rocky_core::path_presence::classify_not_found(&other_manifest_path) {
                    rocky_core::path_presence::PathPresence::Absent => continue,
                    rocky_core::path_presence::PathPresence::Present { detail } => {
                        return Err(manifest_unreadable(&detail));
                    }
                }
            }
            Err(err) => return Err(manifest_unreadable(&err.to_string())),
        };
        let other = Manifest::from_json_bytes(&raw)?;
        if other.output_model == parsed.output_model() {
            return Err(SpecRejected::new(
                "duplicate-output-model",
                format!(
                    "output model '{}' is already claimed by product '{dir_name}' ({}); one \
                     model has one owning product",
                    parsed.output_model(),
                    other.product_id
                ),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Output shapes
// ---------------------------------------------------------------------------

/// JSON output of `rocky product verify`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductVerifyOutput {
    pub version: String,
    pub command: String,
    /// `product:<name>`.
    pub product_id: String,
    /// `sha256:<hex>` over the spec's raw bytes.
    pub spec_digest: String,
    /// The resolved output model name.
    pub output_model: String,
    /// `pass`, `needs_input`, or `fail` — also the exit code (0 / 1 / 2).
    pub status: VerifyStatus,
    /// Why, in plain language.
    pub reason: String,
    /// The corrected `[policy]` block to paste, on a posture
    /// `needs_input`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paste_block: Option<String>,
    /// The resolved agent-propose effect, when evaluation ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub propose_effect: Option<PolicyEffect>,
    /// The resolved agent-apply effect, when evaluation ran.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub apply_effect: Option<PolicyEffect>,
}

/// One emitted artifact, as reported by `rocky product compile`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductArtifactOutput {
    /// Project-root-relative POSIX path.
    pub path: String,
    /// `sha256:<hex>` over the committed bytes.
    pub sha256: String,
}

/// The approval record, as echoed by compile/status output.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductApprovalOutput {
    /// The approved `sha256:<hex>` spec digest.
    pub spec_digest: String,
    /// Best-effort approver identity.
    pub approver: String,
    /// RFC3339 approval instant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approved_at: Option<String>,
    /// Project-root-relative path of the immutable snapshot file.
    pub snapshot_path: String,
}

/// JSON output of `rocky product compile`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductCompileOutput {
    pub version: String,
    pub command: String,
    /// `product:<name>`.
    pub product_id: String,
    /// `sha256:<hex>` over the compiled spec's raw bytes.
    pub spec_digest: String,
    /// Project-root-relative spec path.
    pub spec_path: String,
    /// The resolved output model name.
    pub output_model: String,
    /// Which lowering phase this run committed: `lowered_contract`
    /// (Phase A) or `merged` (Phase B).
    pub phase: String,
    /// The artifacts this phase committed (manifest excluded).
    pub artifacts: Vec<ProductArtifactOutput>,
    /// Project-root-relative path of the committed lowering manifest.
    pub manifest_path: String,
    /// The current approval record, when one exists. Compile is a READER
    /// of the approval: when present, the snapshot bytes were re-verified
    /// against the record digest before this output was produced.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ProductApprovalOutput>,
    /// Whether the compiled spec's digest equals the approved digest.
    /// `null` when no approval exists. `false` is not an error at
    /// compile time — it means the working spec has moved past the
    /// approval (the loop's supersession trigger).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_matches_approval: Option<bool>,
}

/// JSON output of `rocky product approve`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductApproveOutput {
    pub version: String,
    pub command: String,
    /// `product:<name>`.
    pub product_id: String,
    /// The approved `sha256:<hex>` spec digest.
    pub spec_digest: String,
    /// The resolved output model name.
    pub output_model: String,
    /// Best-effort approver identity recorded on the approval.
    pub approver: String,
    /// RFC3339 approval instant.
    pub approved_at: String,
    /// Project-root-relative path of the immutable snapshot file.
    pub snapshot_path: String,
    /// The fulfillment state tag before this approval, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_state: Option<String>,
    /// The fulfillment state after this approval (`spec_approved`).
    pub state: String,
    /// True when this exact digest was already approved and nothing was
    /// re-written.
    pub already_approved: bool,
}

/// JSON output of `rocky product status`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductStatusOutput {
    pub version: String,
    pub command: String,
    /// The product name the status was asked for.
    pub product: String,
    /// Whether `products/<name>.toml` exists and parses.
    pub spec_present: bool,
    /// `product:<name>`, when the spec parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_id: Option<String>,
    /// The working spec's digest, when it parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_digest: Option<String>,
    /// The resolved output model, when the spec parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_model: Option<String>,
    /// Why the spec failed to parse, when it did not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_error: Option<String>,
    /// The committed lowering phase, when a manifest exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_phase: Option<String>,
    /// The committed manifest's spec digest, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_spec_digest: Option<String>,
    /// Byte-verification problems against the committed manifest
    /// (empty = every committed artifact matches its recorded hash).
    #[serde(default)]
    pub artifact_problems: Vec<String>,
    /// Whether an uncommitted staging journal is pending. Status never
    /// mutates; the next compile resolves it.
    pub staging_journal_present: bool,
    /// The approval record, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ProductApprovalOutput>,
    /// Whether the approval snapshot's bytes still digest to the
    /// recorded value. `null` without an approval; `false` is tamper.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_intact: Option<bool>,
    /// Whether the working spec equals the approved revision. `null`
    /// when either side is missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_matches_approval: Option<bool>,
    /// The persisted fulfillment state tag, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fulfill_state: Option<String>,
    /// Number of fulfillment journal rows recorded for this product.
    pub journal_rows: u64,
    /// Delivery state, when the spec declares `deliver_by`: whether the
    /// latest production delivery was on time, how late, and whether the
    /// next delivery is at risk. The estimate times run start to the output
    /// model's finish.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery: Option<crate::output::DeliveryStatus>,
    /// Volume state, when the spec declares `[product.output.volume]`: the
    /// declared range and the latest production count judged against it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume: Option<ProductVolumeStatus>,
}

/// The declared volume range and the latest production count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProductVolumeStatus {
    /// The declared `min_rows` (inclusive), when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_rows: Option<u64>,
    /// The declared `max_rows` (inclusive), when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    /// The declared severity: `warning` (the default) or `error`.
    pub severity: rocky_core::product::spec::VolumeSeverity,
    /// The newest production build that recorded a count. Absent when no
    /// build has been counted yet.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count: Option<u64>,
    /// When that build finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub counted_at: Option<chrono::DateTime<chrono::Utc>>,
    /// The run that took the count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// That count judged against the range the spec declares now:
    /// `in_range`, `below_min`, `above_max`, or `not_evaluated` when no
    /// count exists.
    pub verdict: rocky_core::product::spec::VolumeVerdict,
}

/// One product's row in `rocky product list`.
///
/// A projection of [`ProductStatusOutput`], built by the same function
/// `product status` uses, so the two can never disagree. Missing values are
/// `null`, never invented.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductListEntry {
    /// Product name: the spec file's stem, or the state store's key.
    pub name: String,
    /// Whether `products/<name>.toml` exists and parses.
    pub spec_present: bool,
    /// `product:<name>`, when the spec parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_id: Option<String>,
    /// The resolved output model, when the spec parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_model: Option<String>,
    /// The working spec's digest, when it parses.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_digest: Option<String>,
    /// Why the spec failed to load — absent or unparseable — when it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_error: Option<String>,
    /// The committed lowering phase, when a manifest exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub committed_phase: Option<String>,
    /// Number of committed artifacts whose bytes no longer match the
    /// manifest. `0` is clean.
    pub artifact_problems: usize,
    /// Whether an uncommitted staging journal is pending.
    pub staging_journal_present: bool,
    /// The approval record, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub approval: Option<ProductApprovalOutput>,
    /// Whether the working spec equals the approved revision. `null`
    /// when either side is missing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_matches_approval: Option<bool>,
    /// The persisted fulfillment state tag, when one exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fulfill_state: Option<String>,
    /// Number of fulfillment journal rows recorded for this product.
    pub journal_rows: u64,
}

impl ProductListEntry {
    fn from_status(status: ProductStatusOutput) -> Self {
        Self {
            name: status.product,
            spec_present: status.spec_present,
            product_id: status.product_id,
            output_model: status.output_model,
            spec_digest: status.spec_digest,
            spec_error: status.spec_error,
            committed_phase: status.committed_phase,
            artifact_problems: status.artifact_problems.len(),
            staging_journal_present: status.staging_journal_present,
            approval: status.approval,
            spec_matches_approval: status.spec_matches_approval,
            fulfill_state: status.fulfill_state,
            journal_rows: status.journal_rows,
        }
    }
}

/// JSON output of `rocky product list`.
///
/// One row per product the project knows, sorted by name. A project with
/// no `products/` directory and no records lists nothing; that is not an
/// error.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductListOutput {
    pub version: String,
    pub command: String,
    /// Every product, sorted by name.
    pub products: Vec<ProductListEntry>,
    /// `products.len()`, repeated so a consumer can assert it received the
    /// whole list.
    pub count: usize,
}

/// One persisted fulfillment journal row, as `rocky product journal`
/// prints it: the loop's own record of a transition, field for field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ProductJournalEntry {
    /// Sequence number, also encoded in the row's store key.
    pub seq: u64,
    /// RFC 3339 instant of the transition, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// What happened, in the loop's plain language (`spec approved`, …).
    /// A label to render, not an enum to switch on.
    pub event: String,
    /// The state tag before the transition, when one existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub from_state: Option<String>,
    /// The state tag after the transition.
    pub to_state: String,
    /// The spec digest involved, when the event concerns one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_digest: Option<String>,
    /// The plan involved, when the event concerns one (propose, apply).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_id: Option<String>,
    /// The idempotency key pinned by this event, when it pinned one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
}

/// JSON output of `rocky product journal <name>`.
///
/// The product's fulfillment journal, whole and in append order, read
/// through the same store function the fulfillment loop reads through. A
/// known product with no rows is an empty journal; a product the project
/// does not know (no spec file, no store record) is a refusal, never an
/// empty journal.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ProductJournalOutput {
    pub version: String,
    pub command: String,
    /// The product name as given.
    pub product: String,
    /// `product:<name>`, the store key every record of the product hangs off.
    pub product_id: String,
    /// Every journal row, in append order.
    pub rows: Vec<ProductJournalEntry>,
    /// `rows.len()`, repeated so a consumer can assert it received the
    /// whole journal.
    pub count: usize,
}

// ---------------------------------------------------------------------------
// verify
// ---------------------------------------------------------------------------

/// Run every verification: the D5 posture, classification resolution,
/// and identity collisions — in that order, first refusal wins after the
/// posture (the posture result carries its own status).
pub(crate) fn product_verify_outcome(
    root: &Path,
    config_path: &Path,
    product_name: &str,
) -> Result<(ParsedSpec, PostureResult)> {
    let parsed = load_spec(root, product_name).map_err(|reject| anyhow::anyhow!("{reject}"))?;
    let posture = verify_policy_posture(config_path, &parsed);
    if posture.status != VerifyStatus::Pass {
        return Ok((parsed, posture));
    }
    // The posture passed, so the config loads — a failure here would have
    // been a needs_input above.
    let config = rocky_core::config::load_rocky_config(config_path)
        .context("the posture verification passed but the config no longer loads")?;
    if let Err(reject) = check_classifications(&config, &parsed) {
        let posture = PostureResult {
            status: VerifyStatus::Fail,
            reason: format!("[{}] {}", reject.code, reject.message),
            paste_block: None,
            propose_effect: posture.propose_effect,
            apply_effect: posture.apply_effect,
        };
        return Ok((parsed, posture));
    }
    if let Err(reject) = check_product_collisions(root, &parsed, &spec_rel(product_name))
        .and_then(|()| check_volume_strategy(root, &parsed))
    {
        let posture = PostureResult {
            status: VerifyStatus::Fail,
            reason: format!("[{}] {}", reject.code, reject.message),
            paste_block: None,
            propose_effect: posture.propose_effect,
            apply_effect: posture.apply_effect,
        };
        return Ok((parsed, posture));
    }
    Ok((parsed, posture))
}

/// Refuse `[product.output.volume]` on an output model that is not a
/// full-refresh table (`volume-strategy-unsupported`).
///
/// The range is the table's total rows after a load. On an incremental,
/// merge or partitioned table the total only grows, so the range would
/// break by itself; a view or ephemeral model stores no rows.
///
/// The check lives here, not in the spec parser or the lowering, because
/// neither can know the strategy: the spec does not name it, and the
/// sidecar the lowering merges into often omits it (it resolves from
/// `_defaults.toml` or a config group). This loads the model the way the
/// compiler does. Before the worker drafts the model there is nothing to
/// check, so the check passes; the Phase-B compile runs this verification
/// again once the model exists. `rocky run` also refuses to count a
/// non-full-refresh output (`not_evaluated`), so a strategy changed after
/// verification never yields a false verdict.
///
/// A model that exists but does not load passes here: the compile reports
/// that problem with its own diagnostics.
pub(crate) fn check_volume_strategy(root: &Path, parsed: &ParsedSpec) -> SpecResult<()> {
    if parsed.product().output.volume.is_none() {
        return Ok(());
    }
    let model_name = parsed.output_model();
    // The model's source is a `.sql` file or a `.rocky` DSL file. The
    // compiler's directory loader reads both; `rocky_core`'s reads only
    // `.sql`, so a `.rocky` output would never be judged.
    let sql = root.join(sql_rel(parsed));
    let has_source = sql.is_file() || sql.with_extension("rocky").is_file();
    if !has_source || !root.join(sidecar_rel(parsed)).is_file() {
        return Ok(());
    }
    let models_dir = root.join("models");
    let only_this_model = format!(
        "{}/{}.*",
        glob::Pattern::escape(&models_dir.to_string_lossy()),
        glob::Pattern::escape(model_name)
    );
    let models = match rocky_compiler::project::load_dir_models_matching(
        &models_dir,
        &only_this_model,
        None,
    ) {
        Ok(models) => models,
        Err(err) => {
            tracing::debug!(
                model = model_name,
                error = %err,
                "the output model does not load; the volume strategy check is left to the compile"
            );
            return Ok(());
        }
    };
    let Some(model) = models.iter().find(|model| model.config.name == model_name) else {
        return Ok(());
    };
    if super::run_volume::counts_total_rows(&model.to_model_ir().materialization) {
        return Ok(());
    }
    Err(SpecRejected::new(
        "volume-strategy-unsupported",
        format!(
            "product.output.volume needs a full-refresh output, but model '{model_name}' \
             is not one: the range is the table's total rows after a load, which an \
             incremental, merge or partitioned table only grows, and a view or ephemeral \
             model does not store. Remove [product.output.volume], or make the model \
             `strategy = {{ type = \"full_refresh\" }}`"
        ),
    ))
}

pub(crate) fn verify_output_for(
    parsed: &ParsedSpec,
    posture: &PostureResult,
) -> ProductVerifyOutput {
    ProductVerifyOutput {
        version: VERSION.to_string(),
        command: "product_verify".to_string(),
        product_id: parsed.product_id(),
        spec_digest: parsed.digest.clone(),
        output_model: parsed.output_model().to_string(),
        status: posture.status,
        reason: posture.reason.clone(),
        paste_block: posture.paste_block.clone(),
        propose_effect: posture.propose_effect,
        apply_effect: posture.apply_effect,
    }
}

fn print_verify(output: &ProductVerifyOutput, output_json: bool) -> Result<()> {
    if output_json {
        print_json(output)?;
        return Ok(());
    }
    let label = match output.status {
        VerifyStatus::Pass => "PASS",
        VerifyStatus::NeedsInput => "NEEDS INPUT",
        VerifyStatus::Fail => "FAIL",
    };
    println!("product {} ({})", output.product_id, output.spec_digest);
    println!("verify: {label}");
    println!("  {}", output.reason);
    if let Some(block) = &output.paste_block {
        println!("\nPaste this into rocky.toml:\n\n{block}");
    }
    Ok(())
}

/// Execute `rocky product verify <name>`.
///
/// Exits 0 on `pass`, 1 on `needs_input`, 2 on `fail` — after printing
/// the full report.
pub fn run_product_verify(config_path: &Path, product_name: &str, output_json: bool) -> Result<()> {
    let root = product_command_root(config_path);
    let (parsed, posture) = product_verify_outcome(&root, config_path, product_name)?;
    let output = verify_output_for(&parsed, &posture);
    print_verify(&output, output_json)?;
    match posture.status {
        VerifyStatus::Pass => Ok(()),
        VerifyStatus::NeedsInput => std::process::exit(1),
        VerifyStatus::Fail => std::process::exit(2),
    }
}

// ---------------------------------------------------------------------------
// compile
// ---------------------------------------------------------------------------

/// Is this process the one whose stamp is on `record`?
///
/// The in-flight guard's whole basis (#1493). It is DERIVED from what
/// this process actually is — its pid paired with its own start time,
/// read from the OS — never asserted by a caller. An earlier version
/// took a caller-supplied "who is asking" argument; that let the public
/// façade hand the loop's exemption to anyone who called it, so the
/// exemption is gone and there is no argument left to abuse.
///
/// The start time is what makes this reuse-proof. A pid alone would be
/// wrong in the fail-open direction: a killed loop leaves its stamp
/// behind, and an unrelated process that later recycles that pid would
/// read as the owner. A stamp whose start time we cannot confirm is
/// NOT ours — the guard fails closed and refuses.
fn stamp_belongs_to_this_process(record: &FulfillStateRecord) -> bool {
    rocky_core::process::stamp_is_this_process(record.owner_pid, record.owner_start_time)
}

/// The inner compile: verify, then run the next lowering phase through
/// the staged commit protocol.
///
/// Phase selection: Phase A when no committed manifest exists (or when
/// one exists but the drafted sidecar does not — a Phase-A resume);
/// Phase B once the drafted sidecar exists. Both orchestrators recover a
/// crashed prior commit before staging, refuse cold-start collisions,
/// tampering, and cross-generation mixing, and commit with the manifest
/// rename as the marker.
pub(crate) fn product_compile_in(
    root: &Path,
    config_path: &Path,
    state_path: Option<&Path>,
    product_name: &str,
) -> Result<ProductCompileOutput> {
    let (parsed, posture) = product_verify_outcome(root, config_path, product_name)?;
    if posture.status != VerifyStatus::Pass {
        let block = posture
            .paste_block
            .as_deref()
            .map(|block| format!("\n\nPaste this into rocky.toml:\n\n{block}"))
            .unwrap_or_default();
        bail!(
            "refusing to compile product '{product_name}': verification did not pass \
             ({:?}): {}{block}",
            posture.status,
            posture.reason
        );
    }
    let spec_path = spec_rel(product_name);

    // Compile is a READER of the approval (when one exists): the snapshot
    // bytes must still digest to the recorded value, or the approval is
    // tampered and nothing proceeds.
    let state_store = open_store_if_present(state_path, open_state_store)?;
    let approval = match &state_store {
        Some(store) => store.product_approval_get(product_name)?,
        None => None,
    };
    // No state store means no fulfillment record can exist, so no loop
    // can own this product.
    let observed_state = match &state_store {
        Some(store) => store.fulfill_state_get(product_name)?,
        None => None,
    };
    let mut approval_echo = None;
    let mut spec_matches_approval = None;
    if let Some(record) = &approval {
        verify_approval_snapshot(root, record)?;
        spec_matches_approval = Some(record.spec_digest == parsed.digest);
        approval_echo = Some(approval_output(record));
    }

    // Compile picks its phase from what is on disk, so it must never run
    // concurrently with a fulfillment loop that owns this product. The
    // loop's drafting window is exactly the interval where the manifest
    // says Phase A while last round's sidecar is still on disk: a
    // `rocky product compile` landing there re-merges and re-pins that
    // stale sidecar, which makes the loop's own repair look like tamper
    // all over again (#1493). Refuse while any in-flight stamp is
    // present — the same shape `product_approve_in` uses below.
    //
    // The loop's OWN compile passes, because the stamp it reads is the
    // one it wrote: same pid AND same process start time. Nothing here
    // is supplied by the caller, so no entry point can hand out the
    // loop's identity.
    if let Some(record) = &observed_state
        && !stamp_belongs_to_this_process(record)
        && (record.owner_pid.is_some() || record.driver_pgid.is_some())
    {
        let who = match (record.owner_pid, record.driver_pgid) {
            (Some(pid), _) => format!("a fulfillment loop (pid {pid}) owns it"),
            (None, Some(pgid)) => {
                format!("a worker process group (pgid {pgid}) is recorded as live")
            }
            (None, None) => unreachable!("guarded by the condition above"),
        };
        bail!(
            "[compile-refused-in-flight] refusing to compile product '{product_name}' \
             while fulfillment work is in flight ({who}). Compiling now could re-merge \
             and re-pin artifacts the loop is mid-round on. Stop the running \
             `rocky fulfill` loop (or let it reach its next stop — needs_input, blocked, \
             or observing), then re-run; nothing was written. If that loop is already \
             gone, `rocky fulfill {product_name}` sweeps the dead owner by its start \
             time and takes the record over."
        );
    }

    let sidecar_present = root.join(sidecar_rel(&parsed)).is_file();
    let committed = committed_manifest(root, product_name)?;
    // Phase B (the metadata merge) only continues the CURRENT
    // generation: a committed manifest from a DIFFERENT spec digest
    // means the spec moved past this lowering (FF-DESIGN D6
    // supersession), and `run_phase_b` would refuse to mix generations —
    // so the next generation starts at Phase A instead, re-rendering the
    // contract from the current spec (the resume arm of Phase A's
    // cold-start rule permits it: the committed manifest claims the
    // model).
    let continues_generation = committed
        .as_ref()
        .is_some_and(|manifest| manifest.spec_digest == parsed.digest);
    let lowering = if committed.is_some() && sidecar_present && continues_generation {
        run_phase_b(root, &spec_path, &parsed)
    } else {
        run_phase_a(root, &spec_path, &parsed)
    }
    .map_err(|reject| anyhow::anyhow!("{reject}"))?;

    Ok(ProductCompileOutput {
        version: VERSION.to_string(),
        command: "product_compile".to_string(),
        product_id: parsed.product_id(),
        spec_digest: parsed.digest.clone(),
        spec_path,
        output_model: parsed.output_model().to_string(),
        phase: match lowering.manifest.phase {
            ManifestPhase::LoweredContract => "lowered_contract".to_string(),
            ManifestPhase::Merged => "merged".to_string(),
        },
        artifacts: lowering
            .artifacts
            .iter()
            .map(|artifact| ProductArtifactOutput {
                path: artifact.relpath.clone(),
                sha256: content_digest(&artifact.content),
            })
            .collect(),
        manifest_path: manifest_rel(product_name),
        approval: approval_echo,
        spec_matches_approval,
    })
}

/// Execute `rocky product compile <name>`.
pub fn run_product_compile(
    config_path: &Path,
    product_name: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let root = product_command_root(config_path);
    let output = product_compile_in(&root, config_path, Some(state_path), product_name)?;
    if output_json {
        print_json(&output)?;
    } else {
        println!(
            "product {} compiled: phase {} (spec {})",
            output.product_id, output.phase, output.spec_digest
        );
        for artifact in &output.artifacts {
            println!("  wrote {} ({})", artifact.path, artifact.sha256);
        }
        println!("  manifest {}", output.manifest_path);
        if let Some(false) = output.spec_matches_approval {
            println!(
                "  note: the working spec differs from the approved revision — re-approve \
                 before the loop proposes"
            );
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// approve
// ---------------------------------------------------------------------------

/// Best-effort approver identity — the same convention the review marker
/// uses (git identity when available), with the same honesty caveat: it
/// is an attribution, not an authentication.
///
/// The channel is read the way `rocky review --approve` reads it, from
/// `ROCKY_SESSION_SOURCE`. A terminal approval records the bare email, as it
/// always did. An approval made through the HTTP API (a job the server
/// started; the browser UI uses it) records `"<email> (http_api)"`, and
/// refuses when the server's git identity cannot be read or is the
/// placeholder `unknown`, because the record would then name nobody. The
/// label names the channel, not the person: the identity is the server's git
/// identity, and nothing proves a browser made the call.
fn approver_string() -> Result<String> {
    Ok(approver_label(super::review::review_approver(
        super::run_audit::detect_session_source(),
        super::branch::approver_identity_pub(),
    )?))
}

/// The recorded approver string for a resolved identity: the bare email for a
/// local approval, `"<email> (<channel>)"` for every other channel.
fn approver_label(identity: crate::output::ApproverIdentity) -> String {
    use crate::output::ApproverSource;
    let channel = match identity.source {
        ApproverSource::Local => return identity.email,
        ApproverSource::HttpApi => "http_api",
        ApproverSource::CiOidc => "ci_oidc",
        ApproverSource::Pat => "pat",
    };
    format!("{} ({channel})", identity.email)
}

/// Write the immutable digest-addressed snapshot file, tmp+rename.
///
/// A digest-named file is never overwritten — no reader can observe
/// new-digest-old-bytes. When the file already exists its bytes must
/// still digest to the name it carries; anything else is tamper and the
/// approval refuses.
pub(crate) fn write_approval_snapshot(root: &Path, parsed: &ParsedSpec) -> Result<String> {
    let rel = approval_snapshot_rel(&parsed.product().name, &parsed.digest);
    let path = root.join(&rel);
    // Prove the snapshot target is inside the project through the SAME
    // containment primitive the commit protocol uses. A leaf-only symlink
    // check misses a symlinked ANCESTOR: a static `.rocky/fulfillment/<name>`
    // (or `.rocky`) symlink out of the project would redirect the temp write,
    // its `remove_file`, and its O_EXCL create — an out-of-project write with
    // no race. This refuses that ancestor case AND a symlink or directory at
    // the digest-addressed final itself (which the read below would follow).
    rocky_core::product::commit::contained_write_target(root, &rel)
        .map_err(|reason| anyhow::anyhow!("[approval-snapshot-tampered] {reason}"))?;
    if path.is_file() {
        let existing = std::fs::read(&path)
            .with_context(|| format!("failed to read existing snapshot {}", path.display()))?;
        if content_digest(&existing) != parsed.digest {
            bail!(
                "[approval-snapshot-tampered] {rel} exists but its bytes do not digest to \
                 {} — a digest-addressed snapshot is immutable; refusing to overwrite or \
                 approve over it",
                parsed.digest
            );
        }
        return Ok(rel);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    // Stage the snapshot bytes into a scratch temp. A plain `std::fs::write`
    // FOLLOWS a symlink planted at the temp path (a malicious spec repo or a
    // lower-privilege process could park one at `approved-<hex>.toml.tmp`
    // pointing out of the project), writing the spec bytes through it. This
    // refuses a symlinked temp as tamper and creates the file with O_EXCL so
    // a link swapped in during the race is refused too, never followed.
    let tmp = path.with_extension("toml.tmp");
    stage_snapshot_temp(&tmp, &rel, &parsed.raw)?;
    std::fs::rename(&tmp, &path)
        .with_context(|| format!("failed to commit snapshot at {}", path.display()))?;
    Ok(rel)
}

/// True when `path` is a symlink itself (never following it).
fn is_symlink(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_symlink())
}

/// Write the snapshot bytes to a brand-new scratch temp, refusing a symlink.
///
/// A plain `std::fs::write` follows a symlink at `tmp`; this does not.
/// `create_new` (O_CREAT|O_EXCL) refuses to follow a link at the final
/// component and refuses to clobber, so a link an attacker plants — even
/// one swapped in after the pre-check, during the race — is refused rather
/// than written through. The only legitimate `AlreadyExists` is our own
/// stale scratch from a crashed approve: a regular file is removed
/// (`unlink` never follows a link) and the O_EXCL create retried once; a
/// symlink is refused as tamper.
fn stage_snapshot_temp(tmp: &Path, rel: &str, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let tamper = || {
        anyhow::anyhow!(
            "[approval-snapshot-tampered] the snapshot staging temp for {rel} is a symlink — \
             refusing to write the approved bytes through it"
        )
    };
    if is_symlink(tmp) {
        bail!(tamper());
    }
    let open = || {
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(tmp)
    };
    let mut file = match open() {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
            // A link swapped in during the race is caught by O_EXCL (it
            // returned AlreadyExists rather than following) — refuse it.
            if is_symlink(tmp) {
                bail!(tamper());
            }
            // Our own stale regular-file scratch: unlink (never follows a
            // link) and retry once. A second AlreadyExists means a racer
            // re-planted — refuse rather than loop.
            std::fs::remove_file(tmp).with_context(|| {
                format!("failed to clear stale snapshot temp {}", tmp.display())
            })?;
            open().map_err(|err| {
                if err.kind() == std::io::ErrorKind::AlreadyExists {
                    tamper()
                } else {
                    anyhow::Error::from(err)
                        .context(format!("failed to stage snapshot at {}", tmp.display()))
                }
            })?
        }
        Err(err) => {
            return Err(anyhow::Error::from(err)
                .context(format!("failed to stage snapshot at {}", tmp.display())));
        }
    };
    file.write_all(bytes)
        .with_context(|| format!("failed to stage snapshot at {}", tmp.display()))
}

/// Verify an approval record's snapshot bytes against its digest.
fn verify_approval_snapshot(root: &Path, record: &ProductApprovalRecord) -> Result<()> {
    let path = root.join(&record.snapshot_path);
    let bytes = std::fs::read(&path).with_context(|| {
        format!(
            "[approval-snapshot-missing] the approval record points at {} but the file is \
             unreadable — the record proves an approval happened, never what the bytes now \
             say, so nothing proceeds without them",
            record.snapshot_path
        )
    })?;
    if content_digest(&bytes) != record.spec_digest {
        bail!(
            "[approval-snapshot-tampered] {} no longer digests to the approved {} — \
             tampered snapshot; refusing",
            record.snapshot_path,
            record.spec_digest
        );
    }
    Ok(())
}

fn approval_output(record: &ProductApprovalRecord) -> ProductApprovalOutput {
    ProductApprovalOutput {
        spec_digest: record.spec_digest.clone(),
        approver: record.approver.clone(),
        approved_at: record.approved_at.clone(),
        snapshot_path: record.snapshot_path.clone(),
    }
}

/// Open the product state store (`models/.rocky-state.redb` resolution is
/// the caller's; the product verbs receive the resolved path).
fn open_state_store(state_path: &Path) -> Result<StateStore> {
    StateStore::open(state_path).with_context(|| {
        format!(
            "failed to open the state store at {} for the product records",
            state_path.display()
        )
    })
}

/// The redb cache budget for a request-local read of the product records:
/// open, read a handful of rows, drop. See the `GET /api/v1/runs` RSS
/// measurement on [`StateStore::open_read_only_with_cache`].
const PRODUCT_READ_CACHE_BYTES: usize = 1 << 20;

/// Open the store for a read: `product status` and `product list`, on the
/// CLI and over HTTP. A store that carries every table is opened with a read
/// transaction and never written, whatever its version stamp says; an older
/// one keeps its stamp (a read-only open never stamps, upgrades, or creates
/// a table — a store missing one is refused with `ReadOnlyNeedsInit`); one
/// written by a newer engine is refused.
fn open_state_store_read_only(state_path: &Path) -> Result<StateStore> {
    StateStore::open_read_only_or_empty_with_cache(state_path, PRODUCT_READ_CACHE_BYTES)
        .with_context(|| {
            format!(
                "failed to open the state store at {} to read the product records",
                state_path.display()
            )
        })
}

/// The E4 authority transition, as one function the CLI wraps.
///
/// 1. Snapshot bytes first, immutable and digest-addressed (tmp+rename;
///    never overwritten).
/// 2. Then ONE state-store write transaction that CASes the approval
///    record, CASes the fulfillment state to `spec_approved`, and
///    appends the journal row. A crash between 1 and 2 leaves only an
///    orphan snapshot file (harmless, GC-able); the transaction itself
///    is all-or-nothing.
///
/// Approving the digest that is already approved is a no-op success
/// (`already_approved = true`) — nothing is re-written, no journal row
/// is appended. A lost CAS (another approver won the race between this
/// process's read and its write) is a clean error naming the winning
/// digest.
pub(crate) fn product_approve_in(
    root: &Path,
    state_path: &Path,
    product_name: &str,
) -> Result<ProductApproveOutput> {
    product_approve_with(root, state_path, product_name, None, &approver_string)
}

/// [`product_approve_in`] that approves only the spec whose digest is
/// `expect_digest`. A different working spec refuses with
/// `approval-digest-mismatch` before the state store is opened and before
/// anything is written.
pub(crate) fn product_approve_expecting(
    root: &Path,
    state_path: &Path,
    product_name: &str,
    expect_digest: Option<&str>,
) -> Result<ProductApproveOutput> {
    product_approve_with(
        root,
        state_path,
        product_name,
        expect_digest,
        &approver_string,
    )
}

/// [`product_approve_in`] with the approver resolved by `approver`, so a test
/// can fix the identity. It is called after the in-flight check and before the
/// snapshot is written: an approval that must refuse for want of an identity
/// writes nothing.
fn product_approve_with(
    root: &Path,
    state_path: &Path,
    product_name: &str,
    expect_digest: Option<&str>,
    approver: &dyn Fn() -> Result<String>,
) -> Result<ProductApproveOutput> {
    let parsed = load_spec(root, product_name).map_err(|reject| anyhow::anyhow!("{reject}"))?;
    // The digest the caller confirmed must be the spec on disk now. This is
    // checked before the store is opened, so a mismatch writes nothing.
    if let Some(expected) = expect_digest
        && expected != parsed.digest
    {
        bail!(
            "[approval-digest-mismatch] the working spec of product '{product_name}' has digest \
             {}, not the expected {expected}; the spec changed since you read it. Nothing was \
             written. Read the spec again and approve the digest you mean",
            parsed.digest
        );
    }
    let store = open_state_store(state_path)?;

    let observed_approval = store.product_approval_get(product_name)?;
    let observed_state = store.fulfill_state_get(product_name)?;
    let previous_state = observed_state
        .as_ref()
        .map(|record| record.state.tag().to_string());

    if let Some(existing) = &observed_approval
        && existing.spec_digest == parsed.digest
    {
        // Idempotent re-approve: the digest is already the approved one.
        // Verify the snapshot is intact and report, writing nothing.
        verify_approval_snapshot(root, existing)?;
        return Ok(ProductApproveOutput {
            version: VERSION.to_string(),
            command: "product_approve".to_string(),
            product_id: parsed.product_id(),
            spec_digest: parsed.digest.clone(),
            output_model: parsed.output_model().to_string(),
            approver: existing.approver.clone(),
            approved_at: existing.approved_at.clone().unwrap_or_default(),
            snapshot_path: existing.snapshot_path.clone(),
            previous_state,
            state: "spec_approved".to_string(),
            already_approved: true,
        });
    }

    // A NEW-digest approval replaces the whole fulfillment record — so
    // it must never land on top of in-flight work: the replacement
    // would drop the record's `driver_pgid` (orphaning a live worker
    // group no takeover could sweep) and the pinned idempotency key (an
    // in-flight apply would lose its only authoritative resolution
    // handle). Refuse while the loop is mid-step or a worker group is
    // recorded; approve from the stop states. Refusal — not a pin
    // hand-off — is deliberate: carrying the OLD generation's plan/key
    // pins onto the NEW digest is exactly the inheritance the
    // supersession exists to prevent.
    //
    // The complement is the stop set, and it is deliberate, not an
    // oversight in a non-exhaustive `matches!`: `init`, `spec_approved`,
    // `needs_input`, `blocked`, `observing` — and `observed_failing`.
    // The data-red state is a REST state like `observing`: its apply is
    // terminal, so no pinned key is in play, and no worker is running (a
    // live group is caught by the `driver_pgid` arm below regardless).
    // Re-approving out of it is a legitimate human fix — "the declared
    // check was wrong, here is a corrected spec" — and refusing that
    // would leave a product failing its checks with no way forward but
    // the loop's own repair.
    if let Some(record) = &observed_state {
        let active_state = matches!(
            record.state,
            FulfillState::Elicited
                | FulfillState::LoweredContract
                | FulfillState::Drafting
                | FulfillState::Merged
                | FulfillState::Verifying
                | FulfillState::Proposed
                | FulfillState::PlanApproved
                | FulfillState::Applying
                | FulfillState::ApplyingUnknown
                | FulfillState::Applied
                | FulfillState::Superseded { .. }
        );
        if active_state || record.driver_pgid.is_some() {
            let why = if record.driver_pgid.is_some() {
                format!(
                    "a worker process group (pgid {}) is recorded as live",
                    record.driver_pgid.unwrap_or_default()
                )
            } else {
                format!("the loop state is '{}'", record.state.tag())
            };
            bail!(
                "[approval-refused-in-flight] refusing to approve a new spec revision for \
                 product '{product_name}' while fulfillment work is in flight ({why}). \
                 Stop the running `rocky fulfill` loop (or let it reach its next stop — \
                 needs_input, blocked, or observing), then re-run the approval; nothing \
                 was written."
            );
        }
    }

    // The approver is resolved first: an HTTP API approval with no usable git
    // identity refuses here, before anything is written.
    let approver = approver()?;

    // 1. The immutable snapshot file, before any record.
    let snapshot_path = write_approval_snapshot(root, &parsed)?;

    // 2. One all-or-nothing transaction.
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
    let approval = ProductApprovalRecord {
        product_id: parsed.product_id(),
        spec_digest: parsed.digest.clone(),
        approver: approver.clone(),
        approved_at: Some(now.clone()),
        snapshot_path: snapshot_path.clone(),
    };
    let new_state = FulfillStateRecord::new(
        FulfillState::SpecApproved,
        parsed.product_id(),
        Some(parsed.digest.clone()),
        Some(now.clone()),
    );
    let journal_row = FulfillJournalRow {
        seq: 0, // allocated inside the transaction
        at: Some(now.clone()),
        event: "spec approved".to_string(),
        from_state: previous_state.clone(),
        to_state: "spec_approved".to_string(),
        spec_digest: Some(parsed.digest.clone()),
        plan_id: None,
        idempotency_key: None,
    };
    match store.product_approval_cas(
        product_name,
        observed_approval.as_ref(),
        &approval,
        observed_state.as_ref(),
        &new_state,
        &journal_row,
    )? {
        rocky_core::fulfill::FulfillCas::Won => Ok(ProductApproveOutput {
            version: VERSION.to_string(),
            command: "product_approve".to_string(),
            product_id: parsed.product_id(),
            output_model: parsed.output_model().to_string(),
            spec_digest: parsed.digest.clone(),
            approver,
            approved_at: now,
            snapshot_path,
            previous_state,
            state: "spec_approved".to_string(),
            already_approved: false,
        }),
        rocky_core::fulfill::FulfillCas::Lost {
            current_approval, ..
        } => {
            let winner = current_approval
                .map(|record| record.spec_digest)
                .unwrap_or_else(|| "<none>".to_string());
            bail!(
                "[approval-cas-lost] another process moved product '{product_name}' between \
                 this approve's read and its write (the currently approved digest is \
                 {winner}); nothing was recorded — re-run to approve on top of the current \
                 state"
            )
        }
    }
}

/// Execute `rocky product approve <name>`.
pub fn run_product_approve(
    config_path: &Path,
    product_name: &str,
    state_path: &Path,
    expect_digest: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let root = product_command_root(config_path);
    let output = product_approve_expecting(&root, state_path, product_name, expect_digest)?;
    if output_json {
        print_json(&output)?;
    } else if output.already_approved {
        println!(
            "product {} spec {} was already approved (snapshot {})",
            output.product_id, output.spec_digest, output.snapshot_path
        );
    } else {
        println!(
            "product {} spec {} approved by {} (snapshot {})",
            output.product_id, output.spec_digest, output.approver, output.snapshot_path
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

/// Open the state store at `path`, or `None` when there is genuinely no store.
///
/// Every product verb needs this and each one used to spell it `path.exists()`,
/// which answers **false for every metadata error** — not only for a path that
/// is not there. A store the process could not stat therefore read as "no store
/// yet", and each caller carried on with its empty-world answer: `status`
/// reported a product that had never run, `compile` proceeded as though no
/// approval existed, and the name and journal readers dropped every record the
/// store held.
///
/// The discriminator is `rocky-core`'s, written for #1668 and #1707, and this
/// is the single place the product verbs reach it. Absent stays absent — no
/// store yet is the ordinary case. Anything else refuses.
fn open_store_if_present<T>(
    path: Option<&Path>,
    open: impl FnOnce(&Path) -> Result<T>,
) -> Result<Option<T>> {
    let Some(path) = path else { return Ok(None) };
    match std::fs::metadata(path) {
        Ok(_) => Ok(Some(open(path)?)),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            match rocky_core::path_presence::classify_not_found(path) {
                rocky_core::path_presence::PathPresence::Absent => Ok(None),
                rocky_core::path_presence::PathPresence::Present { detail } => {
                    anyhow::bail!("{} cannot be read: {detail}", path.display())
                }
            }
        }
        Err(err) => anyhow::bail!("{} cannot be read: {err}", path.display()),
    }
}

/// The read-only status report. Never mutates: a pending staging journal
/// is REPORTED, not recovered — the next compile resolves it.
pub(crate) fn product_status_in(
    root: &Path,
    state_path: Option<&Path>,
    product_name: &str,
) -> Result<ProductStatusOutput> {
    product_status_at(root, state_path, product_name, chrono::Utc::now())
}

/// [`product_status_in`] judged at `now`, so a test can fix the clock the
/// delivery verdict reads.
pub(crate) fn product_status_at(
    root: &Path,
    state_path: Option<&Path>,
    product_name: &str,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<ProductStatusOutput> {
    let mut output = ProductStatusOutput {
        version: VERSION.to_string(),
        command: "product_status".to_string(),
        product: product_name.to_string(),
        spec_present: false,
        product_id: None,
        spec_digest: None,
        output_model: None,
        spec_error: None,
        committed_phase: None,
        committed_spec_digest: None,
        artifact_problems: Vec::new(),
        staging_journal_present: false,
        approval: None,
        snapshot_intact: None,
        spec_matches_approval: None,
        fulfill_state: None,
        journal_rows: 0,
        delivery: None,
        volume: None,
    };

    let parsed = match load_spec(root, product_name) {
        Ok(parsed) => {
            output.spec_present = true;
            output.product_id = Some(parsed.product_id());
            output.spec_digest = Some(parsed.digest.clone());
            output.output_model = Some(parsed.output_model().to_string());
            Some(parsed)
        }
        Err(reject) => {
            output.spec_error = Some(format!("{reject}"));
            None
        }
    };

    if let Some(manifest) =
        committed_manifest(root, product_name).map_err(|reject| anyhow::anyhow!("{reject}"))?
    {
        output.committed_phase = Some(
            match manifest.phase {
                ManifestPhase::LoweredContract => "lowered_contract",
                ManifestPhase::Merged => "merged",
            }
            .to_string(),
        );
        output.committed_spec_digest = Some(manifest.spec_digest.clone());
        output.artifact_problems = verify_artifact_hashes(root, &manifest);
    }
    output.staging_journal_present = root
        .join(state_dir_rel(product_name))
        .join(rocky_core::product::commit::STAGING_JOURNAL)
        .is_file();

    // `Path::exists()` answers false for every metadata error, not just for a
    // path that is not there — so a store the process may not stat produced a
    // status reading "the loop has not run", "approval none" and an empty
    // journal. On a screen a reviewer trusts before approving, an unreadable
    // store must refuse, never render as a healthy empty one. `rocky-core`
    // already owns the discriminator this needs (#1668, #1707).
    let store = open_store_if_present(state_path, open_state_store_read_only)?;
    if let Some(parsed) = &parsed
        && let Some(deliver_by) = parsed.product().deliver_by()
    {
        let deadline = rocky_core::delivery::ProductDeadline {
            product: parsed.product().name.clone(),
            output_model: parsed.output_model().to_string(),
            deliver_by,
        };
        let estimate = match &store {
            Some(store) => store.get_duration_estimate(
                &deadline.output_model,
                rocky_core::state::UnrecordedScope::Exclude,
            )?,
            None => rocky_core::delivery::estimate_durations(&[]),
        };
        output.delivery = Some(crate::output::DeliveryStatus::judge(
            &deadline,
            store.as_ref(),
            &estimate,
            crate::output::EstimateBasis::ModelDelivery,
            now,
        )?);
    }
    if let Some(parsed) = &parsed
        && let Some(volume) = &parsed.product().output.volume
    {
        let latest = match &store {
            Some(store) => store.last_volume_count(
                parsed.output_model(),
                rocky_core::state::UnrecordedScope::Exclude,
            )?,
            None => None,
        };
        let row_count = latest
            .as_ref()
            .and_then(|(_, execution)| execution.row_count);
        output.volume = Some(ProductVolumeStatus {
            min_rows: volume.min_rows,
            max_rows: volume.max_rows,
            severity: volume.severity(),
            row_count,
            counted_at: latest.as_ref().map(|(_, execution)| execution.finished_at),
            run_id: latest.map(|(run_id, _)| run_id),
            verdict: row_count.map_or(
                rocky_core::product::spec::VolumeVerdict::NotEvaluated,
                |count| volume.judge(count),
            ),
        });
    }
    if let Some(store) = store {
        if let Some(record) = store.product_approval_get(product_name)? {
            // `snapshot_intact: false` is an accusation — it says the bytes a
            // human approved no longer match. Only a snapshot that was READ
            // may earn it. A snapshot that could not be read has proven
            // nothing, and saying "not intact" there cries tampering over a
            // permission bit. Absent still earns false: the approved bytes are
            // genuinely gone, which is the thing the field exists to report.
            let snapshot_file = root.join(&record.snapshot_path);
            let intact = match std::fs::read(&snapshot_file) {
                Ok(bytes) => content_digest(&bytes) == record.spec_digest,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    match rocky_core::path_presence::classify_not_found(&snapshot_file) {
                        rocky_core::path_presence::PathPresence::Absent => false,
                        rocky_core::path_presence::PathPresence::Present { detail } => {
                            anyhow::bail!(
                                "the approval snapshot {} cannot be read: {detail}",
                                snapshot_file.display()
                            )
                        }
                    }
                }
                Err(err) => anyhow::bail!(
                    "the approval snapshot {} cannot be read: {err}",
                    snapshot_file.display()
                ),
            };
            output.snapshot_intact = Some(intact);
            output.spec_matches_approval = parsed
                .as_ref()
                .map(|parsed| parsed.digest == record.spec_digest);
            output.approval = Some(approval_output(&record));
        }
        if let Some(record) = store.fulfill_state_get(product_name)? {
            output.fulfill_state = Some(record.state.tag().to_string());
            output.journal_rows = record.journal_seq;
        }
    }
    Ok(output)
}

/// Every product name this project knows, sorted and deduplicated.
///
/// The union of the `products/*.toml` stems and every name the state store
/// holds a fulfillment or approval record for. So a product whose spec file
/// was deleted still lists, with `spec_present = false`. A missing directory
/// or a state file that does not exist yet contributes nothing.
pub(crate) fn product_names_in(root: &Path, state_path: Option<&Path>) -> Result<Vec<String>> {
    let mut names = std::collections::BTreeSet::new();

    let dir = root.join("products");
    if dir.is_dir() {
        for entry in
            std::fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))?
        {
            let entry = entry.with_context(|| format!("reading an entry of {}", dir.display()))?;
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("toml") {
                continue;
            }
            // Only a regular file is a spec. A directory named `x.toml`, a
            // FIFO, a socket or a dangling link is not a product — and a
            // FIFO would block a reader that opened it. `metadata()` follows
            // a link to a regular file, the same way `product status` reads
            // one: the project tree is the operator's, and links inside it
            // are followed as every command follows them. A dangling link
            // is skipped; any other failure to stat an entry is reported,
            // never turned into a silently shorter list.
            let is_regular_file = match entry.metadata() {
                Ok(meta) => meta.file_type().is_file(),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => false,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("reading the type of {}", path.display()));
                }
            };
            if !is_regular_file {
                continue;
            }
            if let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) {
                names.insert(stem.to_string());
            }
        }
    }

    if let Some(store) = open_store_if_present(state_path, open_state_store_read_only)? {
        names.extend(store.fulfill_state_product_names()?);
        names.extend(store.product_approval_product_names()?);
    }

    Ok(names.into_iter().collect())
}

/// The `rocky product list` payload: one [`ProductListEntry`] per known
/// product, each a projection of [`product_status_in`].
pub(crate) fn product_list_in(root: &Path, state_path: Option<&Path>) -> Result<ProductListOutput> {
    let products = product_names_in(root, state_path)?
        .into_iter()
        .map(|name| product_status_in(root, state_path, &name).map(ProductListEntry::from_status))
        .collect::<Result<Vec<_>>>()?;
    let count = products.len();
    Ok(ProductListOutput {
        version: VERSION.to_string(),
        command: "product_list".to_string(),
        products,
        count,
    })
}

/// Execute `rocky product list`.
///
/// Reads `products/` next to the config file, as `product status` does.
pub fn run_product_list(config_path: &Path, state_path: &Path, output_json: bool) -> Result<()> {
    let root = product_command_root(config_path);
    let output = product_list_in(&root, Some(state_path))?;
    if output_json {
        print_json(&output)?;
        return Ok(());
    }
    if output.products.is_empty() {
        println!("no products: no products/*.toml, and no product records in the state store");
        return Ok(());
    }
    for product in &output.products {
        let spec = if product.spec_present {
            "ok"
        } else {
            "missing or unreadable"
        };
        println!(
            "{}  spec={}  lowering={}  approved={}  state={}  journal={}",
            product.name,
            spec,
            product.committed_phase.as_deref().unwrap_or("none"),
            if product.approval.is_some() {
                "yes"
            } else {
                "no"
            },
            product.fulfill_state.as_deref().unwrap_or("none"),
            product.journal_rows,
        );
    }
    Ok(())
}

/// The journal of one product, or `None` when the project does not know
/// the name: no `products/<name>.toml` and no fulfillment or approval
/// record in the store. The same rule `product status` and the product
/// routes use for "known".
///
/// Rows come from [`StateStore::fulfill_journal_rows`], the function the
/// fulfillment loop's own driver reads through, so what this shows is what
/// the loop sees. The store is opened read-only; an absent store is an
/// empty journal.
pub(crate) fn product_journal_in(
    root: &Path,
    state_path: Option<&Path>,
    product_name: &str,
) -> Result<Option<ProductJournalOutput>> {
    let known = product_names_in(root, state_path)?;
    if !known.iter().any(|name| name == product_name) {
        return Ok(None);
    }
    let rows: Vec<ProductJournalEntry> =
        match open_store_if_present(state_path, open_state_store_read_only)? {
            Some(store) => store
                .fulfill_journal_rows(product_name)?
                .into_iter()
                .map(|row| ProductJournalEntry {
                    seq: row.seq,
                    at: row.at,
                    event: row.event,
                    from_state: row.from_state,
                    to_state: row.to_state,
                    spec_digest: row.spec_digest,
                    plan_id: row.plan_id,
                    idempotency_key: row.idempotency_key,
                })
                .collect(),
            _ => Vec::new(),
        };
    Ok(Some(ProductJournalOutput {
        version: VERSION.to_string(),
        command: "product_journal".to_string(),
        product: product_name.to_string(),
        product_id: rocky_core::fulfill::fulfill_state_key(product_name),
        count: rows.len(),
        rows,
    }))
}

/// Execute `rocky product journal <name>`.
pub fn run_product_journal(
    config_path: &Path,
    product_name: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let root = product_command_root(config_path);
    let Some(output) = product_journal_in(&root, Some(state_path), product_name)? else {
        anyhow::bail!(
            "product '{product_name}' is not known: no products/{product_name}.toml, and no \
             fulfillment or approval record in the state store"
        );
    };
    if output_json {
        print_json(&output)?;
        return Ok(());
    }
    println!(
        "product {} journal: {} row(s)",
        output.product, output.count
    );
    for row in &output.rows {
        let plan = row
            .plan_id
            .as_deref()
            .map(|plan_id| format!("  plan={plan_id}"))
            .unwrap_or_default();
        println!(
            "  #{:08}  {}  {}  {} -> {}{plan}",
            row.seq,
            row.at.as_deref().unwrap_or("-"),
            row.event,
            row.from_state.as_deref().unwrap_or("(none)"),
            row.to_state,
        );
    }
    Ok(())
}

/// Execute `rocky product status <name>`.
pub fn run_product_status(
    config_path: &Path,
    product_name: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let root = product_command_root(config_path);
    let output = product_status_in(&root, Some(state_path), product_name)?;
    if output_json {
        print_json(&output)?;
    } else {
        println!("product {}", output.product);
        match (&output.product_id, &output.spec_error) {
            (Some(id), _) => println!(
                "  spec: {} ({})",
                id,
                output.spec_digest.as_deref().unwrap_or("")
            ),
            (None, Some(err)) => println!("  spec: unreadable — {err}"),
            (None, None) => println!("  spec: missing"),
        }
        match &output.committed_phase {
            Some(phase) => println!("  lowering: committed phase {phase}"),
            None => println!("  lowering: none committed"),
        }
        for problem in &output.artifact_problems {
            println!("  artifact drift: {problem}");
        }
        if output.staging_journal_present {
            println!("  staging journal: PENDING (the next compile recovers it)");
        }
        match &output.approval {
            Some(approval) => {
                println!(
                    "  approval: {} by {} (snapshot {}, intact: {})",
                    approval.spec_digest,
                    approval.approver,
                    approval.snapshot_path,
                    output
                        .snapshot_intact
                        .map(|intact| intact.to_string())
                        .unwrap_or_else(|| "unknown".to_string()),
                );
                if let Some(false) = output.spec_matches_approval {
                    println!("  note: the working spec differs from the approved revision");
                }
            }
            None => println!("  approval: none"),
        }
        match &output.fulfill_state {
            Some(state) => println!("  state: {state} ({} journal rows)", output.journal_rows),
            None => println!("  state: none recorded"),
        }
        if let Some(delivery) = &output.delivery {
            print_delivery(delivery);
        }
        if let Some(volume) = &output.volume {
            print_volume(volume);
        }
    }
    Ok(())
}

/// The human-readable volume lines of `rocky product status`.
fn print_volume(volume: &ProductVolumeStatus) {
    let bound = |value: Option<u64>| value.map_or_else(|| "-".to_string(), |v| v.to_string());
    println!(
        "  volume: min_rows {}, max_rows {} ({})",
        bound(volume.min_rows),
        bound(volume.max_rows),
        volume.severity.as_str()
    );
    let verdict = match volume.verdict {
        rocky_core::product::spec::VolumeVerdict::InRange => "in range",
        rocky_core::product::spec::VolumeVerdict::BelowMin => "BELOW min_rows",
        rocky_core::product::spec::VolumeVerdict::AboveMax => "ABOVE max_rows",
        rocky_core::product::spec::VolumeVerdict::NotEvaluated => "not evaluated",
    };
    match (volume.row_count, volume.counted_at) {
        (Some(count), Some(at)) => println!(
            "    latest count: {count} row(s) at {} ({verdict})",
            at.to_rfc3339()
        ),
        _ => println!("    latest count: none yet ({verdict})"),
    }
}

/// The human-readable delivery lines of `rocky product status`.
fn print_delivery(delivery: &crate::output::DeliveryStatus) {
    println!(
        "  delivery: by '{}' ({})",
        delivery.deliver_by, delivery.timezone
    );
    let last = match (delivery.on_time, delivery.late_by_seconds) {
        (Some(true), _) => "on time".to_string(),
        (Some(false), Some(late)) => format!("late by {late}s"),
        (Some(false), None) => "late".to_string(),
        (None, _) => "no deadline recorded yet".to_string(),
    };
    println!("    last delivery: {last}");
    let estimate = &delivery.estimate;
    match (estimate.p50_ms, estimate.p90_ms) {
        (Some(p50), Some(p90)) => println!(
            "    estimate: p50 {p50} ms, p90 {p90} ms (n = {} of {})",
            estimate.n, estimate.window
        ),
        _ => println!(
            "    estimate: unknown (n = {}, needs 5 production runs)",
            estimate.n
        ),
    }
    let risk = match delivery.at_risk {
        Some(true) => "AT RISK",
        Some(false) => "not at risk",
        None => "no verdict",
    };
    match delivery.next_due_at {
        Some(due) => println!("    next deadline: {} ({risk})", due.to_rfc3339()),
        None => println!("    next deadline: unknown ({risk})"),
    }
}

/// Recovery surface for tests: expose whether a pending journal would
/// roll back or forward without running a full compile.
#[allow(dead_code)]
pub(crate) fn product_recover_in(root: &Path, product_name: &str) -> Result<RecoveryAction> {
    let parsed = load_spec(root, product_name).map_err(|reject| anyhow::anyhow!("{reject}"))?;
    recover_generation(root, &parsed).map_err(|reject| anyhow::anyhow!("{reject}"))
}

/// The loop's (re)opening of a drafting window (#1493): byte-verify the
/// committed MERGED generation in full, then demote its manifest to
/// Phase A through the staged commit. See
/// [`rocky_core::product::commit::reopen_for_drafting`] for the
/// protocol.
///
/// This wrapper only opens the state store and resolves the spec. The
/// decision evidence is read inside [`reopen_for_drafting`] itself, from
/// that store — never handed to it — so no caller anywhere can
/// manufacture the evidence by building a record of its own.
///
/// # Errors
///
/// Whatever the core reopen refuses (`reopen-undecided` when no record
/// exists or it is not the loop's decided round), plus the spec load and
/// the store open.
pub(crate) fn product_reopen_in(
    root: &Path,
    state_path: &Path,
    product_name: &str,
) -> Result<ReopenOutcome> {
    let parsed = load_spec(root, product_name).map_err(|reject| anyhow::anyhow!("{reject}"))?;
    let spec_path = spec_rel(product_name);
    let store = open_state_store(state_path)?;
    reopen_for_drafting(root, &spec_path, &parsed, &store)
        .map_err(|reject| anyhow::anyhow!("{reject}"))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use rocky_core::product::spec::parse_spec_bytes;

    /// The answer key's spec fixture, shared with the rocky-core lowering
    /// tests (same bytes, same digest, embedded in the goldens).
    const SPEC_FIXTURE: &[u8] =
        include_bytes!("../../../rocky-core/src/product/testdata/revenue_daily.spec.toml");

    /// A minimal ENGINE-VALID config. The prototype's fixture omitted the
    /// pipeline target because its mirror read only the `[policy]` table;
    /// the real verifier loads the whole config through the engine's
    /// schema, which requires one.
    const BASE_CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "test.duckdb"

[pipeline.main]
type = "transformation"
models = "models/**"

[pipeline.main.target.governance]
auto_create_schemas = true
"#;

    /// The FF-DESIGN D5 block, verbatim — the paste-ready posture.
    const D5_BLOCK: &str = r#"[policy]
version = 1
default_agent_effect = "require_review"

# Authoring lane: the agent may draft and propose WITHIN the product's scope…
[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { models = ["<output_model>"] }
effect = "allow"

# …but applying stays a human decision (explicit, though the default already covers it).
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["<output_model>"] }
effect = "require_review"
"#;

    fn parsed_d3() -> ParsedSpec {
        parse_spec_bytes(SPEC_FIXTURE, "products/revenue_daily.toml").expect("fixture parses")
    }

    fn write_file(path: &Path, bytes: &[u8]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("mkdir");
        }
        std::fs::write(path, bytes).expect("write");
    }

    /// A project root carrying the spec fixture and a config assembled
    /// from `BASE_CONFIG` plus `extra`.
    fn project_with_config(dir: &Path, extra: &str) -> (PathBuf, PathBuf) {
        let root = dir.join("project");
        write_file(&root.join("products/revenue_daily.toml"), SPEC_FIXTURE);
        std::fs::create_dir_all(root.join("models")).expect("mkdir");
        let config = root.join("rocky.toml");
        write_file(&config, format!("{BASE_CONFIG}{extra}").as_bytes());
        (root, config)
    }

    fn posture(dir: &Path, extra: &str) -> PostureResult {
        let (_root, config) = project_with_config(dir, extra);
        verify_policy_posture(&config, &parsed_d3())
    }

    // ----- the paste block IS the FF-DESIGN D5 block -----

    #[test]
    fn paste_block_matches_ff_design_d5_exactly() {
        assert_eq!(paste_block("<output_model>"), D5_BLOCK);
        assert_eq!(
            paste_block("revenue_daily"),
            D5_BLOCK.replace("<output_model>", "revenue_daily")
        );
    }

    // ----- the three-check posture verification (checks 1 + 2) -----

    #[test]
    fn absent_policy_block_needs_input_with_paste_block() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), "");
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(
            result.paste_block.as_deref(),
            Some(&*paste_block("revenue_daily"))
        );
        // The trap named: enforcement allows on an absent block even
        // though `policy check` predicts review — existence is checked
        // directly.
        assert!(
            result.reason.contains("ENFORCEMENT allows"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn bare_default_require_review_block_is_not_a_pass() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(
            dir.path(),
            "\n[policy]\nversion = 1\ndefault_agent_effect = \"require_review\"\n",
        );
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(result.propose_effect, Some(PolicyEffect::RequireReview));
        assert!(result.reason.contains("stall"), "{}", result.reason);
        assert!(result.paste_block.is_some());
    }

    #[test]
    fn full_corrected_block_passes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), &format!("\n{}", paste_block("revenue_daily")));
        assert_eq!(result.status, VerifyStatus::Pass, "{}", result.reason);
        assert_eq!(result.propose_effect, Some(PolicyEffect::Allow));
        assert_eq!(result.apply_effect, Some(PolicyEffect::RequireReview));
    }

    #[test]
    fn explicit_agent_allow_apply_reaching_scope_fails_naming_the_rule() {
        let dir = tempfile::tempdir().expect("tempdir");
        let block = r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { models = ["revenue_daily"] }
effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["revenue_*"] }
effect = "allow"
"#;
        let result = posture(dir.path(), block);
        assert_eq!(result.status, VerifyStatus::Fail);
        assert_eq!(result.apply_effect, Some(PolicyEffect::Allow));
        assert!(result.reason.contains("rule 1"), "{}", result.reason);
    }

    #[test]
    fn corrected_block_defends_against_a_broader_apply_allow() {
        // With the explicit scoped require_review in place, a broad `any`
        // apply-allow is dominated (strict-superset specificity) — the
        // corrected posture is defense in depth, exactly why D5 spells
        // the apply rule out despite the default covering it.
        let dir = tempfile::tempdir().expect("tempdir");
        let broad_allow = r#"
[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
"#;
        let result = posture(
            dir.path(),
            &format!("\n{}{broad_allow}", paste_block("revenue_daily")),
        );
        assert_eq!(result.status, VerifyStatus::Pass, "{}", result.reason);
        assert_eq!(result.apply_effect, Some(PolicyEffect::RequireReview));
    }

    // ----- the frozen posture is required EXACTLY -----

    #[test]
    fn permissive_default_with_scoped_review_rule_is_rejected() {
        // Adversarial: default_agent_effect = "allow" + a scoped
        // apply-review rule RESOLVES safely for this product — and is
        // still rejected: the permissive default is global agent
        // authority.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = r#"
[policy]
version = 1
default_agent_effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["revenue_daily"] }
effect = "require_review"
"#;
        let result = posture(dir.path(), block);
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("default_agent_effect"),
            "{}",
            result.reason
        );
        assert_eq!(
            result.paste_block.as_deref(),
            Some(&*paste_block("revenue_daily"))
        );
    }

    #[test]
    fn any_true_propose_allow_is_rejected() {
        // The allow flows through global scope, not the exactly-scoped
        // authoring rule — rejected even though it DOES resolve allow.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["revenue_daily"] }
effect = "require_review"
"#;
        let result = posture(dir.path(), block);
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(result.propose_effect, Some(PolicyEffect::Allow));
        assert!(result.reason.contains("broader"), "{}", result.reason);
        assert_eq!(
            result.paste_block.as_deref(),
            Some(&*paste_block("revenue_daily"))
        );
    }

    #[test]
    fn broader_glob_propose_allow_is_rejected() {
        // A glob that happens to match the output model is still broader
        // than the frozen posture's literal scope.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = paste_block("revenue_daily").replace(
            "scope = { models = [\"revenue_daily\"] }\neffect = \"allow\"",
            "scope = { models = [\"revenue_*\"] }\neffect = \"allow\"",
        );
        let result = posture(dir.path(), &format!("\n{block}"));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(result.reason.contains("broader"), "{}", result.reason);
    }

    #[test]
    fn extra_predicate_on_the_authoring_rule_is_rejected() {
        // The frozen posture's scope is the literal model list and
        // NOTHING else — an added predicate is a different posture, not a
        // variant.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = paste_block("revenue_daily").replace(
            "scope = { models = [\"revenue_daily\"] }\neffect = \"allow\"",
            "scope = { models = [\"revenue_daily\"], contracted = true }\neffect = \"allow\"",
        );
        let result = posture(dir.path(), &format!("\n{block}"));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
    }

    #[test]
    fn budgeted_exact_propose_allow_is_rejected_naming_the_budget() {
        // A budgeted allow is not the frozen posture: the live engine
        // degrades it once the budget exhausts, so a static verification
        // cannot prove the posture that will actually run.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = paste_block("revenue_daily").replacen(
            "scope = { models = [\"revenue_daily\"] }\neffect = \"allow\"\n",
            "scope = { models = [\"revenue_daily\"] }\neffect = \"allow\"\n\
             autonomy_budget = { failures = 2, window = \"7d\" }\n",
            1,
        );
        let result = posture(dir.path(), &format!("\n{block}"));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(result.propose_effect, Some(PolicyEffect::Allow));
        assert!(
            result.reason.contains("autonomy_budget"),
            "{}",
            result.reason
        );
        assert!(result.reason.contains("failures = 2"), "{}", result.reason);
        assert!(result.reason.contains("7d"), "{}", result.reason);
        assert_eq!(
            result.paste_block.as_deref(),
            Some(&*paste_block("revenue_daily"))
        );
    }

    #[test]
    fn ceiling_on_the_authoring_rule_fails_closed_via_unproved_reachability() {
        // A max_downstreams ceiling on the propose allow degrades under
        // the post-image's UNPROVED reachability (None, never
        // 0-by-assumption), so the posture fails closed instead of
        // assuming a dependent-free model.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = paste_block("revenue_daily").replacen(
            "scope = { models = [\"revenue_daily\"] }\neffect = \"allow\"\n",
            "scope = { models = [\"revenue_daily\"], max_downstreams = 5 }\neffect = \"allow\"\n",
            1,
        );
        let result = posture(dir.path(), &format!("\n{block}"));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(result.propose_effect, Some(PolicyEffect::RequireReview));
    }

    #[test]
    fn wrong_policy_version_needs_input() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), "\n[policy]\nversion = 2\n");
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(result.reason.contains("version"), "{}", result.reason);
    }

    // ----- strict parsing IS the engine's serde (the mirror dissolved) -----
    //
    // The prototype mirrored serde's strictness in pydantic and pinned it
    // per shape. Here the config parser under test IS the engine's serde:
    // each shape it refuses turns into a needs_input carrying the parse
    // error. One test per answer-key node keeps the parity mapping 1:1.

    #[test]
    fn unknown_policy_key_needs_input() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), "\n[policy]\nversion = 1\nmode = \"strict\"\n");
        assert_eq!(result.status, VerifyStatus::NeedsInput);
    }

    #[test]
    fn string_policy_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), "\n[policy]\nversion = \"1\"\n");
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("does not parse"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn negative_policy_version_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), "\n[policy]\nversion = -1\n");
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("does not parse"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn integer_where_bool_expected_in_scope_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(
            dir.path(),
            "\n[policy]\nversion = 1\n\n[[policy.rules]]\nprincipal = \"agent\"\n\
             capability = \"propose\"\nscope = { any = 1 }\neffect = \"allow\"\n",
        );
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("does not parse"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn string_budget_failures_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(
            dir.path(),
            "\n[policy]\nversion = 1\n\n[[policy.rules]]\nprincipal = \"agent\"\n\
             capability = \"apply\"\nscope = { any = true }\neffect = \"allow\"\n\
             autonomy_budget = { failures = \"2\", window = \"7d\" }\n",
        );
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("does not parse"),
            "{}",
            result.reason
        );
    }

    // ----- autonomy-budget validation is the engine's validate_policy -----

    fn budget_rule(failures: &str, window: &str) -> String {
        format!(
            "\n[policy]\nversion = 1\n\n[[policy.rules]]\nprincipal = \"agent\"\n\
             capability = \"apply\"\nscope = {{ any = true }}\neffect = \"allow\"\n\
             autonomy_budget = {{ failures = {failures}, window = {window} }}\n"
        )
    }

    #[test]
    fn budget_zero_failures_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), &budget_rule("0", "\"7d\""));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(result.reason.contains("failures"), "{}", result.reason);
    }

    #[test]
    fn budget_invalid_window_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), &budget_rule("2", "\"banana\""));
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(result.reason.contains("banana"), "{}", result.reason);
    }

    #[test]
    fn valid_budget_is_not_flagged() {
        // A well-formed budget passes shape validation (the posture still
        // fails on OTHER grounds here — the rule is a global apply-allow —
        // so assert the budget itself raised no problem).
        let dir = tempfile::tempdir().expect("tempdir");
        let result = posture(dir.path(), &budget_rule("2", "\"7d\""));
        assert!(
            !result.reason.contains("autonomy_budget"),
            "{}",
            result.reason
        );
    }

    #[test]
    fn missing_config_needs_input() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        write_file(&root.join("products/revenue_daily.toml"), SPEC_FIXTURE);
        let result = verify_policy_posture(&root.join("rocky.toml"), &parsed_d3());
        assert_eq!(result.status, VerifyStatus::NeedsInput);
    }

    /// A `rocky.toml` that is a dangling symlink is present, not missing.
    /// `is_file()` followed the link and answered false, so `product verify`
    /// printed "rocky.toml not found at <path>" about a file the operator can
    /// see (#1729). The direction was always safe — still `needs_input` — but
    /// the reason has to be true, or there is nothing to act on.
    ///
    /// `#[cfg(unix)]`: creating a symlink on Windows needs Developer Mode or
    /// `SeCreateSymbolicLinkPrivilege`.
    #[cfg(unix)]
    #[test]
    fn a_dangling_config_symlink_reports_the_link_not_a_missing_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        write_file(&root.join("products/revenue_daily.toml"), SPEC_FIXTURE);
        let target = root.join("shared-prod.toml");
        let config = root.join("rocky.toml");
        std::os::unix::fs::symlink(&target, &config).expect("symlink");
        assert!(
            !config.is_file(),
            "precondition: the probe this replaces reports the config as missing"
        );

        let result = verify_policy_posture(&config, &parsed_d3());
        assert_eq!(
            result.status,
            VerifyStatus::NeedsInput,
            "the direction is unchanged: still needs input, never a pass"
        );
        assert!(
            !result.reason.contains("not found"),
            "a config that is plainly there must not be reported as missing: {}",
            result.reason
        );
        assert!(
            result.reason.contains(&config.display().to_string())
                && result.reason.contains(&target.display().to_string()),
            "the reason must name the config path and the link target: {}",
            result.reason
        );
    }

    /// The control: nothing at the path still reads as a missing config, with
    /// the message it always had.
    #[test]
    fn an_absent_config_still_reports_not_found() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        write_file(&root.join("products/revenue_daily.toml"), SPEC_FIXTURE);
        let config = root.join("rocky.toml");

        let result = verify_policy_posture(&config, &parsed_d3());
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("rocky.toml not found at"),
            "absence keeps its own message: {}",
            result.reason
        );
    }

    /// A directory sitting where `rocky.toml` should be never reached the
    /// parser, so "does not parse" was untrue for it too. It now says what it
    /// is. `is_file()` was false here before, so this case also used to print
    /// "not found".
    #[test]
    fn a_directory_at_the_config_path_reports_a_read_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        write_file(&root.join("products/revenue_daily.toml"), SPEC_FIXTURE);
        let config = root.join("rocky.toml");
        std::fs::create_dir_all(&config).expect("mkdir");

        let result = verify_policy_posture(&config, &parsed_d3());
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert!(
            result.reason.contains("could not be read")
                && result.reason.contains(&config.display().to_string()),
            "a directory must be reported as unreadable, not as missing: {}",
            result.reason
        );
    }

    #[test]
    fn synthetic_post_image_shape() {
        let attrs = synthetic_post_image(&parsed_d3());
        assert_eq!(attrs.name, "revenue_daily");
        assert_eq!(
            attrs.tags.get("product").map(String::as_str),
            Some("revenue_daily")
        );
        assert!(attrs.classifications.contains("pii"));
        assert!(attrs.contracted, "Phase A writes the sibling contract");
        // Reachability is UNPROVED, never 0-by-assumption: an existing
        // model can already reference this name, and resumes exist. None
        // makes any max_downstreams ceiling fail closed.
        assert_eq!(attrs.reachable_downstreams, None);
    }

    #[test]
    fn posture_evaluates_the_post_image_not_the_pre_image() {
        // A deny scoped to the POST-image attributes (the product tag the
        // merge will stamp) must bite at verification time, before
        // anything is written — the gate reads what the change creates,
        // not what exists.
        let dir = tempfile::tempdir().expect("tempdir");
        let block = format!(
            "\n{}\n[[policy.rules]]\nprincipal = \"agent\"\ncapability = \"propose\"\n\
             scope = {{ tags = {{ product = \"revenue_daily\" }} }}\neffect = \"deny\"\n",
            paste_block("revenue_daily")
        );
        let result = posture(dir.path(), &block);
        assert_eq!(result.status, VerifyStatus::NeedsInput);
        assert_eq!(result.propose_effect, Some(PolicyEffect::Deny));
    }

    // ----- classification-tag resolution (REJECT where W004 warns) -----

    fn classification_check(dir: &Path, extra: &str) -> SpecResult<()> {
        let (_root, config_path) = project_with_config(dir, extra);
        let config = rocky_core::config::load_rocky_config(&config_path).expect("config loads");
        check_classifications(&config, &parsed_d3())
    }

    #[test]
    fn unresolved_classification_tag_rejects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let error = classification_check(dir.path(), "").expect_err("unresolved");
        assert_eq!(error.code, "classification-unresolved");
        assert!(error.message.contains("pii"), "{error}");
        // The honesty clause: resolution is closed, application is not.
        assert!(error.message.contains("warehouse-dependent"), "{error}");
    }

    #[test]
    fn top_level_mask_strategy_resolves() {
        let dir = tempfile::tempdir().expect("tempdir");
        classification_check(dir.path(), "\n[mask]\npii = \"hash\"\n").expect("resolves");
    }

    #[test]
    fn env_override_mask_resolves_without_env_gating() {
        // Mirrors W004: a tag defined only under [mask.prod] still
        // resolves — the check is compile-time completeness, not
        // env-scoped.
        let dir = tempfile::tempdir().expect("tempdir");
        classification_check(dir.path(), "\n[mask.prod]\npii = \"none\"\n").expect("resolves");
    }

    #[test]
    fn allow_unmasked_resolves() {
        let dir = tempfile::tempdir().expect("tempdir");
        classification_check(
            dir.path(),
            "\n[classifications]\nallow_unmasked = [\"pii\"]\n",
        )
        .expect("resolves");
    }

    // ----- collision checks against fulfillment state dirs -----

    fn committed_manifest_for(
        root: &Path,
        product_name: &str,
        output_model: &str,
        spec_path: &str,
    ) {
        let manifest = Manifest {
            product_id: format!("product:{product_name}"),
            spec_digest: format!("sha256:{}", "0".repeat(64)),
            spec_path: spec_path.to_string(),
            output_model: output_model.to_string(),
            phase: ManifestPhase::LoweredContract,
            fields: std::collections::BTreeMap::new(),
            artifacts: std::collections::BTreeMap::new(),
        };
        write_file(
            &root.join(manifest_rel(product_name)),
            &manifest.to_json_bytes(),
        );
    }

    #[test]
    fn duplicate_product_name_vs_existing_state_dir_rejects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");
        committed_manifest_for(
            &root,
            "revenue_daily",
            "revenue_daily",
            "products/other_file.toml",
        );
        let error = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect_err("collision");
        assert_eq!(error.code, "duplicate-product-name");
        assert!(
            error.message.contains("products/other_file.toml"),
            "{error}"
        );
    }

    #[test]
    fn same_spec_path_is_not_a_name_collision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");
        committed_manifest_for(
            &root,
            "revenue_daily",
            "revenue_daily",
            "products/revenue_daily.toml",
        );
        check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect("clean");
    }

    #[test]
    fn duplicate_output_model_across_products_rejects() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");
        committed_manifest_for(
            &root,
            "other_product",
            "revenue_daily",
            "products/other.toml",
        );
        let error = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect_err("collision");
        assert_eq!(error.code, "duplicate-output-model");
        assert!(error.message.contains("other_product"), "{error}");
    }

    #[test]
    fn distinct_output_models_do_not_collide() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");
        committed_manifest_for(
            &root,
            "other_product",
            "another_model",
            "products/other.toml",
        );
        check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect("clean");
    }

    #[test]
    fn no_state_dirs_is_clean() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");
        check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect("clean");
    }

    /// #1817: a fulfillment dir the process cannot LIST is not "no other
    /// products". `let Ok(entries) = read_dir(..) else { return Ok(()) }`
    /// cleared the product on a reading nobody took, and `rocky product
    /// verify` reported PASS. Skips when the process can read through mode
    /// 000 (root), because then the condition was never built.
    #[cfg(unix)]
    #[test]
    fn an_unreadable_fulfillment_dir_refuses_the_collision_check() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        let fulfillment = root.join(".rocky").join("fulfillment");
        std::fs::create_dir_all(&fulfillment).expect("mkdir");
        std::fs::set_permissions(&fulfillment, std::fs::Permissions::from_mode(0o000))
            .expect("chmod");

        let reproduced = std::fs::read_dir(&fulfillment).is_err();
        let result = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml");
        std::fs::set_permissions(&fulfillment, std::fs::Permissions::from_mode(0o755)).ok();

        if !reproduced {
            eprintln!("skipping: this process can list a mode-000 directory");
            return;
        }
        let error = result.expect_err("a scan that cannot list the peers cannot clear this one");
        assert_eq!(error.code, "fulfillment-dir-unreadable", "{error}");
        assert!(error.message.contains("cannot be read"), "{error}");
    }

    /// The ancestor shape of the same defect: `.rocky/fulfillment` is a
    /// symlink to nowhere. `read_dir` reports `NotFound`, which the old scan
    /// read as "no fulfillment dir yet" — but an entry IS there.
    #[cfg(unix)]
    #[test]
    fn a_dangling_fulfillment_dir_refuses_the_collision_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        let rocky = root.join(".rocky");
        std::fs::create_dir_all(&rocky).expect("mkdir");
        std::os::unix::fs::symlink(dir.path().join("gone"), rocky.join("fulfillment"))
            .expect("symlink");

        let error = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect_err("a dangling fulfillment dir is present, not absent");
        assert_eq!(error.code, "fulfillment-dir-unreadable", "{error}");
        assert!(error.message.contains("cannot be resolved"), "{error}");
    }

    /// A peer STATE DIR that is a dangling symlink. `path.is_dir()` answered
    /// false and the peer was filtered out as "not a directory" — but an
    /// entry is there, aimed at nothing, and whatever it once held cannot be
    /// scanned. The #1822 review named this as the mutation the first tests
    /// could not catch: restoring the `is_dir()` fold leaves the other
    /// fulfillment-dir tests green, and only this one fails.
    #[cfg(unix)]
    #[test]
    fn a_dangling_peer_state_dir_refuses_the_collision_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        let fulfillment = root.join(".rocky").join("fulfillment");
        std::fs::create_dir_all(&fulfillment).expect("mkdir");
        std::os::unix::fs::symlink(dir.path().join("gone"), fulfillment.join("other_product"))
            .expect("symlink");

        let error = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect_err("a peer that cannot be entered cannot be cleared");
        assert_eq!(error.code, "fulfillment-dir-unreadable", "{error}");
        assert!(error.message.contains("other_product"), "{error}");
    }

    /// A peer whose manifest is a dangling symlink is a peer this scan cannot
    /// see — `is_file()` answered false and the peer was skipped as if it
    /// claimed nothing.
    #[cfg(unix)]
    #[test]
    fn a_peer_with_a_dangling_manifest_refuses_the_collision_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        let peer = root.join(state_dir_rel("other_product"));
        std::fs::create_dir_all(&peer).expect("mkdir");
        std::os::unix::fs::symlink(dir.path().join("gone"), peer.join(MANIFEST_FILENAME))
            .expect("symlink");

        let error = check_product_collisions(&root, &parsed_d3(), "products/revenue_daily.toml")
            .expect_err("an unreadable peer manifest cannot be read as 'claims nothing'");
        assert_eq!(error.code, "manifest-unreadable", "{error}");
    }

    #[test]
    fn collision_check_reads_the_layout_lower_writes() {
        // Guard the seam: the collision check must read the same
        // state-dir/manifest layout the commit protocol writes — pin it
        // by writing through the real commit path, not a hand-built dir.
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(root.join("models")).expect("mkdir");
        let parsed = parsed_d3();
        rocky_core::product::commit::run_phase_a(&root, "products/revenue_daily.toml", &parsed)
            .expect("phase A");
        check_product_collisions(&root, &parsed, "products/revenue_daily.toml").expect("clean");
        let error = check_product_collisions(&root, &parsed, "products/renamed.toml")
            .expect_err("renamed spec file collides");
        assert_eq!(error.code, "duplicate-product-name");
    }

    // ----- the verbs end to end -----

    /// A pass-verification config: the D5 posture plus a pii mask.
    fn passing_config() -> String {
        format!(
            "\n[mask]\npii = \"hash\"\n\n{}",
            paste_block("revenue_daily")
        )
    }

    #[test]
    fn spec_file_name_must_match_its_product_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        // The fixture declares revenue_daily; park it under another name.
        write_file(&root.join("products/misnamed.toml"), SPEC_FIXTURE);
        let error = load_spec(&root, "misnamed").expect_err("mismatch");
        assert_eq!(error.code, "product-name-mismatch");
    }

    #[test]
    fn compile_refuses_until_verification_passes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), "");
        let error =
            product_compile_in(&root, &config, None, "revenue_daily").expect_err("no policy block");
        let message = format!("{error:#}");
        assert!(message.contains("verification did not pass"), "{message}");
        assert!(
            message.contains("[policy]"),
            "the paste block rides along: {message}"
        );
        assert!(
            !root.join("models/revenue_daily.contract.toml").exists(),
            "nothing lowers before verification passes"
        );
    }

    #[test]
    fn compile_runs_phase_a_then_phase_b_and_verifies_bytes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());

        let first = product_compile_in(&root, &config, None, "revenue_daily").expect("phase A");
        assert_eq!(first.phase, "lowered_contract");
        assert_eq!(first.artifacts.len(), 1);
        assert!(root.join("models/revenue_daily.contract.toml").is_file());

        // Draft the model (the worker's half), then compile again → B.
        write_file(&root.join("models/revenue_daily.sql"), b"SELECT 1\n");
        write_file(
            &root.join("models/revenue_daily.toml"),
            b"name = \"revenue_daily\"\n",
        );
        let second = product_compile_in(&root, &config, None, "revenue_daily").expect("phase B");
        assert_eq!(second.phase, "merged");
        assert_eq!(second.artifacts.len(), 1);
        assert_eq!(second.artifacts[0].path, "models/revenue_daily.toml");

        // The committed manifest byte-verifies everything it lists.
        let manifest = rocky_core::product::commit::committed_manifest(&root, "revenue_daily")
            .expect("readable")
            .expect("committed");
        assert!(verify_artifact_hashes(&root, &manifest).is_empty());

        // Compile is idempotent once merged.
        let third = product_compile_in(&root, &config, None, "revenue_daily").expect("again");
        assert_eq!(third.phase, "merged");
    }

    fn temp_state_path(dir: &Path) -> PathBuf {
        dir.join("state.redb")
    }

    /// A project the HTTP tests can serve: the spec fixture, a passing
    /// config, and a pinned state path. Returns `(root, config, state_path)`.
    pub(crate) fn api_fixture_project(dir: &Path) -> (PathBuf, PathBuf, PathBuf) {
        let (root, config) = project_with_config(dir, &passing_config());
        (root, config, temp_state_path(dir))
    }

    #[test]
    fn list_is_empty_for_a_project_without_products() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("project");
        std::fs::create_dir_all(&root).expect("mkdir");

        let out = product_list_in(&root, None).expect("list");
        assert_eq!(out.command, "product_list");
        assert_eq!(out.count, 0);
        assert!(out.products.is_empty());

        // A state path that does not exist yet contributes nothing either.
        let out = product_list_in(&root, Some(&temp_state_path(dir.path()))).expect("list");
        assert_eq!(out.count, 0);
    }

    #[test]
    fn list_projects_status_for_every_spec_and_is_sorted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        // A second spec that sorts first. Its bytes name the other product,
        // so it is refused at parse time and lists with a `spec_error` —
        // the list must carry that row, never drop it.
        write_file(&root.join("products/alpha_daily.toml"), SPEC_FIXTURE);
        // A non-spec file in the directory is not a product.
        write_file(&root.join("products/README.md"), b"not a spec");

        let out = product_list_in(&root, Some(&state_path)).expect("list");
        let names: Vec<&str> = out.products.iter().map(|p| p.name.as_str()).collect();
        assert_eq!(names, ["alpha_daily", "revenue_daily"]);
        assert_eq!(out.count, 2);

        let alpha = &out.products[0];
        assert!(!alpha.spec_present);
        assert!(
            alpha.spec_error.is_some(),
            "the mismatch is reported, not hidden"
        );
        let revenue = &out.products[1];
        assert!(revenue.spec_present);
        assert_eq!(revenue.product_id.as_deref(), Some("product:revenue_daily"));

        // Every row equals the status projection for the same name.
        for row in &out.products {
            let status = product_status_in(&root, Some(&state_path), &row.name).expect("status");
            let projected = ProductListEntry::from_status(status);
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                serde_json::to_value(&projected).unwrap()
            );
        }
    }

    #[test]
    fn list_includes_a_product_only_the_store_knows() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        // The spec file goes away; the store still holds the records.
        std::fs::remove_file(root.join("products/revenue_daily.toml")).expect("remove");

        let out = product_list_in(&root, Some(&state_path)).expect("list");
        assert_eq!(out.count, 1, "{out:?}");
        let row = &out.products[0];
        assert_eq!(row.name, "revenue_daily");
        assert!(!row.spec_present);
        assert!(row.approval.is_some());
        assert_eq!(row.fulfill_state.as_deref(), Some("spec_approved"));
        assert_eq!(row.journal_rows, 1);

        // The name set is the union, deduplicated: one row, not two.
        let names = product_names_in(&root, Some(&state_path)).expect("names");
        assert_eq!(names, ["revenue_daily"]);
    }

    /// Only a regular `*.toml` file is a spec: a directory that happens to
    /// carry the extension, a dangling link, and a non-`.toml` file are not
    /// products.
    #[test]
    fn list_counts_only_regular_toml_files_as_specs() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        std::fs::create_dir_all(root.join("products/not_a_spec.toml")).expect("mkdir");
        write_file(&root.join("products/notes.md"), b"# notes");
        #[cfg(unix)]
        std::os::unix::fs::symlink(
            root.join("products/gone.toml"),
            root.join("products/dangling.toml"),
        )
        .expect("symlink");

        let names = product_names_in(&root, None).expect("names");
        assert_eq!(names, ["revenue_daily"]);
    }

    /// The reads never write: the store file's bytes are the same before
    /// and after a list and a status, and a path that does not exist is
    /// not created.
    #[test]
    fn list_and_status_never_write_the_store() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        let before = std::fs::read(&state_path).expect("store bytes");
        let out = product_list_in(&root, Some(&state_path)).expect("list");
        assert_eq!(out.count, 1);
        let status = product_status_in(&root, Some(&state_path), "revenue_daily").expect("status");
        assert!(status.approval.is_some());
        let after = std::fs::read(&state_path).expect("store bytes");
        assert_eq!(before, after, "a read must not change the store file");

        let absent = dir.path().join("never-created.redb");
        product_list_in(&root, Some(&absent)).expect("list with no store");
        assert!(!absent.exists(), "a read must not create a store");
    }

    #[test]
    fn approve_writes_snapshot_then_records_atomically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let parsed = parsed_d3();

        let output = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        assert!(!output.already_approved);
        assert_eq!(output.spec_digest, parsed.digest);
        assert_eq!(output.state, "spec_approved");
        assert_eq!(output.previous_state, None);

        // The snapshot file holds exactly the approved bytes.
        let snapshot = root.join(&output.snapshot_path);
        assert_eq!(std::fs::read(&snapshot).expect("snapshot"), SPEC_FIXTURE);

        // The records landed together: approval + state + one journal row.
        let store = StateStore::open(&state_path).expect("opens");
        let approval = store
            .product_approval_get("revenue_daily")
            .expect("reads")
            .expect("recorded");
        assert_eq!(approval.spec_digest, parsed.digest);
        assert_eq!(approval.snapshot_path, output.snapshot_path);
        let state = store
            .fulfill_state_get("revenue_daily")
            .expect("reads")
            .expect("recorded");
        assert_eq!(state.state.tag(), "spec_approved");
        assert_eq!(state.journal_seq, 1);
        assert_eq!(
            store
                .fulfill_journal_rows("revenue_daily")
                .expect("reads")
                .len(),
            1
        );
    }

    fn identity(source: crate::output::ApproverSource) -> crate::output::ApproverIdentity {
        crate::output::ApproverIdentity {
            email: "dev@example.com".to_string(),
            name: None,
            host: "host".to_string(),
            source,
        }
    }

    /// The recorded string is the bare email for a local approval, as it
    /// always was, and carries the channel for every other source.
    #[test]
    fn the_approver_string_names_the_channel_unless_it_is_local() {
        use crate::output::ApproverSource;
        assert_eq!(
            approver_label(identity(ApproverSource::Local)),
            "dev@example.com"
        );
        assert_eq!(
            approver_label(identity(ApproverSource::HttpApi)),
            "dev@example.com (http_api)"
        );
        assert_eq!(
            approver_label(identity(ApproverSource::CiOidc)),
            "dev@example.com (ci_oidc)"
        );
        assert_eq!(
            approver_label(identity(ApproverSource::Pat)),
            "dev@example.com (pat)"
        );
    }

    /// Product approve reads the channel through `review_approver`, the
    /// function `rocky review --approve` uses: the HTTP API source is labelled
    /// and a terminal source is not.
    #[test]
    fn product_approve_labels_the_channel_the_way_review_does() {
        use crate::commands::review::review_approver;
        use rocky_core::state::SessionSource;
        let for_source = |source| {
            approver_label(
                review_approver(source, Ok(identity(crate::output::ApproverSource::Local)))
                    .expect("resolved"),
            )
        };
        assert_eq!(
            for_source(SessionSource::HttpApi),
            "dev@example.com (http_api)"
        );
        for source in [
            SessionSource::Cli,
            SessionSource::Dagster,
            SessionSource::Lsp,
        ] {
            assert_eq!(for_source(source), "dev@example.com", "{source:?}");
        }
    }

    /// The production function reads `ROCKY_SESSION_SOURCE` itself. Which
    /// answer it gives depends on this machine's git identity, so the test
    /// asserts the branch that identity selects, and both branches assert
    /// something.
    #[test]
    fn the_production_approver_reads_the_session_source() {
        use crate::commands::review::APPROVER_IDENTITY_UNRESOLVED;
        let _env = crate::testing::lock_pipes_env();
        // SAFETY: serialized by the pipes-env lock, which every reader of
        // ROCKY_SESSION_SOURCE takes; restored below.
        let previous = std::env::var_os("ROCKY_SESSION_SOURCE");
        unsafe { std::env::set_var("ROCKY_SESSION_SOURCE", "http_api") };
        let over_http = approver_string();
        unsafe { std::env::set_var("ROCKY_SESSION_SOURCE", "cli") };
        let in_a_terminal = approver_string();
        unsafe {
            match previous {
                Some(v) => std::env::set_var("ROCKY_SESSION_SOURCE", v),
                None => std::env::remove_var("ROCKY_SESSION_SOURCE"),
            }
        }
        match crate::commands::branch::approver_identity_pub() {
            Ok(id) if !id.email.trim().eq_ignore_ascii_case("unknown") => {
                assert_eq!(
                    over_http.expect("resolved"),
                    format!("{} (http_api)", id.email)
                );
                assert_eq!(in_a_terminal.expect("resolved"), id.email);
            }
            _ => {
                let error = over_http.expect_err("no usable identity refuses over the API");
                assert!(
                    format!("{error:#}").contains(APPROVER_IDENTITY_UNRESOLVED),
                    "{error:#}"
                );
                assert!(
                    in_a_terminal.is_ok(),
                    "a terminal keeps recording `unknown`"
                );
            }
        }
    }

    /// An HTTP API approval is recorded with its channel everywhere the
    /// approver string shows: the command output, the stored record and the
    /// status read. A terminal approval is recorded bare.
    #[test]
    fn an_http_api_approval_is_recorded_with_its_channel() {
        use crate::commands::review::review_approver;
        use rocky_core::state::SessionSource;
        let approve_as = |source: SessionSource| {
            let dir = tempfile::tempdir().expect("tempdir");
            let (root, _config) = project_with_config(dir.path(), &passing_config());
            let state_path = temp_state_path(dir.path());
            let approver = move || {
                Ok(approver_label(review_approver(
                    source,
                    Ok(identity(crate::output::ApproverSource::Local)),
                )?))
            };
            let output = product_approve_with(&root, &state_path, "revenue_daily", None, &approver)
                .expect("approves");
            let stored = StateStore::open(&state_path)
                .expect("opens")
                .product_approval_get("revenue_daily")
                .expect("reads")
                .expect("recorded")
                .approver;
            let status = product_status_in(&root, Some(&state_path), "revenue_daily")
                .expect("status")
                .approval
                .expect("approved")
                .approver;
            assert_eq!(output.approver, stored);
            assert_eq!(output.approver, status);
            output.approver
        };
        assert_eq!(
            approve_as(SessionSource::HttpApi),
            "dev@example.com (http_api)"
        );
        assert_eq!(approve_as(SessionSource::Cli), "dev@example.com");
    }

    /// An HTTP API approval whose server has no usable git identity refuses
    /// BEFORE anything is written: no snapshot file, no store, no record.
    /// (The CLI keeps recording `unknown` for a terminal approval.)
    #[test]
    fn an_http_api_approval_without_an_identity_writes_nothing() {
        use crate::commands::review::{APPROVER_IDENTITY_UNRESOLVED, review_approver};
        use rocky_core::state::SessionSource;
        for identity_result in [
            Err(anyhow::anyhow!("git is not configured")),
            Ok(crate::output::ApproverIdentity {
                email: "unknown".to_string(),
                ..identity(crate::output::ApproverSource::Local)
            }),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (root, _config) = project_with_config(dir.path(), &passing_config());
            let state_path = temp_state_path(dir.path());
            let shared = std::cell::RefCell::new(Some(identity_result));
            let approver = || {
                let identity = shared.borrow_mut().take().expect("called once");
                Ok(approver_label(review_approver(
                    SessionSource::HttpApi,
                    identity,
                )?))
            };
            let error = product_approve_with(&root, &state_path, "revenue_daily", None, &approver)
                .expect_err("refuses");
            assert!(
                format!("{error:#}").contains(APPROVER_IDENTITY_UNRESOLVED),
                "{error:#}"
            );
            let snapshot = root.join(approval_snapshot_rel("revenue_daily", &parsed_d3().digest));
            assert!(!snapshot.exists(), "no snapshot was written");
            let recorded = StateStore::open(&state_path)
                .expect("opens")
                .product_approval_get("revenue_daily")
                .expect("reads");
            assert!(recorded.is_none(), "no approval was recorded");
        }
    }

    /// `--expect-digest`: the approval covers exactly the spec the caller
    /// confirmed. A different working spec refuses BEFORE anything is written
    /// (no store, no snapshot), naming both digests; the matching digest
    /// approves; no digest approves what is on disk, as before.
    #[test]
    fn an_expected_digest_that_does_not_match_writes_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let digest = parsed_d3().digest;
        let wrong = format!("sha256:{}", "0".repeat(64));

        let error = product_approve_expecting(&root, &state_path, "revenue_daily", Some(&wrong))
            .expect_err("a different digest refuses");
        let message = format!("{error:#}");
        assert!(message.contains("approval-digest-mismatch"), "{message}");
        assert!(
            message.contains(&digest) && message.contains(&wrong),
            "{message}"
        );
        assert!(!state_path.exists(), "no store was opened");
        assert!(
            !root
                .join(approval_snapshot_rel("revenue_daily", &digest))
                .exists(),
            "no snapshot was written"
        );
    }

    #[test]
    fn an_expected_digest_that_matches_approves_that_spec() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let digest = parsed_d3().digest;

        let output = product_approve_expecting(&root, &state_path, "revenue_daily", Some(&digest))
            .expect("the matching digest approves");
        assert_eq!(output.spec_digest, digest);
        assert!(!output.already_approved);
        let stored = StateStore::open(&state_path)
            .expect("opens")
            .product_approval_get("revenue_daily")
            .expect("reads")
            .expect("recorded");
        assert_eq!(stored.spec_digest, digest);
        // No flag: the spec on disk is approved, as before (here a no-op).
        let again = product_approve_expecting(&root, &state_path, "revenue_daily", None)
            .expect("no flag still approves");
        assert!(again.already_approved);
    }

    /// The digest is compared before the "already approved" short-cut too: a
    /// stale digest never reports success.
    #[test]
    fn a_stale_expected_digest_is_refused_even_when_the_spec_is_already_approved() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        let wrong = format!("sha256:{}", "1".repeat(64));
        let error = product_approve_expecting(&root, &state_path, "revenue_daily", Some(&wrong))
            .expect_err("refuses");
        assert!(format!("{error:#}").contains("approval-digest-mismatch"));
    }

    #[test]
    fn approving_the_same_digest_twice_is_a_no_op() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());

        let first = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        let second = product_approve_in(&root, &state_path, "revenue_daily").expect("no-op");
        assert!(second.already_approved);
        assert_eq!(second.spec_digest, first.spec_digest);
        let store = StateStore::open(&state_path).expect("opens");
        assert_eq!(
            store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded")
                .journal_seq,
            1,
            "a no-op re-approve appends no journal row"
        );
    }

    /// #1493 F3: `compile` picks its phase from what is on disk, so a
    /// concurrent run during the loop's drafting window would re-merge
    /// and re-pin the PREVIOUS round's sidecar — making the loop's own
    /// repair look like tamper again. It refuses for a foreign owner,
    /// the same shape the D2 approve guard uses.
    #[test]
    fn compiling_a_product_a_live_loop_owns_refuses() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let row = |event: &str, to: &str| FulfillJournalRow {
            seq: 0,
            at: None,
            event: event.to_string(),
            from_state: None,
            to_state: to.to_string(),
            spec_digest: None,
            plan_id: None,
            idempotency_key: None,
        };

        // Each case is a DIFFERENT in-flight stamp, and each must refuse
        // on its own.
        for (why, owner_pid, driver_pgid, expect) in [
            (
                "a live loop owns the record",
                Some(31337u32),
                None,
                "pid 31337",
            ),
            (
                "only a worker group is stamped",
                None,
                Some(4242u32),
                "pgid 4242",
            ),
        ] {
            let dir = tempfile::tempdir().expect("tempdir");
            let (root, config) = project_with_config(dir.path(), &passing_config());
            let state_path = temp_state_path(dir.path());
            product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

            {
                let store = StateStore::open(&state_path).expect("opens");
                let observed = store
                    .fulfill_state_get("revenue_daily")
                    .expect("reads")
                    .expect("recorded");
                let mut drafting = observed.clone();
                drafting.state = FulfillState::Drafting;
                drafting.owner_pid = owner_pid;
                drafting.owner_start_time = owner_pid.map(|_| 1);
                drafting.driver_pgid = driver_pgid;
                store
                    .fulfill_state_cas(
                        "revenue_daily",
                        Some(&observed),
                        &drafting,
                        &row("drafting", "drafting"),
                    )
                    .expect("cas");
            }

            let err = product_compile_in(&root, &config, Some(&state_path), "revenue_daily")
                .expect_err(why);
            let rendered = format!("{err:#}");
            assert!(
                rendered.contains("[compile-refused-in-flight]"),
                "{why}: {rendered}"
            );
            assert!(
                rendered.contains(expect),
                "{why}: names who owns it: {rendered}"
            );
            assert!(
                rendered.contains("rocky fulfill revenue_daily"),
                "{why}: a stale stamp is recoverable, and the refusal must say how: \
                 {rendered}"
            );

            // Nothing was compiled: no manifest, no artifacts.
            assert!(
                committed_manifest(&root, "revenue_daily")
                    .expect("reads")
                    .is_none(),
                "{why}: a refusal must not commit a generation"
            );
            assert!(
                !root.join("models/revenue_daily.contract.toml").exists(),
                "{why}: a refusal must write no artifacts"
            );
        }
    }

    /// The other half of F3: a process compiling a product IT owns is
    /// not refused. That is the loop's own Phase A / Phase B step, and a
    /// guard that blocked it would deadlock the loop at Phase A.
    ///
    /// The stamp here is this process's REAL identity — its pid and the
    /// start time the OS reports — because that is the only thing the
    /// guard accepts. The record also carries a live worker group, so
    /// this pins that owning the record is what passes, not the absence
    /// of other stamps.
    #[test]
    fn the_owning_process_compiles_its_own_product() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        let me = std::process::id();
        let my_start = rocky_core::process::process_liveness(me)
            .expect("probe this process")
            .expect("this process is alive");

        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut drafting = observed.clone();
            drafting.state = FulfillState::Drafting;
            drafting.owner_pid = Some(me);
            drafting.owner_start_time = Some(my_start);
            drafting.driver_pgid = Some(4242);
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &drafting,
                    &FulfillJournalRow {
                        seq: 0,
                        at: None,
                        event: "drafting".to_string(),
                        from_state: None,
                        to_state: "drafting".to_string(),
                        spec_digest: None,
                        plan_id: None,
                        idempotency_key: None,
                    },
                )
                .expect("cas");
        }

        let output = product_compile_in(&root, &config, Some(&state_path), "revenue_daily")
            .expect("the owning process's own compile proceeds");
        assert_eq!(output.phase, "lowered_contract");
    }

    /// R2-1: the PUBLIC façade must not hand out the loop's exemption.
    ///
    /// An earlier fix passed a "who is asking" argument and
    /// `fulfill_api::product_compile` hard-coded the loop's variant, so
    /// every caller of that public entry inherited the loop's identity
    /// and walked past the guard. There is no such argument now — the
    /// guard reads this process's real identity — so the façade and the
    /// CLI verb are guarded identically. This calls the façade against a
    /// record owned by ANOTHER process and requires a refusal.
    #[test]
    fn the_public_facade_does_not_grant_the_loops_exemption() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut drafting = observed.clone();
            drafting.state = FulfillState::Drafting;
            drafting.owner_pid = Some(31337);
            drafting.owner_start_time = Some(1);
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &drafting,
                    &FulfillJournalRow {
                        seq: 0,
                        at: None,
                        event: "drafting".to_string(),
                        from_state: None,
                        to_state: "drafting".to_string(),
                        spec_digest: None,
                        plan_id: None,
                        idempotency_key: None,
                    },
                )
                .expect("cas");
        }

        // The façade — the entry the fulfillment loop consumes, and the
        // one that previously exempted whoever called it.
        let err = crate::commands::fulfill_api::product_compile(
            &root,
            &config,
            Some(&state_path),
            "revenue_daily",
        )
        .expect_err("the façade must be guarded exactly like the CLI verb");
        assert!(
            format!("{err:#}").contains("[compile-refused-in-flight]"),
            "{err:#}"
        );
        assert!(
            committed_manifest(&root, "revenue_daily")
                .expect("reads")
                .is_none(),
            "a refusal must not commit a generation"
        );
    }

    /// The PID-reuse hole the start-time pairing closes (#1493, red-team
    /// finding 2). A crashed loop leaves its pid on the record. An
    /// unrelated `rocky product compile` that later recycles that exact
    /// pid must STILL be refused — ownership is not pid equality.
    ///
    /// The stamp is this test process's own pid, which is precisely the
    /// collision a pid-comparing guard would read as "this is mine".
    #[test]
    fn a_recycled_pid_does_not_buy_ownership_of_a_compile() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut drafting = observed.clone();
            drafting.state = FulfillState::Drafting;
            drafting.owner_pid = Some(std::process::id());
            // A DIFFERENT start time: the stamp is a dead process that
            // happened to hold this pid, not us.
            drafting.owner_start_time = Some(1);
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &drafting,
                    &FulfillJournalRow {
                        seq: 0,
                        at: None,
                        event: "drafting".to_string(),
                        from_state: None,
                        to_state: "drafting".to_string(),
                        spec_digest: None,
                        plan_id: None,
                        idempotency_key: None,
                    },
                )
                .expect("cas");
        }

        let err = product_compile_in(&root, &config, Some(&state_path), "revenue_daily")
            .expect_err("a matching pid must not be read as ownership");
        assert!(
            format!("{err:#}").contains("[compile-refused-in-flight]"),
            "{err:#}"
        );
        assert!(
            committed_manifest(&root, "revenue_daily")
                .expect("reads")
                .is_none(),
            "a refusal must not commit a generation"
        );
    }

    /// D2: a NEW-digest approval must refuse while fulfillment work is
    /// in flight — replacing the record would orphan a live worker
    /// group (`driver_pgid` dropped) and strand an in-flight apply (the
    /// pinned idempotency key dropped). Approval proceeds only from the
    /// stop states.
    #[test]
    fn approving_a_new_digest_over_in_flight_work_refuses() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let first = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        let row = |event: &str, to: &str| FulfillJournalRow {
            seq: 0,
            at: None,
            event: event.to_string(),
            from_state: None,
            to_state: to.to_string(),
            spec_digest: None,
            plan_id: None,
            idempotency_key: None,
        };

        // Drive the record into `drafting` with a live worker group
        // stamped, the way a running loop would hold it.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut drafting = observed.clone();
            drafting.state = FulfillState::Drafting;
            drafting.drafting_attempts = 1;
            drafting.driver_pgid = Some(4242);
            drafting.driver_leader_start_time = Some(1);
            let outcome = store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &drafting,
                    &row("drafting attempt 1", "drafting"),
                )
                .expect("cas");
            assert_eq!(outcome, rocky_core::fulfill::FulfillCas::Won);
        }

        // A spec edit makes a NEW digest; approving it now must refuse.
        let spec_path = root.join("products/revenue_daily.toml");
        let mut edited = std::fs::read_to_string(&spec_path).expect("read");
        edited.push_str("\n# reviewer note\n");
        std::fs::write(&spec_path, &edited).expect("edit");
        let err = product_approve_in(&root, &state_path, "revenue_daily")
            .expect_err("in-flight approval must refuse");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("[approval-refused-in-flight]"),
            "{rendered}"
        );
        assert!(rendered.contains("pgid 4242"), "{rendered}");

        // Nothing was written: the approval still names the FIRST digest
        // and the drafting record (with its worker stamp) is untouched.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let approval = store
                .product_approval_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            assert_eq!(approval.spec_digest, first.spec_digest);
            let state = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            assert_eq!(state.state.tag(), "drafting");
            assert_eq!(state.driver_pgid, Some(4242));
        }

        // `applying` (no worker group, key pinned) refuses too, naming
        // the state.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut applying = observed.clone();
            applying.state = FulfillState::Applying;
            applying.driver_pgid = None;
            applying.driver_leader_start_time = None;
            applying.idempotency_key = Some("product:revenue_daily@sha:a@7".to_string());
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &applying,
                    &row("applying", "applying"),
                )
                .expect("cas");
        }
        let err = product_approve_in(&root, &state_path, "revenue_daily")
            .expect_err("approval over applying must refuse");
        assert!(
            format!("{err:#}").contains("'applying'"),
            "names the state: {err:#}"
        );

        // From a STOP state the same edited spec approves fine, and the
        // supersession fence replaces the record.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut waiting = observed.clone();
            waiting.state = FulfillState::NeedsInput {
                reason: "plan_approval".to_string(),
                payload: "plan-1".to_string(),
            };
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &waiting,
                    &row("awaiting plan review", "needs_input"),
                )
                .expect("cas");
        }
        let output =
            product_approve_in(&root, &state_path, "revenue_daily").expect("a stop state approves");
        assert!(!output.already_approved);
        assert_ne!(output.spec_digest, first.spec_digest);
        assert_eq!(output.previous_state.as_deref(), Some("needs_input"));

        // The SAME-digest idempotent re-approve stays allowed even over
        // in-flight work — it writes nothing.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut drafting = observed.clone();
            drafting.state = FulfillState::Drafting;
            drafting.drafting_attempts = 1;
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &drafting,
                    &row("drafting attempt 1", "drafting"),
                )
                .expect("cas");
        }
        let unchanged = product_approve_in(&root, &state_path, "revenue_daily")
            .expect("same-digest re-approve writes nothing");
        assert!(unchanged.already_approved);
    }

    /// The custody hold's remedy depends on THIS: `applied` refuses an
    /// approval, `observing` accepts one.
    ///
    /// A post-apply check-custody divergence lands the product back at
    /// `applied` (`machine::decide_observation_checks`). The stop tells
    /// the operator they may keep their edit by putting it in the spec
    /// and approving it — and that second half cannot be run from the
    /// state it is printed in, because `applied` is in the in-flight
    /// refusal set above. So the message has to state an ORDER: restore,
    /// re-run until the loop leaves `applied`, and only then approve.
    ///
    /// This test is the ground under that ordering. It is deliberately
    /// not a reading of the `matches!` list — that list is the thing
    /// being checked, and asserting it against itself would prove
    /// nothing. Both halves go through the real verb.
    #[test]
    fn approving_refuses_at_applied_and_permits_at_observing() {
        use rocky_core::fulfill::{FulfillJournalRow, FulfillState};

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let first = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");

        // Drive the record to `state`, the way the loop would.
        let put = |state: FulfillState| {
            let store = StateStore::open(&state_path).expect("opens");
            let observed = store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            let mut next = observed.clone();
            next.state = state.clone();
            next.driver_pgid = None;
            next.driver_leader_start_time = None;
            store
                .fulfill_state_cas(
                    "revenue_daily",
                    Some(&observed),
                    &next,
                    &FulfillJournalRow {
                        seq: 0,
                        at: None,
                        event: format!("test: to {}", state.tag()),
                        from_state: None,
                        to_state: state.tag().to_string(),
                        spec_digest: None,
                        plan_id: None,
                        idempotency_key: None,
                    },
                )
                .expect("cas");
        };

        // A spec edit makes a NEW digest — the case the remedy describes.
        let spec_path = root.join("products/revenue_daily.toml");
        let edited = format!(
            "{}\n# a check the operator wants to keep\n",
            std::fs::read_to_string(&spec_path).expect("read")
        );
        std::fs::write(&spec_path, &edited).expect("edit");

        // HALF ONE — `applied` refuses. This is the state a first custody
        // divergence lands in, so the printed "approve the spec again"
        // cannot be executed there.
        put(FulfillState::Applied);
        let err = product_approve_in(&root, &state_path, "revenue_daily")
            .expect_err("approving from `applied` must refuse");
        let rendered = format!("{err:#}");
        assert!(
            rendered.contains("[approval-refused-in-flight]"),
            "the refusal is the in-flight one: {rendered}"
        );
        assert!(
            rendered.contains("'applied'"),
            "and it names the state the operator is standing in: {rendered}"
        );
        // Nothing moved: the approval still names the first digest.
        {
            let store = StateStore::open(&state_path).expect("opens");
            let approval = store
                .product_approval_get("revenue_daily")
                .expect("reads")
                .expect("recorded");
            assert_eq!(approval.spec_digest, first.spec_digest);
        }

        // HALF TWO — `observed_failing` accepts.
        //
        // Driven FIRST, and driven at all, because the custody stop names
        // it: a restore + re-run lands `observing` when the checks pass
        // and `observed_failing` when one is genuinely red, and the red
        // path is the common one. Reading it out of the `matches!` list
        // would prove nothing — that list is the thing under test. If
        // this refused, the printed sequence would strand every operator
        // whose checks are actually failing.
        put(FulfillState::ObservedFailing);
        let from_red = product_approve_in(&root, &state_path, "revenue_daily")
            .expect("approving from `observed_failing` must be permitted");
        assert!(!from_red.already_approved);
        assert_eq!(from_red.previous_state.as_deref(), Some("observed_failing"));
        assert_ne!(
            from_red.spec_digest, first.spec_digest,
            "the edited spec is the newly approved revision"
        );

        // HALF THREE — `observing` accepts too. That is where the restore
        // + re-run puts a product whose checks pass, and it is what makes
        // the ordering the remedy prints a working sequence rather than a
        // suggestion.
        //
        // A second edit is needed: the approval above already adopted the
        // first one, so re-approving the same digest would take the
        // idempotent path and prove nothing about the state gate.
        std::fs::write(&spec_path, format!("{edited}\n# a second reviewer note\n"))
            .expect("edit again");
        put(FulfillState::Observing);
        let output = product_approve_in(&root, &state_path, "revenue_daily")
            .expect("approving from `observing` must be permitted");
        assert!(!output.already_approved);
        assert_ne!(
            output.spec_digest, from_red.spec_digest,
            "the second edit is a new revision, so this is a real approval"
        );
        assert_eq!(output.previous_state.as_deref(), Some("observing"));
    }

    #[test]
    fn a_crash_between_snapshot_and_transaction_leaves_only_the_orphan_file() {
        // The E4 crash drill at the file/txn boundary: run ONLY the first
        // half (the snapshot write). The world must show the orphan file
        // and NOTHING in the store — harmless, GC-able, and a re-run
        // completes the approval over it.
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let parsed = parsed_d3();

        let snapshot_rel = write_approval_snapshot(&root, &parsed).expect("snapshot");
        assert!(root.join(&snapshot_rel).is_file());
        let store = StateStore::open(&state_path).expect("opens");
        assert!(
            store
                .product_approval_get("revenue_daily")
                .expect("reads")
                .is_none()
        );
        assert!(
            store
                .fulfill_state_get("revenue_daily")
                .expect("reads")
                .is_none()
        );
        drop(store);

        // The re-run completes over the orphan (never overwriting it).
        let output = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        assert_eq!(output.snapshot_path, snapshot_rel);
        assert!(!output.already_approved);
    }

    #[cfg(unix)]
    #[test]
    fn approve_refuses_a_symlinked_snapshot_temp_and_leaves_it_untouched() {
        // The same class as the commit staging vector: `write_approval_snapshot`
        // stages the approved bytes into `approved-<hex>.toml.tmp`. A plain
        // `std::fs::write` there follows an attacker-planted symlink out of the
        // project and writes the spec bytes through it. Plant the link, approve,
        // assert refusal and an untouched target.
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let secret = dir.path().join("outside-secret");
        std::fs::write(&secret, b"bytes the approval temp must never overwrite").expect("write");

        let parsed = parsed_d3();
        // The snapshot temp is `<snapshot>.toml.tmp` — `with_extension`
        // replaces the final `.toml`, exactly as the writer derives it.
        let snapshot = root.join(approval_snapshot_rel("revenue_daily", &parsed.digest));
        let tmp = snapshot.with_extension("toml.tmp");
        std::fs::create_dir_all(tmp.parent().expect("parent")).expect("mkdir");
        std::os::unix::fs::symlink(&secret, &tmp).expect("symlink");

        let error = product_approve_in(&root, &state_path, "revenue_daily")
            .expect_err("symlinked snapshot temp");
        assert!(
            format!("{error:#}").contains("approval-snapshot-tampered"),
            "{error:#}"
        );
        assert_eq!(
            std::fs::read(&secret).expect("still there"),
            b"bytes the approval temp must never overwrite",
            "the out-of-project target must be untouched"
        );
        // Nothing was recorded — the transition never reached the store.
        let store = StateStore::open(&state_path).expect("opens");
        assert!(
            store
                .product_approval_get("revenue_daily")
                .expect("reads")
                .is_none()
        );
    }

    #[cfg(unix)]
    #[test]
    fn approve_refuses_a_symlinked_state_dir_and_leaves_the_out_of_tree_target_untouched() {
        // BLOCKER #2 for approve — the ANCESTOR attack: a symlinked STATE DIR
        // (`.rocky/fulfillment/<name> -> /outside`) redirects the snapshot
        // temp write, its `remove_file`, and its `create_new` retry out of the
        // project THROUGH the symlinked parent, with no race and no symlink at
        // the leaf. Under the leaf-only/O_EXCL-final guard, the `create_new`
        // AlreadyExists path would `remove_file` the out-of-tree victim.
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());
        let parsed = parsed_d3();

        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).expect("mkdir");
        // The victim sits exactly where the temp write would resolve through
        // the symlinked state dir.
        let hex = parsed
            .digest
            .strip_prefix("sha256:")
            .unwrap_or(&parsed.digest);
        let victim = outside.join(format!("approved-{hex}.toml.tmp"));
        std::fs::write(
            &victim,
            b"an out-of-tree file the symlinked state dir points into",
        )
        .expect("write");
        // Plant the symlinked state dir (its parent `.rocky/fulfillment` is a
        // real dir; only the product's state dir is the link).
        let state_parent = root.join(".rocky").join("fulfillment");
        std::fs::create_dir_all(&state_parent).expect("mkdir");
        std::os::unix::fs::symlink(&outside, state_parent.join("revenue_daily")).expect("symlink");

        let error = product_approve_in(&root, &state_path, "revenue_daily")
            .expect_err("symlinked state dir");
        assert!(
            format!("{error:#}").contains("approval-snapshot-tampered"),
            "{error:#}"
        );
        assert!(
            format!("{error:#}").contains("escapes the project root"),
            "the refusal names the ancestor escape: {error:#}"
        );
        assert_eq!(
            std::fs::read(&victim).expect("still there"),
            b"an out-of-tree file the symlinked state dir points into",
            "the out-of-project file behind the symlinked state dir must be untouched"
        );
        let store = StateStore::open(&state_path).expect("opens");
        assert!(
            store
                .product_approval_get("revenue_daily")
                .expect("reads")
                .is_none()
        );
    }

    #[test]
    fn a_tampered_snapshot_refuses_re_approval_and_compile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());

        let output = product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        write_file(&root.join(&output.snapshot_path), b"tampered bytes");

        let error =
            product_approve_in(&root, &state_path, "revenue_daily").expect_err("tampered snapshot");
        assert!(
            format!("{error:#}").contains("approval-snapshot-tampered"),
            "{error:#}"
        );

        // Compile is a reader of the approval and refuses the same way.
        let error = product_compile_in(&root, &config, Some(&state_path), "revenue_daily")
            .expect_err("tampered snapshot");
        assert!(
            format!("{error:#}").contains("approval-snapshot-tampered"),
            "{error:#}"
        );
    }

    #[test]
    fn re_approving_a_new_digest_supersedes_and_journals() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());

        product_approve_in(&root, &state_path, "revenue_daily").expect("approves v1");

        // Edit the spec (a new revision) and approve again.
        let edited = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace("revenue_eur >= 0", "revenue_eur > 0");
        write_file(&root.join("products/revenue_daily.toml"), edited.as_bytes());
        let output = product_approve_in(&root, &state_path, "revenue_daily").expect("approves v2");
        assert!(!output.already_approved);
        assert_eq!(output.previous_state.as_deref(), Some("spec_approved"));

        let store = StateStore::open(&state_path).expect("opens");
        let rows = store.fulfill_journal_rows("revenue_daily").expect("reads");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[1].seq, 2);
        // Both snapshots exist — digest-addressed files are never
        // overwritten, so the history of approved bytes accumulates.
        let state_dir = root.join(".rocky/fulfillment/revenue_daily");
        let snapshots = std::fs::read_dir(&state_dir)
            .expect("state dir")
            .flatten()
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("approved-"))
            .count();
        assert_eq!(snapshots, 2);
    }

    #[test]
    fn status_reports_the_whole_surface() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        let state_path = temp_state_path(dir.path());

        // Cold: spec parses, nothing else exists.
        let cold = product_status_in(&root, Some(&state_path), "revenue_daily").expect("status");
        assert!(cold.spec_present);
        assert_eq!(cold.committed_phase, None);
        assert!(cold.approval.is_none());
        assert!(!cold.staging_journal_present);

        // After approve + compile: everything reported and intact.
        product_approve_in(&root, &state_path, "revenue_daily").expect("approves");
        product_compile_in(&root, &config, Some(&state_path), "revenue_daily").expect("compiles");
        let status = product_status_in(&root, Some(&state_path), "revenue_daily").expect("status");
        assert_eq!(status.committed_phase.as_deref(), Some("lowered_contract"));
        assert!(status.artifact_problems.is_empty());
        assert_eq!(status.snapshot_intact, Some(true));
        assert_eq!(status.spec_matches_approval, Some(true));
        assert_eq!(status.fulfill_state.as_deref(), Some("spec_approved"));
        assert_eq!(status.journal_rows, 1);

        // Tamper with the committed contract: status names the drift.
        let contract = root.join("models/revenue_daily.contract.toml");
        let tampered = std::fs::read_to_string(&contract)
            .expect("contract")
            .replace("Int64", "String");
        write_file(&contract, tampered.as_bytes());
        let status = product_status_in(&root, Some(&state_path), "revenue_daily").expect("status");
        assert_eq!(status.artifact_problems.len(), 1);
        assert!(status.artifact_problems[0].contains("drift"));
    }

    #[test]
    fn the_lowered_artifacts_pass_the_real_engine() {
        // The answer key drove a subprocess `rocky` binary; here the
        // in-process compiler IS that engine, so the probe battery runs
        // directly. Emission correctness is proven by the CONSUMER acting
        // on it: the contract surfaces as targeted E010/E011 diagnostics
        // when violated, the merged [freshness] clears W005, [mask]
        // resolution keeps W004 silent, and the product tag lands on the
        // compiled model.
        use rocky_compiler::compile::{self, CompileError, CompilerConfig};
        use rocky_compiler::types::TypedColumn;
        use rocky_core::product::commit::{run_phase_a, run_phase_b};
        use rocky_ir::RockyType;

        // `revenue_eur` is NOT NULL in the contract. A bare `CAST(SUM(..))`
        // is nullable (SUM over no rows is NULL), so the worker states the
        // default; before #2295 lineage wrongly read the cast as NOT NULL.
        // A SUM over DECIMAL(18,2) has warehouse-dependent digits (#2356),
        // so a cast of it to DECIMAL(18,2) can overflow and return NULL on
        // some warehouses (#2299). The cast goes inside the COALESCE, whose
        // non-null default then replaces that NULL: provably NOT NULL.
        const WORKER_SQL: &str = "SELECT\n    client_id,\n    charged_on AS date,\n    \
             COALESCE(CAST(SUM(amount_eur) AS DECIMAL(18,2)), CAST(0 AS DECIMAL(18,2))) AS revenue_eur\n\
             FROM raw.stripe_charges\n\
             WHERE NOT refunded\nGROUP BY client_id, charged_on\n";
        const DRAFT_SIDECAR: &str = "name = \"revenue_daily\"\nintent = \"Daily gross revenue \
             per client in EUR, refunds excluded\"\n";

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config_path) = project_with_config(dir.path(), &passing_config());
        write_file(
            &root.join("models/_defaults.toml"),
            b"[target]\ncatalog = \"poc\"\nschema = \"gold\"\n",
        );
        let parsed = parsed_d3();

        // The seeded source's typed columns — the in-process stand-in for
        // the answer key's `--with-seed` DuckDB compile.
        let source_columns = [
            ("charge_id", RockyType::Int64),
            ("client_id", RockyType::Int64),
            ("charged_on", RockyType::Date),
            (
                "amount_eur",
                RockyType::Decimal {
                    precision: 18,
                    scale: 2,
                },
            ),
            ("refunded", RockyType::Boolean),
        ];
        let compile_probe = |root: &Path| {
            let cfg = rocky_core::config::load_rocky_config(&config_path).expect("config");
            let mut source_schemas = std::collections::HashMap::new();
            source_schemas.insert(
                "raw.stripe_charges".to_string(),
                source_columns
                    .iter()
                    .map(|(name, ty)| TypedColumn {
                        name: (*name).to_string(),
                        data_type: ty.clone(),
                        nullable: false,
                    })
                    .collect::<Vec<_>>(),
            );
            compile::compile(&CompilerConfig {
                target_dialects: Default::default(),
                strict_contracts: false,
                models_dir: root.join("models"),
                contracts_dir: None,
                required_explicit_contract_model: None,
                source_schemas,
                mask: cfg.mask.clone(),
                allow_unmasked: cfg.classifications.allow_unmasked.clone(),
                project_freshness: cfg.freshness.clone(),
                run_vars: rocky_core::run_vars::RunVars::new(),
                source_provenance: Default::default(),
                preserve_authored_sql: false,
                external_dependencies: Default::default(),
                project: None,
            })
        };
        let diagnostics = |result: &compile::CompileResult, code: &str| -> Vec<String> {
            result
                .diagnostics
                .iter()
                .filter(|d| &*d.code == code)
                .map(|d| d.message.to_string())
                .collect()
        };

        // Posture + tag resolution on the real config (checks 1+2 against
        // the real evaluator).
        let (_, posture) =
            product_verify_outcome(&root, &config_path, "revenue_daily").expect("verifies");
        assert_eq!(posture.status, VerifyStatus::Pass, "{}", posture.reason);

        // Phase A: the contract alone. No model exists yet, so compile
        // refuses on emptiness — the orphan contract itself must not add
        // any failure.
        run_phase_a(&root, "products/revenue_daily.toml", &parsed).expect("phase A");
        let err = match compile_probe(&root) {
            Err(err) => err,
            Ok(_) => panic!("an empty models dir must refuse to compile"),
        };
        assert!(
            matches!(
                err,
                CompileError::Project(rocky_compiler::project::ProjectError::NoModels { .. })
            ),
            "{err}"
        );

        // The worker drafts: SQL + the draft_model-shaped sidecar (name +
        // intent only; target resolves from _defaults.toml).
        write_file(
            &root.join("models/revenue_daily.sql"),
            WORKER_SQL.as_bytes(),
        );
        write_file(
            &root.join("models/revenue_daily.toml"),
            DRAFT_SIDECAR.as_bytes(),
        );
        let result = compile_probe(&root).expect("compiles");
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert!(
            result.project.models[0].contract_path.is_some(),
            "the engine DISCOVERED the lowered contract"
        );

        // Probe 1: drop a required column → E010 naming exactly the
        // contract column the lowering emitted.
        write_file(
            &root.join("models/revenue_daily.sql"),
            b"SELECT client_id, charged_on AS date FROM raw.stripe_charges\n",
        );
        let result = compile_probe(&root).expect("compiles with diagnostics");
        let e010 = diagnostics(&result, "E010");
        assert!(
            e010.iter().any(|m| m.contains("revenue_eur")),
            "E010 names the dropped contract column: {e010:?}"
        );

        // Probe 2: break a declared type → E011 naming the Decimal
        // expectation from the [[columns]] entry. The wrong column carries
        // a KNOWN inferred type (a seeded source column) — an
        // Unknown-typed expression matches any contract type by design.
        write_file(
            &root.join("models/revenue_daily.sql"),
            b"SELECT client_id, charged_on AS date, charged_on AS revenue_eur\n\
              FROM raw.stripe_charges\n",
        );
        let result = compile_probe(&root).expect("compiles with diagnostics");
        let e011 = diagnostics(&result, "E011");
        assert!(
            e011.iter()
                .any(|m| m.contains("Decimal") && m.contains("revenue_eur")),
            "E011 names the declared Decimal: {e011:?}"
        );

        // Restore the good draft; before the merge W005 fires (temporal
        // column, no freshness declared yet).
        write_file(
            &root.join("models/revenue_daily.sql"),
            WORKER_SQL.as_bytes(),
        );
        let result = compile_probe(&root).expect("compiles");
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert!(
            !diagnostics(&result, "W005").is_empty(),
            "pre-merge draft warns W005 (no freshness yet)"
        );

        // Phase B: the metadata merge. The engine consumes every block —
        // freshness clears W005; W004 stays silent because [mask]
        // resolves pii; the product tag lands on the compiled model.
        run_phase_b(&root, "products/revenue_daily.toml", &parsed).expect("phase B");
        let result = compile_probe(&root).expect("compiles");
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert!(
            diagnostics(&result, "W005").is_empty(),
            "freshness clears W005"
        );
        assert!(
            diagnostics(&result, "W004").is_empty(),
            "[mask] resolves pii"
        );
        let model = &result.project.models[0];
        assert_eq!(
            model.config.tags.get("product").map(String::as_str),
            Some("revenue_daily"),
            "the merged product tag is on the compiled model"
        );
        assert!(
            model.config.freshness.is_some(),
            "the merged freshness block is on the compiled model"
        );
    }

    #[test]
    fn status_reports_a_pending_journal_without_resolving_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        product_compile_in(&root, &config, None, "revenue_daily").expect("phase A");
        // Park a (well-formed but uncommitted) journal in the state dir.
        let journal = root
            .join(state_dir_rel("revenue_daily"))
            .join(rocky_core::product::commit::STAGING_JOURNAL);
        write_file(
            &journal,
            format!(
                r#"{{"entries": [{{"final": "{}", "staged_sha": "sha256:{}", "has_prev": true}}], "manifest": "{}"}}"#,
                manifest_rel("revenue_daily"),
                "0".repeat(64),
                manifest_rel("revenue_daily"),
            )
            .as_bytes(),
        );
        let status = product_status_in(&root, None, "revenue_daily").expect("status");
        assert!(status.staging_journal_present);
        assert!(journal.is_file(), "status never mutates");
    }

    /// No state store yet is the ordinary case: an empty status is honest.
    #[test]
    fn a_state_path_that_is_simply_absent_still_reports_a_status() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        product_compile_in(&root, &config, None, "revenue_daily").expect("phase A");

        let missing = dir.path().join("no-store-here.redb");
        let status =
            product_status_in(&root, Some(&missing), "revenue_daily").expect("absent is fine");
        assert!(status.approval.is_none());
        assert_eq!(status.journal_rows, 0);
    }

    /// Compile reads the approval so it can refuse a tampered snapshot — the
    /// comment above that read calls it load-bearing. An unreadable store used
    /// to yield `None`, so compile proceeded as though no approval existed and
    /// the tamper check never ran. That is the gate failing OPEN.
    #[cfg(unix)]
    #[test]
    fn compile_refuses_an_unreadable_store_instead_of_skipping_the_approval_check() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());

        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("mkdir");
        let store = locked.join("state.redb");
        std::fs::write(&store, b"not really a store").expect("write");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");

        let result = product_compile_in(&root, &config, Some(&store), "revenue_daily");

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("restore");

        let err = result.expect_err("compile must refuse a store it cannot read");
        assert!(
            format!("{err:#}").contains("cannot be read"),
            "got: {err:#}"
        );
    }

    /// The name roll-call feeds `product list` AND the journal's existence
    /// check. An unreadable store used to drop every store-only product, so a
    /// journal request for one answered "no such product".
    #[cfg(unix)]
    #[test]
    fn the_product_roll_call_refuses_a_store_it_cannot_read() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, _config) = project_with_config(dir.path(), &passing_config());

        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("mkdir");
        let store = locked.join("state.redb");
        std::fs::write(&store, b"not really a store").expect("write");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");

        let result = product_names_in(&root, Some(&store));

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("restore");

        let err = result.expect_err("the roll-call must refuse a store it cannot read");
        assert!(
            format!("{err:#}").contains("cannot be read"),
            "got: {err:#}"
        );
    }

    /// A store that IS there and cannot be read must refuse. `Path::exists()`
    /// answered false for a metadata error, so the screen rendered "the loop
    /// has not run", "approval none" and an empty journal over a store nobody
    /// could open — a healthy-looking answer built on no evidence.
    #[cfg(unix)]
    #[test]
    fn a_state_store_that_cannot_be_read_refuses_instead_of_reporting_an_empty_one() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        product_compile_in(&root, &config, None, "revenue_daily").expect("phase A");

        // A directory the process may not search, with the store path inside.
        let locked = dir.path().join("locked");
        std::fs::create_dir(&locked).expect("mkdir");
        let store = locked.join("state.redb");
        std::fs::write(&store, b"not really a store").expect("write");
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).expect("chmod");

        let result = product_status_in(&root, Some(&store), "revenue_daily");

        // Restore before asserting, so a failure cannot leave an unremovable dir.
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).expect("restore");

        let err = result.expect_err("an unreadable store must refuse");
        let text = format!("{err:#}");
        assert!(
            text.contains("cannot be read"),
            "the refusal must say the store could not be read, got: {text}"
        );
    }

    /// #1817: the same store under a DANGLING ancestor. `metadata` reports
    /// `NotFound` — and so did the leaf-only discriminator, because
    /// `symlink_metadata(state.redb)` fails the same way under a broken
    /// parent. The store read as "no store yet" and status reported a healthy
    /// product that had never run. No root caveat: a dangling link needs no
    /// permissions to build.
    #[cfg(unix)]
    #[test]
    fn a_state_store_under_a_dangling_ancestor_refuses_instead_of_reporting_an_empty_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (root, config) = project_with_config(dir.path(), &passing_config());
        product_compile_in(&root, &config, None, "revenue_daily").expect("phase A");

        let state_home = dir.path().join("state-home");
        std::os::unix::fs::symlink(dir.path().join("gone"), &state_home).expect("symlink");
        let store = state_home.join("state.redb");
        assert!(
            std::fs::metadata(&store).is_err() && std::fs::symlink_metadata(&store).is_err(),
            "precondition: both probes the old code trusted report the store missing"
        );

        let err = product_status_in(&root, Some(&store), "revenue_daily")
            .expect_err("a store under a dangling ancestor is not an absent store");
        let text = format!("{err:#}");
        assert!(
            text.contains("cannot be read") && text.contains("cannot be resolved"),
            "the refusal must name the broken ancestor, got: {text}"
        );
    }
}

#[cfg(test)]
pub(crate) mod delivery_tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};

    const SPEC: &str =
        include_str!("../../../rocky-core/src/product/testdata/revenue_daily.spec.toml");

    /// The injected clock for the delivery tests: May 2026, UTC.
    pub(crate) fn at(day: u32, hour: u32, minute: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 5, day, hour, minute, 0).unwrap()
    }

    /// Record one production run of pipeline `revenue` that built
    /// `revenue_daily`, stamped exactly as `rocky run` stamps it.
    pub(crate) fn seed_delivery(
        store: &StateStore,
        root: &Path,
        id: &str,
        started: DateTime<Utc>,
        finished: DateTime<Utc>,
    ) {
        let mut record: rocky_core::state::RunRecord = serde_json::from_value(serde_json::json!({
            "run_id": id,
            "started_at": started,
            "finished_at": finished,
            "status": rocky_core::state::RunStatus::Success,
            "models_executed": [{
                "model_name": "revenue_daily",
                "started_at": started,
                "finished_at": finished,
                "duration_ms": (finished - started).num_milliseconds(),
                "rows_affected": 2,
                "status": "success",
                "sql_hash": "h",
                "bytes_scanned": null,
                "bytes_written": null,
            }],
            "trigger": rocky_core::state::RunTrigger::Schedule,
            "config_hash": "c",
            "pipeline": "revenue",
            "run_scope": rocky_core::state::RunScope::Production,
        }))
        .expect("a run record");
        rocky_core::delivery::stamp_delivery_timing(
            &mut record,
            store,
            &product_deadlines_in(root),
        )
        .expect("stamp");
        store.record_run(&record).expect("record");
    }

    /// Five production runs on days 1..=5, each started 05:00 and landed
    /// 05:10 against a 06:00 deadline: on time, with a 10-minute p90.
    pub(crate) fn seed_five_on_time(store: &StateStore, root: &Path) {
        for day in 1..=5 {
            seed_delivery(
                store,
                root,
                &format!("run-{day}"),
                at(day, 5, 0),
                at(day, 5, 10),
            );
        }
    }

    /// The shared spec fixture with a delivery deadline added to `[product]`.
    pub(crate) fn spec_with_deadline(deliver_by: &str) -> String {
        let intent = "intent = \"Daily gross revenue per client in EUR, refunds excluded\"\n";
        assert!(SPEC.contains(intent), "fixture layout changed");
        SPEC.replacen(
            intent,
            &format!("{intent}deliver_by = \"{deliver_by}\"\n"),
            1,
        )
    }

    pub(crate) fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    /// A project root with the deadline spec and an open state store.
    fn project() -> (tempfile::TempDir, PathBuf, PathBuf, StateStore) {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().to_path_buf();
        write(
            &root.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        let state_path = root.join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        (dir, root, state_path, store)
    }

    /// The `delivery` object of `rocky product status --output json`, read
    /// at `now`.
    fn status_json(root: &Path, state_path: &Path, now: DateTime<Utc>) -> serde_json::Value {
        let output =
            product_status_at(root, Some(state_path), "revenue_daily", now).expect("status");
        let json = serde_json::to_value(&output).expect("json");
        json["delivery"].clone()
    }

    #[test]
    fn status_json_reports_an_on_time_delivery() {
        let (_dir, root, state_path, store) = project();
        seed_five_on_time(&store, &root);
        drop(store);

        let delivery = status_json(&root, &state_path, at(5, 12, 0));
        assert_eq!(delivery["product"], "revenue_daily");
        assert_eq!(delivery["deliver_by"], "0 6 * * *");
        assert_eq!(delivery["timezone"], "UTC");
        assert_eq!(delivery["on_time"], true);
        assert_eq!(delivery["late_by_seconds"], 0);
        assert_eq!(delivery["last_due_at"], "2026-05-05T06:00:00Z");
        assert_eq!(delivery["next_due_at"], "2026-05-06T06:00:00Z");
        assert_eq!(delivery["at_risk"], false);
        let estimate = &delivery["estimate"];
        assert_eq!(estimate["state"], "known");
        assert_eq!(estimate["basis"], "model_delivery");
        assert_eq!(estimate["n"], 5);
        assert_eq!(estimate["window"], 20);
        assert_eq!(estimate["p50_ms"], 600_000);
        assert_eq!(estimate["p90_ms"], 600_000);
    }

    #[test]
    fn status_json_reports_a_late_delivery() {
        let (_dir, root, state_path, store) = project();
        seed_five_on_time(&store, &root);
        // Day 6: starts 07:20, after the 06:00 deadline, lands 07:30.
        seed_delivery(&store, &root, "run-6", at(6, 7, 20), at(6, 7, 30));
        drop(store);

        let delivery = status_json(&root, &state_path, at(6, 12, 0));
        assert_eq!(delivery["on_time"], false);
        assert_eq!(delivery["late_by_seconds"], 90 * 60);
        assert_eq!(delivery["last_due_at"], "2026-05-06T06:00:00Z");
        assert_eq!(delivery["last_delivered_at"], "2026-05-06T07:30:00Z");
        assert_eq!(delivery["next_due_at"], "2026-05-07T06:00:00Z");
    }

    #[test]
    fn status_json_reports_an_at_risk_delivery() {
        let (_dir, root, state_path, store) = project();
        seed_five_on_time(&store, &root);
        drop(store);

        // Day 6, 05:55: the open deadline is 06:00, five minutes away, and
        // the p90 is ten minutes.
        let delivery = status_json(&root, &state_path, at(6, 5, 55));
        assert_eq!(delivery["next_due_at"], "2026-05-06T06:00:00Z");
        assert_eq!(delivery["at_risk"], true);
        // The last delivery itself was on time.
        assert_eq!(delivery["on_time"], true);
    }

    #[test]
    fn status_json_gives_no_risk_verdict_below_five_runs() {
        let (_dir, root, state_path, store) = project();
        for day in 1..=4 {
            seed_delivery(
                &store,
                &root,
                &format!("run-{day}"),
                at(day, 5, 0),
                at(day, 5, 10),
            );
        }
        drop(store);

        let delivery = status_json(&root, &state_path, at(5, 5, 55));
        assert_eq!(delivery["estimate"]["state"], "unknown");
        assert_eq!(delivery["estimate"]["n"], 4);
        assert!(delivery["estimate"].get("p90_ms").is_none());
        assert!(
            delivery["at_risk"].is_null(),
            "no verdict without 5 runs: {delivery}"
        );
        // The delivery facts do not need the estimate.
        assert_eq!(delivery["on_time"], true);
    }

    #[test]
    fn a_spec_without_deliver_by_reports_no_delivery() {
        let dir = tempfile::tempdir().unwrap();
        write(&dir.path().join("products/revenue_daily.toml"), SPEC);
        let output =
            product_status_at(dir.path(), None, "revenue_daily", at(1, 0, 0)).expect("status");
        assert!(output.delivery.is_none());
        let json = serde_json::to_value(&output).unwrap();
        assert!(json.get("delivery").is_none(), "absent, not null: {json}");
    }

    #[test]
    fn deadlines_come_from_specs_that_declare_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        // A spec without a deadline and a broken spec contribute nothing.
        write(
            &root.join("products/other.toml"),
            &SPEC.replace("revenue_daily", "other"),
        );
        write(&root.join("products/broken.toml"), "not = [toml");

        let deadlines = product_deadlines_in(root);
        assert_eq!(deadlines.len(), 1, "{deadlines:?}");
        assert_eq!(deadlines[0].product, "revenue_daily");
        assert_eq!(deadlines[0].output_model, "revenue_daily");
        assert_eq!(deadlines[0].deliver_by.cron, "0 6 * * *");
    }

    /// A run, `rocky product status` and the scheduler status all read
    /// `products/` next to the config file, never the working directory. A
    /// scheduled child run (`rocky run -c <config>`) inherits the scheduler's
    /// working directory, so a cwd-anchored writer stamps nothing there. The
    /// config here sits in `proj/`; the working directory is left alone and
    /// holds no `products/`.
    #[test]
    fn a_run_stamps_deadlines_from_the_products_next_to_the_config() {
        use crate::output::{MaterializationMetadata, MaterializationOutput, RunOutput};

        let dir = tempfile::tempdir().unwrap();
        let proj = dir.path().join("proj");
        write(
            &proj.join("products/revenue_daily.toml"),
            &spec_with_deadline("0 6 * * *"),
        );
        // The model `rocky run` builds, so the scheduler status maps the
        // deadline to `revenue` from config.
        write(&proj.join("models/revenue_daily.sql"), "SELECT 1 AS id\n");
        write(
            &proj.join("models/revenue_daily.toml"),
            "depends_on = []\n\n[target]\ncatalog = \"p\"\nschema = \"s\"\n\
             table = \"revenue_daily\"\n",
        );
        let config_path = proj.join("rocky.toml");
        write(
            &config_path,
            "[adapter]\ntype = \"duckdb\"\npath = \"x.duckdb\"\n\n\
             [pipeline.revenue]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
             [pipeline.revenue.target.governance]\nauto_create_schemas = true\n\n\
             [pipeline.revenue.schedule]\ncron = \"0 5 * * *\"\ntimezone = \"UTC\"\n",
        );
        let cwd = std::env::current_dir().unwrap();
        assert!(
            !cwd.join("products").exists(),
            "the test needs a working directory without products/: {}",
            cwd.display()
        );
        let state_path = dir.path().join("state.redb");
        let store = StateStore::open(&state_path).unwrap();

        let mut output = RunOutput::new(String::new(), 1, 1);
        output.materializations.push(MaterializationOutput {
            volume: None,
            asset_key: vec!["revenue_daily".to_string()],
            notes: vec![],
            attempts: Vec::new(),
            rows_copied: Some(2),
            duration_ms: 1,
            started_at: Utc::now(),
            metadata: MaterializationMetadata {
                strategy: "full_refresh".to_string(),
                watermark: None,
                target_table_full_name: None,
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            job_ids: Vec::new(),
            skip_internal: None,
            recipe_identity: None,
            output_column_hashes: None,
            consumed_column_baseline: None,
            output_version: None,
            output_target: None,
            stored_bytes: None,
            upstream_reads: None,
        });
        let audit = crate::commands::run::audit_to_record(
            &crate::commands::run_audit::AuditContext::detect(None, None, None),
        );

        let landed = crate::commands::run::persist_run_record(
            Some(&store),
            &output,
            "run-root",
            Utc::now(),
            "cfg",
            &audit,
            Some("revenue"),
            Some(&config_path),
        );
        assert!(landed, "the record must land");
        let record = store.get_run("run-root").unwrap().expect("record");
        assert!(
            record.models_executed[0].due_at.is_some(),
            "the run must stamp the deadline declared next to the config: {:?}",
            record.models_executed[0]
        );
        drop(store);

        // `rocky product status` resolves the same root and finds the spec.
        let status_root = product_command_root(&config_path);
        assert_eq!(status_root, proj);
        let status = product_status_in(&status_root, Some(&state_path), "revenue_daily")
            .expect("product status");
        assert!(
            status.delivery.is_some(),
            "product status must read the deadline the run stamped"
        );

        // The scheduler status resolves the same root and reports the same
        // product's delivery.
        let schedule = crate::commands::schedule_status::schedule_status_output(
            &config_path,
            &state_path,
            &proj.join(".rocky"),
            Utc::now(),
        )
        .expect("schedule status");
        let json = serde_json::to_value(&schedule).unwrap();
        assert_eq!(json["pipelines"][0]["pipeline"], "revenue");
        assert_eq!(
            json["pipelines"][0]["delivery"]["product"], "revenue_daily",
            "schedule status must read the products/ next to the config: {json}"
        );
    }

    /// A bare config name has an empty parent, so the root is `.`.
    #[test]
    fn a_bare_config_name_roots_at_the_working_directory() {
        assert_eq!(
            product_command_root(Path::new("rocky.toml")),
            std::path::PathBuf::from(".")
        );
        assert_eq!(
            product_command_root(Path::new("/srv/x/rocky.toml")),
            std::path::PathBuf::from("/srv/x")
        );
    }

    #[test]
    fn a_project_without_products_has_no_deadlines() {
        let dir = tempfile::tempdir().unwrap();
        assert!(product_deadlines_in(dir.path()).is_empty());
    }
}
