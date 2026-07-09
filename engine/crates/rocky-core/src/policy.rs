//! Agent-authority policy evaluator (explain-mode, v0).
//!
//! Given a `(principal, capability, model)` triple and a project's
//! `[policy]` block, [`evaluate`] returns the resolved [`PolicyEffect`],
//! the matched rule (if any), and a human-readable reason. This is the
//! *decision* half of the policy plane; v0 only explains decisions — it
//! does not gate any real command.
//!
//! # Frozen semantics
//!
//! 1. **Reads short-circuit.** `read` is always `allow`, before matching.
//! 2. **Deny is a hard override.** If *any* rule matches with
//!    `effect = deny`, the decision is `deny` regardless of specificity —
//!    no `allow` overturns it.
//! 3. **Most-specific wins.** Among matching non-deny rules, rule R1 beats
//!    R2 when R1's *matched-constraint set* is a strict superset of R2's. A
//!    constraint is one satisfied scope predicate; a `schema_change.*` /
//!    `value_change` capability refinement counts as one constraint over
//!    the bare verb. `scope.any = true` carries the empty set, so any rule
//!    with a real predicate outranks it.
//! 4. **Incomparable → most-restrictive.** When no single rule dominates
//!    (matched sets are incomparable or equal), the most restrictive effect
//!    wins (`require_review` > `allow`); a final tie breaks by declaration
//!    order (earliest wins).
//! 5. **Default posture.** No rule matches ⇒ an `agent` on a mutating
//!    capability falls to `default_agent_effect`; a `human` is never gated
//!    (`allow`).
//! 6. **`max_downstreams` degrades, never no-matches.** A rule may carry a
//!    `scope.max_downstreams = N` blast-radius ceiling. It does **not** change
//!    whether the rule's scope matches; instead, when the rule's effect is
//!    `allow` and the target's transitive downstream reachability is either
//!    over `N` **or** uncomputable, the matched effect degrades to
//!    `require_review` (fail-safe). A ceiling can therefore never let an
//!    `allow` stand for an oversized or unverifiable blast radius; `deny` and
//!    `require_review` rules are unaffected (`deny` is unconditional).
//! 7. **Sticky `max_downstreams` safety cap.** A ceiling breach is *sticky*:
//!    if **any** matched rule's `max_downstreams` was exceeded or uncomputable,
//!    the final effect is capped to at least `require_review` — even when a
//!    more-specific sibling `allow` (with no ceiling, or a superset of matched
//!    constraints) would otherwise win the specificity contest and stand as an
//!    ungated `allow`. This closes the false-allow where a broad sibling `allow`
//!    *dominates* a ceilinged rule via the non-dominated filter and thereby lets
//!    an oversized blast radius through. It is applied as a post-winner check:
//!    it never softens a `deny` (deny is resolved first, before any winner) and
//!    it never overrides the uncomputable-blast-radius fail-closed rule (both
//!    point the same way — toward `require_review`).

use std::collections::{BTreeMap, BTreeSet};

use crate::breaking_change::{BreakingChange, BreakingFinding};
use crate::config::{
    PolicyCapability, PolicyConfig, PolicyEffect, PolicyPrincipal, PolicyScope, glob_match,
};

/// Classify one model's change into the capability that governs its apply,
/// per the frozen fail-closed §2.4 rules.
///
/// - Every finding is a bare `ColumnAdded` / `ModelAdded` ⇒
///   [`PolicyCapability::SchemaChangeAdditive`].
/// - The *only* change is `SqlBodyChanged` (alone) ⇒
///   [`PolicyCapability::ValueChange`].
/// - **Everything else — including an empty finding set — ⇒
///   [`PolicyCapability::SchemaChangeBreaking`]** (fail closed). A widening
///   `ColumnTypeChanged`, a `ColumnMaskChanged`, a mixed additive+body change,
///   or "no findings for this model" all resolve up to breaking: if we cannot
///   *prove* a change is additive-or-cosmetic, we treat it as breaking.
///
/// Callers pass the findings that belong to a **single** model (already
/// grouped). An empty slice classifies as breaking — the caller is expected
/// to only classify models it knows were changed; the empty case is the
/// belt-and-braces fail-closed floor.
pub fn classify_model_findings(findings: &[&BreakingFinding]) -> PolicyCapability {
    if findings.is_empty() {
        return PolicyCapability::SchemaChangeBreaking;
    }
    let all_additive = findings.iter().all(|f| {
        matches!(
            f.change,
            BreakingChange::ColumnAdded { .. } | BreakingChange::ModelAdded { .. }
        )
    });
    if all_additive {
        return PolicyCapability::SchemaChangeAdditive;
    }
    let only_body = findings
        .iter()
        .all(|f| matches!(f.change, BreakingChange::SqlBodyChanged { .. }));
    if only_body {
        return PolicyCapability::ValueChange;
    }
    PolicyCapability::SchemaChangeBreaking
}

/// Group a flat findings list by model (`change.model()`) and classify each
/// changed model into its governing capability. Only models that carry at
/// least one finding appear in the result — unchanged models are absent (they
/// are not schema changes and are not gated). Keyed by `target.full_name()`;
/// callers that key on the logical model name must remap.
pub fn classify_findings_by_model(
    findings: &[BreakingFinding],
) -> BTreeMap<String, PolicyCapability> {
    let mut by_model: BTreeMap<String, Vec<&BreakingFinding>> = BTreeMap::new();
    for finding in findings {
        by_model
            .entry(finding.change.model().to_string())
            .or_default()
            .push(finding);
    }
    by_model
        .into_iter()
        .map(|(model, group)| (model, classify_model_findings(&group)))
        .collect()
}

/// The compiled attributes of one model that the policy matcher reads.
///
/// Built from the compiled project (see the `rocky policy check` command):
/// `contracted` and `layer` are best-effort in v0 — `contracted` is the
/// presence of a sibling `.contract.toml`, `layer` is the model's `layer`
/// tag.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModelAttributes {
    /// Model name (matched against `scope.models` globs).
    pub name: String,
    /// Model-level governance tags.
    pub tags: BTreeMap<String, String>,
    /// Distinct column-classification values present on the model
    /// (e.g. `{"pii", "confidential"}`).
    pub classifications: BTreeSet<String>,
    /// Medallion/semantic layer (the model's `layer` tag), if any.
    pub layer: Option<String>,
    /// Whether the model sits behind a contract (best-effort v0: a sibling
    /// `.contract.toml` exists).
    pub contracted: bool,
    /// Direct downstream-consumer count (models that `depends_on` this one).
    /// Informational only — the `max_downstreams` ceiling reads
    /// [`Self::reachable_downstreams`], the transitive blast radius.
    pub downstreams: u64,
    /// Transitive downstream reachability — the count of every model in this
    /// model's blast radius (direct + indirect), excluding itself. This is
    /// what a rule's `max_downstreams` ceiling is compared against.
    ///
    /// `None` means the blast radius could **not** be computed (the model is
    /// absent from the compiled graph, or the project did not compile). A
    /// `max_downstreams` ceiling **fails closed** on `None`: it never grants
    /// `allow`, because an uncounted blast radius is treated as too large.
    pub reachable_downstreams: Option<u64>,
}

/// A single satisfied constraint contributed by a matching rule. The set
/// of these per rule drives the strict-superset specificity ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Constraint {
    CapabilityRefinement,
    Models,
    Tags,
    Classifications,
    ExcludeClassifications,
    Contracted,
    Layer,
}

/// The resolved policy decision for one `(principal, capability, model)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PolicyDecision {
    /// The resolved effect.
    pub effect: PolicyEffect,
    /// Zero-based index of the winning rule in `[[policy.rules]]`, or
    /// `None` when the decision came from a short-circuit or the default
    /// posture.
    pub matched_rule: Option<usize>,
    /// Human-readable explanation of how the effect was reached.
    pub reason: String,
}

/// Returns the satisfied-constraint set for `scope` against `attrs`, or
/// `None` when the scope is not satisfied (so the rule does not match).
///
/// `scope.any` yields the empty set. `max_downstreams` is **not** a scope
/// predicate — it is a post-match condition evaluated in [`evaluate`] (it
/// degrades an `allow`, it never turns a match into a no-match), so it never
/// contributes a constraint or a match failure here.
fn scope_constraints(scope: &PolicyScope, attrs: &ModelAttributes) -> Option<BTreeSet<Constraint>> {
    if scope.any {
        return Some(BTreeSet::new());
    }
    let mut set = BTreeSet::new();

    if !scope.models.is_empty() {
        if scope.models.iter().any(|pat| glob_match(pat, &attrs.name)) {
            set.insert(Constraint::Models);
        } else {
            return None;
        }
    }
    if !scope.tags.is_empty() {
        let all = scope
            .tags
            .iter()
            .all(|(k, v)| attrs.tags.get(k).is_some_and(|got| got == v));
        if all {
            set.insert(Constraint::Tags);
        } else {
            return None;
        }
    }
    if !scope.classifications.is_empty() {
        if scope
            .classifications
            .iter()
            .any(|c| attrs.classifications.contains(c))
        {
            set.insert(Constraint::Classifications);
        } else {
            return None;
        }
    }
    if !scope.exclude_classifications.is_empty() {
        let clean = !scope
            .exclude_classifications
            .iter()
            .any(|c| attrs.classifications.contains(c));
        if clean {
            set.insert(Constraint::ExcludeClassifications);
        } else {
            return None;
        }
    }
    if let Some(want) = scope.contracted {
        if attrs.contracted == want {
            set.insert(Constraint::Contracted);
        } else {
            return None;
        }
    }
    if let Some(want) = &scope.layer {
        if attrs.layer.as_deref() == Some(want.as_str()) {
            set.insert(Constraint::Layer);
        } else {
            return None;
        }
    }
    // `max_downstreams` is a post-match condition (evaluated in `evaluate`),
    // not a scope predicate: never required, never counted here.
    Some(set)
}

/// Restrictiveness rank for aggregating per-model effects into a single
/// plan-level effect: `deny` (2) > `require_review` (1) > `allow` (0). The
/// plan-level enforcement decision (see the CLI apply path) is the
/// most-restrictive effect across every evaluated model, so a single
/// protected model cannot be diluted by bundling it with permissive ones.
pub fn effect_rank(effect: PolicyEffect) -> u8 {
    restrictiveness(effect)
}

/// Restrictiveness rank among non-deny effects — higher wins ties between
/// incomparable rules. `Deny` never reaches this path (handled as a hard
/// override before specificity resolution).
fn restrictiveness(effect: PolicyEffect) -> u8 {
    match effect {
        PolicyEffect::Allow => 0,
        PolicyEffect::RequireReview => 1,
        PolicyEffect::Deny => 2,
    }
}

/// Apply a rule's `max_downstreams` blast-radius ceiling as a post-match
/// condition. Returns the (possibly degraded) effect and, when a degrade
/// happened, a human-readable reason.
///
/// The ceiling is a **narrowing guard on `allow`** and fails closed:
///
/// - No ceiling, or a non-`allow` effect → unchanged (`deny` /
///   `require_review` are never softened; a `deny` ceiling is a no-op).
/// - `allow` with the target's [`ModelAttributes::reachable_downstreams`]
///   present and `≤ limit` → stays `allow`.
/// - `allow` with reachability over the limit → degrades to
///   `require_review`.
/// - `allow` with reachability **uncomputable** (`None`) → degrades to
///   `require_review` — an uncounted blast radius is treated as too large, so
///   a ceiling can never grant `allow` when it cannot be verified.
fn degrade_for_ceiling(
    effect: PolicyEffect,
    limit: Option<u64>,
    attrs: &ModelAttributes,
) -> (PolicyEffect, Option<String>) {
    let Some(limit) = limit else {
        return (effect, None);
    };
    if effect != PolicyEffect::Allow {
        return (effect, None);
    }
    match attrs.reachable_downstreams {
        Some(count) if count <= limit => (effect, None),
        Some(count) => (
            PolicyEffect::RequireReview,
            Some(format!(
                "max_downstreams={limit} exceeded (blast radius {count}) — allow degraded to require_review"
            )),
        ),
        None => (
            PolicyEffect::RequireReview,
            Some(format!(
                "max_downstreams={limit} unverifiable (blast radius uncomputable) — allow degraded to require_review"
            )),
        ),
    }
}

/// Evaluate the policy for one `(principal, capability, model)` triple.
///
/// See the module docs for the full frozen semantics.
pub fn evaluate(
    policy: &PolicyConfig,
    principal: PolicyPrincipal,
    capability: PolicyCapability,
    attrs: &ModelAttributes,
) -> PolicyDecision {
    // 1. Reads short-circuit.
    if capability == PolicyCapability::Read {
        return PolicyDecision {
            effect: PolicyEffect::Allow,
            matched_rule: None,
            reason: "read is always allowed (short-circuit)".to_string(),
        };
    }

    // Collect every matching rule with its matched-constraint set.
    struct Candidate {
        idx: usize,
        effect: PolicyEffect,
        constraints: BTreeSet<Constraint>,
        /// Set when a `max_downstreams` ceiling degraded this rule's `allow`
        /// to `require_review`; carried into the winning-rule reason string.
        degraded: Option<String>,
    }
    let mut candidates: Vec<Candidate> = Vec::new();
    let mut first_deny: Option<usize> = None;
    for (idx, rule) in policy.rules.iter().enumerate() {
        if rule.principal != principal {
            continue;
        }
        if !rule.capability.matches_input(capability) {
            continue;
        }
        let Some(mut constraints) = scope_constraints(&rule.scope, attrs) else {
            continue;
        };
        if rule.capability.is_refinement() {
            constraints.insert(Constraint::CapabilityRefinement);
        }
        // 2. Deny is a hard override — any deny match wins. Keyed on the rule's
        // *declared* effect: the `max_downstreams` degrade below only ever
        // touches `allow`, so a `deny` is never softened away.
        if rule.effect == PolicyEffect::Deny {
            first_deny.get_or_insert(idx);
        }
        // 6. `max_downstreams` ceiling — a post-match condition that degrades
        // an `allow` (never a no-match). Fail-closed: an oversized OR
        // uncomputable blast radius degrades the `allow` to `require_review`.
        let (effect, degraded) =
            degrade_for_ceiling(rule.effect, rule.scope.max_downstreams, attrs);
        candidates.push(Candidate {
            idx,
            effect,
            constraints,
            degraded,
        });
    }

    if let Some(idx) = first_deny {
        return PolicyDecision {
            effect: PolicyEffect::Deny,
            matched_rule: Some(idx),
            reason: format!("denied by rule {idx} (deny overrides)"),
        };
    }

    // 4a. Default posture — no rule matched.
    if candidates.is_empty() {
        return match principal {
            PolicyPrincipal::Human => PolicyDecision {
                effect: PolicyEffect::Allow,
                matched_rule: None,
                reason: "no rule matched; humans are not gated in v0".to_string(),
            },
            PolicyPrincipal::Agent => PolicyDecision {
                effect: policy.default_agent_effect,
                matched_rule: None,
                reason: format!(
                    "no rule matched; default_agent_effect = {}",
                    effect_label(policy.default_agent_effect)
                ),
            },
        };
    }

    // 3. Most-specific wins: keep only rules not strictly dominated by a
    // more-specific matching rule (matched-set strict superset).
    let non_dominated: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| {
            !candidates.iter().any(|other| {
                other.idx != c.idx
                    && other.constraints.is_superset(&c.constraints)
                    && other.constraints.len() > c.constraints.len()
            })
        })
        .collect();

    // Winner among the non-dominated tier.
    // 4b. Incomparable → most-restrictive effect, then earliest order.
    let winner = non_dominated
        .iter()
        .copied()
        .max_by(|a, b| {
            restrictiveness(a.effect)
                .cmp(&restrictiveness(b.effect))
                // earlier declaration order wins ties → treat smaller idx as greater
                .then(b.idx.cmp(&a.idx))
        })
        .expect("non_dominated is non-empty when candidates is non-empty");

    let mut reason = if non_dominated.len() == 1 {
        format!(
            "{} by rule {} (most-specific match)",
            effect_label(winner.effect),
            winner.idx
        )
    } else {
        let all_same_effect = non_dominated
            .iter()
            .all(|c| restrictiveness(c.effect) == restrictiveness(winner.effect));
        if all_same_effect {
            format!(
                "{} by rule {} (earliest of {} equally-specific rules)",
                effect_label(winner.effect),
                winner.idx,
                non_dominated.len()
            )
        } else {
            format!(
                "{} by rule {} (most-restrictive of {} incomparable rules)",
                effect_label(winner.effect),
                winner.idx,
                non_dominated.len()
            )
        }
    };
    if let Some(degrade) = &winner.degraded {
        reason.push_str("; ");
        reason.push_str(degrade);
    }

    // 7. Sticky `max_downstreams` safety cap (post-winner). A ceiling breach on
    // ANY matched rule caps the final effect to at least `require_review`, even
    // when a more-specific sibling `allow` won the specificity contest above.
    // The winner's own `allow` is never ceiling-degraded — a degraded rule is
    // already `require_review`, so `winner.effect == Allow` guarantees the cap
    // reason comes from a *non-winning* breached candidate. Without this, a
    // broad ungated `allow` that dominates a ceilinged sibling (superset of
    // constraints, no ceiling) would let an oversized or unverifiable blast
    // radius stand — the exact false-allow the ceiling exists to prevent.
    let mut effect = winner.effect;
    if effect == PolicyEffect::Allow
        && let Some(breach) = candidates.iter().find(|c| c.degraded.is_some())
    {
        effect = PolicyEffect::RequireReview;
        let detail = breach.degraded.as_deref().unwrap_or("ceiling breached");
        reason.push_str(&format!(
            "; sticky safety cap: max_downstreams ceiling breached on rule {} ({detail}) \
             — allow capped to require_review",
            breach.idx
        ));
    }

    PolicyDecision {
        effect,
        matched_rule: Some(winner.idx),
        reason,
    }
}

/// The wire spelling of an effect, for reason strings.
fn effect_label(effect: PolicyEffect) -> &'static str {
    match effect {
        PolicyEffect::Allow => "allow",
        PolicyEffect::RequireReview => "require_review",
        PolicyEffect::Deny => "deny",
    }
}

// ===========================================================================
// Autonomy budgets + policy freeze — the dynamic, ledger-aware tightening layer
// ===========================================================================
//
// `evaluate` above resolves the *static* effect from the `[policy]` block. Two
// dynamic breakers can then tighten (never widen) that effect at enforcement
// time, both computed as **projections over the existing decision ledger** — no
// new persisted state:
//
// - **Autonomy budget** — a rule's verify-after failures within its rolling
//   window, counted from the ledger. At exhaustion the rule's `allow` degrades
//   to `require_review`.
// - **Policy freeze** — a kill switch recorded as a decision entry. An active
//   (unsuperseded) freeze whose principal + scope match the evaluation forces
//   `deny`.
//
// Both are structurally **monotone toward restriction**: [`autonomy_degradation`]
// only ever returns an effect at least as restrictive as the base, so the
// "auto-tighten only, never auto-widen" guarantee is a property of the code
// shape, not a runtime check.

use chrono::{DateTime, Utc};

use crate::config::{AutonomyBudget, parse_window_duration};
use crate::state::PolicyDecisionRecord;

/// `plan_id` prefix that marks a policy-freeze decision entry in the ledger.
pub const FREEZE_PLAN_PREFIX: &str = "freeze:";
/// `plan_id` prefix that marks a freeze-lifting (unfreeze) decision entry.
pub const UNFREEZE_PLAN_PREFIX: &str = "unfreeze:";

/// How a base effect was dynamically tightened, for the audit reason + brief.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutonomyDegradation {
    /// No dynamic breaker fired — the base effect stands.
    None,
    /// The winning rule's autonomy budget is exhausted; `allow` degraded to
    /// `require_review`.
    BudgetExhausted {
        /// Index of the rule whose budget burned.
        rule_idx: usize,
        /// Verify-after failures counted in the window.
        failures: u64,
        /// The rule's configured failure ceiling.
        limit: u64,
        /// The rule's configured window, verbatim.
        window: String,
    },
    /// An active policy freeze matched; the effect was forced to `deny`.
    Frozen {
        /// The frozen principal.
        principal: PolicyPrincipal,
        /// The freeze's scope selector, verbatim.
        scope: String,
        /// The `plan_id` of the freeze decision — the citation.
        plan_id: String,
    },
}

impl AutonomyDegradation {
    /// A human-readable clause to append to the decision reason, or `None`
    /// when no breaker fired.
    #[must_use]
    pub fn reason_suffix(&self) -> Option<String> {
        match self {
            AutonomyDegradation::None => None,
            AutonomyDegradation::BudgetExhausted {
                rule_idx,
                failures,
                limit,
                window,
            } => Some(format!(
                "autonomy budget exhausted on rule {rule_idx} ({failures}/{limit} verify-after \
                 failures in {window}) — allow degraded to require_review"
            )),
            AutonomyDegradation::Frozen {
                principal,
                scope,
                plan_id,
            } => Some(format!(
                "policy freeze active for {} on scope '{scope}' — forced to deny ({plan_id})",
                effect_principal_label(*principal),
            )),
        }
    }
}

fn effect_principal_label(principal: PolicyPrincipal) -> &'static str {
    match principal {
        PolicyPrincipal::Agent => "agent",
        PolicyPrincipal::Human => "human",
    }
}

/// Return `effect` unless `floor` is strictly more restrictive, in which case
/// return `floor`. The monotone building block: the result is never *less*
/// restrictive than either input, so a caller can only tighten.
fn tighten_to_at_least(effect: PolicyEffect, floor: PolicyEffect) -> PolicyEffect {
    if restrictiveness(effect) >= restrictiveness(floor) {
        effect
    } else {
        floor
    }
}

/// Count the verify-after failures attributable to rule `rule_idx` inside the
/// rolling `window` ending at `now`, from the decision ledger.
///
/// A verify-after failure is recorded as a **custody row** — a
/// [`PolicyDecisionRecord`] with a non-empty `verify_after` list and
/// `effect = deny` — keyed to the `plan_id` of the apply that failed. A failure
/// burns rule `rule_idx`'s budget when that same plan also carries a plain
/// evaluation row whose winning `rule_id` is `rule_idx` (i.e. the rule governed
/// a model in the failing apply). The count is over **distinct plans**, so one
/// multi-model apply burns the budget once.
#[must_use]
pub fn budget_failures_in_window(
    decisions: &[PolicyDecisionRecord],
    rule_idx: usize,
    window: chrono::Duration,
    now: DateTime<Utc>,
) -> u64 {
    let lower_bound = now - window;
    // Plans that suffered a verify-after failure inside the window.
    let failed_plans: BTreeSet<&str> = decisions
        .iter()
        .filter(|d| {
            !d.verify_after.is_empty()
                && d.effect == PolicyEffect::Deny
                && d.timestamp >= lower_bound
                && !is_freeze_record(d)
        })
        .map(|d| d.plan_id.as_str())
        .collect();
    if failed_plans.is_empty() {
        return 0;
    }
    // Distinct failing plans in which `rule_idx` was the winning rule.
    decisions
        .iter()
        .filter(|d| {
            d.verify_after.is_empty()
                && d.rule_id == Some(rule_idx)
                && !is_freeze_record(d)
                && failed_plans.contains(d.plan_id.as_str())
        })
        .map(|d| d.plan_id.as_str())
        .collect::<BTreeSet<&str>>()
        .len() as u64
}

/// `true` when a rule's budget is exhausted: failures in the window reach or
/// exceed the ceiling. Exhaustion is `>=` — the `N`-th failure trips a budget
/// of `N`.
#[must_use]
pub fn budget_is_exhausted(
    budget: &AutonomyBudget,
    decisions: &[PolicyDecisionRecord],
    rule_idx: usize,
    now: DateTime<Utc>,
) -> bool {
    let Some(window) = parse_window_duration(&budget.window) else {
        // An unparseable window fails closed to "not exhausted": config
        // validation already rejects it, so this is belt-and-braces and never
        // fabricates a degrade from a malformed budget.
        return false;
    };
    budget_failures_in_window(decisions, rule_idx, window, now) >= budget.failures
}

/// One active (unsuperseded) policy freeze, projected from the ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveFreeze {
    /// The frozen principal.
    pub principal: PolicyPrincipal,
    /// The scope selector the freeze targets (see [`scope_selector_matches`]).
    pub scope: String,
    /// When the freeze was recorded.
    pub frozen_at: DateTime<Utc>,
    /// The freeze decision's `plan_id` — the audit citation.
    pub plan_id: String,
    /// The freeze's human-readable reason.
    pub reason: String,
}

fn is_freeze_record(d: &PolicyDecisionRecord) -> bool {
    d.plan_id.starts_with(FREEZE_PLAN_PREFIX) || d.plan_id.starts_with(UNFREEZE_PLAN_PREFIX)
}

/// Project the set of currently-active freezes from the decision ledger.
///
/// A freeze and its lifting (unfreeze) are recorded as decision entries keyed
/// by `(principal, scope-selector)`. Within each key the **latest** entry wins:
/// a `freeze:` entry that no later `unfreeze:` entry supersedes is active. This
/// is how a freeze persists with no new table and how the normal
/// policy-change flow lifts it.
#[must_use]
pub fn active_freezes(decisions: &[PolicyDecisionRecord]) -> Vec<ActiveFreeze> {
    // Latest freeze/unfreeze record per (principal, scope) key. Keyed on a
    // principal rank because `PolicyPrincipal` is not `Ord`.
    let principal_rank = |p: PolicyPrincipal| match p {
        PolicyPrincipal::Human => 0u8,
        PolicyPrincipal::Agent => 1u8,
    };
    let mut latest: BTreeMap<(u8, String), &PolicyDecisionRecord> = BTreeMap::new();
    for d in decisions.iter().filter(|d| is_freeze_record(d)) {
        let key = (principal_rank(d.principal), d.model.clone());
        latest
            .entry(key)
            .and_modify(|cur| {
                if d.timestamp >= cur.timestamp {
                    *cur = d;
                }
            })
            .or_insert(d);
    }
    latest
        .into_values()
        .filter(|d| d.plan_id.starts_with(FREEZE_PLAN_PREFIX))
        .map(|d| ActiveFreeze {
            principal: d.principal,
            scope: d.model.clone(),
            frozen_at: d.timestamp,
            plan_id: d.plan_id.clone(),
            reason: d.reason.clone(),
        })
        .collect()
}

/// Match a freeze scope selector against a model's attributes.
///
/// Grammar (validated at freeze time by [`validate_scope_selector`]):
/// - `any` — matches every model.
/// - `layer=<name>` — the model's layer equals `<name>`.
/// - `model=<glob>` — the model name matches the glob (`*` / `?`).
/// - `classification=<value>` — the model carries that column classification.
/// - `tag=<key>` — the model carries a tag with that key (any value).
/// - `tag=<key>=<value>` — the model carries that tag with that exact value.
///
/// An unrecognised selector matches **nothing** (fail-safe: a typo'd freeze is
/// a no-op, never an over-broad deny). Freezes are validated before they are
/// written, so a stored selector is always well-formed.
#[must_use]
pub fn scope_selector_matches(selector: &str, attrs: &ModelAttributes) -> bool {
    let s = selector.trim();
    if s.is_empty() || s == "any" {
        return true;
    }
    if let Some(layer) = s.strip_prefix("layer=") {
        return attrs.layer.as_deref() == Some(layer);
    }
    if let Some(glob) = s.strip_prefix("model=") {
        return glob_match(glob, &attrs.name);
    }
    if let Some(classification) = s.strip_prefix("classification=") {
        return attrs.classifications.contains(classification);
    }
    if let Some(tag) = s.strip_prefix("tag=") {
        return match tag.split_once('=') {
            Some((k, v)) => attrs.tags.get(k).is_some_and(|got| got == v),
            None => attrs.tags.contains_key(tag),
        };
    }
    false
}

/// Validate a freeze scope selector string, returning a usage error message on
/// a malformed selector. Mirrors the grammar of [`scope_selector_matches`].
pub fn validate_scope_selector(selector: &str) -> Result<(), String> {
    let s = selector.trim();
    if s.is_empty() || s == "any" {
        return Ok(());
    }
    for prefix in ["layer=", "model=", "classification="] {
        if let Some(rest) = s.strip_prefix(prefix) {
            return if rest.is_empty() {
                Err(format!(
                    "scope selector '{selector}': '{prefix}' needs a value"
                ))
            } else {
                Ok(())
            };
        }
    }
    if let Some(rest) = s.strip_prefix("tag=") {
        // `tag=key` or `tag=key=value`; the key must be non-empty.
        let key = rest.split_once('=').map_or(rest, |(k, _)| k);
        return if key.is_empty() {
            Err(format!(
                "scope selector '{selector}': tag key must not be empty"
            ))
        } else {
            Ok(())
        };
    }
    Err(format!(
        "scope selector '{selector}' is not recognised — use 'any', 'layer=<n>', 'model=<glob>', \
         'classification=<v>', or 'tag=<key>[=<value>]'"
    ))
}

/// Apply the dynamic breakers to a base effect: an active freeze forces `deny`;
/// an exhausted autonomy budget on the winning rule degrades `allow` to
/// `require_review`. Returns the (possibly tightened) effect and a description
/// of which breaker fired.
///
/// **Monotone toward restriction.** The returned effect is always at least as
/// restrictive as `base` — there is no code path that widens. Callers apply
/// this *after* [`evaluate`] at the mutating enforcement seam, passing the
/// decision ledger snapshot read before the current action's rows are written.
///
/// Freeze is checked first and independent of `matched_rule`, so it overrides
/// even the default posture; budget applies only to the winning rule.
#[must_use]
pub fn autonomy_degradation(
    base: PolicyEffect,
    matched_rule: Option<usize>,
    policy: &PolicyConfig,
    principal: PolicyPrincipal,
    attrs: &ModelAttributes,
    decisions: &[PolicyDecisionRecord],
    now: DateTime<Utc>,
) -> (PolicyEffect, AutonomyDegradation) {
    // 1. Freeze — a kill switch that forces deny for a matching principal+scope.
    for freeze in active_freezes(decisions) {
        if freeze.principal == principal && scope_selector_matches(&freeze.scope, attrs) {
            return (
                tighten_to_at_least(base, PolicyEffect::Deny),
                AutonomyDegradation::Frozen {
                    principal,
                    scope: freeze.scope,
                    plan_id: freeze.plan_id,
                },
            );
        }
    }

    // 2. Autonomy budget — degrades the winning rule's allow to require_review.
    if let Some(idx) = matched_rule
        && let Some(rule) = policy.rules.get(idx)
        && let Some(budget) = &rule.autonomy_budget
        && budget_is_exhausted(budget, decisions, idx, now)
    {
        let failures = parse_window_duration(&budget.window)
            .map(|w| budget_failures_in_window(decisions, idx, w, now))
            .unwrap_or(0);
        return (
            tighten_to_at_least(base, PolicyEffect::RequireReview),
            AutonomyDegradation::BudgetExhausted {
                rule_idx: idx,
                failures,
                limit: budget.failures,
                window: budget.window.clone(),
            },
        );
    }

    (base, AutonomyDegradation::None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{PolicyRule, PolicyScope};

    fn rule(
        principal: PolicyPrincipal,
        capability: PolicyCapability,
        scope: PolicyScope,
        effect: PolicyEffect,
    ) -> PolicyRule {
        PolicyRule {
            principal,
            capability,
            scope,
            effect,
            verify_after: Vec::new(),
            conditions: None,
            autonomy_budget: None,
        }
    }

    fn policy(rules: Vec<PolicyRule>) -> PolicyConfig {
        PolicyConfig {
            version: 1,
            default_agent_effect: PolicyEffect::RequireReview,
            rules,
            tests: Vec::new(),
        }
    }

    fn contracted_model() -> ModelAttributes {
        ModelAttributes {
            name: "fct_orders".to_string(),
            contracted: true,
            ..Default::default()
        }
    }

    fn any_scope() -> PolicyScope {
        PolicyScope {
            any: true,
            ..Default::default()
        }
    }

    fn contracted_scope() -> PolicyScope {
        PolicyScope {
            contracted: Some(true),
            ..Default::default()
        }
    }

    #[test]
    fn read_short_circuits_to_allow_even_with_deny_rule() {
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Read,
            any_scope(),
            PolicyEffect::Deny,
        )]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Read,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
        assert_eq!(d.matched_rule, None);
    }

    #[test]
    fn deny_overrides_a_more_specific_allow() {
        // rule 0: broad deny on any apply; rule 1: specific allow on contracted.
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                any_scope(),
                PolicyEffect::Deny,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                contracted_scope(),
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Deny);
        assert_eq!(d.matched_rule, Some(0));
    }

    #[test]
    fn agent_apply_on_contracted_denied() {
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            contracted_scope(),
            PolicyEffect::Deny,
        )]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Deny);
    }

    #[test]
    fn most_specific_beats_any() {
        // rule 0: any → require_review; rule 1: contracted → allow. The
        // constrained rule strictly supersets the empty `any` set → wins.
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                any_scope(),
                PolicyEffect::RequireReview,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                contracted_scope(),
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
        assert_eq!(d.matched_rule, Some(1));
    }

    #[test]
    fn incomparable_rules_pick_most_restrictive() {
        // model has both a pii classification and layer=silver.
        let attrs = ModelAttributes {
            name: "dim_customer".to_string(),
            layer: Some("silver".to_string()),
            classifications: BTreeSet::from(["pii".to_string()]),
            ..Default::default()
        };
        // rule 0: layer=silver → allow ({Layer}); rule 1: classifications=[pii]
        // → require_review ({Classifications}). Incomparable → most-restrictive.
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                PolicyScope {
                    layer: Some("silver".to_string()),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                PolicyScope {
                    classifications: vec!["pii".to_string()],
                    ..Default::default()
                },
                PolicyEffect::RequireReview,
            ),
        ]);
        let d = evaluate(&p, PolicyPrincipal::Agent, PolicyCapability::Apply, &attrs);
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert_eq!(d.matched_rule, Some(1));
    }

    #[test]
    fn refinement_rule_outranks_bare_verb() {
        // rule 0: apply on layer=bronze → require_review ({Layer});
        // rule 1: schema_change.additive on layer=bronze → allow
        //         ({Layer, CapabilityRefinement}) strictly supersets rule 0.
        let attrs = ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            ..Default::default()
        };
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::Apply,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    ..Default::default()
                },
                PolicyEffect::RequireReview,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &attrs,
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
        assert_eq!(d.matched_rule, Some(1));
    }

    #[test]
    fn bare_apply_rule_matches_refinement_input() {
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            contracted_scope(),
            PolicyEffect::Deny,
        )]);
        // schema_change.additive input against a bare `apply` deny rule.
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Deny);
    }

    #[test]
    fn refinement_rule_does_not_match_other_refinement() {
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeBreaking,
            any_scope(),
            PolicyEffect::Deny,
        )]);
        // additive input must NOT match a breaking-only rule → default posture.
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert_eq!(d.matched_rule, None);
    }

    #[test]
    fn human_never_gated_by_default() {
        let p = policy(vec![]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Human,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
        assert_eq!(d.matched_rule, None);
    }

    #[test]
    fn agent_default_posture_uses_default_agent_effect() {
        let mut p = policy(vec![]);
        p.default_agent_effect = PolicyEffect::Deny;
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::Deny);
        assert_eq!(d.matched_rule, None);
    }

    #[test]
    fn principal_mismatch_does_not_match() {
        // human rule must not apply to an agent.
        let p = policy(vec![rule(
            PolicyPrincipal::Human,
            PolicyCapability::Apply,
            any_scope(),
            PolicyEffect::Deny,
        )]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &contracted_model(),
        );
        assert_eq!(d.effect, PolicyEffect::RequireReview); // agent default
        assert_eq!(d.matched_rule, None);
    }

    #[test]
    fn exclude_classifications_matches_clean_model() {
        let attrs = ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            ..Default::default() // no classifications
        };
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            PolicyScope {
                layer: Some("bronze".to_string()),
                exclude_classifications: vec!["pii".to_string()],
                ..Default::default()
            },
            PolicyEffect::Allow,
        )]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &attrs,
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
    }

    #[test]
    fn exclude_classifications_unsatisfied_on_pii_model() {
        let attrs = ModelAttributes {
            name: "dim_customer".to_string(),
            classifications: BTreeSet::from(["pii".to_string()]),
            ..Default::default()
        };
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            PolicyScope {
                exclude_classifications: vec!["pii".to_string()],
                ..Default::default()
            },
            PolicyEffect::Allow,
        )]);
        // rule scope not satisfied → no match → agent default posture.
        let d = evaluate(&p, PolicyPrincipal::Agent, PolicyCapability::Apply, &attrs);
        assert_eq!(d.matched_rule, None);
        assert_eq!(d.effect, PolicyEffect::RequireReview);
    }

    // ---------- change classification (§2.4, fail-closed) ----------

    use crate::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};

    fn finding(change: BreakingChange, severity: BreakingSeverity) -> BreakingFinding {
        BreakingFinding { change, severity }
    }

    #[test]
    fn classify_all_additive_is_additive() {
        let f1 = finding(
            BreakingChange::ColumnAdded {
                model: "db.s.t".to_string(),
                column: "c".to_string(),
                data_type: "String".to_string(),
                nullable: true,
            },
            BreakingSeverity::Info,
        );
        let f2 = finding(
            BreakingChange::ModelAdded {
                model: "db.s.t".to_string(),
            },
            BreakingSeverity::Info,
        );
        assert_eq!(
            classify_model_findings(&[&f1, &f2]),
            PolicyCapability::SchemaChangeAdditive
        );
    }

    #[test]
    fn classify_only_sql_body_is_value_change() {
        let f = finding(
            BreakingChange::SqlBodyChanged {
                model: "db.s.t".to_string(),
            },
            BreakingSeverity::Info,
        );
        assert_eq!(
            classify_model_findings(&[&f]),
            PolicyCapability::ValueChange
        );
    }

    #[test]
    fn classify_widening_type_change_fails_closed_to_breaking() {
        // Int64 -> String scores Info (not narrowing) but is NOT additive.
        let f = finding(
            BreakingChange::ColumnTypeChanged {
                model: "db.s.t".to_string(),
                column: "c".to_string(),
                old_type: "Int64".to_string(),
                new_type: "String".to_string(),
                narrowing: false,
            },
            BreakingSeverity::Info,
        );
        assert_eq!(
            classify_model_findings(&[&f]),
            PolicyCapability::SchemaChangeBreaking
        );
    }

    #[test]
    fn classify_additive_plus_body_is_breaking() {
        let add = finding(
            BreakingChange::ColumnAdded {
                model: "db.s.t".to_string(),
                column: "c".to_string(),
                data_type: "String".to_string(),
                nullable: true,
            },
            BreakingSeverity::Info,
        );
        let body = finding(
            BreakingChange::SqlBodyChanged {
                model: "db.s.t".to_string(),
            },
            BreakingSeverity::Info,
        );
        assert_eq!(
            classify_model_findings(&[&add, &body]),
            PolicyCapability::SchemaChangeBreaking
        );
    }

    #[test]
    fn classify_empty_findings_fails_closed_to_breaking() {
        assert_eq!(
            classify_model_findings(&[]),
            PolicyCapability::SchemaChangeBreaking
        );
    }

    #[test]
    fn classify_by_model_groups_and_classifies() {
        let findings = vec![
            finding(
                BreakingChange::ModelAdded {
                    model: "db.s.bronze".to_string(),
                },
                BreakingSeverity::Info,
            ),
            finding(
                BreakingChange::ColumnDropped {
                    model: "db.s.gold".to_string(),
                    column: "id".to_string(),
                    data_type: "Int64".to_string(),
                },
                BreakingSeverity::Breaking,
            ),
        ];
        let by = classify_findings_by_model(&findings);
        assert_eq!(
            by.get("db.s.bronze"),
            Some(&PolicyCapability::SchemaChangeAdditive)
        );
        assert_eq!(
            by.get("db.s.gold"),
            Some(&PolicyCapability::SchemaChangeBreaking)
        );
    }

    #[test]
    fn effect_rank_orders_deny_over_review_over_allow() {
        assert!(effect_rank(PolicyEffect::Deny) > effect_rank(PolicyEffect::RequireReview));
        assert!(effect_rank(PolicyEffect::RequireReview) > effect_rank(PolicyEffect::Allow));
    }

    // ---------- max_downstreams ceiling (§1.4, degrade-not-no-match) ----------

    fn bronze_additive_model(reachable: Option<u64>) -> ModelAttributes {
        ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            reachable_downstreams: reachable,
            ..Default::default()
        }
    }

    /// An `allow …{max_downstreams=5}` rule on a bronze additive model.
    fn ceilinged_allow_policy() -> PolicyConfig {
        policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            PolicyScope {
                layer: Some("bronze".to_string()),
                max_downstreams: Some(5),
                ..Default::default()
            },
            PolicyEffect::Allow,
        )])
    }

    #[test]
    fn max_downstreams_within_ceiling_allows() {
        let d = evaluate(
            &ceilinged_allow_policy(),
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &bronze_additive_model(Some(3)),
        );
        assert_eq!(d.effect, PolicyEffect::Allow);
        assert_eq!(d.matched_rule, Some(0));
    }

    #[test]
    fn max_downstreams_exceeded_degrades_to_require_review() {
        let d = evaluate(
            &ceilinged_allow_policy(),
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &bronze_additive_model(Some(20)),
        );
        // The load-bearing soundness invariant: an over-limit blast radius can
        // never yield `allow`.
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert!(
            d.reason.contains("max_downstreams=5 exceeded"),
            "{}",
            d.reason
        );
    }

    #[test]
    fn max_downstreams_unverifiable_degrades_to_require_review() {
        // Reachability uncomputable (model absent from the compiled graph):
        // fail closed — the ceiling never grants `allow` when it can't verify.
        let d = evaluate(
            &ceilinged_allow_policy(),
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &bronze_additive_model(None),
        );
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert!(d.reason.contains("unverifiable"), "{}", d.reason);
    }

    #[test]
    fn max_downstreams_does_not_soften_a_deny() {
        // A `deny` carrying a ceiling stays `deny` regardless of blast radius —
        // the degrade only ever touches `allow`.
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            PolicyScope {
                layer: Some("bronze".to_string()),
                max_downstreams: Some(5),
                ..Default::default()
            },
            PolicyEffect::Deny,
        )]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            &bronze_additive_model(Some(9999)),
        );
        assert_eq!(d.effect, PolicyEffect::Deny);
    }

    #[test]
    fn ceilinged_allow_does_not_leak_via_equal_specificity_sibling() {
        // A broad ceilingless `allow` next to an equally-specific ceilinged
        // `allow` must not grant `allow` for an oversized blast radius: the
        // ceilinged rule degrades to `require_review` and, being equally
        // specific, wins the tie on most-restrictive-effect.
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    max_downstreams: Some(5),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &bronze_additive_model(Some(20)),
        );
        assert_eq!(d.effect, PolicyEffect::RequireReview);
        assert_eq!(d.matched_rule, Some(1));
    }

    /// 🔴 Sticky safety cap — the load-bearing false-allow fix. A ceilinged
    /// `allow` (rule 0, `max_downstreams=5`) is *dominated* by a more-specific
    /// ungated sibling `allow` (rule 1, a strict-superset scope with no
    /// ceiling). The non-dominated filter drops the degraded rule 0, so the
    /// winner is rule 1's ungated `allow`. Without the sticky cap this yields
    /// `allow` for a blast radius of 20 that a matching ceiling of 5 forbids.
    /// The cap must force the final effect to `require_review`.
    #[test]
    fn sticky_cap_more_specific_sibling_allow_cannot_bypass_breached_ceiling() {
        let attrs = ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            classifications: BTreeSet::from(["public".to_string()]),
            reachable_downstreams: Some(20),
            ..Default::default()
        };
        // rule 0: allow {layer=bronze, max_downstreams=5}  ({Layer}, degrades)
        // rule 1: allow {layer=bronze, classifications=[public]}
        //         ({Layer, Classifications}) — strict superset dominates rule 0.
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    max_downstreams: Some(5),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    classifications: vec!["public".to_string()],
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &attrs,
        );
        assert_eq!(
            d.effect,
            PolicyEffect::RequireReview,
            "a breached ceiling on a matched rule must cap the final effect, \
             even when a more-specific ungated sibling allow wins: {}",
            d.reason
        );
        assert!(
            d.reason.contains("sticky safety cap")
                && d.reason.contains("max_downstreams=5 exceeded"),
            "reason must name the sticky cap and the breach: {}",
            d.reason
        );
    }

    /// The cap is *only* a cap: a non-breached ceiling still allows. Same two
    /// rules as above, but the blast radius (3) is within rule 0's ceiling (5),
    /// so no candidate degraded and the more-specific sibling `allow` stands.
    #[test]
    fn sticky_cap_non_breached_ceiling_still_allows() {
        let attrs = ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            classifications: BTreeSet::from(["public".to_string()]),
            reachable_downstreams: Some(3),
            ..Default::default()
        };
        let p = policy(vec![
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    max_downstreams: Some(5),
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
            rule(
                PolicyPrincipal::Agent,
                PolicyCapability::SchemaChangeAdditive,
                PolicyScope {
                    layer: Some("bronze".to_string()),
                    classifications: vec!["public".to_string()],
                    ..Default::default()
                },
                PolicyEffect::Allow,
            ),
        ]);
        let d = evaluate(
            &p,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &attrs,
        );
        assert_eq!(
            d.effect,
            PolicyEffect::Allow,
            "a within-ceiling blast radius must still allow: {}",
            d.reason
        );
        assert!(
            !d.reason.contains("sticky safety cap"),
            "no cap should fire when nothing breached: {}",
            d.reason
        );
    }

    #[test]
    fn models_glob_selector_matches() {
        let attrs = ModelAttributes {
            name: "stg_orders".to_string(),
            ..Default::default()
        };
        let p = policy(vec![rule(
            PolicyPrincipal::Agent,
            PolicyCapability::Apply,
            PolicyScope {
                models: vec!["stg_*".to_string()],
                ..Default::default()
            },
            PolicyEffect::Deny,
        )]);
        let d = evaluate(&p, PolicyPrincipal::Agent, PolicyCapability::Apply, &attrs);
        assert_eq!(d.effect, PolicyEffect::Deny);
    }

    // ---------- autonomy budgets + freeze (dynamic tightening) ----------

    use crate::config::AutonomyBudget;
    use crate::state::PolicyDecisionRecord;

    fn now() -> DateTime<Utc> {
        Utc::now()
    }

    /// A plain evaluation row: winning `rule_id`, empty `verify_after`.
    fn plain_row(plan_id: &str, rule_id: Option<usize>, ts: DateTime<Utc>) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: ts,
            plan_id: plan_id.to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: "raw_events".to_string(),
            effect: PolicyEffect::Allow,
            rule_id,
            reason: "plain".to_string(),
            verify_after: Vec::new(),
        }
    }

    /// A verify-after custody row: non-empty `verify_after`, `effect = deny`
    /// means the post-apply verification FAILED (burns budget).
    fn verify_fail_row(plan_id: &str, ts: DateTime<Utc>) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: ts,
            plan_id: plan_id.to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: "*".to_string(),
            effect: PolicyEffect::Deny,
            rule_id: None,
            reason: "verify_after FAILED".to_string(),
            verify_after: vec!["row_count_drift".to_string()],
        }
    }

    /// A passing verify-after custody row — must NOT burn budget.
    fn verify_pass_row(plan_id: &str, ts: DateTime<Utc>) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: ts,
            plan_id: plan_id.to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: "*".to_string(),
            effect: PolicyEffect::Allow,
            rule_id: None,
            reason: "verify_after passed".to_string(),
            verify_after: vec!["row_count_drift".to_string()],
        }
    }

    fn freeze_row(
        prefix: &str,
        principal: PolicyPrincipal,
        scope: &str,
        ts: DateTime<Utc>,
    ) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: ts,
            plan_id: format!("{prefix}{}", ts.to_rfc3339()),
            principal,
            capability: PolicyCapability::Apply,
            model: scope.to_string(),
            effect: if prefix == FREEZE_PLAN_PREFIX {
                PolicyEffect::Deny
            } else {
                PolicyEffect::Allow
            },
            rule_id: None,
            reason: "freeze".to_string(),
            verify_after: Vec::new(),
        }
    }

    /// A one-failure `n`-plan burn history: `n` failing applies, each with a
    /// plain row for `rule_idx` plus a verify-after failure custody row.
    fn burn_history(n: usize, rule_idx: usize, base: DateTime<Utc>) -> Vec<PolicyDecisionRecord> {
        let mut v = Vec::new();
        for i in 0..n {
            let plan = format!("plan-{i}");
            let ts = base - chrono::Duration::hours(i as i64);
            v.push(plain_row(&plan, Some(rule_idx), ts));
            v.push(verify_fail_row(&plan, ts));
        }
        v
    }

    fn budget_policy(failures: u64, window: &str) -> PolicyConfig {
        let mut rule = rule(
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            PolicyScope {
                layer: Some("bronze".to_string()),
                ..Default::default()
            },
            PolicyEffect::Allow,
        );
        rule.autonomy_budget = Some(AutonomyBudget {
            failures,
            window: window.to_string(),
        });
        policy(vec![rule])
    }

    /// Off-by-one: a budget of N trips on the N-th failure, not before, not
    /// after. This is the load-bearing exhaustion boundary.
    #[test]
    fn budget_degrades_exactly_at_exhaustion() {
        let n = now();
        let p = budget_policy(3, "7d");
        let attrs = bronze_additive_model(Some(0));

        // Two failures: budget intact, allow stands.
        let two = burn_history(2, 0, n);
        let (effect, deg) = autonomy_degradation(
            PolicyEffect::Allow,
            Some(0),
            &p,
            PolicyPrincipal::Agent,
            &attrs,
            &two,
            n,
        );
        assert_eq!(effect, PolicyEffect::Allow, "2 < 3 must not degrade");
        assert_eq!(deg, AutonomyDegradation::None);

        // Third failure: budget exhausted, allow degrades to require_review.
        let three = burn_history(3, 0, n);
        let (effect, deg) = autonomy_degradation(
            PolicyEffect::Allow,
            Some(0),
            &p,
            PolicyPrincipal::Agent,
            &attrs,
            &three,
            n,
        );
        assert_eq!(effect, PolicyEffect::RequireReview, "3 >= 3 must degrade");
        assert!(matches!(
            deg,
            AutonomyDegradation::BudgetExhausted {
                failures: 3,
                limit: 3,
                ..
            }
        ));
    }

    /// The core soundness invariant: for *every* combination of base effect,
    /// freeze state, and budget state, the dynamic layer never widens — the
    /// output is always at least as restrictive as the base. A budget/freeze
    /// can only ever make a rule more restrictive, never less.
    #[test]
    fn autonomy_degradation_never_widens() {
        let n = now();
        let attrs = bronze_additive_model(Some(0));
        let exhausted = burn_history(5, 0, n); // well past a 3-budget
        let frozen = vec![freeze_row(
            FREEZE_PLAN_PREFIX,
            PolicyPrincipal::Agent,
            "any",
            n - chrono::Duration::hours(1),
        )];
        let mut both = exhausted.clone();
        both.extend(frozen.clone());

        let bases = [
            PolicyEffect::Allow,
            PolicyEffect::RequireReview,
            PolicyEffect::Deny,
        ];
        let ledgers: [&[PolicyDecisionRecord]; 4] = [&[], &exhausted, &frozen, &both];

        for base in bases {
            for ledger in ledgers {
                let p = budget_policy(3, "7d");
                let (out, _deg) = autonomy_degradation(
                    base,
                    Some(0),
                    &p,
                    PolicyPrincipal::Agent,
                    &attrs,
                    ledger,
                    n,
                );
                assert!(
                    restrictiveness(out) >= restrictiveness(base),
                    "widened base {base:?} -> {out:?} (a dynamic breaker must never loosen)"
                );
            }
        }
    }

    /// A passing verify-after custody row must not burn budget, and the count
    /// is over distinct plans.
    #[test]
    fn budget_burn_ignores_passes_and_dedups_plans() {
        let n = now();
        let window = chrono::Duration::days(7);
        let mut ledger = burn_history(2, 0, n); // 2 failing plans
        // A passing custody row + its plain row: must not count.
        ledger.push(plain_row("plan-pass", Some(0), n));
        ledger.push(verify_pass_row("plan-pass", n));
        // A duplicate plain row for an already-counted failing plan: still 1.
        ledger.push(plain_row("plan-0", Some(0), n));

        assert_eq!(budget_failures_in_window(&ledger, 0, window, n), 2);
        // A rule that never won any failing plan burns nothing.
        assert_eq!(budget_failures_in_window(&ledger, 9, window, n), 0);
    }

    /// Failures older than the window do not count.
    #[test]
    fn budget_burn_respects_window() {
        let n = now();
        let window = chrono::Duration::days(7);
        // One failure inside the window, one well outside it.
        let ledger = vec![
            plain_row("recent", Some(0), n - chrono::Duration::days(1)),
            verify_fail_row("recent", n - chrono::Duration::days(1)),
            plain_row("stale", Some(0), n - chrono::Duration::days(30)),
            verify_fail_row("stale", n - chrono::Duration::days(30)),
        ];
        assert_eq!(budget_failures_in_window(&ledger, 0, window, n), 1);
    }

    /// A freeze forces deny even when no rule matched (default posture).
    #[test]
    fn freeze_forces_deny_over_default_posture() {
        let n = now();
        let attrs = bronze_additive_model(Some(0));
        let ledger = vec![freeze_row(
            FREEZE_PLAN_PREFIX,
            PolicyPrincipal::Agent,
            "any",
            n,
        )];
        let p = policy(vec![]);
        let (effect, deg) = autonomy_degradation(
            PolicyEffect::RequireReview, // agent default posture
            None,                        // no rule matched
            &p,
            PolicyPrincipal::Agent,
            &attrs,
            &ledger,
            n,
        );
        assert_eq!(effect, PolicyEffect::Deny);
        assert!(matches!(deg, AutonomyDegradation::Frozen { .. }));
    }

    /// A scoped freeze only bites models its selector matches.
    #[test]
    fn freeze_scope_selector_is_honoured() {
        let n = now();
        let p = policy(vec![]);
        let ledger = vec![freeze_row(
            FREEZE_PLAN_PREFIX,
            PolicyPrincipal::Agent,
            "layer=bronze",
            n,
        )];

        let bronze = ModelAttributes {
            name: "raw_events".to_string(),
            layer: Some("bronze".to_string()),
            ..Default::default()
        };
        let silver = ModelAttributes {
            name: "dim_customer".to_string(),
            layer: Some("silver".to_string()),
            ..Default::default()
        };

        let (bronze_effect, _) = autonomy_degradation(
            PolicyEffect::Allow,
            None,
            &p,
            PolicyPrincipal::Agent,
            &bronze,
            &ledger,
            n,
        );
        let (silver_effect, _) = autonomy_degradation(
            PolicyEffect::Allow,
            None,
            &p,
            PolicyPrincipal::Agent,
            &silver,
            &ledger,
            n,
        );
        assert_eq!(bronze_effect, PolicyEffect::Deny, "bronze is frozen");
        assert_eq!(silver_effect, PolicyEffect::Allow, "silver is not frozen");
    }

    /// A freeze targeting the agent does not bite a human principal.
    #[test]
    fn freeze_is_per_principal() {
        let n = now();
        let attrs = bronze_additive_model(Some(0));
        let ledger = vec![freeze_row(
            FREEZE_PLAN_PREFIX,
            PolicyPrincipal::Agent,
            "any",
            n,
        )];
        let p = policy(vec![]);
        let (human_effect, _) = autonomy_degradation(
            PolicyEffect::Allow,
            None,
            &p,
            PolicyPrincipal::Human,
            &attrs,
            &ledger,
            n,
        );
        assert_eq!(
            human_effect,
            PolicyEffect::Allow,
            "an agent freeze must not bite a human"
        );
    }

    /// A later unfreeze supersedes an earlier freeze on the same key.
    #[test]
    fn unfreeze_supersedes_freeze() {
        let n = now();
        let ledger = vec![
            freeze_row(
                FREEZE_PLAN_PREFIX,
                PolicyPrincipal::Agent,
                "any",
                n - chrono::Duration::hours(2),
            ),
            freeze_row(
                UNFREEZE_PLAN_PREFIX,
                PolicyPrincipal::Agent,
                "any",
                n - chrono::Duration::hours(1),
            ),
        ];
        assert!(
            active_freezes(&ledger).is_empty(),
            "an unfreeze must supersede the freeze"
        );

        // A re-freeze after the unfreeze is active again.
        let mut ledger = ledger;
        ledger.push(freeze_row(
            FREEZE_PLAN_PREFIX,
            PolicyPrincipal::Agent,
            "any",
            n,
        ));
        assert_eq!(active_freezes(&ledger).len(), 1);
    }

    #[test]
    fn scope_selector_grammar_validates() {
        assert!(validate_scope_selector("any").is_ok());
        assert!(validate_scope_selector("layer=bronze").is_ok());
        assert!(validate_scope_selector("model=stg_*").is_ok());
        assert!(validate_scope_selector("classification=pii").is_ok());
        assert!(validate_scope_selector("tag=finance").is_ok());
        assert!(validate_scope_selector("tag=env=prod").is_ok());
        assert!(validate_scope_selector("layer=").is_err());
        assert!(validate_scope_selector("tag==prod").is_err());
        assert!(validate_scope_selector("nonsense").is_err());
    }
}
