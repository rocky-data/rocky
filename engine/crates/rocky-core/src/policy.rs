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
    /// Downstream-consumer count. Surfaced for context; `scope.max_downstreams`
    /// is parse-only in v0, so this does not affect matching.
    pub downstreams: u64,
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
/// `scope.any` yields the empty set. `max_downstreams` is parse-only in
/// v0 and never contributes a constraint or a match failure.
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
    // `max_downstreams` is parse-only in v0: neither required nor counted.
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
        // 2. Deny is a hard override — any deny match wins.
        if rule.effect == PolicyEffect::Deny {
            first_deny.get_or_insert(idx);
        }
        candidates.push(Candidate {
            idx,
            effect: rule.effect,
            constraints,
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

    let reason = if non_dominated.len() == 1 {
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

    PolicyDecision {
        effect: winner.effect,
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
            conditions: None,
        }
    }

    fn policy(rules: Vec<PolicyRule>) -> PolicyConfig {
        PolicyConfig {
            version: 1,
            default_agent_effect: PolicyEffect::RequireReview,
            rules,
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
}
