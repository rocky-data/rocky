//! Eligibility gate for policy-governed auto-apply of additive source drift.
//!
//! When an upstream source gains a new nullable column, the run loop can
//! migrate the target table (an `ALTER TABLE ADD COLUMN`) on its own — but
//! *only* when the change is provably additive and a policy rule grants the
//! `schema_change.additive` capability for that scope. This module owns the
//! single predicate that decides whether a detected drift may be applied
//! automatically, and it is deliberately the **only** place that answer is
//! computed.
//!
//! # The one dangerous direction
//!
//! A breaking change misclassified as additive and auto-applied is a silent
//! downstream break. So [`is_auto_apply_eligible`] fails closed at every gate:
//! it returns [`AutoApplyVerdict::Apply`] only when *all* of the following
//! hold, and [`AutoApplyVerdict::Refuse`] otherwise (⇒ fall back to
//! propose / require-review):
//!
//! 1. The diff is non-empty (an empty diff cannot *prove* additive).
//! 2. Every finding is a nullable column addition or a new model — proven
//!    from the diff itself, independent of the caller-supplied
//!    classification, so a wrong classification can never let a drop / retype
//!    / narrowing through.
//! 3. The classification is [`PolicyCapability::SchemaChangeAdditive`]
//!    (defence in depth — it is derived from the same findings).
//! 4. The model does not sit behind a contract (a contract boundary is never
//!    crossed automatically).
//! 5. The policy verdict for `schema_change.additive` is
//!    [`PolicyEffect::Allow`].
//!
//! A false rebuild costs compute; a false auto-apply costs correctness — so
//! anything not provably additive stays behind review.

use std::fmt;

use rocky_ir::{DriftAction, DriftResult};

use crate::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
use crate::config::{PolicyCapability, PolicyEffect};

/// Why a detected drift was refused for auto-apply. Each variant is a
/// fail-closed exit from [`is_auto_apply_eligible`]; the display string is
/// suitable for an operator-facing message and for the custody ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefuseReason {
    /// The diff was empty — additive-ness cannot be proven, so refuse.
    NoFindings,
    /// At least one finding is not a nullable column addition or a new model
    /// (e.g. a dropped, retyped, narrowed, or non-null column) — not additive.
    NotAdditive,
    /// The change classification was not `schema_change.additive`.
    ClassificationNotAdditive(PolicyCapability),
    /// The model sits behind a contract; a contract boundary is never crossed
    /// automatically.
    ContractBoundary,
    /// The policy verdict for `schema_change.additive` was not `allow`.
    PolicyNotAllow(PolicyEffect),
}

impl fmt::Display for RefuseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RefuseReason::NoFindings => {
                write!(
                    f,
                    "no schema-diff findings — additive change cannot be proven"
                )
            }
            RefuseReason::NotAdditive => write!(
                f,
                "drift is not provably additive (a drop, retype, narrowing, or non-null \
                 addition was present)"
            ),
            RefuseReason::ClassificationNotAdditive(cap) => {
                write!(
                    f,
                    "change classified as {cap:?}, not schema_change.additive"
                )
            }
            RefuseReason::ContractBoundary => {
                write!(
                    f,
                    "model is behind a contract — a contract boundary is not crossed automatically"
                )
            }
            RefuseReason::PolicyNotAllow(effect) => {
                write!(
                    f,
                    "policy resolved schema_change.additive to {effect:?}, not allow"
                )
            }
        }
    }
}

/// The resolved decision for a detected drift.
///
/// [`Apply`](AutoApplyVerdict::Apply) is the *only* value that authorises an
/// automatic warehouse mutation; every other outcome is a
/// [`Refuse`](AutoApplyVerdict::Refuse) carrying the fail-closed reason.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutoApplyVerdict {
    /// Provably additive, contract-clean, and policy-allowed — auto-apply may
    /// proceed.
    Apply,
    /// Not eligible for auto-apply; fall back to propose / require-review.
    Refuse(RefuseReason),
}

impl AutoApplyVerdict {
    /// `true` only for [`AutoApplyVerdict::Apply`].
    pub fn is_apply(&self) -> bool {
        matches!(self, AutoApplyVerdict::Apply)
    }

    /// The refusal reason, or `None` when the verdict is `Apply`.
    pub fn refuse_reason(&self) -> Option<&RefuseReason> {
        match self {
            AutoApplyVerdict::Apply => None,
            AutoApplyVerdict::Refuse(r) => Some(r),
        }
    }
}

/// Decide whether a detected schema drift may be auto-applied.
///
/// This is the soundness surface for governed auto-apply. It fails closed:
/// the caller-supplied `classification` and `effect` are *necessary* but never
/// *sufficient* — additive-ness is re-proven from `findings` here, so a
/// mislabelled breaking diff can never be applied even if `classification`
/// says additive and `effect` says allow.
///
/// See the module docs for the full gate ordering.
pub fn is_auto_apply_eligible(
    findings: &[BreakingFinding],
    classification: PolicyCapability,
    effect: PolicyEffect,
    contracted: bool,
) -> AutoApplyVerdict {
    // 1. An empty diff cannot prove an additive change.
    if findings.is_empty() {
        return AutoApplyVerdict::Refuse(RefuseReason::NoFindings);
    }
    // 2. Prove additive-ness from the diff itself, before trusting any label.
    if !findings.iter().all(is_additive_finding) {
        return AutoApplyVerdict::Refuse(RefuseReason::NotAdditive);
    }
    // 3. The classification must agree (defence in depth).
    if classification != PolicyCapability::SchemaChangeAdditive {
        return AutoApplyVerdict::Refuse(RefuseReason::ClassificationNotAdditive(classification));
    }
    // 4. Never cross a contract boundary automatically.
    if contracted {
        return AutoApplyVerdict::Refuse(RefuseReason::ContractBoundary);
    }
    // 5. Policy must grant the additive capability.
    if effect != PolicyEffect::Allow {
        return AutoApplyVerdict::Refuse(RefuseReason::PolicyNotAllow(effect));
    }
    AutoApplyVerdict::Apply
}

/// A single finding is additive iff it is a *nullable* column addition or a
/// brand-new model — and its severity is [`BreakingSeverity::Info`]. A
/// non-null addition (a `Warning`) is not safe to apply blind (it needs a
/// default for existing rows), and every other change kind is non-additive.
fn is_additive_finding(finding: &BreakingFinding) -> bool {
    finding.severity == BreakingSeverity::Info
        && matches!(
            finding.change,
            BreakingChange::ColumnAdded { nullable: true, .. } | BreakingChange::ModelAdded { .. }
        )
}

/// Translate a detected [`DriftResult`] into the typed
/// [`BreakingFinding`]s the classifier and the eligibility gate consume, so
/// source→target replication drift is judged by the exact same machinery as
/// a model-level `ci-diff`.
///
/// Every non-additive aspect of the drift is surfaced as a non-additive
/// finding, so a diff that reaches [`is_auto_apply_eligible`] all-additive is
/// genuinely a pure addition:
///
/// - `added_columns` → [`BreakingChange::ColumnAdded`] (additive only when
///   nullable).
/// - `drifted_columns` (type changes) → [`BreakingChange::ColumnTypeChanged`],
///   always `Breaking`: a type change is never additive, and without the
///   typed IR here we fail closed rather than guess a safe widening.
/// - `columns_to_drop` and `grace_period_columns` (source-side removals) →
///   [`BreakingChange::ColumnDropped`], `Breaking`.
///
/// `model` is the identifier the findings are attributed to (the target
/// table's name); it only labels the findings.
pub fn drift_findings(drift: &DriftResult, model: &str) -> Vec<BreakingFinding> {
    let mut findings = Vec::new();

    for col in &drift.added_columns {
        let severity = if col.nullable {
            BreakingSeverity::Info
        } else {
            BreakingSeverity::Warning
        };
        findings.push(BreakingFinding {
            change: BreakingChange::ColumnAdded {
                model: model.to_string(),
                column: col.name.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
            },
            severity,
        });
    }

    for col in &drift.drifted_columns {
        findings.push(BreakingFinding {
            change: BreakingChange::ColumnTypeChanged {
                model: model.to_string(),
                column: col.name.clone(),
                old_type: col.target_type.clone(),
                new_type: col.source_type.clone(),
                // No typed IR at the replication boundary — treat any type
                // change as the more-severe narrowing so it can never be
                // mistaken for additive.
                narrowing: true,
            },
            severity: BreakingSeverity::Breaking,
        });
    }

    for col in &drift.columns_to_drop {
        findings.push(BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: model.to_string(),
                column: col.to_string(),
                data_type: String::new(),
            },
            severity: BreakingSeverity::Breaking,
        });
    }

    for col in &drift.grace_period_columns {
        findings.push(BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: model.to_string(),
                column: col.name.clone(),
                data_type: col.data_type.clone(),
            },
            severity: BreakingSeverity::Breaking,
        });
    }

    findings
}

/// A one-line, human-readable summary of what a drift would change, for the
/// custody ledger and operator messages. Names the additive columns first,
/// then any non-additive aspects.
pub fn drift_summary(drift: &DriftResult) -> String {
    let mut parts: Vec<String> = Vec::new();
    for col in &drift.added_columns {
        let nullness = if col.nullable { "nullable" } else { "not-null" };
        parts.push(format!(
            "added {nullness} column '{}' ({})",
            col.name, col.data_type
        ));
    }
    for col in &drift.drifted_columns {
        parts.push(format!(
            "column '{}' type {} → {}",
            col.name, col.target_type, col.source_type
        ));
    }
    for col in &drift.columns_to_drop {
        parts.push(format!("dropped column '{col}'"));
    }
    for col in &drift.grace_period_columns {
        parts.push(format!("source removed column '{}' (grace)", col.name));
    }
    if matches!(drift.action, DriftAction::DropAndRecreate) && parts.is_empty() {
        parts.push("incompatible change requiring drop-and-recreate".to_string());
    }
    if parts.is_empty() {
        "no schema change".to_string()
    } else {
        parts.join("; ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_ir::{ColumnInfo, DriftAction, DriftResult, DriftedColumn, TableRef};

    fn tref() -> TableRef {
        TableRef {
            catalog: "wh".to_string(),
            schema: "raw".to_string(),
            table: "orders".to_string(),
        }
    }

    fn added_col_drift(name: &str, ty: &str, nullable: bool) -> DriftResult {
        DriftResult {
            table: tref(),
            drifted_columns: Vec::new(),
            action: DriftAction::Ignore,
            added_columns: vec![ColumnInfo {
                name: name.to_string(),
                data_type: ty.to_string(),
                nullable,
            }],
            grace_period_columns: Vec::new(),
            columns_to_drop: Vec::new(),
        }
    }

    fn retype_drift() -> DriftResult {
        DriftResult {
            table: tref(),
            drifted_columns: vec![DriftedColumn {
                name: "amount".to_string(),
                source_type: "INT".to_string(),
                target_type: "BIGINT".to_string(),
            }],
            action: DriftAction::DropAndRecreate,
            added_columns: Vec::new(),
            grace_period_columns: Vec::new(),
            columns_to_drop: Vec::new(),
        }
    }

    // ---- drift_findings mapping ----

    #[test]
    fn added_nullable_column_maps_to_info_additive() {
        let f = drift_findings(&added_col_drift("email", "VARCHAR", true), "orders");
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].severity, BreakingSeverity::Info);
        assert!(is_additive_finding(&f[0]));
    }

    #[test]
    fn added_not_null_column_maps_to_warning_not_additive() {
        let f = drift_findings(&added_col_drift("email", "VARCHAR", false), "orders");
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].severity, BreakingSeverity::Warning);
        assert!(!is_additive_finding(&f[0]));
    }

    #[test]
    fn type_change_maps_to_breaking() {
        let f = drift_findings(&retype_drift(), "orders");
        assert_eq!(f.len(), 1);
        assert_eq!(f[0].severity, BreakingSeverity::Breaking);
        assert!(matches!(
            f[0].change,
            BreakingChange::ColumnTypeChanged {
                narrowing: true,
                ..
            }
        ));
    }

    #[test]
    fn columns_to_drop_map_to_breaking_drops() {
        let mut d = added_col_drift("email", "VARCHAR", true);
        d.columns_to_drop = vec![std::sync::Arc::from("legacy")];
        let f = drift_findings(&d, "orders");
        // one additive add + one breaking drop
        assert_eq!(f.len(), 2);
        assert!(
            f.iter()
                .any(|x| matches!(x.change, BreakingChange::ColumnDropped { .. })
                    && x.severity == BreakingSeverity::Breaking)
        );
    }

    // ---- the soundness predicate ----

    fn additive() -> Vec<BreakingFinding> {
        drift_findings(&added_col_drift("email", "VARCHAR", true), "orders")
    }

    #[test]
    fn pure_additive_allowed_is_apply() {
        let v = is_auto_apply_eligible(
            &additive(),
            PolicyCapability::SchemaChangeAdditive,
            PolicyEffect::Allow,
            false,
        );
        assert_eq!(v, AutoApplyVerdict::Apply);
        assert!(v.is_apply());
    }

    #[test]
    fn model_added_allowed_is_apply() {
        let findings = vec![BreakingFinding {
            change: BreakingChange::ModelAdded {
                model: "new_table".to_string(),
            },
            severity: BreakingSeverity::Info,
        }];
        assert_eq!(
            is_auto_apply_eligible(
                &findings,
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            ),
            AutoApplyVerdict::Apply
        );
    }

    // --- non-vacuous: breaking diffs must refuse even when policy allows ---

    #[test]
    fn type_change_refused_even_when_policy_allows() {
        // The load-bearing test: a retyped column is a breaking diff. Even
        // when the caller passes `Allow`, auto-apply must refuse.
        let findings = drift_findings(&retype_drift(), "orders");
        let v = is_auto_apply_eligible(
            &findings,
            // Deliberately mislabel it additive to prove the diff-level gate
            // catches it regardless of the classification.
            PolicyCapability::SchemaChangeAdditive,
            PolicyEffect::Allow,
            false,
        );
        assert!(!v.is_apply());
        assert_eq!(v.refuse_reason(), Some(&RefuseReason::NotAdditive));
    }

    #[test]
    fn column_drop_refused_even_when_policy_allows() {
        let findings = vec![BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: "orders".to_string(),
                column: "amount".to_string(),
                data_type: "BIGINT".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        }];
        let v = is_auto_apply_eligible(
            &findings,
            PolicyCapability::SchemaChangeAdditive,
            PolicyEffect::Allow,
            false,
        );
        assert_eq!(v.refuse_reason(), Some(&RefuseReason::NotAdditive));
    }

    #[test]
    fn narrowing_type_change_refused_even_when_policy_allows() {
        let findings = vec![BreakingFinding {
            change: BreakingChange::ColumnTypeChanged {
                model: "orders".to_string(),
                column: "amount".to_string(),
                old_type: "BIGINT".to_string(),
                new_type: "INT".to_string(),
                narrowing: true,
            },
            severity: BreakingSeverity::Breaking,
        }];
        assert_eq!(
            is_auto_apply_eligible(
                &findings,
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::NotAdditive)
        );
    }

    #[test]
    fn non_null_addition_refused_even_when_policy_allows() {
        let findings = drift_findings(&added_col_drift("email", "VARCHAR", false), "orders");
        assert_eq!(
            is_auto_apply_eligible(
                &findings,
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::NotAdditive)
        );
    }

    #[test]
    fn mixed_additive_and_breaking_refused() {
        // A drift that adds a nullable column AND retypes another is not pure.
        let mut d = added_col_drift("email", "VARCHAR", true);
        d.drifted_columns = vec![DriftedColumn {
            name: "amount".to_string(),
            source_type: "INT".to_string(),
            target_type: "BIGINT".to_string(),
        }];
        d.action = DriftAction::DropAndRecreate;
        let findings = drift_findings(&d, "orders");
        assert_eq!(
            is_auto_apply_eligible(
                &findings,
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::NotAdditive)
        );
    }

    #[test]
    fn grace_period_drop_refused() {
        let mut d = added_col_drift("email", "VARCHAR", true);
        d.grace_period_columns = vec![rocky_ir::GracePeriodColumn {
            name: "legacy".to_string(),
            data_type: "VARCHAR".to_string(),
            first_seen_at: chrono::Utc::now(),
            expires_at: chrono::Utc::now(),
            days_remaining: 3,
        }];
        let findings = drift_findings(&d, "orders");
        assert!(
            !is_auto_apply_eligible(
                &findings,
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            )
            .is_apply()
        );
    }

    // --- the necessary-but-not-sufficient gates ---

    #[test]
    fn additive_but_require_review_refused() {
        assert_eq!(
            is_auto_apply_eligible(
                &additive(),
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::RequireReview,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::PolicyNotAllow(PolicyEffect::RequireReview))
        );
    }

    #[test]
    fn additive_but_denied_refused() {
        assert_eq!(
            is_auto_apply_eligible(
                &additive(),
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Deny,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::PolicyNotAllow(PolicyEffect::Deny))
        );
    }

    #[test]
    fn additive_but_contracted_refused() {
        assert_eq!(
            is_auto_apply_eligible(
                &additive(),
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                true,
            )
            .refuse_reason(),
            Some(&RefuseReason::ContractBoundary)
        );
    }

    #[test]
    fn additive_diff_but_breaking_classification_refused() {
        // Defence in depth: even a genuinely additive diff refuses when the
        // classification disagrees (a caller bug must not open the gate).
        assert_eq!(
            is_auto_apply_eligible(
                &additive(),
                PolicyCapability::SchemaChangeBreaking,
                PolicyEffect::Allow,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::ClassificationNotAdditive(
                PolicyCapability::SchemaChangeBreaking
            ))
        );
    }

    #[test]
    fn empty_diff_refused() {
        assert_eq!(
            is_auto_apply_eligible(
                &[],
                PolicyCapability::SchemaChangeAdditive,
                PolicyEffect::Allow,
                false,
            )
            .refuse_reason(),
            Some(&RefuseReason::NoFindings)
        );
    }

    // --- classification agreement with the shared classifier ---

    #[test]
    fn additive_findings_classify_as_additive() {
        let findings = additive();
        let refs: Vec<&BreakingFinding> = findings.iter().collect();
        assert_eq!(
            crate::policy::classify_model_findings(&refs),
            PolicyCapability::SchemaChangeAdditive
        );
    }

    #[test]
    fn drop_recreate_findings_classify_as_breaking() {
        let findings = drift_findings(&retype_drift(), "orders");
        let refs: Vec<&BreakingFinding> = findings.iter().collect();
        assert_eq!(
            crate::policy::classify_model_findings(&refs),
            PolicyCapability::SchemaChangeBreaking
        );
    }

    // --- summary ---

    #[test]
    fn summary_names_additive_column() {
        let s = drift_summary(&added_col_drift("email", "VARCHAR", true));
        assert!(s.contains("added nullable column 'email'"), "{s}");
    }

    #[test]
    fn summary_names_type_change() {
        let s = drift_summary(&retype_drift());
        assert!(s.contains("amount"), "{s}");
        assert!(s.contains("BIGINT"), "{s}");
    }
}
