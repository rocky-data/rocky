//! Run-loop governance for auto-applying additive source drift.
//!
//! Default-off. When `[resilience] auto_apply_additive_drift = true` **and** a
//! `[policy]` rule grants the `schema_change.additive` capability for a
//! table's scope, the replication run loop may migrate a target on its own
//! authority — but only for a *provably additive* change. Every drift
//! mutation the run would otherwise apply unconditionally is first routed
//! through [`DriftGovernor::govern`], which:
//!
//! 1. converts the detected [`DriftResult`] to typed breaking-change findings,
//! 2. classifies them and asks [`rocky_core::auto_apply::is_auto_apply_eligible`]
//!    whether the change is provably additive, contract-clean, and
//!    policy-allowed,
//! 3. records a custody entry either way, and
//! 4. returns [`GovernDecision::Apply`] (proceed with the mutation) or
//!    [`GovernDecision::Refuse`] (do **not** mutate — the run surfaces a
//!    require-review failure for that table).
//!
//! After the run, [`finalize_drift_verify_after`] runs the post-apply
//! `verify_after` gate over every table that was auto-migrated: it reads the
//! run's recorded check outcomes and halts (recording a `deny` custody row) if
//! a required check failed or did not run. There is no rollback substrate on a
//! plain warehouse target, so a failed verification is halt-only — the
//! migration has already landed and stays until a human reverts it.

use anyhow::{Result, bail};
use tokio::sync::Mutex;
use tracing::warn;

use rocky_core::auto_apply::{self, RefuseReason};
use rocky_core::breaking_change::BreakingFinding;
use rocky_core::config::{
    PolicyCapability, PolicyConfig, PolicyEffect, PolicyPrincipal, RockyConfig,
};
use rocky_core::policy::{self, ModelAttributes};
use rocky_core::state::{AutoApplyCustody, PolicyDecisionRecord, StateStore};
use rocky_ir::DriftResult;

/// The wire spelling of a change-classification capability, for custody rows.
fn capability_wire(cap: PolicyCapability) -> &'static str {
    match cap {
        PolicyCapability::SchemaChangeAdditive => "schema_change.additive",
        PolicyCapability::SchemaChangeBreaking => "schema_change.breaking",
        PolicyCapability::ValueChange => "value_change",
        _ => "apply",
    }
}

/// The outcome of governing one detected drift.
pub(crate) enum GovernDecision {
    /// Provably additive + policy-allowed — the caller proceeds with the
    /// mutation exactly as the ungoverned path would.
    Apply,
    /// Not eligible — the caller must **not** mutate. The string is an
    /// operator-facing reason; the run surfaces it as a require-review failure.
    Refuse(String),
}

/// Per-table governor for additive-drift auto-apply, built once per table when
/// the opt-in is set. Carries the policy verdict for `schema_change.additive`
/// on that table's scope so [`Self::govern`] can combine it with the actual
/// detected drift.
///
/// Present on a [`TableTask`](super::run) only when both the config opt-in and
/// a `[policy]` block are configured; absent it, the drift path is untouched
/// and byte-identical to today.
#[derive(Debug, Clone)]
pub(crate) struct DriftGovernor {
    run_id: String,
    /// The *static* policy effect resolved at build time. [`Self::govern`]
    /// tightens it with the ledger-derived breakers (active freezes, autonomy
    /// budget burn) before deciding — the same
    /// [`policy::autonomy_degradation`] step the shared apply seam runs.
    effect: PolicyEffect,
    matched_rule: Option<usize>,
    policy_reason: String,
    verify_after: Vec<String>,
    contracted: bool,
    /// The `[policy]` block, kept so `govern` can project the winning rule's
    /// autonomy budget over the decision ledger. `None` when the governor was
    /// built in the no-`[policy]` fail-closed posture (nothing is granted, so
    /// there is no budget to project).
    policy: Option<PolicyConfig>,
    /// The attributes the static decision was evaluated against — reused for
    /// the dynamic breakers so freeze scope selectors match the same identity
    /// the `[policy]` scope matched.
    attrs: ModelAttributes,
    /// Whether the decision ledger this run opened is authoritative. `false`
    /// when the local store was bootstrapped fresh for a forward-incompatible
    /// schema (`on_schema_mismatch = recreate`) or an authoritative remote
    /// state download failed — in either case an active freeze or an exhausted
    /// budget recorded elsewhere is invisible, so auto-apply must refuse
    /// (fail-closed) rather than proceed with no memory of it.
    ledger_authoritative: bool,
}

impl DriftGovernor {
    /// Build the governor for one table, or `None` when auto-apply is not
    /// enabled for this run (the flag is off or no `[policy]` block exists).
    ///
    /// A replication (bronze) target is not a compiled model, so it is
    /// evaluated against a bare-named [`ModelAttributes`] — the policy scope
    /// matches on the table name, tags/classifications/layer are empty, and
    /// the blast radius is uncomputable (a `max_downstreams` ceiling therefore
    /// fails closed inside the evaluator). `contracted` is `false`: the
    /// bronze layer sits behind no contract.
    /// `ledger_authoritative` is `false` when the state store this run opened
    /// cannot be trusted to carry prior freeze/budget history (recreated for a
    /// forward-incompatible schema, or an authoritative remote download
    /// failed); [`Self::govern`] then refuses every auto-apply (fail-closed).
    pub(crate) fn build(
        cfg: &RockyConfig,
        run_id: &str,
        model_name: &str,
        ledger_authoritative: bool,
    ) -> Option<Self> {
        if !cfg.resilience.auto_apply_additive_drift {
            return None;
        }
        // The opt-in is on. A missing `[policy]` block grants nothing — but it
        // must NOT disable governance, or the pre-existing drift path would
        // mutate UNGOVERNED (fail-open: a breaking retype would silently apply).
        // Govern with a no-grant, fail-closed posture instead: the effect is
        // `require_review`, so `is_auto_apply_eligible` refuses every drift until
        // a `[policy]` rule explicitly grants `schema_change.additive`.
        let attrs = ModelAttributes {
            name: model_name.to_string(),
            ..Default::default()
        };
        let Some(policy) = cfg.policy.as_ref() else {
            return Some(Self {
                run_id: run_id.to_string(),
                effect: PolicyEffect::RequireReview,
                matched_rule: None,
                policy_reason:
                    "auto-apply is enabled but no [policy] block grants schema_change.additive"
                        .to_string(),
                verify_after: Vec::new(),
                contracted: false,
                policy: None,
                attrs,
                ledger_authoritative,
            });
        };
        let decision = policy::evaluate(
            policy,
            PolicyPrincipal::Agent,
            PolicyCapability::SchemaChangeAdditive,
            &attrs,
        );
        let verify_after = decision
            .matched_rule
            .and_then(|i| policy.rules.get(i))
            .map(|r| r.verify_after.clone())
            .unwrap_or_default();
        Some(Self {
            run_id: run_id.to_string(),
            effect: decision.effect,
            matched_rule: decision.matched_rule,
            policy_reason: decision.reason,
            verify_after,
            contracted: false,
            policy: Some(policy.clone()),
            attrs,
            ledger_authoritative,
        })
    }

    /// Govern one detected drift: classify, decide, and record custody.
    ///
    /// `model` is the fully-qualified target name the custody row is attributed
    /// to. The static build-time effect is first tightened by the dynamic
    /// ledger breakers — an active `rocky policy freeze` matching the agent
    /// principal forces `deny`, an exhausted autonomy budget degrades `allow`
    /// to `require_review` — exactly as the shared apply seam does after
    /// `policy::evaluate`. The ledger snapshot is taken per govern call,
    /// before this decision's own row is written. Fail-closed: an unreadable
    /// ledger degrades the effect to at least `require_review` (a possibly
    /// active freeze must never be silently invisible), and anything not
    /// provably additive+allowed is a [`GovernDecision::Refuse`].
    ///
    /// Writes exactly one **plain** decision row (empty `verify_after`,
    /// `rule_id` = the winning rule) under `autoapply:<run_id>`, mirroring the
    /// apply seam's row shape so [`policy::budget_failures_in_window`] can
    /// pair it with the post-run verification row
    /// [`finalize_drift_verify_after`] files under the *same* plan id — the
    /// pairing that lets repeated auto-heal verification failures burn the
    /// granting rule's autonomy budget.
    pub(crate) async fn govern(
        &self,
        drift: &DriftResult,
        model: &str,
        state: &Mutex<StateStore>,
    ) -> GovernDecision {
        let guard = state.lock().await;

        // Dynamic tightening over the prior decision ledger (freeze → deny,
        // exhausted budget → require_review). Monotone: never widens the
        // static effect.
        let (effect, degrade_suffix): (PolicyEffect, Option<String>) = if !self.ledger_authoritative
        {
            // The store this run opened cannot be trusted to carry prior
            // freeze/budget history (recreated for a forward-incompatible
            // schema, or an authoritative remote download failed). An active
            // freeze or exhausted budget recorded elsewhere would be invisible,
            // so refuse auto-apply outright — `require_review` makes
            // `is_auto_apply_eligible` refuse.
            (
                tighten_to_require_review(self.effect),
                Some(
                    "policy ledger non-authoritative (recreated for a forward-incompatible \
                     schema, or an authoritative remote state download failed) — auto-apply \
                     refused (fail-closed)"
                        .to_string(),
                ),
            )
        } else {
            match guard.list_policy_decisions() {
                Ok(prior) => match &self.policy {
                    Some(policy) => {
                        let (effect, degradation) = policy::autonomy_degradation(
                            self.effect,
                            self.matched_rule,
                            policy,
                            PolicyPrincipal::Agent,
                            &self.attrs,
                            &prior,
                            chrono::Utc::now(),
                        );
                        (effect, degradation.reason_suffix())
                    }
                    // No [policy] block: the static posture is already the
                    // fail-closed require-review floor and grants nothing, so
                    // there is no budget (or enforceable freeze) to project.
                    None => (self.effect, None),
                },
                // Fail-closed: with the ledger unreadable, a possibly-active
                // freeze is invisible — never proceed on the static allow.
                Err(e) => (
                    tighten_to_require_review(self.effect),
                    Some(format!(
                        "policy ledger unreadable ({e}) — freeze/budget state unverifiable, \
                         degraded to require_review (fail-closed)"
                    )),
                ),
            }
        };

        let findings = auto_apply::drift_findings(drift, model);
        let refs: Vec<&BreakingFinding> = findings.iter().collect();
        let classification = policy::classify_model_findings(&refs);
        let verdict =
            auto_apply::is_auto_apply_eligible(&findings, classification, effect, self.contracted);
        let applied = verdict.is_apply();
        let summary = auto_apply::drift_summary(drift);

        let (row_effect, mut reason) = if applied {
            let mut reason = format!(
                "auto-applied additive drift on '{model}' ({summary}); {}",
                self.policy_reason
            );
            if !self.verify_after.is_empty() {
                reason.push_str(&format!(
                    "; verify_after due post-run: [{}]",
                    self.verify_after.join(", ")
                ));
            }
            (PolicyEffect::Allow, reason)
        } else {
            let detail = verdict
                .refuse_reason()
                .map(RefuseReason::to_string)
                .unwrap_or_default();
            // The recorded effect for a refusal is the MOST RESTRICTIVE of the
            // eligibility-derived effect and the tightened policy effect. The
            // eligibility gate can trip on a non-policy reason first (e.g.
            // `NotAdditive`), but a freeze that resolved the tightened policy
            // to `deny` must still record `deny` — otherwise a hard freeze on
            // non-additive drift would be logged as a mere `require_review`.
            let eligibility_effect = match verdict.refuse_reason() {
                Some(RefuseReason::PolicyNotAllow(PolicyEffect::Deny)) => PolicyEffect::Deny,
                _ => PolicyEffect::RequireReview,
            };
            let row_effect =
                if policy::effect_rank(effect) >= policy::effect_rank(eligibility_effect) {
                    effect
                } else {
                    eligibility_effect
                };
            (
                row_effect,
                format!("auto-apply of drift on '{model}' refused: {detail} ({summary})"),
            )
        };
        if let Some(suffix) = &degrade_suffix {
            reason.push_str("; ");
            reason.push_str(suffix);
        }

        let record = PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: decision_plan_id(&self.run_id),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: model.to_string(),
            effect: row_effect,
            rule_id: self.matched_rule,
            reason: reason.clone(),
            // A PLAIN evaluation row — the required post-run checks ride on
            // the verification outcome row `finalize_drift_verify_after`
            // files under the same plan id. Keeping this row plain is what
            // lets `budget_failures_in_window` attribute a later verification
            // failure to `rule_id` (its attribution leg only matches rows
            // with an empty `verify_after`).
            verify_after: Vec::new(),
            auto_apply: Some(AutoApplyCustody {
                drift_summary: summary,
                classification: capability_wire(classification).to_string(),
                applied,
                // No rollback substrate on a plain warehouse target — a failed
                // verification is halt-only. A content-addressed / Iceberg
                // target would capture a snapshot pointer here (see
                // `revert_pointer_for`); that path is object-store-only and not
                // reachable on this drive.
                revert_pointer: revert_pointer_for(),
            }),
        };
        let write_result = guard.record_policy_decision(&record);
        drop(guard);

        if let Err(e) = &write_result {
            warn!(
                target: "rocky::policy",
                error = %e,
                model,
                "failed to record auto-apply custody entry"
            );
        }

        govern_outcome(applied, write_result.is_ok(), model, reason)
    }
}

/// Decide the govern outcome from the applied verdict and whether the custody
/// row persisted.
///
/// Fail-closed on a lost custody row (D7): the plain decision row carries the
/// authorization + budget-attribution state, so an *applied* mutation whose row
/// failed to persist is REFUSED — a later verification failure could never pair
/// with a missing row to burn the granting rule's budget, and no mutation may
/// stand without a durable custody record. A refused drift mutated nothing, so
/// a lost refusal row is best-effort and keeps the original refusal reason.
fn govern_outcome(
    applied: bool,
    row_persisted: bool,
    model: &str,
    refuse_reason: String,
) -> GovernDecision {
    if applied && !row_persisted {
        return GovernDecision::Refuse(format!(
            "auto-apply refused for '{model}': the mutation was authorized but its \
             policy-decision row failed to persist. Refusing rather than mutating without a \
             durable custody record that a later verification failure can account against the \
             granting rule's autonomy budget."
        ));
    }
    if applied {
        GovernDecision::Apply
    } else {
        GovernDecision::Refuse(refuse_reason)
    }
}

/// Tighten `effect` to at least `require_review` — the fail-closed floor for
/// an unreadable ledger. Never softens a `deny`.
fn tighten_to_require_review(effect: PolicyEffect) -> PolicyEffect {
    match effect {
        PolicyEffect::Allow => PolicyEffect::RequireReview,
        PolicyEffect::RequireReview | PolicyEffect::Deny => effect,
    }
}

/// The revert pointer captured before an auto-applied migration, when a
/// rollback substrate exists.
///
/// A content-addressed / Iceberg target would return the current snapshot
/// pointer so a failed `verify_after` could restore it with a metadata-only
/// pointer swap. The replication drift path mutates a plain warehouse table
/// (`ALTER TABLE ADD COLUMN`) with no such substrate, so this is always
/// `None` here and a failed verification is halt-only. Kept as a seam so the
/// substrate-present path can fill it in without reshaping the custody record.
fn revert_pointer_for() -> Option<String> {
    None
}

/// The `plan_id` under which this run's auto-apply rows are filed — both the
/// plain decision rows `govern` writes and the post-run verification rows
/// [`finalize_drift_verify_after`] writes. Sharing one plan id is what lets
/// [`policy::budget_failures_in_window`] pair a verification failure (a
/// `deny` row with a non-empty `verify_after`) with the plain decision row
/// carrying the granting `rule_id`, so repeated auto-heal verification
/// failures burn that rule's autonomy budget. The two row shapes stay
/// distinguishable by `verify_after` (empty = decision, non-empty =
/// verification outcome).
fn decision_plan_id(run_id: &str) -> String {
    format!("autoapply:{run_id}")
}

/// Run the post-apply `verify_after` gate over every table this run
/// auto-migrated.
///
/// Reads the run's recorded [`check_outcomes`](rocky_core::state::RunRecord)
/// — the same pass/fail the two-step `rocky apply` gate reads — and, for each
/// applied auto-heal whose granting rule requires named checks (`policy`
/// resolves `rule_id` → the rule's `verify_after` list), confirms every
/// required check ran and passed. Duplicate check names (a per-table check
/// emitted once per table) are AND-aggregated, so a required check passes only
/// if every occurrence passed. Records an `allow`/`deny` verification custody
/// row per table — under the **same** `autoapply:<run_id>` plan id as the
/// decision rows, so a failure burns the granting rule's autonomy budget (see
/// [`decision_plan_id`]) — and returns `Err` (halting the run) when any
/// required check failed or was absent. The migration already landed and,
/// with no rollback substrate, stays in place until a human reverts it.
///
/// A no-op when the store is unavailable, nothing was auto-applied, or no
/// granting rule demanded checks.
pub(crate) fn finalize_drift_verify_after(
    store: Option<&StateStore>,
    run_id: &str,
    policy: Option<&PolicyConfig>,
) -> Result<()> {
    let Some(store) = store else {
        return Ok(());
    };
    let Ok(decisions) = store.list_policy_decisions() else {
        return Ok(());
    };
    let decision_plan = decision_plan_id(run_id);
    // Applied decision rows are PLAIN (empty verify_after); the non-empty
    // filter excludes verification-outcome rows a partial earlier finalize
    // may have written under the same plan id.
    let healed: Vec<&PolicyDecisionRecord> = decisions
        .iter()
        .filter(|d| {
            d.plan_id == decision_plan
                && d.auto_apply.as_ref().is_some_and(|a| a.applied)
                && d.verify_after.is_empty()
        })
        .collect();
    if healed.is_empty() {
        return Ok(());
    }

    // AND-aggregate this run's recorded check outcomes by name. A run recorded
    // under this id must exist (the finalizer runs after `record_run`); an
    // absent run leaves the map empty ⇒ every required check reads as "absent"
    // ⇒ fail closed.
    let outcomes = match store.get_run(run_id) {
        Ok(Some(record)) => {
            let mut map: std::collections::BTreeMap<&str, bool> = std::collections::BTreeMap::new();
            for c in &record.check_outcomes {
                map.entry(c.name.as_str())
                    .and_modify(|p| *p &= c.passed)
                    .or_insert(c.passed);
            }
            map.into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect::<std::collections::BTreeMap<String, bool>>()
        }
        _ => std::collections::BTreeMap::new(),
    };

    let mut halted: Vec<String> = Vec::new();
    for d in healed {
        // The checks the granting rule requires, resolved from the plain
        // decision row's `rule_id`. No rule / no policy / no checks ⇒ this
        // heal carries no post-run gate.
        let required: Vec<String> = match (d.rule_id, policy) {
            (Some(idx), Some(policy)) => policy
                .rules
                .get(idx)
                .map(|r| r.verify_after.clone())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        if required.is_empty() {
            continue;
        }
        let mut failures: Vec<String> = Vec::new();
        for name in &required {
            match outcomes.get(name) {
                Some(true) => {}
                Some(false) => failures.push(format!("{name} (failed)")),
                None => failures.push(format!("{name} (absent — did not run)")),
            }
        }
        let passed = failures.is_empty();
        let reason = if passed {
            format!(
                "verify_after passed for auto-applied drift on '{}': [{}]",
                d.model,
                required.join(", ")
            )
        } else {
            format!(
                "verify_after FAILED for auto-applied drift on '{}': {}. No rollback substrate — \
                 the migration stands; halt-only.",
                d.model,
                failures.join("; ")
            )
        };
        // Filed under the SAME plan id as the decision row (see
        // `decision_plan_id`): a `deny` outcome here + the plain decision row
        // carrying `rule_id` is exactly the pair
        // `budget_failures_in_window` counts against the granting rule.
        let record = PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: decision_plan.clone(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: d.model.clone(),
            effect: if passed {
                PolicyEffect::Allow
            } else {
                PolicyEffect::Deny
            },
            // Keep the granting rule on the outcome row for the audit trail;
            // budget attribution reads the plain decision row, never this one.
            rule_id: d.rule_id,
            reason: reason.clone(),
            verify_after: required.clone(),
            auto_apply: d.auto_apply.clone(),
        };
        if let Err(e) = store.record_policy_decision(&record) {
            warn!(
                target: "rocky::policy",
                error = %e,
                "failed to record verify_after custody entry (continuing)"
            );
        }
        if !passed {
            warn!(
                target: "rocky::policy",
                model = %d.model,
                failures = %failures.join("; "),
                "verify_after post-apply gate FAILED for auto-applied drift"
            );
            halted.push(reason);
        }
    }

    if !halted.is_empty() {
        bail!(
            "verify_after gate FAILED after auto-applying additive drift: {}. The migration(s) \
             already landed and remain in place (no rollback substrate on this backend); revert \
             manually. The failure is recorded in the policy-decision ledger.",
            halted.join("; ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::config::ResilienceConfig;
    use rocky_core::state::CheckOutcome;

    fn temp_store() -> (StateStore, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        (store, dir)
    }

    /// A config with the auto-apply opt-in ON and no `[policy]` block — the
    /// exact shape the fail-open regression guards.
    fn cfg_opt_in_no_policy() -> RockyConfig {
        RockyConfig {
            state: Default::default(),
            adapters: Default::default(),
            pipelines: Default::default(),
            hooks: Default::default(),
            cost: Default::default(),
            budget: Default::default(),
            schema_evolution: Default::default(),
            retry: None,
            portability: Default::default(),
            cache: Default::default(),
            mask: Default::default(),
            classifications: Default::default(),
            roles: Default::default(),
            ai: Default::default(),
            branch: Default::default(),
            freshness: Default::default(),
            imports: Default::default(),
            run: Default::default(),
            reuse: Default::default(),
            policy: None,
            resilience: ResilienceConfig {
                auto_apply_additive_drift: true,
                ..Default::default()
            },
        }
    }

    #[test]
    fn opt_in_without_policy_governs_fail_closed_not_open() {
        // Codex red-team regression (fail-OPEN → fail-CLOSED): with the opt-in
        // ON but NO `[policy]` block, `build` must still return a governor with
        // a no-grant, require-review posture — never `None`. Returning `None`
        // skipped governance entirely and let the pre-existing drift path mutate
        // a breaking change ungoverned. Pre-fix, `build` returned `None` and the
        // `.expect` below panics; post-fix it governs and refuses.
        let cfg = cfg_opt_in_no_policy();
        let gov = DriftGovernor::build(&cfg, "run-x", "wh.raw.orders", true)
            .expect("opt-in on ⇒ a governor must exist even without a [policy] block");
        assert_eq!(
            gov.effect,
            PolicyEffect::RequireReview,
            "no [policy] grant ⇒ every drift is refused to require-review"
        );
    }

    fn tref() -> rocky_ir::TableRef {
        rocky_ir::TableRef {
            catalog: "wh".to_string(),
            schema: "raw".to_string(),
            table: "orders".to_string(),
        }
    }

    fn additive_add_drift() -> DriftResult {
        DriftResult {
            table: tref(),
            drifted_columns: Vec::new(),
            action: rocky_ir::DriftAction::Ignore,
            added_columns: vec![rocky_ir::ColumnInfo {
                name: "email".to_string(),
                data_type: "VARCHAR".to_string(),
                nullable: true,
            }],
            grace_period_columns: Vec::new(),
            columns_to_drop: Vec::new(),
        }
    }

    fn breaking_retype_drift() -> DriftResult {
        DriftResult {
            table: tref(),
            drifted_columns: vec![rocky_ir::DriftedColumn {
                name: "amount".to_string(),
                source_type: "VARCHAR".to_string(),
                target_type: "INT".to_string(),
            }],
            action: rocky_ir::DriftAction::DropAndRecreate,
            added_columns: Vec::new(),
            grace_period_columns: Vec::new(),
            columns_to_drop: Vec::new(),
        }
    }

    #[tokio::test]
    async fn no_policy_governor_refuses_every_drift_at_the_run_loop() {
        // Re-red-team Q5: prove the full fail-closed property end-to-end, not
        // just that `build` returns a governor. With the opt-in on and no
        // [policy] block, `govern` must REFUSE every drift the run loop would
        // otherwise apply — a breaking retype AND a plain additive add (no
        // grant ⇒ nothing auto-applies) — recording a require-review custody
        // row and never authorising a mutation.
        let cfg = cfg_opt_in_no_policy();
        let gov =
            DriftGovernor::build(&cfg, "run-x", "wh.raw.orders", true).expect("governor present");
        let (store, _d) = temp_store();
        let state = Mutex::new(store);

        // A breaking retype is refused (fails the additive proof first).
        let d = gov
            .govern(&breaking_retype_drift(), "wh.raw.orders", &state)
            .await;
        assert!(
            matches!(d, GovernDecision::Refuse(_)),
            "a breaking retype must be refused"
        );

        // A plain additive add is ALSO refused: additive-ness passes, but there
        // is no policy grant, so `effect != Allow` refuses it too.
        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        assert!(
            matches!(d, GovernDecision::Refuse(_)),
            "an additive add must be refused when no [policy] rule grants it"
        );

        // Both refusals recorded a require-review custody row; nothing applied.
        let decisions = state.lock().await.list_policy_decisions().unwrap();
        assert_eq!(decisions.len(), 2, "one custody row per governed drift");
        assert!(
            decisions
                .iter()
                .all(|r| r.effect == PolicyEffect::RequireReview),
            "no-grant refusals record require-review, not allow/deny"
        );
        assert!(
            decisions
                .iter()
                .all(|r| r.auto_apply.as_ref().is_some_and(|a| !a.applied)),
            "nothing was auto-applied"
        );
    }

    /// A `[policy]` block whose rule 0 grants `schema_change.additive` on any
    /// scope with the given `verify_after` checks and optional budget — the
    /// shape the finalize gate resolves required checks from.
    fn granting_policy(
        checks: &[&str],
        budget: Option<rocky_core::config::AutonomyBudget>,
    ) -> PolicyConfig {
        PolicyConfig {
            version: 1,
            default_agent_effect: PolicyEffect::RequireReview,
            rules: vec![rocky_core::config::PolicyRule {
                principal: PolicyPrincipal::Agent,
                capability: PolicyCapability::SchemaChangeAdditive,
                scope: rocky_core::config::PolicyScope {
                    any: true,
                    ..Default::default()
                },
                effect: PolicyEffect::Allow,
                verify_after: checks.iter().map(ToString::to_string).collect(),
                conditions: None,
                autonomy_budget: budget,
            }],
            tests: Vec::new(),
        }
    }

    /// A config with the auto-apply opt-in ON and the given `[policy]` block.
    fn cfg_opt_in_with_policy(policy: PolicyConfig) -> RockyConfig {
        let mut cfg = cfg_opt_in_no_policy();
        cfg.policy = Some(policy);
        cfg
    }

    /// An applied auto-heal *decision* row in the post-fix shape: PLAIN
    /// (empty `verify_after`), `rule_id` = the granting rule, custody payload
    /// attached. Written by `govern` under `autoapply:<run_id>`.
    fn applied_decision(run_id: &str, model: &str) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: decision_plan_id(run_id),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: model.to_string(),
            effect: PolicyEffect::Allow,
            rule_id: Some(0),
            reason: "auto-applied".to_string(),
            verify_after: Vec::new(),
            auto_apply: Some(AutoApplyCustody {
                drift_summary: "added nullable column 'email' (VARCHAR)".to_string(),
                classification: "schema_change.additive".to_string(),
                applied: true,
                revert_pointer: None,
            }),
        }
    }

    /// Find this run's verification-outcome rows: filed under the SAME plan id
    /// as the decision rows, distinguished by a non-empty `verify_after`.
    fn verify_rows(store: &StateStore, run_id: &str) -> Vec<PolicyDecisionRecord> {
        store
            .list_policy_decisions()
            .unwrap()
            .into_iter()
            .filter(|d| d.plan_id == decision_plan_id(run_id) && !d.verify_after.is_empty())
            .collect()
    }

    fn run_with_checks(run_id: &str, checks: &[(&str, bool)]) -> rocky_core::state::RunRecord {
        rocky_core::state::RunRecord {
            run_id: run_id.to_string(),
            started_at: chrono::Utc::now(),
            finished_at: chrono::Utc::now(),
            status: rocky_core::state::RunStatus::Success,
            models_executed: Vec::new(),
            trigger: rocky_core::state::RunTrigger::Manual,
            config_hash: String::new(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "test".to_string(),
            rocky_version: "test".to_string(),
            check_outcomes: checks
                .iter()
                .map(|(n, p)| CheckOutcome {
                    name: n.to_string(),
                    passed: *p,
                })
                .collect(),
        }
    }

    #[test]
    fn no_auto_heals_is_ok() {
        let (store, _d) = temp_store();
        let policy = granting_policy(&["row_count"], None);
        assert!(finalize_drift_verify_after(Some(&store), "run-1", Some(&policy)).is_ok());
    }

    #[test]
    fn passing_verify_after_records_allow_and_succeeds() {
        let (store, _d) = temp_store();
        let policy = granting_policy(&["row_count"], None);
        store
            .record_policy_decision(&applied_decision("run-1", "wh.raw.orders"))
            .unwrap();
        store
            .record_run(&run_with_checks("run-1", &[("row_count", true)]))
            .unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-1", Some(&policy)).is_ok());
        // A verification custody row was written with effect=allow, under the
        // same plan id as the decision row.
        let rows = verify_rows(&store, "run-1");
        assert_eq!(rows.len(), 1, "one verification outcome row");
        assert_eq!(rows[0].effect, PolicyEffect::Allow);
        assert_eq!(rows[0].verify_after, vec!["row_count".to_string()]);
    }

    #[test]
    fn failing_verify_after_halts_and_records_deny() {
        let (store, _d) = temp_store();
        let policy = granting_policy(&["row_count"], None);
        store
            .record_policy_decision(&applied_decision("run-2", "wh.raw.orders"))
            .unwrap();
        store
            .record_run(&run_with_checks("run-2", &[("row_count", false)]))
            .unwrap();
        let err = finalize_drift_verify_after(Some(&store), "run-2", Some(&policy)).unwrap_err();
        assert!(
            err.to_string().contains("verify_after gate FAILED"),
            "{err}"
        );
        let rows = verify_rows(&store, "run-2");
        assert_eq!(rows.len(), 1, "one verification outcome row");
        assert_eq!(rows[0].effect, PolicyEffect::Deny);
        // Custody detail from the decision row is preserved on the outcome row.
        assert!(rows[0].auto_apply.as_ref().is_some_and(|a| a.applied));
    }

    #[test]
    fn absent_required_check_halts() {
        // The required check never ran ⇒ fail closed (halt).
        let (store, _d) = temp_store();
        let policy = granting_policy(&["row_count"], None);
        store
            .record_policy_decision(&applied_decision("run-3", "wh.raw.orders"))
            .unwrap();
        store.record_run(&run_with_checks("run-3", &[])).unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-3", Some(&policy)).is_err());
    }

    #[test]
    fn duplicate_check_names_and_aggregate() {
        // Same check emitted twice, one failing ⇒ the name fails (AND).
        let (store, _d) = temp_store();
        let policy = granting_policy(&["row_count"], None);
        store
            .record_policy_decision(&applied_decision("run-4", "wh.raw.orders"))
            .unwrap();
        store
            .record_run(&run_with_checks(
                "run-4",
                &[("row_count", true), ("row_count", false)],
            ))
            .unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-4", Some(&policy)).is_err());
    }

    /// A freeze-decision row exactly as `rocky policy freeze` records it.
    fn freeze_record(principal: PolicyPrincipal, scope: &str) -> PolicyDecisionRecord {
        let now = chrono::Utc::now();
        let label = match principal {
            PolicyPrincipal::Agent => "agent",
            PolicyPrincipal::Human => "human",
        };
        PolicyDecisionRecord {
            timestamp: now,
            plan_id: format!("{}{label}:{}", policy::FREEZE_PLAN_PREFIX, now.to_rfc3339()),
            principal,
            capability: PolicyCapability::Apply,
            model: scope.to_string(),
            effect: PolicyEffect::Deny,
            rule_id: None,
            reason: "policy freeze: agent actions frozen to deny".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        }
    }

    /// 🔴 Regression (freeze seam): an active `rocky policy freeze` matching
    /// the agent principal must make the auto-apply governor refuse a drift
    /// that a `[policy]` rule statically grants. Pre-fix, `govern` used only
    /// the build-time static effect — the freeze ledger was never consulted —
    /// so the additive add auto-applied straight through an active freeze.
    #[tokio::test]
    async fn active_freeze_blocks_auto_apply_even_with_a_granting_rule() {
        let cfg = cfg_opt_in_with_policy(granting_policy(&[], None));
        let gov =
            DriftGovernor::build(&cfg, "run-fz", "wh.raw.orders", true).expect("governor present");
        assert_eq!(
            gov.effect,
            PolicyEffect::Allow,
            "the static grant must be allow — otherwise this test is vacuous"
        );

        let (store, _d) = temp_store();
        store
            .record_policy_decision(&freeze_record(PolicyPrincipal::Agent, "any"))
            .unwrap();
        let state = Mutex::new(store);

        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        let GovernDecision::Refuse(reason) = d else {
            panic!("an active agent freeze must refuse the auto-apply");
        };
        assert!(
            reason.contains("freeze"),
            "refusal must cite the freeze: {reason}"
        );

        // The custody row records the freeze-forced deny.
        let decisions = state.lock().await.list_policy_decisions().unwrap();
        let row = decisions
            .iter()
            .find(|r| r.plan_id == decision_plan_id("run-fz"))
            .expect("custody row");
        assert_eq!(row.effect, PolicyEffect::Deny);
        assert!(row.auto_apply.as_ref().is_some_and(|a| !a.applied));
    }

    /// A human-scoped freeze does not gate the agent's auto-apply (principals
    /// are matched, not blanket): the granting rule still applies.
    #[tokio::test]
    async fn human_scoped_freeze_does_not_block_agent_auto_apply() {
        let cfg = cfg_opt_in_with_policy(granting_policy(&[], None));
        let gov = DriftGovernor::build(&cfg, "run-hfz", "wh.raw.orders", true).expect("governor");
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&freeze_record(PolicyPrincipal::Human, "any"))
            .unwrap();
        let state = Mutex::new(store);
        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        assert!(
            matches!(d, GovernDecision::Apply),
            "a human-principal freeze must not gate the agent grant"
        );
    }

    /// 🔴 Regression (budget seam): an exhausted autonomy budget on the
    /// granting rule must degrade the auto-apply grant to require-review.
    /// Pre-fix, `govern` never projected the budget, so the grant stood
    /// regardless of prior verify-after failures.
    #[tokio::test]
    async fn exhausted_autonomy_budget_degrades_auto_apply_to_refuse() {
        let budget = rocky_core::config::AutonomyBudget {
            failures: 1,
            window: "7d".to_string(),
        };
        let cfg = cfg_opt_in_with_policy(granting_policy(&["row_count"], Some(budget)));
        let gov = DriftGovernor::build(&cfg, "run-b2", "wh.raw.orders", true).expect("governor");
        assert_eq!(gov.effect, PolicyEffect::Allow);

        let (store, _d) = temp_store();
        // Seed a prior run's failed auto-heal in the post-fix row shape: a
        // plain decision row attributing rule 0 + a deny verification row with
        // a non-empty verify_after, both under the same plan id.
        store
            .record_policy_decision(&applied_decision("run-b1", "wh.raw.orders"))
            .unwrap();
        store
            .record_policy_decision(&PolicyDecisionRecord {
                timestamp: chrono::Utc::now(),
                plan_id: decision_plan_id("run-b1"),
                principal: PolicyPrincipal::Agent,
                capability: PolicyCapability::SchemaChangeAdditive,
                model: "wh.raw.orders".to_string(),
                effect: PolicyEffect::Deny,
                rule_id: Some(0),
                reason: "verify_after FAILED".to_string(),
                verify_after: vec!["row_count".to_string()],
                auto_apply: None,
            })
            .unwrap();
        let state = Mutex::new(store);

        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        let GovernDecision::Refuse(reason) = d else {
            panic!("an exhausted autonomy budget must refuse the auto-apply");
        };
        assert!(
            reason.contains("autonomy budget exhausted"),
            "refusal must cite the budget: {reason}"
        );
    }

    /// 🔴 THE row-shape regression: a failed auto-heal verification must burn
    /// the granting rule's autonomy budget. Pre-fix the two ledger rows could
    /// never pair — the decision row carried a non-empty `verify_after` (so it
    /// failed the attribution leg of `budget_failures_in_window`) and the
    /// verification row was filed under a *different* plan id
    /// (`autoapply-verify:<run_id>`), so the failure leg pointed at a plan
    /// with no attribution row. The count stayed 0 forever, and repeated
    /// auto-heal failures never degraded the rule.
    #[tokio::test]
    async fn auto_heal_verify_failure_burns_the_granting_rules_budget() {
        let policy = granting_policy(&["row_count"], None);
        let cfg = cfg_opt_in_with_policy(policy.clone());
        let gov = DriftGovernor::build(&cfg, "run-burn", "wh.raw.orders", true).expect("governor");

        let (store, _d) = temp_store();
        let state = Mutex::new(store);

        // The governed heal applies (grant is allow, drift provably additive).
        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        assert!(matches!(d, GovernDecision::Apply), "the heal must apply");

        let store = state.into_inner();

        // The decision row is PLAIN (empty verify_after) with the granting
        // rule attributed, and preserves the custody payload.
        let decision = store
            .list_policy_decisions()
            .unwrap()
            .into_iter()
            .find(|r| r.plan_id == decision_plan_id("run-burn"))
            .expect("decision row");
        assert!(
            decision.verify_after.is_empty(),
            "the decision row must be plain so budget attribution can match it"
        );
        assert_eq!(decision.rule_id, Some(0));
        let custody = decision.auto_apply.as_ref().expect("custody payload");
        assert!(custody.applied);
        assert_eq!(custody.classification, "schema_change.additive");
        assert!(custody.drift_summary.contains("email"));

        // The required check fails post-run; the finalize gate halts and files
        // the deny verification row under the SAME plan id.
        store
            .record_run(&run_with_checks("run-burn", &[("row_count", false)]))
            .unwrap();
        let err = finalize_drift_verify_after(Some(&store), "run-burn", Some(&policy)).unwrap_err();
        assert!(err.to_string().contains("verify_after gate FAILED"));

        // The pair now counts against rule 0's budget.
        let decisions = store.list_policy_decisions().unwrap();
        let failures = policy::budget_failures_in_window(
            &decisions,
            0,
            chrono::Duration::days(7),
            chrono::Utc::now(),
        );
        assert_eq!(
            failures, 1,
            "the failed auto-heal verification must burn the granting rule's budget"
        );
    }

    /// 🔴 D2 regression: a non-authoritative ledger (recreated for a
    /// forward-incompatible schema, or an authoritative remote download failed)
    /// must refuse every auto-apply — an active freeze / exhausted budget
    /// recorded elsewhere is invisible to a fresh-empty local store. Pre-fix
    /// the governor had no authority signal and would auto-apply against the
    /// empty ledger. Driven through the real `govern` producer.
    #[tokio::test]
    async fn non_authoritative_ledger_refuses_auto_apply() {
        // A granting policy that WOULD auto-apply an additive add on an
        // authoritative ledger — so a refusal here is due solely to authority.
        let cfg = cfg_opt_in_with_policy(granting_policy(&[], None));
        let gov = DriftGovernor::build(
            &cfg,
            "run-na",
            "wh.raw.orders",
            /* authoritative */ false,
        )
        .expect("governor");
        let (store, _d) = temp_store();
        let state = Mutex::new(store);

        let d = gov
            .govern(&additive_add_drift(), "wh.raw.orders", &state)
            .await;
        let GovernDecision::Refuse(reason) = d else {
            panic!("a non-authoritative ledger must refuse the auto-apply");
        };
        assert!(
            reason.contains("non-authoritative"),
            "refusal must cite the non-authoritative ledger: {reason}"
        );
        // Sanity: the SAME governor with an authoritative ledger applies — proves
        // the refusal is authority-driven, not policy-driven (non-vacuous).
        let gov_ok = DriftGovernor::build(&cfg, "run-ok", "wh.raw.orders", true).expect("governor");
        let (store2, _d2) = temp_store();
        let d2 = gov_ok
            .govern(&additive_add_drift(), "wh.raw.orders", &Mutex::new(store2))
            .await;
        assert!(
            matches!(d2, GovernDecision::Apply),
            "the same grant on an authoritative ledger must apply"
        );
    }

    /// 🔴 D6 regression: freeze composition records the tightened policy
    /// effect. With an Agent freeze active AND non-additive drift, eligibility
    /// returns `NotAdditive` *before* the policy check — so the naive refusal
    /// effect is `require_review`. But the tightened policy resolved to `deny`
    /// (the freeze), and the custody row MUST record `deny`. Pre-fix the row
    /// recorded `require_review`, understating the freeze. Driven through the
    /// real `govern` producer.
    #[tokio::test]
    async fn freeze_plus_non_additive_records_deny_not_require_review() {
        let cfg = cfg_opt_in_with_policy(granting_policy(&[], None));
        let gov = DriftGovernor::build(&cfg, "run-d6", "wh.raw.orders", true).expect("governor");
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&freeze_record(PolicyPrincipal::Agent, "any"))
            .unwrap();
        let state = Mutex::new(store);

        // A breaking retype: eligibility trips on NotAdditive before the policy
        // check, yet the active freeze tightened the policy effect to deny.
        let d = gov
            .govern(&breaking_retype_drift(), "wh.raw.orders", &state)
            .await;
        assert!(matches!(d, GovernDecision::Refuse(_)), "must refuse");

        let row = state
            .lock()
            .await
            .list_policy_decisions()
            .unwrap()
            .into_iter()
            .find(|r| r.plan_id == decision_plan_id("run-d6"))
            .expect("custody row");
        assert_eq!(
            row.effect,
            PolicyEffect::Deny,
            "a freeze that resolved policy to deny must record deny, even when a non-policy \
             eligibility gate (NotAdditive) tripped first"
        );
    }

    /// 🔴 D7 regression: a lost custody row must fail closed. `govern_outcome`
    /// is the production decision the write path calls: an *applied* mutation
    /// whose row failed to persist is REFUSED (never Apply), while a refused
    /// drift keeps its refusal reason regardless of the write result. Pre-fix
    /// govern returned Apply even when the plain-row append failed, so a later
    /// verification failure could never burn the budget.
    #[test]
    fn lost_custody_row_fails_closed_for_an_applied_mutation() {
        // Applied + row did NOT persist → refuse (fail-closed).
        let d = govern_outcome(true, false, "wh.raw.orders", "n/a".to_string());
        let GovernDecision::Refuse(reason) = d else {
            panic!("an applied mutation with a lost custody row must be refused");
        };
        assert!(reason.contains("failed to persist"), "{reason}");

        // Applied + row persisted → apply.
        assert!(matches!(
            govern_outcome(true, true, "wh.raw.orders", "n/a".to_string()),
            GovernDecision::Apply
        ));

        // Refused + row lost → still refuse, keeping the original reason (the
        // drift mutated nothing, so the lost row is best-effort).
        let d = govern_outcome(
            false,
            false,
            "wh.raw.orders",
            "additive proof failed".to_string(),
        );
        let GovernDecision::Refuse(reason) = d else {
            panic!("a refused drift stays refused");
        };
        assert_eq!(reason, "additive proof failed");
    }
}
