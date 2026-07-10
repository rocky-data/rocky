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
use rocky_core::config::{PolicyCapability, PolicyEffect, PolicyPrincipal, RockyConfig};
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
    effect: PolicyEffect,
    matched_rule: Option<usize>,
    policy_reason: String,
    verify_after: Vec<String>,
    contracted: bool,
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
    pub(crate) fn build(cfg: &RockyConfig, run_id: &str, model_name: &str) -> Option<Self> {
        if !cfg.resilience.auto_apply_additive_drift {
            return None;
        }
        // The opt-in is on. A missing `[policy]` block grants nothing — but it
        // must NOT disable governance, or the pre-existing drift path would
        // mutate UNGOVERNED (fail-open: a breaking retype would silently apply).
        // Govern with a no-grant, fail-closed posture instead: the effect is
        // `require_review`, so `is_auto_apply_eligible` refuses every drift until
        // a `[policy]` rule explicitly grants `schema_change.additive`.
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
            });
        };
        let attrs = ModelAttributes {
            name: model_name.to_string(),
            ..Default::default()
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
        })
    }

    /// Govern one detected drift: classify, decide, and record custody.
    ///
    /// `model` is the fully-qualified target name the custody row is attributed
    /// to. Writes exactly one decision row (applied or refused) via the shared
    /// state store, then returns the decision. Fail-closed: anything not
    /// provably additive+allowed is a [`GovernDecision::Refuse`].
    pub(crate) async fn govern(
        &self,
        drift: &DriftResult,
        model: &str,
        state: &Mutex<StateStore>,
    ) -> GovernDecision {
        let findings = auto_apply::drift_findings(drift, model);
        let refs: Vec<&BreakingFinding> = findings.iter().collect();
        let classification = policy::classify_model_findings(&refs);
        let verdict = auto_apply::is_auto_apply_eligible(
            &findings,
            classification,
            self.effect,
            self.contracted,
        );
        let applied = verdict.is_apply();
        let summary = auto_apply::drift_summary(drift);

        let (effect, reason) = if applied {
            (
                PolicyEffect::Allow,
                format!(
                    "auto-applied additive drift on '{model}' ({summary}); {}",
                    self.policy_reason
                ),
            )
        } else {
            let detail = verdict
                .refuse_reason()
                .map(RefuseReason::to_string)
                .unwrap_or_default();
            // A hard `deny` is preserved; every other refusal is a
            // require-review fallback (fail-safe, never a silent apply).
            let effect = match verdict.refuse_reason() {
                Some(RefuseReason::PolicyNotAllow(PolicyEffect::Deny)) => PolicyEffect::Deny,
                _ => PolicyEffect::RequireReview,
            };
            (
                effect,
                format!("auto-apply of drift on '{model}' refused: {detail} ({summary})"),
            )
        };

        let record = PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: decision_plan_id(&self.run_id),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: model.to_string(),
            effect,
            rule_id: self.matched_rule,
            reason: reason.clone(),
            // The applied row carries the checks the post-run gate must
            // confirm; a refused row applied nothing, so it carries none.
            verify_after: if applied {
                self.verify_after.clone()
            } else {
                Vec::new()
            },
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
        {
            let guard = state.lock().await;
            if let Err(e) = guard.record_policy_decision(&record) {
                warn!(
                    target: "rocky::policy",
                    error = %e,
                    model,
                    "failed to record auto-apply custody entry (continuing)"
                );
            }
        }

        if applied {
            GovernDecision::Apply
        } else {
            GovernDecision::Refuse(reason)
        }
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

/// The `plan_id` under which this run's auto-apply *decision* rows are filed.
fn decision_plan_id(run_id: &str) -> String {
    format!("autoapply:{run_id}")
}

/// The `plan_id` under which this run's post-apply *verification* rows are
/// filed (kept distinct from the decision rows so a re-scan never confuses
/// the two).
fn verify_plan_id(run_id: &str) -> String {
    format!("autoapply-verify:{run_id}")
}

/// Run the post-apply `verify_after` gate over every table this run
/// auto-migrated.
///
/// Reads the run's recorded [`check_outcomes`](rocky_core::state::RunRecord)
/// — the same pass/fail the two-step `rocky apply` gate reads — and, for each
/// applied auto-heal that required named checks, confirms every required check
/// ran and passed. Duplicate check names (a per-table check emitted once per
/// table) are AND-aggregated, so a required check passes only if every
/// occurrence passed. Records an `allow`/`deny` verification custody row per
/// table and returns `Err` (halting the run) when any required check failed or
/// was absent — the migration already landed and, with no rollback substrate,
/// stays in place until a human reverts it.
///
/// A no-op when the store is unavailable or nothing was auto-applied.
pub(crate) fn finalize_drift_verify_after(store: Option<&StateStore>, run_id: &str) -> Result<()> {
    let Some(store) = store else {
        return Ok(());
    };
    let Ok(decisions) = store.list_policy_decisions() else {
        return Ok(());
    };
    let decision_plan = decision_plan_id(run_id);
    let healed: Vec<&PolicyDecisionRecord> = decisions
        .iter()
        .filter(|d| {
            d.plan_id == decision_plan
                && d.auto_apply.as_ref().is_some_and(|a| a.applied)
                && !d.verify_after.is_empty()
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

    let verify_plan = verify_plan_id(run_id);
    let mut halted: Vec<String> = Vec::new();
    for d in healed {
        let mut failures: Vec<String> = Vec::new();
        for name in &d.verify_after {
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
                d.verify_after.join(", ")
            )
        } else {
            format!(
                "verify_after FAILED for auto-applied drift on '{}': {}. No rollback substrate — \
                 the migration stands; halt-only.",
                d.model,
                failures.join("; ")
            )
        };
        let record = PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: verify_plan.clone(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: d.model.clone(),
            effect: if passed {
                PolicyEffect::Allow
            } else {
                PolicyEffect::Deny
            },
            rule_id: None,
            reason: reason.clone(),
            verify_after: d.verify_after.clone(),
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
            gc: Default::default(),
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
        let gov = DriftGovernor::build(&cfg, "run-x", "wh.raw.orders")
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
        let gov = DriftGovernor::build(&cfg, "run-x", "wh.raw.orders").expect("governor present");
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

    fn applied_decision(run_id: &str, model: &str, checks: &[&str]) -> PolicyDecisionRecord {
        PolicyDecisionRecord {
            timestamp: chrono::Utc::now(),
            plan_id: decision_plan_id(run_id),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: model.to_string(),
            effect: PolicyEffect::Allow,
            rule_id: Some(0),
            reason: "auto-applied".to_string(),
            verify_after: checks.iter().map(ToString::to_string).collect(),
            auto_apply: Some(AutoApplyCustody {
                drift_summary: "added nullable column 'email' (VARCHAR)".to_string(),
                classification: "schema_change.additive".to_string(),
                applied: true,
                revert_pointer: None,
            }),
        }
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
        assert!(finalize_drift_verify_after(Some(&store), "run-1").is_ok());
    }

    #[test]
    fn passing_verify_after_records_allow_and_succeeds() {
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&applied_decision("run-1", "wh.raw.orders", &["row_count"]))
            .unwrap();
        store
            .record_run(&run_with_checks("run-1", &[("row_count", true)]))
            .unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-1").is_ok());
        // A verification custody row was written with effect=allow.
        let all = store.list_policy_decisions().unwrap();
        let verify = verify_plan_id("run-1");
        let v = all
            .iter()
            .find(|d| d.plan_id == verify)
            .expect("verify row");
        assert_eq!(v.effect, PolicyEffect::Allow);
    }

    #[test]
    fn failing_verify_after_halts_and_records_deny() {
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&applied_decision("run-2", "wh.raw.orders", &["row_count"]))
            .unwrap();
        store
            .record_run(&run_with_checks("run-2", &[("row_count", false)]))
            .unwrap();
        let err = finalize_drift_verify_after(Some(&store), "run-2").unwrap_err();
        assert!(
            err.to_string().contains("verify_after gate FAILED"),
            "{err}"
        );
        let all = store.list_policy_decisions().unwrap();
        let verify = verify_plan_id("run-2");
        let v = all
            .iter()
            .find(|d| d.plan_id == verify)
            .expect("verify row");
        assert_eq!(v.effect, PolicyEffect::Deny);
    }

    #[test]
    fn absent_required_check_halts() {
        // The required check never ran ⇒ fail closed (halt).
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&applied_decision("run-3", "wh.raw.orders", &["row_count"]))
            .unwrap();
        store.record_run(&run_with_checks("run-3", &[])).unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-3").is_err());
    }

    #[test]
    fn duplicate_check_names_and_aggregate() {
        // Same check emitted twice, one failing ⇒ the name fails (AND).
        let (store, _d) = temp_store();
        store
            .record_policy_decision(&applied_decision("run-4", "wh.raw.orders", &["row_count"]))
            .unwrap();
        store
            .record_run(&run_with_checks(
                "run-4",
                &[("row_count", true), ("row_count", false)],
            ))
            .unwrap();
        assert!(finalize_drift_verify_after(Some(&store), "run-4").is_err());
    }
}
