//! Mid-run freeze fence — fresh durable-tier freeze-marker LISTs at the
//! documented execution boundaries.
//!
//! The gate stack consults the marker set once, at command entry. A freeze
//! landing *after* that LIST must still fence the run, so the run loop
//! re-checks at three boundaries: the exec-fingerprint choke-point (governed
//! paths), each execution-layer boundary, and before the post-run governance
//! reconcile; the replication fan-out (no layer boundaries) gets one check
//! bounding the gate-to-copy window. On a fence hit the *remaining* work is
//! withheld fail-closed while already-committed work's state still flows to
//! the normal terminal path (persist + finalize upload) — partial failure,
//! never state loss.

// Fence granularity and the withhold-vs-abandon semantics are specified in
// docs/adr/ADR-CONCURRENCY.md, section "D3 — Freeze = rollout-independent
// ADD-WINS MARKER" (gate-to-mutation race).

use anyhow::Result;

use rocky_core::config::{PolicyPrincipal, StateConfig};
use rocky_core::freeze_marker::{self, ActiveMarkerFreeze};
use rocky_core::object_store::ObjectStoreProvider;
use rocky_core::policy::ModelAttributes;

/// Mid-run freeze fence — a COARSE, name-only, principal-aware breaker over
/// the models that are actually executing. Holds the durable-tier provider
/// (`None` ⇒ inert — Local / Valkey-only backends have no marker tier, and an
/// ungoverned run with no `[policy]` plane) plus the executing run's principal.
/// Every [`Self::check`] performs a FRESH LIST — never a cached set.
///
/// The precise principal+scope decision is the ENTRY GATE's job
/// (`policy::autonomy_degradation`); this fence only catches a freeze that
/// LANDS after that gate's LIST. It matches name-only: bare
/// [`ModelAttributes`] resolve `any` and `model=<glob>` selectors, but never
/// `layer=`/`classification=`/`tag=` — those scoped freezes are deferred to
/// the entry gate and the next run (see docs/adr/ADR-CONCURRENCY.md D3, "the
/// next boundary or the next run").
pub(crate) struct FreezeFence {
    /// The durable-tier marker provider — `Some` ONLY on a governed run (a
    /// `[policy]` plane configured); `None` on any ungoverned run and on
    /// Local / Valkey-only backends. So a present provider always implies
    /// governed, and every listing failure is therefore fail-closed.
    provider: Option<ObjectStoreProvider>,
    /// The executing run's principal, tested against each active marker via
    /// the SAME predicate the entry gate uses
    /// ([`rocky_core::policy::marker_freeze_binds`]), so for an AGENT-governed
    /// run the fence withholds exactly what the gate would deny — an agent-only
    /// freeze binds the agent, not a different principal.
    ///
    /// `None` means the acting principal is unknown, and the fence then binds
    /// ANY marker principal (the coarse, monotone-safe direction). Note this
    /// covers TWO cases in v0: a bare `rocky run`, AND a human apply — the
    /// governed context only exists for agent applies (`governed_run_context`
    /// returns `None` for non-agents, since "humans are ungated in v0"), so a
    /// human apply arrives here as `None`. Consequence: while any freeze is
    /// active, the fence halts a non-agent-governed run too. That is
    /// over-restriction (monotone-safe per docs/adr/ADR-CONCURRENCY.md D3 — the
    /// kill switch never fails to engage), but it is STRICTER than the entry
    /// gate, which leaves humans ungated in v0. Whether the freeze kill switch
    /// should gate humans at all — and per `--principal` — is a v0 product
    /// decision left open here; the safe default halts them.
    run_principal: Option<PolicyPrincipal>,
}

/// One active marker freeze whose scope matched a fenced name.
pub(crate) struct FenceHit {
    /// The matching marker's `freeze_id`.
    pub freeze_id: String,
    /// The marker's scope selector (`"any"` for an unreadable body).
    pub scope: String,
    /// The marker's human-readable reason.
    pub reason: String,
}

impl FreezeFence {
    /// Build the fence from the run's owned `[state]` snapshot and the
    /// executing run's principal.
    ///
    /// The fence keys on `[policy]` presence, never on the governed-apply
    /// context: a bare `rocky run` under a `[policy]` plane is fenced too, and
    /// a run with NO `[policy]` plane is inert — the freeze kill switch
    /// enforces nothing without a policy plane (the ledger freeze, the apply
    /// gate, and the drift governor all sit at their no-policy floors), so the
    /// mid-run fence must not be the lone seam that enforces it. This mirrors
    /// `rocky policy freeze`'s "recorded but NOT enforced until a `[policy]`
    /// block exists" contract; no durable-tier resolution is attempted without
    /// one.
    ///
    /// `run_principal` is the acting principal the fence matches against
    /// (`None` ⇒ unknown, a bare run, binds any marker principal).
    ///
    /// Fail-closed (governed only): an unresolvable durable tier (e.g. a
    /// missing bucket) is an error — an active freeze marker could not be seen.
    pub(crate) fn build(
        state_cfg: &StateConfig,
        policy_configured: bool,
        run_principal: Option<PolicyPrincipal>,
    ) -> Result<Self> {
        if !policy_configured {
            return Ok(Self {
                provider: None,
                run_principal,
            });
        }
        match rocky_core::state_sync::durable_tier_provider(state_cfg) {
            Ok(provider) => Ok(Self {
                provider,
                run_principal,
            }),
            Err(e) => Err(anyhow::Error::new(e).context(
                "refusing a governed run: the durable `[state]` tier for freeze markers could \
                 not be resolved, so an active freeze marker cannot be seen (fail-closed)",
            )),
        }
    }

    /// Fresh LIST + projection of the full active marker set — the run-entry
    /// snapshot threaded into the seams that consume a projection (the in-run
    /// replication gate, the drift auto-apply governor).
    ///
    /// A provider is present only on a governed run (see [`Self::build`]), so a
    /// listing failure is always **fail-closed** — an error is never an empty
    /// active set. No durable tier (ungoverned run, Local / Valkey-only) ⇒
    /// empty set.
    pub(crate) async fn active_snapshot(&self) -> Result<Vec<ActiveMarkerFreeze>> {
        let Some(provider) = &self.provider else {
            return Ok(Vec::new());
        };
        freeze_marker::load_active_marker_freezes(provider)
            .await
            .map_err(|e| {
                anyhow::Error::new(e).context(
                    "refusing a governed run: durable freeze-marker listing failed, so an active \
                     freeze marker cannot be seen (fail-closed). Retry once the `[state]` backend \
                     is reachable.",
                )
            })
    }

    /// Fresh LIST, then match the active set against `names`.
    ///
    /// `None` = clear. `Some(hit)` = an active marker freeze that BINDS any of
    /// `names` under the shared [`rocky_core::policy::marker_freeze_binds`]
    /// predicate — name-only and principal-aware. Each name is tested with bare
    /// [`ModelAttributes`], which resolve `any` and `model=<glob>` selectors;
    /// `layer=`/`classification=`/`tag=` scoped freezes never bind at the fence
    /// (they carry no mid-run attribute map — that precision is the entry
    /// gate's job, deferred to the next run per ADR-CONCURRENCY.md D3). The run
    /// principal is honoured via the shared predicate: a `Some(p)` run is
    /// bound only by markers for `p` (or both principals), so an agent-only
    /// freeze does not withhold a `Some(Human)` run; a `None` principal binds
    /// any marker principal (see the `run_principal` field for which runs
    /// arrive as `None` in v0). An empty `names` fences nothing (no LIST).
    /// Transport failure on a governed run ⇒ `Err` (fail-closed).
    pub(crate) async fn check<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
    ) -> Result<Option<FenceHit>> {
        if self.provider.is_none() {
            return Ok(None);
        }
        let names: Vec<&str> = names.collect();
        if names.is_empty() {
            return Ok(None);
        }
        let active = self.active_snapshot().await?;
        for marker in active {
            let matched = names.iter().any(|name| {
                let attrs = ModelAttributes {
                    name: (*name).to_string(),
                    ..Default::default()
                };
                rocky_core::policy::marker_freeze_binds(&marker, self.run_principal, &attrs)
            });
            if matched {
                return Ok(Some(FenceHit {
                    freeze_id: marker.freeze_id,
                    scope: marker.scope,
                    reason: marker.reason,
                }));
            }
        }
        Ok(None)
    }

    /// [`Self::check`], folding both a hit and a fail-closed transport error
    /// into the withhold message the run loop RECORDS (`Some` = withhold,
    /// `None` = clear) — never an `Err`.
    ///
    /// The recorded-failure shape is load-bearing at the mid-run boundaries:
    /// an `Err` escaping the run body routes to `session.abandon`, stranding
    /// already-committed layers' local state un-uploaded. Recording instead
    /// flows to the normal terminal path (persist run record → finalize
    /// upload), so a fence hit is a partial failure, never state loss.
    pub(crate) async fn check_withhold<'a>(
        &self,
        names: impl Iterator<Item = &'a str>,
    ) -> Option<String> {
        match self.check(names).await {
            Ok(None) => None,
            Ok(Some(hit)) => Some(format!(
                "freeze fence: active freeze marker {} (scope '{}', reason: {}) — remaining \
                 work withheld fail-closed",
                hit.freeze_id, hit.scope, hit.reason
            )),
            Err(e) => Some(format!(
                "freeze fence: durable freeze-marker listing failed ({e:#}) — an active freeze \
                 marker cannot be ruled out, remaining work withheld fail-closed"
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::config::{PolicyPrincipal, StateBackend, StateConfig};
    use rocky_core::fault_store::{FaultMode, FaultOp};
    use rocky_core::freeze_marker::FreezeMarker;
    use rocky_core::state_sync::remote_testing;
    use rocky_core::test_harness::CrossPodHarness;

    fn s3_cfg() -> StateConfig {
        StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("test".into()),
            ..StateConfig::default()
        }
    }

    async fn seed_marker(harness: &CrossPodHarness, id: &str, scope: &str) {
        rocky_core::freeze_marker::write_freeze_marker(
            &harness.provider,
            &FreezeMarker {
                freeze_id: id.to_string(),
                principal: PolicyPrincipal::Agent,
                scope: scope.to_string(),
                reason: format!("test freeze {id}"),
                created_at: chrono::Utc::now(),
            },
        )
        .await
        .expect("seed marker");
    }

    /// The gate-to-mutation recheck property: every `check` performs a FRESH
    /// LIST, so a freeze landing AFTER a clear check is caught by the very
    /// next one — a cached set would sail past it.
    #[tokio::test]
    async fn fence_recheck_catches_freeze_landing_after_gate_list() {
        let _serial = remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        let fence = FreezeFence::build(&s3_cfg(), true, None).expect("build fence");

        assert!(
            fence
                .check(["orders"].into_iter())
                .await
                .expect("clear check")
                .is_none(),
            "no marker yet — the fence must be clear"
        );

        // The freeze lands between the two checks — the gate-to-mutation race.
        seed_marker(&harness, "f-race", "any").await;

        let hit = fence
            .check(["orders"].into_iter())
            .await
            .expect("recheck")
            .expect("the fresh LIST must catch the freeze that landed after the first check");
        assert_eq!(hit.freeze_id, "f-race");
        assert_eq!(hit.scope, "any");
    }

    /// `check_withhold` folds a fail-closed transport failure into the
    /// RECORDED withhold message — never an `Err` — so the mid-run
    /// boundaries flow to the terminal persist + finalize path instead of
    /// `session.abandon`.
    #[tokio::test]
    async fn check_withhold_folds_transport_failure_into_recorded_message() {
        let _serial = remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        let fence = FreezeFence::build(&s3_cfg(), true, None).expect("build fence");

        harness.faults.arm(FaultOp::List, FaultMode::FailAll);
        let msg = fence
            .check_withhold(["orders"].into_iter())
            .await
            .expect("a fail-closed LIST failure must fold into a recorded withhold");
        harness.faults.clear();
        assert!(
            msg.contains("withheld fail-closed"),
            "the recorded message must state the fail-closed withhold; got: {msg}"
        );
    }

    /// With NO `[policy]` plane the fence is inert even on a durable-tier
    /// backend carrying an active marker: the kill switch enforces nothing
    /// without a policy plane (matching the ledger / apply-gate / drift-governor
    /// no-policy floors and `rocky policy freeze`'s "recorded but NOT enforced"
    /// contract). Inertness is proven by performing NO durable-tier LIST — the
    /// run path must never be the lone seam that enforces a freeze without a
    /// policy plane.
    #[tokio::test]
    async fn no_policy_fence_is_inert_and_does_not_list() {
        let _serial = remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        seed_marker(&harness, "f-nopolicy", "any").await;

        let fence = FreezeFence::build(&s3_cfg(), false, None).expect("build inert fence");
        harness.faults.arm(FaultOp::List, FaultMode::FailAll);
        assert!(
            fence
                .check(["orders"].into_iter())
                .await
                .expect("an inert fence never errors")
                .is_none(),
            "a no-[policy] fence must not enforce an active marker"
        );
        assert_eq!(
            harness.faults.count(FaultOp::List),
            0,
            "the inert fence must perform NO durable-tier LIST"
        );
        harness.faults.clear();
    }

    /// The fence honours the run principal exactly as the entry gate does
    /// (`marker_freeze_binds`): an agent-only freeze must NOT withhold a
    /// policy-allowed human run, but DOES withhold an agent run; and a bare run
    /// (unknown principal) binds any marker principal — the coarse fallback.
    #[tokio::test]
    async fn fence_is_principal_aware_like_the_gate() {
        let _serial = remote_testing::serial_guard();
        let harness = CrossPodHarness::new_s3_like();
        // `seed_marker` writes an AGENT-scoped `any` freeze.
        seed_marker(&harness, "f-agent", "any").await;

        // A human run is NOT fenced by an agent-only freeze (matches the gate).
        let human_fence = FreezeFence::build(&s3_cfg(), true, Some(PolicyPrincipal::Human))
            .expect("build human fence");
        assert!(
            human_fence
                .check(["orders"].into_iter())
                .await
                .expect("human check")
                .is_none(),
            "a human run must not be withheld by an agent-only freeze"
        );

        // An agent run IS fenced by the same freeze.
        let agent_fence = FreezeFence::build(&s3_cfg(), true, Some(PolicyPrincipal::Agent))
            .expect("build agent fence");
        let hit = agent_fence
            .check(["orders"].into_iter())
            .await
            .expect("agent check")
            .expect("an agent run must be withheld by the agent freeze");
        assert_eq!(hit.freeze_id, "f-agent");

        // A bare run (unknown principal) binds any marker principal.
        let bare_fence = FreezeFence::build(&s3_cfg(), true, None).expect("build bare fence");
        assert!(
            bare_fence
                .check(["orders"].into_iter())
                .await
                .expect("bare check")
                .is_some(),
            "an unknown principal (bare run) must bind any marker principal"
        );
    }
}
