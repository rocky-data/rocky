//! `rocky gc --derivable` — derivability inventory, plan, and eviction.
//!
//! Answers one question over the content-addressed artifact ledger: *which
//! stored bytes can Rocky prove it can rebuild, and are therefore a cache
//! entry rather than an asset?* The recipe is the truth; a rebuildable table
//! is derivable, and derivable bytes are reclaimable.
//!
//! Three surfaces, one eligibility path:
//!
//! - **`--dry-run`** ([`run_gc_derivable`]): the read-only inventory — an
//!   aggregate ("X bytes / Y% of managed storage is derivable") plus a
//!   per-candidate justification printing all five eligibility checks. No
//!   mutation.
//! - **plan** ([`run_gc_plan`], no `--dry-run`): writes a review-gated
//!   [`crate::plan_store::PlanKind::Gc`] plan listing only the derivable
//!   artifacts. Never deletes.
//! - **apply** ([`run_gc_apply_in`], via `rocky apply <plan-id>`): after an
//!   unconditional review gate, evicts each artifact that is *still* derivable
//!   — a durable tombstone + ledger-row retirement committed atomically, then a
//!   best-effort physical object-store delete.
//!
//! All three consume the single [`gather_eviction_candidates`] verdict, so the
//! eligibility logic is never forked or loosened between report, plan, and the
//! apply-time re-verification.
//!
//! # The join
//!
//! The candidate universe is [`rocky_core::state::StateStore::list_all_artifacts`],
//! grouped by content hash (each distinct hash is one physical artifact). For
//! each hash the inventory joins:
//!
//! - its [`rocky_core::state::ProvenanceRecord`] + input match-strength (via
//!   the shared [`crate::commands::replay::classify_model`] verdict) — checks
//!   1 (recipe recorded) and 2 (replayable);
//! - its ledger refcount via
//!   [`rocky_core::state::StateStore::refcount_for_hash`] — check 3
//!   (unreferenced);
//! - the recorded [`rocky_core::state::ModelExecution`] — the recipe-identity
//!   id and the rebuild-cost estimate.
//!
//! # Fail-closed — a false *derivable* is data loss
//!
//! Each check fails closed: any doubt keeps the artifact non-derivable. The
//! plan approves a *set*, but the apply re-runs the exact same verdict against
//! the live ledger and evicts only what is derivable *now* — a reference that
//! appears between plan and apply refuses the eviction. Every eviction writes
//! its tombstone (recipe triple + provenance pointer) atomically **before** the
//! ledger row is retired, and the physical byte-delete only ever happens after
//! that commit.
//!
//! **Restorability is narrower than eligibility.** `rocky restore` today
//! rebuilds only *single-input*, non-partitioned, content-addressed recipes: it
//! refuses any recipe with recorded upstreams, because re-deriving a multi-input
//! recipe from the recorded upstream bytes needs DAG re-derivation that is not
//! yet implemented. The eviction checks below admit multi-input recipes (a
//! `strong` closure over several content-hashed upstreams still passes
//! [`check_recipe_recorded`]), so gc can evict artifacts restore cannot yet
//! rebuild. The tombstone is still durable and still records the full recipe —
//! nothing is lost — but recovery of a multi-input artifact currently means
//! re-running its pipeline, not `rocky restore`.
//!
//! # Reachability
//!
//! Content-addressed writes are s3-only, so the creds-free playground holds no
//! CAS artifacts. The plan → review → apply → tombstone → ledger-retire flow is
//! driven creds-free over a ledger seeded through the production write APIs
//! (see the `seed_demo_ledger` harness in the tests). The physical
//! object-store delete ([`ObjectStoreEvictor`]) is s3-only and is
//! **code-reviewed here, driven on the sandbox** — creds-free it defers.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::warn;

use rocky_core::config::{PolicyCapability, PolicyPrincipal, StateBackend, load_rocky_config};
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::state::{
    ArtifactRecord, EvictOutcome, ModelExecution, ProvenanceRecord, RunRecord, StateStore,
    TombstoneRecord,
};

use crate::commands::apply::{PolicyGate, ai_plan_is_reviewed, evaluate_apply_policy_with_policy};
use crate::commands::replay::classify_model;
use crate::commands::review::record_plan_review_escalation;
use crate::output::{
    GcApplyOutput, GcCandidateOutput, GcCheckOutput, GcEvictedOutput, GcPlan, GcPlanEviction,
    GcPlanOutput, GcRebuildCostOutput, GcRefusedOutput, GcReportOutput, ReplayCheckModelOutput,
};
use crate::plan_store::{PlanKind, read_plan, write_plan_with_principal};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Resolved cost-model inputs: `(adapter_name, warehouse_type, dbu_per_hour,
/// cost_per_dbu)`. `None` when the config can't be loaded or the adapter is
/// not a billed warehouse.
type AdapterCost = (String, WarehouseType, f64, f64);

// ---------------------------------------------------------------------------
// The eligibility checks — each a pure function, each fails closed.
// ---------------------------------------------------------------------------

/// Check 1 — recipe recorded with a strong (non-weak) input closure.
///
/// A provenance record must exist *and* its input closure must be `strong`
/// (every upstream is a content hash). A `heuristic` closure means at least
/// one input is a mutable-source freshness signal whose data may have moved
/// on — such a table is not derivable.
///
/// **Known gap (asymmetry with `rocky restore`).** A `strong` closure over
/// *several* content-hashed upstreams passes this check, but `restore` refuses
/// any recipe with recorded upstreams (multi-input rebuild needs DAG
/// re-derivation, not yet implemented). So this check's pass set is strictly
/// larger than restore's recovery set. Pinned by
/// `gc_admits_multi_input_recipe_that_restore_refuses_known_gap` in
/// `commands/restore.rs`. Narrowing this predicate is a behavior change
/// pending a design decision.
pub(crate) fn check_recipe_recorded(class: &ReplayCheckModelOutput) -> GcCheckOutput {
    let strong = class.proof_class.as_deref() == Some("strong");
    let passed = class.has_provenance && strong;
    let detail = if !class.has_provenance {
        "no provenance record — the producing run was not content-addressed, so the recipe was \
         never captured"
            .to_string()
    } else if strong {
        "recipe + strong input closure recorded (every upstream is a content hash)".to_string()
    } else {
        format!(
            "input closure is weak (proof_class={}) — derived from a mutable source whose inputs \
             may have moved on",
            class.proof_class.as_deref().unwrap_or("unknown")
        )
    };
    GcCheckOutput {
        check: "recipe_recorded".to_string(),
        passed,
        detail,
    }
}

/// Check 2 — the recipe provably produces THESE EXACT bytes.
///
/// The `derivable` verdict for a `(run, model)` recipe comes from its
/// provenance, but a `(run, model)` can have recorded a **different** output
/// than the artifact under consideration — a sibling output, or a
/// re-materialization at a new hash. Without this check, that artifact would
/// inherit the recipe's "derivable" verdict even though the recipe rebuilds
/// *other* bytes, and evicting it would delete bytes nothing can rebuild.
///
/// So the verdict is bound to the specific candidate: its content hash (and,
/// when the provenance recorded paths, the aligned object path) must appear
/// among the provenance's recorded outputs. Anything else fails closed. This is
/// what makes the hash — not the `(run, model)` pair — the eviction identity.
///
/// Shared with `rocky restore` (`crate::commands::restore`), which re-runs the
/// same binding check before re-deriving a tombstoned artifact — the restore's
/// rebuild claim is bound to the exact evicted hash the same way the eviction
/// verdict was.
pub(crate) fn check_recipe_produces_output(
    artifact_hash: &str,
    artifact_path: &str,
    prov: Option<&ProvenanceRecord>,
) -> GcCheckOutput {
    let (passed, detail) = match prov {
        None => (
            false,
            "no provenance for the producing (run, model) — cannot prove the recipe produces \
             these exact bytes"
                .to_string(),
        ),
        Some(p) if p.output_blake3.is_empty() => (
            false,
            "provenance records no output hashes — cannot prove the recipe produces these exact \
             bytes"
                .to_string(),
        ),
        Some(p) => {
            // Prefer an exact (hash, path) pair when paths were recorded; fall
            // back to hash membership when the provenance carries no paths.
            // Either way the content hash must match — that is the identity.
            let matched = if p.output_path.is_empty() {
                p.output_blake3.iter().any(|h| h == artifact_hash)
            } else {
                p.output_blake3
                    .iter()
                    .zip(p.output_path.iter())
                    .any(|(h, path)| h == artifact_hash && path == artifact_path)
            };
            if matched {
                (
                    true,
                    "provenance records this exact output hash — the recipe rebuilds these bytes"
                        .to_string(),
                )
            } else {
                (
                    false,
                    format!(
                        "the producing (run, model)'s provenance records a DIFFERENT output than \
                         this artifact ({}…) — the recipe rebuilds other bytes, so evicting this \
                         would be unrecoverable",
                        artifact_hash.get(..12).unwrap_or(artifact_hash)
                    ),
                )
            }
        }
    };
    GcCheckOutput {
        check: "recipe_produces_output".to_string(),
        passed,
        detail,
    }
}

/// Check 3 — replayable and deterministic.
///
/// Reuses the [`classify_model`] verdict: the recording must be sufficient to
/// re-execute (`replayable`) *and* the SQL must be deterministic. A
/// nondeterministic recipe may still replay, but a rebuild need not reproduce
/// these exact bytes, so its bytes are unique history — never GC-eligible.
fn check_replayable(class: &ReplayCheckModelOutput) -> GcCheckOutput {
    let replayable = class.verdict == "replayable";
    let passed = replayable && !class.nondeterministic;
    let detail = if !replayable {
        if class.reasons.is_empty() {
            "recording is insufficient to re-execute the recipe".to_string()
        } else {
            format!("not replayable: {}", class.reasons.join("; "))
        }
    } else if class.nondeterministic {
        "recipe SQL contains a nondeterministic construct — a rebuild may not reproduce these \
         exact bytes; retained as unique history"
            .to_string()
    } else {
        "replayable from the recording; SQL is deterministic".to_string()
    };
    GcCheckOutput {
        check: "replayable".to_string(),
        passed,
        detail,
    }
}

/// Check 4 — unreferenced: no other live ledger pointer at these bytes.
///
/// `refcount == 1` means Rocky holds exactly one reference (this candidate),
/// so retiring it releases the bytes. `refcount > 1` means the bytes are
/// shared — a branch or a replayed run points at them too — and evicting
/// would corrupt every other referrer. `refcount == 0` cannot happen for a
/// row we are iterating, so it is treated as a mid-scan anomaly and fails
/// closed.
fn check_unreferenced(refcount: u64) -> GcCheckOutput {
    let passed = refcount == 1;
    let detail = if passed {
        "no other ledger reference (refcount = 1)".to_string()
    } else if refcount == 0 {
        "refcount is 0 — the ledger row vanished mid-scan; treated as not reclaimable (fail-closed)"
            .to_string()
    } else {
        format!(
            "shared bytes: {refcount} ledger references (branches / replayed runs) — evicting \
             would corrupt other referrers"
        )
    };
    GcCheckOutput {
        check: "unreferenced".to_string(),
        passed,
        detail,
    }
}

/// Check 5 — policy allows (surfaced, not yet enforced).
///
/// The GC policy plane — classification holds (`legal_hold` / `finance`),
/// retention windows, and the `gc` capability — arrives in a later phase.
/// This release surfaces the check with an explicit caveat rather than gating
/// on a policy plane that does not exist yet. Because there is no deletion
/// path, passing here only affects a report line, never data.
fn check_policy_allows() -> GcCheckOutput {
    GcCheckOutput {
        check: "policy_allows".to_string(),
        passed: true,
        detail: "no GC-scoped policy denies this artifact. Enforcement of classification holds \
                 (legal_hold / finance), retention windows, and a gc capability is a later phase; \
                 this release surfaces the check without gating on a policy plane."
            .to_string(),
    }
}

/// Check 6 — age / activity threshold.
///
/// The artifact must be at least `min_age_days` old. This adapter has no
/// read-tracking, so the age is *written*-age (build time), not read-recency
/// — stated conservatively so the report never implies it knows the last read.
fn check_age_threshold(
    written_at: DateTime<Utc>,
    now: DateTime<Utc>,
    min_age_days: i64,
) -> GcCheckOutput {
    let age_days = (now - written_at).num_days();
    let passed = age_days >= min_age_days;
    let detail = format!(
        "written {age_days} day(s) ago {} {min_age_days}-day threshold (written-age; no \
         read-tracking on this adapter, so read-recency is unknown)",
        if passed { "≥" } else { "<" }
    );
    GcCheckOutput {
        check: "age_threshold".to_string(),
        passed,
        detail,
    }
}

/// Estimate the cost to rebuild an artifact via replay.
///
/// A replay re-runs the recorded recipe, so the recorded build's duration and
/// scanned bytes are the honest predictor. Priced by the same
/// [`compute_observed_cost_usd`] model `rocky cost` uses. Always an estimate.
fn build_rebuild_cost(
    exec: Option<&ModelExecution>,
    adapter: Option<&AdapterCost>,
) -> GcRebuildCostOutput {
    let (duration_ms, bytes_scanned) = exec
        .map(|e| (e.duration_ms, e.bytes_scanned))
        .unwrap_or((0, None));
    let estimated_usd = adapter.and_then(|(_, wh, dbu_per_hour, cost_per_dbu)| {
        compute_observed_cost_usd(
            *wh,
            bytes_scanned,
            duration_ms,
            *dbu_per_hour,
            *cost_per_dbu,
        )
    });
    GcRebuildCostOutput {
        estimated: true,
        source_duration_ms: duration_ms,
        source_bytes_scanned: bytes_scanned,
        estimated_usd,
    }
}

/// Build one candidate from its representative artifact row + the joined
/// verdicts. Pure: the CLI wires store reads into these arguments, tests drive
/// them directly.
///
/// `prov` is the producing `(run, model)`'s provenance — required for the
/// hash-binding check ([`check_recipe_produces_output`]) so the derivable
/// verdict is bound to *this artifact's* content hash, never inherited from a
/// sibling output of the same recipe.
#[allow(clippy::too_many_arguments)]
fn build_candidate(
    artifact: &ArtifactRecord,
    refcount: u64,
    class: &ReplayCheckModelOutput,
    exec: Option<&ModelExecution>,
    prov: Option<&ProvenanceRecord>,
    now: DateTime<Utc>,
    min_age_days: i64,
    adapter: Option<&AdapterCost>,
) -> GcCandidateOutput {
    let checks = vec![
        check_recipe_recorded(class),
        check_recipe_produces_output(&artifact.blake3_hash, &artifact.file_path, prov),
        check_replayable(class),
        check_unreferenced(refcount),
        check_policy_allows(),
        check_age_threshold(artifact.written_at, now, min_age_days),
    ];
    let derivable = checks.iter().all(|c| c.passed);

    GcCandidateOutput {
        model_name: artifact.model_name.clone(),
        run_id: artifact.run_id.clone(),
        blake3_hash: artifact.blake3_hash.clone(),
        size_bytes: artifact.size_bytes,
        written_at: artifact.written_at.to_rfc3339(),
        refcount,
        recipe_id: exec.and_then(|e| e.recipe_hash.clone()),
        input_proof_class: class.proof_class.clone(),
        rebuild_cost: build_rebuild_cost(exec, adapter),
        derivable,
        checks,
    }
}

/// Standing report-wide caveats an operator must read before trusting the
/// numbers. These are the honesty guardrails the plan mandates.
fn report_notes(min_age_days: i64) -> Vec<String> {
    vec![
        "`derivable` means rebuildable from the recorded recipe — NOT that the file is safe to \
         delete. `rocky apply` reclaims an artifact only when its file is a proven `remove` in \
         its table's Delta log; the append-only writer keeps older versions live, so a derivable \
         file still referenced by the live table is held (only an external compaction/VACUUM \
         makes it reclaimable)."
            .to_string(),
        "Scope: refcounts see Rocky-managed references only. A warehouse-side reference Rocky \
         never recorded (a BI extract, a notebook SELECT INTO) is invisible here."
            .to_string(),
        "Age check uses written-age, not read-recency: this adapter exposes no read-tracking, so \
         recency of reads is unknown and stated conservatively."
            .to_string(),
        "Rebuild-cost figures are estimates modeled from each artifact's recorded build metrics, \
         not measured rebuilds."
            .to_string(),
        format!(
            "policy_allows is surfaced but not enforced: classification holds, retention windows, \
             and the gc capability arrive in a later phase (min age applied: {min_age_days} days)."
        ),
    ]
}

/// A derivable-verdict candidate paired with the raw ledger row and the
/// producing execution's recipe-identity triple that the *plan* and
/// *tombstone* need but the public [`GcCandidateOutput`] does not carry
/// (`file_path`, `commit_version`, `input_hash` / `env_hash` / `hash_scheme`).
///
/// [`GcCandidateOutput`] is a projection of [`Self::output`]; the report, the
/// plan, and the apply-time re-verification all consume the *same*
/// [`gather_eviction_candidates`] verdict, so there is exactly one eligibility
/// path — never a forked or loosened copy.
struct EvictionCandidate {
    artifact: ArtifactRecord,
    output: GcCandidateOutput,
    recipe_hash: Option<String>,
    input_hash: Option<String>,
    input_proof_class: Option<String>,
    env_hash: Option<String>,
    hash_scheme: Option<String>,
    /// The producing model's content-addressed `storage_prefix` (its table
    /// root), parsed from the provenance's canonical `ModelIr`. The apply-time
    /// manifest-truth liveness gate needs it to read *this* table's Delta log
    /// (scoped to the candidate's own target — never a same-named model in
    /// another catalog). `None` when the provenance is missing or its IR is not
    /// a content-addressed model ⇒ liveness is unverifiable ⇒ the eviction is
    /// held (fail-closed).
    storage_prefix: Option<String>,
}

/// Extract a content-addressed model's `storage_prefix` (table root) from a
/// provenance record's canonical `ModelIr`. `None` for a missing provenance, an
/// unparseable IR, or a non-content-addressed materialization.
fn storage_prefix_from_provenance(prov: Option<&ProvenanceRecord>) -> Option<String> {
    let prov = prov?;
    let ir: rocky_ir::ModelIr = serde_json::from_str(&prov.model_ir_canonical_json).ok()?;
    match ir.materialization {
        rocky_ir::MaterializationStrategy::ContentAddressed { storage_prefix, .. } => {
            Some(storage_prefix)
        }
        _ => None,
    }
}

/// Assemble the derivability verdict for every distinct content hash in the
/// ledger — the single source of truth the report, the plan, and the apply-time
/// re-verification all share.
///
/// All store reads live here (the honest reachability seam); the verdicts are a
/// pure function of the ledger contents at `now`. Read-only: opens no write
/// transaction and touches no warehouse.
fn gather_eviction_candidates(
    store: &StateStore,
    adapter: Option<&AdapterCost>,
    now: DateTime<Utc>,
    min_age_days: i64,
) -> Result<Vec<EvictionCandidate>> {
    let artifacts = store
        .list_all_artifacts()
        .context("failed to read the artifact ledger")?;

    // Group by content hash — each distinct hash is one physical artifact.
    // Representative row = the most recent producer (deterministic tie-break
    // on run|model) so the reported model/run is stable across scans.
    let mut by_hash: HashMap<String, Vec<ArtifactRecord>> = HashMap::new();
    for artifact in artifacts {
        by_hash
            .entry(artifact.blake3_hash.clone())
            .or_default()
            .push(artifact);
    }

    let mut run_cache: HashMap<String, Option<RunRecord>> = HashMap::new();
    let mut out: Vec<EvictionCandidate> = Vec::with_capacity(by_hash.len());

    for (hash, mut rows) in by_hash {
        rows.sort_by(|a, b| {
            b.written_at
                .cmp(&a.written_at)
                .then_with(|| a.run_id.cmp(&b.run_id))
                .then_with(|| a.model_name.cmp(&b.model_name))
        });
        let representative = rows.into_iter().next().expect("hash group is non-empty");

        // Refcount via the shipped Phase-6 primitive — the sanctioned "is any
        // other pointer live?" query, not a re-count of the group.
        let refcount = store
            .refcount_for_hash(&hash)
            .with_context(|| format!("failed to refcount artifact {hash}"))?;

        let class = classify_model(store, &representative.run_id, &representative.model_name);

        // Provenance for the producing (run, model) — used to bind the
        // derivable verdict to THIS artifact's exact content hash (a (run,
        // model) can have produced a different output than this row).
        let prov = store
            .get_provenance(&representative.run_id, &representative.model_name)
            .ok()
            .flatten();

        let run = run_cache
            .entry(representative.run_id.clone())
            .or_insert_with(|| store.get_run(&representative.run_id).ok().flatten());
        let exec = run.as_ref().and_then(|r| {
            r.models_executed
                .iter()
                .find(|m| m.model_name == representative.model_name)
        });

        let output = build_candidate(
            &representative,
            refcount,
            &class,
            exec,
            prov.as_ref(),
            now,
            min_age_days,
            adapter,
        );
        let (recipe_hash, input_hash, input_proof_class, env_hash, hash_scheme) = exec
            .map(|e| {
                (
                    e.recipe_hash.clone(),
                    e.input_hash.clone(),
                    e.input_proof_class.clone(),
                    e.env_hash.clone(),
                    e.hash_scheme.clone(),
                )
            })
            .unwrap_or((None, None, None, None, None));

        let storage_prefix = storage_prefix_from_provenance(prov.as_ref());

        out.push(EvictionCandidate {
            artifact: representative,
            output,
            recipe_hash,
            input_hash,
            input_proof_class,
            env_hash,
            hash_scheme,
            storage_prefix,
        });
    }

    // Newest write first; stable tie-break on hash for deterministic output.
    out.sort_by(|a, b| {
        b.output
            .written_at
            .cmp(&a.output.written_at)
            .then_with(|| a.output.blake3_hash.cmp(&b.output.blake3_hash))
    });

    Ok(out)
}

/// Assemble the full inventory from a state store. A thin projection over
/// [`gather_eviction_candidates`] — the report is the derivable verdicts plus
/// the aggregate headline.
///
/// Read-only: opens no write transaction and touches no warehouse.
fn gather_report(
    store: &StateStore,
    adapter: Option<&AdapterCost>,
    now: DateTime<Utc>,
    min_age_days: i64,
) -> Result<GcReportOutput> {
    let candidates: Vec<GcCandidateOutput> =
        gather_eviction_candidates(store, adapter, now, min_age_days)?
            .into_iter()
            .map(|c| c.output)
            .collect();

    let managed_bytes: u64 = candidates.iter().map(|c| c.size_bytes).sum();
    let derivable_bytes: u64 = candidates
        .iter()
        .filter(|c| c.derivable)
        .map(|c| c.size_bytes)
        .sum();
    let derivable_count = candidates.iter().filter(|c| c.derivable).count();
    let derivable_pct =
        (managed_bytes > 0).then(|| (derivable_bytes as f64 / managed_bytes as f64) * 100.0);

    Ok(GcReportOutput {
        version: VERSION.to_string(),
        command: "gc --derivable --dry-run".to_string(),
        managed_bytes,
        derivable_bytes,
        derivable_pct,
        artifact_count: candidates.len(),
        derivable_count,
        read_tracking_available: false,
        min_age_days,
        notes: report_notes(min_age_days),
        candidates,
    })
}

/// Resolve the billed-warehouse type + cost inputs from `rocky.toml`,
/// degrading gracefully to `None` when the config can't be read or the adapter
/// is not a billed warehouse. Mirrors `rocky cost`'s resolution so the two
/// surfaces price rebuilds identically.
fn load_adapter_cost(config_path: &Path) -> Option<AdapterCost> {
    match load_rocky_config(config_path) {
        Ok(cfg) => {
            let dbu_per_hour = warehouse_size_to_dbu_per_hour(&cfg.cost.warehouse_size);
            let cost_per_dbu = cfg.cost.compute_cost_per_dbu;
            let preferred = cfg
                .adapters
                .iter()
                .find(|(k, _)| k.as_str() == "default")
                .or_else(|| cfg.adapters.iter().next())?;
            let wh = WarehouseType::from_adapter_type(&preferred.1.adapter_type)?;
            Some((
                preferred.1.adapter_type.clone(),
                wh,
                dbu_per_hour,
                cost_per_dbu,
            ))
        }
        Err(err) => {
            warn!(
                "failed to load config at {} — rebuild-cost USD estimates will be omitted: {err}",
                config_path.display()
            );
            None
        }
    }
}

fn print_table(report: &GcReportOutput) {
    println!("Rocky derivability inventory (dry-run — nothing is deleted)");
    println!();
    match report.derivable_pct {
        Some(pct) => println!(
            "{} bytes / {:.1}% of {} managed bytes is derivable ({} of {} artifacts)",
            report.derivable_bytes,
            pct,
            report.managed_bytes,
            report.derivable_count,
            report.artifact_count
        ),
        None => {
            println!("no Rocky-managed content-addressed artifacts found — nothing to inventory")
        }
    }
    println!();

    for c in &report.candidates {
        println!(
            "  {}  {}  {} bytes  refcount={}  {}",
            if c.derivable {
                "DERIVABLE    "
            } else {
                "not-derivable"
            },
            c.model_name,
            c.size_bytes,
            c.refcount,
            c.blake3_hash.get(..12).unwrap_or(c.blake3_hash.as_str())
        );
        println!("      run: {}  written: {}", c.run_id, c.written_at);
        if let Some(id) = &c.recipe_id {
            println!("      recipe: {}", id.get(..12).unwrap_or(id.as_str()));
        }
        let cost = &c.rebuild_cost;
        match cost.estimated_usd {
            Some(usd) => println!(
                "      rebuild-cost (est): ${:.6}  ({} ms recorded)",
                usd, cost.source_duration_ms
            ),
            None => println!(
                "      rebuild-cost (est): unpriced  ({} ms recorded)",
                cost.source_duration_ms
            ),
        }
        for check in &c.checks {
            println!(
                "      [{}] {}: {}",
                if check.passed { "PASS" } else { "FAIL" },
                check.check,
                check.detail
            );
        }
    }

    println!();
    println!("notes:");
    for note in &report.notes {
        println!("  - {note}");
    }
}

/// Execute `rocky gc --derivable --dry-run`.
///
/// Opens the state store read-only, assembles the derivability inventory, and
/// emits it as JSON or a human table. No mutation, no warehouse access, no
/// deletion path.
pub fn run_gc_derivable(
    state_path: &Path,
    config_path: &Path,
    min_age_days: i64,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let adapter = load_adapter_cost(config_path);
    let report = gather_report(&store, adapter.as_ref(), Utc::now(), min_age_days)?;

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_table(&report);
    }
    Ok(())
}

// ===========================================================================
// Plan — `rocky gc --derivable` (no --dry-run)
// ===========================================================================

/// Operator caveats surfaced on a `gc` plan and its apply.
fn gc_plan_notes() -> Vec<String> {
    vec![
        "This plan lists artifacts proved *derivable* (rebuildable from the recorded recipe) at \
         plan time. Derivable is NOT the same as reclaimable: `rocky apply` additionally requires \
         each file to be a proven `remove` in its table's Delta log before deleting it. The \
         append-only writer never removes on its own, so a file still referenced by the live \
         table (including an older version) is HELD — only an external compaction/VACUUM makes it \
         reclaimable. Expect apply to reclaim fewer artifacts than this plan lists (and none on a \
         creds-free run, where liveness cannot be verified)."
            .to_string(),
        "`rocky apply` re-verifies each artifact against the live ledger AND its table's Delta \
         log before evicting — a reference or liveness change between plan and apply holds the \
         eviction (fail-closed)."
            .to_string(),
        "Every eviction writes a durable tombstone (recipe triple + restore pointer) BEFORE \
         the ledger row is retired. `rocky restore` rebuilds single-input, non-partitioned \
         recipes from that tombstone; a recipe with recorded upstreams is refused by restore \
         today (multi-input DAG re-derivation is a later phase) and must be recovered by \
         re-running its pipeline."
            .to_string(),
        "Deletion is symmetric-caution gated: review with `rocky review <plan-id> --approve`, \
         then `rocky apply <plan-id>`. There is no direct-delete path."
            .to_string(),
    ]
}

/// Build a [`GcPlan`] from the derivable subset of the current inventory.
///
/// Returns `None` when nothing is derivable — the caller refuses to write an
/// empty plan.
fn build_gc_plan(candidates: &[EvictionCandidate], min_age_days: i64) -> Option<GcPlan> {
    let evictions: Vec<GcPlanEviction> = candidates
        .iter()
        .filter(|c| c.output.derivable)
        .map(|c| GcPlanEviction {
            model_name: c.artifact.model_name.clone(),
            run_id: c.artifact.run_id.clone(),
            blake3_hash: c.artifact.blake3_hash.clone(),
            file_path: c.artifact.file_path.clone(),
            size_bytes: c.artifact.size_bytes,
            commit_version: c.artifact.commit_version,
            written_at: c.artifact.written_at.to_rfc3339(),
            recipe_hash: c.recipe_hash.clone(),
            input_hash: c.input_hash.clone(),
            input_proof_class: c.input_proof_class.clone(),
            env_hash: c.env_hash.clone(),
            hash_scheme: c.hash_scheme.clone(),
        })
        .collect();

    if evictions.is_empty() {
        return None;
    }
    let total_bytes = evictions.iter().map(|e| e.size_bytes).sum();
    Some(GcPlan {
        version: VERSION.to_string(),
        min_age_days,
        total_bytes,
        evictions,
    })
}

/// The representative `model` field for the gc plan's single review-escalation
/// ledger row — a plan-scope summary, since one reclamation plan spans many
/// artifacts and models and one row per artifact would bloat the ledger.
fn gc_plan_scope_summary(plan: &GcPlan) -> String {
    let models: BTreeSet<&str> = plan
        .evictions
        .iter()
        .map(|e| e.model_name.as_str())
        .collect();
    format!(
        "gc: {} artifact(s) across {} model(s)",
        plan.evictions.len(),
        models.len()
    )
}

/// Execute `rocky gc --derivable` in plan mode (no `--dry-run`): write a GC
/// plan to the plan store. **Never deletes** — the plan is a scoped,
/// review-gated proposal.
///
/// The `principal` is the CLI-resolved invoker (`ROCKY_PRINCIPAL` / default
/// human). It rides on the plan so an agent-scoped `deny agent gc` rule fires
/// on an agent-run GC; the review gate is unconditional regardless.
pub fn run_gc_plan(
    state_path: &Path,
    config_path: &Path,
    min_age_days: i64,
    principal: PolicyPrincipal,
    json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_gc_plan_in(&cwd, state_path, config_path, min_age_days, principal, json)
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
pub(crate) fn run_gc_plan_in(
    root: &Path,
    state_path: &Path,
    config_path: &Path,
    min_age_days: i64,
    principal: PolicyPrincipal,
    json: bool,
) -> Result<()> {
    let adapter = load_adapter_cost(config_path);
    let candidates = {
        let store = StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        gather_eviction_candidates(&store, adapter.as_ref(), Utc::now(), min_age_days)?
        // The read handle drops here so the escalation write below can open
        // its own handle on the same store.
    };

    let Some(plan) = build_gc_plan(&candidates, min_age_days) else {
        if json {
            println!(
                "{}",
                serde_json::json!({
                    "version": VERSION,
                    "command": "gc --derivable",
                    "eviction_count": 0,
                    "message": "nothing is provably derivable — no plan written"
                })
            );
        } else {
            println!(
                "Nothing is provably derivable right now — no reclamation plan written. \
                 Run `rocky gc --derivable --dry-run` to see why each artifact was held."
            );
        }
        return Ok(());
    };

    let plan_id = write_plan_with_principal(root, PlanKind::Gc, &plan, principal)
        .context("failed to persist the gc plan")?;

    // A gc plan never passes `evaluate_apply_policy` before its apply bails on
    // the missing review marker, so record its escalation at creation — this
    // one plan-level row is what puts it in the review queue (and in front of
    // the governor's MCP approve path).
    record_plan_review_escalation(
        state_path,
        &plan_id,
        principal,
        PolicyCapability::Gc,
        &gc_plan_scope_summary(&plan),
        "gc plan awaits review — deletion is unconditionally review-gated (even a human gc \
         goes through review, never a direct delete)",
    );

    let output = GcPlanOutput {
        version: VERSION.to_string(),
        command: "gc --derivable".to_string(),
        plan_id: plan_id.clone(),
        eviction_count: plan.evictions.len(),
        total_bytes: plan.total_bytes,
        review_required: true,
        notes: gc_plan_notes(),
        evictions: plan.evictions,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_plan_table(&output);
    }
    Ok(())
}

fn print_plan_table(output: &GcPlanOutput) {
    println!(
        "Rocky gc plan {} — {} derivable artifact(s), {} bytes proposed for reclamation",
        output.plan_id.get(..12).unwrap_or(&output.plan_id),
        output.eviction_count,
        output.total_bytes
    );
    println!();
    for e in &output.evictions {
        println!(
            "  {}  {} bytes  {}",
            e.model_name,
            e.size_bytes,
            e.blake3_hash.get(..12).unwrap_or(&e.blake3_hash)
        );
        println!("      run: {}  path: {}", e.run_id, e.file_path);
    }
    println!();
    println!("This plan is review-gated. To proceed:");
    println!("  rocky review {} --approve", output.plan_id);
    println!("  rocky apply  {}", output.plan_id);
    println!();
    println!("notes:");
    for note in &output.notes {
        println!("  - {note}");
    }
}

// ===========================================================================
// Apply — `rocky apply <gc-plan>`: tombstone + evict + physical reclaim
// ===========================================================================

/// The manifest-truth reclaimability verdict for a candidate artifact.
pub(crate) enum ReclaimVerdict {
    /// The candidate's file is **affirmatively proven removed** in its own
    /// table's Delta log (an external compaction/VACUUM retired it) — the
    /// ledger row may be tombstoned + retired. Carries the Delta head version
    /// the proof validated against, for the TOCTOU re-check + version-scoped
    /// tombstone.
    Reclaimable { head_version: u64 },
    /// The candidate is held: not provably removed, wrong/unresolvable table,
    /// or the log could not be validated. Carries the operator-facing reason.
    Held(String),
}

/// Decides whether a content-addressed artifact may be **tombstoned + retired**
/// from the ledger, by reading the manifest truth of its own table's Delta log.
///
/// The append-only UniForm writer emits `add` actions and **never** `remove`,
/// so multiple file versions are live at once and no ledger/hash heuristic can
/// tell which files a live table still references — only the `_delta_log` can.
/// An artifact is reclaimable **iff** the strict [`RemovalProof`] affirms it was
/// `remove`d in its own table (bound to that table by prefix membership + its
/// own `add` at its recorded commit version). Every other state — still live,
/// absent/checkpoint-truncated, malformed, wrong prefix, unverifiable — holds
/// (fail-closed).
///
/// Physical byte-deletion is deliberately **not** performed here (it needs a
/// protocol-aware VACUUM: retention windows + TOCTOU-safe deletion); the
/// tombstone + retired ledger row is the eviction of record. The oracle still
/// gates the tombstone so a wrong tombstone can never mislead restore/reuse.
///
/// Injectable so tests exercise the SAME decision path over an in-memory store.
#[async_trait]
pub(crate) trait LivenessOracle: Send + Sync {
    /// `storage_prefix` is the candidate table's root; `file_path` is the
    /// artifact's object path; `commit_version` is its recorded Delta version.
    async fn reclaim_verdict(
        &self,
        storage_prefix: &str,
        file_path: &str,
        commit_version: u64,
    ) -> ReclaimVerdict;
}

/// Production oracle: builds the s3 object store for the candidate's own table
/// and runs the shared [`removal_proof_given_store`] decision path.
///
/// Reachable only with object-store credentials (content-addressed storage is
/// s3-only); creds-free the store build/read fails and the candidate is held.
struct ManifestLivenessOracle;

#[async_trait]
impl LivenessOracle for ManifestLivenessOracle {
    async fn reclaim_verdict(
        &self,
        storage_prefix: &str,
        file_path: &str,
        commit_version: u64,
    ) -> ReclaimVerdict {
        let store = match super::run_content_addressed::build_object_store(storage_prefix) {
            Ok(s) => s,
            Err(e) => {
                return ReclaimVerdict::Held(format!(
                    "could not reach the object store to verify removal: {e}; HELD (fail-closed)"
                ));
            }
        };
        map_removal_proof(
            &super::run_content_addressed::removal_proof_given_store(
                store,
                storage_prefix,
                file_path,
                commit_version,
            )
            .await,
        )
    }
}

/// Map a strict [`RemovalProof`] to the gc [`ReclaimVerdict`]: only an
/// affirmative `ProvenRemoved` reclaims; every hold reason is surfaced.
pub(crate) fn map_removal_proof(
    proof: &rocky_iceberg::uniform_writer::RemovalProof,
) -> ReclaimVerdict {
    use rocky_iceberg::uniform_writer::RemovalProof;
    match proof {
        RemovalProof::ProvenRemoved { head_version } => ReclaimVerdict::Reclaimable {
            head_version: *head_version,
        },
        RemovalProof::Held(reason) => ReclaimVerdict::Held(format!(
            "not provably removed from the table's Delta log ({}); HELD (fail-closed)",
            reason.as_str()
        )),
    }
}

/// Apply caveats surfaced on the eviction result.
fn gc_apply_notes() -> Vec<String> {
    vec![
        "Refcounts see Rocky-ledger references only. Multi-ref safety on the UniForm path \
         (branch / env / downstream-deferred-read pointers) is code-reviewed, not driven here."
            .to_string(),
        "Liveness is manifest-truth: an artifact is tombstoned + retired only when its file is \
         AFFIRMATIVELY PROVEN removed in its own table's Delta log (bound by prefix membership + \
         its own `add` at its recorded commit version). The append-only writer never removes on \
         its own, so a still-referenced file (including an older version) is HELD — only an \
         external compaction/VACUUM makes a file reclaimable. Any anomaly (checkpoint, version \
         gap, malformed commit, unsupported protocol, wrong prefix, unverifiable log) holds \
         (fail-closed)."
            .to_string(),
        "The eviction of record is the durable tombstone + retired ledger row, which records the \
         full recipe. Physical byte-deletion is NOT performed: reclaiming bytes \
         safely requires a protocol-aware VACUUM (Delta tombstone-retention windows plus \
         TOCTOU-safe deletion against concurrent re-adds), which is future work. Setting `[gc] \
         physical_delete = true` is a hard error until then."
            .to_string(),
        "`rocky restore <target>` writes a review-gated plan that rebuilds the artifact from its \
         tombstone's recorded recipe and asserts the recomputed blake3 equals the tombstoned hash \
         before any write becomes visible. KNOWN LIMITATION: restore covers single-input, \
         non-partitioned, content-addressed recipes only — it refuses a recipe with recorded \
         upstreams rather than substituting current data, so a multi-input artifact evicted here \
         is recovered by re-running its pipeline, not by `rocky restore`."
            .to_string(),
        "KNOWN LIMITATION: the liveness gate is a conservative-best-effort reader of the Delta \
         log, not a full Delta-protocol implementation. It HOLDs on any log shape it cannot \
         unambiguously prove removed, so on unusual external-writer path/protocol spellings it \
         may forgo reclamation (over-hold) or, rarely, retire a still-live artifact's LEDGER ROW \
         (metadata-only — a superseded tombstone causes a conservative rebuild, never byte loss \
         or wrong results; no bytes are deleted). Full Delta-protocol fidelity (Delta Kernel) is \
         future work."
            .to_string(),
    ]
}

/// The eviction engine: re-derive eligibility against the live ledger, then for
/// each planned artifact that is **still derivable, an exact identity match, AND
/// affirmatively proven removed from its own table's Delta log** write its
/// tombstone and retire its ledger row atomically. No bytes are deleted — the
/// tombstone + retired ledger row is the eviction of record.
///
/// Pure over its inputs (`store`, `oracle`, `now`) so tests drive it directly
/// with a seeded store and a deterministic liveness oracle. The review + policy
/// gates live in [`run_gc_apply_in`] and run *before* this is called.
///
/// **Apply re-derives; it does not trust the plan.** The reviewed plan_id
/// approves a *proposal*, but the persisted rows carry a `blake3_hash` that
/// could be stale, drifted, or hand-authored to point a `file_path` at
/// *different* bytes than the hash claims. So apply:
///
/// 1. re-runs [`gather_eviction_candidates`] against the live ledger and indexes
///    candidates by their FULL identity `(run, model, file_path, blake3_hash)`;
/// 2. resolves the exact live row the plan row names, and refuses unless the
///    live row's hash equals the planned hash;
/// 3. re-derives derivability *now* AND consults the [`LivenessOracle`] for
///    manifest truth — tombstoning only when the candidate's file is
///    affirmatively proven removed in its own table's Delta log (the
///    append-only writer keeps older versions live, so a ledger/hash heuristic
///    cannot authorize a retirement);
/// 4. builds the tombstone from the LIVE candidate, never the plan payload;
/// 5. relies on [`StateStore::evict_artifact`]'s in-transaction hash check as a
///    final guard against a race.
///
/// Any plan row that fails to match a current derivable candidate, or whose file
/// is not provably removed, is refused/held — nothing is retired. The hash is
/// the eviction identity, never the `(run, model)` pair.
async fn execute_gc_apply(
    store: &StateStore,
    oracle: &dyn LivenessOracle,
    plan_id: &str,
    plan: &GcPlan,
    now: DateTime<Utc>,
) -> Result<GcApplyOutput> {
    // Re-run the SAME eligibility path the plan used, against the live ledger,
    // indexed by FULL identity — so a plan row only matches a candidate that is
    // byte-for-byte the same artifact. A plan naming a hash but pointing its
    // path at different bytes finds no match and is refused.
    let fresh = gather_eviction_candidates(store, None, now, plan.min_age_days)?;
    let by_key: HashMap<(&str, &str, &str, &str), &EvictionCandidate> = fresh
        .iter()
        .map(|c| {
            (
                (
                    c.artifact.run_id.as_str(),
                    c.artifact.model_name.as_str(),
                    c.artifact.file_path.as_str(),
                    c.artifact.blake3_hash.as_str(),
                ),
                c,
            )
        })
        .collect();

    let mut evicted: Vec<GcEvictedOutput> = Vec::new();
    let mut refused: Vec<GcRefusedOutput> = Vec::new();
    let mut already_evicted: Vec<String> = Vec::new();

    for ev in &plan.evictions {
        // Resolve the EXACT row this entry names. Absent ⇒ a prior apply evicted
        // it (or it never existed) — a clean idempotent no-op.
        let row = store
            .get_artifact(&ev.run_id, &ev.model_name, &ev.file_path)
            .with_context(|| format!("failed to read artifact row for {}", ev.blake3_hash))?;
        let Some(row) = row else {
            already_evicted.push(ev.blake3_hash.clone());
            continue;
        };

        // The live row's hash MUST match the plan's claimed hash. A mismatch
        // means the plan is stale or hand-authored to point at other bytes:
        // refuse and delete nothing. (Without this, apply could verify one
        // hash's derivability and delete a different-hash row.)
        if row.blake3_hash != ev.blake3_hash {
            refused.push(GcRefusedOutput {
                model_name: ev.model_name.clone(),
                run_id: ev.run_id.clone(),
                blake3_hash: ev.blake3_hash.clone(),
                size_bytes: ev.size_bytes,
                reason: format!(
                    "planned hash {}… does not match the live artifact at this location ({}…) — \
                     the plan is stale or hand-authored; refused (fail-closed)",
                    ev.blake3_hash.get(..12).unwrap_or(&ev.blake3_hash),
                    row.blake3_hash.get(..12).unwrap_or(&row.blake3_hash),
                ),
                failed_checks: Vec::new(),
            });
            continue;
        }

        // Re-derive: does a FRESH candidate with this EXACT identity qualify as
        // derivable NOW? Trust the live candidate, never the plan payload.
        let key = (
            ev.run_id.as_str(),
            ev.model_name.as_str(),
            ev.file_path.as_str(),
            row.blake3_hash.as_str(),
        );
        match by_key.get(&key).copied() {
            Some(cand) if cand.output.derivable => {
                // Manifest-truth liveness gate (authoritative). The append-only
                // UniForm writer never emits `remove`, so an older content-hash
                // file can stay live in the current snapshot alongside a newer
                // one — no ledger/hash heuristic can tell. Read THIS table's
                // Delta log and evict only a file that is a proven `remove`;
                // still-live, absent/truncated, or unverifiable (incl.
                // creds-free) all HOLD.
                let Some(prefix) = cand.storage_prefix.as_deref() else {
                    refused.push(GcRefusedOutput {
                        model_name: cand.artifact.model_name.clone(),
                        run_id: cand.artifact.run_id.clone(),
                        blake3_hash: cand.artifact.blake3_hash.clone(),
                        size_bytes: cand.artifact.size_bytes,
                        reason: "held by the Delta-log liveness gate: no content-addressed \
                                 storage_prefix resolved from provenance — cannot verify the file \
                                 was removed from the table; HELD (fail-closed)"
                            .to_string(),
                        failed_checks: Vec::new(),
                    });
                    continue;
                };
                let verdict = oracle
                    .reclaim_verdict(
                        prefix,
                        &cand.artifact.file_path,
                        cand.artifact.commit_version,
                    )
                    .await;
                let head_version = match verdict {
                    ReclaimVerdict::Reclaimable { head_version } => head_version,
                    ReclaimVerdict::Held(reason) => {
                        refused.push(GcRefusedOutput {
                            model_name: cand.artifact.model_name.clone(),
                            run_id: cand.artifact.run_id.clone(),
                            blake3_hash: cand.artifact.blake3_hash.clone(),
                            size_bytes: cand.artifact.size_bytes,
                            reason: format!("held by the Delta-log liveness gate: {reason}"),
                            failed_checks: Vec::new(),
                        });
                        continue;
                    }
                };

                // TOCTOU narrowing (finding 4): the proof read the Delta log,
                // but the redb retire below happens later; an external Delta
                // writer could re-add the exact path in between (the state-store
                // hash check can't see a Delta-log change). Re-run the proof
                // immediately before the write; HOLD unless it is STILL provably
                // removed AND the head version is unchanged (any new commit to
                // the table — even unrelated — holds, fail-closed). A residual
                // window remains between this re-check and the redb commit; the
                // tombstone is version-scoped to `head_version` so a
                // restore/reuse consumer voids it if the path is later re-added
                // (see `TombstoneRecord::observed_delta_version`).
                match oracle
                    .reclaim_verdict(
                        prefix,
                        &cand.artifact.file_path,
                        cand.artifact.commit_version,
                    )
                    .await
                {
                    ReclaimVerdict::Reclaimable {
                        head_version: recheck,
                    } if recheck == head_version => {}
                    _ => {
                        refused.push(GcRefusedOutput {
                            model_name: cand.artifact.model_name.clone(),
                            run_id: cand.artifact.run_id.clone(),
                            blake3_hash: cand.artifact.blake3_hash.clone(),
                            size_bytes: cand.artifact.size_bytes,
                            reason: "held by the Delta-log liveness gate: the table's Delta head \
                                     advanced between the removal proof and the retire (or the \
                                     file was re-added) — HELD (fail-closed)"
                                .to_string(),
                            failed_checks: Vec::new(),
                        });
                        continue;
                    }
                }

                // Tombstone built from the LIVE candidate (authoritative identity
                // + recipe triple), version-scoped to the proven Delta head.
                let tombstone = TombstoneRecord {
                    blake3_hash: cand.artifact.blake3_hash.clone(),
                    run_id: cand.artifact.run_id.clone(),
                    model_name: cand.artifact.model_name.clone(),
                    file_path: cand.artifact.file_path.clone(),
                    size_bytes: cand.artifact.size_bytes,
                    commit_version: cand.artifact.commit_version,
                    recipe_hash: cand.recipe_hash.clone(),
                    input_hash: cand.input_hash.clone(),
                    input_proof_class: cand.input_proof_class.clone(),
                    env_hash: cand.env_hash.clone(),
                    hash_scheme: cand.hash_scheme.clone(),
                    evicted_at: now,
                    plan_id: plan_id.to_string(),
                    physical_reclaimed: false,
                    observed_delta_version: Some(head_version),
                    restored_at: None,
                    restore_plan_id: None,
                };

                // Atomic tombstone + ledger-row retirement, hash-checked inside
                // `evict_artifact` (final guard against a race). The restore
                // safety net commits before anything else can touch the bytes.
                let outcome = store
                    .evict_artifact(
                        &tombstone,
                        &cand.artifact.run_id,
                        &cand.artifact.model_name,
                        &cand.artifact.file_path,
                    )
                    .with_context(|| {
                        format!("failed to evict artifact {}", cand.artifact.blake3_hash)
                    })?;
                match outcome {
                    EvictOutcome::AlreadyAbsent => {
                        // Lost a race — the row vanished after we read it. No
                        // tombstone written; treat as an idempotent no-op.
                        already_evicted.push(cand.artifact.blake3_hash.clone());
                        continue;
                    }
                    EvictOutcome::HashMismatch { found, .. } => {
                        // The row was rewritten to different bytes between
                        // derivation and eviction. Refuse — deleted nothing.
                        refused.push(GcRefusedOutput {
                            model_name: cand.artifact.model_name.clone(),
                            run_id: cand.artifact.run_id.clone(),
                            blake3_hash: cand.artifact.blake3_hash.clone(),
                            size_bytes: cand.artifact.size_bytes,
                            reason: format!(
                                "the ledger row changed to hash {}… between derivation and \
                                 eviction — refused (fail-closed)",
                                found.get(..12).unwrap_or(&found),
                            ),
                            failed_checks: Vec::new(),
                        });
                        continue;
                    }
                    EvictOutcome::Evicted => {}
                }

                // No physical byte-delete: the tombstone + retired ledger row is
                // the eviction of record. Safe reclamation of the bytes requires
                // a protocol-aware VACUUM (retention windows + TOCTOU-safe
                // deletion), which is future work — `physical_reclaimed` stays
                // false by construction.
                evicted.push(GcEvictedOutput {
                    model_name: cand.artifact.model_name.clone(),
                    run_id: cand.artifact.run_id.clone(),
                    blake3_hash: cand.artifact.blake3_hash.clone(),
                    size_bytes: cand.artifact.size_bytes,
                    tombstone_recorded: true,
                    physical_reclaimed: false,
                    physical_status: "not attempted — physical reclamation is future \
                                      protocol-aware VACUUM work; the tombstone + retired ledger \
                                      row is the eviction of record"
                        .to_string(),
                });
            }
            other => {
                let (reason, failed_checks) = match other {
                    Some(cand) => {
                        let failed: Vec<GcCheckOutput> = cand
                            .output
                            .checks
                            .iter()
                            .filter(|c| !c.passed)
                            .cloned()
                            .collect();
                        let names = failed
                            .iter()
                            .map(|c| c.check.as_str())
                            .collect::<Vec<_>>()
                            .join(", ");
                        (
                            format!(
                                "no longer derivable at apply time — failing checks: [{names}]"
                            ),
                            failed,
                        )
                    }
                    None => (
                        "no matching live derivable artifact at this identity — refused \
                         (fail-closed)"
                            .to_string(),
                        Vec::new(),
                    ),
                };
                refused.push(GcRefusedOutput {
                    model_name: ev.model_name.clone(),
                    run_id: ev.run_id.clone(),
                    blake3_hash: ev.blake3_hash.clone(),
                    size_bytes: ev.size_bytes,
                    reason,
                    failed_checks,
                });
            }
        }
    }

    let bytes_evicted = evicted.iter().map(|e| e.size_bytes).sum();
    let bytes_refused = refused.iter().map(|r| r.size_bytes).sum();
    Ok(GcApplyOutput {
        version: VERSION.to_string(),
        command: "apply".to_string(),
        plan_id: plan_id.to_string(),
        evicted_count: evicted.len(),
        refused_count: refused.len(),
        evicted,
        refused,
        already_evicted,
        bytes_evicted,
        bytes_refused,
        notes: gc_apply_notes(),
    })
}

/// Resolve the models directory the policy evaluator compiles for gc gating.
///
/// Mirrors how the backfill apply threads `run_plan.models_dir` instead of a
/// hardcoded `models`: the base directory comes from the first transformation
/// pipeline's `models` glob (everything before the first wildcard), resolved
/// relative to the config file's parent — the same derivation
/// `crate::scope::resolve_transformation_managed_tables` uses. Falls back to
/// `models` when no config or no transformation pipeline is present.
///
/// Shared with the `rocky restore` apply seam, which gates policy the same way.
pub(crate) fn gc_models_dir(
    cfg: Option<&rocky_core::config::RockyConfig>,
    config_path: &Path,
) -> std::path::PathBuf {
    let project_root = config_path.parent().unwrap_or(Path::new(""));
    let glob = cfg.and_then(|c| {
        c.pipelines.values().find_map(|p| match p {
            rocky_core::config::PipelineConfig::Transformation(t) => Some(t.models.clone()),
            _ => None,
        })
    });
    let base = glob
        .as_deref()
        .and_then(|g| g.split(&['*', '?', '['][..]).next())
        .filter(|b| !b.is_empty())
        .unwrap_or("models");
    project_root.join(base.trim_end_matches('/'))
}

/// Apply a `PlanKind::Gc` plan — evict its derivable artifacts, each behind a
/// durable tombstone.
///
/// Deletion is symmetric-caution gated:
///
/// - It is **unconditionally** review-gated. A `rocky review <plan-id>
///   --approve` marker must exist regardless of principal or of whether a
///   `[policy]` block is configured — a human `gc` still goes through review,
///   never a direct delete. This mirrors the backfill hard rule.
/// - Policy may only make the gate *stricter*: an agent-scoped `deny gc {…}`
///   rule hard-refuses even a reviewed plan.
///
/// Once cleared, [`execute_gc_apply`] re-verifies each planned eviction against
/// the live ledger before deleting anything.
pub(crate) async fn run_gc_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
    json: bool,
) -> Result<()> {
    // The authoritative manifest-truth liveness oracle. Split out so tests can
    // exercise the review/policy gate + eviction wiring with a deterministic
    // oracle (creds-free the real one holds everything, which is correct but
    // untestable at this seam).
    run_gc_apply_in_with(
        root,
        config_path,
        plan_id,
        state_path,
        runtime_principal,
        json,
        &ManifestLivenessOracle,
    )
    .await
}

/// [`run_gc_apply_in`] with an injectable [`LivenessOracle`] — the real path
/// passes [`ManifestLivenessOracle`]; tests pass a deterministic oracle.
pub(crate) async fn run_gc_apply_in_with(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: rocky_core::config::PolicyPrincipal,
    json: bool,
    oracle: &dyn LivenessOracle,
) -> Result<()> {
    let plan_record =
        read_plan(root, plan_id).with_context(|| format!("failed to read gc plan '{plan_id}'"))?;

    if plan_record.kind != PlanKind::Gc {
        bail!(
            "plan '{plan_id}' is a {} plan, not a gc plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan_record.kind,
        );
    }

    let plan: GcPlan = serde_json::from_value(plan_record.payload.clone())
        .context("failed to deserialize gc plan payload")?;

    // HARD RULE: a gc plan is always review-gated, regardless of policy.
    if !ai_plan_is_reviewed(root, plan_id) {
        bail!(
            "gc plan '{plan_id}' has not been reviewed and approved. \
             Deletion is symmetric-caution gated — even a human gc goes through review, never a \
             direct delete. Review the {} artifact(s) it evicts and approve with \
             `rocky review {plan_id} --approve`, then re-run `rocky apply {plan_id}`.",
            plan.evictions.len(),
        );
    }

    // Policy can only tighten the gate: a `deny agent gc {…}` rule hard-refuses
    // even a reviewed plan. Any `require_review` is already satisfied by the
    // marker the always-on gate demands.
    //
    // Fail-closed pre-check: gc eviction runs entirely against the state store
    // and object store, so a config-load ERROR would otherwise silently
    // unenforce a possibly-configured `[policy]` block. Bail instead. A
    // genuinely-missing config file keeps the NotConfigured posture.
    let loaded_cfg = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => Some(cfg),
        Err(rocky_core::config::ConfigError::FileNotFound { .. }) => None,
        Err(e) => {
            return Err(anyhow::anyhow!(e).context(format!(
                "refusing to apply gc plan '{plan_id}': {} failed to load, so any configured \
                 [policy] rules cannot be enforced (fail-closed). Fix the config and re-run \
                 `rocky apply {plan_id}`.",
                config_path.display()
            )));
        }
    };
    let touched: BTreeMap<String, PolicyCapability> = plan
        .evictions
        .iter()
        .map(|e| (e.model_name.clone(), PolicyCapability::Gc))
        .collect();
    let models_dir = gc_models_dir(loaded_cfg.as_ref(), config_path);

    // SEAM-SCOPED SYNC (S1, #1089) — download half, BEFORE the policy gate.
    // Two ledgers matter here and both are among the tables `rocky run`
    // wholesale downloads-at-start / uploads-at-end when `[state]` is remote:
    //   1. POLICY_DECISIONS — the freeze/budget ledger `evaluate_apply_policy`
    //      reads. A freeze uploaded by another pod is invisible unless we pull
    //      the authoritative remote ledger FIRST (finding 4-GC); otherwise the
    //      gate would clear against a stale local snapshot and gc would proceed
    //      through an active cross-pod freeze.
    //   2. TOMBSTONES — the eviction ledger `execute_gc_apply` writes; left
    //      unsynced an eviction would be silently reverted by the next run's
    //      start-download.
    // So when the backend is remote we download the authoritative remote state
    // (overwriting the local file, preserving the local-only tables) BEFORE the
    // policy gate reads it and before we tombstone on top of it, then upload
    // AFTER the commit. A remote-backend `gc apply` therefore requires the
    // backend reachable; a download failure aborts before gating or evicting.
    //
    // KNOWN LIMITATION (concurrency): this closes only the *sequential*
    // between-runs clobber. The remote state object is a whole-file blob with no
    // compare-and-swap, so a concurrent writer (e.g. a `rocky run` whose
    // start-download preceded this apply) can still overwrite these tombstones on
    // its end-upload — a cross-pod lost update. Concurrency is NOT handled here.
    let state_cfg = loaded_cfg
        .as_ref()
        .map(|cfg| cfg.state.clone())
        .unwrap_or_default();
    let remote_state = !matches!(state_cfg.backend, StateBackend::Local);
    if remote_state {
        // WP-01 PR-B (2b): the session half-seam owns the download shape; a
        // successful download of either usable variant means the local ledger
        // now mirrors remote truth; failure still `?`-bails fail-closed
        // (unchanged).
        let _authority =
            rocky_core::state_sync::RemoteStateSession::download_only(&state_cfg, state_path)
                .await
                .with_context(|| {
                    "failed to download remote state before gc apply; a remote-backend gc apply \
                 requires the state backend to be reachable"
                })?;
    }

    // Durable freeze-marker LIST for the gate below, hoisted beside the ledger
    // download above (same guard as the governed apply seams, fail-closed) — a
    // marker-only freeze whose ledger row was erased by a concurrent state
    // upload must still deny this gc. An absent config has no `[policy]` to
    // enforce ⇒ empty set.
    let marker_freezes = match loaded_cfg.as_ref() {
        Some(cfg) => crate::commands::apply::marker_freezes_before_gate(cfg, &touched).await?,
        None => Vec::new(),
    };

    // Finding 1: gate on the SAME `loaded_cfg` snapshot used for the state
    // backend + models-dir above, rather than reloading the config inside
    // `evaluate_apply_policy` — a `rocky.toml` swap between the loads must not let
    // the state-backend/download and the policy gate disagree.
    let gate = evaluate_apply_policy_with_policy(
        loaded_cfg.as_ref().and_then(|c| c.policy.as_ref()),
        plan_id,
        plan_record.enforcement_principal(runtime_principal),
        &touched,
        &models_dir,
        state_path,
        &marker_freezes,
    );
    if let PolicyGate::Deny {
        model,
        rule_id,
        reason,
    } = gate
    {
        let rule = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
        bail!(
            "policy DENIES gc plan '{plan_id}': model '{model}'{rule} — {reason}. \
             A deny cannot be satisfied by review; re-scope the reclamation or have a \
             human apply it."
        );
    }

    // Physical byte-deletion is not implemented: reclaiming bytes safely needs
    // a protocol-aware VACUUM (Delta tombstone-retention windows + TOCTOU-safe
    // deletion against concurrent re-adds). `[gc] physical_delete = true` is a
    // hard error until then rather than a silent no-op, so an operator who
    // expects bytes to be deleted is told plainly they will not be.
    //
    // Only a genuinely ABSENT config disables it. Any other config error
    // (malformed TOML, an unknown key that trips `deny_unknown_fields` — e.g. a
    // typo alongside `physical_delete = true` — a missing env var, a validation
    // failure) is propagated (fail loud): a misconfigured `physical_delete`
    // must never silently degrade into "tombstone + retire" (finding 3).
    //
    // Finding 1: resolved from the SAME `loaded_cfg` snapshot loaded above — a
    // genuine (non-FileNotFound) config error already bailed there, so `None`
    // here is exactly the absent-config case → `physical_delete = false`. No
    // second `load_rocky_config` that a `rocky.toml` swap could point elsewhere.
    let physical_delete = loaded_cfg
        .as_ref()
        .map(|cfg| cfg.gc.physical_delete)
        .unwrap_or(false);
    if physical_delete {
        bail!(
            "`[gc] physical_delete = true` is not supported: physical reclamation of \
             content-addressed bytes requires a protocol-aware VACUUM (Delta tombstone-retention \
             windows + TOCTOU-safe deletion), which is not yet implemented. Unset `[gc] \
             physical_delete` — the durable tombstone + retired ledger row is the eviction of \
             record, and it retains the full recorded recipe (`rocky restore` rebuilds \
             single-input recipes from it; a multi-input recipe is recovered by re-running its \
             pipeline)."
        );
    }

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let output = execute_gc_apply(&store, oracle, plan_id, &plan, Utc::now()).await?;
    // Drop the store to release the advisory lock / flush the file before upload.
    drop(store);

    // SEAM-SCOPED SYNC — upload half, FAIL-CLOSED. Durability is the whole point
    // of the seam: an eviction that commits locally but never reaches the remote
    // would be silently reverted by the next run's start-download while this
    // command reported success. So the upload is forced to `Fail` regardless of
    // the configured `on_upload_failure` (default `skip`) — a failed upload
    // aborts (finding 5).
    if remote_state {
        // WP-01 PR-B (2b): the half-seam owns the forced-`Fail` durability
        // policy (previously a local `StateConfig` clone here).
        rocky_core::state_sync::RemoteStateSession::upload_only_fail_closed(
            &state_cfg, state_path, "gc apply",
        )
        .await
        .with_context(|| "failed to upload remote state after gc apply")?;
    }

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_apply_table(&output);
    }
    Ok(())
}

fn print_apply_table(output: &GcApplyOutput) {
    println!(
        "Rocky gc apply {} — {} evicted ({} bytes), {} refused, {} already evicted",
        output.plan_id.get(..12).unwrap_or(&output.plan_id),
        output.evicted_count,
        output.bytes_evicted,
        output.refused_count,
        output.already_evicted.len(),
    );
    println!();
    for e in &output.evicted {
        println!(
            "  EVICTED       {}  {} bytes  {}  (tombstoned + ledger row retired; bytes retained \
             for a future protocol-aware VACUUM)",
            e.model_name,
            e.size_bytes,
            e.blake3_hash.get(..12).unwrap_or(&e.blake3_hash),
        );
    }
    for r in &output.refused {
        println!(
            "  REFUSED       {}  {} bytes  {}  — {}",
            r.model_name,
            r.size_bytes,
            r.blake3_hash.get(..12).unwrap_or(&r.blake3_hash),
            r.reason,
        );
    }
    for h in &output.already_evicted {
        println!(
            "  already-gone  {}  (idempotent no-op)",
            h.get(..12).unwrap_or(h)
        );
    }
    println!();
    println!("notes:");
    for note in &output.notes {
        println!("  - {note}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Duration;
    use rocky_core::reuse::{OutputArtifact, UpstreamIdentity, build_records};
    use rocky_core::state::{RunRecord, RunStatus, RunTrigger};
    use rocky_ir::types::{RockyType, TypedColumn};
    use rocky_ir::{GovernanceConfig, MaterializationStrategy, ModelIr, TargetRef};
    use tempfile::TempDir;

    const HA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const HC: &str = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

    /// 🔴 FIX 9 regression: the gc apply seam threads the models directory
    /// from the loaded config into the policy evaluator instead of a hardcoded
    /// `models` — mirroring how the backfill seam threads `run_plan.models_dir`.
    /// A project whose transformation pipeline sets `models = "custom/**"` must
    /// resolve to `<project>/custom`, not `<project>/models` (which would
    /// compile the wrong directory and misread the models the `[policy]` scope
    /// matches on).
    #[test]
    fn gc_models_dir_reads_the_transformation_glob() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "transformation"
models = "custom_models/**"

[pipeline.p.target.governance]
auto_create_schemas = true
"#,
        )
        .unwrap();
        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let resolved = super::gc_models_dir(Some(&cfg), &config_path);
        assert_eq!(resolved, dir.path().join("custom_models"));
    }

    /// Absent a config (or a transformation pipeline) the gc models dir falls
    /// back to `<project>/models`.
    #[test]
    fn gc_models_dir_falls_back_to_models() {
        let dir = TempDir::new().unwrap();
        let config_path = dir.path().join("rocky.toml");
        assert_eq!(
            super::gc_models_dir(None, &config_path),
            dir.path().join("models")
        );
    }

    fn model(name: &str) -> ReplayCheckModelOutput {
        ReplayCheckModelOutput {
            model_name: name.to_string(),
            verdict: "replayable".to_string(),
            reasons: Vec::new(),
            has_provenance: true,
            ir_parseable: true,
            nondeterministic: false,
            proof_class: Some("strong".to_string()),
            inputs: Vec::new(),
        }
    }

    // -- individual checks ------------------------------------------------

    #[test]
    fn recipe_recorded_requires_strong_provenance() {
        let mut m = model("x");
        assert!(check_recipe_recorded(&m).passed);

        m.proof_class = Some("heuristic".to_string());
        let c = check_recipe_recorded(&m);
        assert!(!c.passed);
        assert!(c.detail.contains("weak"));

        m.has_provenance = false;
        m.proof_class = None;
        let c = check_recipe_recorded(&m);
        assert!(!c.passed);
        assert!(c.detail.contains("no provenance"));
    }

    #[test]
    fn replayable_fails_closed_on_nondeterminism() {
        let mut m = model("x");
        assert!(check_replayable(&m).passed);

        m.nondeterministic = true;
        let c = check_replayable(&m);
        assert!(!c.passed, "a nondeterministic model is never derivable");
        assert!(c.detail.contains("nondeterministic"));

        let mut m = model("x");
        m.verdict = "non_replayable".to_string();
        m.reasons = vec!["input absent".to_string()];
        let c = check_replayable(&m);
        assert!(!c.passed);
        assert!(c.detail.contains("input absent"));
    }

    #[test]
    fn unreferenced_only_at_refcount_one() {
        assert!(check_unreferenced(1).passed);
        assert!(!check_unreferenced(2).passed);
        assert!(
            !check_unreferenced(0).passed,
            "refcount 0 is an anomaly — fail closed"
        );
        assert!(check_unreferenced(3).detail.contains("shared"));
    }

    #[test]
    fn age_threshold_uses_written_age() {
        let now = Utc::now();
        let old = now - Duration::days(30);
        assert!(check_age_threshold(old, now, 7).passed);

        let fresh = now - Duration::days(1);
        let c = check_age_threshold(fresh, now, 7);
        assert!(!c.passed);
        assert!(c.detail.contains("written-age"));
    }

    // -- candidate assembly ----------------------------------------------

    fn artifact(
        hash: &str,
        model: &str,
        run: &str,
        size: u64,
        written: DateTime<Utc>,
    ) -> ArtifactRecord {
        ArtifactRecord {
            blake3_hash: hash.to_string(),
            run_id: run.to_string(),
            model_name: model.to_string(),
            file_path: format!("s3://b/{hash}.parquet"),
            commit_version: 0,
            size_bytes: size,
            written_at: written,
        }
    }

    /// A provenance record whose recorded output is exactly `art` — so the
    /// hash-binding check ([`check_recipe_produces_output`]) passes for it.
    fn prov_for(art: &ArtifactRecord) -> ProvenanceRecord {
        ProvenanceRecord {
            run_id: art.run_id.clone(),
            model_name: art.model_name.clone(),
            input_hash: "ih".to_string(),
            skip_hash: "sh".to_string(),
            model_ir_canonical_json: "{}".to_string(),
            upstreams: Vec::new(),
            output_blake3: vec![art.blake3_hash.clone()],
            output_path: vec![art.file_path.clone()],
            proof_class: "strong".to_string(),
            recorded_at: Utc::now(),
        }
    }

    #[test]
    fn candidate_derivable_when_all_checks_pass() {
        let now = Utc::now();
        let art = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        let prov = prov_for(&art);
        let c = build_candidate(&art, 1, &model("orders"), None, Some(&prov), now, 7, None);
        assert!(c.derivable);
        assert_eq!(c.checks.len(), 6);
        assert!(c.checks.iter().all(|k| k.passed));
    }

    #[test]
    fn candidate_not_derivable_when_shared() {
        let now = Utc::now();
        let art = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        let prov = prov_for(&art);
        let c = build_candidate(&art, 2, &model("orders"), None, Some(&prov), now, 7, None);
        assert!(!c.derivable);
        let unref = c.checks.iter().find(|k| k.check == "unreferenced").unwrap();
        assert!(!unref.passed);
    }

    #[test]
    fn candidate_not_derivable_when_recipe_produces_different_output() {
        // 🔴 facet 1: the (run, model) recipe provably produces H_GOOD, but the
        // artifact under consideration is H_BAD. H_BAD must NOT inherit the
        // recipe's derivability — evicting it would delete bytes the recipe
        // cannot rebuild. Fail-closed.
        let now = Utc::now();
        let bad = artifact(HB, "orders", "r1", 100, now - Duration::days(30));
        // Provenance records a DIFFERENT output (HA@its path), not HB.
        let good = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        let prov = prov_for(&good);
        let c = build_candidate(&bad, 1, &model("orders"), None, Some(&prov), now, 7, None);
        assert!(!c.derivable, "H_BAD must not inherit H_GOOD's derivability");
        let bind = c
            .checks
            .iter()
            .find(|k| k.check == "recipe_produces_output")
            .unwrap();
        assert!(!bind.passed);
        // The other five checks still pass — the report shows exactly why it's held.
        assert_eq!(c.checks.iter().filter(|k| k.passed).count(), 5);
    }

    #[test]
    fn hash_binding_holds_via_membership_when_provenance_records_no_paths() {
        // The empty-`output_path` fallback branch of `check_recipe_produces_output`:
        // some provenance carries output hashes but no aligned paths. The content
        // hash is the identity, so hash membership is both sufficient and required
        // — a member hash is derivable, a non-member hash is refused. Fail-closed.
        let now = Utc::now();
        let art = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        // Provenance records the output hash but NO paths (forces the fallback).
        let prov = ProvenanceRecord {
            output_path: Vec::new(),
            ..prov_for(&art)
        };

        // A member hash is derivable via membership alone.
        let c = build_candidate(&art, 1, &model("orders"), None, Some(&prov), now, 7, None);
        assert!(
            c.derivable,
            "a member hash must be derivable via membership"
        );
        assert!(
            c.checks
                .iter()
                .find(|k| k.check == "recipe_produces_output")
                .unwrap()
                .passed
        );

        // A different-hash artifact against the same no-path provenance is refused
        // — it must NOT inherit derivability just because paths were absent.
        let bad = artifact(HB, "orders", "r1", 100, now - Duration::days(30));
        let c_bad = build_candidate(&bad, 1, &model("orders"), None, Some(&prov), now, 7, None);
        assert!(
            !c_bad.derivable,
            "a non-member hash must be refused even when provenance records no paths"
        );
        assert!(
            !c_bad
                .checks
                .iter()
                .find(|k| k.check == "recipe_produces_output")
                .unwrap()
                .passed
        );
    }

    // -- end-to-end over a seeded ledger (real store, real join) ----------

    fn ca_ir(table: &str, sql: &str) -> ModelIr {
        let mut ir = ModelIr::transformation(
            TargetRef {
                catalog: "tgt".into(),
                schema: "raw".into(),
                table: table.into(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: format!("s3://b/tgt/raw/{table}"),
                partition_columns: vec![],
            },
            vec![],
            sql.to_string(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        ir.typed_columns = vec![TypedColumn {
            name: "id".into(),
            data_type: RockyType::Int64,
            nullable: false,
        }];
        ir
    }

    /// Seed provenance (via the production `build_records` path) + an artifact
    /// row — exactly what the content-addressed writer records in production.
    #[allow(clippy::too_many_arguments)]
    fn seed(
        store: &StateStore,
        run_id: &str,
        table: &str,
        sql: &str,
        upstreams: &[UpstreamIdentity],
        out_hash: &str,
        size: u64,
        written: DateTime<Utc>,
    ) {
        let ir = ca_ir(table, sql);
        let outputs = vec![OutputArtifact {
            blake3_hash: out_hash.to_string(),
            file_path: format!("s3://b/{out_hash}.parquet"),
        }];
        let (entry, prov) = build_records(&ir, run_id, upstreams, &outputs, written).unwrap();
        store
            .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
            .unwrap();
        store
            .record_artifact(&ArtifactRecord {
                blake3_hash: out_hash.to_string(),
                run_id: run_id.to_string(),
                model_name: table.to_string(),
                file_path: format!("s3://b/{out_hash}.parquet"),
                commit_version: 0,
                size_bytes: size,
                written_at: written,
            })
            .unwrap();
    }

    fn record_run(store: &StateStore, run_id: &str, model: &str) {
        let now = Utc::now();
        let exec = ModelExecution {
            model_name: model.to_string(),
            started_at: now,
            finished_at: now,
            duration_ms: 1234,
            rows_affected: Some(10),
            status: "success".to_string(),
            sql_hash: "sh".to_string(),
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned: Some(4096),
            bytes_written: None,
            tenant: None,
            recipe_hash: Some("recipe-abc".to_string()),
            input_hash: None,
            input_proof_class: None,
            env_hash: Some("env-abc".to_string()),
            hash_scheme: Some("v1".to_string()),
            output_column_hashes: None,
            attempts: Vec::new(),
        };
        store
            .record_run(&RunRecord {
                run_id: run_id.to_string(),
                started_at: now,
                finished_at: now,
                status: RunStatus::Success,
                models_executed: vec![exec],
                trigger: RunTrigger::Manual,
                config_hash: "cfg".to_string(),
                triggering_identity: None,
                session_source: rocky_core::state::SessionSource::Cli,
                git_commit: None,
                git_branch: None,
                idempotency_key: None,
                target_catalog: None,
                hostname: "gc-test".to_string(),
                rocky_version: "0.0.0-test".to_string(),
                check_outcomes: Vec::new(),
                pipeline: None,
                submission_id: None,
            })
            .unwrap();
    }

    /// Seed a content-addressed artifact whose `file_path` lives **under** its
    /// table's `storage_prefix` (`s3://b/tgt/raw/<table>/<hash>.parquet`) and
    /// whose recorded Delta `commit_version` is `commit_version` — the shape the
    /// hardened manifest oracle checks (prefix membership + own-add-at-version).
    /// Pairs with [`InMemoryLivenessOracle::seed_table`], whose bootstrap sits at
    /// v0 so the artifact's own `add` lands at v1+.
    #[allow(clippy::too_many_arguments)]
    fn seed_ca(
        store: &StateStore,
        run_id: &str,
        table: &str,
        out_hash: &str,
        size: u64,
        written: DateTime<Utc>,
        commit_version: u64,
    ) {
        let ir = ca_ir(table, "SELECT 1 AS id");
        let file_path = format!("s3://b/tgt/raw/{table}/{out_hash}.parquet");
        let outputs = vec![OutputArtifact {
            blake3_hash: out_hash.to_string(),
            file_path: file_path.clone(),
        }];
        let (entry, prov) = build_records(&ir, run_id, &[], &outputs, written).unwrap();
        store
            .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
            .unwrap();
        store
            .record_artifact(&ArtifactRecord {
                blake3_hash: out_hash.to_string(),
                run_id: run_id.to_string(),
                model_name: table.to_string(),
                file_path,
                commit_version,
                size_bytes: size,
                written_at: written,
            })
            .unwrap();
        record_run(store, run_id, table);
    }

    #[test]
    fn empty_ledger_reports_nothing() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let report = gather_report(&store, None, Utc::now(), 7).unwrap();
        assert_eq!(report.artifact_count, 0);
        assert_eq!(report.managed_bytes, 0);
        assert_eq!(report.derivable_count, 0);
        assert!(report.derivable_pct.is_none());
    }

    #[test]
    fn seeded_ledger_yields_derivable_candidate() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");

        let report = gather_report(&store, None, Utc::now(), 7).unwrap();
        assert_eq!(report.artifact_count, 1);
        assert_eq!(report.managed_bytes, 500);
        assert_eq!(report.derivable_count, 1);
        assert_eq!(report.derivable_bytes, 500);
        assert_eq!(report.derivable_pct, Some(100.0));

        let c = &report.candidates[0];
        assert!(c.derivable);
        assert_eq!(c.model_name, "orders");
        assert_eq!(c.refcount, 1);
        // Recipe id + rebuild-cost source come from the joined execution — not
        // hardcoded: they match what we recorded.
        assert_eq!(c.recipe_id.as_deref(), Some("recipe-abc"));
        assert_eq!(c.rebuild_cost.source_duration_ms, 1234);
        assert_eq!(c.rebuild_cost.source_bytes_scanned, Some(4096));
        assert_eq!(c.input_proof_class.as_deref(), Some("strong"));
    }

    #[test]
    fn fresh_artifact_fails_age_check() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let fresh = Utc::now() - Duration::hours(1);
        seed(
            &store,
            "r1",
            "orders",
            "SELECT 1 AS id",
            &[],
            HA,
            500,
            fresh,
        );

        let report = gather_report(&store, None, Utc::now(), 7).unwrap();
        assert_eq!(report.derivable_count, 0, "too recent to reclaim");
        let c = &report.candidates[0];
        let age = c
            .checks
            .iter()
            .find(|k| k.check == "age_threshold")
            .unwrap();
        assert!(!age.passed);
        // The other five still pass — the report shows exactly why it's held.
        assert_eq!(c.checks.iter().filter(|k| k.passed).count(), 5);
    }

    #[test]
    fn nondeterministic_recipe_never_derivable() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // `random()` is a volatile construct — flagged nondeterministic.
        seed(
            &store,
            "r1",
            "sample",
            "SELECT random() AS id",
            &[],
            HB,
            500,
            old,
        );

        let report = gather_report(&store, None, Utc::now(), 7).unwrap();
        assert_eq!(report.derivable_count, 0);
        let c = &report.candidates[0];
        let replay = c.checks.iter().find(|k| k.check == "replayable").unwrap();
        assert!(!replay.passed);
    }

    #[test]
    fn shared_bytes_not_derivable_and_managed_counts_hash_once() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // Two runs materialize the SAME output hash HC → refcount 2, shared.
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HC, 500, old);
        seed(&store, "r2", "orders", "SELECT 1 AS id", &[], HC, 500, old);

        let report = gather_report(&store, None, Utc::now(), 7).unwrap();
        // One distinct hash → one candidate; managed bytes count the hash once.
        assert_eq!(report.artifact_count, 1);
        assert_eq!(report.managed_bytes, 500);
        assert_eq!(report.derivable_count, 0);
        let c = &report.candidates[0];
        assert_eq!(c.refcount, 2);
        let unref = c.checks.iter().find(|k| k.check == "unreferenced").unwrap();
        assert!(!unref.passed);
    }

    // -- plan + apply (tombstone / retire / refuse) -----------------------

    fn plan_from_store(store: &StateStore, now: DateTime<Utc>, min_age_days: i64) -> GcPlan {
        let cands = gather_eviction_candidates(store, None, now, min_age_days).unwrap();
        build_gc_plan(&cands, min_age_days).expect("expected at least one derivable candidate")
    }

    /// A deterministic liveness oracle for the tombstone-path tests — returns a
    /// fixed verdict without reading any Delta log. `reclaimable()` lets a test
    /// reach the tombstone regardless of manifest state.
    struct FixedLivenessOracle {
        reclaimable: bool,
    }

    impl FixedLivenessOracle {
        fn reclaimable() -> Self {
            Self { reclaimable: true }
        }
    }

    #[async_trait]
    impl LivenessOracle for FixedLivenessOracle {
        async fn reclaim_verdict(&self, _sp: &str, _fp: &str, _cv: u64) -> ReclaimVerdict {
            if self.reclaimable {
                // A stable head across both the proof and the re-check call, so
                // the TOCTOU re-verify matches and the eviction proceeds.
                ReclaimVerdict::Reclaimable { head_version: 0 }
            } else {
                ReclaimVerdict::Held("test-held".to_string())
            }
        }
    }

    /// A liveness oracle that returns a scripted sequence of verdicts on
    /// successive `reclaim_verdict` calls — used to simulate a Delta head that
    /// advances (or a re-add) BETWEEN the removal proof and the pre-write
    /// re-check (finding 4). After the scripted verdicts are exhausted it
    /// repeats the last one.
    struct SequenceOracle {
        verdicts: std::sync::Mutex<std::collections::VecDeque<ReclaimVerdict>>,
        last: std::sync::Mutex<Option<u64>>,
    }

    impl SequenceOracle {
        fn new(verdicts: Vec<ReclaimVerdict>) -> Self {
            Self {
                verdicts: std::sync::Mutex::new(verdicts.into()),
                last: std::sync::Mutex::new(None),
            }
        }
    }

    #[async_trait]
    impl LivenessOracle for SequenceOracle {
        async fn reclaim_verdict(&self, _sp: &str, _fp: &str, _cv: u64) -> ReclaimVerdict {
            let mut q = self.verdicts.lock().unwrap();
            let v = q.pop_front();
            match v {
                Some(ReclaimVerdict::Reclaimable { head_version }) => {
                    *self.last.lock().unwrap() = Some(head_version);
                    ReclaimVerdict::Reclaimable { head_version }
                }
                Some(ReclaimVerdict::Held(r)) => ReclaimVerdict::Held(r),
                None => match *self.last.lock().unwrap() {
                    Some(hv) => ReclaimVerdict::Reclaimable { head_version: hv },
                    None => ReclaimVerdict::Held("exhausted".to_string()),
                },
            }
        }
    }

    /// A minimal-but-valid Delta bootstrap commit (v0): a commitInfo, a
    /// **supported** protocol (the real UniForm shape — reader v2 with NO reader
    /// features, writer v7 with allowlisted writer features), and a metaData. The
    /// target's `add`/`remove` commits follow at v1+.
    const BOOTSTRAP_V0: &str = concat!(
        r#"{"commitInfo":{}}"#,
        "\n",
        r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":7,"writerFeatures":["columnMapping","icebergCompatV2","invariants","appendOnly"]}}"#,
        "\n",
        r#"{"metaData":{"id":"t"}}"#,
    );

    /// A liveness oracle backed by **real** in-memory Delta logs, keyed by
    /// `storage_prefix`, that runs the SAME production decision path the real
    /// [`ManifestLivenessOracle`] runs — [`removal_proof_given_store`] +
    /// [`map_removal_proof`] — differing only in the injected object store. So
    /// membership, table-relative resolution, and the strict scan are all under
    /// test, not a re-implementation.
    struct InMemoryLivenessOracle {
        tables: HashMap<String, std::sync::Arc<object_store::memory::InMemory>>,
    }

    impl InMemoryLivenessOracle {
        fn new() -> Self {
            Self {
                tables: HashMap::new(),
            }
        }

        /// Seed the table at `storage_prefix` with a v0 bootstrap followed by
        /// `post_commits` at v1, v2, … (each a JSONL body). The log key prefix
        /// is derived from `storage_prefix` exactly as production does.
        async fn seed_table(&mut self, storage_prefix: &str, post_commits: &[&str]) {
            let mut versioned: Vec<(u64, &str)> = vec![(0, BOOTSTRAP_V0)];
            for (i, body) in post_commits.iter().enumerate() {
                versioned.push((i as u64 + 1, body));
            }
            self.seed_table_raw(storage_prefix, &versioned).await;
        }

        /// Seed the table at `storage_prefix` with commits at EXPLICIT versions
        /// — for anomalous histories (a version gap, a duplicate) the contiguous
        /// `seed_table` cannot express.
        async fn seed_table_raw(&mut self, storage_prefix: &str, commits: &[(u64, &str)]) {
            use object_store::path::Path as ObjPath;
            use object_store::{ObjectStoreExt, PutPayload};
            let key_prefix = storage_prefix
                .strip_prefix("s3://")
                .and_then(|r| r.split_once('/'))
                .map(|(_, k)| k)
                .expect("storage_prefix is s3://bucket/key");
            let store = std::sync::Arc::new(object_store::memory::InMemory::new());
            for (v, body) in commits {
                let path = ObjPath::from(format!("{key_prefix}/_delta_log/{v:020}.json"));
                store
                    .put(&path, PutPayload::from(format!("{body}\n").into_bytes()))
                    .await
                    .unwrap();
            }
            self.tables.insert(storage_prefix.to_string(), store);
        }
    }

    #[async_trait]
    impl LivenessOracle for InMemoryLivenessOracle {
        async fn reclaim_verdict(
            &self,
            storage_prefix: &str,
            file_path: &str,
            commit_version: u64,
        ) -> ReclaimVerdict {
            let Some(store) = self.tables.get(storage_prefix) else {
                return ReclaimVerdict::Held(format!("no seeded table log for {storage_prefix}"));
            };
            map_removal_proof(
                &super::super::run_content_addressed::removal_proof_given_store(
                    store.clone(),
                    storage_prefix,
                    file_path,
                    commit_version,
                )
                .await,
            )
        }
    }

    #[test]
    fn build_gc_plan_returns_none_on_empty_ledger() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let cands = gather_eviction_candidates(&store, None, Utc::now(), 7).unwrap();
        assert!(
            build_gc_plan(&cands, 7).is_none(),
            "refuse to write an empty plan"
        );
    }

    #[test]
    fn build_gc_plan_excludes_non_derivable() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // Derivable.
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        // Nondeterministic — never derivable, must be excluded from the plan.
        seed(
            &store,
            "r2",
            "sample",
            "SELECT random() AS id",
            &[],
            HB,
            400,
            old,
        );
        record_run(&store, "r2", "sample");

        let plan = plan_from_store(&store, Utc::now(), 7);
        assert_eq!(plan.evictions.len(), 1);
        assert_eq!(plan.evictions[0].blake3_hash, HA);
        assert_eq!(plan.total_bytes, 500);
    }

    #[tokio::test]
    async fn apply_evicts_still_derivable_artifact_with_tombstone() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);
        assert_eq!(plan.evictions.len(), 1);

        let out = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "plan-x",
            &plan,
            now,
        )
        .await
        .unwrap();

        assert_eq!(out.evicted_count, 1);
        assert_eq!(out.refused_count, 0);
        assert!(out.already_evicted.is_empty());
        assert_eq!(out.bytes_evicted, 500);
        // Ledger row retired.
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 0);
        // A durable tombstone with the full restore payload.
        let tombs = store.list_tombstones().unwrap();
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0].blake3_hash, HA);
        assert_eq!(tombs[0].run_id, "r1");
        assert_eq!(tombs[0].recipe_hash.as_deref(), Some("recipe-abc"));
        // No bytes are deleted — the tombstone + retired row is the eviction of
        // record; `physical_reclaimed` is false by construction.
        assert!(!tombs[0].physical_reclaimed);
        assert!(!out.evicted[0].physical_reclaimed);
        assert!(out.evicted[0].physical_status.contains("not attempted"));
    }

    /// 🔴 The core soundness surface: an artifact derivable at plan time whose
    /// hash gains a SECOND live reference before apply must be REFUSED — never
    /// evicted, never tombstoned — even though the plan approved it. This is the
    /// non-vacuous multi-ref refusal.
    #[tokio::test]
    async fn apply_refuses_eviction_when_a_second_reference_appears() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HC, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        // Plan captures the artifact while it is derivable (refcount 1).
        let plan = plan_from_store(&store, now, 7);
        assert_eq!(plan.evictions.len(), 1);

        // A second run materializes the SAME output hash → refcount 2 (shared).
        seed(&store, "r2", "orders", "SELECT 1 AS id", &[], HC, 500, old);
        assert_eq!(store.refcount_for_hash(HC).unwrap(), 2);

        let out = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "plan-x",
            &plan,
            now,
        )
        .await
        .unwrap();

        assert_eq!(
            out.evicted_count, 0,
            "multi-ref bytes must never be evicted"
        );
        assert_eq!(out.refused_count, 1);
        assert!(out.refused[0].reason.contains("no longer derivable"));
        assert!(
            out.refused[0]
                .failed_checks
                .iter()
                .any(|c| c.check == "unreferenced"),
            "the refusal cites the failed unreferenced check"
        );
        // No tombstone, both rows intact.
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HC).unwrap(), 2);
    }

    /// 🔴 facet 1, end-to-end over a seeded ledger: a `(run, model)` whose
    /// provenance records output `HA` also has a stray `OUTPUT_ARTIFACTS` row at
    /// a DIFFERENT hash `HB`. `HB` must be classified NOT derivable (its bytes
    /// aren't what the recipe rebuilds) and must never enter a plan, even though
    /// the recipe itself is strong + replayable.
    #[test]
    fn ledger_artifact_not_derivable_when_provenance_records_different_output() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // Provenance + artifact for the GOOD output HA.
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        // A stray artifact row at HB for the SAME (run, model) — provenance does
        // NOT record HB.
        store
            .record_artifact(&ArtifactRecord {
                blake3_hash: HB.to_string(),
                run_id: "r1".to_string(),
                model_name: "orders".to_string(),
                file_path: format!("s3://b/{HB}.parquet"),
                commit_version: 0,
                size_bytes: 400,
                written_at: old,
            })
            .unwrap();

        let cands = gather_eviction_candidates(&store, None, Utc::now(), 7).unwrap();
        let hb = cands
            .iter()
            .find(|c| c.artifact.blake3_hash == HB)
            .expect("HB candidate present");
        assert!(
            !hb.output.derivable,
            "HB must not inherit HA's derivability"
        );
        let bind = hb
            .output
            .checks
            .iter()
            .find(|k| k.check == "recipe_produces_output")
            .unwrap();
        assert!(!bind.passed);
        // HA remains derivable, and the plan contains only HA.
        let ha = cands
            .iter()
            .find(|c| c.artifact.blake3_hash == HA)
            .expect("HA candidate present");
        assert!(ha.output.derivable);
        let plan = build_gc_plan(&cands, 7).expect("HA is derivable");
        assert_eq!(plan.evictions.len(), 1);
        assert_eq!(plan.evictions[0].blake3_hash, HA);
    }

    /// 🔴 facets 2/3/4: a hand-authored plan claims a DERIVABLE hash (`HA`) but
    /// points its `file_path` at a DIFFERENT artifact's row (`HB`). Apply must
    /// refuse — verifying `HA`'s derivability while deleting `HB`'s row would be
    /// permanent data loss with a false tombstone. Nothing is deleted.
    #[tokio::test]
    async fn apply_refuses_a_crafted_plan_whose_path_points_at_other_bytes() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // HA — a genuinely derivable artifact.
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        // HB — a different real artifact at its own (run, model, path).
        seed(&store, "r2", "events", "SELECT 2 AS id", &[], HB, 400, old);
        record_run(&store, "r2", "events");

        // Craft a plan eviction that claims HA's (derivable) hash but points the
        // file_path + (run, model) at HB's row. `plan_id` integrity only proves
        // the payload is stable, not that it was engine-generated.
        let crafted = GcPlan {
            version: VERSION.to_string(),
            min_age_days: 7,
            total_bytes: 400,
            evictions: vec![GcPlanEviction {
                model_name: "events".to_string(),
                run_id: "r2".to_string(),
                blake3_hash: HA.to_string(),
                file_path: format!("s3://b/{HB}.parquet"),
                size_bytes: 400,
                commit_version: 0,
                written_at: old.to_rfc3339(),
                recipe_hash: None,
                input_hash: None,
                input_proof_class: None,
                env_hash: None,
                hash_scheme: None,
            }],
        };

        let out = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "crafted",
            &crafted,
            Utc::now(),
        )
        .await
        .unwrap();

        assert_eq!(out.evicted_count, 0, "a crafted plan must delete nothing");
        assert_eq!(out.refused_count, 1);
        assert!(
            out.refused[0]
                .reason
                .contains("does not match the live artifact"),
            "got: {}",
            out.refused[0].reason
        );
        // Both rows intact, no tombstone.
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
        assert_eq!(store.refcount_for_hash(HB).unwrap(), 1);
    }

    #[tokio::test]
    async fn apply_is_idempotent_on_re_run() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let first = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "plan-x",
            &plan,
            now,
        )
        .await
        .unwrap();
        assert_eq!(first.evicted_count, 1);

        // Re-applying the same plan is a clean no-op — the row is already gone.
        let second = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "plan-x",
            &plan,
            now,
        )
        .await
        .unwrap();
        assert_eq!(second.evicted_count, 0);
        assert_eq!(second.refused_count, 0);
        assert_eq!(second.already_evicted, vec![HA.to_string()]);
        // No duplicate tombstone.
        assert_eq!(store.list_tombstones().unwrap().len(), 1);
    }

    /// The eviction of record is the tombstone + retired ledger row — NO bytes
    /// are ever deleted. `physical_reclaimed` is false and no physical status
    /// other than "not attempted" is ever produced (physical reclamation is
    /// future protocol-aware VACUUM work).
    #[tokio::test]
    async fn apply_never_deletes_bytes_only_tombstones_and_retires() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let out = execute_gc_apply(
            &store,
            &FixedLivenessOracle::reclaimable(),
            "plan-x",
            &plan,
            now,
        )
        .await
        .unwrap();

        assert_eq!(out.evicted_count, 1);
        assert!(!out.evicted[0].physical_reclaimed);
        assert!(out.evicted[0].physical_status.contains("not attempted"));
        // The tombstone + retirement are the eviction of record.
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 0);
        let tombs = store.list_tombstones().unwrap();
        assert_eq!(tombs.len(), 1);
        assert!(!tombs[0].physical_reclaimed);
    }

    /// The apply entrypoint is unconditionally review-gated — no marker, no
    /// eviction — regardless of principal or of a `[policy]` block. With a
    /// marker present the eviction proceeds.
    #[tokio::test]
    async fn gc_apply_requires_a_review_marker() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        let plan_id = {
            let store = StateStore::open(&state_path).unwrap();
            let old = Utc::now() - Duration::days(30);
            seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
            record_run(&store, "r1", "orders");
            let plan = plan_from_store(&store, Utc::now(), 7);
            write_plan_with_principal(dir.path(), PlanKind::Gc, &plan, PolicyPrincipal::Human)
                .unwrap()
        }; // drop the store so the apply path can open its own write handle

        // A missing config leaves the policy plane unconfigured — the gate is
        // the unconditional review marker, not policy.
        let config = dir.path().join("nonexistent.toml");

        // Drive the injectable variant with a Reclaimable oracle so the
        // review/policy gate + eviction wiring are exercised without a live
        // Delta log (the real `ManifestLivenessOracle` would hold everything
        // creds-free — correct, but it would mask the review-gate assertion).
        let oracle = FixedLivenessOracle::reclaimable();

        // No marker → refuse.
        let err = run_gc_apply_in_with(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
            &oracle,
        )
        .await
        .expect_err("apply must refuse an unreviewed gc plan");
        assert!(err.to_string().contains("not been reviewed"), "got: {err}");
        {
            let store = StateStore::open(&state_path).unwrap();
            assert!(
                store.list_tombstones().unwrap().is_empty(),
                "nothing evicted while unreviewed"
            );
        }

        // Write the review marker (as `rocky review --approve` would) → proceed.
        let marker = crate::commands::apply::review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "{}").unwrap();
        run_gc_apply_in_with(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
            &oracle,
        )
        .await
        .unwrap();

        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(store.list_tombstones().unwrap().len(), 1);
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 0);
    }

    /// S1 (#1089): with a REMOTE `[state]` backend, `gc apply` brackets the
    /// TOMBSTONES write with a seam-scoped sync whose **download-before-open**
    /// half runs first. A deliberately-misconfigured remote backend (`s3`, no
    /// bucket) makes that download fail fast with `MissingConfig`, aborting the
    /// apply BEFORE any eviction. Proves the download-before half is wired and
    /// fatal (and that no tombstone leaks past a failed sync); without it, the
    /// eviction would proceed and be reverted by the next run's start-download.
    #[tokio::test]
    async fn gc_apply_downloads_remote_state_before_evicting() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        let plan_id = {
            let store = StateStore::open(&state_path).unwrap();
            let old = Utc::now() - Duration::days(30);
            seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
            record_run(&store, "r1", "orders");
            let plan = plan_from_store(&store, Utc::now(), 7);
            write_plan_with_principal(dir.path(), PlanKind::Gc, &plan, PolicyPrincipal::Human)
                .unwrap()
        };

        // A rocky.toml with a REMOTE [state] backend, deliberately missing its
        // bucket so the download-before step fails fast with MissingConfig.
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "[state]\nbackend = \"s3\"\n").unwrap();

        // Satisfy the unconditional review gate so we reach the sync seam.
        let marker = crate::commands::apply::review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "{}").unwrap();

        let oracle = FixedLivenessOracle::reclaimable();
        let err = run_gc_apply_in_with(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
            &oracle,
        )
        .await
        .expect_err("a remote-backend gc apply must abort when the state backend is unreachable");
        assert!(
            err.to_string().contains("download remote state"),
            "download-before-open must be wired and fatal: {err}"
        );

        // Nothing evicted — the download-before aborted before the store write.
        let store = StateStore::open(&state_path).unwrap();
        assert!(
            store.list_tombstones().unwrap().is_empty(),
            "no tombstone should be written when the download-before-open aborts"
        );
    }

    /// 🔴 DEFECT 1 (append-only data loss). The UniForm/Delta writer is
    /// append-only — it emits `add` actions, never `remove` — so two builds of
    /// a table leave BOTH files referenced by the live snapshot. A ledger/hash
    /// heuristic that calls the older file "superseded" would let gc delete
    /// bytes the live table still points at. The manifest-truth gate reads the
    /// Delta log and HOLDs every still-`add`ed file; only an externally-`remove`d
    /// file is reclaimable. Non-vacuous: both files are ledger-derivable, and
    /// pre-gate the plan would have evicted the "older" live file.
    #[tokio::test]
    async fn append_only_live_files_are_held_only_removed_is_reclaimable() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        // Two builds of the SAME table `orders`: older file HA (add at v1),
        // newer file HB (add at v2). Both old, refcount 1, replayable,
        // strong-provenance ⇒ derivable. The append-only log removes neither.
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        seed_ca(&store, "r2", "orders", HB, 600, old, 2);
        let now = Utc::now();
        let cands = gather_eviction_candidates(&store, None, now, 7).unwrap();
        assert!(
            cands.iter().all(|c| c.output.derivable),
            "both are ledger-derivable (the pre-gate state that made the bug reachable)"
        );
        let plan = build_gc_plan(&cands, 7).expect("both derivable");
        assert_eq!(plan.evictions.len(), 2);

        // Manifest reality: v0 bootstrap, v1 add(HA), v2 add(HB), no remove.
        let mut oracle = InMemoryLivenessOracle::new();
        oracle
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"add":{{"path":"{HB}.parquet"}}}}"#),
                ],
            )
            .await;

        let out = execute_gc_apply(&store, &oracle, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "append-only live files (incl. the older version) must never be evicted"
        );
        assert_eq!(out.refused_count, 2);
        assert!(
            out.refused
                .iter()
                .all(|r| r.reason.contains("liveness gate")),
            "each refusal cites the Delta-log liveness gate"
        );
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
        assert_eq!(store.refcount_for_hash(HB).unwrap(), 1);

        // Non-vacuity: once an external compaction `remove`s HA (v3), HA is
        // reclaimable while the still-`add`ed HB stays held.
        let mut oracle2 = InMemoryLivenessOracle::new();
        oracle2
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"add":{{"path":"{HB}.parquet"}}}}"#),
                    &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                ],
            )
            .await;
        let out2 = execute_gc_apply(&store, &oracle2, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out2.evicted_count, 1,
            "a provably-removed file is reclaimable"
        );
        assert_eq!(out2.evicted[0].blake3_hash, HA);
        assert_eq!(
            out2.refused_count, 1,
            "the still-live newer file stays held"
        );
        assert_eq!(out2.refused[0].blake3_hash, HB);
    }

    /// Seed a content-addressed `orders` model in a specific `catalog` (its own
    /// target/table + storage_prefix), so two same-named models can coexist in
    /// the ledger on DIFFERENT tables — the DEFECT 2 collision setup. Its file
    /// lives under its own storage_prefix and its `add` is at v1.
    fn seed_orders_in(
        store: &StateStore,
        run_id: &str,
        catalog: &str,
        out_hash: &str,
        written: DateTime<Utc>,
    ) {
        let mut ir = ModelIr::transformation(
            TargetRef {
                catalog: catalog.into(),
                schema: "raw".into(),
                table: "orders".into(),
            },
            MaterializationStrategy::ContentAddressed {
                storage_prefix: format!("s3://b/{catalog}/raw/orders"),
                partition_columns: vec![],
            },
            vec![],
            "SELECT 1 AS id".to_string(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        ir.typed_columns = vec![TypedColumn {
            name: "id".into(),
            data_type: RockyType::Int64,
            nullable: false,
        }];
        let file_path = format!("s3://b/{catalog}/raw/orders/{out_hash}.parquet");
        let outputs = vec![OutputArtifact {
            blake3_hash: out_hash.to_string(),
            file_path: file_path.clone(),
        }];
        let (entry, prov) = build_records(&ir, run_id, &[], &outputs, written).unwrap();
        store
            .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
            .unwrap();
        store
            .record_artifact(&ArtifactRecord {
                blake3_hash: out_hash.to_string(),
                run_id: run_id.to_string(),
                model_name: "orders".to_string(),
                file_path,
                commit_version: 1,
                size_bytes: 500,
                written_at: written,
            })
            .unwrap();
        record_run(store, run_id, "orders");
    }

    /// 🔴 DEFECT 2 (bare-name collision). Two DISTINCT models both named
    /// `orders` in different catalogs must each be judged against their OWN
    /// table's Delta log — never cross-contaminated by the same-named sibling.
    /// The manifest gate is scoped by the candidate's own provenance
    /// storage_prefix, so model A's live head is held while model B's removed
    /// file is reclaimed, proving no collision.
    #[tokio::test]
    async fn same_named_models_are_scoped_per_table_no_collision() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_orders_in(&store, "rA", "ca", HA, old); // table ca.raw.orders, file HA
        seed_orders_in(&store, "rB", "cb", HB, old); // table cb.raw.orders, file HB
        let now = Utc::now();
        let cands = gather_eviction_candidates(&store, None, now, 7).unwrap();
        assert_eq!(cands.len(), 2);
        // Each candidate resolved its own target's storage_prefix.
        let a = cands.iter().find(|c| c.artifact.blake3_hash == HA).unwrap();
        let b = cands.iter().find(|c| c.artifact.blake3_hash == HB).unwrap();
        assert_eq!(a.storage_prefix.as_deref(), Some("s3://b/ca/raw/orders"));
        assert_eq!(b.storage_prefix.as_deref(), Some("s3://b/cb/raw/orders"));
        let plan = build_gc_plan(&cands, 7).expect("both derivable");
        assert_eq!(plan.evictions.len(), 2);

        // A's file is LIVE in table A (add v1); B's file was REMOVED in table B
        // (add v1, remove v2).
        let mut oracle = InMemoryLivenessOracle::new();
        oracle
            .seed_table(
                "s3://b/ca/raw/orders",
                &[&format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#)],
            )
            .await;
        oracle
            .seed_table(
                "s3://b/cb/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HB}.parquet"}}}}"#),
                    &format!(r#"{{"remove":{{"path":"{HB}.parquet"}}}}"#),
                ],
            )
            .await;

        let out = execute_gc_apply(&store, &oracle, "plan-x", &plan, now)
            .await
            .unwrap();
        // Each judged by its OWN table: A held (live), B reclaimed (removed).
        assert_eq!(out.evicted_count, 1);
        assert_eq!(out.evicted[0].blake3_hash, HB);
        assert_eq!(out.refused_count, 1);
        assert_eq!(out.refused[0].blake3_hash, HA);
        assert!(out.refused[0].reason.contains("liveness gate"));
    }

    /// 🔴 FINDING 1 (absolute-URI re-add alias). Delta permits an `add`/`remove`
    /// path to be relative (`H.parquet`) OR an absolute URI
    /// (`s3://b/<prefix>/H.parquet`). A raw string compare misses an
    /// absolute-URI re-add of a file a prior relative `remove` retired, so the
    /// proof would wrongly conclude `Removed`. Canonicalizing both forms catches
    /// it → HELD. Non-vacuous: the same log WITHOUT the aliased re-add reclaims.
    #[tokio::test]
    async fn absolute_uri_realias_re_add_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        // v1 add(rel), v2 remove(rel), v3 add(ABSOLUTE URI alias) → file is live.
        let mut held = InMemoryLivenessOracle::new();
        held.seed_table(
            "s3://b/tgt/raw/orders",
            &[
                &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                &format!(r#"{{"add":{{"path":"s3://b/tgt/raw/orders/{HA}.parquet"}}}}"#),
            ],
        )
        .await;
        let out = execute_gc_apply(&store, &held, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "an absolute-URI re-add aliasing the file must HOLD"
        );
        assert_eq!(out.refused_count, 1);
        assert!(store.list_tombstones().unwrap().is_empty());

        // Control: without the v3 re-add the file is genuinely removed → reclaims.
        let mut removed = InMemoryLivenessOracle::new();
        removed
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                ],
            )
            .await;
        let out2 = execute_gc_apply(&store, &removed, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out2.evicted_count, 1,
            "a genuinely removed file reclaims (non-vacuity control)"
        );
    }

    /// 🔴 FINDING 1 (membership). A candidate is judged only by ITS OWN table's
    /// log. A foreign same-named table that removed a same-basename file is
    /// ignored — the candidate's own (live) table holds it — and a `file_path`
    /// outside its `storage_prefix` never resolves to a table-relative path.
    #[tokio::test]
    async fn candidate_judged_by_its_own_table_not_a_foreign_one() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let mut oracle = InMemoryLivenessOracle::new();
        // The candidate's OWN table: HA still live (add v1, no remove).
        oracle
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[&format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#)],
            )
            .await;
        // A FOREIGN table that removed a same-basename file — must be ignored.
        oracle
            .seed_table(
                "s3://b/other/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                ],
            )
            .await;

        let out = execute_gc_apply(&store, &oracle, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "judged by its own (live) table, not the foreign removed one"
        );
        assert_eq!(out.refused_count, 1);
        assert!(store.list_tombstones().unwrap().is_empty());

        // A file_path outside the storage_prefix resolves to a hold, never a
        // table-relative path (the membership boundary).
        assert!(matches!(
            super::super::run_content_addressed::table_relative_add_path(
                "s3://b/tgt/raw/orders",
                "s3://b/OTHER/raw/orders/deadbeef.parquet",
            ),
            Err(rocky_iceberg::uniform_writer::RemovalHoldReason::PrefixMismatch)
        ));
    }

    /// 🔴 FINDING 1 (leading-slash absolute re-add). A bucket-root-absolute path
    /// (`/tgt/raw/orders/H.parquet`) re-adding the file after a relative `remove`
    /// makes it live — the proof must canonicalize it (not prefix-join it) and
    /// HOLD. Non-vacuous: without the re-add the file reclaims.
    #[tokio::test]
    async fn leading_slash_absolute_re_add_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        // v1 add(rel), v2 remove(rel), v3 add(LEADING-SLASH absolute) → live.
        let mut held = InMemoryLivenessOracle::new();
        held.seed_table(
            "s3://b/tgt/raw/orders",
            &[
                &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                &format!(r#"{{"add":{{"path":"/tgt/raw/orders/{HA}.parquet"}}}}"#),
            ],
        )
        .await;
        let out = execute_gc_apply(&store, &held, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "a leading-slash absolute re-add aliasing the file must HOLD"
        );
        assert_eq!(out.refused_count, 1);
    }

    /// 🔴 FINDING 3 (same-version add+remove). A single commit that both `add`s
    /// and `remove`s the target is a forbidden/malformed reconciliation — the
    /// decision must not depend on JSONL line order → HOLD the malformed version.
    /// Non-vacuous: an add at v1 and a remove at a SEPARATE v2 reclaims.
    #[tokio::test]
    async fn same_version_add_and_remove_of_target_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        // v1 add(HA), v2 = add(HA)+remove(HA) in ONE commit → malformed. The
        // `remove` is the LAST line so that, absent the per-version guard, the
        // prior last-line-wins heuristic would take the `remove` as the highest
        // reference and WRONGLY evict — the guard must HOLD regardless of order.
        let same_version = format!(
            "{{\"add\":{{\"path\":\"{HA}.parquet\"}}}}\n{{\"remove\":{{\"path\":\"{HA}.parquet\"}}}}"
        );
        let mut malformed = InMemoryLivenessOracle::new();
        malformed
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &same_version,
                ],
            )
            .await;
        let out = execute_gc_apply(&store, &malformed, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "an add+remove of the target in one commit must hold (malformed)"
        );
        assert_eq!(out.refused_count, 1);

        // Non-vacuity: add at v1, remove at a SEPARATE v2 → reclaims.
        let mut ok = InMemoryLivenessOracle::new();
        ok.seed_table(
            "s3://b/tgt/raw/orders",
            &[
                &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
            ],
        )
        .await;
        let out2 = execute_gc_apply(&store, &ok, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out2.evicted_count, 1,
            "add at v1 + remove at a separate v2 reclaims (control)"
        );
    }

    /// 🔴 ROUND-6 FIX 1 (network-path re-add). A `//authority/path` re-add of the
    /// file after a relative `remove` names the same object, but the reader does
    /// not canonicalize that form with confidence → HOLD (never a wrong evict).
    #[tokio::test]
    async fn network_path_re_add_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        // v1 add(rel), v2 remove(rel), v3 add(`//b/...` network-path) → held: the
        // `//` form is not canonicalized, so the proof cannot rule out a re-add.
        let mut oracle = InMemoryLivenessOracle::new();
        oracle
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#),
                    &format!(r#"{{"add":{{"path":"//b/tgt/raw/orders/{HA}.parquet"}}}}"#),
                ],
            )
            .await;
        let out = execute_gc_apply(&store, &oracle, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "a `//authority` network-path add must hold (uncanonicalizable)"
        );
        assert_eq!(out.refused_count, 1);
    }

    /// 🔴 ROUND-6 FIX 3 (missing v0 protocol). A history whose v0 carries no
    /// `protocol` action bypasses the protocol whitelist entirely — it must HOLD.
    /// Non-vacuity: v0 WITH a supported protocol + metadata reclaims (the same
    /// commits otherwise).
    #[tokio::test]
    async fn missing_v0_protocol_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let add = format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#);
        let remove = format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#);

        // v0 = commitInfo + metaData only (NO protocol), v1 add, v2 remove.
        let mut no_protocol = InMemoryLivenessOracle::new();
        no_protocol
            .seed_table_raw(
                "s3://b/tgt/raw/orders",
                &[
                    (0, "{\"commitInfo\":{}}\n{\"metaData\":{\"id\":\"t\"}}"),
                    (1, &add),
                    (2, &remove),
                ],
            )
            .await;
        let out = execute_gc_apply(&store, &no_protocol, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "a history with no v0 protocol must hold"
        );
        assert_eq!(out.refused_count, 1);

        // Non-vacuity: v0 WITH a supported protocol + metadata (the standard
        // bootstrap) reclaims.
        let mut ok = InMemoryLivenessOracle::new();
        ok.seed_table("s3://b/tgt/raw/orders", &[&add, &remove])
            .await;
        let out2 = execute_gc_apply(&store, &ok, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out2.evicted_count, 1,
            "a valid v0 bootstrap reclaims (control)"
        );
    }

    /// 🔴 FINDING 2 (empty / multi-key commit). A `{}` or a multi-key line must
    /// NOT pass as a benign commit — otherwise it leaves a stale earlier
    /// `remove` as the highest reference and the proof would authorize eviction.
    #[tokio::test]
    async fn empty_or_multi_key_commit_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let add = format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#);
        let remove = format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#);

        // v1 add(HA), v2 remove(HA), v3 = `{}` (empty object).
        let mut empty = InMemoryLivenessOracle::new();
        empty
            .seed_table("s3://b/tgt/raw/orders", &[&add, &remove, "{}"])
            .await;
        let out = execute_gc_apply(&store, &empty, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(out.evicted_count, 0, "an empty `{{}}` commit must hold");
        assert_eq!(out.refused_count, 1);

        // v3 = a multi-key line also holds.
        let mut multi = InMemoryLivenessOracle::new();
        multi
            .seed_table(
                "s3://b/tgt/raw/orders",
                &[
                    &add,
                    &remove,
                    r#"{"add":{"path":"x.parquet"},"remove":{"path":"y.parquet"}}"#,
                ],
            )
            .await;
        let out2 = execute_gc_apply(&store, &multi, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(out2.evicted_count, 0, "a multi-key commit line must hold");
    }

    /// 🔴 FINDING 2 (invalid protocol). An empty/unsupported/non-array `protocol`
    /// action anywhere in the history must HOLD — a deletion-vector or unknown
    /// future protocol must not slip through.
    #[tokio::test]
    async fn invalid_protocol_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let add = format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#);
        let remove = format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#);

        for bad_protocol in [
            r#"{"protocol":{}}"#, // missing versions
            r#"{"protocol":{"minReaderVersion":99,"minWriterVersion":7,"writerFeatures":["appendOnly"]}}"#, // unsupported version
            r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":7,"writerFeatures":"deletionVectors"}}"#, // non-array
            r#"{"protocol":{"minReaderVersion":2,"minWriterVersion":7,"writerFeatures":["deletionVectors"]}}"#, // DV feature
        ] {
            let mut oracle = InMemoryLivenessOracle::new();
            oracle
                .seed_table("s3://b/tgt/raw/orders", &[&add, &remove, bad_protocol])
                .await;
            let out = execute_gc_apply(&store, &oracle, "plan-x", &plan, now)
                .await
                .unwrap();
            assert_eq!(
                out.evicted_count, 0,
                "an invalid protocol must hold: {bad_protocol}"
            );
        }
    }

    /// 🔴 FINDING 2 (real version gap). A non-contiguous `_delta_log` (a MISSING
    /// version) must HOLD — the readable tail is not the whole history.
    /// Non-vacuous: the same commits at contiguous versions reclaim.
    #[tokio::test]
    async fn version_gap_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed_ca(&store, "r1", "orders", HA, 500, old, 1);
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let add = format!(r#"{{"add":{{"path":"{HA}.parquet"}}}}"#);
        let remove = format!(r#"{{"remove":{{"path":"{HA}.parquet"}}}}"#);

        // v0, v1, v3 — v2 is MISSING → gap.
        let mut gapped = InMemoryLivenessOracle::new();
        gapped
            .seed_table_raw(
                "s3://b/tgt/raw/orders",
                &[(0, BOOTSTRAP_V0), (1, &add), (3, &remove)],
            )
            .await;
        let out = execute_gc_apply(&store, &gapped, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(out.evicted_count, 0, "a version gap must hold");
        assert_eq!(out.refused_count, 1);

        // Control: contiguous 0,1,2 with the remove at v2 → reclaims.
        let mut contiguous = InMemoryLivenessOracle::new();
        contiguous
            .seed_table_raw(
                "s3://b/tgt/raw/orders",
                &[(0, BOOTSTRAP_V0), (1, &add), (2, &remove)],
            )
            .await;
        let out2 = execute_gc_apply(&store, &contiguous, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out2.evicted_count, 1,
            "contiguous history reclaims (control)"
        );
    }

    /// 🔴 FINDING 4 (TOCTOU). If the table's Delta head advances between the
    /// removal proof and the ledger retire (an external re-add), the pre-write
    /// re-check must HOLD. Non-vacuous: a stable head reclaims and the tombstone
    /// is version-scoped to the proven head.
    #[tokio::test]
    async fn head_advance_between_proof_and_retire_holds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        // Proof sees head 3; the pre-write re-check sees head 4 (advanced).
        let advancing = SequenceOracle::new(vec![
            ReclaimVerdict::Reclaimable { head_version: 3 },
            ReclaimVerdict::Reclaimable { head_version: 4 },
        ]);
        let out = execute_gc_apply(&store, &advancing, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(
            out.evicted_count, 0,
            "a head advance between proof and retire must HOLD"
        );
        assert_eq!(out.refused_count, 1);
        assert!(out.refused[0].reason.contains("head advanced"));
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);

        // Control: a stable head across both reads reclaims + version-scopes.
        let stable = SequenceOracle::new(vec![
            ReclaimVerdict::Reclaimable { head_version: 5 },
            ReclaimVerdict::Reclaimable { head_version: 5 },
        ]);
        let out2 = execute_gc_apply(&store, &stable, "plan-x2", &plan, now)
            .await
            .unwrap();
        assert_eq!(out2.evicted_count, 1, "a stable head reclaims (control)");
        let tombs = store.list_tombstones().unwrap();
        assert_eq!(tombs.len(), 1);
        assert_eq!(
            tombs[0].observed_delta_version,
            Some(5),
            "the tombstone is version-scoped to the proven Delta head"
        );
    }

    /// 🔴 FINDINGS 3+4: physical byte-deletion is not implemented. `[gc]
    /// physical_delete = true` is a **hard error** at apply time (fail loud) —
    /// never a silent delete or silent no-op — and nothing is retired.
    #[tokio::test]
    async fn physical_delete_true_is_a_hard_error_and_deletes_nothing() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        let plan_id = {
            let store = StateStore::open(&state_path).unwrap();
            let old = Utc::now() - Duration::days(30);
            seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
            record_run(&store, "r1", "orders");
            let plan = plan_from_store(&store, Utc::now(), 7);
            write_plan_with_principal(dir.path(), PlanKind::Gc, &plan, PolicyPrincipal::Human)
                .unwrap()
        };
        // Approve the plan so the review gate passes and we reach the config
        // check (proving the error is the physical_delete gate, not review).
        let marker = crate::commands::apply::review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "{}").unwrap();

        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "[gc]\nphysical_delete = true\n").unwrap();

        let err = run_gc_apply_in(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
        )
        .await
        .expect_err("physical_delete = true must be a hard error");
        assert!(
            err.to_string().contains("not supported")
                && err.to_string().contains("physical_delete"),
            "got: {err}"
        );
        let store = StateStore::open(&state_path).unwrap();
        assert!(
            store.list_tombstones().unwrap().is_empty(),
            "nothing is retired when physical_delete errors"
        );
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
    }

    /// 🔴 FINDING 3 (config error must not silently disable). A malformed `[gc]`
    /// config — here an unknown key that trips `deny_unknown_fields` alongside
    /// `physical_delete = true` — must HARD-ERROR at apply, NOT be swallowed
    /// into `physical_delete = false` (which would silently tombstone + retire).
    #[tokio::test]
    async fn malformed_gc_config_hard_errors_not_silently_disabled() {
        let dir = TempDir::new().unwrap();
        let state_path = dir.path().join("state.redb");
        let plan_id = {
            let store = StateStore::open(&state_path).unwrap();
            let old = Utc::now() - Duration::days(30);
            seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
            record_run(&store, "r1", "orders");
            let plan = plan_from_store(&store, Utc::now(), 7);
            write_plan_with_principal(dir.path(), PlanKind::Gc, &plan, PolicyPrincipal::Human)
                .unwrap()
        };
        let marker = crate::commands::apply::review_marker_path(dir.path(), &plan_id);
        std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
        std::fs::write(&marker, "{}").unwrap();

        // An unknown key under `[gc]` → deny_unknown_fields deserialization
        // error. Pre-fix, `unwrap_or(false)` swallowed this and continued as if
        // physical delete were disabled.
        let config = dir.path().join("rocky.toml");
        std::fs::write(&config, "[gc]\nphysical_delete = true\nunexpected = 1\n").unwrap();

        let err = run_gc_apply_in(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
        )
        .await
        .expect_err("a malformed [gc] config must hard-error, not silently disable");
        // The hard error may surface from either config-load seam — the
        // policy pre-check ("… failed to load, so any configured [policy]
        // rules cannot be enforced") runs before the `[gc] physical_delete`
        // resolution — and both satisfy the invariant under test: a
        // malformed config never silently degrades into "tombstone+retire".
        assert!(
            err.to_string().contains("physical_delete")
                || err.to_string().contains("failed to load"),
            "got: {err}"
        );
        let store = StateStore::open(&state_path).unwrap();
        assert!(
            store.list_tombstones().unwrap().is_empty(),
            "nothing is retired when the config is malformed"
        );
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
    }

    /// FIX: a gc plan must enter the decision-driven review queue at creation
    /// (plan creation records its `require_review` escalation) and clear on
    /// the same approve path the governor's MCP `review_queue` tool calls —
    /// end to end: plan → queue lists it → approve → queue empties → apply
    /// proceeds past the review gate.
    #[tokio::test]
    async fn gc_plan_enters_review_queue_and_clears_on_approve() {
        let dir = TempDir::new().unwrap();
        let root = dir.path();
        let state_path = root.join("state.redb");
        {
            let store = StateStore::open(&state_path).unwrap();
            let old = Utc::now() - Duration::days(30);
            seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
            record_run(&store, "r1", "orders");
        } // drop the seeding handle before the plan path opens its own

        // A missing config leaves the policy plane unconfigured — the queue
        // and the gates run off the ledger + plan files alone.
        let config = root.join("rocky.toml");
        let models_dir = root.join("models");

        // 1. Create the reclamation plan.
        run_gc_plan_in(root, &state_path, &config, 7, PolicyPrincipal::Human, true).unwrap();
        let plans_dir = root.join(".rocky").join("plans");
        let plan_id = std::fs::read_dir(&plans_dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter_map(|e| {
                let name = e.file_name().into_string().ok()?;
                name.strip_suffix(".json").map(str::to_string)
            })
            .next()
            .expect("a gc plan file must exist");

        // 2. The review queue lists it: the creation-time escalation row is in
        //    the ledger and the plan file passes the existence filter.
        let queue =
            crate::commands::review::compute_review_queue(root, &config, &state_path, &models_dir)
                .unwrap();
        assert_eq!(queue.total, 1, "pending: {:?}", queue.pending);
        assert_eq!(queue.pending[0].plan_id, plan_id);
        assert_eq!(queue.pending[0].capability, PolicyCapability::Gc);
        assert_eq!(queue.excluded_non_plan_rows, 0);

        // 3. Approve through the exact core the MCP review_queue tool calls.
        let review = crate::commands::review::compute_review(root, &config, &plan_id, "HEAD", true)
            .await
            .unwrap();
        assert!(review.marker_written);

        // 4. The queue empties — the marker filter clears the escalation.
        let queue_after =
            crate::commands::review::compute_review_queue(root, &config, &state_path, &models_dir)
                .unwrap();
        assert_eq!(queue_after.total, 0, "pending: {:?}", queue_after.pending);

        // 5. Apply proceeds past the review gate WITHOUT error. Creds-free, the
        //    manifest-truth liveness oracle cannot read the table's Delta log
        //    (the read needs object-store credentials), so every candidate is
        //    HELD (fail-closed) — nothing is tombstoned/retired. This test's
        //    intent is the queue lifecycle (plan → queued → approve → cleared →
        //    apply runs past the gate); the eviction mechanics are covered by
        //    the manifest-truth tests above.
        run_gc_apply_in(
            root,
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
        )
        .await
        .unwrap();
        let store = StateStore::open(&state_path).unwrap();
        assert!(
            store.list_tombstones().unwrap().is_empty(),
            "creds-free apply holds every candidate (no Delta-log access) — nothing retired"
        );
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
    }

    /// Seed a realistic multi-candidate ledger to the redb path in
    /// `ROCKY_GC_DEMO_DB`, using the same production write APIs the
    /// content-addressed writer uses, so the real `rocky gc` binary can be
    /// driven against it (the creds-free DuckDB path writes no
    /// content-addressed artifacts). Ignored by default — a manual harness,
    /// not a CI test. Run with:
    ///   ROCKY_GC_DEMO_DB=/tmp/gc-demo.redb \
    ///     cargo test -p rocky-cli --lib gc::tests::seed_demo_ledger -- --ignored
    #[test]
    #[ignore = "manual seeding harness for driving the real CLI; needs ROCKY_GC_DEMO_DB"]
    fn seed_demo_ledger() {
        let Ok(path) = std::env::var("ROCKY_GC_DEMO_DB") else {
            eprintln!("set ROCKY_GC_DEMO_DB to a redb path to seed the demo ledger");
            return;
        };
        let _ = std::fs::remove_file(&path);
        let store = StateStore::open(std::path::Path::new(&path)).unwrap();

        let old = Utc::now() - Duration::days(30);
        let fresh = Utc::now() - Duration::hours(2);

        // 1. Clean derivable candidate.
        seed(
            &store,
            "run-01",
            "dim_customers",
            "SELECT 1 AS id",
            &[],
            HA,
            10_240,
            old,
        );
        record_run(&store, "run-01", "dim_customers");

        // 2. Too recent — fails the age check only.
        const HD: &str = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        seed(
            &store,
            "run-02",
            "stg_events_today",
            "SELECT 2 AS id",
            &[],
            HD,
            2_048,
            fresh,
        );
        record_run(&store, "run-02", "stg_events_today");

        // 3. Nondeterministic recipe — never derivable.
        seed(
            &store,
            "run-03",
            "sampled_rows",
            "SELECT random() AS id",
            &[],
            HB,
            4_096,
            old,
        );
        record_run(&store, "run-03", "sampled_rows");

        // 4. Shared bytes across two runs — refcount 2, not derivable.
        seed(
            &store,
            "run-04",
            "fct_orders",
            "SELECT 4 AS id",
            &[],
            HC,
            8_192,
            old,
        );
        seed(
            &store,
            "run-05",
            "fct_orders",
            "SELECT 4 AS id",
            &[],
            HC,
            8_192,
            old,
        );
        record_run(&store, "run-04", "fct_orders");

        eprintln!("seeded demo ledger at {path}");
    }
}
