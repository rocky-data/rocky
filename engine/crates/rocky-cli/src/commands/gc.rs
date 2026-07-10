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
//! ledger row is retired, so an evicted cache entry is always restorable, and
//! the physical byte-delete only ever happens after that commit.
//!
//! # Reachability
//!
//! Content-addressed writes are s3-only, so the creds-free playground holds no
//! CAS artifacts. The plan → review → apply → tombstone → ledger-retire flow is
//! driven creds-free over a ledger seeded through the production write APIs
//! (see the `seed_demo_ledger` harness in the tests). The physical
//! object-store delete ([`ObjectStoreEvictor`]) is s3-only and is
//! **code-reviewed here, driven on the sandbox** — creds-free it defers.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::warn;

use rocky_core::config::{PolicyCapability, PolicyPrincipal, load_rocky_config};
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::state::{
    ArtifactRecord, EvictOutcome, ModelExecution, ProvenanceRecord, RunRecord, StateStore,
    TombstoneRecord,
};

use crate::commands::apply::{PolicyGate, ai_plan_is_reviewed, evaluate_apply_policy};
use crate::commands::replay::classify_model;
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
fn check_recipe_recorded(class: &ReplayCheckModelOutput) -> GcCheckOutput {
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
fn check_recipe_produces_output(
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

        out.push(EvictionCandidate {
            artifact: representative,
            output,
            recipe_hash,
            input_hash,
            input_proof_class,
            env_hash,
            hash_scheme,
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
        "This plan lists only artifacts proved derivable at plan time. `rocky apply` \
         re-verifies each against the live ledger before evicting — a reference that \
         appears between plan and apply refuses the eviction (fail-closed)."
            .to_string(),
        "Every eviction writes a durable tombstone (recipe triple + restore pointer) BEFORE \
         the ledger row is retired, so an evicted cache entry is always restorable."
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
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let adapter = load_adapter_cost(config_path);
    let candidates =
        gather_eviction_candidates(&store, adapter.as_ref(), Utc::now(), min_age_days)?;

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

    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    let plan_id = write_plan_with_principal(&cwd, PlanKind::Gc, &plan, principal)
        .context("failed to persist the gc plan")?;

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

/// Outcome of a best-effort physical reclamation of an evicted artifact's
/// bytes through the object-store adapter.
enum PhysicalReclaim {
    /// The bytes were deleted from the object store.
    Deleted,
    /// The physical delete was intentionally not attempted (no reachable object
    /// store / credentials on this adapter). The byte is a safe leaked orphan.
    Deferred(String),
    /// The physical delete was attempted and failed. The byte remains; the
    /// tombstone stands, so it is a safe leaked orphan a later sweep reclaims.
    Failed(String),
}

/// Physically reclaims an evicted artifact's bytes.
///
/// The abstraction is what makes the eviction path testable creds-free (tests
/// inject a recording evictor) while the real object-store delete stays a
/// single, reviewable implementation.
#[async_trait]
trait ArtifactEvictor: Send + Sync {
    async fn evict_bytes(&self, file_path: &str) -> PhysicalReclaim;
}

/// Deletes evicted bytes through the `object_store` S3 adapter.
///
/// Content-addressed storage is s3-only (`AmazonS3Builder::from_env`), so this
/// is the production reclamation path. The delete itself is exercised only
/// against a live bucket — it is **code-reviewed here, driven on the sandbox**,
/// never against the creds-free playground (which writes no CAS artifacts).
struct ObjectStoreEvictor;

#[async_trait]
impl ArtifactEvictor for ObjectStoreEvictor {
    async fn evict_bytes(&self, file_path: &str) -> PhysicalReclaim {
        use object_store::ObjectStoreExt;

        let store = match super::run_content_addressed::build_object_store(file_path) {
            Ok(s) => s,
            Err(e) => return PhysicalReclaim::Failed(format!("could not build object store: {e}")),
        };
        let Some(key) = s3_object_key(file_path) else {
            return PhysicalReclaim::Failed(format!(
                "could not derive an object key from '{file_path}'"
            ));
        };
        match store.delete(&object_store::path::Path::from(key)).await {
            Ok(()) => PhysicalReclaim::Deleted,
            Err(e) => PhysicalReclaim::Failed(format!("object-store delete failed: {e}")),
        }
    }
}

/// Records every eviction as deferred without touching an object store — the
/// creds-free / non-s3 posture. The ledger eviction (tombstone + row retire)
/// still stands; the byte is a safe leaked orphan a later sweep can reclaim.
struct DeferredEvictor {
    reason: String,
}

#[async_trait]
impl ArtifactEvictor for DeferredEvictor {
    async fn evict_bytes(&self, _file_path: &str) -> PhysicalReclaim {
        PhysicalReclaim::Deferred(self.reason.clone())
    }
}

/// Parse the bucket-relative object key out of an `s3://bucket/key…` URL.
fn s3_object_key(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    Some(parsed.path().trim_start_matches('/').to_string())
}

/// Select the physical evictor for this apply.
///
/// Content-addressed deletion is s3-only, so an `ObjectStoreEvictor` is chosen
/// only when AWS credentials are present in the environment; otherwise the
/// physical delete is deferred (no reachable object store). Gating on creds
/// keeps the creds-free real-CLI drive fast and honest — it defers rather than
/// stalling on an unreachable-bucket retry.
fn choose_evictor() -> Box<dyn ArtifactEvictor> {
    let has_creds = std::env::var("AWS_ACCESS_KEY_ID").is_ok()
        && std::env::var("AWS_SECRET_ACCESS_KEY").is_ok();
    if has_creds {
        Box::new(ObjectStoreEvictor)
    } else {
        Box::new(DeferredEvictor {
            reason: "no reachable object store — content-addressed deletion is s3-only and no \
                     AWS credentials were found in the environment"
                .to_string(),
        })
    }
}

/// Apply caveats surfaced on the eviction result.
fn gc_apply_notes() -> Vec<String> {
    vec![
        "Refcounts see Rocky-ledger references only. Multi-ref safety on the UniForm path \
         (branch / env / downstream-deferred-read pointers) is code-reviewed, not driven here."
            .to_string(),
        "Physical object-store deletion is s3-only and is driven only against a live bucket; \
         on the creds-free path the byte-delete is deferred and the tombstone + retired ledger \
         row are the eviction of record."
            .to_string(),
        "Restore (evict → rebuild → bit-exact) is a later phase and is not exercised here — an \
         evicted artifact's tombstone captures the recipe to rebuild it, but the roundtrip is \
         unverified."
            .to_string(),
    ]
}

/// The eviction engine: re-derive eligibility against the live ledger, then for
/// each planned artifact that is **still derivable AND an exact identity match**
/// write its tombstone and retire its ledger row atomically before a
/// best-effort physical delete.
///
/// Pure over its inputs (`store`, `evictor`, `now`) so tests drive it directly
/// with a seeded store and a recording evictor. The review + policy gates live
/// in [`run_gc_apply_in`] and run *before* this is called.
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
/// 3. evicts only when a freshly-derived candidate matches that full identity
///    and is derivable *now* — building the tombstone from the LIVE candidate,
///    never the plan payload, so the tombstone records the exact bytes deleted;
/// 4. relies on [`StateStore::evict_artifact`]'s in-transaction hash check as a
///    final guard against a race.
///
/// Any plan row that fails to match a current derivable candidate is refused —
/// nothing is deleted. The hash is the eviction identity, never the `(run,
/// model)` pair.
async fn execute_gc_apply(
    store: &StateStore,
    evictor: &dyn ArtifactEvictor,
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
                // Tombstone built from the LIVE candidate (authoritative identity
                // + recipe triple), so it records the bytes actually evicted.
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

                // Physical reclamation happens LAST and is best-effort — a
                // failure here leaves a safe leaked orphan, never a dangling
                // reference (the row is already gone, the tombstone stands).
                let (physical_reclaimed, physical_status) =
                    match evictor.evict_bytes(&cand.artifact.file_path).await {
                        PhysicalReclaim::Deleted => (true, "deleted".to_string()),
                        PhysicalReclaim::Deferred(m) => (false, format!("deferred: {m}")),
                        PhysicalReclaim::Failed(m) => (false, format!("failed: {m}")),
                    };
                if physical_reclaimed {
                    // Flip the custody flag now the bytes are actually gone so a
                    // later sweep skips them. Best-effort — a failed update just
                    // means a redundant future re-check.
                    let mut updated = tombstone.clone();
                    updated.physical_reclaimed = true;
                    if let Err(e) = store.record_tombstone(&updated) {
                        warn!(
                            target: "rocky::gc",
                            error = %e,
                            hash = %cand.artifact.blake3_hash,
                            "evicted bytes but failed to update the tombstone's reclaimed flag"
                        );
                    }
                }

                evicted.push(GcEvictedOutput {
                    model_name: cand.artifact.model_name.clone(),
                    run_id: cand.artifact.run_id.clone(),
                    blake3_hash: cand.artifact.blake3_hash.clone(),
                    size_bytes: cand.artifact.size_bytes,
                    tombstone_recorded: true,
                    physical_reclaimed,
                    physical_status,
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
fn gc_models_dir(
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
    let gate = evaluate_apply_policy(
        config_path,
        plan_id,
        plan_record.enforcement_principal(runtime_principal),
        &touched,
        &models_dir,
        state_path,
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

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let evictor = choose_evictor();
    let output = execute_gc_apply(&store, evictor.as_ref(), plan_id, &plan, Utc::now()).await?;

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
            "  EVICTED       {}  {} bytes  {}  (tombstone written; physical: {})",
            e.model_name,
            e.size_bytes,
            e.blake3_hash.get(..12).unwrap_or(&e.blake3_hash),
            e.physical_status,
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
            })
            .unwrap();
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

    // -- plan + apply (tombstone / evict / refuse) ------------------------

    /// A test evictor that records the paths it was asked to reclaim and
    /// returns a fixed outcome — lets the eviction engine run creds-free while
    /// asserting the physical-delete wiring and the tombstone-before-delete
    /// invariant.
    struct TestEvictor {
        calls: std::sync::Mutex<Vec<String>>,
        outcome: TestOutcome,
    }

    #[derive(Clone, Copy)]
    enum TestOutcome {
        Deleted,
        Deferred,
        Failed,
    }

    impl TestEvictor {
        fn new(outcome: TestOutcome) -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
                outcome,
            }
        }
        fn calls(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl ArtifactEvictor for TestEvictor {
        async fn evict_bytes(&self, file_path: &str) -> PhysicalReclaim {
            self.calls.lock().unwrap().push(file_path.to_string());
            match self.outcome {
                TestOutcome::Deleted => PhysicalReclaim::Deleted,
                TestOutcome::Deferred => PhysicalReclaim::Deferred("test-defer".to_string()),
                TestOutcome::Failed => PhysicalReclaim::Failed("test-fail".to_string()),
            }
        }
    }

    fn plan_from_store(store: &StateStore, now: DateTime<Utc>, min_age_days: i64) -> GcPlan {
        let cands = gather_eviction_candidates(store, None, now, min_age_days).unwrap();
        build_gc_plan(&cands, min_age_days).expect("expected at least one derivable candidate")
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

    /// The bucket-relative object key derivation is the only real logic on the
    /// code-reviewed-not-driven physical-delete path, and restore-roundtrip is
    /// held, so nothing else guards it: a regression here would delete the wrong
    /// key. Lock the parse against the storage-prefix shape the writer emits.
    #[test]
    fn s3_object_key_strips_scheme_bucket_and_leading_slash() {
        assert_eq!(
            s3_object_key("s3://bucket/tgt/raw/orders/x.parquet").as_deref(),
            Some("tgt/raw/orders/x.parquet")
        );
        assert_eq!(
            s3_object_key("s3://b/x.parquet").as_deref(),
            Some("x.parquet")
        );
        // A non-URL yields no key (the evictor then reports a failure rather
        // than deleting anything).
        assert!(s3_object_key("not a url").is_none());
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

        let evictor = TestEvictor::new(TestOutcome::Deferred);
        let out = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
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
        assert!(!tombs[0].physical_reclaimed, "deferred physical delete");
        assert!(out.evicted[0].physical_status.contains("deferred"));
        // The physical evictor was invoked with the artifact's path (wiring).
        assert_eq!(evictor.calls(), vec![format!("s3://b/{HA}.parquet")]);
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

        let evictor = TestEvictor::new(TestOutcome::Deferred);
        let out = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
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
        // No tombstone, both rows intact, the physical evictor never fired.
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HC).unwrap(), 2);
        assert!(evictor.calls().is_empty());
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

        let evictor = TestEvictor::new(TestOutcome::Deferred);
        let out = execute_gc_apply(&store, &evictor, "crafted", &crafted, Utc::now())
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
        // Both rows intact, no tombstone, the physical evictor never fired.
        assert!(store.list_tombstones().unwrap().is_empty());
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 1);
        assert_eq!(store.refcount_for_hash(HB).unwrap(), 1);
        assert!(evictor.calls().is_empty());
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

        let evictor = TestEvictor::new(TestOutcome::Deferred);
        let first = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(first.evicted_count, 1);

        // Re-applying the same plan is a clean no-op — the row is already gone.
        let second = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
            .await
            .unwrap();
        assert_eq!(second.evicted_count, 0);
        assert_eq!(second.refused_count, 0);
        assert_eq!(second.already_evicted, vec![HA.to_string()]);
        // No duplicate tombstone.
        assert_eq!(store.list_tombstones().unwrap().len(), 1);
    }

    /// Tombstone-before-delete: even when the physical object-store delete
    /// FAILS, the tombstone stands and the ledger row is retired. The eviction
    /// of record is the atomic tombstone + row-retirement; the byte delete is
    /// best-effort and its failure leaves only a safe leaked orphan.
    #[tokio::test]
    async fn apply_evicts_even_when_physical_delete_fails() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let evictor = TestEvictor::new(TestOutcome::Failed);
        let out = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
            .await
            .unwrap();

        assert_eq!(out.evicted_count, 1);
        assert!(out.evicted[0].physical_status.contains("failed"));
        assert!(!out.evicted[0].physical_reclaimed);
        // The tombstone + retirement stand despite the failed byte delete.
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 0);
        let tombs = store.list_tombstones().unwrap();
        assert_eq!(tombs.len(), 1);
        assert!(!tombs[0].physical_reclaimed);
    }

    #[tokio::test]
    async fn apply_flips_reclaimed_flag_when_physical_delete_succeeds() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let old = Utc::now() - Duration::days(30);
        seed(&store, "r1", "orders", "SELECT 1 AS id", &[], HA, 500, old);
        record_run(&store, "r1", "orders");
        let now = Utc::now();
        let plan = plan_from_store(&store, now, 7);

        let evictor = TestEvictor::new(TestOutcome::Deleted);
        let out = execute_gc_apply(&store, &evictor, "plan-x", &plan, now)
            .await
            .unwrap();

        assert!(out.evicted[0].physical_reclaimed);
        assert_eq!(out.evicted[0].physical_status, "deleted");
        assert!(store.list_tombstones().unwrap()[0].physical_reclaimed);
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

        // No marker → refuse.
        let err = run_gc_apply_in(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
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
        run_gc_apply_in(
            dir.path(),
            &config,
            &plan_id,
            &state_path,
            PolicyPrincipal::Human,
            true,
        )
        .await
        .unwrap();

        let store = StateStore::open(&state_path).unwrap();
        assert_eq!(store.list_tombstones().unwrap().len(), 1);
        assert_eq!(store.refcount_for_hash(HA).unwrap(), 0);
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
