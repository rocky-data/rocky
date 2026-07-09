//! `rocky gc --derivable --dry-run` — the read-only derivability inventory.
//!
//! Answers one question over the content-addressed artifact ledger: *which
//! stored bytes can Rocky prove it can rebuild, and are therefore a cache
//! entry rather than an asset?* The recipe is the truth; a rebuildable table
//! is derivable, and derivable bytes are reclaimable.
//!
//! This surface is **inventory-only**. It opens the state store read-only and
//! never writes, deletes, or plans a deletion — there is no mutation path in
//! this module at all. The report is the whole product: an aggregate ("X
//! bytes / Y% of managed storage is derivable") plus a per-candidate
//! justification that prints all five eligibility checks.
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
//! # Fail-closed
//!
//! Each check fails closed: any doubt keeps the artifact non-derivable. In
//! this phase there is no deletion, so a false *derivable* is only a dishonest
//! report line — but the discipline is set here because the later evict path
//! inherits these verdicts.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::warn;

use rocky_core::config::load_rocky_config;
use rocky_core::cost::{WarehouseType, compute_observed_cost_usd, warehouse_size_to_dbu_per_hour};
use rocky_core::state::{ArtifactRecord, ModelExecution, RunRecord, StateStore};

use crate::commands::replay::classify_model;
use crate::output::{
    GcCandidateOutput, GcCheckOutput, GcRebuildCostOutput, GcReportOutput, ReplayCheckModelOutput,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Resolved cost-model inputs: `(adapter_name, warehouse_type, dbu_per_hour,
/// cost_per_dbu)`. `None` when the config can't be loaded or the adapter is
/// not a billed warehouse.
type AdapterCost = (String, WarehouseType, f64, f64);

// ---------------------------------------------------------------------------
// The five eligibility checks — each a pure function, each fails closed.
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

/// Check 2 — replayable and deterministic.
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

/// Check 3 — unreferenced: no other live ledger pointer at these bytes.
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

/// Check 4 — policy allows (surfaced, not yet enforced).
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

/// Check 5 — age / activity threshold.
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
fn build_candidate(
    artifact: &ArtifactRecord,
    refcount: u64,
    class: &ReplayCheckModelOutput,
    exec: Option<&ModelExecution>,
    now: DateTime<Utc>,
    min_age_days: i64,
    adapter: Option<&AdapterCost>,
) -> GcCandidateOutput {
    let checks = vec![
        check_recipe_recorded(class),
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

/// Assemble the full inventory from a state store. All store reads live here
/// (the honest reachability seam); the report itself is a pure function of the
/// ledger contents at `now`.
///
/// Read-only: opens no write transaction and touches no warehouse.
fn gather_report(
    store: &StateStore,
    adapter: Option<&AdapterCost>,
    now: DateTime<Utc>,
    min_age_days: i64,
) -> Result<GcReportOutput> {
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
    let mut candidates: Vec<GcCandidateOutput> = Vec::with_capacity(by_hash.len());

    for (hash, mut rows) in by_hash {
        rows.sort_by(|a, b| {
            b.written_at
                .cmp(&a.written_at)
                .then_with(|| a.run_id.cmp(&b.run_id))
                .then_with(|| a.model_name.cmp(&b.model_name))
        });
        let representative = &rows[0];

        // Refcount via the shipped Phase-6 primitive — the sanctioned "is any
        // other pointer live?" query, not a re-count of the group.
        let refcount = store
            .refcount_for_hash(&hash)
            .with_context(|| format!("failed to refcount artifact {hash}"))?;

        let class = classify_model(store, &representative.run_id, &representative.model_name);

        let run = run_cache
            .entry(representative.run_id.clone())
            .or_insert_with(|| store.get_run(&representative.run_id).ok().flatten());
        let exec = run.as_ref().and_then(|r| {
            r.models_executed
                .iter()
                .find(|m| m.model_name == representative.model_name)
        });

        candidates.push(build_candidate(
            representative,
            refcount,
            &class,
            exec,
            now,
            min_age_days,
            adapter,
        ));
    }

    // Newest write first; stable tie-break on hash for deterministic output.
    candidates.sort_by(|a, b| {
        b.written_at
            .cmp(&a.written_at)
            .then_with(|| a.blake3_hash.cmp(&b.blake3_hash))
    });

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

    #[test]
    fn candidate_derivable_when_all_checks_pass() {
        let now = Utc::now();
        let art = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        let c = build_candidate(&art, 1, &model("orders"), None, now, 7, None);
        assert!(c.derivable);
        assert_eq!(c.checks.len(), 5);
        assert!(c.checks.iter().all(|k| k.passed));
    }

    #[test]
    fn candidate_not_derivable_when_shared() {
        let now = Utc::now();
        let art = artifact(HA, "orders", "r1", 100, now - Duration::days(30));
        let c = build_candidate(&art, 2, &model("orders"), None, now, 7, None);
        assert!(!c.derivable);
        let unref = c.checks.iter().find(|k| k.check == "unreferenced").unwrap();
        assert!(!unref.passed);
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
        // The other four still pass — the report shows exactly why it's held.
        assert_eq!(c.checks.iter().filter(|k| k.passed).count(), 4);
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
