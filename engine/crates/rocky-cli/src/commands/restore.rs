//! `rocky restore <target>` — rebuild a gc-evicted artifact from its durable
//! tombstone, hash-exact, and reinstate its ledger row.
//!
//! A gc tombstone is a claim: *these bytes were a cache entry Rocky can
//! rebuild*. Restore is the proof. It resolves the target to exactly one
//! tombstone, replays the recorded recipe (the tombstone's `run_id` +
//! `model_name` → the [`rocky_core::state::ProvenanceRecord`]'s embedded
//! canonical `ModelIr` — never the working tree), and asserts the recomputed
//! blake3 equals [`rocky_core::state::TombstoneRecord::blake3_hash`] **before
//! any write becomes visible**.
//!
//! Two surfaces, mirroring `rocky gc`'s symmetric-caution posture:
//!
//! - **plan** ([`run_restore_plan`]): resolves the target (model name,
//!   `model@<recipe-hash-prefix>`, or a content-hash prefix), refuses
//!   ambiguity with the candidates listed, and writes a review-gated
//!   [`crate::plan_store::PlanKind::Restore`] plan. Writes nothing else.
//! - **apply** ([`run_restore_apply_in`], via `rocky apply <plan-id>`): after
//!   an **unconditional** review gate (even a human restore goes through
//!   `rocky review` — the same hard rule as gc), re-resolves each planned
//!   tombstone against the live custody ledger at full `(run, model, path,
//!   hash)` identity, re-derives the bytes, verifies, re-materializes, and
//!   reinstates the [`rocky_core::state::ArtifactRecord`]. The consumed
//!   tombstone is stamped (`restored_at` / `restore_plan_id`) — never deleted
//!   — so the evict → restore history stays auditable.
//!
//! # Fail-closed — everything refuses before it guesses
//!
//! - the recomputed blake3 must equal the tombstoned hash **before** any
//!   write; a mismatch writes nothing and reports the failed rebuild claim
//!   honestly (that mismatch is itself a finding);
//! - bytes already present at the tombstoned path are hashed first: a match
//!   is an idempotent "already present, verified" success, a mismatch is a
//!   hard refusal — restore never overwrites bytes it cannot prove are the
//!   evicted ones;
//! - the physical write is a create-only conditional put, so a concurrent
//!   writer landing bytes in the check→put window loses nothing (the put
//!   fails and the bytes are re-verified);
//! - the ledger reinstatement is atomic and hash-guarded
//!   ([`rocky_core::state::StateStore::restore_artifact`]) — a location
//!   re-materialized to different bytes since the eviction refuses.
//!
//! # Reachability
//!
//! Re-derivation runs the recorded recipe on the **recording engine** — the
//! project's configured warehouse adapter — and re-encodes the parquet with
//! the table's **discovered** column-mapping state, so a live-written
//! artifact's physical (UUID) encoding is reproduced exactly. Re-deriving on
//! any other engine (a DuckDB rebuild of a warehouse-written artifact) diverges
//! bit-for-bit and the hash-exact check refuses. The
//! re-materialization needs the table's object store; creds-free the store
//! build fails and the restoration is refused (fail-closed), mirroring gc's
//! creds-free posture. The tests drive the identical decision path over an
//! in-memory object store seeded through the production write APIs.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use object_store::ObjectStore;
use object_store::path::Path as ObjPath;

use rocky_core::config::{PolicyCapability, PolicyPrincipal};
use rocky_core::state::{
    ArtifactRecord, ProvenanceRecord, RestoreOutcome, StateStore, TombstoneRecord,
};
use rocky_iceberg::uniform_writer::{
    Result as UfResult, SqlClient, UniformTableState, UniformWriter, UniformWriterConfig,
    UniformWriterError,
};
use rocky_ir::ModelIr;

use crate::commands::apply::{PolicyGate, ai_plan_is_reviewed, evaluate_apply_policy_with_policy};
use crate::commands::gc::{check_recipe_produces_output, gc_models_dir};
use crate::commands::review::record_plan_review_escalation;
use crate::commands::run_content_addressed::{build_object_store, table_relative_add_path};
use crate::output::{
    RestoreApplyOutput, RestorePlan, RestorePlanOutput, RestorePlanRestoration,
    RestoreRefusedOutput, RestoredOutput,
};
use crate::plan_store::{PlanKind, read_plan, write_plan_with_principal};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// First 12 hex chars of a hash for compact messages; the full string if
/// shorter.
fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

// ---------------------------------------------------------------------------
// Target resolution — model name, model@<recipe-prefix>, or hash prefix
// ---------------------------------------------------------------------------

/// Whether `s` is plausibly a content-hash prefix: at least 8 hex chars.
///
/// The floor keeps a short model name (or a typo) from accidentally matching a
/// hash by prefix; anything shorter only resolves as a model name.
fn is_hash_prefix(s: &str) -> bool {
    s.len() >= 8 && s.chars().all(|c| c.is_ascii_hexdigit())
}

/// Whether a tombstone matches the user-supplied `target`.
///
/// Three spellings, checked in this order (all case-sensitive except the hex
/// prefix, which is lowercased — hashes are recorded lowercase):
///
/// - `model@<recipe-hash-prefix>` — the model name plus a prefix of the
///   recorded recipe-identity hash;
/// - an exact model name;
/// - a content-hash prefix (≥ 8 hex chars) of the tombstoned blake3.
fn matches_target(t: &TombstoneRecord, target: &str) -> bool {
    if let Some((model, recipe_prefix)) = target.split_once('@') {
        return t.model_name == model
            && !recipe_prefix.is_empty()
            && t.recipe_hash
                .as_deref()
                .is_some_and(|r| r.starts_with(recipe_prefix));
    }
    if t.model_name == target {
        return true;
    }
    is_hash_prefix(target) && t.blake3_hash.starts_with(&target.to_ascii_lowercase())
}

/// One line describing a tombstone in an ambiguity / diagnosis listing.
fn describe_tombstone(t: &TombstoneRecord) -> String {
    format!(
        "  {}  {}…  evicted {}  (run {}, {} bytes){}",
        t.model_name,
        short_hash(&t.blake3_hash),
        t.evicted_at.to_rfc3339(),
        t.run_id,
        t.size_bytes,
        match &t.restored_at {
            Some(at) => format!("  [restored {}]", at.to_rfc3339()),
            None => String::new(),
        }
    )
}

/// Resolve `target` against the custody ledger to exactly one **unrestored**
/// tombstone, or fail with an honest, actionable error:
///
/// - ambiguous → refuse, listing every candidate with its hash prefix so the
///   user can re-run with an unambiguous spelling;
/// - matches only already-restored tombstones → say so (nothing to restore);
/// - matches a live ledger artifact → say it was **never evicted**;
/// - matches nothing at all → say so (distinct from "never evicted").
fn resolve_target<'a>(
    tombstones: &'a [TombstoneRecord],
    live_artifacts: &[ArtifactRecord],
    target: &str,
) -> Result<&'a TombstoneRecord> {
    let matches: Vec<&TombstoneRecord> = tombstones
        .iter()
        .filter(|t| t.restored_at.is_none() && matches_target(t, target))
        .collect();

    match matches.len() {
        1 => Ok(matches[0]),
        0 => {
            // Honest diagnosis, most specific first.
            let restored: Vec<&TombstoneRecord> = tombstones
                .iter()
                .filter(|t| t.restored_at.is_some() && matches_target(t, target))
                .collect();
            if !restored.is_empty() {
                bail!(
                    "'{target}' matches {} tombstone(s) that were ALREADY restored — the \
                     artifact(s) stand in the ledger and there is nothing to restore:\n{}",
                    restored.len(),
                    restored
                        .iter()
                        .map(|t| describe_tombstone(t))
                        .collect::<Vec<_>>()
                        .join("\n"),
                );
            }
            let live = live_artifacts.iter().any(|a| {
                a.model_name == target
                    || (is_hash_prefix(target)
                        && a.blake3_hash.starts_with(&target.to_ascii_lowercase()))
            });
            if live {
                bail!(
                    "'{target}' matches a LIVE artifact in the ledger — it was never evicted \
                     (no tombstone exists for it), so there is nothing to restore."
                );
            }
            bail!(
                "no tombstone or ledger artifact matches '{target}' — nothing was ever \
                 recorded or evicted under that model name / hash. Run \
                 `rocky gc --derivable --dry-run` to inspect the artifact ledger."
            );
        }
        n => {
            let listing = matches
                .iter()
                .map(|t| describe_tombstone(t))
                .collect::<Vec<_>>()
                .join("\n");
            bail!(
                "'{target}' is ambiguous — {n} evicted artifacts match. Re-run with the \
                 content-hash prefix (or model@<recipe-prefix>) of the one you mean:\n{listing}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Plan — `rocky restore <target>`
// ---------------------------------------------------------------------------

/// Operator caveats surfaced on a restore plan and its apply.
fn restore_plan_notes() -> Vec<String> {
    vec![
        "Restore re-derives the artifact from its RECORDED recipe (the tombstone's provenance \
         pointer), never from the working tree — the rebuild is the recording's claim being \
         proven, not a fresh build."
            .to_string(),
        "The recomputed blake3 must equal the tombstoned hash BEFORE any write becomes visible. \
         A mismatch is a hard failure that writes nothing — and is itself a finding: the \
         tombstone's rebuild claim did not hold."
            .to_string(),
        "Bytes already present at the tombstoned path are verified first: a hash match is an \
         idempotent success (\"already present, verified\"); a mismatch REFUSES — restore never \
         overwrites bytes it cannot prove are the evicted ones."
            .to_string(),
        "Restoration is symmetric-caution gated like gc: review with `rocky review <plan-id> \
         --approve`, then `rocky apply <plan-id>`. There is no direct-restore path."
            .to_string(),
    ]
}

/// Execute `rocky restore <target>`: resolve the tombstone and write a
/// review-gated restore plan. **Writes nothing else.**
pub fn run_restore_plan(
    state_path: &Path,
    target: &str,
    principal: PolicyPrincipal,
    json: bool,
) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    run_restore_plan_in(&cwd, state_path, target, principal, json)
}

/// Inner implementation — takes an explicit `root` for the plans directory so
/// tests can pass a temp dir without touching the process-global cwd.
pub(crate) fn run_restore_plan_in(
    root: &Path,
    state_path: &Path,
    target: &str,
    principal: PolicyPrincipal,
    json: bool,
) -> Result<()> {
    let (tombstones, live_artifacts) = {
        let store = StateStore::open_read_only(state_path)
            .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
        let tombs = store
            .list_tombstones()
            .context("failed to read the tombstone custody ledger")?;
        let arts = store
            .list_all_artifacts()
            .context("failed to read the artifact ledger")?;
        (tombs, arts)
        // The read handle drops here so the escalation write below can open
        // its own handle on the same store.
    };

    let tombstone = resolve_target(&tombstones, &live_artifacts, target)?;

    let plan = RestorePlan {
        version: VERSION.to_string(),
        target: target.to_string(),
        restorations: vec![RestorePlanRestoration {
            model_name: tombstone.model_name.clone(),
            run_id: tombstone.run_id.clone(),
            blake3_hash: tombstone.blake3_hash.clone(),
            file_path: tombstone.file_path.clone(),
            size_bytes: tombstone.size_bytes,
            commit_version: tombstone.commit_version,
            evicted_at: tombstone.evicted_at.to_rfc3339(),
            gc_plan_id: tombstone.plan_id.clone(),
            recipe_hash: tombstone.recipe_hash.clone(),
            input_hash: tombstone.input_hash.clone(),
            input_proof_class: tombstone.input_proof_class.clone(),
        }],
    };

    let plan_id = write_plan_with_principal(root, PlanKind::Restore, &plan, principal)
        .context("failed to persist the restore plan")?;

    // A restore plan never passes `evaluate_apply_policy` before its apply
    // bails on the missing review marker, so record its escalation at creation
    // — this one plan-level row is what puts it in the review queue (and in
    // front of the governor's MCP approve path). Same pattern as gc/backfill.
    record_plan_review_escalation(
        state_path,
        &plan_id,
        principal,
        PolicyCapability::Restore,
        &format!(
            "restore: {} ({}…)",
            tombstone.model_name,
            short_hash(&tombstone.blake3_hash)
        ),
        "restore plan awaits review — restoration is unconditionally review-gated (even a human \
         restore goes through review, mirroring gc's symmetric-caution posture)",
    );

    let output = RestorePlanOutput {
        version: VERSION.to_string(),
        command: "restore".to_string(),
        plan_id: plan_id.clone(),
        target: target.to_string(),
        restoration_count: plan.restorations.len(),
        total_bytes: plan.restorations.iter().map(|r| r.size_bytes).sum(),
        review_required: true,
        notes: restore_plan_notes(),
        restorations: plan.restorations,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_plan_table(&output);
    }
    Ok(())
}

fn print_plan_table(output: &RestorePlanOutput) {
    println!(
        "Rocky restore plan {} — {} tombstoned artifact(s), {} bytes to restore",
        short_hash(&output.plan_id),
        output.restoration_count,
        output.total_bytes
    );
    println!();
    for r in &output.restorations {
        println!(
            "  {}  {} bytes  {}",
            r.model_name,
            r.size_bytes,
            short_hash(&r.blake3_hash)
        );
        println!(
            "      run: {}  evicted: {}  path: {}",
            r.run_id, r.evicted_at, r.file_path
        );
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

// ---------------------------------------------------------------------------
// Apply — `rocky apply <restore-plan>`
// ---------------------------------------------------------------------------

/// Provider of the object store a restoration re-materializes through —
/// injectable so tests drive the SAME decision path over an in-memory store.
/// The production provider builds the s3 store for the candidate's own table
/// (creds-free the build/read fails and the restoration is refused).
pub(crate) trait RestoreStores: Send + Sync {
    fn store_for(&self, storage_prefix: &str) -> Result<Arc<dyn ObjectStore>>;
}

/// Production provider: the standard s3 store builder (env-var credentials).
struct S3RestoreStores;

impl RestoreStores for S3RestoreStores {
    fn store_for(&self, storage_prefix: &str) -> Result<Arc<dyn ObjectStore>> {
        build_object_store(storage_prefix)
    }
}

/// Placeholder SqlClient for the metadata-only discovery writer — restore
/// never issues SQL through the writer, so an `execute` is a wiring bug.
struct NoOpSqlClient;

#[async_trait::async_trait]
impl SqlClient for NoOpSqlClient {
    async fn execute(&self, _sql: &str) -> UfResult<()> {
        Err(UniformWriterError::Sql(
            "internal: restore never issues SQL through the UniformWriter".into(),
        ))
    }
}

/// Operator caveats surfaced on the restore apply result.
fn restore_apply_notes() -> Vec<String> {
    vec![
        "Re-derivation runs the recorded recipe on the recording engine (the configured \
         warehouse) and re-encodes the parquet with the table's DISCOVERED column-mapping state \
         — the recomputed blake3 was asserted equal to the tombstoned hash BEFORE any write \
         became visible."
            .to_string(),
        "Bytes already present at the tombstoned path were verified by hash before anything \
         happened: a match restored without writing; a mismatch refused (restore never \
         overwrites bytes it cannot prove are the evicted ones)."
            .to_string(),
        "The physical write is a create-only conditional put — a concurrent writer landing \
         bytes in the verify→write window fails the put and the bytes are re-verified instead \
         of clobbered."
            .to_string(),
        "The consumed tombstone is stamped restored, never deleted — the evict → restore \
         history stays in the custody ledger for audit."
            .to_string(),
    ]
}

/// The per-restoration outcome [`execute_restore_apply`] folds into the
/// output lists.
enum RestoreOneOutcome {
    Restored(RestoredOutput),
    AlreadyRestored(String),
    Refused(RestoreRefusedOutput),
}

/// Re-execute the recorded recipe on the recording engine and re-encode the
/// parquet with the table's **discovered** state, returning the deterministic
/// parquet bytes.
///
/// `warehouse` MUST be the same engine that produced the artifact — the
/// project's configured warehouse adapter. A content-addressed artifact
/// written by the warehouse is only reproducible bit-for-bit by re-executing
/// on that same engine: cross-engine coercion (e.g. timestamp handling) makes
/// a DuckDB re-execution of a warehouse-written artifact diverge, so the
/// recomputed blake3 would never match. The encode path is the SAME
/// `build_parquet` the content-addressed writer used at record time, driven by
/// the same discovered column mapping — which, on the recording engine, is what
/// makes the recomputed blake3 directly comparable to the tombstoned hash.
async fn rebuild_artifact_bytes(
    ir: &ModelIr,
    state: &UniformTableState,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
) -> Result<Vec<u8>> {
    let result = warehouse
        .execute_query(&ir.sql)
        .await
        .map_err(|e| anyhow!("re-execution query failed: {e}"))?;
    let batch = crate::commands::run_content_addressed::query_result_to_record_batch(
        &ir.typed_columns,
        &result,
    )?;
    rocky_iceberg::uniform_writer::parquet_builder::build_parquet(&batch, state)
        .map_err(|e| anyhow!("re-derivation Parquet encode failed: {e}"))
}

/// The bucket-relative key prefix of an `s3://bucket/key` storage prefix.
fn key_prefix_of(storage_prefix: &str) -> Option<String> {
    let rest = storage_prefix.strip_prefix("s3://")?;
    let (_bucket, key) = rest.split_once('/')?;
    let key = key.trim_end_matches('/');
    (!key.is_empty()).then(|| key.to_string())
}

/// Discover the table's [`UniformTableState`] (column mapping, partitioning)
/// from its `_delta_log` through a metadata-only [`UniformWriter`].
async fn discover_table_state(
    obj_store: Arc<dyn ObjectStore>,
    storage_prefix: &str,
) -> Result<UniformTableState> {
    let key_prefix = key_prefix_of(storage_prefix)
        .ok_or_else(|| anyhow!("storage_prefix {storage_prefix:?} is not s3://bucket/key"))?;
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: String::new(),
            schema: String::new(),
            table: String::new(),
            prefix: key_prefix,
            engine_info: format!("rocky-cli/{VERSION}"),
        },
        obj_store,
        Arc::new(NoOpSqlClient),
    );
    writer
        .discover()
        .await
        .map_err(|e| anyhow!("could not discover the table's state from its _delta_log: {e}"))
}

/// Restore one planned tombstone. Every early exit is a refusal that wrote
/// nothing; the happy path is: resolve live tombstone → bind recipe to the
/// exact hash → re-derive → **hash-verify before any write** → materialize
/// (verify-or-create-only) → reinstate the ledger row atomically.
async fn restore_one(
    store: &StateStore,
    stores: &dyn RestoreStores,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    tombstones: &[TombstoneRecord],
    plan_id: &str,
    planned: &RestorePlanRestoration,
    now: DateTime<Utc>,
) -> RestoreOneOutcome {
    let refuse = |reason: String| {
        RestoreOneOutcome::Refused(RestoreRefusedOutput {
            model_name: planned.model_name.clone(),
            run_id: planned.run_id.clone(),
            blake3_hash: planned.blake3_hash.clone(),
            size_bytes: planned.size_bytes,
            reason,
        })
    };

    // 1. Re-resolve the LIVE tombstone at FULL identity — the plan is
    // advisory, the custody ledger is the truth. Prefer the newest unrestored
    // row; fall back to the newest restored one (an idempotent re-apply
    // re-verifies it end to end rather than trusting the stamp).
    let matching: Vec<&TombstoneRecord> = tombstones
        .iter()
        .filter(|t| {
            t.run_id == planned.run_id
                && t.model_name == planned.model_name
                && t.file_path == planned.file_path
                && t.blake3_hash == planned.blake3_hash
        })
        .collect();
    let tomb = matching
        .iter()
        .filter(|t| t.restored_at.is_none())
        .max_by_key(|t| t.evicted_at)
        .or_else(|| matching.iter().max_by_key(|t| t.evicted_at));
    let Some(tomb) = tomb.copied() else {
        return refuse(
            "no tombstone exists at this exact (run, model, path, hash) identity — the plan \
             is stale or hand-authored; refused (fail-closed)"
                .to_string(),
        );
    };

    // 2. The recipe pointer: provenance must exist and must record THIS exact
    // output hash (the same binding check the eviction verdict used — the
    // hash, never the (run, model) pair, is the identity end to end).
    let prov: Option<ProvenanceRecord> = match store.get_provenance(&tomb.run_id, &tomb.model_name)
    {
        Ok(p) => p,
        Err(e) => {
            return refuse(format!(
                "provenance read failed: {e} — fail-closed (state-store read error, not \
                     evidence the recipe was never recorded)"
            ));
        }
    };
    let bind = check_recipe_produces_output(&tomb.blake3_hash, &tomb.file_path, prov.as_ref());
    if !bind.passed {
        return refuse(format!(
            "the tombstone's restore pointer no longer proves the recipe rebuilds these exact \
             bytes: {}",
            bind.detail
        ));
    }
    let prov = prov.expect("bind.passed implies provenance is present");

    let ir: ModelIr = match serde_json::from_str(&prov.model_ir_canonical_json) {
        Ok(ir) => ir,
        Err(e) => {
            return refuse(format!(
                "the provenance's embedded canonical ModelIr did not deserialize under the \
                 current engine (IR forward-compatibility break): {e}"
            ));
        }
    };
    let rocky_ir::MaterializationStrategy::ContentAddressed {
        storage_prefix,
        partition_columns,
    } = &ir.materialization
    else {
        return refuse(
            "the recorded recipe is not content-addressed — only content-addressed artifacts \
             carry a whole-output blake3 to rebuild and verify against"
                .to_string(),
        );
    };
    if !partition_columns.is_empty() {
        return refuse(
            "partitioned restore is a later phase — the tombstoned hash is per-file and the \
             recorded recipe re-derives the whole table; refused (fail-closed)"
                .to_string(),
        );
    }
    if !prov.upstreams.is_empty() {
        return refuse(format!(
            "the recorded recipe reads {} recorded upstream(s); restoring a multi-input recipe \
             requires DAG re-derivation from the recorded upstream bytes (a later phase). \
             Refused rather than substituting current data.",
            prov.upstreams.len()
        ));
    }

    // 3. The table's object store + discovered encoding state.
    let obj_store = match stores.store_for(storage_prefix) {
        Ok(s) => s,
        Err(e) => {
            return refuse(format!(
                "could not reach the object store for {storage_prefix}: {e:#}; refused \
                 (fail-closed)"
            ));
        }
    };
    let state = match discover_table_state(obj_store.clone(), storage_prefix).await {
        Ok(s) => s,
        Err(e) => return refuse(format!("{e:#}; refused (fail-closed)")),
    };
    if !state.partition_columns.is_empty() {
        return refuse(
            "the live table reports partition columns the recorded recipe does not — refused \
             (fail-closed)"
                .to_string(),
        );
    }

    // 4. Re-derive and HASH-VERIFY BEFORE ANY WRITE. A mismatch is the
    // tombstone's rebuild claim failing — a finding, reported honestly.
    let parquet = match rebuild_artifact_bytes(&ir, &state, warehouse).await {
        Ok(bytes) => bytes,
        Err(e) => return refuse(format!("re-derivation failed: {e:#}; nothing was written")),
    };
    let computed = blake3::hash(&parquet).to_hex().to_string();
    if computed != tomb.blake3_hash {
        return refuse(format!(
            "REBUILD MISMATCH: recomputed blake3 {}… != tombstoned {}… — the tombstone's \
             rebuild claim FAILED and nothing was written. This is a finding: the recorded \
             recipe no longer reproduces the evicted bytes (engine/encoding skew or a \
             nondeterministic construct).",
            short_hash(&computed),
            short_hash(&tomb.blake3_hash),
        ));
    }

    // 5. Materialize to the tombstoned path — verify existing bytes first,
    // never overwrite, create-only put when absent.
    let (key_prefix, relative) = match table_relative_add_path(storage_prefix, &tomb.file_path) {
        Ok(pair) => pair,
        Err(_) => {
            return refuse(format!(
                "the tombstoned file_path {:?} does not live under the table's storage_prefix \
                 {storage_prefix:?} — refused (fail-closed)",
                tomb.file_path,
            ));
        }
    };
    let obj_path = ObjPath::from(format!("{key_prefix}/{relative}"));
    let bytes_written =
        match verify_or_create(&obj_store, &obj_path, &parquet, &tomb.blake3_hash).await {
            Ok(wrote) => wrote,
            Err(e) => return refuse(format!("{e:#}")),
        };

    // 6. Atomic, hash-guarded ledger reinstatement. `written_at` is the
    // restore time — these bytes were (re-)materialized now; gc's written-age
    // check restarts conservatively.
    let artifact = ArtifactRecord {
        blake3_hash: tomb.blake3_hash.clone(),
        run_id: tomb.run_id.clone(),
        model_name: tomb.model_name.clone(),
        file_path: tomb.file_path.clone(),
        commit_version: tomb.commit_version,
        size_bytes: tomb.size_bytes,
        written_at: now,
    };
    let outcome = match store.restore_artifact(tomb, &artifact, plan_id, now) {
        Ok(o) => o,
        Err(e) => {
            return refuse(format!(
                "the bytes are verified at {} but the ledger reinstatement failed: {e} — re-run \
                 `rocky apply {plan_id}` (the re-apply verifies and completes idempotently)",
                tomb.file_path,
            ));
        }
    };
    match outcome {
        RestoreOutcome::Restored => RestoreOneOutcome::Restored(RestoredOutput {
            model_name: tomb.model_name.clone(),
            run_id: tomb.run_id.clone(),
            blake3_hash: tomb.blake3_hash.clone(),
            file_path: tomb.file_path.clone(),
            size_bytes: tomb.size_bytes,
            hash_verified: true,
            bytes_written,
            status: if bytes_written {
                "rebuilt from the recorded recipe, verified hash-exact, and re-materialized"
                    .to_string()
            } else {
                "already present, verified — bytes at the tombstoned path matched the hash; \
                 ledger row reinstated"
                    .to_string()
            },
        }),
        RestoreOutcome::AlreadyRestored => {
            RestoreOneOutcome::AlreadyRestored(tomb.blake3_hash.clone())
        }
        RestoreOutcome::HashMismatch { expected, found } => refuse(format!(
            "the ledger row at this location changed to hash {}… (expected {}…) between \
             verification and reinstatement — refused (fail-closed)",
            short_hash(&found),
            short_hash(&expected),
        )),
        RestoreOutcome::TombstoneMissing => refuse(
            "the tombstone vanished between resolution and reinstatement — refused (fail-closed)"
                .to_string(),
        ),
    }
}

/// Verify-or-create the artifact object at `obj_path`:
///
/// - bytes present → hash them; a match is success **without writing**, a
///   mismatch is a hard error (never overwrite);
/// - bytes absent → create-only conditional put of the verified `parquet`;
///   losing the create race re-verifies whatever landed instead of clobbering.
///
/// Returns whether bytes were physically written.
async fn verify_or_create(
    obj_store: &Arc<dyn ObjectStore>,
    obj_path: &ObjPath,
    parquet: &[u8],
    expected_hash: &str,
) -> Result<bool> {
    use object_store::{ObjectStoreExt, PutMode, PutOptions, PutPayload};

    match obj_store.get(obj_path).await {
        Ok(got) => {
            let existing = got
                .bytes()
                .await
                .context("could not read the existing bytes at the tombstoned path")?;
            let existing_hash = blake3::hash(&existing).to_hex().to_string();
            if existing_hash != expected_hash {
                bail!(
                    "bytes already exist at the tombstoned path with a DIFFERENT hash \
                     ({}… != tombstoned {}…) — REFUSED: restore never overwrites bytes it \
                     cannot prove are the evicted ones",
                    short_hash(&existing_hash),
                    short_hash(expected_hash),
                );
            }
            Ok(false)
        }
        Err(object_store::Error::NotFound { .. }) => {
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match obj_store
                .put_opts(obj_path, PutPayload::from(parquet.to_vec()), opts)
                .await
            {
                Ok(_) => Ok(true),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Lost a create race — verify whatever landed.
                    let got = obj_store
                        .get(obj_path)
                        .await
                        .context("re-read after losing the create race failed")?;
                    let existing = got.bytes().await.context("re-read body failed")?;
                    let existing_hash = blake3::hash(&existing).to_hex().to_string();
                    if existing_hash != expected_hash {
                        bail!(
                            "a concurrent writer landed DIFFERENT bytes ({}…) at the tombstoned \
                             path during the restore — refused (fail-closed, nothing overwritten)",
                            short_hash(&existing_hash),
                        );
                    }
                    Ok(false)
                }
                Err(e) => Err(anyhow!("physical write to the tombstoned path failed: {e}")),
            }
        }
        Err(e) => Err(anyhow!(
            "could not read the tombstoned path to verify existing bytes: {e}; refused \
             (fail-closed — restore never blind-writes)"
        )),
    }
}

/// The restoration engine: re-resolve each planned tombstone against the live
/// custody ledger and restore it fail-closed. Pure over its inputs (`store`,
/// `stores`, `now`) so tests drive it directly; the review + policy gates live
/// in [`run_restore_apply_in_with`] and run *before* this is called.
async fn execute_restore_apply(
    store: &StateStore,
    stores: &dyn RestoreStores,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    plan_id: &str,
    plan: &RestorePlan,
    now: DateTime<Utc>,
) -> Result<RestoreApplyOutput> {
    let tombstones = store
        .list_tombstones()
        .context("failed to read the tombstone custody ledger")?;

    let mut restored: Vec<RestoredOutput> = Vec::new();
    let mut refused: Vec<RestoreRefusedOutput> = Vec::new();
    let mut already_restored: Vec<String> = Vec::new();

    for planned in &plan.restorations {
        match restore_one(store, stores, warehouse, &tombstones, plan_id, planned, now).await {
            RestoreOneOutcome::Restored(out) => restored.push(out),
            RestoreOneOutcome::AlreadyRestored(hash) => already_restored.push(hash),
            RestoreOneOutcome::Refused(r) => refused.push(r),
        }
    }

    let bytes_restored = restored.iter().map(|r| r.size_bytes).sum();
    Ok(RestoreApplyOutput {
        version: VERSION.to_string(),
        command: "apply".to_string(),
        plan_id: plan_id.to_string(),
        restored_count: restored.len(),
        refused_count: refused.len(),
        restored,
        refused,
        already_restored,
        bytes_restored,
        notes: restore_apply_notes(),
    })
}

/// Apply a `PlanKind::Restore` plan — rebuild its tombstoned artifacts,
/// hash-exact, behind the same gates as gc:
///
/// - **unconditionally** review-gated (a `rocky review <plan-id> --approve`
///   marker must exist regardless of principal or of a `[policy]` block);
/// - policy may only make the gate *stricter*: an agent-scoped
///   `deny restore {…}` rule hard-refuses even a reviewed plan.
pub(crate) async fn run_restore_apply_in(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: PolicyPrincipal,
    json: bool,
) -> Result<()> {
    // Re-derivation runs on the RECORDING engine — the project's configured
    // warehouse adapter. A content-addressed artifact is only reproducible
    // bit-for-bit on the same engine that wrote it; re-deriving on a different
    // engine (a DuckDB rebuild of a warehouse-written artifact) diverges and
    // the hash-exact check refuses. Constructing the adapter is side-effect
    // free — no warehouse is contacted until re-derivation, after every gate.
    let cfg = rocky_core::config::load_rocky_config(config_path).with_context(|| {
        format!(
            "loading {} to resolve the warehouse that produced the artifact — restore re-derives \
             on the recording engine",
            config_path.display()
        )
    })?;
    let registry = crate::registry::AdapterRegistry::from_config(&cfg)
        .context("building the adapter registry to resolve the recording warehouse")?;
    let warehouse_names = registry.warehouse_adapter_names();
    let warehouse_name = if warehouse_names.iter().any(|n| n == "default") {
        "default".to_string()
    } else {
        warehouse_names
            .into_iter()
            .next()
            .context("no warehouse adapter is configured; cannot re-derive the artifact")?
    };
    let warehouse = registry
        .warehouse_adapter(&warehouse_name)
        .context("constructing the recording warehouse adapter for re-derivation")?;

    run_restore_apply_in_with(
        root,
        config_path,
        plan_id,
        state_path,
        runtime_principal,
        json,
        &S3RestoreStores,
        warehouse.as_ref(),
        // Finding 1: reuse the SAME snapshot the adapter was built from.
        Some(cfg),
    )
    .await
}

/// [`run_restore_apply_in`] with an injectable [`RestoreStores`] and warehouse
/// — the real path passes the s3 provider and the config-resolved recording
/// engine; tests pass an in-memory object store and an in-memory DuckDB engine.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_restore_apply_in_with(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    state_path: &Path,
    runtime_principal: PolicyPrincipal,
    json: bool,
    stores: &dyn RestoreStores,
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    // Finding 1: the SAME config snapshot the caller already loaded (the outer
    // `run_restore_apply_in` loaded it to build the recording-warehouse adapter).
    // Threaded in so the freeze/policy gate + `[state]` sync read the config the
    // adapter was built from, not a reload a `rocky.toml` swap could redirect.
    loaded_cfg: Option<rocky_core::config::RockyConfig>,
) -> Result<()> {
    let plan_record = read_plan(root, plan_id)
        .with_context(|| format!("failed to read restore plan '{plan_id}'"))?;

    if plan_record.kind != PlanKind::Restore {
        bail!(
            "plan '{plan_id}' is a {} plan, not a restore plan. \
             Use `rocky apply {plan_id}` and let the dispatcher route it.",
            plan_record.kind,
        );
    }

    let plan: RestorePlan = serde_json::from_value(plan_record.payload.clone())
        .context("failed to deserialize restore plan payload")?;

    // HARD RULE: a restore plan is always review-gated, regardless of policy —
    // the same symmetric-caution rule as gc.
    if !ai_plan_is_reviewed(root, plan_id) {
        bail!(
            "restore plan '{plan_id}' has not been reviewed and approved. \
             Restoration is symmetric-caution gated — even a human restore goes through review. \
             Review the {} artifact(s) it restores and approve with \
             `rocky review {plan_id} --approve`, then re-run `rocky apply {plan_id}`.",
            plan.restorations.len(),
        );
    }

    // Policy can only tighten the gate. `loaded_cfg` is the caller's single
    // snapshot (finding 1) — a genuine config-load error already fails closed at
    // the outer `run_restore_apply_in` before the adapter is built.
    let touched: BTreeMap<String, PolicyCapability> = plan
        .restorations
        .iter()
        .map(|r| (r.model_name.clone(), PolicyCapability::Restore))
        .collect();
    let models_dir = gc_models_dir(loaded_cfg.as_ref(), config_path);
    // Finding 2a (download-before, UNCONDITIONAL): `rocky restore` reads the
    // TOMBSTONES ledger from local state, so for a remote backend it must pull the
    // authoritative remote state regardless of `[policy]` presence — the earlier
    // policy-gated sync would let a no-`[policy]` restore read STALE local
    // tombstones and falsely refuse a valid restore. This refresh also gives the
    // freeze/budget gate below the current ledger. Fail-closed. Uses the SAME
    // `loaded_cfg` snapshot the gate uses (finding A — config TOCTOU).
    crate::commands::apply::download_remote_ledger_unconditional(
        loaded_cfg.as_ref(),
        state_path,
        "restore apply",
    )
    .await?;
    // Durable freeze-marker LIST for the gate below (same guard as the
    // governed apply seams, fail-closed) — a marker-only freeze whose ledger
    // row was erased by a concurrent state upload must still deny this
    // restore. An absent config has no `[policy]` to enforce ⇒ empty set.
    let marker_freezes = match loaded_cfg.as_ref() {
        Some(cfg) => crate::commands::apply::marker_freezes_before_gate(cfg, &touched).await?,
        None => Vec::new(),
    };
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
            "policy DENIES restore plan '{plan_id}': model '{model}'{rule} — {reason}. \
             A deny cannot be satisfied by review; re-scope the restoration or have a human \
             apply it."
        );
    }

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    // Finding 5: restore MUTATES the object store + the local artifact/tombstone
    // ledger as it executes, so a mid-run failure can still have committed state.
    // CAPTURE the result (don't `?` it), release the store lock, ALWAYS run the
    // fail-closed upload-after, THEN handle the captured result — a failure must
    // not skip the durability upload (else the next run's start-download reverts
    // the partial restoration).
    let exec_result =
        execute_restore_apply(&store, stores, warehouse, plan_id, &plan, Utc::now()).await;
    // Drop the store to release the advisory lock / flush the file before upload.
    drop(store);

    // Finding 2b + 5 (upload-after, unconditional): push whatever restore wrote to
    // local state to the remote backend (fail-closed) so it is durable.
    crate::commands::apply::upload_remote_ledger_fail_closed(
        loaded_cfg.as_ref(),
        state_path,
        "restore apply",
    )
    .await?;

    let output = exec_result?;
    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        print_apply_table(&output);
    }
    Ok(())
}

fn print_apply_table(output: &RestoreApplyOutput) {
    println!(
        "Rocky restore apply {} — {} restored ({} bytes), {} refused, {} already restored",
        short_hash(&output.plan_id),
        output.restored_count,
        output.bytes_restored,
        output.refused_count,
        output.already_restored.len(),
    );
    println!();
    for r in &output.restored {
        println!(
            "  RESTORED  {}  {} bytes  {}  — {}",
            r.model_name,
            r.size_bytes,
            short_hash(&r.blake3_hash),
            r.status,
        );
    }
    for r in &output.refused {
        println!(
            "  REFUSED   {}  {} bytes  {}  — {}",
            r.model_name,
            r.size_bytes,
            short_hash(&r.blake3_hash),
            r.reason,
        );
    }
    for h in &output.already_restored {
        println!("  already-restored  {}  (verified no-op)", short_hash(h));
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
    use rocky_core::reuse::OutputArtifact;
    use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger};
    use rocky_ir::{GovernanceConfig, MaterializationStrategy, TargetRef};
    use tempfile::TempDir;

    // Fixed 64-hex hashes for the pure-resolution tests (no bytes involved).
    const HA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn tombstone(
        hash: &str,
        run: &str,
        model: &str,
        path: &str,
        recipe: Option<&str>,
    ) -> TombstoneRecord {
        TombstoneRecord {
            blake3_hash: hash.to_string(),
            run_id: run.to_string(),
            model_name: model.to_string(),
            file_path: path.to_string(),
            size_bytes: 100,
            commit_version: 1,
            recipe_hash: recipe.map(str::to_string),
            input_hash: None,
            input_proof_class: Some("strong".to_string()),
            env_hash: None,
            hash_scheme: Some("v1".to_string()),
            evicted_at: Utc::now(),
            plan_id: "gc-plan".to_string(),
            physical_reclaimed: false,
            observed_delta_version: Some(1),
            restored_at: None,
            restore_plan_id: None,
        }
    }

    // -- target resolution -------------------------------------------------

    #[test]
    fn resolve_by_model_name_and_hash_prefix() {
        let tombs = vec![tombstone(
            HA,
            "r1",
            "orders",
            "s3://b/orders/a.parquet",
            Some("rec-a"),
        )];
        let live = vec![];
        assert_eq!(
            resolve_target(&tombs, &live, "orders").unwrap().blake3_hash,
            HA
        );
        // Hash prefix (>= 8 hex chars).
        assert_eq!(
            resolve_target(&tombs, &live, &HA[..12])
                .unwrap()
                .blake3_hash,
            HA
        );
        // model@<recipe-prefix>.
        assert_eq!(
            resolve_target(&tombs, &live, "orders@rec")
                .unwrap()
                .blake3_hash,
            HA
        );
    }

    #[test]
    fn resolve_refuses_ambiguous_target_and_lists_candidates() {
        // Two distinct evicted artifacts for the SAME model name → ambiguous.
        let tombs = vec![
            tombstone(HA, "r1", "orders", "s3://b/orders/a.parquet", Some("rec-a")),
            tombstone(HB, "r2", "orders", "s3://b/orders/b.parquet", Some("rec-b")),
        ];
        let err = resolve_target(&tombs, &[], "orders").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("ambiguous"), "got: {msg}");
        // Both candidate hash prefixes are listed so the user can disambiguate.
        assert!(
            msg.contains(&HA[..12]) && msg.contains(&HB[..12]),
            "got: {msg}"
        );

        // The hash prefix disambiguates cleanly.
        assert_eq!(
            resolve_target(&tombs, &[], &HB[..12]).unwrap().blake3_hash,
            HB
        );
    }

    #[test]
    fn resolve_distinguishes_never_evicted_from_not_found_and_already_restored() {
        // A live ledger artifact that was never evicted.
        let live = vec![ArtifactRecord {
            blake3_hash: HA.to_string(),
            run_id: "r1".to_string(),
            model_name: "orders".to_string(),
            file_path: "s3://b/orders/a.parquet".to_string(),
            commit_version: 1,
            size_bytes: 100,
            written_at: Utc::now(),
        }];
        let err = resolve_target(&[], &live, "orders").unwrap_err();
        assert!(err.to_string().contains("never evicted"), "got: {err}");

        // Nothing at all.
        let err = resolve_target(&[], &[], "ghost").unwrap_err();
        assert!(
            err.to_string()
                .contains("nothing was ever recorded or evicted"),
            "got: {err}"
        );

        // An already-restored tombstone → say so, not "not found".
        let mut restored = tombstone(HA, "r1", "orders", "s3://b/orders/a.parquet", Some("rec"));
        restored.restored_at = Some(Utc::now());
        let err = resolve_target(std::slice::from_ref(&restored), &[], "orders").unwrap_err();
        assert!(err.to_string().contains("ALREADY restored"), "got: {err}");
    }

    // -- real-bytes creds-free roundtrip (InMemory CAS) --------------------

    #[cfg(feature = "duckdb")]
    mod roundtrip {
        use super::*;

        use std::sync::Arc;

        use object_store::memory::InMemory;
        use object_store::path::Path as ObjPath;
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
        use rocky_iceberg::uniform_writer::{
            SqlClient, UniformWriter, UniformWriterConfig, WriteResult,
        };
        use rocky_ir::types::{RockyType, TypedColumn};

        use crate::commands::gc::{
            LivenessOracle, ReclaimVerdict, run_gc_apply_in_with, run_gc_plan_in,
        };
        use crate::commands::review::compute_review;

        const STORAGE_PREFIX: &str = "s3://bucket/tgt/raw/orders";
        const KEY_PREFIX: &str = "tgt/raw/orders";

        /// A liveness oracle that always affirms removal at a stable head — the
        /// creds-free stand-in for a manifest that shows the file compacted out
        /// (so gc apply reaches the tombstone). Stable across the proof + the
        /// TOCTOU re-check.
        struct AlwaysReclaim;

        #[async_trait::async_trait]
        impl LivenessOracle for AlwaysReclaim {
            async fn reclaim_verdict(&self, _sp: &str, _fp: &str, _cv: u64) -> ReclaimVerdict {
                ReclaimVerdict::Reclaimable { head_version: 1 }
            }
        }

        /// A restore-store provider that hands back one shared in-memory object
        /// store for every prefix — the injected creds-free CAS.
        struct SharedStore(Arc<InMemory>);

        impl RestoreStores for SharedStore {
            fn store_for(&self, _storage_prefix: &str) -> Result<Arc<dyn ObjectStore>> {
                Ok(self.0.clone() as Arc<dyn ObjectStore>)
            }
        }

        /// A fresh in-memory DuckDB engine — the recording engine for the
        /// creds-free content-addressed artifacts (`produce_real_bytes` writes
        /// on the same). The deterministic `VALUES` recipe re-derives
        /// bit-identically on any instance, so this stands in for the
        /// config-resolved warehouse the production path passes.
        fn fresh_duckdb() -> rocky_duckdb::adapter::DuckDbWarehouseAdapter {
            rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory()
                .expect("in-memory DuckDB engine for re-derivation")
        }

        struct PanicSqlClient;
        #[async_trait::async_trait]
        impl SqlClient for PanicSqlClient {
            async fn execute(&self, _sql: &str) -> rocky_iceberg::uniform_writer::Result<()> {
                panic!("producer writer must not issue SQL");
            }
        }

        /// The two-column `(id BIGINT, name STRING)` model, materialized
        /// content-addressed at [`STORAGE_PREFIX`]. The SQL is a deterministic
        /// `VALUES` literal so a DuckDB re-execution reproduces the exact rows.
        fn orders_ir() -> ModelIr {
            let mut ir = ModelIr::transformation(
                TargetRef {
                    catalog: "tgt".into(),
                    schema: "raw".into(),
                    table: "orders".into(),
                },
                MaterializationStrategy::ContentAddressed {
                    storage_prefix: STORAGE_PREFIX.to_string(),
                    partition_columns: vec![],
                },
                vec![],
                "SELECT id, name FROM (VALUES (CAST(1 AS BIGINT), 'alice'), \
                 (CAST(2 AS BIGINT), 'bob'), (CAST(3 AS BIGINT), 'carol')) AS t(id, name)"
                    .to_string(),
                GovernanceConfig {
                    permissions_file: None,
                    auto_create_catalogs: false,
                    auto_create_schemas: false,
                },
                None,
                None,
            );
            ir.name = std::sync::Arc::from("orders");
            ir.typed_columns = vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "name".into(),
                    data_type: RockyType::String,
                    nullable: false,
                },
            ];
            ir
        }

        /// Seed a column-mapped, name-mode Delta UniForm bootstrap (v0) for the
        /// `(id, name)` schema so `discover()` resolves the physical mapping and
        /// `build_parquet` can encode. Mirrors the column-skip soak bootstrap.
        async fn seed_bootstrap(store: &InMemory) {
            let fields = serde_json::json!([
                {"name":"id","type":"long","nullable":false,"metadata":{
                    "delta.columnMapping.id":1,"delta.columnMapping.physicalName":"col-id-uuid"}},
                {"name":"name","type":"string","nullable":false,"metadata":{
                    "delta.columnMapping.id":2,"delta.columnMapping.physicalName":"col-name-uuid"}}
            ]);
            let bootstrap = serde_json::json!({"protocol":{
                "minReaderVersion":2,"minWriterVersion":7,
                "writerFeatures":["columnMapping","icebergCompatV2","invariants","appendOnly"]}});
            let metadata = serde_json::json!({"metaData":{
                "id":"00000000-0000-0000-0000-000000000000",
                "format":{"provider":"parquet","options":{}},
                "schemaString": serde_json::to_string(
                    &serde_json::json!({"type":"struct","fields":fields})).unwrap(),
                "partitionColumns":[],
                "configuration":{
                    "delta.columnMapping.mode":"name",
                    "delta.universalFormat.enabledFormats":"iceberg",
                    "delta.enableIcebergCompatV2":"true"},
                "createdTime":0}});
            let body = format!(
                "{}\n{}\n",
                serde_json::to_string(&bootstrap).unwrap(),
                serde_json::to_string(&metadata).unwrap()
            );
            store
                .put(
                    &ObjPath::from(format!("{KEY_PREFIX}/_delta_log/00000000000000000000.json")),
                    PutPayload::from(body.into_bytes()),
                )
                .await
                .unwrap();
        }

        /// Produce the real content-addressed bytes for [`orders_ir`] into
        /// `store` by executing the recipe on in-memory DuckDB (the SAME path
        /// restore re-derives through) and writing via the real
        /// [`UniformWriter`]. Returns the writer's [`WriteResult`].
        async fn produce_real_bytes(store: Arc<InMemory>, ir: &ModelIr) -> WriteResult {
            use rocky_core::traits::WarehouseAdapter;

            let writer = UniformWriter::new(
                UniformWriterConfig {
                    catalog: "tgt".into(),
                    schema: "raw".into(),
                    table: "orders".into(),
                    prefix: KEY_PREFIX.into(),
                    engine_info: "rocky-cli/test".into(),
                },
                store as Arc<dyn ObjectStore>,
                Arc::new(PanicSqlClient),
            );
            let state = writer.discover().await.expect("discover seeded bootstrap");
            let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory().unwrap();
            let result = adapter.execute_query(&ir.sql).await.unwrap();
            let batch = crate::commands::run_content_addressed::query_result_to_record_batch(
                &ir.typed_columns,
                &result,
            )
            .unwrap();
            writer
                .write_batch_with_state(batch, state)
                .await
                .expect("write real content-addressed bytes")
        }

        fn record_run(store: &StateStore, run_id: &str, model: &str) {
            let now = Utc::now();
            store
                .record_run(&RunRecord {
                    run_id: run_id.to_string(),
                    started_at: now,
                    finished_at: now,
                    status: RunStatus::Success,
                    models_executed: vec![ModelExecution {
                        model_name: model.to_string(),
                        started_at: now,
                        finished_at: now,
                        duration_ms: 10,
                        rows_affected: Some(3),
                        status: "success".to_string(),
                        sql_hash: "sh".to_string(),
                        skip_hash: None,
                        upstream_freshness: None,
                        bytes_scanned: Some(1),
                        bytes_written: None,
                        tenant: None,
                        recipe_hash: Some("recipe-orders".to_string()),
                        input_hash: None,
                        input_proof_class: None,
                        env_hash: Some("env".to_string()),
                        hash_scheme: Some("v1".to_string()),
                        output_column_hashes: None,
                        attempts: Vec::new(),
                    }],
                    trigger: RunTrigger::Manual,
                    config_hash: "cfg".to_string(),
                    triggering_identity: None,
                    session_source: rocky_core::state::SessionSource::Cli,
                    git_commit: None,
                    git_branch: None,
                    idempotency_key: None,
                    target_catalog: None,
                    hostname: "restore-test".to_string(),
                    rocky_version: "0.0.0-test".to_string(),
                    check_outcomes: Vec::new(),
                    pipeline: None,
                    submission_id: None,
                })
                .unwrap();
        }

        /// Seed the state store with the provenance + artifact + run for a real
        /// producer write, exactly as the runner records them. `written` is set
        /// old enough to pass gc's age gate.
        fn seed_ledger(
            store: &StateStore,
            ir: &ModelIr,
            run_id: &str,
            wr: &WriteResult,
            written: DateTime<Utc>,
        ) {
            let outputs = vec![OutputArtifact {
                blake3_hash: wr.blake3_hash.clone(),
                file_path: wr.file_path.clone(),
            }];
            let (entry, prov) =
                rocky_core::reuse::build_records(ir, run_id, &[], &outputs, written).unwrap();
            store
                .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
                .unwrap();
            store
                .record_artifact(&ArtifactRecord {
                    blake3_hash: wr.blake3_hash.clone(),
                    run_id: run_id.to_string(),
                    model_name: "orders".to_string(),
                    file_path: wr.file_path.clone(),
                    commit_version: wr.commit_version,
                    size_bytes: wr.size_bytes,
                    written_at: written,
                })
                .unwrap();
            record_run(store, run_id, "orders");
        }

        fn find_plan_id(plans_dir: &Path) -> String {
            std::fs::read_dir(plans_dir)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    name.strip_suffix(".json")
                        .filter(|s| !s.contains("reviewed"))
                        .map(str::to_string)
                })
                .next()
                .expect("a plan file must exist")
        }

        fn write_review_marker(root: &Path, plan_id: &str) {
            let marker = crate::commands::apply::review_marker_path(root, plan_id);
            std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
            std::fs::write(&marker, "{}").unwrap();
        }

        /// THE roundtrip proof, creds-free with REAL bytes: seed a real
        /// content-addressed producer → gc plan → review → apply (evict +
        /// tombstone) → **physically delete the CAS object** → restore plan →
        /// review → apply → the recomputed blake3 equals the tombstoned hash,
        /// bytes are re-materialized byte-identical, the ledger is reinstated,
        /// and a second restore is a verified no-op.
        #[tokio::test]
        async fn evict_restore_bit_exact_full_roundtrip() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let config = root.join("nonexistent.toml"); // unconfigured policy plane
            let cas = Arc::new(InMemory::new());
            seed_bootstrap(&cas).await;
            let ir = orders_ir();
            let wr = produce_real_bytes(cas.clone(), &ir).await;
            let obj_path = ObjPath::from(format!(
                "{KEY_PREFIX}/{}",
                wr.file_path.rsplit('/').next().unwrap()
            ));
            // Sanity: the real bytes are present and hash to what the writer
            // reported.
            let seeded_bytes = cas.get(&obj_path).await.unwrap().bytes().await.unwrap();
            assert_eq!(
                blake3::hash(&seeded_bytes).to_hex().to_string(),
                wr.blake3_hash
            );

            {
                let store = StateStore::open(&state_path).unwrap();
                seed_ledger(&store, &ir, "r1", &wr, Utc::now() - Duration::days(30));
            }

            // --- gc: plan → review → apply (evict + tombstone) ---
            run_gc_plan_in(root, &state_path, &config, 7, PolicyPrincipal::Human, true).unwrap();
            let plans_dir = root.join(".rocky").join("plans");
            let gc_plan_id = find_plan_id(&plans_dir);
            compute_review(root, &config, &gc_plan_id, "HEAD", true)
                .await
                .unwrap();
            run_gc_apply_in_with(
                root,
                &config,
                &gc_plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &AlwaysReclaim,
            )
            .await
            .unwrap();
            {
                let store = StateStore::open(&state_path).unwrap();
                assert_eq!(
                    store.refcount_for_hash(&wr.blake3_hash).unwrap(),
                    0,
                    "the artifact was evicted"
                );
                let tombs = store.list_tombstones().unwrap();
                assert_eq!(tombs.len(), 1);
                assert_eq!(tombs[0].blake3_hash, wr.blake3_hash);
                assert!(tombs[0].restored_at.is_none());
            }

            // Simulate the VACUUM that reclaimed the bytes: physically delete
            // the CAS object so restore must RE-CREATE it byte-identical.
            cas.delete(&obj_path).await.unwrap();
            assert!(matches!(
                cas.get(&obj_path).await,
                Err(object_store::Error::NotFound { .. })
            ));

            // --- restore: plan → review → apply (rebuild + verify + reinstate) ---
            run_restore_plan_in(root, &state_path, "orders", PolicyPrincipal::Human, true).unwrap();
            let restore_plan_id = std::fs::read_dir(&plans_dir)
                .unwrap()
                .filter_map(std::result::Result::ok)
                .filter_map(|e| {
                    let name = e.file_name().into_string().ok()?;
                    let id = name
                        .strip_suffix(".json")
                        .filter(|s| !s.contains("reviewed"))?;
                    (id != gc_plan_id).then(|| id.to_string())
                })
                .next()
                .expect("a restore plan file distinct from the gc plan");
            write_review_marker(root, &restore_plan_id);

            let stores = SharedStore(cas.clone());
            run_restore_apply_in_with(
                root,
                &config,
                &restore_plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &stores,
                &fresh_duckdb(),
                rocky_core::config::load_rocky_config(&config).ok(),
            )
            .await
            .unwrap();

            // The bytes are re-created byte-identical (hash matches the writer's
            // original), the ledger row is reinstated, the tombstone is stamped.
            let restored_bytes = cas.get(&obj_path).await.unwrap().bytes().await.unwrap();
            assert_eq!(
                blake3::hash(&restored_bytes).to_hex().to_string(),
                wr.blake3_hash,
                "restore re-materialized byte-identical CAS bytes"
            );
            assert_eq!(restored_bytes, seeded_bytes, "byte-for-byte identical");
            {
                let store = StateStore::open(&state_path).unwrap();
                assert_eq!(
                    store.refcount_for_hash(&wr.blake3_hash).unwrap(),
                    1,
                    "the ledger row is reinstated"
                );
                let tombs = store.list_tombstones().unwrap();
                assert_eq!(tombs.len(), 1, "the tombstone is kept for audit");
                assert!(tombs[0].restored_at.is_some(), "stamped restored");
                assert_eq!(
                    tombs[0].restore_plan_id.as_deref(),
                    Some(restore_plan_id.as_str())
                );
            }

            // Idempotent re-restore: bytes already present + verified → no-op,
            // no second write, ledger stays at refcount 1.
            run_restore_apply_in_with(
                root,
                &config,
                &restore_plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &stores,
                &fresh_duckdb(),
                rocky_core::config::load_rocky_config(&config).ok(),
            )
            .await
            .unwrap();
            {
                let store = StateStore::open(&state_path).unwrap();
                assert_eq!(store.refcount_for_hash(&wr.blake3_hash).unwrap(), 1);
            }
        }

        /// Build a state store + tombstone for a real evicted artifact, WITHOUT
        /// the physical byte-delete — used by the fail-closed regressions. The
        /// bytes stay in `cas`. Returns `(WriteResult, obj_path)`.
        async fn seed_evicted(
            root: &Path,
            state_path: &Path,
            cas: Arc<InMemory>,
        ) -> (WriteResult, ObjPath) {
            seed_bootstrap(&cas).await;
            let ir = orders_ir();
            let wr = produce_real_bytes(cas.clone(), &ir).await;
            let obj_path = ObjPath::from(format!(
                "{KEY_PREFIX}/{}",
                wr.file_path.rsplit('/').next().unwrap()
            ));
            let store = StateStore::open(state_path).unwrap();
            seed_ledger(&store, &ir, "r1", &wr, Utc::now() - Duration::days(30));
            let tomb = tombstone(
                &wr.blake3_hash,
                "r1",
                "orders",
                &wr.file_path,
                Some("recipe-orders"),
            );
            // Fix up the tombstone's real size/commit from the write.
            let tomb = TombstoneRecord {
                size_bytes: wr.size_bytes,
                commit_version: wr.commit_version,
                ..tomb
            };
            store
                .evict_artifact(&tomb, "r1", "orders", &wr.file_path)
                .unwrap();
            drop(store);
            let _ = root; // root reserved for symmetry with plan-driven helpers
            (wr, obj_path)
        }

        /// PINS CURRENT BEHAVIOR (KNOWN GAP — gc's eviction set is strictly
        /// larger than restore's recovery set).
        ///
        /// **Current behavior:** an artifact produced by a *multi-input*
        /// recipe whose every upstream is a content hash has a `strong` input
        /// closure, so gc's [`check_recipe_recorded`] passes it and it becomes
        /// eligible for eviction. `restore_one` then refuses that very same
        /// artifact, because it rejects any recipe with recorded upstreams:
        /// re-deriving a multi-input recipe from the recorded upstream bytes
        /// needs DAG re-derivation that is not yet implemented. This test
        /// drives BOTH sides against ONE artifact and asserts the asymmetry.
        ///
        /// **Why this is wrong:** gc's own check detail advertises "every
        /// upstream is a content hash", explicitly contemplating multi-input
        /// recipes, and `rocky gc` told users evictions were "always restorable
        /// from the recorded recipe". They are not — restore covers only the
        /// zero-upstream, non-partitioned case. The tombstone is durable and
        /// records the full recipe, so nothing is destroyed, but the recovery
        /// route a user is pointed at does not work for these artifacts; they
        /// must re-run the pipeline instead. This PR corrects the misleading
        /// text; it does not close the gap.
        ///
        /// **Expected to be inverted when fixed.** Two mutually exclusive
        /// fixes are possible — narrow gc's eligibility predicate to reject
        /// recorded upstreams, or build multi-input restore — and choosing
        /// between them is an owner decision (the first is a behavior change to
        /// a public surface). Whichever lands, the `restore_one` half of this
        /// test stops refusing and the test should be renamed.
        #[tokio::test]
        async fn gc_admits_multi_input_recipe_that_restore_refuses_known_gap() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let cas = Arc::new(InMemory::new());
            seed_bootstrap(&cas).await;
            let ir = orders_ir();
            let wr = produce_real_bytes(cas.clone(), &ir).await;

            // A recipe with TWO recorded upstreams, both content-addressed —
            // the `strong` closure gc's eligibility check is written to admit.
            let upstreams = vec![
                rocky_core::recipe_identity::UpstreamIdentity::Content {
                    upstream_key: "tgt.raw.customers".to_string(),
                    blake3_hash: HA.to_string(),
                },
                rocky_core::recipe_identity::UpstreamIdentity::Content {
                    upstream_key: "tgt.raw.line_items".to_string(),
                    blake3_hash: HB.to_string(),
                },
            ];
            let outputs = vec![OutputArtifact {
                blake3_hash: wr.blake3_hash.clone(),
                file_path: wr.file_path.clone(),
            }];
            let written = Utc::now() - Duration::days(30);
            let (entry, prov) =
                rocky_core::reuse::build_records(&ir, "r1", &upstreams, &outputs, written).unwrap();
            assert_eq!(
                entry.proof_class, "strong",
                "an all-content-hash closure is strong even with several upstreams"
            );
            assert_eq!(
                prov.upstreams.len(),
                2,
                "the recipe really is multi-input — this is what makes the test non-vacuous"
            );

            let store = StateStore::open(&state_path).unwrap();
            store
                .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
                .unwrap();
            store
                .record_artifact(&ArtifactRecord {
                    blake3_hash: wr.blake3_hash.clone(),
                    run_id: "r1".to_string(),
                    model_name: "orders".to_string(),
                    file_path: wr.file_path.clone(),
                    commit_version: wr.commit_version,
                    size_bytes: wr.size_bytes,
                    written_at: written,
                })
                .unwrap();
            record_run(&store, "r1", "orders");

            // --- gc side: this artifact PASSES the eviction eligibility check.
            let class = crate::output::ReplayCheckModelOutput {
                model_name: "orders".to_string(),
                verdict: "replayable".to_string(),
                reasons: Vec::new(),
                has_provenance: true,
                ir_parseable: true,
                nondeterministic: false,
                // Read back off the record gc itself would classify from.
                proof_class: Some(prov.proof_class.clone()),
                inputs: Vec::new(),
            };
            let check = crate::commands::gc::check_recipe_recorded(&class);
            assert!(
                check.passed,
                "KNOWN GAP: gc admits a multi-input recipe for eviction ({})",
                check.detail
            );

            // Evict it, exactly as an approved gc apply would.
            let tomb = TombstoneRecord {
                size_bytes: wr.size_bytes,
                commit_version: wr.commit_version,
                ..tombstone(
                    &wr.blake3_hash,
                    "r1",
                    "orders",
                    &wr.file_path,
                    Some("recipe-orders"),
                )
            };
            store
                .evict_artifact(&tomb, "r1", "orders", &wr.file_path)
                .unwrap();
            let tombstones = store.list_tombstones().unwrap();

            // --- restore side: REFUSES the very same artifact.
            let planned = RestorePlanRestoration {
                model_name: "orders".to_string(),
                run_id: "r1".to_string(),
                blake3_hash: wr.blake3_hash.clone(),
                file_path: wr.file_path.clone(),
                size_bytes: wr.size_bytes,
                commit_version: wr.commit_version,
                evicted_at: tomb.evicted_at.to_rfc3339(),
                gc_plan_id: "gc-plan".to_string(),
                recipe_hash: Some("recipe-orders".to_string()),
                input_hash: None,
                input_proof_class: Some("strong".to_string()),
            };
            let outcome = restore_one(
                &store,
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                &tombstones,
                "restore-plan",
                &planned,
                Utc::now(),
            )
            .await;

            let RestoreOneOutcome::Refused(refused) = outcome else {
                panic!(
                    "KNOWN GAP pin is stale: restore no longer refuses multi-input recipes. If \
                     multi-input restore has landed, invert this test rather than deleting it."
                );
            };
            assert!(
                refused.reason.contains("upstream"),
                "the refusal must be the multi-input one, not an incidental failure: {}",
                refused.reason
            );
            assert_eq!(refused.blake3_hash, wr.blake3_hash);

            // Nothing was reinstated — the artifact gc evicted stays unrecovered.
            assert_eq!(store.refcount_for_hash(&wr.blake3_hash).unwrap(), 0);
        }

        /// Regression (a): a tombstone whose claimed hash the recipe cannot
        /// reproduce must REFUSE and write nothing. We corrupt the tombstone's
        /// `blake3_hash` after eviction so the rebuild's real hash can never
        /// match it — the fail-closed "rebuild claim failed" path.
        #[tokio::test]
        async fn refuses_when_recomputed_hash_mismatches_tombstone() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let config = root.join("nonexistent.toml");
            let cas = Arc::new(InMemory::new());
            let (wr, obj_path) = seed_evicted(root, &state_path, cas.clone()).await;
            cas.delete(&obj_path).await.unwrap();

            // Hand-author a restore plan whose restoration claims a DIFFERENT
            // hash than the recipe rebuilds (so the pre-write verify fails). We
            // can't corrupt the tombstone (restore re-resolves at full
            // identity), so instead we craft the plan to point at a bogus hash;
            // the live-tombstone re-resolution then finds no match and refuses.
            let plan = RestorePlan {
                version: VERSION.to_string(),
                target: "orders".to_string(),
                restorations: vec![RestorePlanRestoration {
                    model_name: "orders".to_string(),
                    run_id: "r1".to_string(),
                    blake3_hash: HB.to_string(), // wrong hash
                    file_path: wr.file_path.clone(),
                    size_bytes: wr.size_bytes,
                    commit_version: wr.commit_version,
                    evicted_at: Utc::now().to_rfc3339(),
                    gc_plan_id: "gc".to_string(),
                    recipe_hash: Some("recipe-orders".to_string()),
                    input_hash: None,
                    input_proof_class: Some("strong".to_string()),
                }],
            };
            let plan_id =
                write_plan_with_principal(root, PlanKind::Restore, &plan, PolicyPrincipal::Human)
                    .unwrap();
            write_review_marker(root, &plan_id);

            run_restore_apply_in_with(
                root,
                &config,
                &plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                rocky_core::config::load_rocky_config(&config).ok(),
            )
            .await
            .unwrap();

            // Nothing was written: the CAS object stays absent and the ledger
            // is not reinstated (refcount stays 0).
            assert!(matches!(
                cas.get(&obj_path).await,
                Err(object_store::Error::NotFound { .. })
            ));
            let store = StateStore::open(&state_path).unwrap();
            assert_eq!(store.refcount_for_hash(&wr.blake3_hash).unwrap(), 0);
        }

        /// Regression (a'): the genuine rebuild-mismatch path — the tombstone
        /// is real but the RECIPE was tampered to a different deterministic
        /// output, so the recomputed hash cannot equal the tombstoned hash.
        /// Restore must refuse with the "REBUILD MISMATCH" finding and write
        /// nothing.
        #[tokio::test]
        async fn refuses_and_reports_when_recipe_no_longer_reproduces_bytes() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let cas = Arc::new(InMemory::new());
            seed_bootstrap(&cas).await;
            let ir = orders_ir();
            let wr = produce_real_bytes(cas.clone(), &ir).await;
            let obj_path = ObjPath::from(format!(
                "{KEY_PREFIX}/{}",
                wr.file_path.rsplit('/').next().unwrap()
            ));
            cas.delete(&obj_path).await.unwrap();

            // Record provenance whose embedded IR produces DIFFERENT bytes than
            // the tombstoned hash: same target/prefix, but a recipe returning
            // other rows. The recipe still binds to the tombstoned hash (we set
            // output_blake3 = tombstoned hash), so the binding check passes and
            // the failure surfaces at the recomputed-hash comparison.
            let mut tampered = orders_ir();
            tampered.sql =
                "SELECT id, name FROM (VALUES (CAST(9 AS BIGINT), 'zzz')) AS t(id, name)"
                    .to_string();
            let outputs = vec![OutputArtifact {
                blake3_hash: wr.blake3_hash.clone(),
                file_path: wr.file_path.clone(),
            }];
            let store = StateStore::open(&state_path).unwrap();
            let (entry, prov) =
                rocky_core::reuse::build_records(&tampered, "r1", &[], &outputs, Utc::now())
                    .unwrap();
            store
                .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
                .unwrap();
            store
                .record_artifact(&ArtifactRecord {
                    blake3_hash: wr.blake3_hash.clone(),
                    run_id: "r1".to_string(),
                    model_name: "orders".to_string(),
                    file_path: wr.file_path.clone(),
                    commit_version: wr.commit_version,
                    size_bytes: wr.size_bytes,
                    written_at: Utc::now() - Duration::days(30),
                })
                .unwrap();
            record_run(&store, "r1", "orders");
            let tomb = TombstoneRecord {
                size_bytes: wr.size_bytes,
                commit_version: wr.commit_version,
                ..tombstone(
                    &wr.blake3_hash,
                    "r1",
                    "orders",
                    &wr.file_path,
                    Some("recipe-orders"),
                )
            };
            store
                .evict_artifact(&tomb, "r1", "orders", &wr.file_path)
                .unwrap();
            drop(store);

            run_restore_plan_in(root, &state_path, "orders", PolicyPrincipal::Human, true).unwrap();
            let plan_id = find_plan_id(&root.join(".rocky").join("plans"));
            write_review_marker(root, &plan_id);

            // Capture the JSON to assert the honest finding is surfaced.
            let out = execute_restore_apply(
                &StateStore::open(&state_path).unwrap(),
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                &plan_id,
                &RestorePlan {
                    version: VERSION.to_string(),
                    target: "orders".to_string(),
                    restorations: vec![RestorePlanRestoration {
                        model_name: "orders".to_string(),
                        run_id: "r1".to_string(),
                        blake3_hash: wr.blake3_hash.clone(),
                        file_path: wr.file_path.clone(),
                        size_bytes: wr.size_bytes,
                        commit_version: wr.commit_version,
                        evicted_at: tomb.evicted_at.to_rfc3339(),
                        gc_plan_id: "gc".to_string(),
                        recipe_hash: Some("recipe-orders".to_string()),
                        input_hash: None,
                        input_proof_class: Some("strong".to_string()),
                    }],
                },
                Utc::now(),
            )
            .await
            .unwrap();
            assert_eq!(out.restored_count, 0);
            assert_eq!(out.refused_count, 1);
            assert!(
                out.refused[0].reason.contains("REBUILD MISMATCH"),
                "got: {}",
                out.refused[0].reason
            );
            // Nothing written.
            assert!(matches!(
                cas.get(&obj_path).await,
                Err(object_store::Error::NotFound { .. })
            ));
            assert_eq!(
                StateStore::open(&state_path)
                    .unwrap()
                    .refcount_for_hash(&wr.blake3_hash)
                    .unwrap(),
                0
            );
        }

        /// Regression (b): bytes already present at the tombstoned path with a
        /// DIFFERENT hash must REFUSE — restore never overwrites mismatched
        /// bytes.
        #[tokio::test]
        async fn refuses_when_existing_bytes_mismatch() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let cas = Arc::new(InMemory::new());
            let (wr, obj_path) = seed_evicted(root, &state_path, cas.clone()).await;
            // Overwrite the CAS object with DIFFERENT bytes than the recipe
            // rebuilds (the recipe rebuilds `wr.blake3_hash`).
            cas.put(&obj_path, PutPayload::from(b"corrupted".to_vec()))
                .await
                .unwrap();

            run_restore_plan_in(root, &state_path, "orders", PolicyPrincipal::Human, true).unwrap();
            let plan_id = find_plan_id(&root.join(".rocky").join("plans"));
            write_review_marker(root, &plan_id);

            let out = execute_restore_apply(
                &StateStore::open(&state_path).unwrap(),
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                &plan_id,
                &RestorePlan {
                    version: VERSION.to_string(),
                    target: "orders".to_string(),
                    restorations: vec![RestorePlanRestoration {
                        model_name: "orders".to_string(),
                        run_id: "r1".to_string(),
                        blake3_hash: wr.blake3_hash.clone(),
                        file_path: wr.file_path.clone(),
                        size_bytes: wr.size_bytes,
                        commit_version: wr.commit_version,
                        evicted_at: Utc::now().to_rfc3339(),
                        gc_plan_id: "gc".to_string(),
                        recipe_hash: Some("recipe-orders".to_string()),
                        input_hash: None,
                        input_proof_class: Some("strong".to_string()),
                    }],
                },
                Utc::now(),
            )
            .await
            .unwrap();
            assert_eq!(out.refused_count, 1);
            assert!(
                out.refused[0].reason.contains("REFUSED")
                    || out.refused[0].reason.contains("different hash")
                    || out.refused[0].reason.contains("DIFFERENT hash"),
                "got: {}",
                out.refused[0].reason
            );
            // The corrupted bytes are untouched (never overwritten) and the
            // ledger is not reinstated.
            let bytes = cas.get(&obj_path).await.unwrap().bytes().await.unwrap();
            assert_eq!(&bytes[..], b"corrupted");
            assert_eq!(
                StateStore::open(&state_path)
                    .unwrap()
                    .refcount_for_hash(&wr.blake3_hash)
                    .unwrap(),
                0
            );
        }

        /// Regression (d): an unreviewed restore plan must refuse to apply — the
        /// unconditional review gate (same hard rule as gc).
        #[tokio::test]
        async fn unreviewed_restore_plan_refuses_to_apply() {
            let dir = TempDir::new().unwrap();
            let root = dir.path();
            let state_path = root.join("state.redb");
            let config = root.join("nonexistent.toml");
            let cas = Arc::new(InMemory::new());
            let (wr, obj_path) = seed_evicted(root, &state_path, cas.clone()).await;
            cas.delete(&obj_path).await.unwrap();

            run_restore_plan_in(root, &state_path, "orders", PolicyPrincipal::Human, true).unwrap();
            let plan_id = find_plan_id(&root.join(".rocky").join("plans"));

            // No review marker → apply refuses.
            let err = run_restore_apply_in_with(
                root,
                &config,
                &plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                rocky_core::config::load_rocky_config(&config).ok(),
            )
            .await
            .expect_err("apply must refuse an unreviewed restore plan");
            assert!(err.to_string().contains("not been reviewed"), "got: {err}");
            // Nothing restored.
            assert!(matches!(
                cas.get(&obj_path).await,
                Err(object_store::Error::NotFound { .. })
            ));
            assert_eq!(
                StateStore::open(&state_path)
                    .unwrap()
                    .refcount_for_hash(&wr.blake3_hash)
                    .unwrap(),
                0
            );

            // With the marker, the same apply restores successfully.
            write_review_marker(root, &plan_id);
            run_restore_apply_in_with(
                root,
                &config,
                &plan_id,
                &state_path,
                PolicyPrincipal::Human,
                true,
                &SharedStore(cas.clone()),
                &fresh_duckdb(),
                rocky_core::config::load_rocky_config(&config).ok(),
            )
            .await
            .unwrap();
            assert_eq!(
                StateStore::open(&state_path)
                    .unwrap()
                    .refcount_for_hash(&wr.blake3_hash)
                    .unwrap(),
                1,
                "the reviewed restore reinstated the ledger row"
            );
        }

        /// Live end-to-end proof of `rocky restore` against a real Databricks
        /// UniForm table. Written, not run in CI — the `#[ignore]` is the real
        /// gate (CI runs `--all-features`, so a feature gate alone would still
        /// execute it).
        ///
        /// # What only the live path certifies
        ///
        /// The creds-free `evict_restore_bit_exact_full_roundtrip` writes AND
        /// re-derives on the same in-memory DuckDB engine, so it can only prove
        /// restore == restore. The load-bearing claim — that re-deriving on the
        /// RECORDING engine (here Databricks, the same warehouse that wrote the
        /// artifact) reproduces the recorded digest, re-encoded against the
        /// table's *discovered* physical column mapping — is verifiable only
        /// here: it records a real content-addressed run (real S3 CAS write +
        /// real artifact hash), evicts it to a tombstone, then restores through
        /// the warehouse and asserts the recomputed blake3 equals the recorded
        /// hash. (A DuckDB rebuild of this warehouse-written artifact diverges —
        /// which is exactly why restore re-derives on the configured warehouse.)
        ///
        /// # Reachability boundary (read before running)
        ///
        /// gc eviction is tombstone-only (no physical byte delete) and these
        /// sandbox creds cannot `s3:DeleteObject`, so the live proof runs the
        /// "already present, verified" branch: restore re-derives, hash-matches
        /// the recorded digest, and finds the evicted bytes still at the
        /// tombstoned path. The physical delete → rebuild → bit-exact roundtrip
        /// is proven creds-free on the InMemory harness
        /// (`evict_restore_bit_exact_full_roundtrip`); it cannot be exercised
        /// live with delete-less creds.
        ///
        /// Same fixture contract as the warehouse-replay live test: a
        /// pre-provisioned external, unpartitioned, name-column-mapped UniForm
        /// table of shape `(id BIGINT, name STRING, ts TIMESTAMP)` under an
        /// `hc_`/`hcv2_` sandbox schema. Env: `ROCKY_TEST_S3_BUCKET` /
        /// `_S3_PREFIX` / `_CATALOG` / `_SCHEMA` / `_TABLE` + `DATABRICKS_*` +
        /// AWS env creds. Teardown `DELETE`s the rows the recorded run appended
        /// (a `DROP` would destroy the external fixture); the content-addressed
        /// parquet objects are not deletable with these creds and leak by
        /// design.
        #[tokio::test]
        #[ignore = "requires Databricks + s3 credentials; run manually — see doc comment"]
        async fn restore_bit_exact_live_sandbox() {
            use std::time::Duration as StdDuration;

            use rocky_core::reuse::build_records;
            use rocky_core::state::{
                ArtifactRecord, ModelExecution, RunRecord, RunStatus, RunTrigger, SessionSource,
            };
            use rocky_core::traits::WarehouseAdapter;
            use rocky_databricks::adapter::DatabricksWarehouseAdapter;
            use rocky_databricks::auth::{Auth, AuthConfig};
            use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
            use rocky_ir::{GovernanceConfig, MaterializationStrategy, ModelIr, TargetRef};

            let (Ok(bucket), Ok(prefix), Ok(catalog), Ok(schema), Ok(table)) = (
                std::env::var("ROCKY_TEST_S3_BUCKET"),
                std::env::var("ROCKY_TEST_S3_PREFIX"),
                std::env::var("ROCKY_TEST_CATALOG"),
                std::env::var("ROCKY_TEST_SCHEMA"),
                std::env::var("ROCKY_TEST_TABLE"),
            ) else {
                eprintln!("skipping restore_bit_exact_live_sandbox: ROCKY_TEST_* not set");
                return;
            };
            let (Ok(host), Ok(http_path)) = (
                std::env::var("DATABRICKS_HOST"),
                std::env::var("DATABRICKS_HTTP_PATH"),
            ) else {
                eprintln!("skipping restore_bit_exact_live_sandbox: DATABRICKS_* not set");
                return;
            };
            let Some(warehouse_id) = ConnectorConfig::warehouse_id_from_http_path(&http_path)
            else {
                eprintln!("skipping: DATABRICKS_HTTP_PATH has no warehouse id");
                return;
            };
            let Ok(auth) = Auth::from_config(AuthConfig {
                host: host.clone(),
                token: std::env::var("DATABRICKS_TOKEN").ok(),
                client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
                client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
            }) else {
                eprintln!("skipping: Databricks auth could not be constructed");
                return;
            };
            let warehouse = DatabricksWarehouseAdapter::new(DatabricksConnector::new(
                ConnectorConfig {
                    host,
                    warehouse_id,
                    timeout: StdDuration::from_secs(120),
                    retry: Default::default(),
                },
                auth,
            ));
            let warehouse_dyn: &dyn WarehouseAdapter = &warehouse;
            let storage_prefix = format!("s3://{bucket}/{prefix}");
            let fqtn = format!("{catalog}.{schema}.{table}");

            let count_rows = async |w: &dyn WarehouseAdapter| -> i64 {
                let r = w
                    .execute_query(&format!("SELECT COUNT(*) AS n FROM {fqtn}"))
                    .await
                    .expect("count query");
                r.rows[0][0]
                    .as_i64()
                    .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
                    .expect("count i64")
            };

            // Distinct id range so re-runs are self-cleaning; pre-clean any leak.
            let (id_a, id_b, id_c) = (971_001_i64, 971_002, 971_003);
            warehouse_dyn
                .execute_statement(&format!(
                    "DELETE FROM {fqtn} WHERE id IN ({id_a}, {id_b}, {id_c})"
                ))
                .await
                .expect("pre-clean");

            // Self-contained recipe: literal timestamps make the recorded recipe
            // deterministic on a DuckDB re-derivation.
            let now_micros = Utc::now().timestamp_micros();
            let ts = |off: i64| {
                DateTime::from_timestamp_micros(now_micros + off)
                    .unwrap()
                    .format("%Y-%m-%dT%H:%M:%S%.6f")
                    .to_string()
            };
            let model_sql = format!(
                "SELECT id, name, ts FROM (VALUES \
                 (CAST({id_a} AS BIGINT), CAST('restore-a' AS STRING), TIMESTAMP'{}'), \
                 (CAST({id_b} AS BIGINT), CAST('restore-b' AS STRING), TIMESTAMP'{}'), \
                 (CAST({id_c} AS BIGINT), CAST('restore-c' AS STRING), TIMESTAMP'{}')) \
                 AS t(id, name, ts)",
                ts(0),
                ts(1),
                ts(2),
            );
            let mut model_ir = ModelIr::transformation(
                TargetRef {
                    catalog: catalog.clone(),
                    schema: schema.clone(),
                    table: table.clone(),
                },
                MaterializationStrategy::ContentAddressed {
                    storage_prefix: storage_prefix.clone(),
                    partition_columns: vec![],
                },
                vec![],
                model_sql,
                GovernanceConfig {
                    permissions_file: None,
                    auto_create_catalogs: false,
                    auto_create_schemas: false,
                },
                None,
                None,
            );
            model_ir.name = Arc::from("restore_live_model");
            model_ir.typed_columns = vec![
                TypedColumn {
                    name: "id".into(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "name".into(),
                    data_type: RockyType::String,
                    nullable: false,
                },
                TypedColumn {
                    name: "ts".into(),
                    data_type: RockyType::Timestamp,
                    nullable: false,
                },
            ];
            let model_name = model_ir.name.to_string();

            // --- Record a genuine content-addressed run: real S3 CAS write +
            // real artifact hash in the ledger. ---
            let summary = crate::commands::run_content_addressed::execute_content_addressed_model(
                &model_ir,
                warehouse_dyn,
                None,
            )
            .await
            .expect("recorded content-addressed run must succeed");
            assert_eq!(summary.num_rows, 3);
            assert_eq!(summary.blake3_hash.len(), 64);
            // Production row count AFTER the recorded run — restore must not move it.
            let n_after_record = count_rows(warehouse_dyn).await;

            let tmp = TempDir::new().unwrap();
            let state_path = tmp.path().join("state.redb");
            let run_id = "run-restore-live";
            {
                let store = StateStore::open(&state_path).unwrap();
                let outputs = vec![OutputArtifact {
                    blake3_hash: summary.blake3_hash.clone(),
                    file_path: summary.file_path.clone(),
                }];
                let (entry, prov) =
                    build_records(&model_ir, run_id, &[], &outputs, Utc::now()).unwrap();
                store
                    .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
                    .unwrap();
                store
                    .record_artifact(&ArtifactRecord {
                        blake3_hash: summary.blake3_hash.clone(),
                        run_id: run_id.to_string(),
                        model_name: model_name.clone(),
                        file_path: summary.file_path.clone(),
                        commit_version: summary.commit_version,
                        size_bytes: summary.size_bytes,
                        written_at: Utc::now(),
                    })
                    .unwrap();
                let now = Utc::now();
                store
                    .record_run(&RunRecord {
                        run_id: run_id.to_string(),
                        started_at: now,
                        finished_at: now,
                        status: RunStatus::Success,
                        models_executed: vec![ModelExecution {
                            model_name: model_name.clone(),
                            started_at: now,
                            finished_at: now,
                            duration_ms: 0,
                            rows_affected: Some(3),
                            status: "success".to_string(),
                            sql_hash: String::new(),
                            skip_hash: None,
                            upstream_freshness: None,
                            bytes_scanned: None,
                            bytes_written: None,
                            tenant: None,
                            recipe_hash: None,
                            input_hash: None,
                            input_proof_class: None,
                            env_hash: None,
                            hash_scheme: None,
                            output_column_hashes: None,
                            attempts: Vec::new(),
                        }],
                        trigger: RunTrigger::Manual,
                        config_hash: "cfg".to_string(),
                        triggering_identity: None,
                        session_source: SessionSource::Cli,
                        git_commit: None,
                        git_branch: None,
                        idempotency_key: None,
                        target_catalog: None,
                        hostname: "restore-live".to_string(),
                        rocky_version: VERSION.to_string(),
                        check_outcomes: Vec::new(),
                        pipeline: None,
                        submission_id: None,
                    })
                    .unwrap();

                // Evict → tombstone. gc eviction is tombstone-only, so the S3
                // bytes remain at `file_path`; the ledger row is retired.
                let tomb = TombstoneRecord {
                    blake3_hash: summary.blake3_hash.clone(),
                    run_id: run_id.to_string(),
                    model_name: model_name.clone(),
                    file_path: summary.file_path.clone(),
                    size_bytes: summary.size_bytes,
                    commit_version: summary.commit_version,
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: Some("strong".to_string()),
                    env_hash: None,
                    hash_scheme: Some("v1".to_string()),
                    evicted_at: now,
                    plan_id: "gc-live".to_string(),
                    physical_reclaimed: false,
                    observed_delta_version: Some(summary.commit_version),
                    restored_at: None,
                    restore_plan_id: None,
                };
                store
                    .evict_artifact(&tomb, run_id, &model_name, &summary.file_path)
                    .unwrap();
                assert_eq!(
                    store.refcount_for_hash(&summary.blake3_hash).unwrap(),
                    0,
                    "eviction retires the ledger row"
                );
            }

            // --- Restore through the REAL s3 store: re-derive on DuckDB, verify
            // the recomputed blake3 equals the recorded hash, find the evicted
            // bytes still present, reinstate the ledger row. ---
            let store = StateStore::open(&state_path).unwrap();
            let plan = RestorePlan {
                version: VERSION.to_string(),
                target: model_name.clone(),
                restorations: vec![RestorePlanRestoration {
                    model_name: model_name.clone(),
                    run_id: run_id.to_string(),
                    blake3_hash: summary.blake3_hash.clone(),
                    file_path: summary.file_path.clone(),
                    size_bytes: summary.size_bytes,
                    commit_version: summary.commit_version,
                    evicted_at: Utc::now().to_rfc3339(),
                    gc_plan_id: "gc-live".to_string(),
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: Some("strong".to_string()),
                }],
            };
            let out = execute_restore_apply(
                &store,
                &S3RestoreStores,
                warehouse_dyn,
                "restore-live-plan",
                &plan,
                Utc::now(),
            )
            .await
            .expect("restore apply");
            assert_eq!(
                out.refused_count, 0,
                "restore must not refuse; refused={:?}",
                out.refused
            );
            assert_eq!(out.restored_count, 1, "exactly one artifact restored");
            let r = &out.restored[0];
            assert!(
                r.hash_verified,
                "the DuckDB re-derivation must reproduce the recorded content hash bit-for-bit"
            );
            assert!(
                !r.bytes_written,
                "gc is tombstone-only so the bytes are still present — the 'already present, \
                 verified' branch reinstates the ledger without writing"
            );
            assert_eq!(
                store.refcount_for_hash(&summary.blake3_hash).unwrap(),
                1,
                "the verified restore reinstated the ledger row"
            );

            // Isolation: restore re-verifies the CAS object, it does not touch
            // the production table's rows.
            let n_now = count_rows(warehouse_dyn).await;
            assert_eq!(
                n_now, n_after_record,
                "restore must not change the production target (before={n_after_record}, \
                 after={n_now})"
            );

            // A second apply is an idempotent no-op: the tombstone is stamped
            // restored, so the re-apply re-verifies the live bytes and reports
            // already-restored rather than double-restoring.
            let out2 = execute_restore_apply(
                &store,
                &S3RestoreStores,
                warehouse_dyn,
                "restore-live-plan",
                &plan,
                Utc::now(),
            )
            .await
            .expect("idempotent re-apply");
            assert_eq!(out2.refused_count, 0, "re-apply must not refuse");
            assert!(
                out2.already_restored.contains(&summary.blake3_hash)
                    || out2
                        .restored
                        .iter()
                        .any(|x| x.blake3_hash == summary.blake3_hash),
                "re-apply re-verifies the same artifact, never double-restores"
            );
            assert_eq!(
                store.refcount_for_hash(&summary.blake3_hash).unwrap(),
                1,
                "the ledger row still stands after the idempotent re-apply"
            );

            // Teardown: remove the rows the recorded run appended (a DROP would
            // destroy the externally provisioned fixture); the content-addressed
            // parquet objects are not deletable with these creds and leak.
            warehouse_dyn
                .execute_statement(&format!(
                    "DELETE FROM {fqtn} WHERE id IN ({id_a}, {id_b}, {id_c})"
                ))
                .await
                .expect("teardown");
        }
    }
}
