//! `rocky replay <run_id|latest>` — inspect a recorded run.
//!
//! Surfaces the per-model SQL hashes, row counts, bytes, and timings captured
//! by the state store's `RunRecord`. Re-execution with pinned inputs is an
//! Arc-1 follow-up once the content-addressed write path arrives — the
//! inspection surface exists today so the reproducibility claim has a
//! concrete artefact to point at.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::reuse::UpstreamIdentity;
use rocky_core::state::{
    ModelExecution, ProvenanceRecord, RunRecord, RunStatus, RunTrigger, StateStore,
};
use rocky_ir::ModelIr;

use crate::output::{
    ReplayCheckInputOutput, ReplayCheckModelOutput, ReplayCheckOutput, ReplayExecuteModelOutput,
    ReplayExecuteOutput, ReplayModelOutput, ReplayOutput,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn status_str(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
        RunStatus::SkippedIdempotent => "skipped_idempotent",
        RunStatus::SkippedInFlight => "skipped_in_flight",
    }
}

fn trigger_str(trigger: &RunTrigger) -> &'static str {
    match trigger {
        RunTrigger::Manual => "manual",
        RunTrigger::Sensor => "sensor",
        RunTrigger::Schedule => "schedule",
        RunTrigger::Ci => "ci",
    }
}

fn to_model(exec: &ModelExecution) -> ReplayModelOutput {
    ReplayModelOutput {
        model_name: exec.model_name.clone(),
        status: exec.status.clone(),
        started_at: exec.started_at.to_rfc3339(),
        finished_at: exec.finished_at.to_rfc3339(),
        duration_ms: exec.duration_ms,
        sql_hash: exec.sql_hash.clone(),
        rows_affected: exec.rows_affected,
        bytes_scanned: exec.bytes_scanned,
        bytes_written: exec.bytes_written,
    }
}

fn resolve(store: &StateStore, target: &str) -> Result<RunRecord> {
    if target == "latest" {
        let runs = store.list_runs(1)?;
        return runs
            .into_iter()
            .next()
            .context("no runs recorded yet — nothing to replay");
    }
    store
        .get_run(target)?
        .with_context(|| format!("no run with id '{target}' in the state store"))
}

/// Execute `rocky replay`.
pub fn run_replay(
    state_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;

    let models: Vec<ReplayModelOutput> = record
        .models_executed
        .iter()
        .filter(|m| match model_filter {
            Some(name) => m.model_name == name,
            None => true,
        })
        .map(to_model)
        .collect();

    if let Some(name) = model_filter
        && models.is_empty()
    {
        anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
    }

    if json {
        let output = ReplayOutput {
            version: VERSION.to_string(),
            command: "replay".to_string(),
            run_id: record.run_id.clone(),
            status: status_str(&record.status).to_string(),
            trigger: trigger_str(&record.trigger).to_string(),
            started_at: record.started_at.to_rfc3339(),
            finished_at: record.finished_at.to_rfc3339(),
            config_hash: record.config_hash.clone(),
            models,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("run: {}", record.run_id);
        println!("status: {}", status_str(&record.status));
        println!("trigger: {}", trigger_str(&record.trigger));
        println!("started_at: {}", record.started_at.to_rfc3339());
        println!("finished_at: {}", record.finished_at.to_rfc3339());
        println!("config_hash: {}", record.config_hash);
        println!("models ({}):", models.len());
        for m in &models {
            print!("  {}  {}  sql_hash={}", m.model_name, m.status, m.sql_hash);
            if let Some(rows) = m.rows_affected {
                print!("  rows={rows}");
            }
            if let Some(bytes) = m.bytes_written {
                print!("  bytes_written={bytes}");
            }
            println!("  duration_ms={}", m.duration_ms);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky replay --check` — read-only replayability audit (Phase 0)
// ---------------------------------------------------------------------------

/// Classify one input (declared upstream) for replayability.
///
/// An upstream is replay-resolvable when its recorded bytes are locatable
/// from the ledger without touching the working tree or a live warehouse:
///
/// - a `Content` upstream resolves iff a matching `blake3_hash` still has an
///   `ArtifactRecord` in the content-addressed ledger (the row attests the
///   bytes were written; P0 does not re-`b3sum` object storage);
/// - a `Watermark` upstream is a *freshness* signal over a mutable source,
///   never a byte identity, so it is non-replayable and reported honestly
///   rather than silently substituted with current data. (The current engine
///   never records a watermark upstream on a provenance record — the reuse
///   spine indexes a model only when every read is a strong content hash — so
///   this arm is defensive against a future heuristic-population change.)
fn classify_input(store: &StateStore, upstream: &UpstreamIdentity) -> ReplayCheckInputOutput {
    match upstream {
        UpstreamIdentity::Content {
            upstream_key,
            blake3_hash,
        } => {
            let present = store
                .list_artifacts_by_hash(blake3_hash)
                .map(|rows| !rows.is_empty())
                .unwrap_or(false);
            let reason = if present {
                None
            } else {
                Some(format!(
                    "upstream '{upstream_key}' output (blake3 {}) is absent from the artifact ledger",
                    short_hash(blake3_hash)
                ))
            };
            ReplayCheckInputOutput {
                upstream_key: upstream_key.clone(),
                kind: "content".to_string(),
                resolvable: present,
                reason,
            }
        }
        UpstreamIdentity::Watermark { upstream_key, .. } => ReplayCheckInputOutput {
            upstream_key: upstream_key.clone(),
            kind: "watermark".to_string(),
            resolvable: false,
            reason: Some(format!(
                "upstream '{upstream_key}' resolved by a freshness watermark, not content — \
                 reads a mutable source (non-replayable)"
            )),
        },
    }
}

/// First 12 hex chars of a hash for compact reasons; full string if shorter.
fn short_hash(hash: &str) -> &str {
    hash.get(..12).unwrap_or(hash)
}

/// Classify one model in a recorded run.
///
/// Read-only: consults only the provenance record + artifact ledger. Never
/// reconstructs the model from the working tree.
fn classify_model(store: &StateStore, run_id: &str, model_name: &str) -> ReplayCheckModelOutput {
    let provenance: Option<ProvenanceRecord> =
        store.get_provenance(run_id, model_name).ok().flatten();

    let Some(prov) = provenance else {
        return ReplayCheckModelOutput {
            model_name: model_name.to_string(),
            verdict: "non_replayable".to_string(),
            reasons: vec![
                "no provenance recorded for this model (the run was not content-addressed, \
                 or auditable reuse was disabled)"
                    .to_string(),
            ],
            has_provenance: false,
            ir_parseable: false,
            nondeterministic: false,
            proof_class: None,
            inputs: Vec::new(),
        };
    };

    let mut reasons: Vec<String> = Vec::new();

    // Reconstruct the recipe from the record — the recording is the truth.
    let ir: Option<ModelIr> = serde_json::from_str(&prov.model_ir_canonical_json).ok();
    let ir_parseable = ir.is_some();
    if !ir_parseable {
        reasons.push(
            "embedded canonical ModelIr did not deserialize under the current engine \
             (IR forward-compatibility break)"
                .to_string(),
        );
    }

    // Static non-determinism scan over the reconstructed SQL. A flag, not a
    // verdict: a nondeterministic model is still replayable, but a future
    // `--execute` may legitimately diverge. Empty SQL carries no volatile
    // construct, so it is not flagged (the pessimistic scan would otherwise
    // treat an unparseable empty body as volatile).
    let nondeterministic = ir
        .as_ref()
        .map(|ir| !ir.sql.trim().is_empty() && !rocky_sql::determinism::is_deterministic(&ir.sql))
        .unwrap_or(false);

    // Resolve every declared input against the ledger.
    let inputs: Vec<ReplayCheckInputOutput> = prov
        .upstreams
        .iter()
        .map(|u| classify_input(store, u))
        .collect();
    for input in &inputs {
        if let Some(reason) = &input.reason {
            reasons.push(reason.clone());
        }
    }

    let all_inputs_resolvable = inputs.iter().all(|i| i.resolvable);
    let replayable = ir_parseable && all_inputs_resolvable;

    ReplayCheckModelOutput {
        model_name: model_name.to_string(),
        verdict: if replayable {
            "replayable".to_string()
        } else {
            "non_replayable".to_string()
        },
        reasons,
        has_provenance: true,
        ir_parseable,
        nondeterministic,
        proof_class: Some(prov.proof_class.clone()),
        inputs,
    }
}

/// Execute `rocky replay --check` — a read-only replayability audit.
///
/// For each model in the recorded run, classify whether it could be
/// re-executed from its recording alone (provenance present, embedded IR
/// parses under the current engine, inputs ledger-resolvable) and flag
/// static non-determinism. Nothing is executed; nothing is written.
pub fn run_replay_check(
    state_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;

    let models: Vec<ReplayCheckModelOutput> = record
        .models_executed
        .iter()
        .filter(|m| match model_filter {
            Some(name) => m.model_name == name,
            None => true,
        })
        .map(|m| classify_model(&store, &record.run_id, &m.model_name))
        .collect();

    if let Some(name) = model_filter
        && models.is_empty()
    {
        anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
    }

    let replayable_count = models.iter().filter(|m| m.verdict == "replayable").count();
    let all_replayable = replayable_count == models.len();

    if json {
        let output = ReplayCheckOutput {
            version: VERSION.to_string(),
            command: "replay --check".to_string(),
            run_id: record.run_id.clone(),
            status: status_str(&record.status).to_string(),
            replayable: all_replayable,
            model_count: models.len(),
            replayable_count,
            models,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("run: {}", record.run_id);
        println!("status: {}", status_str(&record.status));
        println!("replayable: {}/{} models", replayable_count, models.len());
        for m in &models {
            print!("  {}  {}", m.model_name, m.verdict);
            if m.nondeterministic {
                print!("  [nondeterministic]");
            }
            println!();
            for reason in &m.reasons {
                println!("      - {reason}");
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky replay --execute [--verify]` — single-model DuckDB re-execution
// ---------------------------------------------------------------------------

/// Build a deterministic [`UniformTableState`] for replay-time Parquet
/// encoding from a model's typed columns.
///
/// Replay re-derives the output blake3 through the *same*
/// [`build_parquet`](rocky_iceberg::uniform_writer::parquet_builder::build_parquet)
/// the content-addressed writer uses, so the two digests are comparable. A
/// live UniForm table maps each logical column to a random physical UUID (read
/// at runtime from its `_delta_log`); with no live table to discover, replay
/// pins a deterministic mapping (physical name == logical name, `field_id` =
/// 1-based ordinal). That is self-consistent between the recording and the
/// replay, but it means a locally re-derived hash never matches a
/// *live-warehouse* recording whose physical names are UUIDs — a comparison
/// only the warehouse replay path (a later phase) makes.
#[cfg(feature = "duckdb")]
fn deterministic_replay_state(
    typed_columns: &[rocky_ir::types::TypedColumn],
) -> rocky_iceberg::uniform_writer::UniformTableState {
    use std::collections::HashMap;

    let mut physical = HashMap::with_capacity(typed_columns.len());
    let mut field_id = HashMap::with_capacity(typed_columns.len());
    for (i, tc) in typed_columns.iter().enumerate() {
        physical.insert(tc.name.clone(), tc.name.clone());
        field_id.insert(tc.name.clone(), (i + 1) as i32);
    }
    rocky_iceberg::uniform_writer::UniformTableState {
        physical,
        field_id,
        partition_columns: Vec::new(),
        row_tracking_enabled: false,
        deletion_vectors_enabled: false,
        next_commit_version: 1,
        row_tracking_next_id: 0,
    }
}

/// Re-execute a reconstructed recipe against a caller-owned in-memory DuckDB
/// engine and return `(rows, output_blake3_hex)`.
///
/// The recipe's `SELECT` runs against the passed-in `adapter` — nothing is
/// persisted to any *production* schema, so no production identity is touched.
/// For single-model replay the caller hands in a throwaway engine (isolation
/// is vacuous for a self-contained `SELECT`); for DAG replay the caller hands
/// in a *shared* engine into which upstream outputs have already been
/// materialized, so `ir.sql`'s `catalog.schema.table` references resolve to the
/// **replayed** upstream tables rather than to production or recorded bytes.
/// The whole ephemeral engine is the replay namespace; it is discarded after
/// the run.
///
/// The result rows are converted to the same Arrow → Parquet encoding the
/// content-addressed writer emits and hashed with blake3, so the digest is
/// directly comparable to the recorded output hash.
#[cfg(feature = "duckdb")]
async fn execute_and_hash(
    adapter: &rocky_duckdb::adapter::DuckDbWarehouseAdapter,
    ir: &ModelIr,
) -> Result<(u64, String)> {
    use rocky_core::traits::WarehouseAdapter;

    let result = adapter
        .execute_query(&ir.sql)
        .await
        .map_err(|e| anyhow::anyhow!("re-execution query failed: {e}"))?;
    let rows = result.rows.len() as u64;
    let batch = crate::commands::run_content_addressed::query_result_to_record_batch(
        &ir.typed_columns,
        &result,
    )?;
    let state = deterministic_replay_state(&ir.typed_columns);
    let parquet = rocky_iceberg::uniform_writer::parquet_builder::build_parquet(&batch, &state)
        .map_err(|e| anyhow::anyhow!("replay Parquet encode failed: {e}"))?;
    Ok((rows, blake3::hash(&parquet).to_hex().to_string()))
}

/// A `non_replayable` per-model result — the fail-closed default for any
/// re-execution the recording alone cannot support.
#[cfg(feature = "duckdb")]
fn non_replayable_exec(
    model_name: &str,
    nondeterministic: bool,
    reasons: Vec<String>,
) -> ReplayExecuteModelOutput {
    ReplayExecuteModelOutput {
        model_name: model_name.to_string(),
        verdict: "non_replayable".to_string(),
        nondeterministic,
        recorded_hash: None,
        computed_hash: None,
        rows: None,
        reasons,
    }
}

/// Re-execute a single recorded model and classify the result.
///
/// Fail-safe by construction: the `bit_exact` verdict is reachable *only*
/// through the final happy path (a successful re-execution whose re-derived
/// blake3 equals the recorded one). Every earlier exit — non-replayable
/// classification, content upstreams, a partitioned or non-content strategy, a
/// failed re-execution, a missing recorded hash — returns `non_replayable`,
/// and a successful-but-mismatched digest returns `diverged`.
#[cfg(feature = "duckdb")]
async fn replay_execute_model(
    store: &StateStore,
    run_id: &str,
    model_name: &str,
    verify: bool,
) -> ReplayExecuteModelOutput {
    // Reuse the read-only `--check` classifier: it establishes provenance
    // exists, the embedded IR parses, and every declared input resolves.
    let check = classify_model(store, run_id, model_name);
    if check.verdict != "replayable" {
        return non_replayable_exec(model_name, check.nondeterministic, check.reasons);
    }

    // classify_model returned `replayable`, so the record + a parseable IR are
    // present. Reconstruct the recipe from the recording — never the tree.
    let Some(prov) = store.get_provenance(run_id, model_name).ok().flatten() else {
        return non_replayable_exec(
            model_name,
            check.nondeterministic,
            vec!["provenance record vanished between classification and execution".to_string()],
        );
    };
    let Ok(ir) = serde_json::from_str::<ModelIr>(&prov.model_ir_canonical_json) else {
        return non_replayable_exec(
            model_name,
            check.nondeterministic,
            vec![
                "embedded canonical ModelIr did not deserialize under the current engine"
                    .to_string(),
            ],
        );
    };

    // Single-model re-execution requires a self-contained recipe. A content
    // upstream's recorded bytes live at an object-store path this creds-free
    // path never reads, and substituting current data is forbidden. Resolving
    // inputs to *replayed* upstream outputs is multi-model DAG replay (a later
    // phase).
    if !prov.upstreams.is_empty() {
        return non_replayable_exec(
            model_name,
            check.nondeterministic,
            vec![format!(
                "single-model re-execution requires a self-contained recipe, but this model \
                 reads {} recorded upstream(s); resolving inputs to their replayed upstream \
                 outputs is multi-model DAG replay (a later phase). Recorded upstream bytes are \
                 not read and current data is never substituted.",
                prov.upstreams.len()
            )],
        );
    }

    // Only an unpartitioned content-addressed model carries a single
    // whole-output blake3 to reproduce and compare against.
    match &ir.materialization {
        rocky_ir::MaterializationStrategy::ContentAddressed {
            partition_columns, ..
        } if partition_columns.is_empty() => {}
        rocky_ir::MaterializationStrategy::ContentAddressed { .. } => {
            return non_replayable_exec(
                model_name,
                check.nondeterministic,
                vec![
                    "partitioned re-execution is a later phase (the recorded hash is \
                      per-partition, not a single whole-output digest)"
                        .to_string(),
                ],
            );
        }
        _ => {
            return non_replayable_exec(
                model_name,
                check.nondeterministic,
                vec![
                    "only content-addressed models carry a whole-output blake3 to compare \
                      against"
                        .to_string(),
                ],
            );
        }
    }

    // Re-execute + re-derive the output digest. Single-model replay uses a
    // fresh, throwaway in-memory engine — the recipe is self-contained (the
    // `!upstreams.is_empty()` guard above already rejected any recorded
    // upstream), so no pre-materialized upstream tables are needed. Any failure
    // is fail-closed to non_replayable — never diverged, never bit_exact.
    let adapter = match rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory() {
        Ok(a) => a,
        Err(e) => {
            return non_replayable_exec(
                model_name,
                check.nondeterministic,
                vec![format!(
                    "could not start an in-memory DuckDB engine for replay: {e}"
                )],
            );
        }
    };
    let (rows, computed) = match execute_and_hash(&adapter, &ir).await {
        Ok(v) => v,
        Err(e) => {
            return non_replayable_exec(
                model_name,
                check.nondeterministic,
                vec![format!(
                    "re-execution could not reproduce the artifact: {e:#}"
                )],
            );
        }
    };

    let recorded = prov.output_blake3.first().cloned();

    if !verify {
        return ReplayExecuteModelOutput {
            model_name: model_name.to_string(),
            verdict: "executed".to_string(),
            nondeterministic: check.nondeterministic,
            recorded_hash: recorded,
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: Vec::new(),
        };
    }

    let Some(recorded) = recorded else {
        return ReplayExecuteModelOutput {
            model_name: model_name.to_string(),
            verdict: "non_replayable".to_string(),
            nondeterministic: check.nondeterministic,
            recorded_hash: None,
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: vec!["provenance record carried no output hash to verify against".to_string()],
        };
    };

    if computed == recorded {
        ReplayExecuteModelOutput {
            model_name: model_name.to_string(),
            verdict: "bit_exact".to_string(),
            nondeterministic: check.nondeterministic,
            recorded_hash: Some(recorded),
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: Vec::new(),
        }
    } else {
        let mut reasons = vec![format!(
            "re-executed output blake3 {} != recorded {}",
            short_hash(&computed),
            short_hash(&recorded)
        )];
        if check.nondeterministic {
            reasons.push(
                "expected: the recipe contains a nondeterministic construct \
                 (now()/random()/…), so a byte-identical replay is not guaranteed"
                    .to_string(),
            );
        }
        ReplayExecuteModelOutput {
            model_name: model_name.to_string(),
            verdict: "diverged".to_string(),
            nondeterministic: check.nondeterministic,
            recorded_hash: Some(recorded),
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons,
        }
    }
}

// ---------------------------------------------------------------------------
// `rocky replay --execute [--verify]` (whole run) — DAG-order multi-model
// re-execution
// ---------------------------------------------------------------------------

/// Fully-qualified `catalog.schema.table` identity of a model's output.
///
/// This is the *same* string a downstream model's recorded
/// [`UpstreamIdentity::Content::upstream_key`] carries (both are built from the
/// producer's `TargetRef`), so it is the join key for the replay DAG's edges.
fn target_fqn(ir: &ModelIr) -> String {
    format!(
        "{}.{}.{}",
        ir.target.catalog, ir.target.schema, ir.target.table
    )
}

/// A model eligible to be *executed* in the replay DAG: it has a provenance
/// record, its embedded IR parses, and it materialises a single whole-output
/// blake3 (unpartitioned content-addressed). Models failing any of these get a
/// direct `non_replayable` verdict and are never producers in the graph.
#[cfg(feature = "duckdb")]
struct DagCandidate {
    model_name: String,
    ir: ModelIr,
    output_fqn: String,
    recorded_hash: Option<String>,
    nondeterministic: bool,
    /// The recorded upstream identities, exactly as folded into the model's
    /// `input_hash`; resolved into edges vs blocks in pass B.
    upstreams: Vec<UpstreamIdentity>,
    /// In-run upstream FQNs (each produced by *another* candidate in this run).
    in_run_upstreams: Vec<String>,
    /// A reason this node cannot be executed even though it is a candidate:
    /// an upstream that no in-run node produces (recorded bytes on object
    /// storage the creds-free replay never reads) or a mutable-source
    /// watermark. `Some` ⇒ statically blocked ⇒ `non_replayable`, no table.
    blocked_reason: Option<String>,
}

/// Ensure the `catalog.schema` namespace for `ir`'s target exists in the shared
/// replay engine so a later `CREATE OR REPLACE TABLE catalog.schema.table`
/// (and the downstream `SELECT`s that reference it) resolve. Each non-default
/// catalog is `ATTACH`ed as its own throwaway in-memory database exactly once.
#[cfg(feature = "duckdb")]
async fn ensure_namespace(
    adapter: &rocky_duckdb::adapter::DuckDbWarehouseAdapter,
    ir: &ModelIr,
    attached_catalogs: &mut std::collections::HashSet<String>,
) -> Result<()> {
    use rocky_core::traits::WarehouseAdapter;

    let catalog = &ir.target.catalog;
    // `memory` is DuckDB's built-in in-memory catalog; every other catalog name
    // is materialised as its own attached `:memory:` database so a 3-part FQN
    // resolves. The whole engine is ephemeral, so these are all replay-scoped.
    if catalog != "memory" && attached_catalogs.insert(catalog.clone()) {
        adapter
            .execute_statement(&format!("ATTACH ':memory:' AS {catalog}"))
            .await
            .map_err(|e| anyhow::anyhow!("could not attach replay catalog {catalog:?}: {e}"))?;
    }
    adapter
        .execute_statement(&format!(
            "CREATE SCHEMA IF NOT EXISTS {}.{}",
            ir.target.catalog, ir.target.schema
        ))
        .await
        .map_err(|e| {
            anyhow::anyhow!("could not create replay schema for {}: {e}", target_fqn(ir))
        })?;
    Ok(())
}

/// Re-execute the *whole* recorded run in dependency order.
///
/// This is the DAG-order path taken when no `--model` filter is given. Each
/// content-addressed model is reconstructed from its recording and executed on
/// a **single shared** in-memory DuckDB engine in topological order, so a
/// downstream model's `SELECT` reads its upstream's **replayed** output
/// (materialised into the shared engine as `catalog.schema.table`) rather than
/// the recorded object-store bytes or any production table. That is the real
/// test of recipe sufficiency: a recipe that under-specifies its inputs
/// diverges here because it consumed a freshly-replayed upstream.
///
/// Fail-closed cascade: a node whose in-run upstream did not materialise
/// (blocked, errored, or itself `non_replayable`) is reported `non_replayable`
/// and never runs against a missing/stale table — a divergent or errored
/// upstream can never let a downstream fabricate a `bit_exact`. An upstream
/// that *executed* but merely `diverged` still materialises (a diverged replay
/// is still a replayed output), so its downstream reads those replayed bytes.
///
/// The `nondeterministic` flag is a static scan of a node's **own** SQL. A
/// deterministic downstream of a nondeterministic upstream can therefore
/// `diverge` without carrying the flag itself — the divergence is inherited
/// through the replayed input, which is the honest, expected behaviour (the
/// reason string still reports the byte mismatch). Propagating the flag
/// transitively is a later refinement.
///
/// Isolation carries through from single-model replay: the entire engine is an
/// ephemeral replay namespace, nothing is written to any warehouse or object
/// store, and the working tree is never consulted.
#[cfg(feature = "duckdb")]
async fn replay_execute_dag(
    store: &StateStore,
    record: &RunRecord,
    verify: bool,
) -> Vec<ReplayExecuteModelOutput> {
    use std::collections::{HashMap, HashSet};

    let run_id = &record.run_id;
    let mut finished: HashMap<String, ReplayExecuteModelOutput> = HashMap::new();
    let mut candidates: Vec<DagCandidate> = Vec::new();

    // --- Pass A: reconstruct each model; separate executable candidates from
    // directly-non_replayable models. Reconstruct from the recording only. ---
    for exec in &record.models_executed {
        let name = exec.model_name.clone();
        let Some(prov) = store.get_provenance(run_id, &name).ok().flatten() else {
            finished.insert(
                name.clone(),
                non_replayable_exec(
                    &name,
                    false,
                    vec![
                        "no provenance recorded for this model (the run was not \
                         content-addressed, or auditable reuse was disabled)"
                            .to_string(),
                    ],
                ),
            );
            continue;
        };
        let Ok(ir) = serde_json::from_str::<ModelIr>(&prov.model_ir_canonical_json) else {
            finished.insert(
                name.clone(),
                non_replayable_exec(
                    &name,
                    false,
                    vec![
                        "embedded canonical ModelIr did not deserialize under the current engine \
                         (IR forward-compatibility break)"
                            .to_string(),
                    ],
                ),
            );
            continue;
        };
        let nondeterministic =
            !ir.sql.trim().is_empty() && !rocky_sql::determinism::is_deterministic(&ir.sql);

        match &ir.materialization {
            rocky_ir::MaterializationStrategy::ContentAddressed {
                partition_columns, ..
            } if partition_columns.is_empty() => {}
            rocky_ir::MaterializationStrategy::ContentAddressed { .. } => {
                finished.insert(
                    name.clone(),
                    non_replayable_exec(
                        &name,
                        nondeterministic,
                        vec![
                            "partitioned re-execution is a later phase (the recorded hash is \
                              per-partition, not a single whole-output digest)"
                                .to_string(),
                        ],
                    ),
                );
                continue;
            }
            _ => {
                finished.insert(
                    name.clone(),
                    non_replayable_exec(
                        &name,
                        nondeterministic,
                        vec![
                            "only content-addressed models carry a whole-output blake3 to compare \
                              against"
                                .to_string(),
                        ],
                    ),
                );
                continue;
            }
        }

        candidates.push(DagCandidate {
            model_name: name,
            output_fqn: target_fqn(&ir),
            recorded_hash: prov.output_blake3.first().cloned(),
            nondeterministic,
            upstreams: prov.upstreams,
            in_run_upstreams: Vec::new(),
            blocked_reason: None,
            ir,
        });
    }

    let produced: HashSet<String> = candidates.iter().map(|c| c.output_fqn.clone()).collect();

    // --- Pass B: classify each candidate's upstreams into in-run edges vs a
    // blocking external/watermark read. `upstreams` is moved out so the loop
    // can mutate the candidate's other fields without an aliasing borrow. ---
    for cand in &mut candidates {
        for upstream in std::mem::take(&mut cand.upstreams) {
            match upstream {
                UpstreamIdentity::Content { upstream_key, .. } => {
                    if produced.contains(&upstream_key) {
                        cand.in_run_upstreams.push(upstream_key);
                    } else {
                        cand.blocked_reason = Some(format!(
                            "upstream '{upstream_key}' is content-addressed but is not produced by \
                             any model in this recorded run; its recorded bytes live on object \
                             storage that the creds-free DAG replay never reads (a warehouse-path \
                             replay is a later phase)"
                        ));
                        break;
                    }
                }
                UpstreamIdentity::Watermark { upstream_key, .. } => {
                    cand.blocked_reason = Some(format!(
                        "upstream '{upstream_key}' is resolved by a freshness watermark over a \
                         mutable source (non-replayable)"
                    ));
                    break;
                }
            }
        }
    }

    // --- Topological order over the in-run edges (Kahn). A cycle (which the
    // real engine's acyclic DAG never produces) leaves its members unordered;
    // they are handled as an unmaterialised-upstream cascade below. ---
    let order = topo_order(&candidates);

    // --- Execute in order on ONE shared engine. Upstream outputs are
    // materialised so downstreams read the replayed bytes. ---
    let adapter = match rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory() {
        Ok(a) => a,
        Err(e) => {
            // Engine start failed: every candidate is non_replayable. Directly
            // non_replayable models keep their pass-A verdict.
            for cand in &candidates {
                finished.entry(cand.model_name.clone()).or_insert_with(|| {
                    non_replayable_exec(
                        &cand.model_name,
                        cand.nondeterministic,
                        vec![format!(
                            "could not start an in-memory DuckDB engine for replay: {e}"
                        )],
                    )
                });
            }
            return assemble(record, finished);
        }
    };
    let mut attached: HashSet<String> = HashSet::new();
    let mut materialized: HashSet<String> = HashSet::new();
    let by_name: HashMap<&str, &DagCandidate> = candidates
        .iter()
        .map(|c| (c.model_name.as_str(), c))
        .collect();

    for name in order {
        let cand = by_name[name.as_str()];

        // Statically blocked (external/watermark upstream): non_replayable, no
        // table — its downstreams cascade.
        if let Some(reason) = &cand.blocked_reason {
            finished.insert(
                cand.model_name.clone(),
                non_replayable_exec(
                    &cand.model_name,
                    cand.nondeterministic,
                    vec![reason.clone()],
                ),
            );
            continue;
        }

        // Fail-closed cascade: every in-run upstream must have materialised.
        if let Some(missing) = cand
            .in_run_upstreams
            .iter()
            .find(|u| !materialized.contains(*u))
        {
            finished.insert(
                cand.model_name.clone(),
                non_replayable_exec(
                    &cand.model_name,
                    cand.nondeterministic,
                    vec![format!(
                        "upstream '{missing}' could not be replayed, so its replayed output is \
                         unavailable to feed this model (cascade)"
                    )],
                ),
            );
            continue;
        }

        finished.insert(
            cand.model_name.clone(),
            replay_execute_dag_node(&adapter, cand, verify, &mut attached, &mut materialized).await,
        );
    }

    assemble(record, finished)
}

/// Execute one topologically-ready DAG node against the shared engine, compare
/// to its recorded hash (when `verify`), and materialise its output so
/// downstream nodes read the replayed bytes. On any execution/materialisation
/// error the node is `non_replayable` and **no** table is created, so the
/// fail-closed cascade denies its downstreams.
#[cfg(feature = "duckdb")]
async fn replay_execute_dag_node(
    adapter: &rocky_duckdb::adapter::DuckDbWarehouseAdapter,
    cand: &DagCandidate,
    verify: bool,
    attached: &mut std::collections::HashSet<String>,
    materialized: &mut std::collections::HashSet<String>,
) -> ReplayExecuteModelOutput {
    use rocky_core::traits::WarehouseAdapter;

    if let Err(e) = ensure_namespace(adapter, &cand.ir, attached).await {
        return non_replayable_exec(
            &cand.model_name,
            cand.nondeterministic,
            vec![format!(
                "re-execution could not prepare the replay namespace: {e:#}"
            )],
        );
    }

    // Hash the node from a direct `execute_query(sql)` — the same canonical
    // row order the content-addressed writer recorded — against upstream tables
    // already materialised in the shared engine.
    let (rows, computed) = match execute_and_hash(adapter, &cand.ir).await {
        Ok(v) => v,
        Err(e) => {
            return non_replayable_exec(
                &cand.model_name,
                cand.nondeterministic,
                vec![format!(
                    "re-execution could not reproduce the artifact: {e:#}"
                )],
            );
        }
    };

    // Materialise this node's output so downstream `SELECT`s resolve its FQN to
    // the replayed rows. This is an independent re-materialisation of the same
    // recipe (a second evaluation): byte-identical to the hashed output for a
    // deterministic recipe; for a nondeterministic one the divergence is the
    // flagged, expected boundary. A failure here creates no table, so the
    // downstream cascade denies dependents.
    let ctas = format!(
        "CREATE OR REPLACE TABLE {} AS {}",
        cand.output_fqn, cand.ir.sql
    );
    if let Err(e) = adapter.execute_statement(&ctas).await {
        return non_replayable_exec(
            &cand.model_name,
            cand.nondeterministic,
            vec![format!(
                "re-executed but could not materialise the replayed output for downstream \
                 consumers: {e}"
            )],
        );
    }
    materialized.insert(cand.output_fqn.clone());

    if !verify {
        return ReplayExecuteModelOutput {
            model_name: cand.model_name.clone(),
            verdict: "executed".to_string(),
            nondeterministic: cand.nondeterministic,
            recorded_hash: cand.recorded_hash.clone(),
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: Vec::new(),
        };
    }

    let Some(recorded) = cand.recorded_hash.clone() else {
        return ReplayExecuteModelOutput {
            model_name: cand.model_name.clone(),
            verdict: "non_replayable".to_string(),
            nondeterministic: cand.nondeterministic,
            recorded_hash: None,
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: vec!["provenance record carried no output hash to verify against".to_string()],
        };
    };

    if computed == recorded {
        ReplayExecuteModelOutput {
            model_name: cand.model_name.clone(),
            verdict: "bit_exact".to_string(),
            nondeterministic: cand.nondeterministic,
            recorded_hash: Some(recorded),
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons: Vec::new(),
        }
    } else {
        let mut reasons = vec![format!(
            "re-executed output blake3 {} != recorded {}",
            short_hash(&computed),
            short_hash(&recorded)
        )];
        if cand.nondeterministic {
            reasons.push(
                "expected: the recipe contains a nondeterministic construct \
                 (now()/random()/…), so a byte-identical replay is not guaranteed"
                    .to_string(),
            );
        }
        ReplayExecuteModelOutput {
            model_name: cand.model_name.clone(),
            verdict: "diverged".to_string(),
            nondeterministic: cand.nondeterministic,
            recorded_hash: Some(recorded),
            computed_hash: Some(computed),
            rows: Some(rows),
            reasons,
        }
    }
}

/// Kahn topological sort of the candidates over their in-run edges. Any node
/// left over after the queue drains (only possible under a cycle the real
/// acyclic engine never emits) is appended so it still receives an
/// unmaterialised-upstream cascade verdict.
#[cfg(feature = "duckdb")]
fn topo_order(candidates: &[DagCandidate]) -> Vec<String> {
    use std::collections::{HashMap, VecDeque};

    let mut indegree: HashMap<&str, usize> = candidates
        .iter()
        .map(|c| (c.model_name.as_str(), 0usize))
        .collect();
    // Map producer FQN → its model name, to translate upstream FQNs to nodes.
    let producer: HashMap<&str, &str> = candidates
        .iter()
        .map(|c| (c.output_fqn.as_str(), c.model_name.as_str()))
        .collect();
    // Edges: producer → consumer. Count only in-run upstreams that map to a
    // producer (a blocked node's external upstream contributes no edge).
    let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
    for c in candidates {
        for up in &c.in_run_upstreams {
            if let Some(prod) = producer.get(up.as_str()) {
                dependents.entry(prod).or_default().push(&c.model_name);
                *indegree.get_mut(c.model_name.as_str()).unwrap() += 1;
            }
        }
    }

    let mut queue: VecDeque<&str> = candidates
        .iter()
        .filter(|c| indegree[c.model_name.as_str()] == 0)
        .map(|c| c.model_name.as_str())
        .collect();
    let mut order: Vec<String> = Vec::with_capacity(candidates.len());
    while let Some(node) = queue.pop_front() {
        order.push(node.to_string());
        if let Some(children) = dependents.get(node) {
            for child in children {
                let d = indegree.get_mut(*child).unwrap();
                *d -= 1;
                if *d == 0 {
                    queue.push_back(child);
                }
            }
        }
    }
    // Any node not emitted sits in a cycle; append so it still gets a verdict.
    for c in candidates {
        if !order.iter().any(|n| n == &c.model_name) {
            order.push(c.model_name.clone());
        }
    }
    order
}

/// Assemble the per-model verdicts back into `record.models_executed` order.
#[cfg(feature = "duckdb")]
fn assemble(
    record: &RunRecord,
    mut finished: std::collections::HashMap<String, ReplayExecuteModelOutput>,
) -> Vec<ReplayExecuteModelOutput> {
    record
        .models_executed
        .iter()
        .map(|exec| {
            finished.remove(&exec.model_name).unwrap_or_else(|| {
                non_replayable_exec(
                    &exec.model_name,
                    false,
                    vec!["model was not reached by the replay DAG".to_string()],
                )
            })
        })
        .collect()
}

/// Execute `rocky replay --execute [--verify]`.
///
/// With `--model <m>`, single-model DuckDB re-execution: reconstructs the
/// targeted model's recipe from its recorded [`ProvenanceRecord`] and, because
/// a single model cannot resolve a recorded upstream's bytes, replays only the
/// self-contained case. Without `--model`, the **whole run** replays in
/// DAG order (see [`replay_execute_dag`]): each downstream reads its upstream's
/// *replayed* output from a shared ephemeral engine.
///
/// In both modes the recipe comes from the recording (never the working tree),
/// nothing is materialized to any warehouse schema, and every verdict —
/// including `diverged` and `non_replayable` — is a classification, not a tool
/// failure, so this returns `Ok` (exit 0) unless the run itself cannot be
/// resolved; callers inspect `verdict` / `bit_exact_count`.
#[cfg(feature = "duckdb")]
pub async fn run_replay_execute(
    state_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    verify: bool,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;

    let models: Vec<ReplayExecuteModelOutput> = if let Some(name) = model_filter {
        let mut v: Vec<ReplayExecuteModelOutput> = Vec::new();
        for exec in &record.models_executed {
            if exec.model_name == name {
                v.push(
                    replay_execute_model(&store, &record.run_id, &exec.model_name, verify).await,
                );
            }
        }
        if v.is_empty() {
            anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
        }
        v
    } else {
        replay_execute_dag(&store, &record, verify).await
    };

    let bit_exact_count = models.iter().filter(|m| m.verdict == "bit_exact").count();

    if json {
        let output = ReplayExecuteOutput {
            version: VERSION.to_string(),
            command: if verify {
                "replay --execute --verify".to_string()
            } else {
                "replay --execute".to_string()
            },
            run_id: record.run_id.clone(),
            status: status_str(&record.status).to_string(),
            verified: verify,
            model_count: models.len(),
            bit_exact_count,
            models,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("run: {}", record.run_id);
        println!("status: {}", status_str(&record.status));
        if verify {
            println!("bit-exact: {}/{} models", bit_exact_count, models.len());
        } else {
            println!("re-executed: {} models", models.len());
        }
        for m in &models {
            print!("  {}  {}", m.model_name, m.verdict);
            if m.nondeterministic {
                print!("  [nondeterministic]");
            }
            if let Some(rows) = m.rows {
                print!("  rows={rows}");
            }
            println!();
            if let (Some(c), Some(r)) = (&m.computed_hash, &m.recorded_hash) {
                println!(
                    "      computed={}  recorded={}",
                    short_hash(c),
                    short_hash(r)
                );
            }
            for reason in &m.reasons {
                println!("      - {reason}");
            }
        }
    }
    Ok(())
}

/// Stub for builds without the `duckdb` feature: re-execution needs a local
/// engine to run the recipe against.
#[cfg(not(feature = "duckdb"))]
pub async fn run_replay_execute(
    _state_path: &Path,
    _target: &str,
    _model_filter: Option<&str>,
    _verify: bool,
    _json: bool,
) -> Result<()> {
    anyhow::bail!(
        "`rocky replay --execute` re-executes the recipe on a local DuckDB engine, which needs \
         the `duckdb` feature (enabled in the default build)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::Utc;
    use tempfile::TempDir;

    fn sample_run(run_id: &str, models: Vec<(&str, &str)>) -> RunRecord {
        let now = Utc::now();
        let models_executed: Vec<ModelExecution> = models
            .into_iter()
            .map(|(name, status)| ModelExecution {
                model_name: name.to_string(),
                started_at: now,
                finished_at: now,
                duration_ms: 42,
                rows_affected: Some(100),
                status: status.to_string(),
                sql_hash: format!("hash_{name}"),
                skip_hash: None,
                upstream_freshness: None,
                bytes_scanned: Some(1024),
                bytes_written: Some(2048),
                tenant: None,
                recipe_hash: None,
                input_hash: None,
                input_proof_class: None,
                env_hash: None,
                hash_scheme: None,
                output_column_hashes: None,
                attempts: Vec::new(),
            })
            .collect();
        RunRecord {
            run_id: run_id.to_string(),
            started_at: now,
            finished_at: now,
            status: RunStatus::Success,
            models_executed,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "replay-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
        }
    }

    #[test]
    fn resolve_by_run_id() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        store
            .record_run(&sample_run("run-1", vec![("orders", "success")]))
            .unwrap();

        let resolved = resolve(&store, "run-1").unwrap();
        assert_eq!(resolved.run_id, "run-1");
    }

    #[test]
    fn resolve_latest() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        store
            .record_run(&sample_run("old", vec![("m", "success")]))
            .unwrap();
        // Brief gap so the second run's started_at actually sorts after.
        std::thread::sleep(std::time::Duration::from_millis(5));
        store
            .record_run(&sample_run("new", vec![("m", "success")]))
            .unwrap();

        let resolved = resolve(&store, "latest").unwrap();
        assert_eq!(resolved.run_id, "new");
    }

    #[test]
    fn resolve_missing_run_id_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let err = resolve(&store, "does-not-exist").unwrap_err();
        assert!(err.to_string().contains("does-not-exist"));
    }

    #[test]
    fn resolve_latest_empty_store_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let err = resolve(&store, "latest").unwrap_err();
        assert!(err.to_string().contains("nothing to replay"));
    }

    // -- `--check` classification -----------------------------------------

    use rocky_core::reuse::{OutputArtifact, build_records};
    use rocky_core::state::ArtifactRecord;
    use rocky_ir::types::{RockyType, TypedColumn};
    use rocky_ir::{GovernanceConfig, MaterializationStrategy, TargetRef};

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

    /// Seed a provenance record (via the production `build_records` path) and,
    /// optionally, an artifact for the model's own output hash.
    fn seed(
        store: &StateStore,
        run_id: &str,
        table: &str,
        sql: &str,
        upstreams: &[UpstreamIdentity],
        out_hash: &str,
        record_artifact: bool,
    ) {
        let ir = ca_ir(table, sql);
        let outputs = vec![OutputArtifact {
            blake3_hash: out_hash.to_string(),
            file_path: format!("s3://b/{out_hash}.parquet"),
        }];
        let (entry, prov) = build_records(&ir, run_id, upstreams, &outputs, Utc::now()).unwrap();
        store
            .record_reuse_spine(std::slice::from_ref(&entry), std::slice::from_ref(&prov))
            .unwrap();
        if record_artifact {
            store
                .record_artifact(&ArtifactRecord {
                    blake3_hash: out_hash.to_string(),
                    run_id: run_id.to_string(),
                    model_name: table.to_string(),
                    file_path: format!("s3://b/{out_hash}.parquet"),
                    commit_version: 0,
                    size_bytes: 1,
                    written_at: Utc::now(),
                })
                .unwrap();
        }
    }

    const HA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const HB: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

    fn content(table: &str, hash: &str) -> UpstreamIdentity {
        UpstreamIdentity::Content {
            upstream_key: format!("tgt.raw.{table}"),
            blake3_hash: hash.to_string(),
        }
    }

    #[test]
    fn classify_no_provenance_is_non_replayable() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let v = classify_model(&store, "r", "orphan");
        assert_eq!(v.verdict, "non_replayable");
        assert!(!v.has_provenance);
        assert!(!v.ir_parseable);
        assert!(v.reasons[0].contains("no provenance recorded"));
    }

    #[test]
    fn classify_replayable_when_inputs_resolve() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        seed(&store, "r", "root", "SELECT 1 AS id", &[], HA, true);
        seed(
            &store,
            "r",
            "leaf",
            "SELECT id FROM tgt.raw.root",
            &[content("root", HA)],
            HB,
            true,
        );
        let v = classify_model(&store, "r", "leaf");
        assert_eq!(v.verdict, "replayable");
        assert!(v.has_provenance && v.ir_parseable);
        assert_eq!(v.proof_class.as_deref(), Some("strong"));
        assert_eq!(v.inputs.len(), 1);
        assert!(v.inputs[0].resolvable);
        assert!(!v.nondeterministic);
    }

    #[test]
    fn classify_non_replayable_when_upstream_artifact_absent() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        // Upstream artifact for HA is never recorded.
        seed(
            &store,
            "r",
            "leaf",
            "SELECT id FROM tgt.raw.root",
            &[content("root", HA)],
            HB,
            true,
        );
        let v = classify_model(&store, "r", "leaf");
        assert_eq!(v.verdict, "non_replayable");
        assert!(v.has_provenance && v.ir_parseable);
        assert!(!v.inputs[0].resolvable);
        assert!(
            v.reasons
                .iter()
                .any(|r| r.contains("absent from the artifact ledger"))
        );
    }

    #[test]
    fn classify_nondeterministic_flag_does_not_demote() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        seed(&store, "r", "root", "SELECT 1 AS id", &[], HA, true);
        seed(
            &store,
            "r",
            "nd",
            "SELECT id, now() AS t FROM tgt.raw.root",
            &[content("root", HA)],
            HB,
            true,
        );
        let v = classify_model(&store, "r", "nd");
        assert_eq!(
            v.verdict, "replayable",
            "nondeterminism is a flag, not a demotion"
        );
        assert!(v.nondeterministic);
    }

    #[test]
    fn short_hash_truncates() {
        assert_eq!(short_hash("0123456789abcdef"), "0123456789ab");
        assert_eq!(short_hash("short"), "short");
    }

    // -- `--execute` re-execution -----------------------------------------

    #[cfg(feature = "duckdb")]
    #[test]
    fn deterministic_replay_state_maps_logical_to_ordinal() {
        let cols = vec![
            TypedColumn {
                name: "id".into(),
                data_type: RockyType::Int64,
                nullable: false,
            },
            TypedColumn {
                name: "name".into(),
                data_type: RockyType::String,
                nullable: true,
            },
        ];
        let state = deterministic_replay_state(&cols);
        assert_eq!(state.physical.get("id").map(String::as_str), Some("id"));
        assert_eq!(state.physical.get("name").map(String::as_str), Some("name"));
        assert_eq!(state.field_id.get("id"), Some(&1));
        assert_eq!(state.field_id.get("name"), Some(&2));
        assert!(state.partition_columns.is_empty());
    }

    #[cfg(feature = "duckdb")]
    fn fresh_engine() -> rocky_duckdb::adapter::DuckDbWarehouseAdapter {
        rocky_duckdb::adapter::DuckDbWarehouseAdapter::in_memory().unwrap()
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn execute_and_hash_is_stable_across_calls() {
        let ir = ca_ir("root", "SELECT CAST(1 AS BIGINT) AS id");
        let (rows_a, hash_a) = execute_and_hash(&fresh_engine(), &ir).await.unwrap();
        let (rows_b, hash_b) = execute_and_hash(&fresh_engine(), &ir).await.unwrap();
        assert_eq!(rows_a, 1);
        assert_eq!(rows_b, 1);
        assert_eq!(
            hash_a, hash_b,
            "a deterministic recipe must re-derive an identical blake3 across executions"
        );
        assert_eq!(hash_a.len(), 64, "blake3 hex digest is 64 chars");
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn execute_and_hash_differs_for_different_output() {
        let one = ca_ir("root", "SELECT CAST(1 AS BIGINT) AS id");
        let two = ca_ir("root", "SELECT CAST(2 AS BIGINT) AS id");
        let (_, h1) = execute_and_hash(&fresh_engine(), &one).await.unwrap();
        let (_, h2) = execute_and_hash(&fresh_engine(), &two).await.unwrap();
        assert_ne!(
            h1, h2,
            "different output bytes must yield different digests (content-sensitive hash)"
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn replay_execute_bit_exact_when_seeded_with_real_hash() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let sql = "SELECT CAST(7 AS BIGINT) AS id";
        // The recorded hash is produced by a genuine prior execution.
        let (_, real) = execute_and_hash(&fresh_engine(), &ca_ir("root", sql))
            .await
            .unwrap();
        seed(&store, "r", "root", sql, &[], &real, true);
        let v = replay_execute_model(&store, "r", "root", true).await;
        assert_eq!(v.verdict, "bit_exact");
        assert_eq!(v.computed_hash.as_deref(), Some(real.as_str()));
        assert_eq!(v.recorded_hash.as_deref(), Some(real.as_str()));
        assert_eq!(v.rows, Some(1));
        assert!(!v.nondeterministic);
        assert!(v.reasons.is_empty());
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn replay_execute_mutated_recipe_diverges() {
        // Seed the recorded hash from recipe-1's real output, but store
        // recipe-2 AS the recording. Replay reconstructs recipe-2, executes
        // it, and its digest must NOT match recipe-1's — proving the verdict
        // is a function of the re-executed bytes, not an echo of the seed.
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let (_, hash_one) = execute_and_hash(
            &fresh_engine(),
            &ca_ir("root", "SELECT CAST(1 AS BIGINT) AS id"),
        )
        .await
        .unwrap();
        seed(
            &store,
            "r",
            "root",
            "SELECT CAST(2 AS BIGINT) AS id",
            &[],
            &hash_one,
            true,
        );
        let v = replay_execute_model(&store, "r", "root", true).await;
        assert_eq!(v.verdict, "diverged");
        assert_eq!(v.recorded_hash.as_deref(), Some(hash_one.as_str()));
        assert_ne!(v.computed_hash.as_deref(), Some(hash_one.as_str()));
        assert!(!v.nondeterministic);
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn replay_execute_non_replayable_with_content_upstream() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        seed(
            &store,
            "r",
            "root",
            "SELECT CAST(1 AS BIGINT) AS id",
            &[],
            HA,
            true,
        );
        seed(
            &store,
            "r",
            "leaf",
            "SELECT id FROM tgt.raw.root",
            &[content("root", HA)],
            HB,
            true,
        );
        let v = replay_execute_model(&store, "r", "leaf", true).await;
        assert_eq!(v.verdict, "non_replayable");
        assert!(v.computed_hash.is_none());
        assert!(
            v.reasons
                .iter()
                .any(|r| r.contains("self-contained recipe"))
        );
    }

    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn replay_execute_without_verify_reports_executed() {
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("s.redb")).unwrap();
        let sql = "SELECT CAST(3 AS BIGINT) AS id";
        // The recorded hash is irrelevant here — no comparison is made.
        seed(&store, "r", "root", sql, &[], HA, true);
        let v = replay_execute_model(&store, "r", "root", false).await;
        assert_eq!(v.verdict, "executed");
        assert!(v.computed_hash.is_some());
        assert_eq!(v.rows, Some(1));
        assert!(v.reasons.is_empty());
    }
}
