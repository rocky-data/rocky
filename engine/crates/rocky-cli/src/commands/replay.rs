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

/// Re-execute a reconstructed recipe on an ephemeral in-memory DuckDB engine
/// and return `(rows, output_blake3_hex)`.
///
/// The recipe's `SELECT` runs against a throwaway in-memory database — nothing
/// is persisted to any schema, so no production identity is touched (isolation
/// is vacuous for a self-contained `SELECT`). The result rows are converted to
/// the same Arrow → Parquet encoding the content-addressed writer emits and
/// hashed with blake3, so the digest is directly comparable to the recorded
/// output hash.
#[cfg(feature = "duckdb")]
async fn execute_and_hash(ir: &ModelIr) -> Result<(u64, String)> {
    use rocky_core::traits::WarehouseAdapter;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    let adapter = DuckDbWarehouseAdapter::in_memory()
        .context("failed to start an in-memory DuckDB engine for replay re-execution")?;
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

    // Re-execute + re-derive the output digest. Any failure is fail-closed to
    // non_replayable — never diverged, never bit_exact.
    let (rows, computed) = match execute_and_hash(&ir).await {
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

/// Execute `rocky replay --execute [--verify]` — single-model DuckDB
/// re-execution.
///
/// Reconstructs each targeted model's recipe from its recorded
/// [`ProvenanceRecord`], re-executes the self-contained ones on an ephemeral
/// in-memory DuckDB engine, and (with `verify`) compares the re-derived output
/// blake3 against the recorded hash. Nothing is materialized to any warehouse
/// schema, and the working tree is never consulted. Every verdict — including
/// `diverged` and `non_replayable` — is a classification, not a tool failure,
/// so this returns `Ok` (exit 0) unless the run itself cannot be resolved;
/// callers inspect `verdict` / `bit_exact_count`.
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

    let mut models: Vec<ReplayExecuteModelOutput> = Vec::new();
    for exec in &record.models_executed {
        if let Some(name) = model_filter
            && exec.model_name != name
        {
            continue;
        }
        models.push(replay_execute_model(&store, &record.run_id, &exec.model_name, verify).await);
    }

    if let Some(name) = model_filter
        && models.is_empty()
    {
        anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
    }

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
    #[tokio::test]
    async fn execute_and_hash_is_stable_across_calls() {
        let ir = ca_ir("root", "SELECT CAST(1 AS BIGINT) AS id");
        let (rows_a, hash_a) = execute_and_hash(&ir).await.unwrap();
        let (rows_b, hash_b) = execute_and_hash(&ir).await.unwrap();
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
        let (_, h1) = execute_and_hash(&one).await.unwrap();
        let (_, h2) = execute_and_hash(&two).await.unwrap();
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
        let (_, real) = execute_and_hash(&ca_ir("root", sql)).await.unwrap();
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
        let (_, hash_one) = execute_and_hash(&ca_ir("root", "SELECT CAST(1 AS BIGINT) AS id"))
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
