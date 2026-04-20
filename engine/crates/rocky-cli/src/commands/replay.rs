//! `rocky replay <run_id|latest>` — inspect a recorded run.
//!
//! Surfaces the per-model SQL hashes, row counts, bytes, and timings captured
//! by the state store's `RunRecord`. Re-execution with pinned inputs is an
//! Arc-1 follow-up once the content-addressed write path arrives — the
//! inspection surface exists today so the reproducibility claim has a
//! concrete artefact to point at.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger, StateStore};

use crate::output::{ReplayModelOutput, ReplayOutput};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn status_str(status: &RunStatus) -> &'static str {
    match status {
        RunStatus::Success => "success",
        RunStatus::PartialFailure => "partial_failure",
        RunStatus::Failure => "failure",
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

    if let Some(name) = model_filter {
        if models.is_empty() {
            anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
        }
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
                bytes_scanned: Some(1024),
                bytes_written: Some(2048),
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
}
