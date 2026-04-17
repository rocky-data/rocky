//! `rocky history` — display run history from the state store.

use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use rocky_core::state::StateStore;

use crate::output::{
    HistoryOutput, ModelExecutionRecord, ModelHistoryOutput, RunHistoryRecord, RunModelRecord,
    print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky history`.
pub fn run_history(
    state_path: &Path,
    model_filter: Option<&str>,
    since: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)?;

    let since_ts: Option<DateTime<Utc>> = since
        .map(|s| {
            s.parse::<DateTime<Utc>>()
                .or_else(|_| {
                    // Try parsing as date-only (YYYY-MM-DD)
                    chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
                })
                .map_err(|e| anyhow::anyhow!("invalid --since date: {e}"))
        })
        .transpose()?;

    if let Some(model_name) = model_filter {
        // Show history for a specific model
        let executions = store.get_model_history(model_name, 50)?;

        let filtered: Vec<_> = if let Some(ts) = since_ts {
            executions
                .into_iter()
                .filter(|e| e.started_at >= ts)
                .collect()
        } else {
            executions
        };

        if output_json {
            let executions: Vec<ModelExecutionRecord> = filtered
                .iter()
                .map(|e| ModelExecutionRecord {
                    started_at: e.started_at,
                    duration_ms: e.duration_ms,
                    rows_affected: e.rows_affected,
                    status: format!("{:?}", e.status),
                    sql_hash: e.sql_hash.clone(),
                })
                .collect();
            let output = ModelHistoryOutput {
                version: VERSION.to_string(),
                command: "history".to_string(),
                model: model_name.to_string(),
                count: executions.len(),
                executions,
            };
            print_json(&output)?;
        } else {
            println!("History for model: {model_name}");
            println!(
                "{:<24} {:<10} {:<12} {:<14} {:<10}",
                "STARTED", "DURATION", "ROWS", "STATUS", "SQL HASH"
            );
            println!("{}", "-".repeat(72));

            for exec in &filtered {
                println!(
                    "{:<24} {:<10} {:<12} {:<14} {:<10}",
                    exec.started_at.format("%Y-%m-%d %H:%M:%S"),
                    format!("{}ms", exec.duration_ms),
                    exec.rows_affected
                        .map(|r| r.to_string())
                        .unwrap_or_else(|| "-".to_string()),
                    exec.status,
                    &exec.sql_hash[..exec.sql_hash.len().min(8)],
                );
            }
            println!("\nTotal executions: {}", filtered.len());
        }
    } else {
        // Show all runs
        let runs = store.list_runs(50)?;

        let filtered: Vec<_> = if let Some(ts) = since_ts {
            runs.into_iter().filter(|r| r.started_at >= ts).collect()
        } else {
            runs
        };

        if output_json {
            let runs: Vec<RunHistoryRecord> = filtered
                .iter()
                .map(|r| {
                    let duration_ms =
                        (r.finished_at - r.started_at).num_milliseconds().max(0) as u64;
                    let models: Vec<RunModelRecord> = r
                        .models_executed
                        .iter()
                        .map(|m| RunModelRecord {
                            model_name: m.model_name.clone(),
                            duration_ms: m.duration_ms,
                            rows_affected: m.rows_affected,
                            status: m.status.clone(),
                        })
                        .collect();
                    RunHistoryRecord {
                        run_id: r.run_id.clone(),
                        started_at: r.started_at,
                        status: format!("{:?}", r.status),
                        trigger: format!("{:?}", r.trigger),
                        models_executed: r.models_executed.len(),
                        duration_ms,
                        models,
                    }
                })
                .collect();
            let output = HistoryOutput {
                version: VERSION.to_string(),
                command: "history".to_string(),
                count: runs.len(),
                runs,
            };
            print_json(&output)?;
        } else {
            println!(
                "{:<12} {:<24} {:<10} {:<8} {:<10}",
                "RUN ID", "STARTED", "STATUS", "MODELS", "TRIGGER"
            );
            println!("{}", "-".repeat(66));

            for run in &filtered {
                let status = format!("{:?}", run.status);
                let trigger = format!("{:?}", run.trigger);
                println!(
                    "{:<12} {:<24} {:<10} {:<8} {:<10}",
                    &run.run_id[..run.run_id.len().min(11)],
                    run.started_at.format("%Y-%m-%d %H:%M:%S"),
                    status,
                    run.models_executed.len(),
                    trigger,
                );
            }
            println!("\nTotal runs: {}", filtered.len());
        }
    }

    Ok(())
}
