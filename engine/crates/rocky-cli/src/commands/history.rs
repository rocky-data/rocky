//! `rocky history` — display run history from the state store.

use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use rocky_core::state::{RunRecord, SessionSource, StateStore};

use crate::output::{
    HistoryOutput, ModelExecutionRecord, ModelHistoryOutput, RunHistoryRecord, RunModelRecord,
    print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Convert a [`SessionSource`] enum value to its JSON wire string.
/// Kept as a free fn (not a `Display` impl) so the string shape stays
/// local to `history.rs` — adding or renaming a variant shouldn't ripple
/// through other `rocky` commands that read the enum without emitting
/// JSON.
fn session_source_str(source: SessionSource) -> &'static str {
    match source {
        SessionSource::Cli => "cli",
        SessionSource::Dagster => "dagster",
        SessionSource::Lsp => "lsp",
        SessionSource::HttpApi => "http_api",
    }
}

/// Build a [`RunHistoryRecord`] from the state-store [`RunRecord`].
/// The `audit` flag controls whether the 8-field governance audit
/// trail is populated on the output record — default `false` keeps the
/// JSON payload byte-identical to schema v5 for downstream consumers
/// that haven't updated yet.
fn record_to_history(run: &RunRecord, audit: bool) -> RunHistoryRecord {
    let duration_ms = (run.finished_at - run.started_at).num_milliseconds().max(0) as u64;
    let models: Vec<RunModelRecord> = run
        .models_executed
        .iter()
        .map(|m| RunModelRecord {
            model_name: m.model_name.clone(),
            duration_ms: m.duration_ms,
            rows_affected: m.rows_affected,
            status: m.status.clone(),
        })
        .collect();
    let (
        triggering_identity,
        session_source,
        git_commit,
        git_branch,
        idempotency_key,
        target_catalog,
        hostname,
        rocky_version,
    ) = if audit {
        (
            run.triggering_identity.clone(),
            Some(session_source_str(run.session_source).to_string()),
            run.git_commit.clone(),
            run.git_branch.clone(),
            run.idempotency_key.clone(),
            run.target_catalog.clone(),
            Some(run.hostname.clone()),
            Some(run.rocky_version.clone()),
        )
    } else {
        (None, None, None, None, None, None, None, None)
    };
    RunHistoryRecord {
        run_id: run.run_id.clone(),
        started_at: run.started_at,
        status: format!("{:?}", run.status),
        trigger: format!("{:?}", run.trigger),
        models_executed: run.models_executed.len(),
        duration_ms,
        models,
        triggering_identity,
        session_source,
        git_commit,
        git_branch,
        idempotency_key,
        target_catalog,
        hostname,
        rocky_version,
    }
}

/// Execute `rocky history`.
///
/// When `audit` is true, both the JSON output (the eight new optional
/// fields on [`RunHistoryRecord`]) and the text output (a second
/// governance table below the default table) include the full audit
/// trail. Defaults preserved so existing consumers don't see a
/// schema change.
pub fn run_history(
    state_path: &Path,
    model_filter: Option<&str>,
    since: Option<&str>,
    audit: bool,
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
            let runs: Vec<RunHistoryRecord> =
                filtered.iter().map(|r| record_to_history(r, audit)).collect();
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

            if audit {
                print_audit_table(&filtered);
            }
        }
    }

    Ok(())
}

/// Print the governance audit trail as a second table below the
/// default run summary. Keeps the default output untouched; operators
/// explicitly pass `--audit` when they need the governance view.
///
/// Column widths are intentionally modest — `git_commit` is printed
/// short (8 chars) and `hostname` truncated to 16, so the table stays
/// usable in a 160-column terminal.
fn print_audit_table(runs: &[RunRecord]) {
    if runs.is_empty() {
        return;
    }
    println!();
    println!("Governance audit trail (--audit):");
    println!(
        "{:<12} {:<18} {:<8} {:<10} {:<16} {:<20} {:<12}",
        "RUN ID", "IDENTITY", "SOURCE", "COMMIT", "BRANCH", "CATALOG", "HOST"
    );
    println!("{}", "-".repeat(100));
    for run in runs {
        let run_id = &run.run_id[..run.run_id.len().min(11)];
        let identity = run
            .triggering_identity
            .as_deref()
            .unwrap_or("-");
        let identity = if identity.len() > 17 {
            &identity[..17]
        } else {
            identity
        };
        let source = session_source_str(run.session_source);
        let commit = run
            .git_commit
            .as_deref()
            .map(|c| &c[..c.len().min(8)])
            .unwrap_or("-");
        let branch_full = run.git_branch.as_deref().unwrap_or("-");
        let branch = if branch_full.len() > 15 {
            &branch_full[..15]
        } else {
            branch_full
        };
        let catalog_full = run.target_catalog.as_deref().unwrap_or("-");
        let catalog = if catalog_full.len() > 19 {
            &catalog_full[..19]
        } else {
            catalog_full
        };
        let host_full = run.hostname.as_str();
        let host = if host_full.len() > 11 {
            &host_full[..11]
        } else {
            host_full
        };
        println!(
            "{:<12} {:<18} {:<8} {:<10} {:<16} {:<20} {:<12}",
            run_id, identity, source, commit, branch, catalog, host
        );
    }
    println!();
    // Emit version + idempotency key as an extra per-run detail line
    // because they don't fit a fixed-column layout cleanly.
    for run in runs {
        let run_id = &run.run_id[..run.run_id.len().min(11)];
        let key = run.idempotency_key.as_deref().unwrap_or("-");
        println!(
            "  {}  version={}  idempotency_key={}",
            run_id, run.rocky_version, key
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::state::{ModelExecution, RunStatus, RunTrigger};

    fn sample_record() -> RunRecord {
        RunRecord {
            run_id: "run-abc-001".to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now() + chrono::Duration::seconds(30),
            status: RunStatus::Success,
            models_executed: vec![ModelExecution {
                model_name: "orders".to_string(),
                started_at: Utc::now(),
                finished_at: Utc::now(),
                duration_ms: 1234,
                rows_affected: Some(100),
                status: "success".to_string(),
                sql_hash: "hash1".to_string(),
                bytes_scanned: None,
                bytes_written: None,
            }],
            trigger: RunTrigger::Manual,
            config_hash: "cfg-hash".to_string(),
            triggering_identity: Some("hugo".to_string()),
            session_source: SessionSource::Dagster,
            git_commit: Some("1234567890abcdef1234567890abcdef12345678".to_string()),
            git_branch: Some("feat/governance".to_string()),
            idempotency_key: Some("my-key".to_string()),
            target_catalog: Some("warehouse_main".to_string()),
            hostname: "dev-laptop".to_string(),
            rocky_version: "1.16.0".to_string(),
        }
    }

    #[test]
    fn record_to_history_skips_audit_fields_by_default() {
        let record = sample_record();
        let history = record_to_history(&record, false);
        assert_eq!(history.run_id, "run-abc-001");
        assert_eq!(history.trigger, "Manual");
        assert!(history.triggering_identity.is_none());
        assert!(history.session_source.is_none());
        assert!(history.git_commit.is_none());
        assert!(history.hostname.is_none());
        assert!(history.rocky_version.is_none());
    }

    #[test]
    fn record_to_history_populates_audit_fields_when_requested() {
        let record = sample_record();
        let history = record_to_history(&record, true);
        assert_eq!(history.triggering_identity.as_deref(), Some("hugo"));
        assert_eq!(history.session_source.as_deref(), Some("dagster"));
        assert_eq!(
            history.git_commit.as_deref(),
            Some("1234567890abcdef1234567890abcdef12345678")
        );
        assert_eq!(history.git_branch.as_deref(), Some("feat/governance"));
        assert_eq!(history.idempotency_key.as_deref(), Some("my-key"));
        assert_eq!(history.target_catalog.as_deref(), Some("warehouse_main"));
        assert_eq!(history.hostname.as_deref(), Some("dev-laptop"));
        assert_eq!(history.rocky_version.as_deref(), Some("1.16.0"));
    }

    #[test]
    fn session_source_str_round_trip() {
        assert_eq!(session_source_str(SessionSource::Cli), "cli");
        assert_eq!(session_source_str(SessionSource::Dagster), "dagster");
        assert_eq!(session_source_str(SessionSource::Lsp), "lsp");
        assert_eq!(session_source_str(SessionSource::HttpApi), "http_api");
    }
}
