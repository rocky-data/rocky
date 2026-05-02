//! `rocky history` — display run history from the state store.

use std::path::Path;

use anyhow::Result;
use chrono::{DateTime, Utc};

use rocky_core::state::{RunRecord, SessionSource, StateStore};

use crate::output::{
    HistoryOutput, ModelExecutionRecord, ModelHistoryOutput, RollingDimension, RollingStats,
    RunHistoryRecord, RunModelRecord, print_json,
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
///
/// When `rolling_stats` is true and `model_filter` is set, augments
/// `ModelHistoryOutput` with a [`RollingStats`] block computed over the
/// `window` most recent successful executions.
pub fn run_history(
    state_path: &Path,
    model_filter: Option<&str>,
    since: Option<&str>,
    audit: bool,
    rolling_stats: bool,
    window: usize,
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
        // Fetch a wide enough pool so that rolling stats can find `window`
        // successful executions even when recent history includes failures.
        let fetch_limit = if rolling_stats {
            window.saturating_mul(5).max(100)
        } else {
            50
        };
        let executions = store.get_model_history(model_name, fetch_limit)?;

        let filtered: Vec<_> = if let Some(ts) = since_ts {
            executions
                .into_iter()
                .filter(|e| e.started_at >= ts)
                .collect()
        } else {
            executions
        };

        // Compute rolling stats before consuming `filtered`.
        let stats: Option<RollingStats> = if rolling_stats {
            Some(compute_rolling_stats(&filtered, window))
        } else {
            None
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
                rolling_stats: stats,
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
                .map(|r| record_to_history(r, audit))
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
        let identity = run.triggering_identity.as_deref().unwrap_or("-");
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

/// Compute population mean and std dev for a slice of `f64` values.
/// Returns `(mean, std_dev)`. Returns `(0.0, 0.0)` for an empty slice.
fn pop_mean_std(values: &[f64]) -> (f64, f64) {
    let n = values.len();
    if n == 0 {
        return (0.0, 0.0);
    }
    let mean = values.iter().sum::<f64>() / n as f64;
    let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / n as f64;
    (mean, variance.sqrt())
}

/// Compute z-score of `value` given `mean` and `std_dev`.
/// Returns `None` when `std_dev` is 0.
fn z_score(value: f64, mean: f64, std_dev: f64) -> Option<f64> {
    if std_dev == 0.0 {
        None
    } else {
        Some((value - mean) / std_dev)
    }
}

/// Compute rolling stats over `executions`, taking up to `window` most-recent
/// successful executions (status `"success"`, case-insensitive lowercase).
///
/// `executions` is expected to be newest-first (as returned by
/// `StateStore::get_model_history`). The most-recent successful execution
/// supplies the "latest" value used for the z-score.
fn compute_rolling_stats(
    executions: &[rocky_core::state::ModelExecution],
    window: usize,
) -> RollingStats {
    // Filter to successful executions, newest-first, take up to `window`.
    let successful: Vec<_> = executions
        .iter()
        .filter(|e| e.status.eq_ignore_ascii_case("success"))
        .take(window)
        .collect();

    let samples = successful.len();

    // Duration dimension — all successful executions contribute.
    let duration_values: Vec<f64> = successful.iter().map(|e| e.duration_ms as f64).collect();
    let (dur_mean, dur_std) = pop_mean_std(&duration_values);
    let dur_latest = successful.first().map(|e| e.duration_ms as f64);
    let dur_z = if samples >= 2 {
        dur_latest.and_then(|v| z_score(v, dur_mean, dur_std))
    } else {
        None
    };

    // Rows dimension — only executions where rows_affected is Some.
    let rows_values: Vec<f64> = successful
        .iter()
        .filter_map(|e| e.rows_affected.map(|r| r as f64))
        .collect();
    let (rows_mean, rows_std) = pop_mean_std(&rows_values);
    // "Latest" for rows: the most-recent successful execution that has rows_affected set.
    let rows_latest = successful
        .iter()
        .find_map(|e| e.rows_affected.map(|r| r as f64));
    let rows_z = if rows_values.len() >= 2 {
        rows_latest.and_then(|v| z_score(v, rows_mean, rows_std))
    } else {
        None
    };

    // Health score: 1.0 - clamp((max(|z_rows|, |z_duration|) - 2) / 4, 0, 1)
    let max_abs_z = match (rows_z, dur_z) {
        (Some(rz), Some(dz)) => rz.abs().max(dz.abs()),
        (Some(rz), None) => rz.abs(),
        (None, Some(dz)) => dz.abs(),
        (None, None) => 0.0,
    };
    let health_score = 1.0 - ((max_abs_z - 2.0) / 4.0).clamp(0.0, 1.0);

    RollingStats {
        window,
        samples,
        rows_affected: RollingDimension {
            mean: rows_mean,
            std_dev: rows_std,
            latest_z_score: rows_z,
        },
        duration_ms: RollingDimension {
            mean: dur_mean,
            std_dev: dur_std,
            latest_z_score: dur_z,
        },
        health_score,
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

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_exec(duration_ms: u64, rows_affected: Option<u64>, status: &str) -> ModelExecution {
        ModelExecution {
            model_name: "orders".to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            duration_ms,
            rows_affected,
            status: status.to_string(),
            sql_hash: "abc".to_string(),
            bytes_scanned: None,
            bytes_written: None,
        }
    }

    // ── rolling stats unit tests ─────────────────────────────────────────────

    /// All samples equal → std_dev = 0, z_score = None, health = 1.0.
    #[test]
    fn rolling_stats_all_equal_samples() {
        let execs = vec![
            make_exec(100, Some(50), "success"),
            make_exec(100, Some(50), "success"),
            make_exec(100, Some(50), "success"),
        ];
        let stats = compute_rolling_stats(&execs, 20);
        assert_eq!(stats.samples, 3);
        assert!((stats.duration_ms.mean - 100.0).abs() < 1e-9);
        assert!((stats.duration_ms.std_dev).abs() < 1e-9);
        assert!(
            stats.duration_ms.latest_z_score.is_none(),
            "std_dev=0 → z=None"
        );
        assert!((stats.rows_affected.mean - 50.0).abs() < 1e-9);
        assert!(stats.rows_affected.std_dev.abs() < 1e-9);
        assert!(stats.rows_affected.latest_z_score.is_none());
        assert!((stats.health_score - 1.0).abs() < 1e-9);
    }

    /// Window of size 1: single sample → std_dev = 0, z = None, health = 1.0.
    #[test]
    fn rolling_stats_single_sample() {
        let execs = vec![make_exec(200, Some(10), "success")];
        let stats = compute_rolling_stats(&execs, 20);
        assert_eq!(stats.samples, 1);
        assert!((stats.duration_ms.mean - 200.0).abs() < 1e-9);
        assert!(stats.duration_ms.std_dev.abs() < 1e-9);
        assert!(stats.duration_ms.latest_z_score.is_none());
        assert!((stats.rows_affected.mean - 10.0).abs() < 1e-9);
        assert!(stats.rows_affected.latest_z_score.is_none());
        assert!((stats.health_score - 1.0).abs() < 1e-9);
    }

    /// Normal case — hand-computed values.
    ///
    /// Samples (newest-first, as `get_model_history` returns):
    ///   duration_ms = [30, 20, 10]  → mean=20, pop-std=√(200/3) ≈ 8.165
    ///   rows_affected = [30, 20, 10]
    ///   latest (first element) = 30
    ///   z_duration = (30 - 20) / √(200/3) ≈ 1.2247
    ///   z_rows = same
    ///   max |z| ≈ 1.2247 < 2 → health = 1.0
    #[test]
    fn rolling_stats_normal_case() {
        // Newest-first
        let execs = vec![
            make_exec(30, Some(30), "success"),
            make_exec(20, Some(20), "success"),
            make_exec(10, Some(10), "success"),
        ];
        let stats = compute_rolling_stats(&execs, 20);
        assert_eq!(stats.samples, 3);

        let expected_mean = 20.0_f64;
        let expected_std = (200.0_f64 / 3.0).sqrt();
        let expected_z = (30.0 - expected_mean) / expected_std;

        assert!((stats.duration_ms.mean - expected_mean).abs() < 1e-9);
        assert!((stats.duration_ms.std_dev - expected_std).abs() < 1e-9);
        let dz = stats
            .duration_ms
            .latest_z_score
            .expect("z_score should be Some");
        assert!((dz - expected_z).abs() < 1e-9);

        assert!((stats.rows_affected.mean - expected_mean).abs() < 1e-9);
        let rz = stats
            .rows_affected
            .latest_z_score
            .expect("rows z_score should be Some");
        assert!((rz - expected_z).abs() < 1e-9);

        // max |z| ≈ 1.2247 < 2 → health_score = 1.0
        assert!((stats.health_score - 1.0).abs() < 1e-9);
    }

    /// Non-successful executions are excluded from rolling stats.
    #[test]
    fn rolling_stats_filters_non_success() {
        let execs = vec![
            make_exec(100, Some(10), "success"),
            make_exec(999, Some(9999), "failure"),
            make_exec(100, Some(10), "success"),
        ];
        let stats = compute_rolling_stats(&execs, 20);
        assert_eq!(stats.samples, 2, "failure row should be excluded");
        assert!((stats.duration_ms.mean - 100.0).abs() < 1e-9);
    }

    /// Health degrades linearly from 1.0 at |z|=2 to 0.0 at |z|=6.
    #[test]
    fn rolling_stats_health_score_formula() {
        // Construct samples where latest z ≈ 4 (midpoint → health = 0.5).
        // Use values [0, 0, ..., 0, 8] (9 zeros + one 8) → mean=0.8, pop-std ≈ 2.4,
        // latest (8) → z ≈ 3.0 → health = 1 - (3-2)/4 = 0.75
        let mut execs: Vec<ModelExecution> =
            std::iter::repeat_with(|| make_exec(0, Some(0), "success"))
                .take(9)
                .collect();
        // Prepend the "newest" outlier (duration=8, rows=8)
        execs.insert(0, make_exec(8, Some(8), "success"));

        let stats = compute_rolling_stats(&execs, 20);
        // mean = (8 + 0*9) / 10 = 0.8
        // variance = (sum of (v - 0.8)^2) / 10 = (9 * 0.64 + 1 * 51.84) / 10 = 5.76 → std = 2.4
        let mean = 0.8_f64;
        let std = 2.4_f64;
        let z = (8.0 - mean) / std; // ≈ 3.0
        let expected_health = 1.0 - ((z - 2.0) / 4.0).clamp(0.0, 1.0);

        assert!((stats.duration_ms.mean - mean).abs() < 1e-9);
        assert!((stats.duration_ms.std_dev - std).abs() < 1e-9);
        let dz = stats.duration_ms.latest_z_score.expect("z should be Some");
        assert!((dz - z).abs() < 1e-9);
        assert!((stats.health_score - expected_health).abs() < 1e-9);
    }
}
