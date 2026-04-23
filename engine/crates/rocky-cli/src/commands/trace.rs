//! `rocky trace <run_id|latest>` — render a completed run as a timeline.
//!
//! Sibling command to `rocky replay` but tuned for the observability
//! trust outcome ("I can see everything that happened"): offsets are
//! expressed relative to the run start, so the human output reads as a
//! DAG timeline instead of a flat per-model list. Data source is the
//! same `RunRecord` / `ModelExecution` in the state store — nothing new
//! gets recorded here, this is a renderer.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger, StateStore};

use crate::output::{TraceModelEntry, TraceOutput};

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

/// Resolve `target` (either a literal `run_id` or the alias `"latest"`)
/// against the state store. Mirrors `rocky replay`'s convention so users
/// who already know `rocky replay latest` don't have to learn a second
/// syntax for this command.
fn resolve(store: &StateStore, target: &str) -> Result<RunRecord> {
    if target == "latest" {
        let runs = store.list_runs(1)?;
        return runs
            .into_iter()
            .next()
            .context("no runs recorded yet — nothing to trace");
    }
    store
        .get_run(target)?
        .with_context(|| format!("no run with id '{target}' in the state store"))
}

fn offset_ms(run_start: chrono::DateTime<chrono::Utc>, t: chrono::DateTime<chrono::Utc>) -> i64 {
    (t - run_start).num_milliseconds()
}

/// Convert a [`ModelExecution`] into the CLI-output shape. Offsets are
/// expressed relative to the run start so the consumer can draw a Gantt
/// without re-deriving the base timestamp.
fn to_entry(run_start: chrono::DateTime<chrono::Utc>, exec: &ModelExecution) -> TraceModelEntry {
    TraceModelEntry {
        model_name: exec.model_name.clone(),
        status: exec.status.clone(),
        start_offset_ms: offset_ms(run_start, exec.started_at).max(0),
        duration_ms: exec.duration_ms,
        sql_hash: exec.sql_hash.clone(),
        lane: 0,
        rows_affected: exec.rows_affected,
        bytes_scanned: exec.bytes_scanned,
        bytes_written: exec.bytes_written,
    }
}

/// Assign entries to concurrency lanes. A lane is a set of entries
/// that never overlap in time, so rendering them on the same row
/// produces a readable Gantt. Uses a greedy first-fit: sort by start
/// offset, then place each entry on the first lane whose current end
/// is ≤ the entry's start. Writes the lane index into each entry's
/// `lane` field and returns the total lane count.
fn assign_lanes(entries: &mut [TraceModelEntry]) -> usize {
    let mut order: Vec<usize> = (0..entries.len()).collect();
    order.sort_by_key(|&i| entries[i].start_offset_ms);

    let mut lane_end_ms: Vec<i64> = Vec::new();

    for idx in order {
        let start = entries[idx].start_offset_ms;
        let end = start + entries[idx].duration_ms as i64;

        let lane = lane_end_ms
            .iter()
            .position(|&e| e <= start)
            .unwrap_or_else(|| {
                lane_end_ms.push(0);
                lane_end_ms.len() - 1
            });
        lane_end_ms[lane] = end;
        entries[idx].lane = lane;
    }

    lane_end_ms.len()
}

/// Render a Gantt-style row: `name [▓▓░░░] duration  status  rows  bytes`.
fn render_row(entry: &TraceModelEntry, run_duration_ms: i64, bar_width: usize) -> String {
    let run_ms = run_duration_ms.max(1);
    let start_frac = (entry.start_offset_ms as f64) / (run_ms as f64);
    let end_frac = ((entry.start_offset_ms + entry.duration_ms as i64) as f64) / (run_ms as f64);
    let start_col = ((start_frac * bar_width as f64).round() as usize).min(bar_width);
    let end_col = ((end_frac * bar_width as f64).round() as usize)
        .max(start_col + 1)
        .min(bar_width);

    let mut bar = String::with_capacity(bar_width);
    for i in 0..bar_width {
        if i < start_col {
            bar.push('.');
        } else if i < end_col {
            bar.push('#');
        } else {
            bar.push('.');
        }
    }

    let mut line = format!(
        "  {name:<28}  [{bar}]  {duration_s:>7.2}s  {status}",
        name = truncate(&entry.model_name, 28),
        bar = bar,
        duration_s = entry.duration_ms as f64 / 1000.0,
        status = entry.status,
    );
    if let Some(rows) = entry.rows_affected {
        line.push_str(&format!("  rows={rows}"));
    }
    if let Some(bytes) = entry.bytes_written {
        line.push_str(&format!("  bytes_written={bytes}"));
    }
    line
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        s.to_string()
    } else {
        let cut = max.saturating_sub(1);
        format!("{}…", &s[..cut])
    }
}

/// Execute `rocky trace`.
pub fn run_trace(
    state_path: &Path,
    target: &str,
    model_filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = resolve(&store, target)?;
    let run_duration_ms = (record.finished_at - record.started_at).num_milliseconds();

    let mut entries: Vec<TraceModelEntry> = record
        .models_executed
        .iter()
        .filter(|m| match model_filter {
            Some(name) => m.model_name == name,
            None => true,
        })
        .map(|m| to_entry(record.started_at, m))
        .collect();

    entries.sort_by_key(|e| e.start_offset_ms);

    if let Some(name) = model_filter
        && entries.is_empty()
    {
        anyhow::bail!("run '{}' did not execute model '{name}'", record.run_id);
    }

    let lane_count = assign_lanes(&mut entries);

    if json {
        let output = TraceOutput {
            version: VERSION.to_string(),
            command: "trace".to_string(),
            run_id: record.run_id.clone(),
            status: status_str(&record.status).to_string(),
            trigger: trigger_str(&record.trigger).to_string(),
            started_at: record.started_at.to_rfc3339(),
            finished_at: record.finished_at.to_rfc3339(),
            run_duration_ms: run_duration_ms.max(0) as u64,
            lane_count,
            models: entries,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("run: {}", record.run_id);
        println!(
            "status: {}   trigger: {}   duration: {:.2}s",
            status_str(&record.status),
            trigger_str(&record.trigger),
            run_duration_ms.max(0) as f64 / 1000.0,
        );
        println!("started:  {}", record.started_at.to_rfc3339());
        println!("finished: {}", record.finished_at.to_rfc3339());
        if lane_count > 1 {
            println!("parallelism: {lane_count} lanes");
        }
        println!();
        let bar_width: usize = 40;
        println!(
            "  {:<28}  {:<bar$}    duration  status",
            "model",
            "timeline",
            bar = bar_width + 2,
        );
        for entry in &entries {
            println!("{}", render_row(entry, run_duration_ms.max(1), bar_width));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::{Duration as ChronoDuration, Utc};
    use tempfile::TempDir;

    fn exec(name: &str, start: chrono::DateTime<chrono::Utc>, dur_ms: i64) -> ModelExecution {
        ModelExecution {
            model_name: name.to_string(),
            started_at: start,
            finished_at: start + ChronoDuration::milliseconds(dur_ms),
            duration_ms: dur_ms as u64,
            rows_affected: Some(42),
            status: "success".to_string(),
            sql_hash: format!("hash_{name}"),
            bytes_scanned: Some(1024),
            bytes_written: Some(2048),
        }
    }

    fn sample_run(models: Vec<ModelExecution>) -> RunRecord {
        let run_start = models
            .first()
            .map(|m| m.started_at)
            .unwrap_or_else(Utc::now);
        let run_end = models
            .iter()
            .map(|m| m.finished_at)
            .max()
            .unwrap_or(run_start);
        RunRecord {
            run_id: "run-trace-test".to_string(),
            started_at: run_start,
            finished_at: run_end,
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
            triggering_identity: None,
            session_source: rocky_core::state::SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "trace-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
        }
    }

    #[test]
    fn sequential_models_single_lane() {
        let start = Utc::now();
        let mut entries = vec![
            to_entry(start, &exec("a", start, 1_000)),
            to_entry(
                start,
                &exec("b", start + ChronoDuration::milliseconds(1_200), 500),
            ),
        ];
        let lane_count = assign_lanes(&mut entries);
        assert_eq!(lane_count, 1, "no overlap → single lane");
        assert_eq!(entries[0].lane, 0);
        assert_eq!(entries[1].lane, 0);
    }

    #[test]
    fn overlapping_models_distinct_lanes() {
        let start = Utc::now();
        let a = to_entry(start, &exec("a", start, 2_000));
        // b starts while a is still running → must go on lane 1.
        let b = to_entry(
            start,
            &exec("b", start + ChronoDuration::milliseconds(500), 2_000),
        );
        let mut entries = vec![a, b];
        let lane_count = assign_lanes(&mut entries);
        assert_eq!(lane_count, 2, "overlap → two lanes");
        assert_ne!(entries[0].lane, entries[1].lane);
    }

    #[test]
    fn resolve_latest() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let now = Utc::now();
        store
            .record_run(&sample_run(vec![exec("old", now, 100)]))
            .unwrap();
        let rec = resolve(&store, "latest").unwrap();
        assert_eq!(rec.run_id, "run-trace-test");
    }

    #[test]
    fn resolve_missing_run_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        assert!(resolve(&store, "does-not-exist").is_err());
    }

    #[test]
    fn resolve_latest_empty_store_errors() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        assert!(resolve(&store, "latest").is_err());
    }

    #[test]
    fn render_row_includes_bar_and_status() {
        let start = Utc::now();
        let entry = to_entry(
            start,
            &exec("orders", start + ChronoDuration::milliseconds(1000), 2000),
        );
        let row = render_row(&entry, 5000, 20);
        assert!(row.contains("orders"));
        assert!(row.contains("success"));
        assert!(row.contains('#'), "row must have a filled bar segment");
    }

    #[test]
    fn truncate_long_names() {
        assert_eq!(truncate("short", 28), "short");
        let long = "a".repeat(40);
        let truncated = truncate(&long, 28);
        assert_eq!(truncated.chars().count(), 28);
        assert!(truncated.ends_with('…'));
    }
}
