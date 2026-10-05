//! `rocky metrics` — display quality metrics and trends for models.

use std::path::Path;

use anyhow::Result;

use indexmap::IndexMap;
use rocky_core::state::StateStore;

use crate::output::{
    ColumnTrendPoint, MetricsAlert, MetricsOutput, MetricsSnapshotEntry, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build the quality-metrics output from the state store. Pure compute — no
/// printing — so other surfaces (the MCP `metrics` tool) can reuse it.
///
/// `trend` pulls up to 20 snapshots (vs the single latest); `column` adds a
/// per-run trend for one column; `alerts` derives freshness / null-rate
/// alerts. An absent metric history yields an output with `message` set and
/// empty snapshots.
pub fn metrics_output(
    state_path: &Path,
    model_name: &str,
    trend: bool,
    column: Option<&str>,
    alerts: bool,
) -> Result<MetricsOutput> {
    let store = StateStore::open_read_only_or_empty(state_path)?;

    // Production snapshots only (#2201). A snapshot carries the run that
    // wrote it; one written by a shadow or branch run measured another table.
    // The limit applies after the filter, so newer shadow snapshots cannot
    // push the production ones out.
    let limit = if trend { 20 } else { 1 };
    let mut snapshots = Vec::with_capacity(limit);
    let mut excluded_non_production_snapshots = 0usize;
    for snapshot in store.get_quality_trend(model_name, usize::MAX)? {
        if snapshots.len() == limit {
            break;
        }
        let production = store
            .get_run(&snapshot.run_id)?
            .is_none_or(|run| run.counts_as_production(rocky_core::state::UnrecordedScope::Count));
        if production {
            snapshots.push(snapshot);
        } else {
            excluded_non_production_snapshots += 1;
        }
    }

    if snapshots.is_empty() {
        return Ok(MetricsOutput {
            version: VERSION.to_string(),
            command: "metrics".to_string(),
            model: model_name.to_string(),
            snapshots: vec![],
            count: 0,
            alerts: vec![],
            column: None,
            column_trend: vec![],
            message: Some("no quality metrics available".to_string()),
            excluded_non_production_snapshots,
        });
    }

    // Build alerts if requested
    let alert_entries: Vec<MetricsAlert> = if alerts {
        let mut entries = Vec::new();
        for snapshot in &snapshots {
            if let Some(lag) = snapshot.metrics.freshness_lag_seconds
                && lag > 86400
            {
                entries.push(MetricsAlert {
                    kind: "freshness".to_string(),
                    severity: "warning".to_string(),
                    message: format!("stale data: {lag}s since last update"),
                    run_id: snapshot.run_id.clone(),
                    column: None,
                });
            }
            for (col, rate) in &snapshot.metrics.null_rates {
                if *rate > 0.5 {
                    entries.push(MetricsAlert {
                        kind: "null_rate".to_string(),
                        severity: "critical".to_string(),
                        message: format!("null rate {:.1}% exceeds 50% threshold", rate * 100.0),
                        run_id: snapshot.run_id.clone(),
                        column: Some(col.clone()),
                    });
                } else if *rate > 0.2 {
                    entries.push(MetricsAlert {
                        kind: "null_rate".to_string(),
                        severity: "warning".to_string(),
                        message: format!("null rate {:.1}% exceeds 20% threshold", rate * 100.0),
                        run_id: snapshot.run_id.clone(),
                        column: Some(col.clone()),
                    });
                }
            }
        }
        entries
    } else {
        Vec::new()
    };

    let typed_snapshots: Vec<MetricsSnapshotEntry> = snapshots
        .iter()
        .map(|s| MetricsSnapshotEntry {
            run_id: s.run_id.clone(),
            timestamp: s.timestamp,
            row_count: s.metrics.row_count,
            freshness_lag_seconds: s.metrics.freshness_lag_seconds,
            null_rates: s
                .metrics
                .null_rates
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect::<IndexMap<_, _>>(),
        })
        .collect();

    let column_trend: Vec<ColumnTrendPoint> = if let Some(col_name) = column {
        snapshots
            .iter()
            .map(|s| ColumnTrendPoint {
                run_id: s.run_id.clone(),
                timestamp: s.timestamp,
                null_rate: s.metrics.null_rates.get(col_name).copied(),
                row_count: s.metrics.row_count,
            })
            .collect()
    } else {
        vec![]
    };

    Ok(MetricsOutput {
        version: VERSION.to_string(),
        command: "metrics".to_string(),
        model: model_name.to_string(),
        count: typed_snapshots.len(),
        snapshots: typed_snapshots,
        alerts: alert_entries,
        column: column.map(std::string::ToString::to_string),
        column_trend,
        message: None,
        excluded_non_production_snapshots,
    })
}

/// Execute `rocky metrics`.
pub fn run_metrics(
    state_path: &Path,
    model_name: &str,
    trend: bool,
    column: Option<&str>,
    alerts: bool,
    output_json: bool,
) -> Result<()> {
    let output = metrics_output(state_path, model_name, trend, column, alerts)?;

    if output_json {
        print_json(&output)?;
        return Ok(());
    }

    if output.message.is_some() {
        println!("No quality metrics available for model: {model_name}");
        println!("Run `rocky run` with checks enabled to collect metrics.");
        return Ok(());
    }

    let snapshots = &output.snapshots;
    let alert_entries = &output.alerts;

    println!("Quality metrics for model: {model_name}");
    println!();

    if trend {
        println!(
            "{:<24} {:<12} {:<24} {:<14}",
            "TIMESTAMP", "ROW COUNT", "RUN ID", "FRESHNESS"
        );
        println!("{}", "-".repeat(76));

        for snapshot in snapshots {
            let freshness = snapshot
                .freshness_lag_seconds
                .map(|s| format!("{s}s"))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<24} {:<12} {:<24} {:<14}",
                snapshot.timestamp.format("%Y-%m-%d %H:%M:%S"),
                snapshot.row_count,
                snapshot.run_id,
                freshness,
            );
        }
    } else if let Some(latest) = snapshots.first() {
        println!("Latest snapshot (run: {}):", latest.run_id);
        println!("  Row count: {}", latest.row_count);
        if let Some(lag) = latest.freshness_lag_seconds {
            println!("  Freshness lag: {lag}s");
        }

        if !latest.null_rates.is_empty() {
            println!("  Null rates:");
            let mut rates: Vec<_> = latest.null_rates.iter().collect();
            rates.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
            for (col, rate) in &rates {
                if let Some(filter_col) = column
                    && col.as_str() != filter_col
                {
                    continue;
                }
                println!("    {col}: {:.2}%", *rate * 100.0);
            }
        }
    }

    if alerts && !alert_entries.is_empty() {
        println!();
        println!("ALERTS:");
        for alert in alert_entries {
            let sev_marker = match alert.severity.as_str() {
                "critical" => "[CRITICAL]",
                "warning" => "[WARNING]",
                _ => "[INFO]",
            };
            println!("  {sev_marker} {}", alert.message);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #2201: a newer snapshot written by a shadow run is not the model's
    /// production quality. The latest production snapshot is reported, and
    /// the output counts what was left out.
    #[test]
    fn metrics_report_production_snapshots_only() {
        let dir = tempfile::tempdir().unwrap();
        let state_path = dir.path().join("state.redb");
        let store = StateStore::open(&state_path).unwrap();
        for (id, hour, scope, rows) in [
            ("prod", 1, serde_json::json!("production"), 10u64),
            (
                "shadow",
                2,
                serde_json::json!({"shadow": {"schema": null}}),
                99,
            ),
        ] {
            let at = format!("2026-05-01T{hour:02}:00:00Z");
            let run: rocky_core::state::RunRecord = serde_json::from_value(serde_json::json!({
                "run_id": id,
                "started_at": at,
                "finished_at": at,
                "status": "Success",
                "models_executed": [],
                "trigger": "Manual",
                "config_hash": "c",
                "run_scope": scope,
            }))
            .unwrap();
            store.record_run(&run).unwrap();
            store
                .record_quality(
                    &serde_json::from_value(serde_json::json!({
                        "timestamp": at,
                        "run_id": id,
                        "model_name": "m",
                        "metrics": {"row_count": rows, "null_rates": {}},
                    }))
                    .unwrap(),
                )
                .unwrap();
        }
        drop(store);

        let out = metrics_output(&state_path, "m", false, None, false).unwrap();
        assert_eq!(out.snapshots.len(), 1);
        assert_eq!(out.snapshots[0].run_id, "prod");
        assert_eq!(out.snapshots[0].row_count, 10);
        assert_eq!(out.excluded_non_production_snapshots, 1);
    }
}
