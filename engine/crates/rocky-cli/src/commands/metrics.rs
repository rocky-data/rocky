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
    let store = StateStore::open_read_only(state_path)?;

    let snapshots = store.get_quality_trend(model_name, if trend { 20 } else { 1 })?;

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
            "{:<24} {:<12} {:<10} {:<14}",
            "TIMESTAMP", "ROW COUNT", "RUN ID", "FRESHNESS"
        );
        println!("{}", "-".repeat(62));

        for snapshot in snapshots {
            let freshness = snapshot
                .freshness_lag_seconds
                .map(|s| format!("{s}s"))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<24} {:<12} {:<10} {:<14}",
                snapshot.timestamp.format("%Y-%m-%d %H:%M:%S"),
                snapshot.row_count,
                &snapshot.run_id[..snapshot.run_id.len().min(9)],
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
