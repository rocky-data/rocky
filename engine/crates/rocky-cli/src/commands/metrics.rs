//! `rocky metrics` — display quality metrics and trends for models.

use std::path::Path;

use anyhow::Result;

use indexmap::IndexMap;
use rocky_core::state::StateStore;

use crate::output::{
    ColumnTrendPoint, MetricsAlert, MetricsOutput, MetricsSnapshotEntry, print_json,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Execute `rocky metrics`.
pub fn run_metrics(
    state_path: &Path,
    model_name: &str,
    trend: bool,
    column: Option<&str>,
    alerts: bool,
    output_json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)?;

    let snapshots = store.get_quality_trend(model_name, if trend { 20 } else { 1 })?;

    if snapshots.is_empty() {
        if output_json {
            let output = MetricsOutput {
                version: VERSION.to_string(),
                command: "metrics".to_string(),
                model: model_name.to_string(),
                snapshots: vec![],
                count: 0,
                alerts: vec![],
                column: None,
                column_trend: vec![],
                message: Some("no quality metrics available".to_string()),
            };
            print_json(&output)?;
        } else {
            println!("No quality metrics available for model: {model_name}");
            println!("Run `rocky run` with checks enabled to collect metrics.");
        }
        return Ok(());
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

    if output_json {
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

        let output = MetricsOutput {
            version: VERSION.to_string(),
            command: "metrics".to_string(),
            model: model_name.to_string(),
            count: typed_snapshots.len(),
            snapshots: typed_snapshots,
            alerts: alert_entries.clone(),
            column: column.map(std::string::ToString::to_string),
            column_trend,
            message: None,
        };
        print_json(&output)?;
    } else {
        println!("Quality metrics for model: {model_name}");
        println!();

        if trend {
            println!(
                "{:<24} {:<12} {:<10} {:<14}",
                "TIMESTAMP", "ROW COUNT", "RUN ID", "FRESHNESS"
            );
            println!("{}", "-".repeat(62));

            for snapshot in &snapshots {
                let freshness = snapshot
                    .metrics
                    .freshness_lag_seconds
                    .map(|s| format!("{s}s"))
                    .unwrap_or_else(|| "-".to_string());
                println!(
                    "{:<24} {:<12} {:<10} {:<14}",
                    snapshot.timestamp.format("%Y-%m-%d %H:%M:%S"),
                    snapshot.metrics.row_count,
                    &snapshot.run_id[..snapshot.run_id.len().min(9)],
                    freshness,
                );
            }
        } else if let Some(latest) = snapshots.first() {
            println!("Latest snapshot (run: {}):", latest.run_id);
            println!("  Row count: {}", latest.metrics.row_count);
            if let Some(lag) = latest.metrics.freshness_lag_seconds {
                println!("  Freshness lag: {lag}s");
            }

            if !latest.metrics.null_rates.is_empty() {
                println!("  Null rates:");
                let mut rates: Vec<_> = latest.metrics.null_rates.iter().collect();
                rates.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(std::cmp::Ordering::Equal));
                for (col, rate) in &rates {
                    if let Some(filter_col) = column {
                        if col.as_str() != filter_col {
                            continue;
                        }
                    }
                    println!("    {col}: {:.2}%", *rate * 100.0);
                }
            }
        }

        if alerts && !alert_entries.is_empty() {
            println!();
            println!("ALERTS:");
            for alert in &alert_entries {
                let sev_marker = match alert.severity.as_str() {
                    "critical" => "[CRITICAL]",
                    "warning" => "[WARNING]",
                    _ => "[INFO]",
                };
                println!("  {sev_marker} {}", alert.message);
            }
        }
    }

    Ok(())
}
