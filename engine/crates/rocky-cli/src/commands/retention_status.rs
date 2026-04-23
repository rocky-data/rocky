//! `rocky retention-status` — report per-model data retention policies.
//!
//! Compiles the project, walks the resolved model set, and emits one row per
//! model with the declared retention (if any). v1 reports declaration-only;
//! `--drift` would probe the warehouse via `SHOW TBLPROPERTIES`
//! (Databricks) / `SHOW PARAMETERS` (Snowflake) and fill `warehouse_days`,
//! but that probe is deferred to v2 per the waveplan.

use std::path::Path;

use anyhow::{Context, Result};

use crate::output::{ModelRetentionStatus, RetentionStatusOutput, print_json};

/// Execute `rocky retention-status`.
///
/// `model_filter`, when set, restricts the output to a single model. `drift`
/// is accepted for forward-compatibility but ignored in v1 — the probe is
/// not implemented yet and `warehouse_days` is always `None`.
pub fn run_retention_status(
    models_dir: &Path,
    model_filter: Option<&str>,
    drift: bool,
    output_json: bool,
) -> Result<()> {
    // Compile the project so we see the resolved ModelConfig (with
    // retention flattened to a typed `Option<RetentionPolicy>`). We
    // don't need the full SQL typecheck result — just the models.
    let compile = rocky_compiler::compile::compile(&rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
    })
    .context("failed to compile project for retention-status")?;

    // Build one row per model (filtered to the requested one when
    // --model is set).
    let mut rows: Vec<ModelRetentionStatus> = compile
        .project
        .models
        .iter()
        .filter(|m| model_filter.is_none_or(|name| m.config.name == name))
        .map(|m| {
            let configured = m.config.retention.map(|r| r.duration_days);
            // v1: no warehouse probe. The --drift stretch goal is deferred
            // to v2; until then `warehouse_days` stays `None` and
            // `in_sync` collapses to "did the user declare anything?".
            let warehouse_days = None;
            let in_sync = configured == warehouse_days;
            ModelRetentionStatus {
                model: m.config.name.clone(),
                configured_days: configured,
                warehouse_days,
                in_sync,
            }
        })
        .collect();

    if drift {
        // --drift was requested but the probe isn't implemented yet.
        // Filter to models with a declared policy so the UX at least
        // surfaces "here's what would be checked" — and leave a note.
        rows.retain(|r| r.configured_days.is_some());
        if !output_json {
            eprintln!("note: --drift probe is deferred to v2; warehouse_days will be null.");
        }
    }

    if output_json {
        print_json(&RetentionStatusOutput::new(rows))?;
    } else {
        println!(
            "{:<40} {:<16} {:<16} {:<8}",
            "MODEL", "CONFIGURED", "WAREHOUSE", "IN SYNC"
        );
        println!("{}", "-".repeat(82));
        for row in &rows {
            let cfg = row
                .configured_days
                .map(|d| format!("{d} days"))
                .unwrap_or_else(|| "-".to_string());
            let wh = row
                .warehouse_days
                .map(|d| format!("{d} days"))
                .unwrap_or_else(|| "-".to_string());
            println!(
                "{:<40} {:<16} {:<16} {:<8}",
                row.model,
                cfg,
                wh,
                if row.in_sync { "yes" } else { "no" }
            );
        }
        if rows.is_empty() {
            println!(
                "No models{}.",
                if model_filter.is_some() {
                    " matched the filter"
                } else {
                    ""
                }
            );
        }
    }

    Ok(())
}
