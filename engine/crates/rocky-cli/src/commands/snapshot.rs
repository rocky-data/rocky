//! `rocky snapshot` — execute SCD Type 2 snapshot pipelines.
//!
//! Reads a `type = "snapshot"` pipeline from `rocky.toml`, generates the
//! SCD2 MERGE SQL, and executes it against the configured warehouse adapter.
//!
//! This is the CLI entry point for the snapshot SQL generation in
//! `rocky_core::snapshots`.

use std::path::Path;
use std::time::Instant;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::snapshots::{SnapshotConfig, generate_initial_load_sql, generate_snapshot_sql};

use crate::output::{SnapshotOutput, SnapshotStepOutput, print_json};
use crate::registry::{AdapterRegistry, resolve_pipeline};

/// Execute `rocky snapshot`: generate and run SCD2 MERGE statements.
pub async fn run_snapshot(
    config_path: &Path,
    pipeline_name: Option<&str>,
    dry_run: bool,
    json: bool,
) -> Result<()> {
    let start = Instant::now();

    // Load config and build adapter registry.
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

    // Resolve the pipeline — must be a snapshot type.
    let (name, pipeline_cfg) = resolve_pipeline(&rocky_cfg, pipeline_name)?;
    let snapshot_cfg = pipeline_cfg.as_snapshot().with_context(|| {
        format!(
            "pipeline '{name}' is type '{}', but `rocky snapshot` requires a snapshot pipeline",
            pipeline_cfg.pipeline_type_str()
        )
    })?;

    // Get the target warehouse adapter.
    let adapter_name = &snapshot_cfg.target.adapter;
    let adapter = registry.warehouse_adapter(adapter_name)?;
    let dialect = adapter.dialect();

    // Build the snapshot config from pipeline config.
    let config = SnapshotConfig::from_pipeline_config(snapshot_cfg);

    info!(
        pipeline = name,
        source = format!(
            "{}.{}.{}",
            config.source.catalog, config.source.schema, config.source.table
        ),
        target = format!(
            "{}.{}.{}",
            config.target.catalog, config.target.schema, config.target.table
        ),
        strategy = match &config.strategy {
            rocky_core::snapshots::SnapshotStrategy::Timestamp { .. } => "timestamp",
            rocky_core::snapshots::SnapshotStrategy::Check { .. } => "check",
        },
        "running snapshot"
    );

    let mut steps: Vec<SnapshotStepOutput> = Vec::new();

    // Step 1: Ensure the target table exists (initial load DDL).
    let init_sql = generate_initial_load_sql(&config, dialect)
        .context("failed to generate initial load SQL")?;

    if dry_run {
        steps.push(SnapshotStepOutput {
            step: "initial_load".into(),
            sql: init_sql.clone(),
            status: "dry_run".into(),
            duration_ms: 0,
            error: None,
        });
    } else {
        let step_start = Instant::now();
        match adapter.execute_statement(&init_sql).await {
            Ok(_) => {
                info!("initial load DDL executed (CREATE TABLE IF NOT EXISTS)");
                steps.push(SnapshotStepOutput {
                    step: "initial_load".into(),
                    sql: init_sql,
                    status: "ok".into(),
                    duration_ms: step_start.elapsed().as_millis() as u64,
                    error: None,
                });
            }
            Err(e) => {
                let err_msg = format!("{e:#}");
                steps.push(SnapshotStepOutput {
                    step: "initial_load".into(),
                    sql: init_sql,
                    status: "error".into(),
                    duration_ms: step_start.elapsed().as_millis() as u64,
                    error: Some(err_msg.clone()),
                });
                anyhow::bail!("initial load failed: {err_msg}");
            }
        }
    }

    // Step 2: Generate and execute the SCD2 MERGE statements.
    let merge_stmts =
        generate_snapshot_sql(&config, dialect).context("failed to generate snapshot SQL")?;

    for (i, stmt) in merge_stmts.iter().enumerate() {
        let step_name = format!("merge_{}", i + 1);

        if dry_run {
            steps.push(SnapshotStepOutput {
                step: step_name,
                sql: stmt.clone(),
                status: "dry_run".into(),
                duration_ms: 0,
                error: None,
            });
        } else {
            let step_start = Instant::now();
            match adapter.execute_statement(stmt).await {
                Ok(_) => {
                    info!(step = %step_name, "snapshot step executed");
                    steps.push(SnapshotStepOutput {
                        step: step_name,
                        sql: stmt.clone(),
                        status: "ok".into(),
                        duration_ms: step_start.elapsed().as_millis() as u64,
                        error: None,
                    });
                }
                Err(e) => {
                    let err_msg = format!("{e:#}");
                    tracing::error!(step = %step_name, error = %err_msg, "snapshot step failed");
                    steps.push(SnapshotStepOutput {
                        step: step_name,
                        sql: stmt.clone(),
                        status: "error".into(),
                        duration_ms: step_start.elapsed().as_millis() as u64,
                        error: Some(err_msg.clone()),
                    });
                    anyhow::bail!("snapshot step failed: {err_msg}");
                }
            }
        }
    }

    let duration_ms = start.elapsed().as_millis() as u64;
    let has_errors = steps.iter().any(|s| s.error.is_some());

    if json {
        let output = SnapshotOutput {
            version: env!("CARGO_PKG_VERSION").into(),
            command: "snapshot".into(),
            pipeline: name.to_string(),
            source: format!(
                "{}.{}.{}",
                config.source.catalog, config.source.schema, config.source.table
            ),
            target: format!(
                "{}.{}.{}",
                config.target.catalog, config.target.schema, config.target.table
            ),
            dry_run,
            steps_total: steps.len(),
            steps_ok: steps.iter().filter(|s| s.status == "ok").count(),
            steps,
            duration_ms,
        };
        print_json(&output)?;
    } else if dry_run {
        println!("Snapshot dry-run for pipeline '{name}':");
        println!(
            "  Source: {}.{}.{}",
            config.source.catalog, config.source.schema, config.source.table
        );
        println!(
            "  Target: {}.{}.{}",
            config.target.catalog, config.target.schema, config.target.table
        );
        println!();
        for step in &steps {
            println!("-- {} --", step.step);
            println!("{}", step.sql);
            println!();
        }
    } else {
        let status = if has_errors { "FAILED" } else { "OK" };
        println!(
            "\nSnapshot [{status}] pipeline '{name}' ({} ms)",
            duration_ms
        );
        println!(
            "  Source: {}.{}.{}",
            config.source.catalog, config.source.schema, config.source.table
        );
        println!(
            "  Target: {}.{}.{}",
            config.target.catalog, config.target.schema, config.target.table
        );
        for step in &steps {
            let icon = if step.error.is_some() { "FAIL" } else { "OK" };
            println!("  [{icon}] {} ({} ms)", step.step, step.duration_ms);
            if let Some(ref err) = step.error {
                println!("       {err}");
            }
        }
    }

    if has_errors {
        anyhow::bail!("snapshot had errors");
    }

    Ok(())
}
