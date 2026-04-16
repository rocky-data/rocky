//! Pipeline execution for non-replication pipeline types.
//!
//! Contains the execution paths for transformation, quality, and snapshot
//! pipelines. Replication pipelines are handled by [`super::run::run`], which
//! provides production-grade execution (parallel processing, drift detection,
//! batched checks, governance, checkpoint/resume) for ALL adapters.

use std::path::Path;
use std::time::Instant;

use anyhow::Result;
use chrono::Utc;
use tracing::warn;

use rocky_core::ir::*;
use rocky_core::sql_gen;
use rocky_core::state::StateStore;

use crate::output::*;
use crate::registry::AdapterRegistry;

/// Execute `rocky run` for a transformation pipeline.
///
/// Compiles and executes models from the pipeline's `models` glob using the
/// target adapter. Reuses the existing [`super::run::execute_models`] path.
#[tracing::instrument(skip_all, name = "run_transformation")]
pub async fn run_transformation(
    config_path: &Path,
    pipeline: &rocky_core::config::TransformationPipelineConfig,
    rocky_cfg: &rocky_core::config::RockyConfig,
    output_json: bool,
    partition_opts: &super::run::PartitionRunOptions,
) -> Result<()> {
    let start = Instant::now();

    let pipes = crate::pipes::PipesEmitter::detect();
    if let Some(p) = &pipes {
        p.log("INFO", "rocky run starting (transformation pipeline)");
    }

    let adapter_registry = AdapterRegistry::from_config(rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;
    let concurrency = pipeline.execution.concurrency.max_concurrency();

    let mut output = RunOutput::new(String::new(), 0, concurrency);
    output.pipeline_type = Some("transformation".to_string());

    // Resolve models directory relative to the config file
    let config_dir = config_path.parent().unwrap_or(Path::new("."));
    let models_base = pipeline
        .models
        .split("**")
        .next()
        .unwrap_or(&pipeline.models)
        .trim_end_matches('/');
    let models_dir = config_dir.join(models_base);

    if models_dir.exists() {
        let run_id = format!("run-{}", Utc::now().format("%Y%m%d-%H%M%S-%3f"));
        let state_path = config_dir.join(".rocky_state");
        let state_store = StateStore::open(&state_path)?;

        super::run::execute_models(
            &models_dir,
            warehouse_adapter.as_ref(),
            Some(&state_store),
            partition_opts,
            &run_id,
            None, // no model filter in local execution path
            &mut output,
        )
        .await?;
    } else {
        warn!(
            models_dir = %models_dir.display(),
            "models directory not found — nothing to execute"
        );
    }

    output.duration_ms = start.elapsed().as_millis() as u64;

    if let Some(p) = &pipes {
        super::run::emit_pipes_events(p, &output);
        p.log(
            "INFO",
            &format!(
                "rocky run complete: {} models executed in {}ms",
                output.materializations.len(),
                output.duration_ms,
            ),
        );
    }

    if output_json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "transformation pipeline complete: {} model(s) executed in {}ms",
            output.materializations.len(),
            output.duration_ms
        );
        for m in &output.materializations {
            println!("  {} ({})", m.asset_key.join("."), m.metadata.strategy);
        }
    }

    Ok(())
}

/// Execute `rocky run` for a quality pipeline.
///
/// Runs data quality checks against the specified tables without any data movement.
#[tracing::instrument(skip_all, name = "run_quality")]
pub async fn run_quality(
    _config_path: &Path,
    pipeline: &rocky_core::config::QualityPipelineConfig,
    rocky_cfg: &rocky_core::config::RockyConfig,
    output_json: bool,
) -> Result<()> {
    use rocky_core::checks::{CheckDetails, CheckResult};
    use rocky_core::tests::{TestSeverity, generate_test_sql};

    let start = Instant::now();

    let pipes = crate::pipes::PipesEmitter::detect();
    if let Some(p) = &pipes {
        p.log("INFO", "rocky run starting (quality pipeline)");
    }

    let adapter_registry = AdapterRegistry::from_config(rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;
    let dialect = warehouse_adapter.dialect();

    let mut output = RunOutput::new(
        String::new(),
        0,
        pipeline.execution.concurrency.max_concurrency(),
    );
    output.pipeline_type = Some("quality".to_string());

    let row_count_severity = pipeline.checks.row_count.severity();
    let custom_default_severity = TestSeverity::Error;

    if !pipeline.checks.enabled {
        warn!("quality pipeline checks are disabled — nothing to do");
    } else {
        for table_ref in &pipeline.tables {
            let tables_to_check: Vec<String> = if let Some(ref table) = table_ref.table {
                vec![table.clone()]
            } else {
                match warehouse_adapter
                    .execute_query(&format!(
                        "SELECT table_name FROM {}.information_schema.tables WHERE table_schema = '{}'",
                        table_ref.catalog, table_ref.schema
                    ))
                    .await
                {
                    Ok(result) => result
                        .rows
                        .into_iter()
                        .filter_map(|r| r.first().and_then(|v| v.as_str().map(String::from)))
                        .collect(),
                    Err(e) => {
                        warn!(
                            catalog = table_ref.catalog.as_str(),
                            schema = table_ref.schema.as_str(),
                            error = %e,
                            "failed to list tables in schema — skipping"
                        );
                        continue;
                    }
                }
            };

            for table_name in &tables_to_check {
                let full_table = dialect
                    .format_table_ref(&table_ref.catalog, &table_ref.schema, table_name)
                    .unwrap_or_else(|_| {
                        format!("{}.{}.{}", table_ref.catalog, table_ref.schema, table_name)
                    });

                let asset_key = vec![
                    table_ref.catalog.clone(),
                    table_ref.schema.clone(),
                    table_name.clone(),
                ];
                let mut checks = Vec::new();

                // Row count check
                if pipeline.checks.row_count.enabled() {
                    match warehouse_adapter
                        .execute_query(&format!("SELECT COUNT(*) FROM {full_table}"))
                        .await
                    {
                        Ok(result) => {
                            let count: u64 = result
                                .rows
                                .first()
                                .and_then(|r| r.first())
                                .and_then(|v| {
                                    v.as_u64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                                .unwrap_or(0);
                            checks.push(CheckResult {
                                name: "row_count".into(),
                                passed: count > 0,
                                severity: row_count_severity,
                                details: CheckDetails::RowCount {
                                    source_count: count,
                                    target_count: count,
                                },
                            });
                        }
                        Err(e) => {
                            checks.push(CheckResult {
                                name: "row_count".into(),
                                passed: false,
                                severity: row_count_severity,
                                details: CheckDetails::Custom {
                                    query: format!("SELECT COUNT(*) FROM {full_table}"),
                                    result_value: 0,
                                    threshold: 1,
                                },
                            });
                            warn!(error = %e, table = %full_table, "row count query failed");
                        }
                    }
                }

                // Custom checks
                for custom in &pipeline.checks.custom {
                    let sql = custom.sql.replace("{table}", &full_table);
                    let severity = if custom.severity == TestSeverity::default() {
                        custom_default_severity
                    } else {
                        custom.severity
                    };
                    match warehouse_adapter.execute_query(&sql).await {
                        Ok(result) => {
                            let result_value: u64 = result
                                .rows
                                .first()
                                .and_then(|r| r.first())
                                .and_then(|v| {
                                    v.as_u64()
                                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                                })
                                .unwrap_or(0);
                            checks.push(CheckResult {
                                name: custom.name.clone(),
                                passed: result_value >= custom.threshold,
                                severity,
                                details: CheckDetails::Custom {
                                    query: sql,
                                    result_value,
                                    threshold: custom.threshold,
                                },
                            });
                        }
                        Err(e) => {
                            checks.push(CheckResult {
                                name: custom.name.clone(),
                                passed: false,
                                severity,
                                details: CheckDetails::Custom {
                                    query: sql,
                                    result_value: 0,
                                    threshold: custom.threshold,
                                },
                            });
                            warn!(error = %e, check = custom.name.as_str(), "custom check query failed");
                        }
                    }
                }

                // Row-level assertions — match by unqualified table name.
                for assertion in &pipeline.checks.assertions {
                    if assertion.table != *table_name {
                        continue;
                    }
                    let test = &assertion.test;
                    let sql = match generate_test_sql(test, &full_table) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!(
                                error = %e,
                                table = %full_table,
                                "failed to generate assertion SQL — skipping"
                            );
                            continue;
                        }
                    };
                    let kind = test_type_kind(&test.test_type);
                    let name = format!("{kind}:{}", test.column.as_deref().unwrap_or("-"));

                    match warehouse_adapter.execute_query(&sql).await {
                        Ok(result) => {
                            let (passed, failing_rows) =
                                classify_assertion(&test.test_type, &result.rows);
                            checks.push(CheckResult {
                                name,
                                passed,
                                severity: test.severity,
                                details: CheckDetails::Assertion {
                                    kind: kind.to_string(),
                                    column: test.column.clone(),
                                    failing_rows,
                                },
                            });
                        }
                        Err(e) => {
                            warn!(
                                error = %e,
                                table = %full_table,
                                assertion = %name,
                                "assertion query failed"
                            );
                            checks.push(CheckResult {
                                name,
                                passed: false,
                                severity: test.severity,
                                details: CheckDetails::Assertion {
                                    kind: kind.to_string(),
                                    column: test.column.clone(),
                                    failing_rows: 0,
                                },
                            });
                        }
                    }
                }

                output
                    .check_results
                    .push(TableCheckOutput { asset_key, checks });
            }
        }
    }

    output.duration_ms = start.elapsed().as_millis() as u64;

    if let Some(p) = &pipes {
        super::run::emit_pipes_events(p, &output);
    }

    let (error_failures, warning_failures) = count_failures_by_severity(&output);

    if output_json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        let total_checks: usize = output.check_results.iter().map(|t| t.checks.len()).sum();
        println!(
            "quality pipeline complete: {total_checks} check(s) across {} table(s), {error_failures} error / {warning_failures} warning failed, in {}ms",
            output.check_results.len(),
            output.duration_ms
        );
    }

    if error_failures > 0 && pipeline.checks.fail_on_error {
        anyhow::bail!("quality pipeline failed: {error_failures} error-severity check(s) failed");
    }

    Ok(())
}

/// Short snake_case tag for each `TestType` variant — embedded in check
/// result details so downstream consumers can distinguish assertion kinds.
fn test_type_kind(t: &rocky_core::tests::TestType) -> &'static str {
    use rocky_core::tests::TestType;
    match t {
        TestType::NotNull => "not_null",
        TestType::Unique => "unique",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Relationships { .. } => "relationships",
        TestType::Expression { .. } => "expression",
        TestType::RowCountRange { .. } => "row_count_range",
    }
}

/// Classify an assertion query's result into `(passed, failing_rows)`.
///
/// The classification is kind-dependent:
/// - `NotNull` / `Expression`: first cell is a failure count; 0 passes.
/// - `Unique` / `AcceptedValues` / `Relationships`: every result row is a
///   violation; empty result passes. `failing_rows` is the row count.
/// - `RowCountRange`: first cell is the total row count; pass/fail decided
///   by the caller against `min`/`max` (handled below).
fn classify_assertion(
    t: &rocky_core::tests::TestType,
    rows: &[Vec<serde_json::Value>],
) -> (bool, u64) {
    use rocky_core::tests::TestType;
    let first_cell_u64 = || -> u64 {
        rows.first()
            .and_then(|r| r.first())
            .and_then(|v| {
                v.as_u64()
                    .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
            })
            .unwrap_or(0)
    };
    match t {
        TestType::NotNull | TestType::Expression { .. } => {
            let n = first_cell_u64();
            (n == 0, n)
        }
        TestType::Unique | TestType::AcceptedValues { .. } | TestType::Relationships { .. } => {
            let n = rows.len() as u64;
            (n == 0, n)
        }
        TestType::RowCountRange { min, max } => {
            let n = first_cell_u64();
            let within_min = min.map(|m| n >= m).unwrap_or(true);
            let within_max = max.map(|m| n <= m).unwrap_or(true);
            (within_min && within_max, n)
        }
    }
}

/// Count failed checks bucketed by severity across every table result.
fn count_failures_by_severity(output: &RunOutput) -> (usize, usize) {
    use rocky_core::tests::TestSeverity;
    let mut error = 0usize;
    let mut warning = 0usize;
    for t in &output.check_results {
        for c in &t.checks {
            if c.passed {
                continue;
            }
            match c.severity {
                TestSeverity::Error => error += 1,
                TestSeverity::Warning => warning += 1,
            }
        }
    }
    (error, warning)
}

/// Execute `rocky run` for a snapshot (SCD Type 2) pipeline.
///
/// Generates and executes SCD2 MERGE SQL against the target adapter.
#[tracing::instrument(skip_all, name = "run_snapshot")]
pub async fn run_snapshot(
    _config_path: &Path,
    pipeline: &rocky_core::config::SnapshotPipelineConfig,
    rocky_cfg: &rocky_core::config::RockyConfig,
    output_json: bool,
) -> Result<()> {
    let start = Instant::now();

    let pipes = crate::pipes::PipesEmitter::detect();
    if let Some(p) = &pipes {
        p.log("INFO", "rocky run starting (snapshot pipeline)");
    }

    let adapter_registry = AdapterRegistry::from_config(rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;

    let mut output = RunOutput::new(String::new(), 0, 1);
    output.pipeline_type = Some("snapshot".to_string());

    let plan = rocky_core::ir::SnapshotPlan {
        source: SourceRef {
            catalog: pipeline.source.catalog.clone(),
            schema: pipeline.source.schema.clone(),
            table: pipeline.source.table.clone(),
        },
        target: TargetRef {
            catalog: pipeline.target.catalog.clone(),
            schema: pipeline.target.schema.clone(),
            table: pipeline.target.table.clone(),
        },
        unique_key: pipeline.unique_key.clone(),
        updated_at: pipeline.updated_at.clone(),
        invalidate_hard_deletes: pipeline.invalidate_hard_deletes,
        governance: rocky_core::ir::GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
            auto_create_schemas: pipeline.target.governance.auto_create_schemas,
        },
    };

    let dialect = warehouse_adapter.dialect();
    let stmts = sql_gen::generate_snapshot_sql(&plan, dialect)?;

    let mut tables_failed = 0usize;
    for stmt in &stmts {
        if let Err(e) = warehouse_adapter.execute_query(stmt).await {
            warn!(error = %e, "snapshot statement failed");
            tables_failed += 1;
        }
    }

    let target_name = format!(
        "{}.{}.{}",
        pipeline.target.catalog, pipeline.target.schema, pipeline.target.table
    );

    if tables_failed == 0 {
        output.tables_copied = 1;
        output.materializations.push(MaterializationOutput {
            asset_key: vec![
                pipeline.target.catalog.clone(),
                pipeline.target.schema.clone(),
                pipeline.target.table.clone(),
            ],
            rows_copied: None,
            duration_ms: start.elapsed().as_millis() as u64,
            metadata: MaterializationMetadata {
                strategy: "snapshot_scd2".to_string(),
                watermark: None,
                target_table_full_name: Some(target_name),
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
        });
    } else {
        output.tables_failed = 1;
    }

    output.duration_ms = start.elapsed().as_millis() as u64;

    if let Some(p) = &pipes {
        super::run::emit_pipes_events(p, &output);
    }

    if output_json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "snapshot pipeline complete: {}.{}.{} -> {}.{}.{} in {}ms",
            pipeline.source.catalog,
            pipeline.source.schema,
            pipeline.source.table,
            pipeline.target.catalog,
            pipeline.target.schema,
            pipeline.target.table,
            output.duration_ms
        );
    }

    if tables_failed > 0 {
        anyhow::bail!("snapshot pipeline failed");
    }
    Ok(())
}
