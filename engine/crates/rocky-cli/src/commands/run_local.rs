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
    // Already-overridden schema cache config. Caller (`run::run`) has
    // folded the CLI `--cache-ttl` flag into this value via
    // `with_ttl_override` so the override propagates consistently
    // across replication and transformation paths.
    schema_cache_cfg: &rocky_core::config::SchemaCacheConfig,
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
            None, // run_local doesn't build a HookRegistry
            None,
            schema_cache_cfg,
        )
        .await?;
    } else {
        warn!(
            models_dir = %models_dir.display(),
            "models directory not found — nothing to execute"
        );
    }

    output.duration_ms = start.elapsed().as_millis() as u64;

    // Compute per-model cost_usd from accumulated bytes / duration. The
    // replication path (run.rs:3030) and model-only path (run.rs:872)
    // already do this; without it transformation runs always emit
    // `cost_usd: null` even when adapters report bytes_scanned.
    let adapter_type = rocky_cfg
        .adapters
        .get(&pipeline.target.adapter)
        .map(|a| a.adapter_type.clone())
        .unwrap_or_default();
    output.populate_cost_summary(&adapter_type, &rocky_cfg.cost);

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
    use rocky_core::tests::generate_test_sql_with_dialect;

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

    if !pipeline.checks.enabled {
        warn!("quality pipeline checks are disabled — nothing to do");
    } else {
        for table_ref in &pipeline.tables {
            let tables_to_check: Vec<String> = if let Some(ref table) = table_ref.table {
                vec![table.clone()]
            } else {
                let list_sql = match dialect.list_tables_sql(&table_ref.catalog, &table_ref.schema)
                {
                    Ok(sql) => sql,
                    Err(e) => {
                        warn!(
                            catalog = table_ref.catalog.as_str(),
                            schema = table_ref.schema.as_str(),
                            error = %e,
                            "failed to build list-tables SQL — skipping"
                        );
                        continue;
                    }
                };
                match warehouse_adapter.execute_query(&list_sql).await {
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
                    let severity = custom.severity;
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
                    let sql = match generate_test_sql_with_dialect(test, &full_table, dialect) {
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
                    let name = assertion.name.clone().unwrap_or_else(|| {
                        format!("{kind}:{}", test.column.as_deref().unwrap_or("-"))
                    });

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

                output.check_results.push(TableCheckOutput {
                    asset_key: asset_key.clone(),
                    checks,
                });

                // Row quarantine — split/tag/drop rows that violate
                // error-severity row-level assertions. Compiles from
                // config (not from assertion query results) so a failed
                // assertion query does not suppress quarantine.
                if let Some(ref q_cfg) = pipeline.checks.quarantine
                    && q_cfg.enabled
                {
                    let ir_ref = rocky_core::ir::TableRef {
                        catalog: table_ref.catalog.clone(),
                        schema: table_ref.schema.clone(),
                        table: table_name.clone(),
                    };
                    match rocky_core::quarantine::compile_quarantine_sql(
                        &pipeline.checks.assertions,
                        table_name,
                        &ir_ref,
                        dialect,
                        q_cfg,
                    ) {
                        Ok(Some(plan)) => {
                            let q_output = execute_quarantine_plan(
                                warehouse_adapter.as_ref(),
                                asset_key,
                                plan,
                            )
                            .await;
                            output.quarantine.push(q_output);
                        }
                        Ok(None) => {} // no quarantinable assertions
                        Err(e) => warn!(
                            error = %e,
                            table = %full_table,
                            "failed to compile quarantine SQL — skipping"
                        ),
                    }
                }
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
        let quarantine_summary = if output.quarantine.is_empty() {
            String::new()
        } else {
            let total_q: u64 = output
                .quarantine
                .iter()
                .filter_map(|q| q.quarantined_rows)
                .sum();
            format!(
                ", quarantined {total_q} row(s) across {} table(s)",
                output.quarantine.len()
            )
        };
        println!(
            "quality pipeline complete: {total_checks} check(s) across {} table(s), {error_failures} error / {warning_failures} warning failed{quarantine_summary}, in {}ms",
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
        TestType::InRange { .. } => "in_range",
        TestType::RegexMatch { .. } => "regex_match",
        TestType::Aggregate { .. } => "aggregate",
        TestType::Composite { .. } => "composite",
        TestType::NotInFuture => "not_in_future",
        TestType::OlderThanNDays { .. } => "older_than_n_days",
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
        // Count-based: first cell is the count of failing rows.
        TestType::NotNull
        | TestType::Expression { .. }
        | TestType::InRange { .. }
        | TestType::RegexMatch { .. }
        | TestType::NotInFuture
        | TestType::OlderThanNDays { .. } => {
            let n = first_cell_u64();
            (n == 0, n)
        }
        // Row-set-based: every returned row is a violation.
        TestType::Unique
        | TestType::AcceptedValues { .. }
        | TestType::Relationships { .. }
        | TestType::Composite { .. } => {
            let n = rows.len() as u64;
            (n == 0, n)
        }
        TestType::RowCountRange { min, max } => {
            let n = first_cell_u64();
            let within_min = min.map(|m| n >= m).unwrap_or(true);
            let within_max = max.map(|m| n <= m).unwrap_or(true);
            (within_min && within_max, n)
        }
        // Aggregate: the test SQL returns a single 0/1 cell (0 = pass).
        TestType::Aggregate { .. } => {
            let n = first_cell_u64();
            (n == 0, n)
        }
    }
}

/// Execute a compiled [`rocky_core::quarantine::QuarantinePlan`] against
/// the warehouse and return a [`QuarantineOutput`] summarizing the result.
///
/// Statements execute in plan order — quarantine-first for `split` mode
/// so a partial failure leaves a stray quarantine table rather than a
/// stale valid table downstream pipelines might read. Row counts are
/// captured via `SELECT COUNT(*)` against the written tables; adapters
/// that cannot count leave the fields as `None`.
async fn execute_quarantine_plan(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    asset_key: Vec<String>,
    plan: rocky_core::quarantine::QuarantinePlan,
) -> QuarantineOutput {
    let mode_str = match plan.mode {
        rocky_core::config::QuarantineMode::Split => "split",
        rocky_core::config::QuarantineMode::Tag => "tag",
        rocky_core::config::QuarantineMode::Drop => "drop",
    };
    let mut out = QuarantineOutput {
        asset_key,
        mode: mode_str.to_string(),
        valid_table: plan.valid_table.clone(),
        quarantine_table: plan.quarantine_table.clone(),
        valid_rows: None,
        quarantined_rows: None,
        ok: true,
        error: None,
    };

    for stmt in &plan.statements {
        if let Err(e) = warehouse.execute_statement(&stmt.sql).await {
            out.ok = false;
            out.error = Some(format!("{}: {}", role_label(stmt.role), e));
            warn!(
                role = role_label(stmt.role),
                target = stmt.target.as_str(),
                error = %e,
                "quarantine statement failed"
            );
            return out;
        }
    }

    if !plan.valid_table.is_empty() {
        out.valid_rows = count_rows(warehouse, &plan.valid_table).await;
    }
    if !plan.quarantine_table.is_empty() {
        out.quarantined_rows = count_rows(warehouse, &plan.quarantine_table).await;
    }

    out
}

fn role_label(role: rocky_core::quarantine::StatementRole) -> &'static str {
    use rocky_core::quarantine::StatementRole;
    match role {
        StatementRole::Quarantine => "quarantine",
        StatementRole::Valid => "valid",
        StatementRole::Tag => "tag",
    }
}

async fn count_rows(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    full_table: &str,
) -> Option<u64> {
    let sql = format!("SELECT COUNT(*) FROM {full_table}");
    let result = warehouse.execute_query(&sql).await.ok()?;
    result.rows.first().and_then(|r| r.first()).and_then(|v| {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
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
    let started_at = chrono::Utc::now();

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
        unique_key: pipeline
            .unique_key
            .iter()
            .map(|s| std::sync::Arc::from(s.as_str()))
            .collect(),
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
            started_at,
            metadata: MaterializationMetadata {
                strategy: "snapshot_scd2".to_string(),
                watermark: None,
                target_table_full_name: Some(target_name),
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
            // snapshot_scd2 uses `execute_query` (multi-statement) and
            // doesn't thread stats through yet — BigQuery snapshot cost
            // attribution is a follow-up wave.
            bytes_scanned: None,
            bytes_written: None,
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
