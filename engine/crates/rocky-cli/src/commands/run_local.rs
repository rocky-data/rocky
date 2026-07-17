//! Pipeline execution for non-replication pipeline types.
//!
//! Contains the execution paths for transformation, quality, and snapshot
//! pipelines. Replication pipelines are handled by [`super::run::run`], which
//! provides production-grade execution (parallel processing, drift detection,
//! batched checks, governance, checkpoint/resume) for ALL adapters.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use tracing::warn;

use rocky_core::sql_gen;
use rocky_core::state::StateStore;
use rocky_ir::*;

use crate::output::*;
use crate::registry::AdapterRegistry;

/// The transformation run's single models-directory decision.
///
/// Resolved ONCE by the dispatch site in `run::run` — the same decision that
/// governs whether the transformation arm carries a
/// [`RemoteStateSession`][rocky_core::state_sync::RemoteStateSession] — and
/// threaded into [`run_transformation`], which consumes it INSTEAD of
/// re-checking the filesystem. Before this type existed, the session decision
/// (run.rs) and the execution decision (here) were two separate `exists()`
/// checks: a directory created (or a symlink retargeted) between them made
/// `run_transformation` open a state store, execute models, and persist a
/// `RunRecord` with NO session — the RD-003 sessionless-mutation shape
/// re-opened through a filesystem race. With one threaded decision, `Absent`
/// guarantees a no-op regardless of what the filesystem holds at execution
/// time. Both variants carry the resolved path (config-dir-joined `models`
/// glob base) for execution and diagnostics.
pub enum ModelsDirDecision {
    /// The models directory existed at decision time — execute from it.
    Present(PathBuf),
    /// The models directory was absent at decision time — the run is a
    /// sessionless no-op (or the governed `expects_models` fail-closed
    /// bail), even if a directory exists on disk by execution time.
    Absent(PathBuf),
}

/// Execute `rocky run` for a transformation pipeline.
///
/// Compiles and executes models from the caller-resolved
/// [`ModelsDirDecision`] using the target adapter. Reuses the existing
/// [`super::run::execute_models`] path.
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, name = "run_transformation")]
pub async fn run_transformation(
    // The single models-directory decision (see [`ModelsDirDecision`]),
    // resolved by the dispatch site in `run::run` at the SAME instant it
    // decided whether to create this run's `RemoteStateSession`. This
    // function must never re-check the filesystem for the models dir.
    models_dir: ModelsDirDecision,
    pipeline: &rocky_core::config::TransformationPipelineConfig,
    rocky_cfg: &rocky_core::config::RockyConfig,
    output_json: bool,
    partition_opts: &super::run::PartitionRunOptions,
    // Already-overridden schema cache config. Caller (`run::run`) has
    // folded the CLI `--cache-ttl` flag into this value via
    // `with_ttl_override` so the override propagates consistently
    // across replication and transformation paths.
    schema_cache_cfg: &rocky_core::config::SchemaCacheConfig,
    // Fully-resolved model-skip gate (default-off unless `--skip-unchanged`
    // / `[run] skip_unchanged` is set). Passed straight through to
    // `execute_models`.
    skip_gate: super::run::SkipGateConfig,
    // Whether `--no-reuse` was passed. The documented escape hatch that
    // forces every content-addressed model to BUILD for this invocation:
    // it disables both the point-to reuse decision and the column-level
    // skip, even when `[reuse]` opts in via config.
    no_reuse: bool,
    // Canonical state path resolved once by `main.rs` (`--state-path` /
    // `--state-namespace` / the global `<models>/.rocky-state.redb`
    // default) — the SAME path the replication path opens. The
    // transformation run persists its `RunRecord` here so `rocky history`
    // / `rocky state` see it and `--skip-unchanged` has a prior-successful
    // baseline to compare against.
    state_path: &Path,
    // `run()`'s `run_id` + `started_at`, threaded so the persisted
    // `RunRecord::run_id` matches the idempotency stamp at the dispatch
    // site (invariant: `IdempotencyEntry::run_id == RunRecord::run_id`).
    // `run_id` is derived from `started_at`, so they always travel together.
    run_id: &str,
    started_at: DateTime<Utc>,
    // Config fingerprint computed once in `run()` (mirrors every other
    // `persist_run_record` call site).
    config_hash: &str,
    // Caller-supplied `--idempotency-key` (verbatim, as `run()` received
    // it). Threaded so the persisted audit records the claimed key — the
    // model-only (`run.rs`) and replication paths pass the same value into
    // `AuditContext::detect`. Without it the persisted `RunRecord`'s
    // `idempotency_key` is `None` even when the run finalized the
    // idempotency entry under `K`, so `rocky history --audit` shows
    // `idempotency_key=-` instead of `K`.
    idempotency_key: Option<&str>,
    // `--var` per-run variables threaded from `run()`. Substituted into
    // `@var()` markers in this pipeline's model SQL at compile time (both the
    // execution compile and the governance reconcile compile below).
    run_vars: &rocky_core::run_vars::RunVars,
    // Governed-apply TOCTOU gate (E) — `Some` for an agent transformation apply.
    exec_fp_gate: Option<&super::apply::ExecFingerprintGate>,
    // Finding #2 (missing-dir): the governed plan reviewed a non-empty model set,
    // so a missing models directory at execution is a fail-closed error rather than
    // the legitimate no-op silent-skip. `false` for a bare `rocky run` and for a
    // governed plan with an empty reviewed set — both keep the silent-skip.
    expects_models: bool,
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

    // Open the canonical state store up front so it stays in scope through
    // cost population + `persist_run_record` below. Only opened when the
    // dispatch site's single models-dir decision resolved `Present` — a
    // no-op run must not create a state file. The PASSED decision governs,
    // never a fresh `exists()` check: re-checking here would re-open the
    // two-check race [`ModelsDirDecision`] exists to close (a dir created
    // between the session decision and execution would mutate state with no
    // session). `persist_run_record` accepts `Option<&StateStore>`, so the
    // `Absent` arm flows through cleanly.
    let mut state_store: Option<StateStore> = None;
    match &models_dir {
        ModelsDirDecision::Present(models_dir) => {
            let store = StateStore::open(state_path).with_context(|| {
                format!("failed to open state store at {}", state_path.display())
            })?;

            // Finding #1: baseline failures so a soft model failure skips governance.
            let failures_before = output.tables_failed;
            let exec_result = super::run::execute_models(
                models_dir,
                warehouse_adapter.as_ref(),
                Some(&store),
                partition_opts,
                run_id,
                None, // no model filter in local execution path
                None, // no backfill model-set scope in local execution path
                &mut output,
                None, // run_local doesn't build a HookRegistry
                None,
                schema_cache_cfg,
                pipeline.target.governance.auto_create_schemas,
                // run_local has no `--model` selection; defer is a no-op here.
                &super::run::DeferOptions::default(),
                skip_gate,
                // Reuse is active iff `[reuse]` is enabled AND `--no-reuse` was
                // not passed (clause 1 of the fail-closed decision) — same
                // resolution as the replication / model-only entry points.
                rocky_cfg.reuse.enabled && !no_reuse,
                // Content-addressed column-level skip (its own `[reuse]`
                // sub-key), likewise disabled by the `--no-reuse` escape hatch.
                rocky_cfg.reuse.column_level && !no_reuse,
                run_vars,
                rocky_cfg.resilience.clone(),
                super::resilience::retry_policy_allows(rocky_cfg),
                exec_fp_gate,
                // Finding #4: the transformation route reconciles no masks.
                false,
            )
            .await;

            match exec_result {
                // Finding #1: only when the model phase is CLEAN (no new soft
                // failures) — a soft failure returns `Ok`.
                Ok(snapshot) if output.tables_failed == failures_before => {
                    // --- Governance: per-model `[governance.tags]` ---
                    //
                    // After every model materializes, apply each model's
                    // `[governance.tags]` to its target securable. `None` filter:
                    // the full-DAG path builds every model, so every model is
                    // tagged. Best-effort; no-op on DuckDB's Noop adapter.
                    // #1093: reads the snapshot `execute_models` captured at the
                    // fingerprint gate, never a fresh disk compile.
                    let governance_adapter =
                        adapter_registry.governance_adapter(&pipeline.target.adapter);
                    super::run::apply_model_governance_tags(
                        &snapshot,
                        governance_adapter.as_ref(),
                        None,
                    )
                    .await;
                }
                // Soft model failure — skip governance, fall through.
                Ok(_) => {}
                Err(e) => {
                    // A runtime model failure (warehouse rejected the SQL, an
                    // unresolved upstream, ...) surfaces here as `Err`. Record it
                    // as a first-class run failure so the JSON `RunOutput` below
                    // still emits with `status` / `errors[]` / any sibling
                    // materializations BEFORE the non-zero exit, mirroring the
                    // compile-error path which records into `output` and returns
                    // `Ok`. Without this the `?` short-circuited before the JSON
                    // emit, leaving an orchestrator (Dagster) consuming
                    // `--output json` with empty stdout on a runtime failure. The
                    // terminal-status exit contract is honoured by
                    // `run_status_exit_result` below.
                    output.tables_failed += 1;
                    output.errors.push(crate::output::TableErrorOutput {
                        asset_key: vec!["<runtime>".to_string()],
                        error: format!("{e:#}"),
                        failure_kind: crate::output::FailureKind::Unknown,
                        cooldown_seconds: None,
                    });
                }
            }

            state_store = Some(store);
        }
        ModelsDirDecision::Absent(models_dir) if expects_models => {
            // ‼️ Finding #2 (missing-dir), transformation executor leg: this is
            // the PRIMARY guard for the transformation path — the apply seam
            // does not check here because the dispatch site resolves
            // `config_dir.join(models_base)` from the pipeline config (this
            // decision's path), which the seam's `run_plan.models_dir` does not
            // match. A governed apply whose plan reviewed a non-empty model set
            // but whose (config-resolved) models directory was absent at the
            // decision must FAIL CLOSED rather than silently succeed with
            // nothing built. Fires before the state store is opened — no
            // warehouse mutation. A bare `rocky run` / an empty reviewed set
            // has `expects_models == false` and keeps the silent no-op below.
            anyhow::bail!(
                "refusing to complete governed apply: the reviewed models directory '{}' does \
                 not exist at execution (deleted or renamed since the plan was authorized), so \
                 its planned models cannot run — refusing rather than reporting success for \
                 models that never executed. Re-plan with `rocky plan` before applying.",
                models_dir.display()
            );
        }
        ModelsDirDecision::Absent(models_dir) => {
            warn!(
                models_dir = %models_dir.display(),
                "models directory not found — nothing to execute"
            );
        }
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

    // Enforce the global run-level `[budget]`. The replication (run.rs) and
    // model-only paths both run this after cost population; without it the
    // guardrail is a silent no-op on the transformation path — the highest-
    // compute pipeline type, exactly where a scan/cost/duration cap matters
    // most. `check_and_record_budget` records any breach into
    // `output.budget_breaches` (surfaced in the JSON) and returns `Err` when
    // `on_breach = "error"`; the error is propagated after the JSON is emitted
    // (below) so a consumer still sees the final payload.
    let budget_result = output.check_and_record_budget(&rocky_cfg.budget, Some(run_id));

    // Persist the RunRecord (after cost population, so the recorded
    // `ModelExecution` entries carry cost + `skip_hash` + upstream
    // freshness signatures). Mirrors the replication / model-only call
    // sites in `run.rs`. This is what makes `--skip-unchanged` work on the
    // full-DAG path — the next run's skip gate reads these per-model
    // executions as its prior-successful baseline — and what surfaces the
    // run in `rocky history` / `rocky state`. Failures are logged and
    // swallowed: bookkeeping must not flip a successful run's exit code.
    //
    // Transformation runs have no single target catalog (per-model targets
    // resolve from each model's sidecar), so `target_catalog = None` — the
    // same posture as the model-only path.
    let audit_ctx =
        super::run_audit::AuditContext::detect(idempotency_key.map(str::to_string), None);
    let audit = super::run::audit_to_record(&audit_ctx);
    super::run::persist_run_record(
        state_store.as_ref(),
        &output,
        run_id,
        started_at,
        config_hash,
        &audit,
    );

    // Stamp the terminal status onto the emitted payload so a JSON
    // consumer reads it directly instead of re-deriving from counts. A
    // model that failed to compile makes this `Failure` / `PartialFailure`
    // rather than the `Success` default.
    output.status = output.derive_run_status();

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
        crate::status_line!(
            "transformation pipeline complete: {} model(s) executed in {}ms",
            output.materializations.len(),
            output.duration_ms
        );
        for m in &output.materializations {
            crate::status_line!("  {} ({})", m.asset_key.join("."), m.metadata.strategy);
        }
    }

    // Propagate a hard `[budget]` breach (`on_breach = "error"`) now that the
    // JSON payload has been emitted, matching the replication / model-only
    // ordering.
    budget_result?;

    // Honour the run-status exit-code contract. A model that failed to
    // compile (recorded in `output.errors` / `tables_failed` by
    // `execute_models`) makes the run a `Failure` (nothing built) or
    // `PartialFailure` (some models built). The JSON `RunOutput` was
    // already emitted above, so a consumer keying on `status` / `errors`
    // sees the failure; this just propagates the non-zero exit code.
    super::run::run_status_exit_result(&output, run_id)
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

                // Custom checks — shared with the replication runner (run.rs)
                // via `run_custom_checks`.
                checks.extend(
                    run_custom_checks(
                        warehouse_adapter.as_ref(),
                        &full_table,
                        &pipeline.checks.custom,
                    )
                    .await,
                );

                // Row-level assertions — match by unqualified table name.
                // Shared with the replication runner (run.rs) via
                // `run_table_assertions` so uniqueness and the other assertions
                // fire identically on both surfaces.
                checks.extend(
                    run_table_assertions(
                        warehouse_adapter.as_ref(),
                        dialect,
                        &full_table,
                        table_name,
                        &pipeline.checks.assertions,
                    )
                    .await,
                );

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
                    let ir_ref = rocky_ir::TableRef {
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
        crate::status_line!(
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

/// Execute every assertion targeting `table_unqualified` against `full_table`,
/// returning one [`CheckResult`] per matching assertion.
///
/// Shared by the quality runner (this module) and the replication runner
/// (`run.rs`) so uniqueness and the other row-level assertions fire on both
/// surfaces with identical pass/fail classification. `full_table` must already
/// be dialect-formatted (quoted/qualified). Degradation is graceful: an
/// SQL-generation error skips that assertion; a query error records a failed
/// `CheckResult` — neither aborts the caller.
/// Execute every custom check against `full_table`, returning one
/// [`CheckResult`] per check. Shared by the quality runner (this module) and
/// the replication runner (`run.rs`) so `[[checks.custom]]` fires identically
/// on both surfaces. `full_table` must already be dialect-formatted
/// (quoted/qualified). A query error records a failed `CheckResult` rather than
/// aborting the caller.
pub(crate) async fn run_custom_checks(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    full_table: &str,
    customs: &[rocky_core::config::CustomCheckConfig],
) -> Vec<rocky_core::checks::CheckResult> {
    use rocky_core::checks::{CheckDetails, CheckResult, cell_as_u64};

    let mut results = Vec::new();
    for custom in customs {
        let sql = custom.sql.replace("{table}", full_table);
        let severity = custom.severity;
        match warehouse.execute_query(&sql).await {
            Ok(result) => {
                // `cell_as_u64` returns `None` on an absent/unparseable cell
                // *specifically* so a parse failure isn't read as a real 0.
                // With the default `threshold = 0` a violation-counting check
                // passes iff `result_value <= 0`, so defaulting `None` to 0
                // would report a never-evaluated check as green. Treat an
                // unparseable count as a check failure instead.
                match cell_as_u64(result.rows.first().and_then(|r| r.first())) {
                    Some(result_value) => {
                        // Delegate to the single canonical custom-check evaluator so
                        // the wired path and `checks::check_custom` can't drift on the
                        // pass condition again. `threshold` is the MAX allowed
                        // failing-row count, so the check passes iff
                        // `result_value <= threshold`. `check_custom` defaults
                        // severity to Error; restore the check's configured severity.
                        let mut check = rocky_core::checks::check_custom(
                            &custom.name,
                            &sql,
                            result_value,
                            custom.threshold,
                        );
                        check.severity = severity;
                        results.push(check);
                    }
                    None => {
                        warn!(
                            check = custom.name.as_str(),
                            "custom check returned no parseable count; failing the check"
                        );
                        results.push(CheckResult {
                            name: custom.name.clone(),
                            passed: false,
                            severity,
                            details: CheckDetails::Custom {
                                query: sql,
                                result_value: 0,
                                threshold: custom.threshold,
                            },
                        });
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, check = custom.name.as_str(), "custom check query failed");
                results.push(CheckResult {
                    name: custom.name.clone(),
                    passed: false,
                    severity,
                    details: CheckDetails::Custom {
                        query: sql,
                        result_value: 0,
                        threshold: custom.threshold,
                    },
                });
            }
        }
    }
    results
}

pub(crate) async fn run_table_assertions(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    dialect: &dyn rocky_core::traits::SqlDialect,
    full_table: &str,
    table_unqualified: &str,
    assertions: &[rocky_core::config::QualityAssertion],
) -> Vec<rocky_core::checks::CheckResult> {
    use rocky_core::checks::{CheckDetails, CheckResult};
    use rocky_core::tests::generate_test_sql_with_dialect;

    let mut results = Vec::new();
    for assertion in assertions {
        if assertion.table.as_str() != table_unqualified {
            continue;
        }
        let test = &assertion.test;
        let sql = match generate_test_sql_with_dialect(test, full_table, dialect) {
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
        let kind = rocky_core::tests::test_type_kind(&test.test_type);
        let name = assertion.resolved_name();

        match warehouse.execute_query(&sql).await {
            Ok(result) => {
                let (passed, failing_rows) = classify_assertion(&test.test_type, &result.rows);
                results.push(CheckResult {
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
                warn!(error = %e, table = %full_table, assertion = %name, "assertion query failed");
                results.push(CheckResult {
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
    results
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
        | TestType::UniqueExpr { .. }
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

    // sql_gen consumes the typed IR directly.
    let model_ir = ModelIr::snapshot(
        TargetRef {
            catalog: pipeline.target.catalog.clone(),
            schema: pipeline.target.schema.clone(),
            table: pipeline.target.table.clone(),
        },
        SourceRef {
            catalog: pipeline.source.catalog.clone(),
            schema: pipeline.source.schema.clone(),
            table: pipeline.source.table.clone(),
        },
        pipeline
            .unique_key
            .iter()
            .map(|s| std::sync::Arc::from(s.as_str()))
            .collect(),
        pipeline.updated_at.clone(),
        pipeline.invalidate_hard_deletes,
        rocky_ir::GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
            auto_create_schemas: pipeline.target.governance.auto_create_schemas,
        },
    );

    let dialect = warehouse_adapter.dialect();
    let stmts = sql_gen::generate_snapshot_sql(&model_ir, dialect)?;

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
            attempts: Vec::new(),
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
            // attribution + job-id capture is a follow-up wave.
            bytes_scanned: None,
            bytes_written: None,
            // snapshot models carry no tenant dimension.
            tenant: None,
            job_ids: Vec::new(),
            skip_internal: None,
            recipe_identity: Some(crate::output::recipe_identity_internal(
                &model_ir,
                dialect.name(),
            )),
            // Not the content-addressed write path — no in-process column bytes.
            output_column_hashes: None,
            // Consumer baseline is content-addressed-path only.
            consumed_column_baseline: None,
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
        crate::status_line!(
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

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use std::path::Path;

    use rocky_core::state::StateStore;
    use rocky_core::traits::WarehouseAdapter;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    use super::super::run::{DeferOptions, PartitionRunOptions, SkipRunOptions};

    /// Seed a raw `main.src` table with `n` integer rows in the persistent
    /// DuckDB file. Drops the connection before returning so `rocky run` can
    /// reopen the file.
    async fn seed_src(db: &Path, n: u32) {
        let a = DuckDbWarehouseAdapter::open(db).expect("seed open");
        a.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
            .await
            .unwrap();
        a.execute_statement("DROP TABLE IF EXISTS main.src")
            .await
            .unwrap();
        let values: Vec<String> = (1..=n).map(|i| format!("({i})")).collect();
        a.execute_statement(&format!(
            "CREATE TABLE main.src AS SELECT * FROM (VALUES {}) AS t(id)",
            values.join(", ")
        ))
        .await
        .unwrap();
    }

    /// Insert a sentinel row into `main.<table>` so a later run can detect
    /// whether the target was rewritten: a full_refresh CTAS drops it, a skip
    /// leaves it.
    async fn insert_sentinel(db: &Path, table: &str, sentinel: i64) {
        let a = DuckDbWarehouseAdapter::open(db).expect("sentinel open");
        a.execute_statement(&format!("INSERT INTO main.{table} VALUES ({sentinel})"))
            .await
            .unwrap();
    }

    /// Count rows in `main.<table>`.
    async fn count_rows(db: &Path, table: &str) -> i64 {
        let a = DuckDbWarehouseAdapter::open(db).expect("count open");
        let r = a
            .execute_query(&format!("SELECT COUNT(*) FROM main.{table}"))
            .await
            .unwrap();
        r.rows[0][0]
            .as_i64()
            .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .unwrap()
    }

    /// Write a `full_refresh` SQL model (`name.sql` + `name.toml`) into the
    /// project's `main` schema, with an optional `depends_on` sidecar key.
    fn write_model(dir: &Path, name: &str, sql: &str, depends_on: &[&str]) {
        std::fs::write(dir.join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
        let dep_line = if depends_on.is_empty() {
            String::new()
        } else {
            let list = depends_on
                .iter()
                .map(|d| format!("\"{d}\""))
                .collect::<Vec<_>>()
                .join(", ");
            format!("depends_on = [{list}]\n")
        };
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "{dep_line}[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    /// Drive `super::super::run::run()` end-to-end against the given config +
    /// canonical `state_path`, exercising the full transformation dispatch
    /// (`run() → run_transformation`). `skip_unchanged` toggles the gate.
    async fn run_full_dag(config_path: &Path, state_path: &Path, skip_unchanged: bool) {
        let opts = PartitionRunOptions::default();
        let skip_opts = SkipRunOptions {
            skip_unchanged,
            force_rebuild: false,
            no_reuse: false,
            no_prune: false,
        };
        super::super::run::run(
            config_path,
            std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(config_path).unwrap(),
            ),
            None, // filter
            None, // pipeline_name_arg — single pipeline resolves
            state_path,
            None,  // governance_override
            false, // output_json
            None,  // models_dir override — take from config
            false, // run_all
            None,  // resume_run_id
            false, // resume_latest
            None,  // shadow_config
            &opts,
            None, // model_name_filter
            None, // cache_ttl_override
            None, // idempotency_key
            None, // env
            &DeferOptions::default(),
            &skip_opts,
            &rocky_core::run_vars::RunVars::new(),
            None,  // no run_id override — mint the usual timestamp id
            None,  // no governance ctx (test)
            false, // assume_fresh_state (test)
        )
        .await
        .expect("full-DAG transformation run should succeed");
    }

    /// Write the project `rocky.toml`. `skip_block` controls whether the
    /// `[run]` gate is enabled in config — passed empty for the default-off
    /// test so the CLI overlay is the only thing that could turn it on.
    fn write_config(dir: &Path, db: &Path, skip_block: &str) {
        std::fs::write(
            dir.join("rocky.toml"),
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{db}"
{skip_block}
[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target.governance]
auto_create_schemas = true
"#,
                db = db.display()
            ),
        )
        .unwrap();
    }

    /// The `[run]` block that enables the skip gate + rowcount fallback (a
    /// full_refresh leaf over a raw source has no tracked timestamp column, so
    /// rowcount is the available B3 signal).
    const SKIP_ENABLED: &str = "\n[run]\nskip_unchanged = true\nskip_rowcount_fallback = true\n";

    /// Regression test for the full-DAG `--skip-unchanged` bug.
    ///
    /// Before the fix, `run() → run_transformation` opened a non-canonical
    /// `.rocky_state` file and never persisted a `RunRecord`, so the skip
    /// gate had no prior-successful baseline and the full-DAG path NEVER
    /// skipped — and the run never showed in `rocky history`.
    ///
    /// This drives `run()` end-to-end (NO `--model`) over a 2-model
    /// transformation DAG and asserts the second byte-identical run SKIPS
    /// (sentinel rows survive), a later logic change rebuilds only the changed
    /// model, and the run is persisted to the CANONICAL `state_path`.
    #[tokio::test]
    async fn full_dag_skip_unchanged_skips_and_persists_history() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        // Canonical state path — the SAME location `main.rs` resolves and the
        // replication path opens. The bug persisted to `.rocky_state` instead.
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        write_model(&models_dir, "stg", "SELECT id FROM main.src", &[]);
        write_model(&models_dir, "mart", "SELECT id FROM main.stg", &["stg"]);
        write_config(dir, &db, SKIP_ENABLED);
        let config_path = dir.join("rocky.toml");

        // ---- Run 1: fresh state ⇒ both models build. -------------------
        run_full_dag(&config_path, &state_path, true).await;
        assert_eq!(count_rows(&db, "stg").await, 3, "run 1 builds stg");
        assert_eq!(count_rows(&db, "mart").await, 3, "run 1 builds mart");

        // The fix's history half: the canonical state store now carries this
        // run. Before the fix, `run_transformation` never persisted, so the
        // canonical store had nothing and `rocky history` was empty.
        //
        // redb permits a single writer per file, so this read handle MUST be
        // dropped before the next `run()` opens the same path — hence the
        // explicit scope.
        {
            let store = StateStore::open(&state_path).expect("open canonical state");
            let runs = store.list_runs(10).expect("list runs");
            assert!(
                !runs.is_empty(),
                "a full-DAG transformation run must appear in history at the \
                 canonical state path"
            );
            assert!(
                store
                    .get_model_history("stg", 1)
                    .expect("model history")
                    .into_iter()
                    .any(|m| m.status == "success"),
                "the prior successful ModelExecution baseline must be persisted"
            );
        }

        // Sentinels: a skip leaves these rows (4 each); a rebuild's CTAS drops
        // them (back to 3).
        insert_sentinel(&db, "stg", 999).await;
        insert_sentinel(&db, "mart", 999).await;
        assert_eq!(count_rows(&db, "stg").await, 4);
        assert_eq!(count_rows(&db, "mart").await, 4);

        // ---- Run 2: byte-identical ⇒ both SKIP (sentinels survive). ----
        run_full_dag(&config_path, &state_path, true).await;
        assert_eq!(
            count_rows(&db, "stg").await,
            4,
            "run 2 must SKIP stg — sentinel survives (this is the bug's \
             regression assertion)"
        );
        assert_eq!(
            count_rows(&db, "mart").await,
            4,
            "run 2 must SKIP mart — sentinel survives"
        );

        // ---- Run 3: change mart's logic ⇒ mart rebuilds, stg still skips.
        write_model(
            &models_dir,
            "mart",
            "SELECT id FROM main.stg WHERE id > 1",
            &["stg"],
        );
        run_full_dag(&config_path, &state_path, true).await;
        assert_eq!(
            count_rows(&db, "stg").await,
            4,
            "run 3 must still SKIP stg — its logic + upstream are unchanged"
        );
        // mart rebuilt: the CTAS dropped the sentinel and re-derived from stg
        // (4 rows in stg: ids 1,2,3,999 — WHERE id > 1 keeps 2,3,999 ⇒ 3 rows).
        assert_eq!(
            count_rows(&db, "mart").await,
            3,
            "run 3 must REBUILD mart — changed logic drops the sentinel"
        );
    }

    /// Drive `run()` end-to-end over the full-DAG transformation path WITH an
    /// `--idempotency-key`, then assert the persisted `RunRecord` carries that
    /// key in its audit.
    ///
    /// Before the fix, `run_transformation` built its audit with
    /// `AuditContext::detect(None, None)`, so the persisted `RunRecord`'s
    /// `idempotency_key` was `None` even though the run finalized the
    /// idempotency entry under `K` — `rocky history --audit` showed
    /// `idempotency_key=-` instead of `K`. The model-only / replication paths
    /// pass the claimed key through; this is the transformation path matching
    /// them.
    #[tokio::test]
    async fn full_dag_persists_idempotency_key_in_audit() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        // Canonical state path — the SAME location `main.rs` resolves and the
        // replication path opens.
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        write_model(&models_dir, "stg", "SELECT id FROM main.src", &[]);
        write_config(dir, &db, "");
        let config_path = dir.join("rocky.toml");

        let key = "ci-deadbeef-1234";
        let opts = PartitionRunOptions::default();
        super::super::run::run(
            &config_path,
            std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            ),
            None, // filter
            None, // pipeline_name_arg — single pipeline resolves
            &state_path,
            None,  // governance_override
            false, // output_json
            None,  // models_dir override — take from config
            false, // run_all
            None,  // resume_run_id
            false, // resume_latest
            None,  // shadow_config
            &opts,
            None,      // model_name_filter
            None,      // cache_ttl_override
            Some(key), // idempotency_key — the value under test
            None,      // env
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            None,  // no run_id override — mint the usual timestamp id
            None,  // no governance ctx (test)
            false, // assume_fresh_state (test)
        )
        .await
        .expect("full-DAG transformation run with idempotency key should succeed");

        // The persisted audit must carry the claimed key, matching the
        // idempotency entry the dispatch site finalized under the same value.
        let store = StateStore::open(&state_path).expect("open canonical state");
        let runs = store.list_runs(10).expect("list runs");
        let record = runs
            .iter()
            .find(|r| r.idempotency_key.as_deref() == Some(key))
            .unwrap_or_else(|| {
                panic!(
                    "persisted RunRecord must carry idempotency_key={key:?}; got {:?}",
                    runs.iter().map(|r| &r.idempotency_key).collect::<Vec<_>>()
                )
            });
        assert_eq!(
            record.idempotency_key.as_deref(),
            Some(key),
            "the full-DAG transformation run's persisted audit must record the \
             claimed idempotency key (not None)"
        );
    }

    /// Default-off must stay byte-identical: with `--skip-unchanged` NOT set,
    /// every model rebuilds every run (sentinels are dropped), even when logic
    /// + inputs are unchanged.
    #[tokio::test]
    async fn full_dag_default_off_always_rebuilds() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        write_model(&models_dir, "stg", "SELECT id FROM main.src", &[]);
        // Empty `[run]` block ⇒ the CLI overlay is the only thing that could
        // enable the gate; with `skip_unchanged = false` it stays fully inert.
        write_config(dir, &db, "");
        let config_path = dir.join("rocky.toml");

        run_full_dag(&config_path, &state_path, false).await;
        insert_sentinel(&db, "stg", 999).await;
        assert_eq!(count_rows(&db, "stg").await, 4);

        // Gate off ⇒ run 2 rebuilds unconditionally ⇒ sentinel dropped.
        run_full_dag(&config_path, &state_path, false).await;
        assert_eq!(
            count_rows(&db, "stg").await,
            3,
            "default-off must rebuild every run — sentinel dropped"
        );
    }

    /// Reached-ness guard for per-model `[governance.tags]` application.
    ///
    /// Drives `run() → run_transformation` over a transformation pipeline whose
    /// model is a **view** carrying a `[governance.tags]` block. The post-
    /// materialize governance step must execute without panicking and the run
    /// must succeed end-to-end. DuckDB's `GovernanceAdapter` is a no-op, so
    /// this proves only that the apply block is *reached* on the real run path
    /// (the recurring trap is a generator wired nowhere) — the actual
    /// `ALTER VIEW ... SET TAGS` DDL is proven by the live Databricks test.
    #[tokio::test]
    async fn full_dag_view_governance_tags_run_reaches_apply_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        // A view-strategy model with a `[governance.tags]` block — the exact
        // shape that routes to `TagTarget::View` in the apply step.
        std::fs::write(models_dir.join("v_orders.sql"), "SELECT id FROM main.src\n").unwrap();
        std::fs::write(
            models_dir.join("v_orders.toml"),
            "[strategy]\ntype = \"view\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"v_orders\"\n\n\
             [governance.tags]\ndomain = \"finance\"\ntier = \"gold\"\n",
        )
        .unwrap();
        write_config(dir, &db, "");
        let config_path = dir.join("rocky.toml");

        // The run must succeed: the governance.tags apply block runs (no-op on
        // DuckDB) without aborting the transformation.
        run_full_dag(&config_path, &state_path, false).await;

        // The view materialized and is queryable.
        let a = DuckDbWarehouseAdapter::open(&db).expect("open db");
        let r = a
            .execute_query("SELECT COUNT(*) FROM main.v_orders")
            .await
            .expect("view must be queryable after run");
        let n = r.rows[0][0]
            .as_i64()
            .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .unwrap();
        assert_eq!(n, 3, "view over the 3-row source must return 3 rows");
    }

    /// Reached-ness guard for the `rocky run --model <X>` path's per-model
    /// `[governance.tags]` application — the call site B4 fixed.
    ///
    /// Drives `run()` with `model_name_filter = Some("v_orders")` over a view
    /// model carrying a `[governance.tags]` block. The model-only path must
    /// reach the new `apply_model_governance_tags` step (no-op on DuckDB's Noop
    /// adapter) without aborting, and the model must materialize. Before B4
    /// this path never called the tag loop at all.
    #[tokio::test]
    async fn model_only_governance_tags_run_reaches_apply_path() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        std::fs::write(models_dir.join("v_orders.sql"), "SELECT id FROM main.src\n").unwrap();
        std::fs::write(
            models_dir.join("v_orders.toml"),
            "[strategy]\ntype = \"view\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"v_orders\"\n\n\
             [governance.tags]\ndomain = \"finance\"\ntier = \"gold\"\n",
        )
        .unwrap();
        write_config(dir, &db, "");
        let config_path = dir.join("rocky.toml");

        let opts = PartitionRunOptions::default();
        super::super::run::run(
            &config_path,
            std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            ),
            None,
            None,
            &state_path,
            None,
            false,             // output_json
            Some(&models_dir), // models_dir override — model-only path needs it
            false,             // run_all
            None,
            false,
            None,
            &opts,
            Some("v_orders"), // model_name_filter — the path under test
            None,
            None,
            None,
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            None,  // no run_id override — mint the usual timestamp id
            None,  // no governance ctx (test)
            false, // assume_fresh_state (test)
        )
        .await
        .expect("model-only run must reach the governance.tags apply path and succeed");

        // The view materialized — proving the model-only path ran end-to-end
        // through the new apply step.
        let a = DuckDbWarehouseAdapter::open(&db).expect("open db");
        let r = a
            .execute_query("SELECT COUNT(*) FROM main.v_orders")
            .await
            .expect("view must be queryable after model-only run");
        let n = r.rows[0][0]
            .as_i64()
            .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .unwrap();
        assert_eq!(n, 3, "view over the 3-row source must return 3 rows");
    }

    /// A transformation model with valid SQL that fails at *execution* (it
    /// reads a table the warehouse can't resolve) is a first-class run
    /// failure, not a short-circuit that strands the orchestrator with empty
    /// stdout.
    ///
    /// The run must (a) exit non-zero — `run()` returns `Err` — and (b) reach
    /// the post-execute path: `persist_run_record` runs *before* the JSON emit
    /// on the fall-through, so a persisted `RunRecord` with status `Failure`
    /// proves the JSON `RunOutput` was emitted on the runtime-failure path.
    /// Before the fix the `?` on `execute_models` short-circuited before both,
    /// so no record was persisted and nothing was written to stdout.
    #[tokio::test]
    async fn runtime_failure_persists_failure_record_and_exits_nonzero() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        let state_path = dir.join(".rocky-state.redb");

        // Valid SQL, but `main.does_not_exist_xyz` is absent — DuckDB rejects
        // it at execute time, a runtime failure (not a compile error).
        std::fs::write(
            models_dir.join("bad.sql"),
            "SELECT * FROM main.does_not_exist_xyz\n",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("bad.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"bad\"\n",
        )
        .unwrap();
        write_config(dir, &db, "");
        let config_path = dir.join("rocky.toml");

        let opts = PartitionRunOptions::default();
        let result = super::super::run::run(
            &config_path,
            std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            ),
            None,
            None,
            &state_path,
            None,
            true, // output_json — the path an orchestrator drives
            None,
            false,
            None,
            false,
            None,
            &opts,
            None,
            None,
            None,
            None,
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            None,  // no run_id override — mint the usual timestamp id
            None,  // no governance ctx (test)
            false, // assume_fresh_state (test)
        )
        .await;

        // (a) Non-zero exit: the run surfaces as Err.
        assert!(
            result.is_err(),
            "a runtime model failure must exit non-zero, not report success"
        );

        // (b) The post-execute path was reached (JSON emit + persist), so a
        // RunRecord exists and its derived status is Failure.
        let store = StateStore::open(&state_path).expect("open canonical state");
        let runs = store.list_runs(10).expect("list runs");
        assert_eq!(
            runs.len(),
            1,
            "the runtime-failing run must still persist a RunRecord \
             (proving it reached the post-execute JSON-emit path)"
        );
        assert!(
            matches!(runs[0].status, rocky_core::state::RunStatus::Failure),
            "the persisted run status must be Failure, got {:?}",
            runs[0].status
        );
    }

    #[tokio::test]
    async fn custom_check_with_unparseable_count_fails_closed() {
        // A custom check whose query succeeds but returns a non-numeric cell
        // must FAIL, not silently pass. Previously the count defaulted to 0
        // and `0 <= threshold(0)` reported the never-evaluated check green.
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("checks.duckdb");
        {
            let a = DuckDbWarehouseAdapter::open(&db).expect("open");
            a.execute_statement("CREATE SCHEMA IF NOT EXISTS main")
                .await
                .unwrap();
            a.execute_statement("CREATE TABLE main.t AS SELECT 1 AS id")
                .await
                .unwrap();
        }
        let warehouse = DuckDbWarehouseAdapter::open(&db).expect("reopen");

        let unparseable = rocky_core::config::CustomCheckConfig {
            name: "bad_count".to_string(),
            // Succeeds, but the single cell is a string cell_as_u64 can't parse.
            sql: "SELECT 'not-a-number'".to_string(),
            threshold: 0,
            severity: rocky_core::tests::TestSeverity::Error,
        };
        let good = rocky_core::config::CustomCheckConfig {
            name: "zero_violations".to_string(),
            sql: "SELECT 0".to_string(),
            threshold: 0,
            severity: rocky_core::tests::TestSeverity::Error,
        };

        let results = super::run_custom_checks(&warehouse, "main.t", &[unparseable, good]).await;

        assert_eq!(results.len(), 2);
        assert!(
            !results[0].passed,
            "an unparseable count must fail the check, not pass on a defaulted 0"
        );
        assert!(results[1].passed, "a genuine 0 <= threshold still passes");
    }

    /// Single-decision lazy gate: the dispatch site's resolved
    /// [`super::ModelsDirDecision`] — not a fresh filesystem check — governs
    /// whether `run_transformation` executes. The models directory EXISTS on
    /// disk with a compilable model, but the passed decision says `Absent`
    /// (modeling a directory created between `run()`'s session decision and
    /// execution): the run must be a guaranteed no-op — no state store opened
    /// (no state file created), nothing built. Pre-fix RED:
    /// `run_transformation` re-checked `models_dir.exists()` itself, so the
    /// on-disk directory resurrected execution and persisted a `RunRecord`
    /// with NO session (the RD-003 sessionless-mutation shape via a race).
    #[tokio::test]
    async fn passed_absent_decision_governs_even_when_dir_exists() {
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path();
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        let db = dir.join("t.duckdb");
        let state_path = dir.join(".rocky-state.redb");

        seed_src(&db, 3).await;
        write_model(&models_dir, "stg", "SELECT id FROM main.src", &[]);
        write_config(dir, &db, "");
        let loaded =
            rocky_core::config::load_rocky_config_fingerprinted(&dir.join("rocky.toml")).unwrap();
        let rocky_cfg = &loaded.config;
        let (_, pipeline_config) =
            crate::registry::resolve_pipeline(rocky_cfg, None).expect("resolve pipeline");
        let rocky_core::config::PipelineConfig::Transformation(t) = pipeline_config else {
            panic!("test config must resolve to a transformation pipeline");
        };

        super::run_transformation(
            super::ModelsDirDecision::Absent(models_dir.clone()),
            t,
            rocky_cfg,
            false, // output_json
            &PartitionRunOptions::default(),
            &rocky_core::config::SchemaCacheConfig::default(),
            super::super::run::SkipGateConfig::off(),
            false, // no_reuse
            &state_path,
            "run-decision-governs",
            chrono::Utc::now(),
            "cfg-hash-test",
            None, // idempotency_key
            &rocky_core::run_vars::RunVars::new(),
            None,  // exec_fp_gate
            false, // expects_models
        )
        .await
        .expect("an Absent decision is the silent no-op, even with the dir on disk");

        assert!(
            !state_path.exists(),
            "the PASSED decision must govern: no state store may be opened (no state \
             file created) even though the models directory exists on disk"
        );
        let a = DuckDbWarehouseAdapter::open(&db).expect("open db");
        let r = a
            .execute_query(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = 'main' AND table_name = 'stg'",
            )
            .await
            .unwrap();
        let built = r.rows[0][0]
            .as_i64()
            .or_else(|| r.rows[0][0].as_str().and_then(|s| s.parse().ok()))
            .unwrap();
        assert_eq!(
            built, 0,
            "nothing may materialize on an Absent decision, even with the dir on disk"
        );
    }
}
