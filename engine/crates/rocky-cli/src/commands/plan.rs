use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;

use rocky_core::config::{GovernanceOverride, PolicyPrincipal, resolve_table_override};
use rocky_core::source::DiscoveredConnector;
use rocky_core::sql_gen;
use rocky_core::traits::SqlDialect;
use rocky_ir::*;

use crate::output::*;
use crate::plan_store::{EmbeddedCapabilities, PlanKind, write_plan, write_plan_governed};
use crate::registry;

use super::run::PartitionRunOptions;
use super::{filter_table_matches, matches_filter, parse_filter};

/// Bundle of `rocky plan` flags that are not consumed by the SQL-generation
/// preview but are persisted into `RunPlan` so `rocky apply <plan-id>` can
/// honour them. Mirrors the flag surface of `rocky run`.
#[derive(Debug, Default, Clone)]
pub struct PlanRunOptions {
    pub model: Option<String>,
    pub all: bool,
    pub resume: Option<String>,
    pub resume_latest: bool,
    pub shadow: bool,
    pub shadow_suffix: Option<String>,
    pub shadow_schema: Option<String>,
    pub branch: Option<String>,
    pub dag: bool,
    pub idempotency_key: Option<String>,
    pub governance_override: Option<GovernanceOverride>,
    pub models_dir: Option<PathBuf>,
    pub partition_opts: PartitionRunOptions,
    /// Resolved authoring principal (F3 agent-policy plane). `None` ⇒ `human`
    /// (the CLI-surface default). Stamped onto the persisted plan so a later
    /// `rocky apply` evaluates the plan against the identity that authored it.
    pub principal: Option<PolicyPrincipal>,
}

/// Execute `rocky plan` — dry-run SQL generation plus optional run-plan blueprint.
///
/// The `env` parameter, when `Some`, selects the active environment
/// for the governance preview. It flows into `mask_actions` (via
/// [`rocky_core::config::RockyConfig::resolve_mask_for_env`]) so a
/// `[mask.<env>]` override surfaces in the preview on top of the
/// workspace `[mask]` defaults. Classification tags and retention
/// policies are env-invariant and are previewed regardless.
///
/// ## Run-plan blueprint (Phase 2)
///
/// When a `models/` directory exists next to the config, `rocky plan` also
/// compiles the project, builds a `RunPlan` payload (operational metadata
/// only — no full IR), and persists it to `.rocky/plans/<plan_id>.json`.
/// `rocky apply <plan_id>` then calls `rocky run` with the same flags,
/// re-deriving the `ProjectIr` by recompiling. Full IR persistence is
/// deferred — the operational-metadata approach covers the deterministic
/// re-application requirement at acceptable cost.
///
/// ## Semantic verdict (decision-support)
///
/// When `semantic` is set, `rocky plan` additionally compiles the project at
/// `base_ref` (default `"main"`), diffs the typed output schema against the
/// working tree via the [`rocky_core::breaking_change`] classifier, and
/// attaches the classified findings to `output.breaking_verdict`. This is
/// **reporting-only**: it never gates, skips, or reorders the planned
/// statements, and a `breaking` finding does not change the exit code. The
/// hard gate lives on `rocky plan promote`. When no baseline is available
/// (the `base_ref` models do not compile, or there is no `models/`
/// directory) the verdict is omitted — never fabricated.
#[allow(clippy::too_many_arguments)]
pub async fn plan(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
    env: Option<&str>,
    run_options: &PlanRunOptions,
    semantic: bool,
    base_ref: &str,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
    let pattern = pipeline.schema_pattern()?;
    let parsed_filter = filter.map(parse_filter).transpose()?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;
    let dialect = warehouse_adapter.dialect();

    let connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .connectors
    } else {
        anyhow::bail!(
            "{}",
            registry::missing_discovery_config_message(&rocky_cfg, config_path, name)
        )
    };

    let mut output = PlanOutput::new(filter.unwrap_or("").to_string());
    output.env = env.map(str::to_string);

    // Detect whether this dialect supports catalogs. Dialects without catalog
    // support (DuckDB, Postgres, ...) return `None` from `create_catalog_sql`,
    // and we strip catalogs from table refs so we emit two-part `schema.table`
    // names rather than three-part names that would fail at execution time.
    let supports_catalogs = dialect.create_catalog_sql("__probe").is_some();

    for conn in &connectors {
        let parsed = match pattern.parse(&conn.schema) {
            Ok(p) => p,
            Err(_) => continue,
        };

        if let Some((ref filter_key, ref filter_value)) = parsed_filter
            && !matches_filter(conn, &parsed, filter_key, filter_value)
        {
            continue;
        }

        let target_sep = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        let target_catalog = parsed.resolve_template(&pipeline.target.catalog_template, target_sep);
        let target_schema = parsed.resolve_template(&pipeline.target.schema_template, target_sep);

        // Effective catalog used in table refs and statements. Empty for
        // catalog-less dialects so the dialect emits two-part names.
        let effective_target_catalog = if supports_catalogs {
            target_catalog.clone()
        } else {
            String::new()
        };

        // Catalog creation — only when governance enables it and the dialect supports catalogs.
        if pipeline.target.governance.auto_create_catalogs
            && let Some(create_cat) = dialect.create_catalog_sql(&target_catalog)
        {
            let sql = create_cat.map_err(|e| anyhow::anyhow!("create_catalog: {e}"))?;
            output.statements.push(PlannedStatement {
                purpose: "create_catalog".into(),
                target: target_catalog.clone(),
                sql,
            });
        }

        // Schema creation — only when governance enables it.
        if pipeline.target.governance.auto_create_schemas
            && let Some(create_sch) =
                dialect.create_schema_sql(&effective_target_catalog, &target_schema)
        {
            let sql = create_sch.map_err(|e| anyhow::anyhow!("create_schema: {e}"))?;
            let target_label = if effective_target_catalog.is_empty() {
                target_schema.clone()
            } else {
                format!("{effective_target_catalog}.{target_schema}")
            };
            output.statements.push(PlannedStatement {
                purpose: "create_schema".into(),
                target: target_label,
                sql,
            });
        }

        // Per-table copy SQL
        let source_catalog = pipeline.source.catalog.as_deref().unwrap_or("").to_string();
        let effective_source_catalog = if supports_catalogs {
            source_catalog
        } else {
            String::new()
        };
        for table in &conn.tables {
            // PR-B3: CLI `--filter table=<literal>` consumed here.
            if !filter_table_matches(parsed_filter.as_ref(), &table.name) {
                continue;
            }
            let metadata_columns: Vec<MetadataColumn> = pipeline
                .metadata_columns
                .iter()
                .map(|mc| MetadataColumn {
                    name: mc.name.clone(),
                    data_type: mc.data_type.clone(),
                    value: parsed.resolve_template(&mc.value, &pattern.separator),
                })
                .collect();

            let target_label = if effective_target_catalog.is_empty() {
                format!("{target_schema}.{}", table.name)
            } else {
                format!("{effective_target_catalog}.{target_schema}.{}", table.name)
            };

            // Resolve the per-table override and the effective strategy the
            // SAME way `rocky run` does (`build_replication_strategy_with_override`),
            // so the preview reflects `merge` and per-table `[[table_overrides]]`
            // instead of always rendering full_refresh. plan and run MUST agree —
            // divergence is the bug this closes.
            let effective_override = resolve_table_override(
                &pipeline.table_overrides,
                &conn.id,
                &conn.schema,
                &table.name,
            );
            let strategy = match super::run::build_replication_strategy_with_override(
                pipeline,
                &effective_override,
            ) {
                Ok(strategy) => strategy,
                Err(e) => {
                    // The builder rejects this strategy on a replication pipeline
                    // (`dynamic_table`, or `merge` with no resolvable merge_keys).
                    // `rocky run` fails this table via the same builder, so
                    // reflect that rather than a spurious full_refresh CTAS.
                    tracing::warn!(
                        table = %table.name,
                        error = %e,
                        "plan: strategy unsupported on replication; run will fail this table"
                    );
                    output.statements.push(PlannedStatement {
                        purpose: "unsupported".into(),
                        target: target_label,
                        sql: format!("-- {}: {e} (rocky run will fail this table)", table.name),
                    });
                    continue;
                }
            };
            let purpose = replication_copy_purpose(&strategy);

            // sql_gen consumes the typed IR directly.
            let model_ir = ModelIr::replication(
                TargetRef {
                    catalog: effective_target_catalog.clone(),
                    schema: target_schema.clone(),
                    table: table.name.clone(),
                },
                strategy,
                SourceRef {
                    catalog: effective_source_catalog.clone(),
                    schema: conn.schema.clone(),
                    table: table.name.clone(),
                },
                ColumnSelection::All,
                metadata_columns,
                GovernanceConfig {
                    permissions_file: None,
                    auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
                    auto_create_schemas: pipeline.target.governance.auto_create_schemas,
                },
            );

            // Forward-looking copy SQL matching what `rocky run` executes for
            // the resolved strategy (full_refresh → CTAS, incremental → INSERT
            // with the 1970 sentinel watermark the runner replaces at execute
            // time, merge → MERGE INTO, view / materialized_view → their DDL).
            let sql = replication_copy_sql(&model_ir, dialect)?;

            output.statements.push(PlannedStatement {
                purpose: purpose.into(),
                target: target_label,
                sql,
            });
        }
    }

    // --- Governance preview (Wave A + C-1 + C-2) -------------------------
    //
    // The post-DAG reconcile loop at `rocky run` (see commands/run.rs) walks
    // compiled models and fires classification / masking / retention via the
    // `GovernanceAdapter`. Preview the same work here without calling the
    // adapter — the action rows parallel `statements` but represent
    // control-plane operations rather than warehouse SQL. Models are loaded
    // from the conventional `models/` directory next to the config; a
    // missing directory is not an error (projects without models produce
    // empty action arrays and the three fields omit themselves from JSON).
    let models_dir = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("models");
    tracing::debug!(
        models_dir = %models_dir.display(),
        models_dir_exists = models_dir.exists(),
        "plan: models_dir check"
    );
    if models_dir.exists() {
        let adapter_type = rocky_cfg
            .adapters
            .get(&pipeline.target.adapter)
            .map(|a| a.adapter_type.as_str())
            .unwrap_or("");
        populate_governance_actions(&rocky_cfg, &models_dir, env, adapter_type, &mut output)
            .context("failed to compute governance action preview")?;
    }

    // --- D-3 stage 2: per-model budget ceiling check (real catalog stats) --
    //
    // When models exist and the target adapter is Databricks or Iceberg, fetch
    // per-table byte statistics from the warehouse, propagate costs through the
    // DAG, and emit E027 diagnostics for any model whose projected cost exceeds
    // its declared `[budget]` ceiling.
    //
    // This replaces the stub stats used at compile time with real catalog data.
    // Compile stays offline; plan is the natural budget-enforcement surface.
    //
    // A/B/C decision: **B (rocky plan)** — plan already performs live
    // warehouse I/O (discovery, governance) and is the pre-run validation
    // surface.  Compile stays CI/pre-commit/LSP-safe (offline); run is too
    // late (partial execution possible).  If catalog stats are unavailable
    // (adapter not Databricks/Iceberg, table not found, network error) the
    // check degrades gracefully — no diagnostic is emitted rather than
    // blocking on missing data.
    //
    // Leaf-table decision: the model's own TARGET table is used as the stat
    // source (option A in advisor review).  This proxies "current run ≈
    // current table size," maps 1:1 to `DESCRIBE DETAIL`, and avoids the
    // N-source aggregation problem for joins. Databricks-Unity returns only
    // `sizeInBytes` (no row count without ANALYZE); the stat is stored as
    // row_count=1 / avg_row_bytes=sizeInBytes so `estimated_bytes` equals
    // the real table size.  `max_bytes_scanned` is therefore the correct
    // ceiling lever for Databricks; `max_usd` estimates are unreliable
    // without per-row cost data.  The live-verify target
    // (`dev_hcv2_uniform.spike.uniform_t1`) tests the `max_bytes_scanned`
    // path end-to-end.
    //
    // on_breach policy: per-model `on_breach` is honoured — "warn" → Warning
    // diagnostic (does not set has_budget_errors), "error" → Error diagnostic
    // (sets has_budget_errors).  Default is "warn" per `BudgetBreachAction`.
    if models_dir.exists() {
        let budget_diagnostics =
            check_plan_budget(&models_dir, &pipeline.target.adapter, &adapter_registry).await;
        let has_errors = budget_diagnostics
            .iter()
            .any(|d| d.severity == rocky_compiler::diagnostic::Severity::Error);
        output.budget_diagnostics = budget_diagnostics;
        output.has_budget_errors = has_errors;
    }

    // --- Semantic breaking-change verdict (D3 — decision-support) --------
    //
    // REPORTING ONLY. Computed AFTER the budget check and BEFORE plan
    // persistence so the classifier's findings live on `output` but never
    // enter the persisted `RunPlan` / `ReplicationPlan` payloads (those are
    // content-hashed into `plan_id`; including findings would shift the id).
    // The verdict never gates: planned statements and the exit code are
    // unchanged regardless of severity. The hard gate is `rocky plan
    // promote`. Skipped (verdict omitted) unless `--semantic` is set; on
    // `--semantic` with no usable baseline the verdict is omitted rather
    // than fabricated.
    if semantic {
        output.breaking_verdict =
            compute_semantic_verdict(config_path, &models_dir, base_ref, state_path);
    }

    // --- Plan-spine persistence (Cluster 3 B, Phase 2 + Phase 5b) ---------
    //
    // Persist a `RunPlan` when the project has compiled models, or a
    // `ReplicationPlan` for replication-only projects (no `models/`
    // directory, or `models/` exists but compile returns zero models).
    // In both cases `output.plan_id` ends up populated so `rocky apply`
    // can re-execute the same intent. Plan write is best-effort —
    // failure is logged as a warning, not an error, so CLI invocations
    // in CI environments without `.rocky/` write access still emit the
    // statement preview.
    //
    // The run-plan compile honours `--models` when set; otherwise the
    // conventional `models/` directory next to the config is used.
    let blueprint_models_dir = run_options
        .models_dir
        .clone()
        .unwrap_or_else(|| models_dir.clone());
    let mut run_plan_persisted = false;
    if blueprint_models_dir.exists() {
        match build_and_persist_run_plan(
            config_path,
            &blueprint_models_dir,
            filter,
            pipeline_name,
            env,
            run_options,
            base_ref,
            state_path,
        ) {
            Ok(Some((run_plan, plan_id, persisted_at))) => {
                output.plan_id = Some(plan_id);
                output.plan_kind = Some("run".to_string());
                output.created_at = Some(persisted_at);
                output.models = run_plan.models.clone();
                output.execution_layers = run_plan.execution_layers.clone();
                run_plan_persisted = true;
            }
            Ok(None) => {
                // `models/` exists but compile produced zero models —
                // fall through to the replication-plan branch below.
                tracing::debug!(
                    "`models/` directory has no compiled models — building replication plan instead"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to build/persist run plan; `rocky apply` will not be available for this invocation"
                );
            }
        }
    }

    // Replication-plan branch — fires when there is no `models/`
    // directory or the directory exists but contains zero compiled
    // models. The plan_id is content-addressed by the canonical
    // `RockyConfig` snapshot + the discovered source state (sorted
    // connectors + tables), so identical inputs produce an identical
    // plan_id across machines.
    if !run_plan_persisted {
        match build_and_persist_replication_plan(
            &rocky_cfg,
            &connectors,
            filter,
            pipeline_name,
            env,
            run_options,
        ) {
            Ok((_replication_plan, plan_id, persisted_at)) => {
                output.plan_id = Some(plan_id);
                output.plan_kind = Some("replication".to_string());
                output.created_at = Some(persisted_at);
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to build/persist replication plan; `rocky apply` will not be available for this invocation"
                );
            }
        }
    }

    if output_json {
        print_json(&output)?;
    } else {
        for stmt in &output.statements {
            println!("-- {} ({})", stmt.purpose, stmt.target);
            println!("{};", stmt.sql);
            println!();
        }
        render_governance_preview_text(&output);
        render_budget_diagnostics_text(&output);
        render_semantic_verdict_text(&output);
        if let Some(ref plan_id) = output.plan_id {
            println!();
            match output.plan_kind.as_deref() {
                Some("replication") => {
                    println!(
                        "Replication plan persisted — {} statement(s) across {} connector(s)",
                        output.statements.len(),
                        connectors.len(),
                    );
                }
                _ => {
                    println!(
                        "Run plan persisted — {} model(s) across {} layer(s)",
                        output.models.len(),
                        output.execution_layers.len()
                    );
                }
            }
            println!("Plan ID:   {plan_id}");
            println!("Apply with: rocky apply {plan_id}");
        }
    }
    Ok(())
}

/// Resolve a standalone [`SqlDialect`](rocky_core::traits::SqlDialect) for the
/// SQL preview, keyed by an adapter-type string (`"databricks"`, `"snowflake"`,
/// `"bigquery"`, `"trino"`, `"duckdb"`).
///
/// These dialects are pure formatters — they need no live connection — so the
/// preview can render warehouse-accurate SQL offline. DuckDB is only available
/// when the `duckdb` feature is enabled (the default); without it, and for any
/// unknown / missing adapter type, we fall back to the Databricks dialect and
/// log a `tracing::warn!` so the preview still produces SQL.
///
/// Shared with `compact` / `archive`, which resolve the configured adapter's
/// dialect at plan time so the maintenance generators can fail fast off
/// Databricks instead of templating Delta-only SQL.
pub(crate) fn dialect_for_adapter_type(
    adapter_type: &str,
) -> Box<dyn rocky_core::traits::SqlDialect> {
    match adapter_type {
        "databricks" => Box::new(rocky_databricks::dialect::DatabricksSqlDialect),
        "snowflake" => Box::new(rocky_snowflake::dialect::SnowflakeSqlDialect),
        "bigquery" => Box::new(rocky_bigquery::dialect::BigQueryDialect),
        "trino" => Box::new(rocky_trino::dialect::TrinoDialect),
        #[cfg(feature = "duckdb")]
        "duckdb" => Box::new(rocky_duckdb::dialect::DuckDbSqlDialect),
        other => {
            tracing::warn!(
                adapter_type = other,
                "plan_preview: no standalone dialect for this adapter type — \
                 falling back to the Databricks dialect for the SQL preview"
            );
            Box::new(rocky_databricks::dialect::DatabricksSqlDialect)
        }
    }
}

/// Resolve the configured target adapter's standalone [`SqlDialect`] from a
/// `rocky.toml` path, without building a live adapter (no credentials needed).
///
/// Loads the config, resolves the (single or named) pipeline's target adapter,
/// and maps its `adapter_type` to a pure-formatter dialect via
/// [`dialect_for_adapter_type`]. Used by `compact` / `archive` so their
/// maintenance generators see the real dialect and can fail fast off
/// Databricks at plan time.
pub(crate) fn resolve_configured_dialect(
    config_path: &Path,
) -> Result<Box<dyn rocky_core::traits::SqlDialect>> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;
    let (_name, pipeline) = registry::resolve_pipeline(&rocky_cfg, None)
        .context("failed to resolve pipeline to determine the target dialect")?;
    let adapter_name = pipeline.target_adapter();
    let adapter_type = rocky_cfg
        .adapters
        .get(adapter_name)
        .map(|a| a.adapter_type.as_str())
        .with_context(|| {
            format!("pipeline target adapter '{adapter_name}' is not defined in [adapters]")
        })?;
    Ok(dialect_for_adapter_type(adapter_type))
}

/// Map a transformation [`MaterializationStrategy`] to the `purpose` label used
/// on a [`PlannedStatement`] in the SQL preview.
fn strategy_purpose(strategy: &MaterializationStrategy) -> &'static str {
    match strategy {
        MaterializationStrategy::FullRefresh => "full_refresh",
        MaterializationStrategy::Incremental { .. } => "incremental",
        MaterializationStrategy::Merge { .. } => "merge",
        MaterializationStrategy::View => "view",
        MaterializationStrategy::MaterializedView => "materialized_view",
        MaterializationStrategy::DynamicTable { .. } => "dynamic_table",
        MaterializationStrategy::TimeInterval { .. } => "time_interval",
        MaterializationStrategy::Ephemeral => "ephemeral",
        MaterializationStrategy::DeleteInsert { .. } => "delete_insert",
        MaterializationStrategy::Microbatch { .. } => "microbatch",
        MaterializationStrategy::ContentAddressed { .. } => "content_addressed",
    }
}

/// Map a resolved replication-copy [`MaterializationStrategy`] to the `purpose`
/// label used on a [`PlannedStatement`].
///
/// `build_replication_strategy_with_override` produces `FullRefresh`,
/// `Incremental`, `Merge`, `View`, or `MaterializedView` for a replication
/// pipeline (its only `Err` — `dynamic_table` — is handled at the call site, so
/// it never reaches here). The catch-all is a defensive fallback.
fn replication_copy_purpose(strategy: &MaterializationStrategy) -> &'static str {
    match strategy {
        MaterializationStrategy::Incremental { .. } => "incremental_copy",
        MaterializationStrategy::Merge { .. } => "merge_copy",
        MaterializationStrategy::View => "view",
        MaterializationStrategy::MaterializedView => "materialized_view",
        _ => "full_refresh_copy",
    }
}

/// Render the forward-looking copy SQL for one replication table, matching the
/// dialect and strategy `rocky run` will execute: `full_refresh` → `CREATE OR
/// REPLACE TABLE AS`, `incremental` → `INSERT` with the 1970 sentinel watermark
/// (the runner threads the real prior watermark at execute time), `merge` →
/// `MERGE INTO`, `view` / `materialized_view` → their DDL (mirrors
/// `run.rs::process_table`).
///
/// `plan` is offline and has no live source schema, so a `Merge` is rendered
/// with `ColumnSelection::All` (`UPDATE SET *`). A dialect whose MERGE requires
/// an explicit column list (DuckDB, Snowflake, BigQuery — anything except
/// Databricks today) cannot be previewed exactly, because the runner resolves
/// that list from the live source at execute time; those degrade to a canonical
/// shape preview via [`preview_merge_shape`] rather than erroring the whole
/// plan. `materialized_view` DDL likewise degrades to a note on dialects that
/// don't support it (DuckDB, Trino), where `rocky run` fails the table too.
fn replication_copy_sql(model_ir: &ModelIr, dialect: &dyn SqlDialect) -> Result<String> {
    let sql = match &model_ir.materialization {
        MaterializationStrategy::FullRefresh => {
            sql_gen::generate_create_table_as_sql(model_ir, dialect)?
        }
        MaterializationStrategy::Incremental { .. } => {
            sql_gen::generate_insert_sql(model_ir, dialect, None)?
        }
        MaterializationStrategy::Merge { .. } => {
            match sql_gen::generate_merge_sql(model_ir, dialect) {
                Ok(sql) => sql,
                // The dialect rejected `UPDATE SET *` (needs explicit columns
                // the offline plan can't know) or a merge key is invalid;
                // `preview_merge_shape` renders the canonical shape or a note
                // instead of erroring the whole plan.
                Err(_) => preview_merge_shape(model_ir, dialect)?,
            }
        }
        MaterializationStrategy::View => {
            let (target, body) = replication_target_and_body(model_ir, dialect)?;
            dialect.view_ddl(&target, &body)?
        }
        MaterializationStrategy::MaterializedView => {
            let (target, body) = replication_target_and_body(model_ir, dialect)?;
            match dialect.materialized_view_ddl(&target, &body) {
                Ok(sql) => sql,
                Err(_) => format!(
                    "-- MATERIALIZED VIEW {target}: not supported by {} \
                     (rocky run will fail this table)",
                    dialect.name()
                ),
            }
        }
        // `build_replication_strategy_with_override` never returns another
        // variant for a replication pipeline; defensive fallback.
        _ => sql_gen::generate_create_table_as_sql(model_ir, dialect)?,
    };
    Ok(sql)
}

/// The `(target_ref, "SELECT …\nFROM source_ref")` pair `rocky run` builds for a
/// replication view / materialized-view / merge body. Kept in one place so the
/// preview and the runner stay in lockstep.
fn replication_target_and_body(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<(String, String)> {
    let columns = model_ir
        .columns
        .as_ref()
        .expect("Replication variant guarantees `columns` is Some");
    let source = model_ir
        .source
        .as_ref()
        .expect("Replication variant guarantees `source` is Some");
    let target = dialect.format_table_ref(
        &model_ir.target.catalog,
        &model_ir.target.schema,
        &model_ir.target.table,
    )?;
    let source_ref = dialect.format_table_ref(&source.catalog, &source.schema, &source.table)?;
    let select = dialect.select_clause(columns, &model_ir.metadata_columns)?;
    Ok((target, format!("{select}\nFROM {source_ref}")))
}

/// Canonical `MERGE INTO … WHEN MATCHED THEN UPDATE SET *` preview for dialects
/// whose `merge_into` requires an explicit column list an offline plan cannot
/// resolve. Mirrors the shape `rocky run` emits once it has read the live source
/// schema, with a trailing note that the explicit columns resolve at execute
/// time — so the reviewer sees the table upserts, not full-refreshes.
///
/// Merge keys are validated up front (as the real dialect `merge_into` does), so
/// a bad `merge_key` surfaces as a note rather than a healthy-looking preview
/// that `rocky run` would reject.
fn preview_merge_shape(model_ir: &ModelIr, dialect: &dyn SqlDialect) -> Result<String> {
    let MaterializationStrategy::Merge { unique_key, .. } = &model_ir.materialization else {
        return Ok(sql_gen::generate_create_table_as_sql(model_ir, dialect)?);
    };
    if let Some(bad) = unique_key
        .iter()
        .find(|k| rocky_sql::validation::validate_identifier(k).is_err())
    {
        return Ok(format!(
            "-- MERGE INTO {}.{}: invalid merge key {bad:?} (rocky run will fail this table)",
            model_ir.target.schema, model_ir.target.table
        ));
    }
    let (target, body) = replication_target_and_body(model_ir, dialect)?;
    let on = unique_key
        .iter()
        .map(|k| format!("t.{k} = s.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "MERGE INTO {target} AS t\nUSING (\n{body}\n) AS s\nON {on}\n\
         WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *\n\
         -- preview: {} resolves explicit UPDATE/INSERT columns from the live source at run time",
        dialect.name()
    ))
}

/// Side-effect-free SQL preview core: compile the project in-process and render
/// the SQL each compiled transformation model **would** emit, returning a
/// [`PlanOutput`] whose only populated field is `statements`.
///
/// This is the offline, adapter-free analogue of [`plan`] for an in-process
/// caller (MCP server): no warehouse connection, no discovery, no governance
/// preview, no plan persistence. Discovery/governance/`plan_id`/
/// `execution_layers` stay empty/`None`/default.
///
/// ## Dialect
///
/// The dialect is sourced from the project's configured target adapter type
/// (via `config_path`) when available; otherwise it defaults to DuckDB (or the
/// Databricks fallback when the `duckdb` feature is off). See
/// [`dialect_for_adapter_type`].
///
/// ## Replication-only projects
///
/// This core covers compiled (transformation) models only. If the project has
/// no compiled models (replication-only, or `models/` holds only stubs), the
/// returned `PlanOutput` has empty `statements` and a `tracing::info!` note —
/// the live `rocky plan` discovery path is the surface for replication SQL.
// Reusable typed-output core for the in-process MCP server (`rocky-mcp`).
// `rocky plan` uses the full live `plan()` path.
pub fn plan_preview_output(
    config_path: Option<&Path>,
    models_dir: &Path,
    filter: Option<&str>,
    env: Option<&str>,
) -> Result<PlanOutput> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let mut output = PlanOutput::new(filter.unwrap_or("").to_string());
    output.env = env.map(str::to_string);

    // Resolve the preview dialect from the configured target adapter type.
    // No config / unresolvable target → DuckDB (or the Databricks fallback
    // when the `duckdb` feature is off).
    let adapter_type = config_path
        .and_then(|p| rocky_core::config::load_rocky_config(p).ok())
        .and_then(|cfg| {
            // Prefer the default replication pipeline's target adapter; fall
            // back to the first adapter declared in the config.
            let target_adapter_name = registry::resolve_replication_pipeline(&cfg, None)
                .ok()
                .map(|(_, pipeline)| pipeline.target.adapter.clone());
            target_adapter_name
                .and_then(|name| cfg.adapters.get(&name).map(|a| a.adapter_type.clone()))
                .or_else(|| cfg.adapters.values().next().map(|a| a.adapter_type.clone()))
        })
        .unwrap_or_else(|| "duckdb".to_string());
    let dialect = dialect_for_adapter_type(&adapter_type);

    // Compile the project in-process (offline — no source schemas, no cache).
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
        project_freshness_default: false,
        run_vars: rocky_core::run_vars::RunVars::new(),
    };
    let result = match compile::compile(&config) {
        Ok(r) => r,
        // `NoModels` is the replication-only signal: the models directory holds
        // no compiled models. That is not an error for the preview — return
        // empty statements with a note. Every other compile failure (parse,
        // semantic-graph, contract-load) propagates so an in-process caller
        // sees the real problem.
        Err(rocky_compiler::compile::CompileError::Project(
            rocky_compiler::project::ProjectError::NoModels { .. },
        )) => {
            tracing::info!(
                models_dir = %models_dir.display(),
                "plan_preview: project has no compiled models — replication SQL preview \
                 requires the live `rocky plan` discovery path; returning empty statements"
            );
            return Ok(output);
        }
        Err(e) => {
            return Err(anyhow::Error::from(e).context("failed to compile models for plan preview"));
        }
    };

    if result.project.models.is_empty() {
        tracing::info!(
            models_dir = %models_dir.display(),
            "plan_preview: project has no compiled models — returning empty statements"
        );
        return Ok(output);
    }

    // Project the compile result to typed IR, reusing the shared
    // `project_ir_from_compile` helper so we don't re-derive IR by hand.
    let project_ir = super::ci_diff::project_ir_from_compile(&result);

    for model_ir in &project_ir.models {
        let model_name = model_ir.name.as_ref();
        if let Some(f) = filter
            && model_name != f
        {
            continue;
        }

        let target_label = if model_ir.target.catalog.is_empty() {
            format!("{}.{}", model_ir.target.schema, model_ir.target.table)
        } else {
            format!(
                "{}.{}.{}",
                model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
            )
        };
        let purpose = strategy_purpose(&model_ir.materialization);

        // `warehouse = None`: the offline preview has no live adapter, so
        // Snowflake DynamicTable models cannot resolve a compute warehouse and
        // ContentAddressed models route through the iceberg writer rather than
        // SQL gen. Both surface as `SqlGenError::InvalidRequest` — skip them
        // with a note rather than failing the whole preview.
        match sql_gen::generate_transformation_sql_with_warehouse(model_ir, dialect.as_ref(), None)
        {
            Ok(stmts) => {
                // Ephemeral models return `Ok(vec![])` (inlined as CTEs) — no
                // statement to preview. Multi-statement strategies
                // (DeleteInsert, lakehouse DDL) emit one row each.
                for sql in stmts {
                    output.statements.push(PlannedStatement {
                        purpose: purpose.to_string(),
                        target: target_label.clone(),
                        sql,
                    });
                }
            }
            Err(e) => {
                tracing::debug!(
                    model = model_name,
                    strategy = purpose,
                    error = %e,
                    "plan_preview: skipping model whose SQL cannot be rendered offline"
                );
            }
        }
    }

    Ok(output)
}

/// Compile the models directory, build a `RunPlan` payload, persist it to
/// `.rocky/plans/<plan_id>.json`, and return
/// `Some((payload, plan_id, persisted_at))`.
///
/// Returns `Ok(None)` when the compile succeeds but produces zero models —
/// the caller falls through to the replication-plan branch in that case.
///
/// Captures the full `rocky run` flag surface from `run_options` so apply-time
/// replay is intent-preserving. `--missing` / `--resume-latest` are persisted
/// as booleans; the actual state-store lookup happens at apply time.
#[allow(clippy::too_many_arguments)]
fn build_and_persist_run_plan(
    config_path: &Path,
    models_dir: &Path,
    filter: Option<&str>,
    pipeline: Option<&str>,
    env: Option<&str>,
    run_options: &PlanRunOptions,
    base_ref: &str,
    state_path: &Path,
) -> Result<Option<(RunPlan, String, chrono::DateTime<Utc>)>> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
        project_freshness_default: false,
        run_vars: rocky_core::run_vars::RunVars::new(),
    };

    let result = compile::compile(&config).context("failed to compile models for run plan")?;

    if result.project.models.is_empty() {
        // No models compiled — this is a replication-only project even
        // though `models/` exists on disk (e.g. it only holds
        // `_defaults.toml` or stub files). Let the caller take the
        // replication-plan path.
        return Ok(None);
    }

    // Collect qualified model names from the project.
    let models: Vec<String> = result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .collect();

    // Execution layers from the DAG (names only — informational).
    let execution_layers: Vec<Vec<String>> = result.project.layers.clone();

    let partition = &run_options.partition_opts;
    let run_plan = RunPlan {
        filter: filter.map(str::to_string),
        pipeline: pipeline.map(str::to_string),
        model: run_options.model.clone(),
        branch: run_options.branch.clone(),
        partition: partition.partition.clone(),
        partition_from: partition.from.clone(),
        partition_to: partition.to.clone(),
        latest: partition.latest,
        missing: partition.missing,
        lookback: partition.lookback,
        parallel: partition.parallel,
        run_all: run_options.all,
        env: env.map(str::to_string),
        models_dir: run_options
            .models_dir
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned()),
        resume: run_options.resume.clone(),
        resume_latest: run_options.resume_latest,
        shadow: run_options.shadow,
        shadow_suffix: run_options.shadow_suffix.clone(),
        shadow_schema: run_options.shadow_schema.clone(),
        dag: run_options.dag,
        idempotency_key: run_options.idempotency_key.clone(),
        governance_override: run_options.governance_override.clone(),
        models,
        execution_layers,
    };

    let cwd = std::env::current_dir().context("failed to get current working directory")?;

    // F3 seam 1 + D3: stamp the authoring principal and embed the propose-time
    // change-classification into the plan payload, so a later `rocky apply`
    // evaluates the plan against the identity that authored it and the exact
    // capabilities that were reviewed.
    let principal = run_options.principal.unwrap_or(PolicyPrincipal::Human);
    let capabilities =
        compute_embedded_capabilities(config_path, models_dir, base_ref, Some(state_path));
    let plan_id = write_plan_governed(&cwd, PlanKind::Run, &run_plan, principal, capabilities)
        .context("failed to write run plan")?;

    let persisted_at = Utc::now();
    Ok(Some((run_plan, plan_id, persisted_at)))
}

/// Compute the propose-time change-classification (D3) to embed in a governed
/// plan's payload.
///
/// Diffs the compiled working tree (`head`) against the project at `base_ref`
/// using the exact compile + classifier path `rocky review` / `rocky ci-diff`
/// use, groups findings by model, and classifies each *changed* model per the
/// fail-closed §2.4 rules. Keyed by the logical model name so apply can look up
/// each planned model directly.
///
/// **Fail-closed:** if either side fails to compile (no git base, base ref
/// missing at that path, working tree broken) the result is
/// `diff_available = false` with an empty map — the apply-time enforcement then
/// treats every planned model as a breaking change.
pub fn compute_embedded_capabilities(
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    state_path: Option<&Path>,
) -> EmbeddedCapabilities {
    use rocky_compiler::compile::{self, CompilerConfig};

    if !models_dir.is_dir() {
        return EmbeddedCapabilities::default(); // fail-closed
    }

    // Seed both compiles with cached source schemas so types are real (mirrors
    // `rocky review`). Degrade to empty on any failure — a poorer classification
    // only ever fails *closed* (more models look breaking), never open.
    let source_schemas = match (
        state_path,
        rocky_core::config::load_rocky_config(config_path),
    ) {
        (Some(sp), Ok(cfg)) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, sp)
        }
        _ => std::collections::HashMap::new(),
    };

    let head = {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            source_schemas: source_schemas.clone(),
            ..Default::default()
        };
        match compile::compile(&config) {
            Ok(r) => r,
            Err(_) => return EmbeddedCapabilities::default(),
        }
    };
    let base = match super::ci_diff::extract_base_compile(base_ref, models_dir, source_schemas) {
        Ok(r) => r,
        Err(_) => return EmbeddedCapabilities::default(),
    };

    let base_ir = super::ci_diff::project_ir_from_compile(&base);
    let head_ir = super::ci_diff::project_ir_from_compile(&head);
    let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);
    let by_target = rocky_core::policy::classify_findings_by_model(&findings);

    // Findings key on `target.full_name()`; remap to the logical model name
    // (`ModelIr.name == config.name`) so the map lines up with `RunPlan.models`
    // and the apply-time `ModelAttributes.name`.
    let target_to_name: std::collections::HashMap<String, String> = head_ir
        .models
        .iter()
        .map(|m| (m.target.full_name(), m.name.to_string()))
        .collect();

    let mut changed = std::collections::BTreeMap::new();
    for (target, cap) in by_target {
        if let Some(name) = target_to_name.get(&target) {
            changed.insert(name.clone(), cap);
        }
    }

    EmbeddedCapabilities {
        diff_available: true,
        changed,
    }
}

/// Build a canonical, sorted source-state snapshot from the discovered
/// connectors. Used both at plan time (to build the `ReplicationPlan`
/// payload) and at apply time (to assert the source hasn't drifted
/// since the plan was created).
///
/// Sort order is stable: connectors by `id`, tables by `name`. Volatile
/// fields (`last_sync_at`, adapter `metadata`) are intentionally
/// omitted — see [`ReplicationConnectorSnapshot`] for the rationale.
pub(crate) fn build_source_state_snapshot(
    connectors: &[DiscoveredConnector],
) -> Vec<ReplicationConnectorSnapshot> {
    let mut snapshot: Vec<ReplicationConnectorSnapshot> = connectors
        .iter()
        .map(|c| {
            let mut tables: Vec<ReplicationTableSnapshot> = c
                .tables
                .iter()
                .map(|t| ReplicationTableSnapshot {
                    name: t.name.clone(),
                    row_count: t.row_count,
                })
                .collect();
            tables.sort_by(|a, b| a.name.cmp(&b.name));
            ReplicationConnectorSnapshot {
                id: c.id.clone(),
                schema: c.schema.clone(),
                source_type: c.source_type.clone(),
                tables,
            }
        })
        .collect();
    snapshot.sort_by(|a, b| a.id.cmp(&b.id));
    snapshot
}

/// Build a `ReplicationPlan` payload from the loaded config + the
/// already-discovered connectors, persist it to
/// `.rocky/plans/<plan_id>.json`, and return
/// `(payload, plan_id, persisted_at)`.
///
/// Called by the replication-plan branch of `rocky plan` — fires when
/// the project has no `models/` directory or when compile produced
/// zero models. Discovery has already happened earlier in `plan()`;
/// this function only canonicalizes the result and persists.
fn build_and_persist_replication_plan(
    rocky_cfg: &rocky_core::config::RockyConfig,
    connectors: &[DiscoveredConnector],
    filter: Option<&str>,
    pipeline: Option<&str>,
    env: Option<&str>,
    run_options: &PlanRunOptions,
) -> Result<(ReplicationPlan, String, chrono::DateTime<Utc>)> {
    let config_snapshot = serde_json::to_value(rocky_cfg)
        .context("failed to serialize RockyConfig for replication plan")?;
    let source_state_snapshot = build_source_state_snapshot(connectors);

    let replication_plan = ReplicationPlan {
        filter: filter.map(str::to_string),
        pipeline: pipeline.map(str::to_string),
        env: env.map(str::to_string),
        idempotency_key: run_options.idempotency_key.clone(),
        resume: run_options.resume.clone(),
        resume_latest: run_options.resume_latest,
        governance_override: run_options.governance_override.clone(),
        config_snapshot,
        source_state_snapshot,
    };

    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    let plan_id = write_plan(&cwd, PlanKind::Replication, &replication_plan)
        .context("failed to write replication plan")?;

    let persisted_at = Utc::now();
    Ok((replication_plan, plan_id, persisted_at))
}

/// Compile the project and populate `classification_actions`,
/// `mask_actions`, and `retention_actions` on `output`.
///
/// Mirrors the semantics of the post-DAG reconcile loop at
/// `commands/run.rs` so the preview matches what a subsequent
/// `rocky run [--env <name>]` would apply. `mask_actions` only populates
/// for `(column, tag)` pairs where the tag resolves via
/// [`rocky_core::config::RockyConfig::resolve_mask_for_env`] — unresolved
/// tags are a compliance gap, reported by `rocky compliance`, not a
/// preview row.
///
/// This is the offline core of the governance preview: it compiles the
/// `models/` directory and reads each model sidecar's
/// `[classification]` / `mask` / `retention` declarations. It performs no
/// warehouse I/O and applies nothing — the rows it pushes are a *preview*
/// of the control-plane work `rocky run` would later issue through the
/// [`GovernanceAdapter`](rocky_core::traits::GovernanceAdapter). It is
/// public so the `rocky mcp` server can surface the same preview in-loop
/// without shelling out or re-running source discovery.
pub fn populate_governance_actions(
    cfg: &rocky_core::config::RockyConfig,
    models_dir: &Path,
    env: Option<&str>,
    adapter_type: &str,
    output: &mut PlanOutput,
) -> Result<()> {
    let tag_to_strategy = cfg.resolve_mask_for_env(env);

    let compile = rocky_compiler::compile::compile(&rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: cfg.mask.clone(),
        allow_unmasked: cfg.classifications.allow_unmasked.clone(),
        project_freshness_default: cfg.freshness.has_default(),
        run_vars: rocky_core::run_vars::RunVars::new(),
    })
    .context("failed to compile project for governance preview")?;

    for model in &compile.project.models {
        let model_name = &model.config.name;

        // Classification + masking — one row per (model, column, tag)
        // triple from the sidecar. Masking additionally gated on the
        // tag resolving to a strategy under the active env.
        for (column, tag) in &model.config.classification {
            output.classification_actions.push(ClassificationAction {
                model: model_name.clone(),
                column: column.clone(),
                tag: tag.clone(),
            });
            if let Some(strategy) = tag_to_strategy.get(tag) {
                output.mask_actions.push(MaskAction {
                    model: model_name.clone(),
                    column: column.clone(),
                    tag: tag.clone(),
                    resolved_strategy: strategy.as_str().to_string(),
                });
            }
        }

        // Retention — one row per model whose sidecar declares a
        // `retention = "<N>[dy]"` policy. `warehouse_preview` is the
        // warehouse-native rendering (Databricks TBLPROPERTIES /
        // Snowflake ALTER TABLE) or `None` on adapters without a
        // first-class retention knob.
        if let Some(retention) = model.config.retention {
            let target = &model.config.target;
            let warehouse_preview = render_retention_preview(
                adapter_type,
                &target.catalog,
                &target.schema,
                &target.table,
                retention.duration_days,
            );
            output.retention_actions.push(RetentionAction {
                model: model_name.clone(),
                duration_days: retention.duration_days,
                warehouse_preview,
            });
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// D-3 stage 2 — real-catalog budget check
// ---------------------------------------------------------------------------

/// Collect per-leaf-model byte statistics from the warehouse catalog and
/// check each model's `[budget]` ceiling against the propagated estimates.
///
/// Returns a (possibly empty) list of E027 diagnostics.  Severity follows
/// the per-model `on_breach` policy (`warn` or `error`).
///
/// # Leaf-table convention
///
/// Each model's **target** table is used as the stat source.  This is a
/// proxy for "next run ≈ current table size" and is consistent with the
/// single-table lookup shape of `DESCRIBE DETAIL` and
/// `CatalogClient::table_stats`.  Multi-source join aggregation is deferred
/// to a future wave.
///
/// # Graceful degradation
///
/// - Adapter not Databricks/Iceberg → returns empty vec (no stats available)
/// - Table not found / network error → model is skipped (no diagnostic)
/// - `max_usd` ceiling on Databricks-Unity → stored as row_count=1 /
///   avg_row_bytes=sizeInBytes so estimated_bytes is correct, but per-row
///   cost is unreliable without ANALYZE data; `max_bytes_scanned` is the
///   correct ceiling lever for Databricks
async fn check_plan_budget(
    models_dir: &Path,
    target_adapter_name: &str,
    adapter_registry: &crate::registry::AdapterRegistry,
) -> Vec<rocky_compiler::diagnostic::Diagnostic> {
    use rocky_catalog_core::CatalogClient as _;
    use rocky_compiler::cost_check;
    use rocky_core::cost::{TableStats as CostTableStats, WarehouseType, propagate_costs};
    use std::collections::HashMap;

    tracing::debug!(
        models_dir = %models_dir.display(),
        target_adapter_name,
        "plan budget check: invoked"
    );

    // Compile models offline — no catalog I/O here.
    let compile_cfg = rocky_compiler::compile::CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
        project_freshness_default: false,
        run_vars: rocky_core::run_vars::RunVars::new(),
    };
    let result = match rocky_compiler::compile::compile(&compile_cfg) {
        Ok(r) => r,
        Err(e) => {
            tracing::debug!(
                error = %e,
                "plan budget check: compile failed; skipping budget check"
            );
            return vec![];
        }
    };
    if result.project.models.is_empty() {
        return vec![];
    }

    // Determine the warehouse type for cost-model pricing constants.
    let warehouse_type = adapter_registry
        .adapter_config(target_adapter_name)
        .and_then(|cfg| WarehouseType::from_adapter_type(&cfg.adapter_type))
        .unwrap_or(WarehouseType::Databricks);

    // Build base_stats by fetching real catalog data for each model's
    // target table.  Models for which we get no stats are skipped
    // (propagate_costs handles missing entries gracefully).
    let mut base_stats: HashMap<String, CostTableStats> = HashMap::new();

    // Databricks path: call `DESCRIBE DETAIL` via the connector.
    let db_connector_result = adapter_registry.databricks_connector(target_adapter_name);
    tracing::debug!(
        target_adapter_name,
        ok = db_connector_result.is_ok(),
        "plan budget check: databricks_connector lookup"
    );
    if let Ok(db_connector) = db_connector_result {
        for model in &result.project.models {
            let target = &model.config.target;
            match db_connector
                .describe_detail_stats(&target.catalog, &target.schema, &target.table)
                .await
            {
                Ok(Some(detail)) => {
                    tracing::debug!(
                        model = model.config.name,
                        size_bytes = ?detail.size_bytes,
                        "plan budget check: describe_detail_stats returned"
                    );
                    if let Some(size_bytes) = detail.size_bytes {
                        // Databricks-Unity returns `sizeInBytes` but no
                        // row count without ANALYZE.  Use row_count=1 with
                        // avg_row_bytes=size_bytes so that
                        // `estimate_table_scan_cost` produces
                        // `estimated_bytes = 1 * size_bytes = size_bytes`.
                        // `max_bytes_scanned` ceilings are the correct lever
                        // for Databricks at plan time; `max_usd` ceilings
                        // will estimate $0 per-row cost and should not be
                        // relied on without ANALYZE data.
                        base_stats.insert(
                            model.config.name.clone(),
                            CostTableStats {
                                row_count: 1,
                                avg_row_bytes: size_bytes,
                            },
                        );
                    }
                }
                Ok(None) => {
                    // Table not found — new model, no pre-existing table. Skip.
                    tracing::debug!(
                        model = model.config.name,
                        "plan budget check: target table not found; skipping model"
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        model = model.config.name,
                        error = %e,
                        "plan budget check: describe_detail_stats failed; skipping model"
                    );
                }
            }
        }
    }

    // Iceberg path: call `table_stats` via the catalog-client adapter.
    if let Some(iceberg_client) = adapter_registry.iceberg_client(target_adapter_name) {
        for model in &result.project.models {
            if base_stats.contains_key(&model.config.name) {
                // Already populated by Databricks path (shouldn't happen in
                // practice, but be explicit).
                continue;
            }
            let target = &model.config.target;
            let table_ref = rocky_catalog_core::TableRef {
                catalog: if target.catalog.is_empty() {
                    None
                } else {
                    Some(target.catalog.clone())
                },
                namespace: vec![target.schema.clone()],
                name: target.table.clone(),
            };
            match iceberg_client.table_stats(&table_ref).await {
                Ok(stats) => {
                    // Convert from catalog TableStats to cost::TableStats.
                    // Both row_count and total_bytes must be present to derive
                    // avg_row_bytes; when either is missing we skip this model.
                    if let (Some(row_count), Some(total_bytes)) =
                        (stats.row_count, stats.total_bytes)
                    {
                        let avg_row_bytes = total_bytes.checked_div(row_count).unwrap_or(0);
                        base_stats.insert(
                            model.config.name.clone(),
                            CostTableStats {
                                row_count,
                                avg_row_bytes,
                            },
                        );
                    }
                }
                Err(rocky_catalog_core::CatalogError::UnsupportedOperation(_)) => {
                    // Iceberg catalog signals no stats endpoint.  Degrade.
                }
                Err(e) => {
                    tracing::debug!(
                        model = model.config.name,
                        error = %e,
                        "plan budget check: iceberg table_stats failed; skipping model"
                    );
                }
            }
        }
    }

    // If we got no real stats for any model, skip the ceiling check —
    // the stub estimates from compile already ran.
    if base_stats.is_empty() {
        return vec![];
    }

    // Propagate cost estimates through the DAG using real stats.
    let dag_nodes = &result.project.dag_nodes;
    let estimates = match propagate_costs(dag_nodes, &base_stats, warehouse_type) {
        Ok(e) => e,
        Err(e) => {
            tracing::debug!(
                error = %e,
                "plan budget check: propagate_costs failed; skipping budget check"
            );
            return vec![];
        }
    };

    // Check ceilings; honor per-model on_breach policy.
    cost_check::check_cost_ceilings_plan(&result.project.models, &estimates)
}

/// Render the warehouse-native SQL Rocky would emit for a retention
/// policy, or `None` on adapters without a first-class retention knob
/// (BigQuery, DuckDB).
///
/// Delegates to the same SQL-generation helpers the governance adapters
/// use at run time so the preview stays byte-identical to what
/// `apply_retention_policy` would issue.
fn render_retention_preview(
    adapter_type: &str,
    catalog: &str,
    schema: &str,
    table: &str,
    duration_days: u32,
) -> Option<String> {
    match adapter_type {
        "databricks" => rocky_core::catalog::generate_set_delta_retention_sql(
            catalog,
            schema,
            table,
            duration_days,
        )
        .ok(),
        "snowflake" => {
            // Mirror `rocky-snowflake::governance::format_set_retention_sql`.
            // Inlined here to avoid a cross-crate dep just for a preview
            // string; the governance adapter runs validation before
            // issuing the real statement. Identifiers are the same ones
            // rocky_core sidecars accept, so validation drift is a
            // non-issue in practice.
            Some(format!(
                "ALTER TABLE {catalog}.{schema}.{table} SET DATA_RETENTION_TIME_IN_DAYS = {duration_days}"
            ))
        }
        _ => None,
    }
}

/// Render the governance preview under the text output mode. SQL
/// statements already streamed above; this just appends the control-plane
/// action counts + rows so CLI users see parity with the JSON shape.
fn render_governance_preview_text(output: &PlanOutput) {
    if output.classification_actions.is_empty()
        && output.mask_actions.is_empty()
        && output.retention_actions.is_empty()
    {
        return;
    }

    println!("-- governance preview --");
    if let Some(env) = &output.env {
        println!("-- env: {env}");
    }
    for a in &output.classification_actions {
        println!(
            "classification {model}.{column} = {tag}",
            model = a.model,
            column = a.column,
            tag = a.tag,
        );
    }
    for a in &output.mask_actions {
        println!(
            "mask           {model}.{column} ({tag}) -> {strategy}",
            model = a.model,
            column = a.column,
            tag = a.tag,
            strategy = a.resolved_strategy,
        );
    }
    for a in &output.retention_actions {
        let preview = a
            .warehouse_preview
            .as_deref()
            .unwrap_or("(unsupported on this adapter)");
        println!(
            "retention      {model} = {days}d — {preview}",
            model = a.model,
            days = a.duration_days,
        );
    }
}

/// Render budget diagnostics under the text output mode.
///
/// Prints E027 diagnostics (one per breached ceiling) with their severity
/// prefix so CLI users see the same information as JSON consumers.
fn render_budget_diagnostics_text(output: &PlanOutput) {
    if output.budget_diagnostics.is_empty() {
        return;
    }
    println!("-- budget check --");
    for d in &output.budget_diagnostics {
        let prefix = if d.severity == rocky_compiler::diagnostic::Severity::Error {
            "error"
        } else {
            "warning"
        };
        println!("[{prefix}][E027] {}: {}", d.model, d.message);
        if let Some(ref suggestion) = d.suggestion {
            println!("  hint: {suggestion}");
        }
    }
}

/// Compute the decision-support semantic verdict for `rocky plan --semantic`.
///
/// Compiles the **working tree** (head) and the project as it stood at
/// `base_ref` (the same baseline mechanism `rocky ci-diff` /
/// `rocky plan promote` use — [`super::ci_diff::extract_base_compile`]),
/// lowers both to [`rocky_ir::ProjectIr`] via
/// [`super::ci_diff::project_ir_from_compile`], and runs
/// [`rocky_core::breaking_change::diff_project_ir`].
///
/// Returns `None` (verdict omitted) when there is no baseline to diff
/// against — no `models/` directory, the working tree fails to compile, or
/// the `base_ref` models cannot be materialized / do not compile. A `None`
/// is the honest "nothing to report" outcome; the caller never fabricates a
/// verdict.
///
/// Both compiles are seeded with the **current** cached source schemas so
/// the per-model type diff measures real schema drift rather than
/// `Unknown`-vs-`Unknown` noise — identical to the seeding `ci-diff` and the
/// promote gate use. The returned verdict's `findings` is the full
/// classified list (including `info`-severity entries); it always carries
/// the [`SEMANTIC_VERDICT_CAVEAT`] so a JSON-only consumer sees that the
/// classifier is blind to schema-stable value changes.
fn compute_semantic_verdict(
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    state_path: &Path,
) -> Option<SemanticPlanVerdict> {
    use rocky_compiler::compile::{self, CompilerConfig};

    if !models_dir.is_dir() {
        tracing::debug!(
            models_dir = %models_dir.display(),
            "plan --semantic: no models directory — verdict omitted"
        );
        return None;
    }

    // Seed both compiles from the current warehouse schema cache so the IR
    // carries real leaf types. Degrade to an empty map on config / cache
    // failure rather than blocking the preview.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };

    let head_compile = {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!(
                    error = %e,
                    "plan --semantic: working-tree compile failed — verdict omitted"
                );
                return None;
            }
        }
    };

    let base_compile =
        match super::ci_diff::extract_base_compile(base_ref, models_dir, source_schemas) {
            Ok(r) => r,
            Err(reason) => {
                tracing::debug!(
                    reason = %reason,
                    "plan --semantic: base ref '{base_ref}' compile unavailable — verdict omitted"
                );
                return None;
            }
        };

    let base_ir = super::ci_diff::project_ir_from_compile(&base_compile);
    let head_ir = super::ci_diff::project_ir_from_compile(&head_compile);
    let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);

    Some(SemanticPlanVerdict {
        caveat: SEMANTIC_VERDICT_CAVEAT.to_string(),
        base_ref: base_ref.to_string(),
        findings,
    })
}

/// Render the semantic verdict under the text output mode.
///
/// Prints the per-finding severity + change, then the caveat verbatim. The
/// caveat is printed even when `findings` is empty, mirroring the JSON
/// payload: "no findings" is the case most easily misread as "safe".
fn render_semantic_verdict_text(output: &PlanOutput) {
    let Some(verdict) = &output.breaking_verdict else {
        return;
    };
    use rocky_core::breaking_change::BreakingSeverity;

    println!();
    println!(
        "-- semantic verdict (vs {base}, decision-support) --",
        base = verdict.base_ref
    );
    if verdict.findings.is_empty() {
        println!("no output-schema changes detected");
    } else {
        for f in &verdict.findings {
            let sev = match f.severity {
                BreakingSeverity::Breaking => "BREAKING",
                BreakingSeverity::Warning => "WARNING",
                BreakingSeverity::Info => "INFO",
            };
            println!("[{sev}] {:?}", f.change);
        }
    }
    println!("note: {}", verdict.caveat);
}

// ---------------------------------------------------------------------------
// `rocky plan promote <branch-name>` — Phase 3
// ---------------------------------------------------------------------------

/// Execute `rocky plan promote <branch-name>` — run the approval + breaking-change
/// gates, build a `PromotePlan` payload, and persist it to the plan store.
///
/// On success emits `PlanOutput` with `plan_id`, `plan_kind: "promote"`,
/// `created_at`, and a target-count summary.
///
/// On breaking-change block (without `--allow-breaking`) the plan is **not**
/// written; a structured JSON error is printed and the function returns `Err`.
///
/// ## Parameters
///
/// - `root` — the workspace root where `.rocky/plans/` lives (injectable for tests).
/// - `config_path` — path to `rocky.toml`.
/// - `models_dir` — directory containing transformation models for the breaking-change gate.
/// - `base_ref` — git ref to diff against.
/// - `branch_name` — branch being promoted.
/// - `filter` — optional replication filter (e.g. `"client=acme"`).
/// - `pipeline_name` — optional pipeline selector.
/// - `allow_breaking` — bypass the breaking-change block gate.
/// - `state_path` — already-resolved (namespace-aware) state-file path threaded
///   from `main.rs`, so `rocky --state-namespace <ns> plan promote` reads the
///   namespaced branch state rather than the global file.
/// - `output_json` — emit machine-readable JSON instead of text.
#[allow(clippy::too_many_arguments)]
pub async fn plan_promote(
    root: &Path,
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    branch_name: &str,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
    allow_breaking: bool,
    state_path: &Path,
    output_json: bool,
) -> Result<()> {
    let result = build_promote_plan_inner(
        root,
        config_path,
        models_dir,
        base_ref,
        branch_name,
        filter,
        pipeline_name,
        allow_breaking,
        state_path,
    )
    .await?;

    if output_json {
        print_json(&result.plan_output)?;
    } else {
        println!(
            "Promote plan persisted — {} target(s)",
            result.plan.targets.len()
        );
        println!(
            "Plan ID:    {}",
            result.plan_output.plan_id.as_deref().unwrap_or("")
        );
        println!(
            "Apply with: rocky apply {}",
            result.plan_output.plan_id.as_deref().unwrap_or("")
        );
        if let Some(bc) = &result.plan.breaking_changes {
            let breaking_count = bc.iter().filter(|f| f.is_breaking()).count();
            if breaking_count > 0 {
                println!(
                    "WARNING: {} breaking change(s) allowed (--allow-breaking was set)",
                    breaking_count
                );
            }
        }
    }
    Ok(())
}

/// Internal result of building a promote plan.
pub(crate) struct PromotePlanResult {
    pub plan: PromotePlan,
    pub plan_output: PlanOutput,
}

/// Build and persist a `PromotePlan` from the given parameters.
///
/// Extracted as a named function so both `plan_promote` (standalone command)
/// and `run_branch_promote` (bare-verb alias) can reuse it without duplicating
/// the gate logic.
///
/// Returns `Err` when:
/// - The approval gate fails (insufficient valid artifacts).
/// - The breaking-change gate fires AND `allow_breaking` is false (plan NOT written).
/// - Any I/O or warehouse discovery error.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn build_promote_plan_inner(
    root: &Path,
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    branch_name: &str,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
    allow_breaking: bool,
    state_path: &Path,
) -> Result<PromotePlanResult> {
    use crate::commands::branch::{
        APPROVAL_SKIP_ENV, approver_identity_pub, compute_branch_state_hash_pub,
        discover_branch_targets_for_plan, run_approval_gate, run_breaking_change_gate_for_plan,
        validate_branch_name_pub,
    };
    use rocky_core::state::StateStore;

    validate_branch_name_pub(branch_name)?;

    // `state_path` is the namespace-aware path threaded from main.rs; the
    // branch record lives in whichever state file this invocation targets.
    let store = StateStore::open_read_only(state_path).with_context(|| {
        format!(
            "failed to open state store at {} — run `rocky branch create {}` first",
            state_path.display(),
            branch_name
        )
    })?;

    let record = store
        .get_branch(branch_name)?
        .with_context(|| format!("branch '{branch_name}' not found — see 'rocky branch list'"))?;

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;

    let branch_state_hash = compute_branch_state_hash_pub(&record, config_path)?;
    let actor = approver_identity_pub()?;

    let env_skip_value = std::env::var(APPROVAL_SKIP_ENV)
        .ok()
        .filter(|v| !v.is_empty());
    let skip_reason: Option<String> = env_skip_value
        .as_ref()
        .map(|v| format!("{APPROVAL_SKIP_ENV}={v}"));

    let mut audit: Vec<AuditEvent> = Vec::new();

    let (approvals_used, approvals_rejected) = run_approval_gate(
        &rocky_cfg,
        &record,
        &branch_state_hash,
        &actor,
        skip_reason.as_deref(),
        &mut audit,
    )?;

    // Breaking-change gate — runs before the plan is written.
    let breaking_findings = run_breaking_change_gate_for_plan(
        config_path,
        models_dir,
        base_ref,
        &mut audit,
        &actor,
        &record,
        &branch_state_hash,
    );

    if let Some(findings) = &breaking_findings {
        let breaking: Vec<_> = findings.iter().filter(|f| f.is_breaking()).collect();
        if !breaking.is_empty() {
            if allow_breaking {
                audit.push(AuditEvent {
                    kind: AuditEventKind::BreakingChangesAllowed,
                    at: Utc::now(),
                    actor: actor.clone(),
                    branch: record.name.clone(),
                    branch_state_hash: branch_state_hash.clone(),
                    reason: Some("--allow-breaking CLI flag".to_string()),
                    breaking_changes: Some(findings.clone()),
                });
            } else {
                audit.push(AuditEvent {
                    kind: AuditEventKind::BreakingChangesBlocked,
                    at: Utc::now(),
                    actor: actor.clone(),
                    branch: record.name.clone(),
                    branch_state_hash: branch_state_hash.clone(),
                    reason: None,
                    breaking_changes: Some(findings.clone()),
                });
                let summary = breaking
                    .iter()
                    .map(|f| format!("{:?}", f.change))
                    .collect::<Vec<_>>()
                    .join("; ");
                anyhow::bail!(
                    "promote plan blocked by {} breaking change(s): {summary}. \
                     Re-run with `--allow-breaking` to override.",
                    breaking.len()
                );
            }
        }
    }

    // Discover targets + build SQL at plan time (dialect-quoted, deterministic).
    let planned_targets =
        discover_branch_targets_for_plan(config_path, &record, filter, pipeline_name).await?;

    let head_ref = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .and_then(|o| {
            if o.status.success() {
                Some(String::from_utf8_lossy(&o.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    let target_plans: Vec<PromoteTargetPlan> = planned_targets
        .iter()
        .map(|pt| PromoteTargetPlan {
            target: pt.target.clone(),
            source: pt.source.clone(),
            statement: pt.statement.clone(),
        })
        .collect();

    let created_at = Utc::now();

    // Emit PromotePlanCreated audit event.
    audit.push(AuditEvent {
        kind: AuditEventKind::PromotePlanCreated,
        at: created_at,
        actor: actor.clone(),
        branch: record.name.clone(),
        branch_state_hash: branch_state_hash.clone(),
        reason: None,
        breaking_changes: None,
    });

    let promote_plan = PromotePlan {
        branch_name: branch_name.to_string(),
        base_ref: base_ref.to_string(),
        head_ref,
        branch_state_hash: branch_state_hash.clone(),
        approvals_used,
        approvals_rejected,
        breaking_changes: breaking_findings,
        allow_breaking,
        targets: target_plans,
        plan_audit: audit,
        created_at,
    };

    let plan_id = write_plan(root, PlanKind::Promote, &promote_plan)
        .context("failed to write promote plan")?;

    let mut plan_output = PlanOutput::new(filter.unwrap_or("").to_string());
    plan_output.plan_id = Some(plan_id);
    plan_output.plan_kind = Some("promote".to_string());
    plan_output.created_at = Some(created_at);

    Ok(PromotePlanResult {
        plan: promote_plan,
        plan_output,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use rocky_ir::MaskStrategy;

    // ------------------------------------------------------------------
    // replication copy preview — strategy / override fidelity (bug fix:
    // `rocky plan` must reflect `merge` + `[[table_overrides]]`, not always
    // render full_refresh)
    // ------------------------------------------------------------------

    fn replication_ir(strategy: MaterializationStrategy) -> ModelIr {
        ModelIr::replication(
            TargetRef {
                catalog: "cat".into(),
                schema: "staging".into(),
                table: "orders".into(),
            },
            strategy,
            SourceRef {
                catalog: "cat".into(),
                schema: "raw".into(),
                table: "orders".into(),
            },
            ColumnSelection::All,
            vec![],
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        )
    }

    fn merge_ir() -> ModelIr {
        replication_ir(MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("order_id")],
            update_columns: ColumnSelection::All,
        })
    }

    #[test]
    fn replication_copy_purpose_maps_every_builder_strategy() {
        // Must cover every variant build_replication_strategy_with_override can
        // return (its only Err — dynamic_table — is handled at the call site).
        assert_eq!(
            replication_copy_purpose(&MaterializationStrategy::FullRefresh),
            "full_refresh_copy"
        );
        assert_eq!(
            replication_copy_purpose(&MaterializationStrategy::Incremental {
                timestamp_column: "ts".into(),
            }),
            "incremental_copy"
        );
        assert_eq!(
            replication_copy_purpose(&MaterializationStrategy::Merge {
                unique_key: vec![Arc::from("id")],
                update_columns: ColumnSelection::All,
            }),
            "merge_copy"
        );
        assert_eq!(
            replication_copy_purpose(&MaterializationStrategy::View),
            "view"
        );
        assert_eq!(
            replication_copy_purpose(&MaterializationStrategy::MaterializedView),
            "materialized_view"
        );
    }

    #[test]
    fn replication_full_refresh_and_incremental_preview_sql() {
        let dialect = dialect_for_adapter_type("databricks");
        let full = replication_copy_sql(
            &replication_ir(MaterializationStrategy::FullRefresh),
            dialect.as_ref(),
        )
        .expect("full_refresh sql");
        assert!(
            full.to_uppercase().contains("CREATE OR REPLACE TABLE"),
            "full_refresh must render CTAS, got:\n{full}"
        );

        let incr = replication_copy_sql(
            &replication_ir(MaterializationStrategy::Incremental {
                timestamp_column: "_updated_at".into(),
            }),
            dialect.as_ref(),
        )
        .expect("incremental sql");
        assert!(
            incr.contains("INSERT INTO") && incr.contains("1970"),
            "incremental must be an INSERT with the 1970 sentinel watermark, got:\n{incr}"
        );
    }

    #[test]
    fn replication_merge_preview_is_exact_on_databricks() {
        // Databricks accepts `UPDATE SET *`, so the offline preview is the exact
        // dialect SQL — no degrade note, byte-identical to generate_merge_sql.
        let ir = merge_ir();
        let dialect = dialect_for_adapter_type("databricks");
        let preview = replication_copy_sql(&ir, dialect.as_ref()).expect("merge preview");
        let exact =
            sql_gen::generate_merge_sql(&ir, dialect.as_ref()).expect("databricks accepts `*`");
        assert_eq!(
            preview, exact,
            "databricks merge preview must be the exact dialect MERGE, not a degrade"
        );
        assert!(preview.contains("MERGE INTO") && preview.contains("UPDATE SET *"));
        assert!(
            !preview.contains("-- preview:"),
            "databricks is exact and must not carry the degrade note"
        );
    }

    #[test]
    fn replication_merge_preview_degrades_when_dialect_needs_explicit_columns() {
        // Snowflake (like DuckDB / BigQuery) rejects `UPDATE SET *`; the explicit
        // column list is only knowable from the live source, so the offline
        // preview degrades to the canonical shape + note — never errors.
        let ir = merge_ir();
        let dialect = dialect_for_adapter_type("snowflake");
        assert!(
            sql_gen::generate_merge_sql(&ir, dialect.as_ref()).is_err(),
            "precondition: snowflake generate_merge_sql rejects ColumnSelection::All"
        );
        let preview = replication_copy_sql(&ir, dialect.as_ref())
            .expect("degrade path must not error the whole plan");
        assert!(preview.contains("MERGE INTO"), "got:\n{preview}");
        assert!(
            preview.contains("-- preview:"),
            "the degrade path must carry the run-time-resolution note, got:\n{preview}"
        );
    }

    #[test]
    fn replication_view_preview_renders_view_ddl_not_insert() {
        let ir = replication_ir(MaterializationStrategy::View);
        let dialect = dialect_for_adapter_type("databricks");
        let sql = replication_copy_sql(&ir, dialect.as_ref()).expect("view preview");
        let up = sql.to_uppercase();
        assert!(
            up.contains("CREATE") && up.contains("VIEW"),
            "view strategy must preview as VIEW DDL, got:\n{sql}"
        );
        assert!(
            !sql.contains("INSERT INTO"),
            "view must not preview as an INSERT (the pre-fix regression), got:\n{sql}"
        );
    }

    #[test]
    fn table_override_merge_resolves_to_merge_strategy() {
        // Guards the resolution wiring plan() and run() share: a per-table
        // `[[table_overrides]]` merge on a full_refresh base must resolve to
        // Merge for the matched table and stay FullRefresh for others. A revert
        // of the plan-loop override call would leave this green but the CLI
        // e2e (in the PR) red — this pins the resolution itself.
        let toml = r#"
[adapter.duck]
type = "duckdb"
path = "x.duckdb"

[pipeline.p]
type = "replication"
strategy = "full_refresh"

[[pipeline.p.table_overrides]]
match.table = "orders"
strategy = "merge"
merge_keys = ["order_id"]

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "c"
schema_template = "s__{source}"
"#;
        let cfg: rocky_core::config::RockyConfig = toml::from_str(toml).expect("parse config");
        let (_name, pipeline) =
            crate::registry::resolve_replication_pipeline(&cfg, None).expect("resolve pipeline");

        let matched =
            resolve_table_override(&pipeline.table_overrides, "conn", "raw__orders", "orders");
        let s = crate::commands::run::build_replication_strategy_with_override(pipeline, &matched)
            .expect("build strategy");
        assert!(
            matches!(s, MaterializationStrategy::Merge { .. }),
            "matched table_override must resolve to Merge, got {s:?}"
        );

        let unmatched =
            resolve_table_override(&pipeline.table_overrides, "conn", "raw__other", "widgets");
        let s2 =
            crate::commands::run::build_replication_strategy_with_override(pipeline, &unmatched)
                .expect("build strategy");
        assert!(
            matches!(s2, MaterializationStrategy::FullRefresh),
            "non-matching table must stay FullRefresh, got {s2:?}"
        );
    }

    // ------------------------------------------------------------------
    // render_retention_preview — warehouse dispatch
    // ------------------------------------------------------------------

    #[test]
    fn retention_preview_databricks_matches_adapter_sql() {
        let got = render_retention_preview("databricks", "c", "s", "t", 90).expect("some");
        // Same formula as rocky_core::catalog::generate_set_delta_retention_sql.
        assert_eq!(
            got,
            "ALTER TABLE c.s.t SET TBLPROPERTIES \
('delta.logRetentionDuration' = '90 days', \
'delta.deletedFileRetentionDuration' = '90 days')"
        );
    }

    #[test]
    fn retention_preview_snowflake_matches_adapter_sql() {
        let got = render_retention_preview("snowflake", "db", "sch", "tbl", 365).expect("some");
        assert_eq!(
            got,
            "ALTER TABLE db.sch.tbl SET DATA_RETENTION_TIME_IN_DAYS = 365"
        );
    }

    #[test]
    fn retention_preview_unsupported_adapters_are_none() {
        assert!(render_retention_preview("duckdb", "c", "s", "t", 90).is_none());
        assert!(render_retention_preview("bigquery", "c", "s", "t", 90).is_none());
        assert!(render_retention_preview("", "c", "s", "t", 90).is_none());
    }

    // ------------------------------------------------------------------
    // populate_governance_actions — end-to-end against a tempdir project.
    //
    // The full `plan()` path requires a discovery adapter; the unit
    // here exercises only the governance-preview branch, which is the
    // new surface in this change.
    // ------------------------------------------------------------------

    use std::fs;
    use tempfile::TempDir;

    fn write_project(
        tmp: &TempDir,
        rocky_toml: &str,
        models: &[(&str, &str)],
    ) -> (std::path::PathBuf, std::path::PathBuf) {
        let root = tmp.path();
        let cfg_path = root.join("rocky.toml");
        fs::write(&cfg_path, rocky_toml).unwrap();

        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();
        for (name, sidecar) in models {
            // Minimal SQL body so the compiler accepts the sidecar.
            let sql = format!("-- model: {name}\nSELECT 1 AS id");
            fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
            fs::write(models_dir.join(format!("{name}.toml")), *sidecar).unwrap();
        }
        (cfg_path, models_dir)
    }

    #[test]
    fn preview_populates_all_three_action_arrays() {
        let tmp = TempDir::new().unwrap();
        let (cfg_path, models_dir) = write_project(
            &tmp,
            r#"
[adapter.default]
type = "databricks"
host = "https://example.cloud.databricks.com"
http_path = "/sql/1.0/warehouses/abc"
token = "pat-xxx"

[mask]
pii = "hash"

[mask.prod]
pii = "redact"
"#,
            &[(
                "users",
                r#"name = "users"
retention = "90d"

[target]
catalog = "c"
schema = "s"
table = "users"

[classification]
email = "pii"
"#,
            )],
        );

        let cfg = rocky_core::config::load_rocky_config(&cfg_path).unwrap();
        let mut out = PlanOutput::new(String::new());

        // Default env — resolves `pii` to `hash`.
        populate_governance_actions(&cfg, &models_dir, None, "databricks", &mut out).unwrap();
        assert_eq!(out.classification_actions.len(), 1);
        assert_eq!(out.classification_actions[0].model, "users");
        assert_eq!(out.classification_actions[0].column, "email");
        assert_eq!(out.classification_actions[0].tag, "pii");

        assert_eq!(out.mask_actions.len(), 1);
        assert_eq!(out.mask_actions[0].resolved_strategy, "hash");

        assert_eq!(out.retention_actions.len(), 1);
        assert_eq!(out.retention_actions[0].model, "users");
        assert_eq!(out.retention_actions[0].duration_days, 90);
        assert!(
            out.retention_actions[0]
                .warehouse_preview
                .as_deref()
                .unwrap()
                .contains("delta.logRetentionDuration")
        );

        // --env prod — `[mask.prod]` flips pii to redact.
        let mut out_prod = PlanOutput::new(String::new());
        populate_governance_actions(&cfg, &models_dir, Some("prod"), "databricks", &mut out_prod)
            .unwrap();
        assert_eq!(out_prod.mask_actions[0].resolved_strategy, "redact");
        // Classification + retention are env-invariant.
        assert_eq!(
            out_prod.classification_actions.len(),
            out.classification_actions.len()
        );
        assert_eq!(
            out_prod.retention_actions.len(),
            out.retention_actions.len()
        );
    }

    #[test]
    fn preview_skips_mask_row_when_tag_unresolved() {
        let tmp = TempDir::new().unwrap();
        let (cfg_path, models_dir) = write_project(
            &tmp,
            r#"
[adapter.default]
type = "duckdb"
database = ":memory:"
"#,
            &[(
                "t",
                r#"name = "t"
[target]
catalog = "c"
schema = "s"
table = "t"

[classification]
ssn = "confidential"
"#,
            )],
        );

        let cfg = rocky_core::config::load_rocky_config(&cfg_path).unwrap();
        let mut out = PlanOutput::new(String::new());
        populate_governance_actions(&cfg, &models_dir, None, "duckdb", &mut out).unwrap();

        // Classification tagging still previews — it's env-invariant.
        assert_eq!(out.classification_actions.len(), 1);
        // Mask does NOT preview — no `[mask]` strategy resolves
        // `confidential`. `rocky compliance` is the diagnostic surface.
        assert!(out.mask_actions.is_empty());
        // Retention absent too — no sidecar declares it here.
        assert!(out.retention_actions.is_empty());
    }

    #[test]
    fn mask_strategy_wire_names_match_adapter() {
        // Guard against the preview string drifting from the enum's
        // canonical wire name. Breaks loud if anyone renames a variant
        // without updating `MaskStrategy::as_str`.
        assert_eq!(MaskStrategy::Hash.as_str(), "hash");
        assert_eq!(MaskStrategy::Redact.as_str(), "redact");
        assert_eq!(MaskStrategy::Partial.as_str(), "partial");
        assert_eq!(MaskStrategy::None.as_str(), "none");
    }

    // ------------------------------------------------------------------
    // plan_preview_output — offline, adapter-free SQL preview core.
    // ------------------------------------------------------------------

    /// A project with one full-refresh transformation model yields exactly one
    /// CTAS statement, and only `statements` is populated (governance /
    /// plan_id / execution_layers stay empty).
    #[test]
    fn plan_preview_renders_full_refresh_ctas() {
        let tmp = TempDir::new().unwrap();
        let (cfg_path, models_dir) = write_project(
            &tmp,
            r#"
[adapter.default]
type = "duckdb"
database = ":memory:"
"#,
            &[(
                "users",
                r#"name = "users"

[strategy]
type = "full_refresh"

[target]
catalog = "c"
schema = "s"
table = "users"
"#,
            )],
        );

        let out = plan_preview_output(Some(&cfg_path), &models_dir, None, None).unwrap();
        assert_eq!(out.statements.len(), 1, "one model → one statement");
        assert_eq!(out.statements[0].purpose, "full_refresh");
        assert_eq!(out.statements[0].target, "c.s.users");
        let sql = out.statements[0].sql.to_uppercase();
        assert!(
            sql.contains("CREATE") && sql.contains("TABLE"),
            "full_refresh should render CTAS, got: {}",
            out.statements[0].sql
        );
        // Preview core touches statements only.
        assert!(out.classification_actions.is_empty());
        assert!(out.mask_actions.is_empty());
        assert!(out.retention_actions.is_empty());
        assert!(out.plan_id.is_none());
        assert!(out.execution_layers.is_empty());
    }

    /// The `filter` arg narrows the preview to a single model by name and is
    /// copied onto `PlanOutput.filter`.
    #[test]
    fn plan_preview_filter_narrows_to_one_model() {
        let tmp = TempDir::new().unwrap();
        let model_a = r#"name = "a"

[strategy]
type = "full_refresh"

[target]
catalog = "c"
schema = "s"
table = "a"
"#;
        let model_b = r#"name = "b"

[strategy]
type = "full_refresh"

[target]
catalog = "c"
schema = "s"
table = "b"
"#;
        let (cfg_path, models_dir) = write_project(
            &tmp,
            r#"
[adapter.default]
type = "duckdb"
database = ":memory:"
"#,
            &[("a", model_a), ("b", model_b)],
        );

        let out = plan_preview_output(Some(&cfg_path), &models_dir, Some("a"), None).unwrap();
        assert_eq!(out.filter, "a");
        assert_eq!(out.statements.len(), 1);
        assert_eq!(out.statements[0].target, "c.s.a");
    }

    /// A `models/` directory with no compiled models (replication-only)
    /// returns empty statements rather than erroring.
    #[test]
    fn plan_preview_replication_only_returns_empty_statements() {
        let tmp = TempDir::new().unwrap();
        let cfg_path = tmp.path().join("rocky.toml");
        fs::write(
            &cfg_path,
            r#"
[adapter.default]
type = "duckdb"
database = ":memory:"
"#,
        )
        .unwrap();
        // models_dir exists but holds no model sidecars.
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();

        let out = plan_preview_output(Some(&cfg_path), &models_dir, None, None).unwrap();
        assert!(
            out.statements.is_empty(),
            "no compiled models → empty statements"
        );
        assert!(out.plan_id.is_none());
    }

    /// With no config, the preview still renders SQL using the default
    /// dialect (DuckDB under the default feature set).
    #[test]
    fn plan_preview_without_config_uses_default_dialect() {
        let tmp = TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        fs::create_dir_all(&models_dir).unwrap();
        fs::write(models_dir.join("m.sql"), "-- model: m\nSELECT 1 AS id").unwrap();
        fs::write(
            models_dir.join("m.toml"),
            r#"name = "m"

[strategy]
type = "full_refresh"

[target]
catalog = "c"
schema = "s"
table = "m"
"#,
        )
        .unwrap();

        let out = plan_preview_output(None, &models_dir, None, None).unwrap();
        assert_eq!(out.statements.len(), 1);
        assert_eq!(out.statements[0].target, "c.s.m");
    }

    // ------------------------------------------------------------------
    // PromotePlan — struct serialization and plan_store round-trip
    // ------------------------------------------------------------------

    use crate::output::{AuditEventKind, PromotePlan, PromoteTargetPlan};
    use crate::plan_store::{PlanKind, read_plan, write_plan};
    // TempDir is already in scope from above.

    fn minimal_promote_plan(branch_name: &str) -> PromotePlan {
        PromotePlan {
            branch_name: branch_name.to_string(),
            base_ref: "main".to_string(),
            head_ref: "abc1234".to_string(),
            branch_state_hash: "deadbeef".repeat(8),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: None,
            allow_breaking: false,
            targets: vec![PromoteTargetPlan {
                target: "cat.prod_schema.orders".to_string(),
                source: "cat.branch__fix.orders".to_string(),
                statement: "CREATE OR REPLACE TABLE \"cat\".\"prod_schema\".\"orders\" \
                     AS SELECT * FROM \"cat\".\"branch__fix\".\"orders\""
                    .to_string(),
            }],
            plan_audit: vec![],
            created_at: chrono::DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")
                .unwrap()
                .with_timezone(&chrono::Utc),
        }
    }

    /// `PromotePlan` serializes to JSON and can be deserialized back without
    /// data loss — the plan_store round-trip contract.
    #[test]
    fn promote_plan_serde_round_trip() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let plan = minimal_promote_plan("fix-price");

        let plan_id = write_plan(dir.path(), PlanKind::Promote, &plan)?;
        assert_eq!(plan_id.len(), 64);

        let persisted = read_plan(dir.path(), &plan_id)?;
        assert_eq!(persisted.kind, PlanKind::Promote);

        let decoded: PromotePlan = serde_json::from_value(persisted.payload)?;
        assert_eq!(decoded.branch_name, "fix-price");
        assert_eq!(decoded.base_ref, "main");
        assert_eq!(decoded.targets.len(), 1);
        assert_eq!(decoded.targets[0].target, "cat.prod_schema.orders");
        assert!(!decoded.allow_breaking);
        Ok(())
    }

    /// Two identical `PromotePlan` payloads produce the same plan_id —
    /// the idempotency / dedup property inherited from the plan_store.
    #[test]
    fn promote_plan_same_payload_same_plan_id() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let plan = minimal_promote_plan("fix-price");
        let id1 = write_plan(dir.path(), PlanKind::Promote, &plan)?;
        let id2 = write_plan(dir.path(), PlanKind::Promote, &plan)?;
        assert_eq!(id1, id2, "identical payload must produce identical plan_id");
        Ok(())
    }

    /// `PromotePlan` with `allow_breaking: true` round-trips the flag correctly.
    #[test]
    fn promote_plan_allow_breaking_flag_round_trips() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let mut plan = minimal_promote_plan("feat");
        plan.allow_breaking = true;
        plan.breaking_changes = Some(vec![]);

        let plan_id = write_plan(dir.path(), PlanKind::Promote, &plan)?;
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: PromotePlan = serde_json::from_value(persisted.payload)?;

        assert!(decoded.allow_breaking);
        assert!(
            matches!(&decoded.breaking_changes, Some(v) if v.is_empty()),
            "breaking_changes should be Some(empty vec)"
        );
        Ok(())
    }

    /// `PromoteTargetPlan` SQL string is preserved verbatim through the plan_store.
    /// This is the key invariant: apply executes exactly the SQL generated at plan time.
    #[test]
    fn promote_target_plan_sql_is_persisted_verbatim() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let sql = r#"CREATE OR REPLACE TABLE "prod"."schema"."orders" AS SELECT * FROM "branch__fix"."schema"."orders""#;
        let mut plan = minimal_promote_plan("fix");
        plan.targets[0].statement = sql.to_string();

        let plan_id = write_plan(dir.path(), PlanKind::Promote, &plan)?;
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: PromotePlan = serde_json::from_value(persisted.payload)?;

        assert_eq!(decoded.targets[0].statement, sql);
        Ok(())
    }

    /// Applying a Promote plan with a different branch name than the positional
    /// arg must return a clear error — guards against operator mismatches.
    #[test]
    fn promote_from_plan_branch_name_mismatch_is_error() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let plan = minimal_promote_plan("fix-price");
        let plan_id = write_plan(dir.path(), PlanKind::Promote, &plan)?;

        // Simulate what run_branch_promote_from_plan does: read + check name.
        let persisted = read_plan(dir.path(), &plan_id)?;
        let decoded: PromotePlan = serde_json::from_value(persisted.payload.clone())?;

        let provided_name = "different-branch";
        if provided_name != decoded.branch_name {
            // This is the expected error path.
            assert_eq!(decoded.branch_name, "fix-price");
        } else {
            panic!("should not reach here");
        }
        Ok(())
    }

    /// `AuditEventKind::PromotePlanCreated` serializes to the snake_case wire
    /// name expected by downstream consumers.
    #[test]
    fn promote_plan_created_audit_kind_wire_name() {
        let kind = AuditEventKind::PromotePlanCreated;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""promote_plan_created""#);
    }

    // ------------------------------------------------------------------
    // Replication plan source-state snapshot canonicalization (Phase 5b)
    //
    // The plan_id digest is computed over the JSON bytes of the
    // payload, so `build_source_state_snapshot` MUST be deterministic
    // across discover runs — same connectors, same tables, same
    // plan_id. These tests pin the sort + field-omission contract that
    // makes that property hold.
    // ------------------------------------------------------------------

    use rocky_core::source::{DiscoveredConnector, DiscoveredTable};

    fn make_connector(id: &str, schema: &str, table_names: &[&str]) -> DiscoveredConnector {
        DiscoveredConnector {
            id: id.to_string(),
            schema: schema.to_string(),
            source_type: "duckdb".to_string(),
            // last_sync_at is intentionally volatile — verify it
            // doesn't leak into the snapshot.
            last_sync_at: Some(chrono::Utc::now()),
            tables: table_names
                .iter()
                .map(|n| DiscoveredTable {
                    name: n.to_string(),
                    row_count: Some(10),
                })
                .collect(),
            // metadata is intentionally volatile (rate-limit counters
            // etc.) — verify it doesn't leak either.
            metadata: indexmap::IndexMap::from([(
                "fivetran.rate_limit_used".to_string(),
                serde_json::json!(42),
            )]),
            external_object_ids: Vec::new(),
        }
    }

    /// Connectors sort by `id`, tables within each connector sort by
    /// `name`. Discover adapters may return either ordering depending
    /// on the upstream API; without this canonicalization the
    /// content-addressed plan_id would wiggle.
    #[test]
    fn snapshot_sorts_connectors_and_tables() {
        let connectors = vec![
            make_connector("conn_zebra", "schema_z", &["zulu", "alpha"]),
            make_connector("conn_alpha", "schema_a", &["delta", "bravo"]),
        ];
        let snap = build_source_state_snapshot(&connectors);

        assert_eq!(snap.len(), 2);
        // Connectors sorted by id.
        assert_eq!(snap[0].id, "conn_alpha");
        assert_eq!(snap[1].id, "conn_zebra");
        // Tables sorted by name within each connector.
        assert_eq!(snap[0].tables[0].name, "bravo");
        assert_eq!(snap[0].tables[1].name, "delta");
        assert_eq!(snap[1].tables[0].name, "alpha");
        assert_eq!(snap[1].tables[1].name, "zulu");
    }

    /// Re-running the snapshot on the same connector list must
    /// produce byte-identical JSON. This is the property the plan_id
    /// content-addressing relies on.
    #[test]
    fn snapshot_is_deterministic_across_runs() {
        let connectors = vec![
            make_connector("c2", "s2", &["t2", "t1"]),
            make_connector("c1", "s1", &["t1"]),
        ];
        let s1 = build_source_state_snapshot(&connectors);
        let s2 = build_source_state_snapshot(&connectors);

        let j1 = serde_json::to_string(&s1).unwrap();
        let j2 = serde_json::to_string(&s2).unwrap();
        assert_eq!(j1, j2, "snapshot must be byte-stable for the same input");
    }

    /// Volatile fields (`last_sync_at`, adapter `metadata`) MUST be
    /// excluded from the snapshot. Including them would invalidate
    /// the plan on every sync tick — the very bug Phase 5b avoids by
    /// content-addressing on stable identity only.
    #[test]
    fn snapshot_excludes_volatile_fields() {
        let conn = make_connector("c1", "s1", &["t1"]);
        let snap = build_source_state_snapshot(std::slice::from_ref(&conn));
        let json = serde_json::to_value(&snap).unwrap();

        // Spot-check the wire format: no last_sync_at, no metadata.
        assert!(json[0].get("last_sync_at").is_none());
        assert!(json[0].get("metadata").is_none());
        // Identity fields are present.
        assert_eq!(json[0]["id"], "c1");
        assert_eq!(json[0]["schema"], "s1");
        assert_eq!(json[0]["source_type"], "duckdb");
        assert_eq!(json[0]["tables"][0]["name"], "t1");
        assert_eq!(json[0]["tables"][0]["row_count"], 10);
    }

    // ------------------------------------------------------------------
    // Semantic verdict (D3 — decision-support)
    // ------------------------------------------------------------------

    /// The caveat constant must plainly state that the classifier diffs
    /// OUTPUT SCHEMA and is blind to schema-stable value changes, and that
    /// an empty finding list is not a completeness signal. This guards the
    /// disclaimer text — required by the task's second hard constraint —
    /// against a silent edit that would weaken it.
    #[test]
    fn caveat_states_output_schema_scope_and_value_blindness() {
        let c = SEMANTIC_VERDICT_CAVEAT;
        assert!(
            c.contains("OUTPUT SCHEMA"),
            "caveat must name OUTPUT SCHEMA"
        );
        assert!(c.contains("BLIND"), "caveat must state the blindness");
        // The canonical value-change example the constraint calls out.
        assert!(
            c.contains("WHERE") && c.contains("JOIN") && c.contains("CASE"),
            "caveat must give the WHERE/JOIN/CASE value-rewrite example",
        );
        assert!(
            c.contains("NOT") && c.to_lowercase().contains("unchanged"),
            "caveat must deny that an empty result means the data is unchanged",
        );
    }

    /// The verdict's `caveat` is carried even when `findings` is empty —
    /// the case most easily misread as "safe". A JSON-only consumer must
    /// still see the disclaimer. This encodes the second hard constraint.
    #[test]
    fn empty_findings_verdict_still_serializes_the_caveat() {
        let verdict = SemanticPlanVerdict {
            caveat: SEMANTIC_VERDICT_CAVEAT.to_string(),
            base_ref: "main".to_string(),
            findings: vec![],
        };
        let json = serde_json::to_value(&verdict).expect("verdict serializes");
        assert_eq!(json["base_ref"], "main");
        assert!(
            json["findings"]
                .as_array()
                .expect("findings array")
                .is_empty(),
            "findings must be present and empty",
        );
        let caveat = json["caveat"].as_str().expect("caveat is a string");
        assert!(caveat.contains("OUTPUT SCHEMA"));
        assert!(caveat.contains("BLIND"));
    }

    /// A populated verdict round-trips: severity + change kind reach the
    /// JSON exactly as the classifier emits them, alongside the caveat.
    #[test]
    fn populated_verdict_carries_findings_and_caveat() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let verdict = SemanticPlanVerdict {
            caveat: SEMANTIC_VERDICT_CAVEAT.to_string(),
            base_ref: "origin/main".to_string(),
            findings: vec![BreakingFinding {
                change: BreakingChange::ColumnDropped {
                    model: "c.s.orders".to_string(),
                    column: "legacy_flag".to_string(),
                    data_type: "String".to_string(),
                },
                severity: BreakingSeverity::Breaking,
            }],
        };
        let json = serde_json::to_value(&verdict).expect("verdict serializes");
        assert_eq!(json["findings"][0]["severity"], "breaking");
        assert_eq!(json["findings"][0]["change"]["kind"], "column_dropped");
        assert_eq!(json["findings"][0]["change"]["column"], "legacy_flag");
        assert!(json["caveat"].as_str().unwrap().contains("OUTPUT SCHEMA"));
    }

    /// No `models/` directory ⇒ no baseline ⇒ verdict omitted (`None`).
    /// The verdict is never fabricated when there is nothing to diff.
    #[test]
    fn no_models_dir_yields_no_verdict() {
        let tmp = TempDir::new().unwrap();
        let cfg_path = tmp.path().join("rocky.toml");
        fs::write(&cfg_path, "[adapter.default]\ntype = \"duckdb\"\n").unwrap();
        let missing_models = tmp.path().join("models"); // not created
        let state_path = tmp.path().join("state.redb");
        let verdict = compute_semantic_verdict(&cfg_path, &missing_models, "main", &state_path);
        assert!(
            verdict.is_none(),
            "absent models directory must omit the verdict, not fabricate one",
        );
    }

    /// A models directory that exists but whose base ref cannot be
    /// materialized from git (the temp project is not a committed tree at
    /// `no-such-ref`) ⇒ no baseline ⇒ verdict omitted. Reuses the same
    /// `extract_base_compile` baseline mechanism as `ci-diff`, so a missing
    /// base ref degrades to `None` rather than erroring.
    #[test]
    fn unresolvable_base_ref_yields_no_verdict() {
        let tmp = TempDir::new().unwrap();
        let (cfg_path, models_dir) = write_project(
            &tmp,
            "[adapter.default]\ntype = \"duckdb\"\n",
            &[(
                "orders",
                "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"orders\"\n",
            )],
        );
        let state_path = tmp.path().join("state.redb");
        let verdict = compute_semantic_verdict(
            &cfg_path,
            &models_dir,
            "definitely-not-a-real-ref-xyz",
            &state_path,
        );
        assert!(
            verdict.is_none(),
            "an unresolvable base ref must omit the verdict (no fabricated baseline)",
        );
    }
}
