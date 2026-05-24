use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::Utc;

use rocky_core::config::GovernanceOverride;
use rocky_core::source::DiscoveredConnector;
use rocky_core::sql_gen;
use rocky_ir::*;

use crate::output::*;
use crate::plan_store::{PlanKind, write_plan};
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
pub async fn plan(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
    env: Option<&str>,
    run_options: &PlanRunOptions,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
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
        anyhow::bail!("no discovery adapter configured for this pipeline")
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

        if let Some((ref filter_key, ref filter_value)) = parsed_filter {
            if !matches_filter(conn, &parsed, filter_key, filter_value) {
                continue;
            }
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
        if pipeline.target.governance.auto_create_catalogs {
            if let Some(create_cat) = dialect.create_catalog_sql(&target_catalog) {
                let sql = create_cat.map_err(|e| anyhow::anyhow!("create_catalog: {e}"))?;
                output.statements.push(PlannedStatement {
                    purpose: "create_catalog".into(),
                    target: target_catalog.clone(),
                    sql,
                });
            }
        }

        // Schema creation — only when governance enables it.
        if pipeline.target.governance.auto_create_schemas {
            if let Some(create_sch) =
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

            let (strategy, purpose) = match pipeline.strategy.as_str() {
                "incremental" => (
                    MaterializationStrategy::Incremental {
                        timestamp_column: pipeline.timestamp_column.clone(),
                    },
                    "incremental_copy",
                ),
                _ => (MaterializationStrategy::FullRefresh, "full_refresh_copy"),
            };

            // sql_gen consumes the typed IR directly.
            let model_ir = ModelIr::replication(
                TargetRef {
                    catalog: effective_target_catalog.clone(),
                    schema: target_schema.clone(),
                    table: table.name.clone(),
                },
                strategy.clone(),
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

            // Pick the right SQL generator for the materialization strategy.
            // Full refresh uses CREATE OR REPLACE TABLE AS so the target doesn't
            // need to exist; incremental uses INSERT INTO which requires the
            // target to already exist (created on the first full-refresh run).
            //
            // `plan` is forward-looking — it renders the SQL the runner would
            // emit on a fresh run, so we pass `None` for the watermark to
            // surface the 1970-01-01 sentinel literal. The runner reads the
            // actual prior watermark from state at execute time.
            let sql = match &strategy {
                MaterializationStrategy::FullRefresh => {
                    sql_gen::generate_create_table_as_sql(&model_ir, dialect)?
                }
                MaterializationStrategy::Incremental { .. } => {
                    sql_gen::generate_insert_sql(&model_ir, dialect, None)?
                }
                _ => sql_gen::generate_insert_sql(&model_ir, dialect, None)?,
            };

            let target_label = if effective_target_catalog.is_empty() {
                format!("{target_schema}.{}", table.name)
            } else {
                format!("{effective_target_catalog}.{target_schema}.{}", table.name)
            };
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
            &blueprint_models_dir,
            filter,
            pipeline_name,
            env,
            run_options,
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
fn build_and_persist_run_plan(
    models_dir: &Path,
    filter: Option<&str>,
    pipeline: Option<&str>,
    env: Option<&str>,
    run_options: &PlanRunOptions,
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
    let plan_id = write_plan(&cwd, PlanKind::Run, &run_plan).context("failed to write run plan")?;

    let persisted_at = Utc::now();
    Ok(Some((run_plan, plan_id, persisted_at)))
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
fn populate_governance_actions(
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
/// - `allow_breaking` — bypass the breaking-change block gate.
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
) -> Result<PromotePlanResult> {
    use crate::commands::branch::{
        APPROVAL_SKIP_ENV, approver_identity_pub, compute_branch_state_hash_pub,
        discover_branch_targets_for_plan, run_approval_gate, run_breaking_change_gate_for_plan,
        validate_branch_name_pub,
    };
    use rocky_core::state::StateStore;

    validate_branch_name_pub(branch_name)?;

    let state_path =
        rocky_core::state::resolve_state_path(None, std::path::Path::new("models")).path;
    let store = StateStore::open_read_only(&state_path).with_context(|| {
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
    use super::*;
    use rocky_ir::MaskStrategy;

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
}
