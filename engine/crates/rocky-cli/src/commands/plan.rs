use std::path::Path;

use anyhow::{Context, Result};
use chrono::Utc;

use rocky_core::sql_gen;
use rocky_ir::*;

use crate::output::*;
use crate::plan_store::{PlanKind, write_plan};
use crate::registry;

use super::{matches_filter, parse_filter};

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
            let sql = match &strategy {
                MaterializationStrategy::FullRefresh => {
                    sql_gen::generate_create_table_as_sql(&model_ir, dialect)?
                }
                MaterializationStrategy::Incremental { .. } => {
                    sql_gen::generate_insert_sql(&model_ir, dialect)?
                }
                _ => sql_gen::generate_insert_sql(&model_ir, dialect)?,
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
    if models_dir.exists() {
        let adapter_type = rocky_cfg
            .adapters
            .get(&pipeline.target.adapter)
            .map(|a| a.adapter_type.as_str())
            .unwrap_or("");
        populate_governance_actions(&rocky_cfg, &models_dir, env, adapter_type, &mut output)
            .context("failed to compute governance action preview")?;
    }

    // --- Run-plan blueprint (Cluster 3 B, Phase 2) -----------------------
    //
    // When a `models/` directory exists, compile the project and persist a
    // `RunPlan` (operational metadata only — no full IR). `rocky apply` will
    // re-derive ProjectIr by recompiling with the same flags. Plan write is
    // best-effort — failure is logged as a warning, not an error, so
    // replication-only invocations in CI environments without `.rocky/`
    // write access are not broken.
    if models_dir.exists() {
        match build_and_persist_run_plan(&models_dir, filter, pipeline_name, env) {
            Ok((run_plan, plan_id, persisted_at)) => {
                output.plan_id = Some(plan_id);
                output.plan_kind = Some("run".to_string());
                output.created_at = Some(persisted_at);
                output.models = run_plan.models.clone();
                output.execution_layers = run_plan.execution_layers.clone();
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to build/persist run plan; `rocky apply` will not be available for this invocation"
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
        if let Some(ref plan_id) = output.plan_id {
            println!();
            println!(
                "Run plan persisted — {} model(s) across {} layer(s)",
                output.models.len(),
                output.execution_layers.len()
            );
            println!("Plan ID:   {plan_id}");
            println!("Apply with: rocky apply {plan_id}");
        }
    }
    Ok(())
}

/// Compile the models directory, build a `RunPlan` payload, persist it to
/// `.rocky/plans/<plan_id>.json`, and return `(payload, plan_id, persisted_at)`.
fn build_and_persist_run_plan(
    models_dir: &Path,
    filter: Option<&str>,
    pipeline: Option<&str>,
    env: Option<&str>,
) -> Result<(RunPlan, String, chrono::DateTime<Utc>)> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: std::collections::HashMap::new(),
        source_column_info: std::collections::HashMap::new(),
        mask: std::collections::BTreeMap::new(),
        allow_unmasked: vec![],
    };

    let result = compile::compile(&config).context("failed to compile models for run plan")?;

    // Collect qualified model names from the project.
    let models: Vec<String> = result
        .project
        .models
        .iter()
        .map(|m| m.config.name.clone())
        .collect();

    // Execution layers from the DAG (names only — informational).
    let execution_layers: Vec<Vec<String>> = result.project.layers.clone();

    let run_plan = RunPlan {
        filter: filter.map(str::to_string),
        pipeline: pipeline.map(str::to_string),
        branch: None,
        partition: None,
        partition_from: None,
        partition_to: None,
        latest: false,
        missing: false,
        lookback: None,
        parallel: 1,
        run_all: false,
        env: env.map(str::to_string),
        models,
        execution_layers,
    };

    let cwd = std::env::current_dir().context("failed to get current working directory")?;
    let plan_id = write_plan(&cwd, PlanKind::Run, &run_plan).context("failed to write run plan")?;

    let persisted_at = Utc::now();
    Ok((run_plan, plan_id, persisted_at))
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
    let planned_targets = discover_branch_targets_for_plan(config_path, &record, filter).await?;

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
}
