use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::ir::*;
use rocky_core::sql_gen;

use crate::output::*;
use crate::registry;

use super::{matches_filter, parse_filter};

/// Execute `rocky plan` — dry-run SQL generation.
///
/// The `env` parameter, when `Some`, selects the active environment
/// for the governance preview. It flows into `mask_actions` (via
/// [`rocky_core::config::RockyConfig::resolve_mask_for_env`]) so a
/// `[mask.<env>]` override surfaces in the preview on top of the
/// workspace `[mask]` defaults. Classification tags and retention
/// policies are env-invariant and are previewed regardless.
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

            let plan = ReplicationPlan {
                source: SourceRef {
                    catalog: effective_source_catalog.clone(),
                    schema: conn.schema.clone(),
                    table: table.name.clone(),
                },
                target: TargetRef {
                    catalog: effective_target_catalog.clone(),
                    schema: target_schema.clone(),
                    table: table.name.clone(),
                },
                strategy: strategy.clone(),
                columns: ColumnSelection::All,
                metadata_columns,
                governance: GovernanceConfig {
                    permissions_file: None,
                    auto_create_catalogs: pipeline.target.governance.auto_create_catalogs,
                    auto_create_schemas: pipeline.target.governance.auto_create_schemas,
                },
            };

            // Phase 2b.1: ModelIr built alongside Plan; sql_gen consumes it in 2b.2.
            let _model_ir = ModelIr::from(&Plan::Replication(plan.clone()));

            // Pick the right SQL generator for the materialization strategy.
            // Full refresh uses CREATE OR REPLACE TABLE AS so the target doesn't
            // need to exist; incremental uses INSERT INTO which requires the
            // target to already exist (created on the first full-refresh run).
            let sql = match &strategy {
                MaterializationStrategy::FullRefresh => {
                    sql_gen::generate_create_table_as_sql(&plan, dialect)?
                }
                MaterializationStrategy::Incremental { .. } => {
                    sql_gen::generate_insert_sql(&plan, dialect)?
                }
                _ => sql_gen::generate_insert_sql(&plan, dialect)?,
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

    if output_json {
        print_json(&output)?;
    } else {
        for stmt in &output.statements {
            println!("-- {} ({})", stmt.purpose, stmt.target);
            println!("{};", stmt.sql);
            println!();
        }
        render_governance_preview_text(&output);
    }
    Ok(())
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
            let plan = model.to_plan();
            let warehouse_preview = render_retention_preview(
                adapter_type,
                &plan.target.catalog,
                &plan.target.schema,
                &plan.target.table,
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

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::traits::MaskStrategy;

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
}
