use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::ir::*;
use rocky_core::sql_gen;

use crate::output::*;
use crate::registry;

use super::{matches_filter, parse_filter};

/// Execute `rocky plan` — dry-run SQL generation.
pub async fn plan(
    config_path: &Path,
    filter: &str,
    pipeline_name: Option<&str>,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, pipeline_name)?;
    let pattern = pipeline.schema_pattern()?;
    let (filter_key, filter_value) = parse_filter(filter)?;

    let adapter_registry = registry::AdapterRegistry::from_config(&rocky_cfg)?;
    let warehouse_adapter = adapter_registry.warehouse_adapter(&pipeline.target.adapter)?;
    let dialect = warehouse_adapter.dialect();

    let connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = adapter_registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        anyhow::bail!("no discovery adapter configured for this pipeline")
    };

    let mut output = PlanOutput::new(filter.to_string());

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

        if !matches_filter(conn, &parsed, &filter_key, &filter_value) {
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

        // Catalog creation — only when the dialect supports catalogs.
        if let Some(create_cat) = dialect.create_catalog_sql(&target_catalog) {
            let sql = create_cat.map_err(|e| anyhow::anyhow!("create_catalog: {e}"))?;
            output.statements.push(PlannedStatement {
                purpose: "create_catalog".into(),
                target: target_catalog.clone(),
                sql,
            });
        }

        // Schema creation — pass empty catalog when the dialect is catalog-less.
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
                        watermark: None,
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

    if output_json {
        print_json(&output)?;
    } else {
        for stmt in &output.statements {
            println!("-- {} ({})", stmt.purpose, stmt.target);
            println!("{};", stmt.sql);
            println!();
        }
    }
    Ok(())
}
