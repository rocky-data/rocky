//! Runtime side of a transformation `incremental` model (`rocky run`).
//!
//! SQL generation ([`rocky_core::sql_gen::generate_incremental_transformation_sql`])
//! already filters the model to rows past the target's `MAX(<watermark>)`.
//! What it cannot know offline is whether the target's columns still match
//! the model's output. This module checks that before an incremental run,
//! applies the model's `on_schema_change` policy, and reads the watermark
//! before and after so the run output can report it.
//!
//! ```text
//!   target absent ──────────────► bootstrap CTAS (filter = TRUE)   [run.rs]
//!   --full-refresh ─────────────► CREATE OR REPLACE (filter = TRUE) [run.rs]
//!   target present ─► compare columns ─► fail | ALTER ADD ─► INSERT/MERGE
//!                     read MAX(wm) before ─────────────────► read MAX(wm) after
//! ```

use anyhow::{Context, Result};
use rocky_core::models::StrategyConfig;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::{ColumnInfo, MaterializationStrategy, ModelIr, OnSchemaChange, TableRef};

/// What [`prepare`] decided for one incremental run against an existing target.
#[derive(Debug)]
pub(super) struct IncrementalRun {
    /// The statements to execute in place of the generic incremental SQL.
    pub exec_stmts: Vec<String>,
    /// The target's `MAX(<watermark>)` before this run, rendered as text.
    /// `None` when the target is empty.
    pub prior_watermark: Option<String>,
    /// Operator-visible actions taken (columns added).
    pub notes: Vec<String>,
}

/// Whether `--full-refresh` rebuilds a model of this strategy: `incremental`
/// only. Its SQL marks the filter with `@incremental_filter`, so resolving
/// that to `TRUE` is the full result. A `merge` or `delete_insert` model's SQL
/// often selects only recent rows on purpose, and rebuilding the table from
/// it would replace the history with that window.
pub(super) fn rebuilds_on_full_refresh(strategy: &MaterializationStrategy) -> bool {
    match strategy {
        MaterializationStrategy::Incremental { .. } => true,
        MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::FullRefresh
        | MaterializationStrategy::View
        | MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. }
        | MaterializationStrategy::TimeInterval { .. }
        | MaterializationStrategy::Ephemeral
        | MaterializationStrategy::Microbatch { .. }
        | MaterializationStrategy::ContentAddressed { .. } => false,
        // A snapshot's table is its history; rebuilding it from the current
        // SELECT would erase every closed version (dbt also ignores
        // `--full-refresh` for snapshots).
        MaterializationStrategy::Snapshot(_) => false,
    }
}

/// The IR a `--full-refresh` run executes: the same model rebuilt with
/// `CREATE OR REPLACE TABLE ... AS`, every `@incremental_filter` resolved to
/// `TRUE`.
pub(super) fn full_refresh_ir(model_ir: &ModelIr) -> ModelIr {
    let mut rebuilt = model_ir.clone();
    rebuilt.sql = rocky_core::incremental_filter::unfiltered_sql(&model_ir.sql);
    rebuilt.materialization = MaterializationStrategy::FullRefresh;
    rebuilt
}

/// Plan an incremental run against an existing target.
///
/// Returns `Ok(None)` when the target cannot be described: the caller's
/// existence probe then decides between bootstrap and failure, exactly as for
/// any other table-backed strategy.
///
/// # Errors
///
/// A column mismatch the model's `on_schema_change` does not allow, a failed
/// probe of the model's output columns, or a failed `ALTER TABLE`.
pub(super) async fn prepare(
    model: &rocky_core::models::Model,
    model_ir: &ModelIr,
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
) -> Result<Option<IncrementalRun>> {
    let model_name = model.config.name.as_str();
    let MaterializationStrategy::Incremental {
        timestamp_column,
        unique_key,
        ..
    } = &model_ir.materialization
    else {
        return Ok(None);
    };
    let on_schema_change = match &model.config.strategy {
        StrategyConfig::Incremental {
            on_schema_change, ..
        } => *on_schema_change,
        _ => OnSchemaChange::default(),
    };
    let table = TableRef {
        catalog: model_ir.target.catalog.clone(),
        schema: model_ir.target.schema.clone(),
        table: model_ir.target.table.clone(),
    };
    let target_columns = match warehouse.describe_table(&table).await {
        Ok(columns) => columns,
        // Transient: existence unknown, so fail closed like the caller's
        // own existence probe does.
        Err(e) if warehouse.classify_failure(&e).is_retryable() => {
            return Err(anyhow::Error::from(e).context(format!(
                "model '{model_name}': describing the target failed with a retryable error"
            )));
        }
        Err(_) => return Ok(None),
    };
    let target_ref = dialect
        .format_table_ref(&table.catalog, &table.schema, &table.table)
        .map_err(anyhow::Error::from)?;

    let model_columns = probe_output_columns(model_ir, warehouse, &target_ref).await?;
    let mut notes = Vec::new();
    let mut insert_columns = None;
    if !model_columns.is_empty() {
        let diff = ColumnDiff::new(&model_columns, &target_columns);
        if !diff.removed.is_empty() {
            anyhow::bail!(
                "model '{model_name}': the target {target_ref} has column(s) {} that the model \
                 no longer outputs; on_schema_change = \"{}\" does not drop columns. Run \
                 `rocky run --full-refresh` to rebuild the table",
                diff.removed.join(", "),
                on_schema_change.as_str()
            );
        }
        if !diff.added.is_empty() {
            match on_schema_change {
                OnSchemaChange::Fail => anyhow::bail!(
                    "model '{model_name}': the model outputs new column(s) {} that the target \
                     {target_ref} lacks (on_schema_change = \"fail\"). Set \
                     `on_schema_change = \"append_new_columns\"` in [strategy] to add them, or \
                     run `rocky run --full-refresh` to rebuild the table",
                    diff.added.join(", ")
                ),
                OnSchemaChange::AppendNewColumns => {
                    let added =
                        typed_added_columns(model_ir, warehouse, dialect, &table, &diff.added)
                            .await?;
                    let alters =
                        rocky_core::drift::generate_add_column_sql(&table, &added, dialect)
                            .map_err(anyhow::Error::from)?;
                    for alter in &alters {
                        warehouse
                            .execute_statement(alter)
                            .await
                            .map_err(anyhow::Error::from)
                            .with_context(|| {
                                format!("model '{model_name}': adding a new column failed")
                            })?;
                    }
                    notes.push(format!(
                        "Added column(s) {} to {target_ref} for model '{model_name}' \
                         (on_schema_change = \"append_new_columns\"); existing rows hold NULL",
                        diff.added.join(", ")
                    ));
                }
            }
        }
        // After any ALTER the target's order is its old columns plus the
        // added ones at the end. A positional INSERT is only safe when that
        // matches the model's order exactly.
        let target_order: Vec<String> = target_columns
            .iter()
            .map(|c| c.name.to_ascii_lowercase())
            .chain(diff.added.iter().map(|c| c.to_ascii_lowercase()))
            .collect();
        let model_order: Vec<String> = model_columns
            .iter()
            .map(|c| c.to_ascii_lowercase())
            .collect();
        if unique_key.is_empty() && target_order != model_order {
            insert_columns = Some(model_columns.clone());
        }
    }

    let exec_stmts = rocky_core::sql_gen::generate_incremental_transformation_sql(
        model_ir,
        dialect,
        insert_columns.as_deref(),
    )?;
    let prior_watermark = query_max(warehouse, &target_ref, timestamp_column).await?;
    Ok(Some(IncrementalRun {
        exec_stmts,
        prior_watermark,
        notes,
    }))
}

/// The target's `MAX(<watermark>)` as text, `None` for an empty target.
pub(super) async fn query_max(
    warehouse: &dyn WarehouseAdapter,
    target_ref: &str,
    column: &str,
) -> Result<Option<String>> {
    rocky_sql::validation::validate_identifier(column)
        .with_context(|| format!("invalid watermark column '{column}'"))?;
    let result = warehouse
        .execute_query(&format!("SELECT MAX({column}) FROM {target_ref}"))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("reading MAX({column}) from {target_ref} failed"))?;
    Ok(result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            serde_json::Value::Null => None,
            serde_json::Value::String(s) => Some(s.clone()),
            other => Some(other.to_string()),
        }))
}

/// The model's output column names, in order, from a zero-row query.
///
/// Empty when the adapter reports no columns for an empty result; the caller
/// then skips the comparison rather than guessing.
async fn probe_output_columns(
    model_ir: &ModelIr,
    warehouse: &dyn WarehouseAdapter,
    target_ref: &str,
) -> Result<Vec<String>> {
    let body = rocky_core::incremental_filter::unfiltered_sql(&model_ir.sql);
    let result = warehouse
        .execute_query(&format!(
            "SELECT * FROM (\n{body}\n) AS _rocky_probe LIMIT 0"
        ))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| {
            format!(
                "model '{}': reading its output columns to compare with {target_ref} failed",
                model_ir.name
            )
        })?;
    Ok(result.columns)
}

/// Warehouse types for the model's new columns, read from a zero-row probe
/// table created next to the target and dropped right after.
async fn typed_added_columns(
    model_ir: &ModelIr,
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
    table: &TableRef,
    added: &[String],
) -> Result<Vec<ColumnInfo>> {
    // Unique per call, so it never names a user's table or a concurrent
    // run's probe.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    let probe = TableRef {
        catalog: table.catalog.clone(),
        schema: table.schema.clone(),
        table: format!(
            "{}__rocky_probe_{}_{nonce}",
            table.table,
            std::process::id()
        ),
    };
    let probe_ref = dialect
        .format_table_ref(&probe.catalog, &probe.schema, &probe.table)
        .map_err(anyhow::Error::from)?;
    let body = rocky_core::incremental_filter::unfiltered_sql(&model_ir.sql);
    let drop_sql = dialect.drop_table_sql(&probe_ref);
    warehouse
        .execute_statement(&dialect.create_table_as_new(
            &probe_ref,
            &format!("SELECT * FROM (\n{body}\n) AS _rocky_probe LIMIT 0"),
        ))
        .await
        .map_err(anyhow::Error::from)
        .with_context(|| format!("creating the schema probe {probe_ref} failed"))?;
    let described = warehouse.describe_table(&probe).await;
    let dropped = warehouse.execute_statement(&drop_sql).await;
    let described = described
        .map_err(anyhow::Error::from)
        .with_context(|| format!("describing the schema probe {probe_ref} failed"))?;
    dropped
        .map_err(anyhow::Error::from)
        .with_context(|| format!("dropping the schema probe {probe_ref} failed"))?;
    added
        .iter()
        .map(|name| {
            described
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(name))
                .cloned()
                .ok_or_else(|| {
                    anyhow::anyhow!("the schema probe {probe_ref} has no column '{name}'")
                })
        })
        .collect()
}

/// Case-insensitive difference between model output and target columns.
struct ColumnDiff {
    /// In the model, not in the target. Model order.
    added: Vec<String>,
    /// In the target, not in the model. Target order.
    removed: Vec<String>,
}

impl ColumnDiff {
    fn new(model: &[String], target: &[ColumnInfo]) -> Self {
        let added = model
            .iter()
            .filter(|m| !target.iter().any(|t| t.name.eq_ignore_ascii_case(m)))
            .cloned()
            .collect();
        let removed = target
            .iter()
            .filter(|t| !model.iter().any(|m| m.eq_ignore_ascii_case(&t.name)))
            .map(|t| t.name.clone())
            .collect();
        Self { added, removed }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.into(),
            data_type: "INTEGER".into(),
            nullable: true,
        }
    }

    #[test]
    fn column_diff_is_case_insensitive_and_ordered() {
        let model = vec!["ID".to_string(), "amount".to_string(), "note".to_string()];
        let target = vec![col("id"), col("amount"), col("legacy")];
        let diff = ColumnDiff::new(&model, &target);
        assert_eq!(diff.added, vec!["note".to_string()]);
        assert_eq!(diff.removed, vec!["legacy".to_string()]);
    }

    /// The incremental-run SQL per warehouse: each dialect's MERGE wraps the
    /// same watermark-filtered body, and `lookback` uses that dialect's
    /// interval literal.
    #[test]
    fn incremental_sql_per_dialect() {
        use rocky_ir::{GovernanceConfig, IncrementalLookback, LookbackUnit, TargetRef};

        let mut ir = ModelIr::transformation(
            TargetRef {
                // BigQuery project ids need 6+ characters.
                catalog: "project123".into(),
                schema: "silver".into(),
                table: "fct".into(),
            },
            MaterializationStrategy::Incremental {
                timestamp_column: "updated_at".into(),
                unique_key: vec!["id".into()],
                lookback: Some(IncrementalLookback {
                    amount: 3,
                    unit: LookbackUnit::Day,
                }),
                filter_column: None,
            },
            vec![],
            "SELECT id, v, updated_at FROM cat.raw.t WHERE @incremental_filter".into(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        ir.typed_columns = ["id", "v", "updated_at"]
            .into_iter()
            .map(|name| rocky_ir::TypedColumn {
                name: name.into(),
                data_type: rocky_ir::RockyType::Unknown,
                nullable: true,
            })
            .collect();

        let cases: Vec<(Box<dyn SqlDialect>, &str)> = vec![
            (
                Box::new(rocky_databricks::dialect::DatabricksSqlDialect),
                "INTERVAL '3' DAY",
            ),
            (
                Box::new(rocky_snowflake::dialect::SnowflakeSqlDialect),
                "INTERVAL '3 DAY'",
            ),
            (
                Box::new(rocky_bigquery::dialect::BigQueryDialect),
                "INTERVAL 3 DAY",
            ),
        ];
        for (dialect, interval) in cases {
            let name = dialect.name();
            let target = dialect
                .format_table_ref("project123", "silver", "fct")
                .unwrap();
            let stmts = rocky_core::sql_gen::generate_transformation_sql(&ir, dialect.as_ref())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            assert_eq!(stmts.len(), 1, "{name}");
            let sql = &stmts[0];
            assert!(
                sql.starts_with(&format!("MERGE INTO {target}")),
                "{name}: {sql}"
            );
            assert!(
                sql.contains(&format!(
                    "(updated_at > (SELECT MAX(updated_at) - {interval} FROM {target}) \
                     OR NOT EXISTS (SELECT 1 FROM {target}))"
                )),
                "{name}: {sql}"
            );
            assert!(!sql.contains("@incremental_filter"), "{name}: {sql}");
        }
    }

    /// `--full-refresh` turns the skip gate off, so an unchanged model is
    /// rebuilt rather than reported as skipped.
    #[test]
    fn full_refresh_disables_the_skip_gate() {
        let mut gate = super::super::run::SkipGateConfig::off();
        gate.feature_enabled = true;
        assert!(gate.is_active());
        gate.full_refresh = true;
        assert!(!gate.is_active());
    }

    #[test]
    fn full_refresh_covers_only_incremental() {
        assert!(rebuilds_on_full_refresh(
            &MaterializationStrategy::Incremental {
                timestamp_column: "ts".into(),
                unique_key: Vec::new(),
                lookback: None,
                filter_column: None,
            }
        ));
        // merge/delete_insert SQL may select only recent rows.
        assert!(!rebuilds_on_full_refresh(
            &MaterializationStrategy::DeleteInsert {
                partition_by: vec!["d".into()],
            }
        ));
        assert!(!rebuilds_on_full_refresh(&MaterializationStrategy::Merge {
            unique_key: vec!["id".into()],
            update_columns: rocky_ir::ColumnSelection::All,
        }));
        assert!(!rebuilds_on_full_refresh(&MaterializationStrategy::View));
        assert!(!rebuilds_on_full_refresh(
            &MaterializationStrategy::FullRefresh
        ));
    }
}
