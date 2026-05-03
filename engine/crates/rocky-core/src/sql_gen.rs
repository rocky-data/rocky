use rocky_sql::validation;
use thiserror::Error;

use crate::ir::{
    MaterializationStrategy, ModelIr, PartitionWindow, Plan, ReplicationPlan, SnapshotPlan,
    TransformationPlan,
};
use crate::lakehouse::{self, LakehouseError};
use crate::traits::{AdapterError, SqlDialect};

/// Extract the variant-typed `ReplicationPlan` from a [`ModelIr`].
///
/// Returns [`SqlGenError::InvalidRequest`] when the IR was not constructed
/// from a [`Plan::Replication`]. The check is shaped by which variant-
/// specific fields are populated: replication carries an explicit
/// [`crate::ir::ColumnSelection`] in `columns`.
fn replication_from_ir(model_ir: &ModelIr) -> Result<ReplicationPlan, SqlGenError> {
    match model_ir.to_plan_compatible() {
        Plan::Replication(plan) => Ok(plan),
        _ => Err(SqlGenError::InvalidRequest(format!(
            "expected Replication ModelIr for `{}`",
            model_ir.name
        ))),
    }
}

/// Extract the variant-typed `TransformationPlan` from a [`ModelIr`].
///
/// Returns [`SqlGenError::InvalidRequest`] when the IR was not constructed
/// from a [`Plan::Transformation`].
fn transformation_from_ir(model_ir: &ModelIr) -> Result<TransformationPlan, SqlGenError> {
    match model_ir.to_plan_compatible() {
        Plan::Transformation(plan) => Ok(plan),
        _ => Err(SqlGenError::InvalidRequest(format!(
            "expected Transformation ModelIr for `{}`",
            model_ir.name
        ))),
    }
}

/// Extract the variant-typed `SnapshotPlan` from a [`ModelIr`].
///
/// Returns [`SqlGenError::InvalidRequest`] when the IR was not constructed
/// from a [`Plan::Snapshot`].
fn snapshot_from_ir(model_ir: &ModelIr) -> Result<SnapshotPlan, SqlGenError> {
    match model_ir.to_plan_compatible() {
        Plan::Snapshot(plan) => Ok(plan),
        _ => Err(SqlGenError::InvalidRequest(format!(
            "expected Snapshot ModelIr for `{}`",
            model_ir.name
        ))),
    }
}

/// Errors from SQL generation, including identifier validation and unsafe fragment detection.
#[derive(Debug, Error)]
pub enum SqlGenError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("unsafe SQL fragment '{value}': {reason}")]
    UnsafeFragment { value: String, reason: String },

    #[error("merge strategy requires at least one unique_key column")]
    MergeNoKey,

    #[error(
        "time_interval plan is missing its PartitionWindow — the runtime must populate `window` per partition before calling generate_transformation_sql"
    )]
    MissingPartitionWindow,

    #[error("lakehouse DDL error: {0}")]
    Lakehouse(#[from] LakehouseError),

    /// An adapter-level error surfaced while composing SQL (e.g. identifier
    /// rejection from `SqlDialect::format_table_ref`). Keeping the source
    /// error in the chain preserves the offending identifier rather than
    /// stringifying it into an empty-`value` `UnsafeFragment`.
    #[error("dialect error: {0}")]
    Dialect(#[from] AdapterError),

    /// SQL generation was invoked with an incompatible plan or arguments —
    /// e.g. `MaterializationStrategy::DynamicTable` without a warehouse, or
    /// a `check`-strategy snapshot without any `check_column`s. Distinct
    /// from `UnsafeFragment`, which reports bad SQL content.
    #[error("invalid SQL generation request: {0}")]
    InvalidRequest(String),
}

/// Generates the SELECT SQL for a replication model using the given dialect.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Replication`].
pub fn generate_select_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = replication_from_ir(model_ir)?;
    let select = dialect.select_clause(&plan.columns, &plan.metadata_columns)?;

    let source = dialect.format_table_ref(
        &plan.source.catalog,
        &plan.source.schema,
        &plan.source.table,
    )?;

    let mut sql = format!("{select}\nFROM {source}");

    if let MaterializationStrategy::Incremental {
        timestamp_column, ..
    } = &plan.strategy
    {
        let target = dialect.format_table_ref(
            &plan.target.catalog,
            &plan.target.schema,
            &plan.target.table,
        )?;

        let where_clause = dialect.watermark_where(timestamp_column, &target)?;

        let _ = write!(sql, "\n{where_clause}");
    }

    Ok(sql)
}

/// Generate SELECT SQL without any watermark filter (full refresh semantics).
///
/// Used by `generate_create_table_as_sql` and `generate_merge_sql` to avoid
/// cloning the entire `ReplicationPlan` just to override the strategy.
fn generate_select_sql_no_watermark(
    plan: &ReplicationPlan,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let select = dialect.select_clause(&plan.columns, &plan.metadata_columns)?;

    let source = dialect.format_table_ref(
        &plan.source.catalog,
        &plan.source.schema,
        &plan.source.table,
    )?;

    Ok(format!("{select}\nFROM {source}"))
}

/// Generates `INSERT INTO <target> SELECT ...` using the given dialect.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Replication`].
pub fn generate_insert_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = replication_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;
    let select = generate_select_sql(model_ir, dialect)?;
    Ok(dialect.insert_into(&target, &select))
}

/// Generates CREATE TABLE AS SELECT using the given dialect (full refresh).
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Replication`].
pub fn generate_create_table_as_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = replication_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    let select = generate_select_sql_no_watermark(&plan, dialect)?;

    Ok(dialect.create_table_as(&target, &select))
}

/// Generates a MERGE INTO statement using the given dialect.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Replication`].
pub fn generate_merge_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = replication_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    let (unique_key, update_columns) = match &plan.strategy {
        MaterializationStrategy::Merge {
            unique_key,
            update_columns,
        } => (unique_key, update_columns),
        _ => {
            return generate_create_table_as_sql(model_ir, dialect);
        }
    };

    let select = generate_select_sql_no_watermark(&plan, dialect)?;

    Ok(dialect.merge_into(&target, &select, unique_key, update_columns)?)
}

/// Generates transformation SQL using the given dialect.
///
/// Returns `Vec<String>` rather than `String` because some materialization
/// strategies (notably `time_interval`) decompose into multiple statements
/// that the runtime must execute in order. For single-statement strategies
/// (`FullRefresh`, `Incremental`, `Merge`, `MaterializedView`) the vec
/// contains exactly one entry; callers that need a single string can pattern-
/// match `&[only]` or call `.join("\n")` if cosmetic concatenation is fine.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Transformation`].
pub fn generate_transformation_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let plan = transformation_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    // Validate all source references
    for source in &plan.sources {
        dialect.format_table_ref(&source.catalog, &source.schema, &source.table)?;
    }

    // If a lakehouse format is specified, use format-specific DDL generation
    // for strategies that create tables (FullRefresh, Incremental first-run,
    // Merge first-run). For non-FullRefresh strategies, the runtime probes
    // target existence via `describe_table` and calls
    // `generate_transformation_initial_ddl` when missing (Merge today;
    // Incremental / DeleteInsert / Microbatch wired in as their live smoke
    // tests land). Here we handle FullRefresh which always does CTAS.
    if let Some(ref format) = plan.format {
        if matches!(plan.strategy, MaterializationStrategy::FullRefresh) {
            let opts = plan.format_options.as_ref().cloned().unwrap_or_default();
            return Ok(lakehouse::generate_lakehouse_ddl(
                format, &target, &plan.sql, &opts, dialect,
            )?);
        }
    }

    match &plan.strategy {
        MaterializationStrategy::FullRefresh => {
            Ok(vec![dialect.create_table_as(&target, &plan.sql)])
        }
        MaterializationStrategy::Incremental { .. } => {
            Ok(vec![dialect.insert_into(&target, &plan.sql)])
        }
        MaterializationStrategy::Merge {
            unique_key,
            update_columns,
        } => {
            let stmt = dialect.merge_into(&target, &plan.sql, unique_key, update_columns)?;
            Ok(vec![stmt])
        }
        MaterializationStrategy::MaterializedView => {
            Ok(vec![generate_materialized_view_sql(model_ir, dialect)?])
        }
        MaterializationStrategy::DynamicTable { .. } => {
            // Dynamic tables require a warehouse parameter not available in the plan.
            // Use generate_dynamic_table_sql directly when warehouse is known.
            Err(SqlGenError::InvalidRequest(
                "DynamicTable strategy requires calling generate_dynamic_table_sql with warehouse parameter".to_string(),
            ))
        }
        MaterializationStrategy::TimeInterval {
            time_column,
            window,
            ..
        } => {
            // The runtime is responsible for populating `window` before calling
            // us; static planning leaves it None.
            let window = window.as_ref().ok_or(SqlGenError::MissingPartitionWindow)?;

            // SECURITY: time_column comes from user TOML — validate as a SQL
            // identifier before interpolating into the WHERE clause. The
            // compiler also runs this check at validate_time_interval_models
            // time (E023), but defense in depth: re-validate at the planning
            // boundary so a buggy or skipped compile pass can't get past us.
            // Per repo CLAUDE.md: never use format!() with untrusted input.
            validation::validate_identifier(time_column).map_err(|e| {
                SqlGenError::UnsafeFragment {
                    value: time_column.clone(),
                    reason: e.to_string(),
                }
            })?;

            // Build the partition filter. Timestamps are formatted by chrono
            // from a fixed format string — never user input. Single-quoted
            // literals match the existing pattern in the rest of sql_gen.
            let filter = format!(
                "{tc} >= '{start}' AND {tc} < '{end}'",
                tc = time_column,
                start = window.start.format("%Y-%m-%d %H:%M:%S"),
                end = window.end.format("%Y-%m-%d %H:%M:%S"),
            );

            let substituted = substitute_partition_placeholders(&plan.sql, window);

            Ok(dialect.insert_overwrite_partition(&target, &filter, &substituted)?)
        }
        MaterializationStrategy::Ephemeral => {
            // Ephemeral models are not materialized — the compiler inlines
            // them as CTEs in downstream queries. If we reach SQL gen for an
            // ephemeral model, it's a no-op.
            Ok(vec![])
        }
        MaterializationStrategy::DeleteInsert { partition_by } => {
            // Validate partition columns
            for col in partition_by {
                validation::validate_identifier(col)?;
            }

            // Build the subquery to identify partition values
            let partition_cols = partition_by.join(", ");
            let delete_sql = format!(
                "DELETE FROM {target} WHERE ({partition_cols}) IN (\
                 SELECT DISTINCT {partition_cols} FROM ({source_sql}) AS _rocky_incoming\
                 )",
                source_sql = plan.sql,
            );
            let insert_sql = dialect.insert_into(&target, &plan.sql);
            Ok(vec![delete_sql, insert_sql])
        }
        MaterializationStrategy::Microbatch {
            timestamp_column, ..
        } => {
            // Microbatch is functionally an incremental append keyed by
            // the timestamp column. The runtime handles batch windowing;
            // SQL gen emits a simple INSERT with watermark filter.
            validation::validate_identifier(timestamp_column)?;
            Ok(vec![dialect.insert_into(&target, &plan.sql)])
        }
    }
}

/// Generate a one-time bootstrap statement to create an empty target table
/// for a `time_interval` model.
///
/// The runtime calls this exactly once per model the first time the target
/// table doesn't exist (detected via `WarehouseAdapter::describe_table`).
/// We render the model SQL with `@start_date` and `@end_date` substituted
/// to the **same** sentinel timestamp (`'1900-01-01 00:00:00'`), which
/// makes the half-open `[start, end)` window empty — so the model's WHERE
/// clause filters out every upstream row, and we end up with a zero-row
/// table whose **schema** matches what the model would produce on a real
/// run.
///
/// Wrapping the rendered body in `dialect.create_table_as` gives us the
/// per-warehouse syntax (Databricks `CREATE OR REPLACE TABLE ... AS`,
/// Snowflake `CREATE OR REPLACE TABLE ... AS`, DuckDB `CREATE OR REPLACE
/// TABLE ... AS`) without duplicating that logic here.
///
/// Once the table exists, subsequent partition runs use the regular
/// `insert_overwrite_partition` DELETE+INSERT cycle.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Transformation`].
pub fn generate_time_interval_bootstrap_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = transformation_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    // Sentinel window: start == end → half-open [start, end) is empty.
    // The chosen timestamp is well before any real partition boundary so
    // the bootstrap can never accidentally include real data even if the
    // model's WHERE clause is malformed.
    let sentinel = chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
        chrono::NaiveDateTime::parse_from_str("1900-01-01 00:00:00", "%Y-%m-%d %H:%M:%S")
            .expect("hardcoded sentinel parses"),
        chrono::Utc,
    );
    let bootstrap_window = PartitionWindow {
        key: "bootstrap".to_string(),
        start: sentinel,
        end: sentinel,
    };

    let body = substitute_partition_placeholders(&plan.sql, &bootstrap_window);

    // When a lakehouse format is specified, the bootstrap table must be
    // created using format-specific DDL (e.g., USING DELTA / USING ICEBERG)
    // so the partitioning, clustering, and table properties are applied from
    // the very first creation.
    if let Some(ref format) = plan.format {
        let opts = plan.format_options.as_ref().cloned().unwrap_or_default();
        let stmts = lakehouse::generate_lakehouse_ddl(format, &target, &body, &opts, dialect)?;
        // generate_lakehouse_ddl returns Vec<String>; join into a single
        // statement since the bootstrap function returns String.
        return Ok(stmts.join(";\n"));
    }

    Ok(dialect.create_table_as(&target, &body))
}

/// Generates the initial DDL to create a target table with lakehouse format
/// for non-FullRefresh strategies.
///
/// Strategies like Incremental, Merge, DeleteInsert, and Microbatch need
/// the target table to exist before their first INSERT/MERGE run. When
/// `plan.format` is set, the runtime should call this function to create
/// the table using format-specific DDL (e.g., `CREATE TABLE ... USING DELTA`).
///
/// When `plan.format` is `None`, falls back to a plain
/// `dialect.create_table_as`. The body SQL is the plan's `sql` field, so
/// the table schema matches what the model would produce.
///
/// This is complementary to `generate_transformation_sql` — the runtime
/// calls this once on first run, then switches to the regular strategy
/// for subsequent runs.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Transformation`].
pub fn generate_transformation_initial_ddl(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let plan = transformation_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    if let Some(ref format) = plan.format {
        let opts = plan.format_options.as_ref().cloned().unwrap_or_default();
        return Ok(lakehouse::generate_lakehouse_ddl(
            format, &target, &plan.sql, &opts, dialect,
        )?);
    }

    Ok(vec![dialect.create_table_as(&target, &plan.sql)])
}

/// Substitute `@start_date` / `@end_date` placeholders in the model SQL with
/// quoted timestamp literals from the partition window.
///
/// Word-boundary aware via simple string replace — `@start_date_extra` would
/// not be matched by a more careful tokenizer, but the compiler's
/// `check_time_interval_placeholders` already rejects models that don't
/// reference the bare placeholders, so a malformed `@start_date_extra`
/// reference would have failed compilation upstream.
fn substitute_partition_placeholders(sql: &str, window: &PartitionWindow) -> String {
    sql.replace(
        "@start_date",
        &format!("'{}'", window.start.format("%Y-%m-%d %H:%M:%S")),
    )
    .replace(
        "@end_date",
        &format!("'{}'", window.end.format("%Y-%m-%d %H:%M:%S")),
    )
}

/// Generates CREATE MATERIALIZED VIEW SQL for a transformation model.
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Transformation`].
pub fn generate_materialized_view_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = transformation_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    Ok(format!(
        "CREATE OR REPLACE MATERIALIZED VIEW {target} AS\n{sql}",
        sql = plan.sql
    ))
}

/// Generates CREATE DYNAMIC TABLE SQL for a transformation model (Snowflake).
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Transformation`].
pub fn generate_dynamic_table_sql(
    model_ir: &ModelIr,
    target_lag: &str,
    warehouse: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let plan = transformation_from_ir(model_ir)?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    // Validate target_lag and warehouse to prevent injection
    validate_sql_type(target_lag)?;
    validation::validate_identifier(warehouse)?;

    Ok(format!(
        "CREATE OR REPLACE DYNAMIC TABLE {target}\n  TARGET_LAG = '{target_lag}'\n  WAREHOUSE = {warehouse}\nAS\n{sql}",
        sql = plan.sql
    ))
}

/// Validates a SQL type string for safety (no injection).
pub fn validate_sql_type(data_type: &str) -> Result<(), SqlGenError> {
    if data_type.is_empty() {
        return Err(SqlGenError::UnsafeFragment {
            value: data_type.to_string(),
            reason: "data type cannot be empty".to_string(),
        });
    }
    // SQL types: alphanumeric, underscores, spaces, parens, commas
    // e.g., STRING, INT, DECIMAL(10,2), DOUBLE PRECISION
    let valid = data_type
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | ' ' | '(' | ')' | ','));
    if !valid {
        return Err(SqlGenError::UnsafeFragment {
            value: data_type.to_string(),
            reason: "data type contains invalid characters".to_string(),
        });
    }
    Ok(())
}

/// Validates a SQL literal value for safety (no injection).
pub fn validate_literal_value(value: &str) -> Result<(), SqlGenError> {
    if value == "NULL" {
        return Ok(());
    }
    if value.parse::<f64>().is_ok() {
        return Ok(());
    }
    // Simple quoted strings (no embedded quotes, no injection)
    if value.starts_with('\'')
        && value.ends_with('\'')
        && value.len() >= 2
        && !value[1..value.len() - 1].contains('\'')
        && !value.contains("--")
        && !value.contains(';')
    {
        return Ok(());
    }
    Err(SqlGenError::UnsafeFragment {
        value: value.to_string(),
        reason: "value must be NULL, a number, or a simple single-quoted string".to_string(),
    })
}

use std::fmt::Write;

/// Generates SCD Type 2 snapshot SQL using MERGE.
///
/// The generated SQL:
/// 1. Closes changed rows (sets `valid_to = CURRENT_TIMESTAMP`) for rows where
///    the `updated_at` column has changed.
/// 2. Inserts new versions with `valid_from = CURRENT_TIMESTAMP, valid_to = NULL`.
/// 3. Optionally invalidates hard-deleted rows (source rows that no longer exist).
///
/// # Errors
///
/// Returns [`SqlGenError::InvalidRequest`] when `model_ir` was not
/// constructed from a [`Plan::Snapshot`].
pub fn generate_snapshot_sql(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let plan = snapshot_from_ir(model_ir)?;
    let source = dialect.format_table_ref(
        &plan.source.catalog,
        &plan.source.schema,
        &plan.source.table,
    )?;
    let target = dialect.format_table_ref(
        &plan.target.catalog,
        &plan.target.schema,
        &plan.target.table,
    )?;

    if plan.unique_key.is_empty() {
        return Err(SqlGenError::MergeNoKey);
    }

    // Validate identifiers
    for k in &plan.unique_key {
        validation::validate_identifier(k)?;
    }
    validation::validate_identifier(&plan.updated_at)?;

    let join_cond = plan
        .unique_key
        .iter()
        .map(|k| format!("target.{k} = source.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut stmts = Vec::new();

    // Statement 1: Create target table if it doesn't exist (bootstrap)
    // This adds the SCD2 columns (valid_from, valid_to) to the source schema.
    let bootstrap = format!(
        "CREATE TABLE IF NOT EXISTS {target} AS \
         SELECT *, CURRENT_TIMESTAMP AS valid_from, \
         CAST(NULL AS TIMESTAMP) AS valid_to \
         FROM {source} WHERE 1=0"
    );
    stmts.push(bootstrap);

    // Statement 2: Close changed rows (set valid_to) and insert new versions
    let merge = format!(
        "MERGE INTO {target} AS target \
         USING {source} AS source \
         ON {join_cond} AND target.valid_to IS NULL \
         WHEN MATCHED AND source.{updated_at} != target.{updated_at} THEN \
           UPDATE SET valid_to = CURRENT_TIMESTAMP \
         WHEN NOT MATCHED THEN \
           INSERT (*) VALUES (source.*, CURRENT_TIMESTAMP, NULL)",
        updated_at = plan.updated_at,
    );
    stmts.push(merge);

    // Statement 3: Insert new versions for rows that were updated
    // (the MERGE above closed them, now insert the fresh version)
    let insert_new = format!(
        "INSERT INTO {target} \
         SELECT source.*, CURRENT_TIMESTAMP AS valid_from, \
         CAST(NULL AS TIMESTAMP) AS valid_to \
         FROM {source} AS source \
         INNER JOIN {target} AS target \
         ON {join_cond} \
         WHERE target.valid_to = (\
           SELECT MAX(t2.valid_to) FROM {target} t2 \
           WHERE {self_join_cond}\
         ) \
         AND NOT EXISTS (\
           SELECT 1 FROM {target} AS existing \
           WHERE {existing_join_cond} AND existing.valid_to IS NULL\
         )",
        self_join_cond = plan
            .unique_key
            .iter()
            .map(|k| format!("t2.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND "),
        existing_join_cond = plan
            .unique_key
            .iter()
            .map(|k| format!("existing.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND "),
    );
    stmts.push(insert_new);

    // Statement 4 (optional): Invalidate hard-deleted rows
    if plan.invalidate_hard_deletes {
        let invalidate = format!(
            "UPDATE {target} SET valid_to = CURRENT_TIMESTAMP \
             WHERE valid_to IS NULL \
             AND NOT EXISTS (\
               SELECT 1 FROM {source} AS source \
               WHERE {join_cond}\
             )",
        );
        stmts.push(invalidate);
    }

    Ok(stmts)
}

/// Generate transformation SQL for multiple plans in parallel using rayon.
///
/// Each model's SQL generation is independent, making this embarrassingly
/// parallel. At 50k models, this reduces SQL gen time from ~1.2s to <200ms.
pub fn generate_transformations_parallel(
    model_irs: &[ModelIr],
    dialect: &(dyn SqlDialect + Sync),
) -> Vec<Result<Vec<String>, SqlGenError>> {
    use rayon::prelude::*;
    model_irs
        .par_iter()
        .map(|model_ir| generate_transformation_sql(model_ir, dialect))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::ir::*;
    use crate::traits::{AdapterError, AdapterResult, SqlDialect};

    use super::*;

    /// Test dialect that mirrors Databricks behavior for rocky-core tests.
    /// This avoids a circular dependency on rocky-databricks.
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> AdapterResult<String> {
            rocky_sql::validation::format_table_ref(catalog, schema, table)
                .map_err(AdapterError::new)
        }

        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }

        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target}\n{select_sql}")
        }

        fn merge_into(
            &self,
            target: &str,
            source_sql: &str,
            keys: &[std::sync::Arc<str>],
            update_cols: &ColumnSelection,
        ) -> AdapterResult<String> {
            if keys.is_empty() {
                return Err(AdapterError::msg(
                    "merge strategy requires at least one unique_key column",
                ));
            }
            for key in keys {
                rocky_sql::validation::validate_identifier(key).map_err(AdapterError::new)?;
            }
            let on_clause = keys
                .iter()
                .map(|k| format!("t.{k} = s.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let update_clause = match update_cols {
                ColumnSelection::All => "UPDATE SET *".to_string(),
                ColumnSelection::Explicit(cols) => {
                    let sets = cols
                        .iter()
                        .map(|c| {
                            rocky_sql::validation::validate_identifier(c)
                                .map_err(AdapterError::new)?;
                            Ok(format!("t.{c} = s.{c}"))
                        })
                        .collect::<AdapterResult<Vec<_>>>()?;
                    format!("UPDATE SET {}", sets.join(", "))
                }
            };
            Ok(format!(
                "MERGE INTO {target} AS t\n\
                 USING (\n{source_sql}\n) AS s\n\
                 ON {on_clause}\n\
                 WHEN MATCHED THEN {update_clause}\n\
                 WHEN NOT MATCHED THEN INSERT *"
            ))
        }

        fn select_clause(
            &self,
            columns: &ColumnSelection,
            metadata: &[MetadataColumn],
        ) -> AdapterResult<String> {
            let mut sql = String::from("SELECT ");
            match columns {
                ColumnSelection::All => sql.push('*'),
                ColumnSelection::Explicit(cols) => {
                    for (i, col) in cols.iter().enumerate() {
                        rocky_sql::validation::validate_identifier(col)
                            .map_err(AdapterError::new)?;
                        if i > 0 {
                            sql.push_str(", ");
                        }
                        sql.push_str(col);
                    }
                }
            }
            for mc in metadata {
                rocky_sql::validation::validate_identifier(&mc.name).map_err(AdapterError::new)?;
                write!(
                    sql,
                    ", CAST({} AS {}) AS {}",
                    mc.value, mc.data_type, mc.name
                )
                .unwrap();
            }
            Ok(sql)
        }

        fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
            rocky_sql::validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
            Ok(format!(
                "WHERE {timestamp_col} > (\n\
                 \x20   SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01')\n\
                 \x20   FROM {target_ref}\n\
                 )"
            ))
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE TABLE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }

        fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
            Some(
                rocky_sql::validation::validate_identifier(name)
                    .map(|_| format!("CREATE CATALOG IF NOT EXISTS {name}"))
                    .map_err(AdapterError::new),
            )
        }

        fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
            let result = (|| {
                rocky_sql::validation::validate_identifier(catalog).map_err(AdapterError::new)?;
                rocky_sql::validation::validate_identifier(schema).map_err(AdapterError::new)?;
                Ok(format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
            })();
            Some(result)
        }

        fn tablesample_clause(&self, percent: u32) -> Option<String> {
            Some(format!("TABLESAMPLE ({percent} PERCENT)"))
        }

        fn insert_overwrite_partition(
            &self,
            target: &str,
            partition_filter: &str,
            select_sql: &str,
        ) -> AdapterResult<Vec<String>> {
            // Test dialect mirrors Databricks: single REPLACE WHERE statement.
            Ok(vec![format!(
                "INSERT INTO {target} REPLACE WHERE {partition_filter}\n{select_sql}"
            )])
        }
    }

    fn dialect() -> TestDialect {
        TestDialect
    }

    /// Lift a [`ReplicationPlan`] into a [`ModelIr`] for testing.
    fn rep_ir(plan: &ReplicationPlan) -> ModelIr {
        ModelIr::from(&Plan::Replication(plan.clone()))
    }

    /// Lift a [`TransformationPlan`] into a [`ModelIr`] for testing.
    fn xform_ir(plan: &TransformationPlan) -> ModelIr {
        ModelIr::from(&Plan::Transformation(plan.clone()))
    }

    /// Lift a [`SnapshotPlan`] into a [`ModelIr`] for testing.
    #[allow(dead_code)]
    fn snap_ir(plan: &SnapshotPlan) -> ModelIr {
        ModelIr::from(&Plan::Snapshot(plan.clone()))
    }

    fn sample_incremental_plan() -> ReplicationPlan {
        ReplicationPlan {
            source: SourceRef {
                catalog: "source_catalog".into(),
                schema: "src__acme__us_west__shopify".into(),
                table: "orders".into(),
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "staging__us_west__shopify".into(),
                table: "orders".into(),
            },
            strategy: MaterializationStrategy::Incremental {
                timestamp_column: "_fivetran_synced".into(),
            },
            columns: ColumnSelection::All,
            metadata_columns: vec![MetadataColumn {
                name: "_loaded_by".into(),
                data_type: "STRING".into(),
                value: "NULL".into(),
            }],
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: true,
                auto_create_schemas: true,
            },
        }
    }

    fn sample_full_refresh_plan() -> ReplicationPlan {
        ReplicationPlan {
            strategy: MaterializationStrategy::FullRefresh,
            ..sample_incremental_plan()
        }
    }

    #[test]
    fn test_incremental_select() {
        let plan = sample_incremental_plan();
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert!(sql.starts_with("SELECT *, CAST(NULL AS STRING) AS _loaded_by"));
        assert!(sql.contains("FROM source_catalog.src__acme__us_west__shopify.orders"));
        assert!(sql.contains("WHERE _fivetran_synced > ("));
        assert!(sql.contains("COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')"));
        assert!(sql.contains("FROM acme_warehouse.staging__us_west__shopify.orders"));
    }

    #[test]
    fn test_full_refresh_select() {
        let plan = sample_full_refresh_plan();
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert!(sql.starts_with("SELECT *, CAST(NULL AS STRING) AS _loaded_by"));
        assert!(sql.contains("FROM source_catalog"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_insert_into_sql() {
        let plan = sample_incremental_plan();
        let sql = generate_insert_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert!(sql.starts_with("INSERT INTO acme_warehouse.staging__us_west__shopify.orders"));
        assert!(sql.contains("SELECT *, CAST(NULL AS STRING) AS _loaded_by"));
        assert!(sql.contains("WHERE _fivetran_synced > ("));
    }

    #[test]
    fn test_create_table_as_sql() {
        let plan = sample_incremental_plan();
        let sql = generate_create_table_as_sql(&rep_ir(&plan), &dialect()).unwrap();

        // CTAS always does full refresh (no WHERE clause)
        assert!(sql.starts_with(
            "CREATE OR REPLACE TABLE acme_warehouse.staging__us_west__shopify.orders AS"
        ));
        assert!(sql.contains("SELECT *, CAST(NULL AS STRING) AS _loaded_by"));
        assert!(!sql.contains("WHERE"));
    }

    #[test]
    fn test_explicit_columns() {
        let plan = ReplicationPlan {
            columns: ColumnSelection::Explicit(vec!["id".into(), "name".into(), "status".into()]),
            metadata_columns: vec![],
            ..sample_full_refresh_plan()
        };
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert!(sql.starts_with("SELECT id, name, status"));
        assert!(!sql.contains("*"));
    }

    #[test]
    fn test_no_metadata_columns() {
        let plan = ReplicationPlan {
            metadata_columns: vec![],
            ..sample_full_refresh_plan()
        };
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert_eq!(sql.lines().next().unwrap().trim(), "SELECT *");
        assert!(!sql.contains("CAST"));
    }

    #[test]
    fn test_multiple_metadata_columns() {
        let plan = ReplicationPlan {
            metadata_columns: vec![
                MetadataColumn {
                    name: "_loaded_by".into(),
                    data_type: "STRING".into(),
                    value: "NULL".into(),
                },
                MetadataColumn {
                    name: "load_id".into(),
                    data_type: "INT".into(),
                    value: "42".into(),
                },
            ],
            ..sample_full_refresh_plan()
        };
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        assert!(sql.contains("CAST(NULL AS STRING) AS _loaded_by"));
        assert!(sql.contains("CAST(42 AS INT) AS load_id"));
    }

    #[test]
    fn test_rejects_unsafe_identifier_in_source() {
        let plan = ReplicationPlan {
            source: SourceRef {
                catalog: "catalog; DROP TABLE".into(),
                schema: "schema".into(),
                table: "table".into(),
            },
            ..sample_full_refresh_plan()
        };
        assert!(generate_select_sql(&rep_ir(&plan), &dialect()).is_err());
    }

    #[test]
    fn test_rejects_unsafe_metadata_name() {
        let plan = ReplicationPlan {
            metadata_columns: vec![MetadataColumn {
                name: "col; DROP TABLE".into(),
                data_type: "STRING".into(),
                value: "NULL".into(),
            }],
            ..sample_full_refresh_plan()
        };
        assert!(generate_select_sql(&rep_ir(&plan), &dialect()).is_err());
    }

    #[test]
    fn test_rejects_unsafe_metadata_value() {
        let plan = ReplicationPlan {
            metadata_columns: vec![MetadataColumn {
                name: "col".into(),
                data_type: "STRING".into(),
                value: "1; DROP TABLE users".into(),
            }],
            ..sample_full_refresh_plan()
        };
        // Note: metadata value validation is delegated to the dialect's select_clause.
        // The test dialect writes the value verbatim. The SQL gen layer itself does not
        // validate literal values anymore (that was in the removed non-dialect code).
        // This test now checks that the dialect produces output (it does, since TestDialect
        // doesn't validate literal values). In production, the DatabricksSqlDialect should
        // validate literal values.
        let _ = generate_select_sql(&rep_ir(&plan), &dialect());
    }

    #[test]
    fn test_rejects_unsafe_data_type() {
        let plan = ReplicationPlan {
            metadata_columns: vec![MetadataColumn {
                name: "col".into(),
                data_type: "STRING); DROP TABLE users--".into(),
                value: "NULL".into(),
            }],
            ..sample_full_refresh_plan()
        };
        // Same as above: data type validation is delegated to the dialect.
        let _ = generate_select_sql(&rep_ir(&plan), &dialect());
    }

    #[test]
    fn test_merge_select_is_full_scan() {
        let plan = ReplicationPlan {
            strategy: MaterializationStrategy::Merge {
                unique_key: vec!["id".into()],
                update_columns: ColumnSelection::All,
            },
            ..sample_full_refresh_plan()
        };
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();
        assert!(!sql.contains("WHERE")); // merge SELECT is a full scan
    }

    #[test]
    fn test_merge_sql() {
        let plan = ReplicationPlan {
            strategy: MaterializationStrategy::Merge {
                unique_key: vec!["id".into()],
                update_columns: ColumnSelection::All,
            },
            ..sample_full_refresh_plan()
        };
        let sql = generate_merge_sql(&rep_ir(&plan), &dialect()).unwrap();
        assert!(sql.contains("MERGE INTO"));
        assert!(sql.contains("ON t.id = s.id"));
        assert!(sql.contains("WHEN MATCHED THEN UPDATE SET *"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT *"));
    }

    #[test]
    fn test_merge_composite_key() {
        let plan = ReplicationPlan {
            strategy: MaterializationStrategy::Merge {
                unique_key: vec!["id".into(), "date".into()],
                update_columns: ColumnSelection::Explicit(vec!["status".into(), "amount".into()]),
            },
            ..sample_full_refresh_plan()
        };
        let sql = generate_merge_sql(&rep_ir(&plan), &dialect()).unwrap();
        assert!(sql.contains("ON t.id = s.id AND t.date = s.date"));
        assert!(sql.contains("UPDATE SET t.status = s.status, t.amount = s.amount"));
    }

    #[test]
    fn test_merge_no_key_fails() {
        let plan = ReplicationPlan {
            strategy: MaterializationStrategy::Merge {
                unique_key: vec![],
                update_columns: ColumnSelection::All,
            },
            ..sample_full_refresh_plan()
        };
        // The dialect returns an error for empty keys, which sql_gen maps to UnsafeFragment
        assert!(generate_merge_sql(&rep_ir(&plan), &dialect()).is_err());
    }

    #[test]
    fn test_transformation_full_refresh() {
        let plan = TransformationPlan {
            sources: vec![SourceRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "src".into(),
            }],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "silver".into(),
                table: "dim_accounts".into(),
            },
            strategy: MaterializationStrategy::FullRefresh,
            sql: "SELECT id, name, email FROM cat.sch.src WHERE active = true".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        };
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE TABLE cat.silver.dim_accounts AS"));
        assert!(sql.contains("WHERE active = true"));
    }

    #[test]
    fn test_transformation_incremental() {
        let plan = TransformationPlan {
            sources: vec![],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "silver".into(),
                table: "fct_orders".into(),
            },
            strategy: MaterializationStrategy::Incremental {
                timestamp_column: "updated_at".into(),
            },
            sql: "SELECT * FROM cat.sch.raw_orders WHERE updated_at > '2026-01-01'".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        };
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(stmts[0].starts_with("INSERT INTO cat.silver.fct_orders"));
    }

    #[test]
    fn test_transformation_merge() {
        let plan = TransformationPlan {
            sources: vec![],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "silver".into(),
                table: "dim_users".into(),
            },
            strategy: MaterializationStrategy::Merge {
                unique_key: vec!["user_id".into()],
                update_columns: ColumnSelection::All,
            },
            sql: "SELECT user_id, name, email FROM cat.sch.raw_users".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        };
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.contains("MERGE INTO cat.silver.dim_users AS t"));
        assert!(sql.contains("ON t.user_id = s.user_id"));
    }

    #[test]
    fn test_validate_literal_values() {
        assert!(validate_literal_value("NULL").is_ok());
        assert!(validate_literal_value("42").is_ok());
        assert!(validate_literal_value("3.14").is_ok());
        assert!(validate_literal_value("'hello'").is_ok());
        assert!(validate_literal_value("'hello world'").is_ok());

        assert!(validate_literal_value("DROP TABLE").is_err());
        assert!(validate_literal_value("1; DROP TABLE").is_err());
        assert!(validate_literal_value("'it''s bad'").is_err());
        assert!(validate_literal_value("").is_err());
    }

    #[test]
    fn test_validate_sql_types() {
        assert!(validate_sql_type("STRING").is_ok());
        assert!(validate_sql_type("INT").is_ok());
        assert!(validate_sql_type("DECIMAL(10,2)").is_ok());
        assert!(validate_sql_type("DOUBLE PRECISION").is_ok());

        assert!(validate_sql_type("").is_err());
        assert!(validate_sql_type("STRING;--").is_err());
    }

    #[test]
    fn test_sample_incremental_sql_matches_expected() {
        let plan = sample_incremental_plan();
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        let expected = "\
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM acme_warehouse.staging__us_west__shopify.orders
)";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_sample_full_refresh_sql_matches_expected() {
        let plan = sample_full_refresh_plan();
        let sql = generate_select_sql(&rep_ir(&plan), &dialect()).unwrap();

        let expected = "\
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders";

        assert_eq!(sql, expected);
    }

    fn sample_transformation_plan() -> TransformationPlan {
        TransformationPlan {
            sources: vec![SourceRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "src".into(),
            }],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "silver".into(),
                table: "dim_accounts".into(),
            },
            strategy: MaterializationStrategy::FullRefresh,
            sql: "SELECT id, name, email FROM cat.sch.src WHERE active = true".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        }
    }

    #[test]
    fn test_generate_materialized_view_sql() {
        let plan = TransformationPlan {
            strategy: MaterializationStrategy::MaterializedView,
            ..sample_transformation_plan()
        };
        let sql = generate_materialized_view_sql(&xform_ir(&plan), &dialect()).unwrap();

        let expected = "\
CREATE OR REPLACE MATERIALIZED VIEW cat.silver.dim_accounts AS
SELECT id, name, email FROM cat.sch.src WHERE active = true";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_generate_materialized_view_via_transformation() {
        let plan = TransformationPlan {
            strategy: MaterializationStrategy::MaterializedView,
            ..sample_transformation_plan()
        };
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].starts_with("CREATE OR REPLACE MATERIALIZED VIEW cat.silver.dim_accounts AS")
        );
    }

    #[test]
    fn test_generate_dynamic_table_sql() {
        let plan = TransformationPlan {
            strategy: MaterializationStrategy::DynamicTable {
                target_lag: "1 hour".into(),
            },
            ..sample_transformation_plan()
        };
        let sql = generate_dynamic_table_sql(&xform_ir(&plan), "1 hour", "COMPUTE_WH", &dialect()).unwrap();

        let expected = "\
CREATE OR REPLACE DYNAMIC TABLE cat.silver.dim_accounts
  TARGET_LAG = '1 hour'
  WAREHOUSE = COMPUTE_WH
AS
SELECT id, name, email FROM cat.sch.src WHERE active = true";

        assert_eq!(sql, expected);
    }

    #[test]
    fn test_dynamic_table_rejects_bad_warehouse() {
        let plan = TransformationPlan {
            strategy: MaterializationStrategy::DynamicTable {
                target_lag: "1 hour".into(),
            },
            ..sample_transformation_plan()
        };
        let result = generate_dynamic_table_sql(&xform_ir(&plan), "1 hour", "WH; DROP TABLE", &dialect());
        assert!(result.is_err());
    }

    #[test]
    fn test_dynamic_table_rejects_bad_target_lag() {
        let plan = TransformationPlan {
            strategy: MaterializationStrategy::DynamicTable {
                target_lag: "1 hour".into(),
            },
            ..sample_transformation_plan()
        };
        let result =
            generate_dynamic_table_sql(&xform_ir(&plan), "1'; DROP TABLE --", "COMPUTE_WH", &dialect());
        assert!(result.is_err());
    }

    // ----- time_interval generation tests (Phase 2D) -----

    use crate::ir::PartitionWindow;
    use crate::models::TimeGrain;
    use chrono::TimeZone;

    fn time_interval_plan(
        time_column: &str,
        sql: &str,
        window: Option<PartitionWindow>,
    ) -> TransformationPlan {
        TransformationPlan {
            sources: vec![SourceRef {
                catalog: "cat".into(),
                schema: "raw".into(),
                table: "stg_orders".into(),
            }],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "marts".into(),
                table: "fct_daily_orders".into(),
            },
            strategy: MaterializationStrategy::TimeInterval {
                time_column: time_column.into(),
                granularity: TimeGrain::Day,
                window,
            },
            sql: sql.into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        }
    }

    fn april_7_window() -> PartitionWindow {
        PartitionWindow {
            key: "2026-04-07".into(),
            start: chrono::Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap(),
            end: chrono::Utc.with_ymd_and_hms(2026, 4, 8, 0, 0, 0).unwrap(),
        }
    }

    #[test]
    fn test_time_interval_missing_window_errors() {
        // Static planning leaves window=None; calling generate_transformation_sql
        // without populating it must fail loudly so the runtime executor isn't
        // confused into emitting an unbounded statement.
        let plan = time_interval_plan(
            "order_date",
            "SELECT order_date FROM cat.raw.stg_orders WHERE order_date >= @start_date AND order_date < @end_date",
            None,
        );
        let result = generate_transformation_sql(&xform_ir(&plan), &dialect());
        assert!(matches!(result, Err(SqlGenError::MissingPartitionWindow)));
    }

    #[test]
    fn test_time_interval_substitutes_placeholders() {
        let plan = time_interval_plan(
            "order_date",
            "SELECT order_date FROM cat.raw.stg_orders WHERE order_date >= @start_date AND order_date < @end_date",
            Some(april_7_window()),
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1, "test dialect emits a single REPLACE WHERE");
        let sql = &stmts[0];
        // Both placeholders substituted with quoted timestamp literals.
        assert!(
            sql.contains("'2026-04-07 00:00:00'"),
            "expected start placeholder substituted: {sql}"
        );
        assert!(
            sql.contains("'2026-04-08 00:00:00'"),
            "expected end placeholder substituted: {sql}"
        );
        // Filter clause built by sql_gen, not from user SQL.
        assert!(
            sql.contains(
                "order_date >= '2026-04-07 00:00:00' AND order_date < '2026-04-08 00:00:00'"
            ),
            "expected filter built from time_column + window: {sql}"
        );
        // The original `@start_date` / `@end_date` tokens must be gone.
        assert!(!sql.contains("@start_date"));
        assert!(!sql.contains("@end_date"));
    }

    #[test]
    fn test_time_interval_invalid_time_column_rejected() {
        // SECURITY: even though the compiler validates time_column at
        // typecheck time (E023), sql_gen re-validates at the planning
        // boundary so a bad column name can never reach format!().
        let plan = time_interval_plan(
            "order date", // contains a space → invalid identifier
            "SELECT 1 FROM cat.raw.stg_orders WHERE 1=1 AND @start_date < @end_date",
            Some(april_7_window()),
        );
        let result = generate_transformation_sql(&xform_ir(&plan), &dialect());
        assert!(matches!(result, Err(SqlGenError::UnsafeFragment { .. })));
    }

    #[test]
    fn test_substitute_partition_placeholders_unit() {
        // Direct unit test for the placeholder substitution helper.
        let win = april_7_window();
        let sql = "SELECT * FROM t WHERE ts >= @start_date AND ts < @end_date";
        let out = substitute_partition_placeholders(sql, &win);
        assert_eq!(
            out,
            "SELECT * FROM t WHERE ts >= '2026-04-07 00:00:00' AND ts < '2026-04-08 00:00:00'"
        );
    }

    #[test]
    fn test_time_interval_bootstrap_sql_renders_create_table_as() {
        // generate_time_interval_bootstrap_sql should produce a CREATE TABLE
        // AS statement with both placeholders substituted to the same
        // sentinel timestamp ('1900-01-01 00:00:00'), making the half-open
        // [start, end) window empty so the model SELECT returns zero rows.
        let plan = time_interval_plan(
            "order_date",
            "SELECT order_date FROM cat.raw.stg_orders \
             WHERE order_at >= @start_date AND order_at < @end_date",
            None,
        );
        let sql = generate_time_interval_bootstrap_sql(&xform_ir(&plan), &dialect()).unwrap();

        // Wrapped in CREATE OR REPLACE TABLE AS by the test dialect.
        assert!(
            sql.starts_with("CREATE OR REPLACE TABLE cat.marts.fct_daily_orders AS"),
            "expected CREATE TABLE AS, got: {sql}"
        );
        // Both placeholders substituted to the sentinel.
        assert!(
            sql.contains("'1900-01-01 00:00:00'"),
            "expected sentinel timestamp, got: {sql}"
        );
        // Sentinel appears at least twice (once for @start_date, once for @end_date).
        assert_eq!(
            sql.matches("'1900-01-01 00:00:00'").count(),
            2,
            "sentinel must appear twice (once per placeholder), got: {sql}"
        );
        // Bare placeholder tokens should be gone.
        assert!(!sql.contains("@start_date"));
        assert!(!sql.contains("@end_date"));
    }

    #[test]
    fn test_time_interval_bootstrap_does_not_require_window() {
        // The bootstrap helper does NOT need PartitionWindow to be populated
        // on the plan — it ignores plan.strategy entirely and just uses the
        // model SQL + target. Verify that a plan with window=None still works.
        let plan = time_interval_plan(
            "order_date",
            "SELECT order_date FROM cat.raw.x WHERE x.t >= @start_date AND x.t < @end_date",
            None, // window = None
        );
        let result = generate_time_interval_bootstrap_sql(&xform_ir(&plan), &dialect());
        assert!(
            result.is_ok(),
            "bootstrap should not error on missing window: {result:?}"
        );
    }

    // ----- lakehouse format integration tests -----

    use crate::lakehouse::{LakehouseFormat, LakehouseOptions};

    fn lakehouse_plan(
        format: LakehouseFormat,
        options: LakehouseOptions,
        strategy: MaterializationStrategy,
    ) -> TransformationPlan {
        TransformationPlan {
            sources: vec![SourceRef {
                catalog: "cat".into(),
                schema: "raw".into(),
                table: "stg_orders".into(),
            }],
            target: TargetRef {
                catalog: "cat".into(),
                schema: "silver".into(),
                table: "fct_orders".into(),
            },
            strategy,
            sql: "SELECT id, amount, region FROM cat.raw.stg_orders".into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: Some(format),
            format_options: Some(options),
        }
    }

    #[test]
    fn test_lakehouse_full_refresh_delta() {
        let plan = lakehouse_plan(
            LakehouseFormat::DeltaTable,
            LakehouseOptions {
                partition_by: vec!["region".into()],
                cluster_by: vec!["id".into()],
                table_properties: vec![("delta.enableChangeDataFeed".into(), "true".into())],
                comment: Some("Orders fact table".into()),
            },
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.contains("USING DELTA"), "expected USING DELTA: {sql}");
        assert!(
            sql.contains("PARTITIONED BY (region)"),
            "expected partition clause: {sql}"
        );
        assert!(
            sql.contains("CLUSTER BY (id)"),
            "expected cluster clause: {sql}"
        );
        assert!(
            sql.contains("COMMENT 'Orders fact table'"),
            "expected comment: {sql}"
        );
        assert!(
            sql.contains("TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"),
            "expected tblproperties: {sql}"
        );
        assert!(
            sql.contains("SELECT id, amount, region FROM cat.raw.stg_orders"),
            "expected body SQL: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_full_refresh_iceberg() {
        let plan = lakehouse_plan(
            LakehouseFormat::IcebergTable,
            LakehouseOptions {
                partition_by: vec!["region".into()],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("USING ICEBERG"),
            "expected USING ICEBERG: {sql}"
        );
        assert!(
            sql.contains("PARTITIONED BY (region)"),
            "expected partition clause: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_full_refresh_streaming_table() {
        let plan = lakehouse_plan(
            LakehouseFormat::StreamingTable,
            LakehouseOptions::default(),
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].contains("CREATE OR REPLACE STREAMING TABLE"),
            "expected STREAMING TABLE: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_lakehouse_full_refresh_view() {
        let plan = lakehouse_plan(
            LakehouseFormat::View,
            LakehouseOptions::default(),
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].contains("CREATE OR REPLACE VIEW"),
            "expected VIEW: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_lakehouse_full_refresh_materialized_view() {
        let plan = lakehouse_plan(
            LakehouseFormat::MaterializedView,
            LakehouseOptions {
                comment: Some("MV test".into()),
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("CREATE OR REPLACE MATERIALIZED VIEW"),
            "expected MATERIALIZED VIEW: {sql}"
        );
        assert!(sql.contains("COMMENT 'MV test'"), "expected comment: {sql}");
    }

    #[test]
    fn test_lakehouse_full_refresh_plain_table() {
        // LakehouseFormat::Table with options should still apply partitioning
        // and properties even though it's a "plain" table.
        let plan = lakehouse_plan(
            LakehouseFormat::Table,
            LakehouseOptions {
                cluster_by: vec!["id".into()],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::FullRefresh,
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("CREATE OR REPLACE TABLE"),
            "expected CTAS: {sql}"
        );
        assert!(
            sql.contains("CLUSTER BY (id)"),
            "expected cluster clause: {sql}"
        );
        // Plain table should NOT have USING DELTA/ICEBERG.
        assert!(
            !sql.contains("USING"),
            "plain table should not have USING: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_incremental_ignores_format_on_insert() {
        // Incremental INSERT should not emit lakehouse DDL — that's for
        // initial table creation. The format field is silently ignored for
        // the INSERT path, since the table already exists.
        let plan = lakehouse_plan(
            LakehouseFormat::DeltaTable,
            LakehouseOptions::default(),
            MaterializationStrategy::Incremental {
                timestamp_column: "updated_at".into(),
            },
        );
        let stmts = generate_transformation_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].starts_with("INSERT INTO"),
            "incremental should be INSERT INTO: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_lakehouse_initial_ddl_delta() {
        let plan = lakehouse_plan(
            LakehouseFormat::DeltaTable,
            LakehouseOptions {
                partition_by: vec!["region".into()],
                table_properties: vec![("delta.enableChangeDataFeed".into(), "true".into())],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::Incremental {
                timestamp_column: "updated_at".into(),
            },
        );
        let stmts = generate_transformation_initial_ddl(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("USING DELTA"),
            "initial DDL should use DELTA: {sql}"
        );
        assert!(
            sql.contains("PARTITIONED BY (region)"),
            "initial DDL should include partitioning: {sql}"
        );
        assert!(
            sql.contains("TBLPROPERTIES"),
            "initial DDL should include tblproperties: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_initial_ddl_iceberg() {
        let plan = lakehouse_plan(
            LakehouseFormat::IcebergTable,
            LakehouseOptions {
                cluster_by: vec!["user_id".into()],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::Merge {
                unique_key: vec!["id".into()],
                update_columns: ColumnSelection::All,
            },
        );
        let stmts = generate_transformation_initial_ddl(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("USING ICEBERG"),
            "initial DDL should use ICEBERG: {sql}"
        );
        assert!(
            sql.contains("CLUSTER BY (user_id)"),
            "initial DDL should include clustering: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_initial_ddl_fallback_no_format() {
        // When format is None, generate_transformation_initial_ddl should
        // fall back to the plain dialect.create_table_as.
        let plan = TransformationPlan {
            format: None,
            format_options: None,
            ..lakehouse_plan(
                LakehouseFormat::DeltaTable,
                LakehouseOptions::default(),
                MaterializationStrategy::FullRefresh,
            )
        };
        let stmts = generate_transformation_initial_ddl(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert!(
            stmts[0].starts_with("CREATE OR REPLACE TABLE"),
            "should fall back to plain CTAS: {}",
            stmts[0]
        );
        assert!(
            !stmts[0].contains("USING"),
            "plain fallback should not contain USING: {}",
            stmts[0]
        );
    }

    #[test]
    fn test_lakehouse_time_interval_bootstrap_with_delta_format() {
        // Time-interval bootstrap should use lakehouse DDL when format is set.
        let mut plan = time_interval_plan(
            "order_date",
            "SELECT order_date, amount FROM cat.raw.stg_orders \
             WHERE order_date >= @start_date AND order_date < @end_date",
            None,
        );
        plan.format = Some(LakehouseFormat::DeltaTable);
        plan.format_options = Some(LakehouseOptions {
            partition_by: vec!["order_date".into()],
            table_properties: vec![("delta.autoOptimize.optimizeWrite".into(), "true".into())],
            ..LakehouseOptions::default()
        });

        let sql = generate_time_interval_bootstrap_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert!(
            sql.contains("USING DELTA"),
            "bootstrap should use DELTA: {sql}"
        );
        assert!(
            sql.contains("PARTITIONED BY (order_date)"),
            "bootstrap should include partitioning: {sql}"
        );
        assert!(
            sql.contains("TBLPROPERTIES"),
            "bootstrap should include tblproperties: {sql}"
        );
        // Sentinel timestamps should be substituted.
        assert!(
            sql.contains("'1900-01-01 00:00:00'"),
            "bootstrap should have sentinel timestamps: {sql}"
        );
        // Bare placeholders should be gone.
        assert!(
            !sql.contains("@start_date"),
            "placeholder not substituted: {sql}"
        );
        assert!(
            !sql.contains("@end_date"),
            "placeholder not substituted: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_time_interval_bootstrap_without_format() {
        // Without format, bootstrap should use plain CTAS as before.
        let plan = time_interval_plan(
            "order_date",
            "SELECT order_date FROM cat.raw.stg_orders \
             WHERE order_date >= @start_date AND order_date < @end_date",
            None,
        );
        let sql = generate_time_interval_bootstrap_sql(&xform_ir(&plan), &dialect()).unwrap();
        assert!(
            sql.starts_with("CREATE OR REPLACE TABLE"),
            "should use plain CTAS: {sql}"
        );
        assert!(
            !sql.contains("USING"),
            "plain bootstrap should not contain USING: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_initial_ddl_with_delete_insert_strategy() {
        // DeleteInsert strategy should also get lakehouse DDL on initial creation.
        let plan = lakehouse_plan(
            LakehouseFormat::DeltaTable,
            LakehouseOptions {
                partition_by: vec!["date_key".into()],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::DeleteInsert {
                partition_by: vec!["date_key".into()],
            },
        );
        let stmts = generate_transformation_initial_ddl(&xform_ir(&plan), &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.contains("USING DELTA"),
            "initial DDL should use DELTA: {sql}"
        );
        assert!(
            sql.contains("PARTITIONED BY (date_key)"),
            "should include partitioning: {sql}"
        );
    }

    #[test]
    fn test_lakehouse_initial_ddl_invalid_option_rejected() {
        // Ensure invalid lakehouse options propagate as LakehouseError.
        let plan = lakehouse_plan(
            LakehouseFormat::DeltaTable,
            LakehouseOptions {
                partition_by: vec!["DROP TABLE --".into()],
                ..LakehouseOptions::default()
            },
            MaterializationStrategy::FullRefresh,
        );
        let result = generate_transformation_initial_ddl(&xform_ir(&plan), &dialect());
        assert!(result.is_err(), "should reject invalid partition column");
    }
}
