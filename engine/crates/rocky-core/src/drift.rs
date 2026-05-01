use chrono::{DateTime, Duration, Utc};
use tracing::warn;

use crate::column_map;
use crate::ir::{ColumnInfo, DriftAction, DriftResult, DriftedColumn, GracePeriodColumn, TableRef};
use crate::sql_gen::SqlGenError;
use crate::state::GracePeriodRecord;
use crate::traits::SqlDialect;

/// Compares source and target column types to detect schema drift.
///
/// Three categories surface from a single pass over the source columns:
///
/// - **Type mismatches** on an existing column populate `drifted_columns`
///   and drive the [`DriftAction`] (`AlterColumnTypes` when every change
///   is a safe widening, `DropAndRecreate` otherwise).
/// - **Added columns** (present in the source, missing from the target)
///   populate `added_columns`. The runtime issues `ALTER TABLE ADD
///   COLUMN` for each before the incremental INSERT — without that
///   step, a `SELECT * FROM source` produces more columns than the
///   target accepts, and BigQuery / Snowflake / Databricks all reject
///   the INSERT.
///
/// `DriftAction` reflects only the type-change side of drift; added
/// columns are an additional ALTER step the runtime applies regardless
/// of `action` (and even when `action == Ignore`).
pub fn detect_drift(
    table: &TableRef,
    source_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
    dialect: &dyn SqlDialect,
) -> DriftResult {
    let target_map = column_map::build_column_map(target_columns);

    let mut drifted_columns = Vec::new();
    let mut added_columns = Vec::new();

    for source_col in source_columns {
        // §P1.9: look up via CiStr borrow — no allocation per column.
        match target_map.get(column_map::CiStr::new(&source_col.name)) {
            Some(target_col) => {
                if source_col.data_type.to_lowercase() != target_col.data_type.to_lowercase() {
                    drifted_columns.push(DriftedColumn {
                        name: source_col.name.clone(),
                        source_type: source_col.data_type.clone(),
                        target_type: target_col.data_type.clone(),
                    });
                }
            }
            None => {
                added_columns.push(source_col.clone());
            }
        }
    }

    let action = if drifted_columns.is_empty() {
        DriftAction::Ignore
    } else if drifted_columns
        .iter()
        .all(|d| dialect.is_safe_type_widening(&d.source_type, &d.target_type))
    {
        DriftAction::AlterColumnTypes
    } else {
        DriftAction::DropAndRecreate
    };

    DriftResult {
        table: table.clone(),
        drifted_columns,
        action,
        added_columns,
        grace_period_columns: Vec::new(),
        columns_to_drop: Vec::new(),
    }
}

/// Generates DESCRIBE TABLE SQL using the given dialect.
pub fn generate_describe_table_sql(
    table: &TableRef,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    Ok(dialect.describe_table_sql(&ref_str))
}

/// Generates DROP TABLE SQL using the given dialect.
pub fn generate_drop_table_sql(
    table: &TableRef,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;
    Ok(dialect.drop_table_sql(&ref_str))
}

/// Default body of [`SqlDialect::is_safe_type_widening`] — Databricks /
/// Spark / DuckDB / Snowflake share enough type-name conventions that
/// a single allowlist covers them.
///
/// Dialects with different type spellings (BigQuery `INT64` /
/// `NUMERIC` / `FLOAT64` / `BIGNUMERIC`) override
/// [`SqlDialect::is_safe_type_widening`] in their dialect impl rather
/// than bolting names onto this list — keeping the global default
/// dialect-neutral (no BQ types here) avoids the asymmetry of having
/// e.g. both `BIGINT → STRING` and `INT64 → STRING` for what is
/// semantically the same conversion.
pub(crate) fn default_is_safe_type_widening(source_type: &str, target_type: &str) -> bool {
    let src = source_type.to_uppercase();
    let tgt = target_type.to_uppercase();

    matches!(
        (tgt.as_str(), src.as_str()),
        // Integer widening
        ("INT", "BIGINT")
            | ("INTEGER", "BIGINT")
            | ("SMALLINT", "INT")
            | ("SMALLINT", "INTEGER")
            | ("SMALLINT", "BIGINT")
            | ("TINYINT", "SMALLINT")
            | ("TINYINT", "INT")
            | ("TINYINT", "INTEGER")
            | ("TINYINT", "BIGINT")
            | ("INT", "LONG")
            | ("INTEGER", "LONG")
            // Float widening
            | ("FLOAT", "DOUBLE")
            // Any numeric to STRING (always safe, just changes representation)
            | ("INT", "STRING")
            | ("INTEGER", "STRING")
            | ("BIGINT", "STRING")
            | ("LONG", "STRING")
            | ("FLOAT", "STRING")
            | ("DOUBLE", "STRING")
            | ("DECIMAL", "STRING")
            | ("BOOLEAN", "STRING"),
    ) || is_safe_decimal_widening(&tgt, &src)
        || is_safe_varchar_widening(&tgt, &src)
}

/// Checks DECIMAL(p1,s) -> DECIMAL(p2,s) where p2 >= p1 (precision widening).
pub(crate) fn is_safe_decimal_widening(target: &str, source: &str) -> bool {
    fn parse_decimal(t: &str) -> Option<(u32, u32)> {
        let t = t.trim();
        if !t.starts_with("DECIMAL(") || !t.ends_with(')') {
            return None;
        }
        let inner = &t[8..t.len() - 1];
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            return None;
        }
        let p = parts[0].trim().parse::<u32>().ok()?;
        let s = parts[1].trim().parse::<u32>().ok()?;
        Some((p, s))
    }

    if let (Some((tp, ts)), Some((sp, ss))) = (parse_decimal(target), parse_decimal(source)) {
        sp >= tp && ss == ts // precision can widen, scale must match
    } else {
        false
    }
}

/// Checks VARCHAR(n) -> VARCHAR(m) where m >= n.
pub(crate) fn is_safe_varchar_widening(target: &str, source: &str) -> bool {
    fn parse_varchar(t: &str) -> Option<u32> {
        let t = t.trim();
        if !t.starts_with("VARCHAR(") || !t.ends_with(')') {
            return None;
        }
        t[8..t.len() - 1].trim().parse::<u32>().ok()
    }

    if let (Some(tn), Some(sn)) = (parse_varchar(target), parse_varchar(source)) {
        sn >= tn
    } else {
        false
    }
}

/// Generates ALTER TABLE SQL for safe type changes via the dialect's
/// `alter_column_type_sql` (which validates identifier + type and emits
/// the dialect-specific syntax). Most dialects use the default ANSI
/// `ALTER COLUMN x TYPE y` form; BigQuery overrides to its
/// `ALTER COLUMN x SET DATA TYPE y` form.
pub fn generate_alter_column_sql(
    table: &TableRef,
    drifted_columns: &[DriftedColumn],
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;

    let mut statements = Vec::new();
    for col in drifted_columns {
        statements.push(dialect.alter_column_type_sql(&table_ref, &col.name, &col.source_type)?);
    }
    Ok(statements)
}

/// Generates `ALTER TABLE ... ADD COLUMN` SQL for columns present in
/// the source but missing from the target.
///
/// Standard SQL across BigQuery / Snowflake / Databricks / DuckDB; no
/// dialect override needed today. Each new column is added as
/// nullable (the syntax `<name> <type>` defaults to nullable on every
/// supported dialect), which matches the runtime's
/// `INSERT INTO target SELECT * FROM source` semantic — backfill is
/// the responsibility of the next replication run.
pub fn generate_add_column_sql(
    table: &TableRef,
    added_columns: &[ColumnInfo],
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;

    let mut statements = Vec::new();
    for col in added_columns {
        rocky_sql::validation::validate_identifier(&col.name)?;
        crate::sql_gen::validate_sql_type(&col.data_type)?;
        statements.push(format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            table_ref, col.name, col.data_type
        ));
    }
    Ok(statements)
}

/// Detects columns that exist in the target but have been dropped from the
/// source, and evaluates them against the grace-period policy.
///
/// This function is pure — it takes existing grace-period records as input
/// and returns three categories of columns:
///
/// - **New drops** (`new_records`): columns not yet tracked that need a
///   `GracePeriodRecord` created in the state store.
/// - **In grace period** (`in_grace_period`): columns whose grace period has
///   not yet expired.
/// - **Expired** (`expired`): columns whose grace period has elapsed and
///   should be dropped from the target table.
///
/// Columns that existed in the grace-period records but now reappear in the
/// source are returned in `reappeared` so the caller can clean up the state.
pub fn detect_column_drops(
    source_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
    grace_period_days: u32,
    existing_records: &[GracePeriodRecord],
    now: DateTime<Utc>,
) -> ColumnDropResult {
    let source_set: std::collections::HashSet<String> = source_columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .collect();

    let record_map: std::collections::HashMap<String, &GracePeriodRecord> = existing_records
        .iter()
        .map(|r| (r.column_name.to_lowercase(), r))
        .collect();

    let mut new_records = Vec::new();
    let mut in_grace_period = Vec::new();
    let mut expired = Vec::new();

    for target_col in target_columns {
        let col_lower = target_col.name.to_lowercase();
        if source_set.contains(&col_lower) {
            continue;
        }

        // Column is in the target but not in the source — it's been dropped.
        if let Some(record) = record_map.get(&col_lower) {
            if now >= record.expires_at {
                expired.push(record.column_name.clone());
            } else {
                let days_remaining = (record.expires_at - now).num_days().max(0) as u32;
                warn!(
                    column = %record.column_name,
                    expires_at = %record.expires_at,
                    days_remaining,
                    "column in grace period (NULL-filled), will be dropped in {days_remaining} day(s)"
                );
                in_grace_period.push(GracePeriodColumn {
                    name: record.column_name.clone(),
                    data_type: record.data_type.clone(),
                    first_seen_at: record.first_seen_at,
                    expires_at: record.expires_at,
                    days_remaining,
                });
            }
        } else {
            // New drop — no existing record.
            let first_seen_at = now;
            let expires_at = now + Duration::days(i64::from(grace_period_days));
            let days_remaining = grace_period_days;
            warn!(
                column = %target_col.name,
                grace_period_days,
                expires_at = %expires_at,
                "column dropped from source, entering {grace_period_days}-day grace period (NULL-filled)"
            );
            new_records.push(GracePeriodRecord {
                table_key: String::new(), // caller fills this in
                column_name: target_col.name.clone(),
                data_type: target_col.data_type.clone(),
                first_seen_at,
                expires_at,
            });
            in_grace_period.push(GracePeriodColumn {
                name: target_col.name.clone(),
                data_type: target_col.data_type.clone(),
                first_seen_at,
                expires_at,
                days_remaining,
            });
        }
    }

    // Find columns that were in the grace-period records but have reappeared
    // in the source (the source added the column back).
    let reappeared: Vec<String> = existing_records
        .iter()
        .filter(|r| source_set.contains(&r.column_name.to_lowercase()))
        .map(|r| r.column_name.clone())
        .collect();

    ColumnDropResult {
        new_records,
        in_grace_period,
        expired,
        reappeared,
    }
}

/// Result of [`detect_column_drops`].
#[derive(Debug, Clone)]
pub struct ColumnDropResult {
    /// Columns newly detected as dropped — need a `GracePeriodRecord` in state.
    pub new_records: Vec<GracePeriodRecord>,
    /// Columns currently in their grace period (NULL-filled).
    pub in_grace_period: Vec<GracePeriodColumn>,
    /// Column names whose grace period has expired — ready to be dropped.
    pub expired: Vec<String>,
    /// Column names that reappeared in the source — their grace-period
    /// records should be removed from the state store.
    pub reappeared: Vec<String>,
}

/// Generates `ALTER TABLE ... DROP COLUMN` SQL for columns whose grace
/// period has expired.
pub fn generate_drop_column_sql(
    table: &TableRef,
    columns: &[String],
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let table_ref = dialect.format_table_ref(&table.catalog, &table.schema, &table.table)?;

    let mut statements = Vec::new();
    for col_name in columns {
        rocky_sql::validation::validate_identifier(col_name)?;
        statements.push(format!(
            "ALTER TABLE {} DROP COLUMN {}",
            table_ref, col_name
        ));
    }
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ColumnSelection;
    use crate::ir::MetadataColumn;
    use crate::traits::{AdapterError, AdapterResult, SqlDialect};

    /// Test dialect that mirrors Databricks behavior for rocky-core tests.
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
            _target: &str,
            _source_sql: &str,
            _keys: &[std::sync::Arc<str>],
            _update_cols: &ColumnSelection,
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn select_clause(
            &self,
            _columns: &ColumnSelection,
            _metadata: &[MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _target_ref: &str,
        ) -> AdapterResult<String> {
            unimplemented!()
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE TABLE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }

        fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
            None
        }

        fn create_schema_sql(
            &self,
            _catalog: &str,
            _schema: &str,
        ) -> Option<AdapterResult<String>> {
            None
        }

        fn tablesample_clause(&self, _percent: u32) -> Option<String> {
            None
        }

        fn insert_overwrite_partition(
            &self,
            _target: &str,
            _partition_filter: &str,
            _select_sql: &str,
        ) -> AdapterResult<Vec<String>> {
            unimplemented!()
        }
    }

    fn dialect() -> TestDialect {
        TestDialect
    }

    fn table_ref() -> TableRef {
        TableRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west__shopify".into(),
            table: "orders".into(),
        }
    }

    fn col(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
        }
    }

    #[test]
    fn test_no_drift() {
        let source = vec![col("id", "INT"), col("name", "STRING")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_type_mismatch() {
        let source = vec![col("id", "INT"), col("status", "INT")];
        let target = vec![col("id", "INT"), col("status", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        assert_eq!(result.drifted_columns.len(), 1);
        assert_eq!(result.drifted_columns[0].name, "status");
        assert_eq!(result.drifted_columns[0].source_type, "INT");
        assert_eq!(result.drifted_columns[0].target_type, "STRING");
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_multiple_mismatches() {
        let source = vec![
            col("id", "BIGINT"),
            col("amount", "DOUBLE"),
            col("name", "STRING"),
        ];
        let target = vec![
            col("id", "INT"),
            col("amount", "STRING"),
            col("name", "STRING"),
        ];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        assert_eq!(result.drifted_columns.len(), 2);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_case_insensitive_column_names() {
        let source = vec![col("ID", "INT"), col("Name", "STRING")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_case_insensitive_types() {
        let source = vec![col("id", "int"), col("name", "string")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_new_column_in_source_populates_added_columns() {
        let source = vec![
            col("id", "INT"),
            col("name", "STRING"),
            col("new_col", "STRING"),
        ];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        // No type drift, so action stays `Ignore` — but the added
        // column is captured separately so the runtime can ALTER it
        // before the next INSERT.
        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
        assert_eq!(result.added_columns.len(), 1);
        assert_eq!(result.added_columns[0].name, "new_col");
        assert_eq!(result.added_columns[0].data_type, "STRING");
    }

    #[test]
    fn test_added_columns_alongside_type_drift() {
        let source = vec![
            col("id", "BIGINT"),
            col("name", "STRING"),
            col("new_col", "STRING"),
        ];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        // INT→BIGINT is safe widening, so action = AlterColumnTypes.
        // The new column is captured independently.
        assert_eq!(result.drifted_columns.len(), 1);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
        assert_eq!(result.added_columns.len(), 1);
        assert_eq!(result.added_columns[0].name, "new_col");
    }

    #[test]
    fn test_generate_add_column_sql() {
        let table = table_ref();
        let added = vec![ColumnInfo {
            name: "region".into(),
            data_type: "STRING".into(),
            nullable: true,
        }];
        let stmts = generate_add_column_sql(&table, &added, &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "ALTER TABLE acme_warehouse.staging__us_west__shopify.orders ADD COLUMN region STRING"
        );
    }

    #[test]
    fn test_generate_add_column_rejects_bad_identifier() {
        let table = table_ref();
        let added = vec![ColumnInfo {
            name: "bad; DROP".into(),
            data_type: "STRING".into(),
            nullable: true,
        }];
        assert!(generate_add_column_sql(&table, &added, &dialect()).is_err());
    }

    #[test]
    fn test_generate_add_column_rejects_bad_type() {
        let table = table_ref();
        let added = vec![ColumnInfo {
            name: "region".into(),
            data_type: "STRING'; DROP".into(),
            nullable: true,
        }];
        assert!(generate_add_column_sql(&table, &added, &dialect()).is_err());
    }

    #[test]
    fn test_column_removed_from_source_not_drift() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());

        // Columns only in target are not checked (source drives drift detection)
        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_empty_columns() {
        let result = detect_drift(&table_ref(), &[], &[], &dialect());
        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_describe_table_sql() {
        let sql = generate_describe_table_sql(&table_ref(), &dialect()).unwrap();
        assert_eq!(
            sql,
            "DESCRIBE TABLE acme_warehouse.staging__us_west__shopify.orders"
        );
    }

    #[test]
    fn test_drop_table_sql() {
        let sql = generate_drop_table_sql(&table_ref(), &dialect()).unwrap();
        assert_eq!(
            sql,
            "DROP TABLE IF EXISTS acme_warehouse.staging__us_west__shopify.orders"
        );
    }

    #[test]
    fn test_describe_rejects_bad_identifier() {
        let bad = TableRef {
            catalog: "bad; DROP".into(),
            schema: "schema".into(),
            table: "table".into(),
        };
        assert!(generate_describe_table_sql(&bad, &dialect()).is_err());
    }

    // --- Schema evolution (ALTER TABLE) tests ---

    #[test]
    fn test_safe_int_to_bigint() {
        let source = vec![col("id", "BIGINT"), col("name", "STRING")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.drifted_columns.len(), 1);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_safe_float_to_double() {
        let source = vec![col("amount", "DOUBLE")];
        let target = vec![col("amount", "FLOAT")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_safe_decimal_widening() {
        let source = vec![col("price", "DECIMAL(18,2)")];
        let target = vec![col("price", "DECIMAL(10,2)")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_unsafe_decimal_scale_change() {
        let source = vec![col("price", "DECIMAL(10,4)")];
        let target = vec![col("price", "DECIMAL(10,2)")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_safe_varchar_widening() {
        let source = vec![col("code", "VARCHAR(200)")];
        let target = vec![col("code", "VARCHAR(100)")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_unsafe_varchar_narrowing() {
        let source = vec![col("code", "VARCHAR(50)")];
        let target = vec![col("code", "VARCHAR(100)")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_mixed_safe_and_unsafe() {
        // One safe (INT->BIGINT) + one unsafe (STRING->INT) = DropAndRecreate
        let source = vec![col("id", "BIGINT"), col("status", "INT")];
        let target = vec![col("id", "INT"), col("status", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.drifted_columns.len(), 2);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_int_to_string_safe() {
        let source = vec![col("code", "STRING")];
        let target = vec![col("code", "INT")];
        let result = detect_drift(&table_ref(), &source, &target, &dialect());
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_generate_alter_column_sql() {
        let table = table_ref();
        let drifted = vec![DriftedColumn {
            name: "id".into(),
            source_type: "BIGINT".into(),
            target_type: "INT".into(),
        }];
        let stmts = generate_alter_column_sql(&table, &drifted, &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "ALTER TABLE acme_warehouse.staging__us_west__shopify.orders ALTER COLUMN id TYPE BIGINT"
        );
    }

    #[test]
    fn test_generate_alter_rejects_bad_column() {
        let table = table_ref();
        let drifted = vec![DriftedColumn {
            name: "bad; DROP".into(),
            source_type: "BIGINT".into(),
            target_type: "INT".into(),
        }];
        assert!(generate_alter_column_sql(&table, &drifted, &dialect()).is_err());
    }

    // --- Grace-period column drop tests ---

    #[test]
    fn test_detect_new_column_drop() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let now = Utc::now();

        let result = detect_column_drops(&source, &target, 7, &[], now);

        assert_eq!(result.new_records.len(), 1);
        assert_eq!(result.new_records[0].column_name, "old_col");
        assert_eq!(result.new_records[0].data_type, "STRING");
        assert_eq!(result.in_grace_period.len(), 1);
        assert_eq!(result.in_grace_period[0].name, "old_col");
        assert_eq!(result.in_grace_period[0].days_remaining, 7);
        assert!(result.expired.is_empty());
        assert!(result.reappeared.is_empty());
    }

    #[test]
    fn test_column_within_grace_period() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let now = Utc::now();
        let first_seen = now - Duration::days(3);
        let expires = first_seen + Duration::days(7);

        let existing = vec![GracePeriodRecord {
            table_key: "cat.sch.tbl".into(),
            column_name: "old_col".into(),
            data_type: "STRING".into(),
            first_seen_at: first_seen,
            expires_at: expires,
        }];

        let result = detect_column_drops(&source, &target, 7, &existing, now);

        assert!(result.new_records.is_empty());
        assert_eq!(result.in_grace_period.len(), 1);
        assert_eq!(result.in_grace_period[0].name, "old_col");
        assert_eq!(result.in_grace_period[0].days_remaining, 4);
        assert!(result.expired.is_empty());
        assert!(result.reappeared.is_empty());
    }

    #[test]
    fn test_column_past_grace_period() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let now = Utc::now();
        let first_seen = now - Duration::days(10);
        let expires = first_seen + Duration::days(7); // expired 3 days ago

        let existing = vec![GracePeriodRecord {
            table_key: "cat.sch.tbl".into(),
            column_name: "old_col".into(),
            data_type: "STRING".into(),
            first_seen_at: first_seen,
            expires_at: expires,
        }];

        let result = detect_column_drops(&source, &target, 7, &existing, now);

        assert!(result.new_records.is_empty());
        assert!(result.in_grace_period.is_empty());
        assert_eq!(result.expired.len(), 1);
        assert_eq!(result.expired[0], "old_col");
        assert!(result.reappeared.is_empty());
    }

    #[test]
    fn test_column_reappears_in_source() {
        let source = vec![col("id", "INT"), col("old_col", "STRING")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let now = Utc::now();
        let first_seen = now - Duration::days(2);
        let expires = first_seen + Duration::days(7);

        let existing = vec![GracePeriodRecord {
            table_key: "cat.sch.tbl".into(),
            column_name: "old_col".into(),
            data_type: "STRING".into(),
            first_seen_at: first_seen,
            expires_at: expires,
        }];

        let result = detect_column_drops(&source, &target, 7, &existing, now);

        // Column is back in source — not a drop at all.
        assert!(result.new_records.is_empty());
        assert!(result.in_grace_period.is_empty());
        assert!(result.expired.is_empty());
        assert_eq!(result.reappeared.len(), 1);
        assert_eq!(result.reappeared[0], "old_col");
    }

    #[test]
    fn test_zero_grace_period_drops_immediately() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let now = Utc::now();

        let result = detect_column_drops(&source, &target, 0, &[], now);

        // With 0-day grace period, column enters grace period but expires
        // immediately on the next run. On the first detection it is still
        // "new" so it gets a record, but the caller should detect expiry
        // immediately because `expires_at == first_seen_at`.
        assert_eq!(result.new_records.len(), 1);
        assert_eq!(result.in_grace_period.len(), 1);
        assert_eq!(result.in_grace_period[0].days_remaining, 0);
    }

    #[test]
    fn test_no_drops_detected() {
        let source = vec![col("id", "INT"), col("name", "STRING")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let now = Utc::now();

        let result = detect_column_drops(&source, &target, 7, &[], now);

        assert!(result.new_records.is_empty());
        assert!(result.in_grace_period.is_empty());
        assert!(result.expired.is_empty());
        assert!(result.reappeared.is_empty());
    }

    #[test]
    fn test_multiple_drops_mixed_state() {
        let source = vec![col("id", "INT")];
        let target = vec![
            col("id", "INT"),
            col("new_drop", "STRING"),
            col("in_grace", "INT"),
            col("expired_col", "BOOLEAN"),
        ];
        let now = Utc::now();

        let existing = vec![
            GracePeriodRecord {
                table_key: "cat.sch.tbl".into(),
                column_name: "in_grace".into(),
                data_type: "INT".into(),
                first_seen_at: now - Duration::days(3),
                expires_at: now + Duration::days(4),
            },
            GracePeriodRecord {
                table_key: "cat.sch.tbl".into(),
                column_name: "expired_col".into(),
                data_type: "BOOLEAN".into(),
                first_seen_at: now - Duration::days(10),
                expires_at: now - Duration::days(3),
            },
        ];

        let result = detect_column_drops(&source, &target, 7, &existing, now);

        assert_eq!(result.new_records.len(), 1);
        assert_eq!(result.new_records[0].column_name, "new_drop");

        assert_eq!(result.in_grace_period.len(), 2); // new_drop + in_grace
        let names: Vec<&str> = result
            .in_grace_period
            .iter()
            .map(|g| g.name.as_str())
            .collect();
        assert!(names.contains(&"new_drop"));
        assert!(names.contains(&"in_grace"));

        assert_eq!(result.expired.len(), 1);
        assert_eq!(result.expired[0], "expired_col");

        assert!(result.reappeared.is_empty());
    }

    #[test]
    fn test_generate_drop_column_sql() {
        let table = table_ref();
        let columns = vec!["old_col".to_string()];
        let stmts = generate_drop_column_sql(&table, &columns, &dialect()).unwrap();
        assert_eq!(stmts.len(), 1);
        assert_eq!(
            stmts[0],
            "ALTER TABLE acme_warehouse.staging__us_west__shopify.orders DROP COLUMN old_col"
        );
    }

    #[test]
    fn test_generate_drop_column_rejects_bad_name() {
        let table = table_ref();
        let columns = vec!["bad; DROP".to_string()];
        assert!(generate_drop_column_sql(&table, &columns, &dialect()).is_err());
    }
}
