use crate::column_map;
use crate::ir::{ColumnInfo, DriftAction, DriftResult, DriftedColumn, TableRef};
use crate::sql_gen::SqlGenError;
use crate::traits::SqlDialect;

/// Compares source and target column types to detect schema drift.
///
/// A type mismatch on any existing column triggers `DropAndRecreate` (current default behavior:
/// drop the target table and do a full refresh). Columns present in the source but missing
/// from the target are not considered drift — they appear naturally via `SELECT *`.
pub fn detect_drift(
    table: &TableRef,
    source_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
) -> DriftResult {
    let target_map = column_map::build_column_map(target_columns);

    let mut drifted_columns = Vec::new();

    for source_col in source_columns {
        if let Some(target_col) = target_map.get(&source_col.name.to_lowercase()) {
            if source_col.data_type.to_lowercase() != target_col.data_type.to_lowercase() {
                drifted_columns.push(DriftedColumn {
                    name: source_col.name.clone(),
                    source_type: source_col.data_type.clone(),
                    target_type: target_col.data_type.clone(),
                });
            }
        }
    }

    let action = if drifted_columns.is_empty() {
        DriftAction::Ignore
    } else if drifted_columns
        .iter()
        .all(|d| is_safe_type_widening(&d.source_type, &d.target_type))
    {
        DriftAction::AlterColumnTypes
    } else {
        DriftAction::DropAndRecreate
    };

    DriftResult {
        table: table.clone(),
        drifted_columns,
        action,
    }
}

/// Generates DESCRIBE TABLE SQL using the given dialect.
pub fn generate_describe_table_sql(
    table: &TableRef,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect
        .format_table_ref(&table.catalog, &table.schema, &table.table)
        .map_err(|e| SqlGenError::UnsafeFragment {
            value: String::new(),
            reason: e.to_string(),
        })?;
    Ok(dialect.describe_table_sql(&ref_str))
}

/// Generates DROP TABLE SQL using the given dialect.
pub fn generate_drop_table_sql(
    table: &TableRef,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let ref_str = dialect
        .format_table_ref(&table.catalog, &table.schema, &table.table)
        .map_err(|e| SqlGenError::UnsafeFragment {
            value: String::new(),
            reason: e.to_string(),
        })?;
    Ok(dialect.drop_table_sql(&ref_str))
}

/// Determines if a column type change from `target_type` to `source_type` is a safe
/// widening that can be handled via ALTER TABLE instead of DROP+recreate.
///
/// Conservative allowlist -- only well-known safe promotions. Unknown types
/// or narrowing changes fall back to DropAndRecreate.
fn is_safe_type_widening(source_type: &str, target_type: &str) -> bool {
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
fn is_safe_decimal_widening(target: &str, source: &str) -> bool {
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
fn is_safe_varchar_widening(target: &str, source: &str) -> bool {
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

/// Generates ALTER TABLE SQL for safe type changes.
pub fn generate_alter_column_sql(
    table: &TableRef,
    drifted_columns: &[DriftedColumn],
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    let table_ref = dialect
        .format_table_ref(&table.catalog, &table.schema, &table.table)
        .map_err(|e| SqlGenError::UnsafeFragment {
            value: String::new(),
            reason: e.to_string(),
        })?;

    let mut statements = Vec::new();
    for col in drifted_columns {
        // Validate column name
        rocky_sql::validation::validate_identifier(&col.name)?;
        // Validate the type string to prevent injection
        crate::sql_gen::validate_sql_type(&col.source_type)?;
        statements.push(format!(
            "ALTER TABLE {} ALTER COLUMN {} TYPE {}",
            table_ref, col.name, col.source_type
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
            _keys: &[String],
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
        let result = detect_drift(&table_ref(), &source, &target);

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_type_mismatch() {
        let source = vec![col("id", "INT"), col("status", "INT")];
        let target = vec![col("id", "INT"), col("status", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);

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
        let result = detect_drift(&table_ref(), &source, &target);

        assert_eq!(result.drifted_columns.len(), 2);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_case_insensitive_column_names() {
        let source = vec![col("ID", "INT"), col("Name", "STRING")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_case_insensitive_types() {
        let source = vec![col("id", "int"), col("name", "string")];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);

        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_new_column_in_source_not_drift() {
        let source = vec![
            col("id", "INT"),
            col("name", "STRING"),
            col("new_col", "STRING"),
        ];
        let target = vec![col("id", "INT"), col("name", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);

        // New columns in source are not drift — SELECT * picks them up
        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_column_removed_from_source_not_drift() {
        let source = vec![col("id", "INT")];
        let target = vec![col("id", "INT"), col("old_col", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);

        // Columns only in target are not checked (source drives drift detection)
        assert!(result.drifted_columns.is_empty());
        assert_eq!(result.action, DriftAction::Ignore);
    }

    #[test]
    fn test_empty_columns() {
        let result = detect_drift(&table_ref(), &[], &[]);
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
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.drifted_columns.len(), 1);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_safe_float_to_double() {
        let source = vec![col("amount", "DOUBLE")];
        let target = vec![col("amount", "FLOAT")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_safe_decimal_widening() {
        let source = vec![col("price", "DECIMAL(18,2)")];
        let target = vec![col("price", "DECIMAL(10,2)")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_unsafe_decimal_scale_change() {
        let source = vec![col("price", "DECIMAL(10,4)")];
        let target = vec![col("price", "DECIMAL(10,2)")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_safe_varchar_widening() {
        let source = vec![col("code", "VARCHAR(200)")];
        let target = vec![col("code", "VARCHAR(100)")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.action, DriftAction::AlterColumnTypes);
    }

    #[test]
    fn test_unsafe_varchar_narrowing() {
        let source = vec![col("code", "VARCHAR(50)")];
        let target = vec![col("code", "VARCHAR(100)")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_mixed_safe_and_unsafe() {
        // One safe (INT->BIGINT) + one unsafe (STRING->INT) = DropAndRecreate
        let source = vec![col("id", "BIGINT"), col("status", "INT")];
        let target = vec![col("id", "INT"), col("status", "STRING")];
        let result = detect_drift(&table_ref(), &source, &target);
        assert_eq!(result.drifted_columns.len(), 2);
        assert_eq!(result.action, DriftAction::DropAndRecreate);
    }

    #[test]
    fn test_int_to_string_safe() {
        let source = vec![col("code", "STRING")];
        let target = vec![col("code", "INT")];
        let result = detect_drift(&table_ref(), &source, &target);
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
}
