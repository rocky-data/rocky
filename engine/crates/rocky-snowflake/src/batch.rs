//! Batched warehouse operations for Snowflake.
//!
//! The only operation currently implemented is `batch_describe_schema`,
//! which replaces N `DESCRIBE TABLE` calls with one
//! `INFORMATION_SCHEMA.COLUMNS` query per schema. `batch_row_counts` and
//! `batch_freshness` remain unimplemented, and this adapter declares that
//! through `supports_row_counts()` / `supports_freshness()` so callers fall
//! back to the per-table [`WarehouseAdapter`] path for those (#1719).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, FreshnessResult, RowCountResult, SqlDialect,
};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_sql::validation::{self, ValidationError};

use crate::connector::{ConnectorError, QueryResult, SnowflakeConnector};
use crate::dialect::SnowflakeSqlDialect;
use crate::types::{InformationSchemaType, canonical_type};

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
}

/// Batched check adapter for Snowflake.
pub struct SnowflakeBatchCheckAdapter {
    connector: Arc<SnowflakeConnector>,
}

impl SnowflakeBatchCheckAdapter {
    pub fn new(connector: Arc<SnowflakeConnector>) -> Self {
        Self { connector }
    }
}

/// Generates the Snowflake `INFORMATION_SCHEMA.COLUMNS` query that describes
/// every table in a single schema in one round trip.
///
/// Snowflake exposes `INFORMATION_SCHEMA.COLUMNS` per database; the query
/// scopes to `"<database>".information_schema.columns`. The database name is
/// quoted as written, the same as the writes, so a lowercase database that
/// Rocky created as `"acme"` is found (#2413). `table_schema` is matched
/// case-insensitively.
///
/// `data_type` is the bare type name. The length, precision and scale
/// columns are read too, so [`parse_batch_describe_rows`] can rebuild the
/// type `DESCRIBE TABLE` prints (#2344).
fn generate_batch_describe_sql(catalog: &str, schema: &str) -> Result<String, BatchError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    let database = SnowflakeSqlDialect.quote_identifier(catalog);

    Ok(format!(
        "SELECT LOWER(table_name), LOWER(column_name), data_type, is_nullable,\n\
         character_maximum_length, numeric_precision, numeric_scale, datetime_precision\n\
         FROM {database}.information_schema.columns\n\
         WHERE UPPER(table_schema) = UPPER('{schema}')\n\
         ORDER BY table_name, ordinal_position"
    ))
}

#[async_trait]
impl BatchCheckAdapter for SnowflakeBatchCheckAdapter {
    // Snowflake could batch both with a UNION ALL over the supplied tables,
    // but it is not wired yet. The capability says so; the methods below are
    // unreachable through it.
    //
    // These used to return `Err("not yet implemented")` as the fallback
    // signal, and the fallback the comment promised was never written. Since
    // #1655/#1700 the runner folds a failed leg into a `not_evaluated` check
    // at error severity, so every row-count and freshness check on Snowflake
    // failed on healthy data and gated the run (#1719).
    fn supports_row_counts(&self) -> bool {
        false
    }

    fn supports_freshness(&self) -> bool {
        false
    }

    async fn batch_row_counts(&self, _tables: &[TableRef]) -> AdapterResult<Vec<RowCountResult>> {
        Err(AdapterError::msg(
            "batch_row_counts is not implemented for Snowflake and \
             supports_row_counts() reports false — this call ignored the capability",
        ))
    }

    async fn batch_freshness(
        &self,
        _tables: &[TableRef],
        _timestamp_col: &str,
    ) -> AdapterResult<Vec<FreshnessResult>> {
        Err(AdapterError::msg(
            "batch_freshness is not implemented for Snowflake and \
             supports_freshness() reports false — this call ignored the capability",
        ))
    }

    async fn batch_describe_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> AdapterResult<HashMap<String, Vec<ColumnInfo>>> {
        let sql = generate_batch_describe_sql(catalog, schema).map_err(AdapterError::new)?;
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(AdapterError::new)?;

        Ok(parse_batch_describe_rows(&result))
    }
}

/// Reads an optional whole number from a result cell. The SQL API returns
/// numbers as JSON strings; a JSON number is accepted too. NULL, a missing
/// cell or anything else is `None`.
fn cell_u64(value: Option<&serde_json::Value>) -> Option<u64> {
    let value = value?;
    match value.as_str() {
        Some(s) => s.trim().parse().ok(),
        None => value.as_u64(),
    }
}

/// Groups the rows of [`generate_batch_describe_sql`] by table, with each
/// column's type spelled as `DESCRIBE TABLE` spells it.
fn parse_batch_describe_rows(result: &QueryResult) -> HashMap<String, Vec<ColumnInfo>> {
    let mut map: HashMap<String, Vec<ColumnInfo>> = HashMap::new();

    for row in &result.rows {
        let table = row
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let col_name = row
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let reported = InformationSchemaType {
            data_type: row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string(),
            character_maximum_length: cell_u64(row.get(4)),
            numeric_precision: cell_u64(row.get(5)),
            numeric_scale: cell_u64(row.get(6)),
            datetime_precision: cell_u64(row.get(7)),
        };
        // A Snowflake float is 64-bit whatever its name (#2333).
        let data_type = canonical_type(&reported.describe_type());
        // Snowflake's `is_nullable` is "YES" / "NO".
        let nullable = row
            .get(3)
            .and_then(|v| v.as_str())
            .map(|s| s.eq_ignore_ascii_case("YES"))
            .unwrap_or(true);

        if table.is_empty() || col_name.is_empty() {
            continue;
        }

        map.entry(table).or_default().push(ColumnInfo {
            name: col_name,
            data_type,
            nullable,
        });
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_describe_sql_scopes_to_database_and_schema() {
        let sql = generate_batch_describe_sql("MY_DB", "analytics").unwrap();
        assert!(sql.contains("FROM \"MY_DB\".information_schema.columns"));
        assert!(sql.contains("UPPER('analytics')"));
        assert!(sql.contains("ORDER BY table_name, ordinal_position"));
    }

    /// #2413: a lowercase database is read as written, not folded to upper.
    #[test]
    fn batch_describe_sql_quotes_a_lowercase_database_as_written() {
        let sql = generate_batch_describe_sql("acme", "marts").unwrap();
        assert!(
            sql.contains("FROM \"acme\".information_schema.columns"),
            "{sql}"
        );
    }

    fn query_result(headers: &[&str], rows: Vec<Vec<serde_json::Value>>) -> QueryResult {
        QueryResult {
            statement_handle: String::new(),
            columns: headers
                .iter()
                .map(|name| crate::connector::ColumnMetaData {
                    name: (*name).into(),
                    type_name: None,
                    nullable: None,
                })
                .collect(),
            rows,
            total_row_count: None,
        }
    }

    /// The `DESCRIBE TABLE` type of one column, through the per-table parser.
    fn describe_type(reported: &str) -> String {
        let result = query_result(
            &["name", "type", "kind", "null?"],
            vec![vec![
                serde_json::json!("c"),
                serde_json::json!(reported),
                serde_json::json!("COLUMN"),
                serde_json::json!("Y"),
            ]],
        );
        let columns = crate::adapter::parse_describe_columns(&result);
        columns[0].data_type.clone()
    }

    /// One `INFORMATION_SCHEMA.COLUMNS` row for table `t`, in the shape the
    /// SQL API returns it: numbers as JSON strings, NULL as JSON null.
    fn info_row(
        column: &str,
        data_type: &str,
        [len, precision, scale, datetime_precision]: [Option<u64>; 4],
    ) -> Vec<serde_json::Value> {
        let num = |v: Option<u64>| match v {
            Some(n) => serde_json::json!(n.to_string()),
            None => serde_json::Value::Null,
        };
        vec![
            serde_json::json!("t"),
            serde_json::json!(column),
            serde_json::json!(data_type),
            serde_json::json!("YES"),
            num(len),
            num(precision),
            num(scale),
            num(datetime_precision),
        ]
    }

    const BATCH_HEADERS: [&str; 8] = [
        "LOWER(TABLE_NAME)",
        "LOWER(COLUMN_NAME)",
        "DATA_TYPE",
        "IS_NULLABLE",
        "CHARACTER_MAXIMUM_LENGTH",
        "NUMERIC_PRECISION",
        "NUMERIC_SCALE",
        "DATETIME_PRECISION",
    ];

    /// The batch-path type of one column, through the batch row parser.
    fn batch_type(data_type: &str, extra: [Option<u64>; 4]) -> String {
        let result = query_result(&BATCH_HEADERS, vec![info_row("c", data_type, extra)]);
        let map = parse_batch_describe_rows(&result);
        map["t"][0].data_type.clone()
    }

    /// #2344: for each type family, the batch path stores exactly the type
    /// the per-table `DESCRIBE` path stores for the same column. The
    /// `DESCRIBE` strings are the ones Snowflake's data type pages show.
    #[test]
    fn batch_type_matches_describe_for_every_type_family() {
        let none = None;
        // (DESCRIBE type, DATA_TYPE, [length, precision, scale, datetime_precision])
        let cases: &[(&str, &str, [Option<u64>; 4])] = &[
            ("NUMBER(38,0)", "NUMBER", [none, Some(38), Some(0), none]),
            ("NUMBER(10,1)", "NUMBER", [none, Some(10), Some(1), none]),
            ("NUMBER(20,2)", "NUMBER", [none, Some(20), Some(2), none]),
            ("DECFLOAT(38)", "DECFLOAT", [none, Some(38), none, none]),
            ("FLOAT", "FLOAT", [none, Some(53), none, none]),
            (
                "VARCHAR(16777216)",
                "TEXT",
                [Some(16_777_216), none, none, none],
            ),
            ("VARCHAR(1)", "TEXT", [Some(1), none, none, none]),
            (
                "BINARY(8388608)",
                "BINARY",
                [Some(8_388_608), none, none, none],
            ),
            ("BINARY(100)", "BINARY", [Some(100), none, none, none]),
            ("BOOLEAN", "BOOLEAN", [none; 4]),
            ("DATE", "DATE", [none; 4]),
            ("TIME(9)", "TIME", [none, Some(0), Some(9), none]),
            (
                "TIMESTAMP_NTZ(9)",
                "TIMESTAMP_NTZ",
                [none, Some(0), Some(9), none],
            ),
            (
                "TIMESTAMP_NTZ(3)",
                "TIMESTAMP_NTZ",
                [none, none, none, Some(3)],
            ),
            (
                "TIMESTAMP_LTZ(9)",
                "TIMESTAMP_LTZ",
                [none, Some(0), Some(9), none],
            ),
            (
                "TIMESTAMP_TZ(6)",
                "TIMESTAMP_TZ",
                [none, Some(0), Some(6), none],
            ),
            ("VARIANT", "VARIANT", [none; 4]),
            ("OBJECT", "OBJECT", [none; 4]),
            ("ARRAY", "ARRAY", [none; 4]),
            ("GEOGRAPHY", "GEOGRAPHY", [none; 4]),
            ("GEOMETRY", "GEOMETRY", [none; 4]),
        ];
        for (describe, data_type, extra) in cases {
            assert_eq!(
                batch_type(data_type, *extra),
                describe_type(describe),
                "{data_type} {extra:?} vs DESCRIBE {describe}"
            );
        }
    }

    /// A NULL parameter leaves the type bare rather than guessing one, and
    /// a parameter sent as a JSON number reads the same as a string.
    #[test]
    fn batch_type_without_parameters_stays_bare() {
        assert_eq!(batch_type("NUMBER", [None, Some(38), None, None]), "NUMBER");
        assert_eq!(batch_type("TEXT", [None; 4]), "TEXT");
        assert_eq!(batch_type("TIMESTAMP_NTZ", [None; 4]), "TIMESTAMP_NTZ");

        let mut row = info_row("c", "NUMBER", [None; 4]);
        row[5] = serde_json::json!(12);
        row[6] = serde_json::json!(2);
        let map = parse_batch_describe_rows(&query_result(&BATCH_HEADERS, vec![row]));
        assert_eq!(map["t"][0].data_type, "NUMBER(12,2)");
    }

    /// #2344: drift on columns read through the batch path sees precision
    /// and length changes, and picks the in-place widening where Snowflake
    /// allows it. With bare type names both sides read `NUMBER` / `TEXT`
    /// and nothing drifts.
    #[test]
    fn batch_described_columns_drive_drift_on_precision_and_length() {
        use rocky_core::drift::detect_drift;
        use rocky_ir::DriftAction;

        let table = TableRef {
            catalog: "db".into(),
            schema: "s".into(),
            table: "t".into(),
        };
        let dialect = crate::dialect::SnowflakeSqlDialect;
        let read =
            |rows| parse_batch_describe_rows(&query_result(&BATCH_HEADERS, rows))["t"].clone();
        let number = |p| info_row("n", "NUMBER", [None, Some(p), Some(0), None]);
        let text = |n| info_row("v", "TEXT", [Some(n), None, None, None]);

        // Source NUMBER(38,0) over target NUMBER(10,0): a safe widening.
        let drift = detect_drift(
            &table,
            &read(vec![number(38)]),
            &read(vec![number(10)]),
            &dialect,
        );
        assert_eq!(drift.drifted_columns.len(), 1);
        assert_eq!(drift.drifted_columns[0].source_type, "NUMBER(38,0)");
        assert_eq!(drift.drifted_columns[0].target_type, "NUMBER(10,0)");
        assert_eq!(drift.action, DriftAction::AlterColumnTypes);

        // VARCHAR(10) -> VARCHAR(20) in the source over a VARCHAR(10) target:
        // the target widens in place.
        let drift = detect_drift(
            &table,
            &read(vec![text(20)]),
            &read(vec![text(10)]),
            &dialect,
        );
        assert_eq!(drift.drifted_columns.len(), 1);
        assert_eq!(drift.drifted_columns[0].source_type, "VARCHAR(20)");
        assert_eq!(drift.drifted_columns[0].target_type, "VARCHAR(10)");
        assert_eq!(drift.action, DriftAction::AlterColumnTypes);

        // A narrower source is drift too, but not a safe widening.
        let drift = detect_drift(
            &table,
            &read(vec![text(10)]),
            &read(vec![text(20)]),
            &dialect,
        );
        assert_eq!(drift.drifted_columns.len(), 1);
        assert_eq!(drift.action, DriftAction::DropAndRecreate);

        // The same type on both sides is no drift.
        let drift = detect_drift(
            &table,
            &read(vec![text(10)]),
            &read(vec![text(10)]),
            &dialect,
        );
        assert!(drift.drifted_columns.is_empty());
        assert_eq!(drift.action, DriftAction::Ignore);
    }

    #[test]
    fn batch_describe_sql_rejects_invalid_identifiers() {
        assert!(generate_batch_describe_sql("bad; drop table users", "s").is_err());
        assert!(generate_batch_describe_sql("c", "bad'; select *").is_err());
    }
}
