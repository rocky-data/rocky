use std::fmt::Write;

use rocky_sql::validation;
use thiserror::Error;

use crate::connector::{ConnectorError, DatabricksConnector, QueryResult};

#[derive(Debug, Error)]
pub enum BatchError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),
}

/// Table reference for batch operations.
#[derive(Debug, Clone)]
pub struct BatchTableRef {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

/// Result of a batched row count query.
#[derive(Debug, Clone)]
pub struct RowCountResult {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub count: u64,
}

/// Default batch size for UNION ALL queries. Higher values reduce round-trips
/// but increase per-query complexity.
pub const DEFAULT_BATCH_SIZE: usize = 200;

/// Generates a batched row count query using UNION ALL.
///
/// ```sql
/// SELECT 'cat' AS c, 'sch' AS s, 'tbl' AS t, COUNT(*) AS cnt FROM cat.sch.tbl
/// UNION ALL
/// SELECT 'cat' AS c, 'sch' AS s, 'tbl2' AS t, COUNT(*) AS cnt FROM cat.sch.tbl2
/// ```
pub fn generate_batch_row_count_sql(tables: &[BatchTableRef]) -> Result<String, BatchError> {
    let mut sql = String::new();

    for (i, table) in tables.iter().enumerate() {
        validation::validate_identifier(&table.catalog)?;
        validation::validate_identifier(&table.schema)?;
        validation::validate_identifier(&table.table)?;

        if i > 0 {
            let _ = write!(sql, "\nUNION ALL\n");
        }

        let _ = write!(
            sql,
            "SELECT '{catalog}' AS c, '{schema}' AS s, '{table}' AS t, COUNT(*) AS cnt FROM {catalog}.{schema}.{table}",
            catalog = table.catalog,
            schema = table.schema,
            table = table.table,
        );
    }

    Ok(sql)
}

/// Generates a batched column introspection query.
///
/// ```sql
/// SELECT lower(table_schema), lower(table_name), lower(column_name)
/// FROM <catalog>.information_schema.columns
/// WHERE table_schema IN ('schema1', 'schema2')
/// ORDER BY table_schema, table_name, ordinal_position
/// ```
pub fn generate_batch_columns_sql(catalog: &str, schemas: &[String]) -> Result<String, BatchError> {
    validation::validate_identifier(catalog)?;

    let schema_list: Vec<String> = schemas
        .iter()
        .map(|s| {
            validation::validate_identifier(s)?;
            Ok(format!("'{s}'"))
        })
        .collect::<Result<Vec<_>, validation::ValidationError>>()?;

    Ok(format!(
        "SELECT lower(table_schema), lower(table_name), lower(column_name)\n\
         FROM {catalog}.information_schema.columns\n\
         WHERE table_schema IN ({})\n\
         ORDER BY table_schema, table_name, ordinal_position",
        schema_list.join(", ")
    ))
}

/// Result of a batched column describe query.
#[derive(Debug, Clone)]
pub struct ColumnDescribeResult {
    pub schema: String,
    pub table: String,
    pub columns: Vec<rocky_ir::ColumnInfo>,
}

/// Generates a batched column describe query including data types.
///
/// ```sql
/// SELECT lower(table_schema), lower(table_name), lower(column_name),
///        lower(data_type)
/// FROM <catalog>.information_schema.columns
/// WHERE table_schema = '<schema>'
/// ORDER BY table_schema, table_name, ordinal_position
/// ```
///
/// Unlike `generate_batch_columns_sql` (which only returns column names),
/// this includes `data_type` so the result can be used for drift detection.
pub fn generate_batch_describe_sql(catalog: &str, schema: &str) -> Result<String, BatchError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;

    Ok(format!(
        "SELECT lower(table_schema), lower(table_name), lower(column_name), lower(data_type)\n\
         FROM {catalog}.information_schema.columns\n\
         WHERE lower(table_schema) = lower('{schema}')\n\
         ORDER BY table_schema, table_name, ordinal_position"
    ))
}

/// Executes a batch describe for all tables in a schema, returning column
/// metadata grouped by table. Replaces N individual `DESCRIBE TABLE` calls
/// with a single `information_schema.columns` query.
pub async fn execute_batch_describe(
    connector: &DatabricksConnector,
    catalog: &str,
    schema: &str,
) -> Result<std::collections::HashMap<String, Vec<rocky_ir::ColumnInfo>>, BatchError> {
    let sql = generate_batch_describe_sql(catalog, schema)?;
    let result = connector.execute_sql(&sql).await?;

    let mut map: std::collections::HashMap<String, Vec<rocky_ir::ColumnInfo>> =
        std::collections::HashMap::new();

    for row in &result.rows {
        let table = row
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let col_name = row
            .get(2)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let data_type = row
            .get(3)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if col_name.is_empty() || col_name.starts_with('#') {
            continue;
        }

        map.entry(table).or_default().push(rocky_ir::ColumnInfo {
            name: col_name,
            data_type,
            nullable: true,
        });
    }

    Ok(map)
}

/// Executes batched row counts, splitting large sets into chunks of 200.
pub async fn execute_batch_row_counts(
    connector: &DatabricksConnector,
    tables: &[BatchTableRef],
) -> Result<Vec<RowCountResult>, BatchError> {
    let mut results = Vec::with_capacity(tables.len());

    for chunk in tables.chunks(DEFAULT_BATCH_SIZE) {
        let sql = generate_batch_row_count_sql(chunk)?;
        let query_result: QueryResult = connector.execute_sql(&sql).await?;

        results.extend(parse_row_count_rows(&query_result.rows));
    }

    Ok(results)
}

/// Parse the `(catalog, schema, table, count)` rows of a batched row-count
/// query. A row whose count cell does not read as a non-negative integer is
/// OMITTED, so the caller reports the table as not evaluated rather than as a
/// measured zero (#1926). The reason goes to the log: the trait returns only
/// results (#1928).
fn parse_row_count_rows(rows: &[Vec<serde_json::Value>]) -> Vec<RowCountResult> {
    let mut results = Vec::with_capacity(rows.len());
    for row in rows {
        let catalog = row
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let schema = row
            .get(1)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let table = row
            .get(2)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let Some(count) = rocky_core::checks::cell_as_u64(row.get(3)) else {
            // The reason lives in the log because the trait returns only
            // results — `run.rs` sees the absence, not the cause (#1926).
            tracing::warn!(
                table = format!("{catalog}.{schema}.{table}"),
                cell = ?row.get(3),
                "row count cell was not a non-negative integer — reporting the table as not evaluated"
            );
            continue;
        };

        results.push(RowCountResult {
            catalog,
            schema,
            table,
            count,
        });
    }
    results
}

/// Result of a batched freshness query.
#[derive(Debug, Clone)]
pub struct FreshnessResult {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub max_timestamp: Option<String>,
}

/// Generates a batched freshness query using UNION ALL.
///
/// ```sql
/// SELECT 'cat' AS c, 'sch' AS s, 'tbl' AS t, CAST(MAX(ts_col) AS STRING) AS max_ts FROM cat.sch.tbl
/// UNION ALL ...
/// ```
pub fn generate_batch_freshness_sql(
    tables: &[BatchTableRef],
    timestamp_column: &str,
) -> Result<String, BatchError> {
    validation::validate_identifier(timestamp_column)?;
    let mut sql = String::new();

    for (i, table) in tables.iter().enumerate() {
        validation::validate_identifier(&table.catalog)?;
        validation::validate_identifier(&table.schema)?;
        validation::validate_identifier(&table.table)?;

        if i > 0 {
            let _ = write!(sql, "\nUNION ALL\n");
        }

        let _ = write!(
            sql,
            "SELECT '{catalog}' AS c, '{schema}' AS s, '{table}' AS t, CAST(MAX({ts}) AS STRING) AS max_ts FROM {catalog}.{schema}.{table}",
            catalog = table.catalog,
            schema = table.schema,
            table = table.table,
            ts = timestamp_column,
        );
    }

    Ok(sql)
}

/// Executes batched freshness checks, splitting into chunks of 200.
pub async fn execute_batch_freshness(
    connector: &DatabricksConnector,
    tables: &[BatchTableRef],
    timestamp_column: &str,
) -> Result<Vec<FreshnessResult>, BatchError> {
    let mut results = Vec::with_capacity(tables.len());

    for chunk in tables.chunks(DEFAULT_BATCH_SIZE) {
        let sql = generate_batch_freshness_sql(chunk, timestamp_column)?;
        if sql.is_empty() {
            continue;
        }
        let query_result: QueryResult = connector.execute_sql(&sql).await?;

        for row in &query_result.rows {
            let catalog = row
                .first()
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let schema = row
                .get(1)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let table = row
                .get(2)
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let max_timestamp = row
                .get(3)
                .and_then(|v| v.as_str())
                .map(std::string::ToString::to_string);

            results.push(FreshnessResult {
                catalog,
                schema,
                table,
                max_timestamp,
            });
        }
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1926. A count cell that does not read as a non-negative integer is
    /// omitted, so `run.rs` reports the table not evaluated. It used to become
    /// `count: 0`, indistinguishable from an empty table — and when BOTH sides
    /// were unreadable, `0 == 0` passed a check that measured nothing.
    #[test]
    fn an_unreadable_count_cell_is_omitted_not_reported_as_zero() {
        use serde_json::json;

        let row = |count: serde_json::Value| vec![json!("cat"), json!("sch"), json!("tbl"), count];

        for (label, cell) in [
            ("null", json!(null)),
            ("a non-numeric string", json!("n/a")),
            ("a fraction", json!(5.5)),
            ("an out-of-range float", json!(1e30)),
            ("a bool", json!(true)),
        ] {
            let parsed = parse_row_count_rows(&[row(cell)]);
            assert!(
                parsed.is_empty(),
                "{label}: an unreadable count must not become a measured row: {parsed:?}"
            );
        }

        // A missing cell entirely.
        let parsed = parse_row_count_rows(&[vec![json!("cat"), json!("sch"), json!("tbl")]]);
        assert!(parsed.is_empty(), "missing cell: {parsed:?}");

        // The shapes that ARE counts still parse, including a real zero.
        for (label, cell, expected) in [
            ("integer", json!(7), 7u64),
            ("numeric string", json!("7"), 7),
            ("integral float", json!(7.0), 7),
            ("a real zero", json!(0), 0),
        ] {
            let parsed = parse_row_count_rows(&[row(cell)]);
            assert_eq!(parsed.len(), 1, "{label}: {parsed:?}");
            assert_eq!(parsed[0].count, expected, "{label}: {parsed:?}");
            assert_eq!(parsed[0].table, "tbl", "{label}");
        }
    }

    #[test]
    fn test_single_table_row_count() {
        let tables = vec![BatchTableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        }];
        let sql = generate_batch_row_count_sql(&tables).unwrap();
        assert_eq!(
            sql,
            "SELECT 'cat' AS c, 'sch' AS s, 'tbl' AS t, COUNT(*) AS cnt FROM cat.sch.tbl"
        );
    }

    #[test]
    fn test_multiple_tables_union_all() {
        let tables = vec![
            BatchTableRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl1".into(),
            },
            BatchTableRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl2".into(),
            },
        ];
        let sql = generate_batch_row_count_sql(&tables).unwrap();
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("tbl1"));
        assert!(sql.contains("tbl2"));
    }

    #[test]
    fn test_rejects_invalid_identifier() {
        let tables = vec![BatchTableRef {
            catalog: "bad; DROP".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        }];
        assert!(generate_batch_row_count_sql(&tables).is_err());
    }

    #[test]
    fn test_batch_columns_sql() {
        let sql = generate_batch_columns_sql("my_catalog", &["schema1".into(), "schema2".into()])
            .unwrap();
        assert!(sql.contains("FROM my_catalog.information_schema.columns"));
        assert!(sql.contains("WHERE table_schema IN ('schema1', 'schema2')"));
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_batch_columns_rejects_bad_catalog() {
        let result = generate_batch_columns_sql("bad catalog", &["schema".into()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_batch_columns_rejects_bad_schema() {
        let result = generate_batch_columns_sql("catalog", &["bad schema".into()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_tables() {
        let sql = generate_batch_row_count_sql(&[]).unwrap();
        assert!(sql.is_empty());
    }

    #[test]
    fn test_batch_freshness_single() {
        let tables = vec![BatchTableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        }];
        let sql = generate_batch_freshness_sql(&tables, "_fivetran_synced").unwrap();
        assert!(sql.contains("MAX(_fivetran_synced)"));
        assert!(sql.contains("CAST("));
        assert!(sql.contains("FROM cat.sch.tbl"));
    }

    #[test]
    fn test_batch_freshness_multiple() {
        let tables = vec![
            BatchTableRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl1".into(),
            },
            BatchTableRef {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl2".into(),
            },
        ];
        let sql = generate_batch_freshness_sql(&tables, "_synced").unwrap();
        assert!(sql.contains("UNION ALL"));
        assert!(sql.contains("tbl1"));
        assert!(sql.contains("tbl2"));
    }

    #[test]
    fn test_batch_freshness_rejects_bad_column() {
        let tables = vec![BatchTableRef {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
        }];
        assert!(generate_batch_freshness_sql(&tables, "col; DROP TABLE").is_err());
    }

    #[test]
    fn test_batch_freshness_empty() {
        let sql = generate_batch_freshness_sql(&[], "_synced").unwrap();
        assert!(sql.is_empty());
    }

    #[test]
    fn test_batch_describe_sql() {
        let sql =
            generate_batch_describe_sql("source_warehouse", "q__raw__acme__na__fb_ads").unwrap();
        assert!(sql.contains("FROM source_warehouse.information_schema.columns"));
        assert!(sql.contains("lower(data_type)"));
        assert!(sql.contains("q__raw__acme__na__fb_ads"));
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_batch_describe_rejects_bad_input() {
        assert!(generate_batch_describe_sql("bad; DROP", "schema").is_err());
        assert!(generate_batch_describe_sql("catalog", "bad; DROP").is_err());
    }
}
