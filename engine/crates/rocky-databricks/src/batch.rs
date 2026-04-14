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
    pub columns: Vec<rocky_core::ir::ColumnInfo>,
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
) -> Result<std::collections::HashMap<String, Vec<rocky_core::ir::ColumnInfo>>, BatchError> {
    let sql = generate_batch_describe_sql(catalog, schema)?;
    let result = connector.execute_sql(&sql).await?;

    let mut map: std::collections::HashMap<String, Vec<rocky_core::ir::ColumnInfo>> =
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

        map.entry(table)
            .or_default()
            .push(rocky_core::ir::ColumnInfo {
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
            let count = row
                .get(3)
                .and_then(|v| {
                    v.as_str()
                        .and_then(|s| s.parse::<u64>().ok())
                        .or_else(|| v.as_u64())
                })
                .unwrap_or(0);

            results.push(RowCountResult {
                catalog,
                schema,
                table,
                count,
            });
        }
    }

    Ok(results)
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
