//! Batched warehouse operations for BigQuery.
//!
//! Only `batch_describe_schema` is implemented; `batch_row_counts` and
//! `batch_freshness` return "not yet implemented" so the run loop falls
//! back to per-table queries via the generic `WarehouseAdapter` path.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use rocky_core::traits::{
    AdapterError, AdapterResult, BatchCheckAdapter, FreshnessResult, RowCountResult,
    WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};

use crate::connector::BigQueryAdapter;

/// Batched check adapter for BigQuery.
///
/// Holds a shared reference to the underlying [`BigQueryAdapter`] and
/// re-uses its authenticated HTTP client + query path. The batch-describe
/// query targets \`{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS\`, which
/// BigQuery scopes to a single dataset per reference — one round trip
/// returns every table's columns in the dataset.
pub struct BigQueryBatchCheckAdapter {
    adapter: Arc<BigQueryAdapter>,
}

impl BigQueryBatchCheckAdapter {
    pub fn new(adapter: Arc<BigQueryAdapter>) -> Self {
        Self { adapter }
    }
}

/// Generate the BigQuery `INFORMATION_SCHEMA.COLUMNS` query that describes
/// every table in a single dataset in one round trip.
///
/// BigQuery requires `INFORMATION_SCHEMA` references to be fully qualified
/// by project + dataset (`` `{project}`.`{dataset}`.INFORMATION_SCHEMA.COLUMNS ``).
/// Identifiers are backtick-quoted; this function assumes callers already
/// validated them (the wiring in `registry.rs` trusts config values the
/// same way the per-table `describe_table` path does at
/// `connector.rs:183-209`).
fn generate_batch_describe_sql(catalog: &str, schema: &str) -> String {
    format!(
        "SELECT LOWER(table_name), LOWER(column_name), data_type, is_nullable \
         FROM `{catalog}`.`{schema}`.INFORMATION_SCHEMA.COLUMNS \
         ORDER BY table_name, ordinal_position"
    )
}

/// Parse the four-column `INFORMATION_SCHEMA.COLUMNS` result
/// (`table_name`, `column_name`, `data_type`, `is_nullable`) into a map of
/// table name → columns. Rows are expected pre-grouped by table + ordinal
/// (the query's `ORDER BY`); blank table/column rows are skipped.
///
/// Pure (rows in, map out) so the casing + `is_nullable` parsing can be
/// unit-tested without an HTTP round trip — the BigQuery connector has no
/// base-URL override for wiremock, unlike the Snowflake connector.
fn parse_describe_rows(rows: &[Vec<serde_json::Value>]) -> HashMap<String, Vec<ColumnInfo>> {
    let mut map: HashMap<String, Vec<ColumnInfo>> = HashMap::new();

    for row in rows {
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
        let data_type = row
            .get(2)
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
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

#[async_trait]
impl BatchCheckAdapter for BigQueryBatchCheckAdapter {
    async fn batch_row_counts(&self, _tables: &[TableRef]) -> AdapterResult<Vec<RowCountResult>> {
        Err(AdapterError::msg(
            "batch_row_counts not yet implemented for BigQuery",
        ))
    }

    async fn batch_freshness(
        &self,
        _tables: &[TableRef],
        _timestamp_col: &str,
    ) -> AdapterResult<Vec<FreshnessResult>> {
        Err(AdapterError::msg(
            "batch_freshness not yet implemented for BigQuery",
        ))
    }

    async fn batch_describe_schema(
        &self,
        catalog: &str,
        schema: &str,
    ) -> AdapterResult<HashMap<String, Vec<ColumnInfo>>> {
        let sql = generate_batch_describe_sql(catalog, schema);
        let result = self.adapter.execute_query(&sql).await?;
        Ok(parse_describe_rows(&result.rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_describe_sql_scopes_to_project_and_dataset() {
        let sql = generate_batch_describe_sql("my-project", "analytics");
        assert!(sql.contains("`my-project`.`analytics`.INFORMATION_SCHEMA.COLUMNS"));
        assert!(sql.contains("ORDER BY table_name, ordinal_position"));
    }

    #[test]
    fn batch_describe_sql_selects_nullable_column() {
        let sql = generate_batch_describe_sql("p", "s");
        assert!(sql.contains("is_nullable"));
        assert!(sql.contains("data_type"));
    }

    fn row(table: &str, col: &str, ty: &str, nullable: &str) -> Vec<serde_json::Value> {
        vec![table.into(), col.into(), ty.into(), nullable.into()]
    }

    #[test]
    fn parse_groups_columns_by_table_and_parses_nullability() {
        let rows = vec![
            row("orders", "id", "INT64", "NO"),
            row("orders", "customer", "STRING", "YES"),
            row("events", "event_id", "INT64", "YES"),
        ];
        let map = parse_describe_rows(&rows);

        assert_eq!(map.len(), 2);
        let orders = &map["orders"];
        assert_eq!(orders.len(), 2);
        assert_eq!(orders[0].name, "id");
        assert_eq!(orders[0].data_type, "INT64");
        assert!(!orders[0].nullable, "is_nullable=NO -> not nullable");
        assert_eq!(orders[1].name, "customer");
        assert!(orders[1].nullable, "is_nullable=YES -> nullable");
        assert_eq!(map["events"].len(), 1);
    }

    #[test]
    fn parse_is_nullable_is_case_insensitive() {
        let rows = vec![row("t", "c", "STRING", "yes")];
        let map = parse_describe_rows(&rows);
        assert!(map["t"][0].nullable);
    }

    #[test]
    fn parse_skips_blank_table_or_column_rows() {
        let rows = vec![
            row("", "c", "STRING", "YES"),
            row("t", "", "STRING", "YES"),
            row("t", "c", "STRING", "YES"),
        ];
        let map = parse_describe_rows(&rows);
        assert_eq!(map.len(), 1);
        assert_eq!(map["t"].len(), 1);
    }

    #[test]
    fn parse_empty_rows_yields_empty_map() {
        assert!(parse_describe_rows(&[]).is_empty());
    }
}
