//! BigQuery SQL dialect implementation.

use rocky_core::ir::{ColumnSelection, MetadataColumn};
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_sql::validation;

/// BigQuery SQL dialect.
#[derive(Debug, Clone)]
pub struct BigQueryDialect;

impl SqlDialect for BigQueryDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // BigQuery uses project.dataset.table (three-part)
        validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        validation::validate_identifier(table).map_err(AdapterError::new)?;
        Ok(format!("`{catalog}`.`{schema}`.`{table}`"))
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
        keys: &[String],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        if keys.is_empty() {
            return Err(AdapterError::msg(
                "MERGE requires at least one unique_key column",
            ));
        }

        for key in keys {
            validation::validate_identifier(key).map_err(AdapterError::new)?;
        }

        let join_cond = keys
            .iter()
            .map(|k| format!("target.{k} = source.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let update_set = match update_cols {
            ColumnSelection::All => "target = source".to_string(),
            ColumnSelection::Explicit(cols) => cols
                .iter()
                .map(|c| format!("target.{c} = source.{c}"))
                .collect::<Vec<_>>()
                .join(", "),
        };

        Ok(format!(
            "MERGE INTO {target} AS target\n\
             USING ({source_sql}) AS source\n\
             ON {join_cond}\n\
             WHEN MATCHED THEN UPDATE SET {update_set}\n\
             WHEN NOT MATCHED THEN INSERT ROW"
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let base = match columns {
            ColumnSelection::All => "SELECT *".to_string(),
            ColumnSelection::Explicit(cols) => {
                for col in cols {
                    validation::validate_identifier(col).map_err(AdapterError::new)?;
                }
                format!("SELECT {}", cols.join(", "))
            }
        };

        if metadata.is_empty() {
            return Ok(base);
        }

        let meta_cols: Vec<String> = metadata
            .iter()
            .map(|m| format!("CAST({} AS {}) AS {}", m.value, m.data_type, m.name))
            .collect();

        Ok(format!("{base}, {}", meta_cols.join(", ")))
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        Ok(format!(
            "WHERE {timestamp_col} > (\n  \
             SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01')\n  \
             FROM {target_ref}\n)"
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        // BigQuery doesn't have DESCRIBE; use INFORMATION_SCHEMA
        format!(
            "SELECT column_name, data_type, is_nullable \
             FROM `INFORMATION_SCHEMA`.`COLUMNS` \
             WHERE table_name = '{table_ref}'"
        )
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // BigQuery projects are not created via SQL
        None
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        // BigQuery: CREATE SCHEMA = CREATE DATASET
        let validate = || -> AdapterResult<String> {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!(
                "CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`"
            ))
        };
        Some(validate())
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE SYSTEM ({percent} PERCENT)"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // BigQuery supports DML transactions since 2023
        Ok(vec![
            "BEGIN TRANSACTION".to_string(),
            format!("DELETE FROM {target} WHERE {partition_filter}"),
            format!("INSERT INTO {target}\n{select_sql}"),
            "COMMIT TRANSACTION".to_string(),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_table_ref() {
        let d = BigQueryDialect;
        let r = d.format_table_ref("project", "dataset", "table").unwrap();
        assert_eq!(r, "`project`.`dataset`.`table`");
    }

    #[test]
    fn test_create_table_as() {
        let d = BigQueryDialect;
        let sql = d.create_table_as("`p`.`d`.`t`", "SELECT 1");
        assert!(sql.starts_with("CREATE OR REPLACE TABLE"));
    }

    #[test]
    fn test_merge_into() {
        let d = BigQueryDialect;
        let sql = d
            .merge_into(
                "`p`.`d`.`t`",
                "SELECT * FROM src",
                &["id".into()],
                &ColumnSelection::All,
            )
            .unwrap();
        assert!(sql.contains("MERGE INTO"));
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT ROW"));
    }

    #[test]
    fn test_insert_overwrite_partition() {
        let d = BigQueryDialect;
        let stmts = d
            .insert_overwrite_partition(
                "`p`.`d`.`t`",
                "date_col >= '2024-01-01'",
                "SELECT * FROM src",
            )
            .unwrap();
        assert_eq!(stmts.len(), 4);
        assert_eq!(stmts[0], "BEGIN TRANSACTION");
        assert_eq!(stmts[3], "COMMIT TRANSACTION");
    }

    #[test]
    fn test_merge_requires_keys() {
        let d = BigQueryDialect;
        let result = d.merge_into("`p`.`d`.`t`", "SELECT 1", &[], &ColumnSelection::All);
        assert!(result.is_err());
    }

    #[test]
    fn test_select_clause_all() {
        let d = BigQueryDialect;
        let sql = d.select_clause(&ColumnSelection::All, &[]).unwrap();
        assert_eq!(sql, "SELECT *");
    }

    #[test]
    fn test_select_clause_with_metadata() {
        let d = BigQueryDialect;
        let meta = vec![MetadataColumn {
            name: "_loaded_by".to_string(),
            data_type: "STRING".to_string(),
            value: "NULL".to_string(),
        }];
        let sql = d.select_clause(&ColumnSelection::All, &meta).unwrap();
        assert!(sql.contains("CAST(NULL AS STRING) AS _loaded_by"));
    }

    #[test]
    fn test_watermark_where() {
        let d = BigQueryDialect;
        let sql = d
            .watermark_where("_fivetran_synced", "`p`.`d`.`t`")
            .unwrap();
        assert!(sql.contains("TIMESTAMP '1970-01-01'"));
    }

    #[test]
    fn test_create_catalog_returns_none() {
        let d = BigQueryDialect;
        assert!(d.create_catalog_sql("project").is_none());
    }

    #[test]
    fn test_create_schema() {
        let d = BigQueryDialect;
        let sql = d.create_schema_sql("project", "dataset").unwrap().unwrap();
        assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS `project`.`dataset`");
    }

    #[test]
    fn test_tablesample() {
        let d = BigQueryDialect;
        assert_eq!(
            d.tablesample_clause(10).unwrap(),
            "TABLESAMPLE SYSTEM (10 PERCENT)"
        );
    }

    #[test]
    fn test_drop_table() {
        let d = BigQueryDialect;
        assert_eq!(
            d.drop_table_sql("`p`.`d`.`t`"),
            "DROP TABLE IF EXISTS `p`.`d`.`t`"
        );
    }
}
