use std::collections::BTreeMap;
use std::fmt::Write;

use rocky_sql::validation::{self, ValidationError};
use thiserror::Error;

/// Errors from catalog/schema SQL generation, including validation and tag safety checks.
#[derive(Debug, Error)]
pub enum CatalogError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error(
        "unsafe tag {kind} '{value}': must not contain single quotes, semicolons, or comment markers"
    )]
    UnsafeTag { kind: &'static str, value: String },

    #[error("empty tag {kind}")]
    EmptyTag { kind: &'static str },
}

/// Generates `CREATE CATALOG IF NOT EXISTS <catalog>`.
pub fn generate_create_catalog_sql(catalog: &str) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    Ok(format!("CREATE CATALOG IF NOT EXISTS {catalog}"))
}

/// Generates `CREATE SCHEMA IF NOT EXISTS <catalog>.<schema>`.
pub fn generate_create_schema_sql(catalog: &str, schema: &str) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    Ok(format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
}

/// Generates `ALTER CATALOG <catalog> SET TAGS (...)`.
///
/// Tags are key-value pairs wrapped in single quotes.
pub fn generate_set_catalog_tags_sql(
    catalog: &str,
    tags: &BTreeMap<String, String>,
) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    let tags_clause = format_tags(tags)?;
    Ok(format!("ALTER CATALOG {catalog} SET TAGS ({tags_clause})"))
}

/// Generates `ALTER SCHEMA <catalog>.<schema> SET TAGS (...)`.
pub fn generate_set_schema_tags_sql(
    catalog: &str,
    schema: &str,
    tags: &BTreeMap<String, String>,
) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    let tags_clause = format_tags(tags)?;
    Ok(format!(
        "ALTER SCHEMA {catalog}.{schema} SET TAGS ({tags_clause})"
    ))
}

/// Generates `ALTER TABLE <catalog>.<schema>.<table> SET TAGS (...)` with key-value pairs.
pub fn generate_set_table_tags_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    tags: &BTreeMap<String, String>,
) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(table)?;
    let tags_clause = format_tags(tags)?;
    Ok(format!(
        "ALTER TABLE {catalog}.{schema}.{table} SET TAGS ({tags_clause})"
    ))
}

/// Generates `ALTER TABLE <catalog>.<schema>.<table> ALTER COLUMN <column> SET TAGS (...)`
/// for a single column. Returns `Ok(None)` when `tags` is empty so callers
/// can skip the statement rather than emit a syntactically-empty `SET TAGS ()`.
///
/// Used by [`GovernanceAdapter::apply_column_tags`] on Databricks — Unity
/// Catalog supports column-level tags via this DDL since Runtime 13.3+.
///
/// [`GovernanceAdapter::apply_column_tags`]: crate::traits::GovernanceAdapter::apply_column_tags
pub fn generate_set_column_tags_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    column: &str,
    tags: &BTreeMap<String, String>,
) -> Result<Option<String>, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(table)?;
    validation::validate_identifier(column)?;
    if tags.is_empty() {
        return Ok(None);
    }
    let tags_clause = format_tags(tags)?;
    Ok(Some(format!(
        "ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} SET TAGS ({tags_clause})"
    )))
}

/// Generates `DESCRIBE CATALOG <catalog>`.
pub fn generate_describe_catalog_sql(catalog: &str) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    Ok(format!("DESCRIBE CATALOG {catalog}"))
}

/// Generates `ALTER TABLE <catalog>.<schema>.<table> SET TBLPROPERTIES (
/// 'delta.logRetentionDuration' = '<N> days',
/// 'delta.deletedFileRetentionDuration' = '<N> days'
/// )` for Delta Lake time-travel retention.
///
/// Both properties are set together so a single statement covers the pair
/// Delta uses for time-travel (log retention) and tombstone eligibility
/// (deleted-file retention). Used by
/// [`GovernanceAdapter::apply_retention_policy`] on Databricks.
///
/// Identifiers are validated against Rocky's SQL identifier allowlist
/// (`rocky-sql/validation.rs`) before interpolation — never `format!` on
/// unvalidated input.
///
/// [`GovernanceAdapter::apply_retention_policy`]: crate::traits::GovernanceAdapter::apply_retention_policy
pub fn generate_set_delta_retention_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    duration_days: u32,
) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(table)?;
    Ok(format!(
        "ALTER TABLE {catalog}.{schema}.{table} SET TBLPROPERTIES \
('delta.logRetentionDuration' = '{duration_days} days', \
'delta.deletedFileRetentionDuration' = '{duration_days} days')"
    ))
}

/// Generates `SHOW SCHEMAS IN <catalog>`.
pub fn generate_show_schemas_sql(catalog: &str) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    Ok(format!("SHOW SCHEMAS IN {catalog}"))
}

/// Lists tables in a schema via `information_schema.tables`.
///
/// ```sql
/// SELECT table_name FROM <catalog>.information_schema.tables
/// WHERE table_schema = '<schema>'
/// ```
pub fn generate_list_tables_sql(catalog: &str, schema: &str) -> Result<String, CatalogError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    Ok(format!(
        "SELECT table_name FROM {catalog}.information_schema.tables WHERE table_schema = '{schema}'"
    ))
}

/// Generates the discovery query for finding managed catalogs by tag.
///
/// ```sql
/// SELECT catalog_name
/// FROM system.information_schema.catalog_tags
/// WHERE tag_name = '<tag_name>' AND tag_value = '<tag_value>'
/// ```
pub fn generate_discover_managed_catalogs_sql(
    tag_name: &str,
    tag_value: &str,
) -> Result<String, CatalogError> {
    validate_tag_value(tag_name, "key")?;
    validate_tag_value(tag_value, "value")?;
    Ok(format!(
        "SELECT catalog_name FROM system.information_schema.catalog_tags WHERE tag_name = '{tag_name}' AND tag_value = '{tag_value}'"
    ))
}

// --- helpers ---

fn format_tags(tags: &BTreeMap<String, String>) -> Result<String, CatalogError> {
    let mut result = String::new();
    for (i, (key, value)) in tags.iter().enumerate() {
        validate_tag_value(key, "key")?;
        validate_tag_value(value, "value")?;
        if i > 0 {
            write!(result, ", ").unwrap();
        }
        write!(result, "'{key}' = '{value}'").unwrap();
    }
    Ok(result)
}

fn validate_tag_value(value: &str, kind: &'static str) -> Result<(), CatalogError> {
    if value.is_empty() {
        return Err(CatalogError::EmptyTag { kind });
    }
    let dangerous = ["--", "/*", "*/", "'", ";", "\n", "\r"];
    for pat in &dangerous {
        if value.contains(pat) {
            return Err(CatalogError::UnsafeTag {
                kind,
                value: value.to_string(),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_catalog() {
        let sql = generate_create_catalog_sql("acme_warehouse").unwrap();
        assert_eq!(sql, "CREATE CATALOG IF NOT EXISTS acme_warehouse");
    }

    #[test]
    fn test_create_schema() {
        let sql =
            generate_create_schema_sql("acme_warehouse", "staging__us_west__shopify").unwrap();
        assert_eq!(
            sql,
            "CREATE SCHEMA IF NOT EXISTS acme_warehouse.staging__us_west__shopify"
        );
    }

    #[test]
    fn test_set_catalog_tags() {
        let mut tags = BTreeMap::new();
        tags.insert("client".to_string(), "acme".to_string());
        tags.insert("managed_by".to_string(), "my-pipeline".to_string());
        tags.insert("product".to_string(), "my_product".to_string());

        let sql = generate_set_catalog_tags_sql("acme_warehouse", &tags).unwrap();

        assert_eq!(
            sql,
            "ALTER CATALOG acme_warehouse SET TAGS ('client' = 'acme', 'managed_by' = 'my-pipeline', 'product' = 'my_product')"
        );
    }

    #[test]
    fn test_set_schema_tags() {
        let mut tags = BTreeMap::new();
        tags.insert("client".to_string(), "acme".to_string());
        tags.insert("source".to_string(), "shopify".to_string());
        tags.insert("region-1".to_string(), "us_west".to_string());
        tags.insert("layer".to_string(), "raw".to_string());

        let sql =
            generate_set_schema_tags_sql("acme_warehouse", "staging__us_west__shopify", &tags)
                .unwrap();

        assert!(
            sql.starts_with("ALTER SCHEMA acme_warehouse.staging__us_west__shopify SET TAGS (")
        );
        assert!(sql.contains("'client' = 'acme'"));
        assert!(sql.contains("'source' = 'shopify'"));
        assert!(sql.contains("'region-1' = 'us_west'"));
        assert!(sql.contains("'layer' = 'raw'"));
    }

    #[test]
    fn test_set_column_tags() {
        let mut tags = BTreeMap::new();
        tags.insert("classification".to_string(), "pii".to_string());
        let sql = generate_set_column_tags_sql("warehouse", "raw", "users", "email", &tags)
            .unwrap()
            .expect("non-empty tags yield a statement");
        assert_eq!(
            sql,
            "ALTER TABLE warehouse.raw.users ALTER COLUMN email SET TAGS ('classification' = 'pii')"
        );
    }

    #[test]
    fn test_set_column_tags_empty_is_none() {
        let tags = BTreeMap::new();
        let result = generate_set_column_tags_sql("db", "s", "t", "c", &tags).unwrap();
        assert!(
            result.is_none(),
            "empty tags should skip the statement rather than emit SET TAGS ()"
        );
    }

    #[test]
    fn test_set_column_tags_rejects_bad_column_name() {
        let mut tags = BTreeMap::new();
        tags.insert("classification".to_string(), "pii".to_string());
        assert!(generate_set_column_tags_sql("db", "s", "t", "bad col", &tags).is_err());
        assert!(generate_set_column_tags_sql("db", "s", "t", "bad;DROP", &tags).is_err());
    }

    #[test]
    fn test_describe_catalog() {
        let sql = generate_describe_catalog_sql("acme_warehouse").unwrap();
        assert_eq!(sql, "DESCRIBE CATALOG acme_warehouse");
    }

    #[test]
    fn test_set_delta_retention_emits_both_properties() {
        let sql = generate_set_delta_retention_sql("warehouse", "silver", "events", 90).unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE warehouse.silver.events SET TBLPROPERTIES \
             ('delta.logRetentionDuration' = '90 days', \
             'delta.deletedFileRetentionDuration' = '90 days')"
                .replace("             ", "")
        );
    }

    #[test]
    fn test_set_delta_retention_large_value() {
        let sql = generate_set_delta_retention_sql("w", "s", "t", 2555).unwrap();
        assert!(sql.contains("'delta.logRetentionDuration' = '2555 days'"));
        assert!(sql.contains("'delta.deletedFileRetentionDuration' = '2555 days'"));
    }

    #[test]
    fn test_set_delta_retention_rejects_invalid_identifier() {
        assert!(generate_set_delta_retention_sql("db; DROP", "s", "t", 90).is_err());
        assert!(generate_set_delta_retention_sql("db", "s ace", "t", 90).is_err());
        assert!(generate_set_delta_retention_sql("db", "s", "t' --", 90).is_err());
    }

    #[test]
    fn test_show_schemas() {
        let sql = generate_show_schemas_sql("acme_warehouse").unwrap();
        assert_eq!(sql, "SHOW SCHEMAS IN acme_warehouse");
    }

    #[test]
    fn test_list_tables() {
        let sql = generate_list_tables_sql("source_warehouse", "q__raw__acme__na__fb_ads").unwrap();
        assert_eq!(
            sql,
            "SELECT table_name FROM source_warehouse.information_schema.tables WHERE table_schema = 'q__raw__acme__na__fb_ads'"
        );
    }

    #[test]
    fn test_list_tables_rejects_bad_input() {
        assert!(generate_list_tables_sql("bad; DROP", "schema").is_err());
        assert!(generate_list_tables_sql("catalog", "bad; DROP").is_err());
    }

    #[test]
    fn test_discover_managed_catalogs() {
        let sql = generate_discover_managed_catalogs_sql("managed_by", "my-pipeline").unwrap();
        assert_eq!(
            sql,
            "SELECT catalog_name FROM system.information_schema.catalog_tags WHERE tag_name = 'managed_by' AND tag_value = 'my-pipeline'"
        );
    }

    #[test]
    fn test_rejects_bad_catalog_name() {
        assert!(generate_create_catalog_sql("bad; DROP").is_err());
    }

    #[test]
    fn test_rejects_bad_schema_name() {
        assert!(generate_create_schema_sql("catalog", "bad schema").is_err());
    }

    #[test]
    fn test_rejects_injection_in_tag_key() {
        let mut tags = BTreeMap::new();
        tags.insert("key'; DROP TABLE".to_string(), "value".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags).is_err());
    }

    #[test]
    fn test_rejects_injection_in_tag_value() {
        let mut tags = BTreeMap::new();
        tags.insert("key".to_string(), "value'; DROP TABLE--".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags).is_err());
    }

    #[test]
    fn test_rejects_empty_tag_key() {
        let mut tags = BTreeMap::new();
        tags.insert(String::new(), "value".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags).is_err());
    }

    #[test]
    fn test_rejects_block_comment_in_tag() {
        let mut tags = BTreeMap::new();
        tags.insert("key".to_string(), "val /* comment */ ue".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags).is_err());
    }

    #[test]
    fn test_rejects_newline_in_tag() {
        let mut tags = BTreeMap::new();
        tags.insert("key".to_string(), "line1\nline2".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags).is_err());

        let mut tags2 = BTreeMap::new();
        tags2.insert("key".to_string(), "line1\rline2".to_string());
        assert!(generate_set_catalog_tags_sql("catalog", &tags2).is_err());
    }

    #[test]
    fn test_tags_sorted_by_key() {
        let mut tags = BTreeMap::new();
        tags.insert("z_key".to_string(), "z_val".to_string());
        tags.insert("a_key".to_string(), "a_val".to_string());
        tags.insert("m_key".to_string(), "m_val".to_string());

        let sql = generate_set_catalog_tags_sql("catalog", &tags).unwrap();

        // BTreeMap guarantees sorted order
        let tags_part = sql
            .split("SET TAGS (")
            .nth(1)
            .unwrap()
            .trim_end_matches(')');
        let keys: Vec<&str> = tags_part
            .split(", ")
            .map(|pair| pair.split(" = ").next().unwrap().trim_matches('\''))
            .collect();
        assert_eq!(keys, vec!["a_key", "m_key", "z_key"]);
    }
}
