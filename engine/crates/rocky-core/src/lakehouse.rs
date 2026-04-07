//! Lakehouse-native materialization types and DDL generation.
//!
//! Modern lakehouses support table formats beyond plain managed tables:
//! Delta Lake, Apache Iceberg, materialized views, and streaming tables.
//! This module provides the enum + options for each format and a
//! single entry point ([`generate_lakehouse_ddl`]) that emits the
//! correct `CREATE` statement(s) for a given format/dialect pair.
//!
//! **Scope:** Foundation types and DDL generators only. Model config
//! integration and CLI surface are out of scope (Plan 37 follow-up).

use rocky_sql::validation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::traits::SqlDialect;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during lakehouse DDL generation.
#[derive(Debug, Error)]
pub enum LakehouseError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("unsupported format for dialect: {0}")]
    UnsupportedFormat(String),

    #[error("invalid option: {0}")]
    InvalidOption(String),
}

// ---------------------------------------------------------------------------
// LakehouseFormat
// ---------------------------------------------------------------------------

/// The physical format used to materialize a model in the lakehouse.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LakehouseFormat {
    /// Delta Lake table (`CREATE TABLE ... USING DELTA`).
    DeltaTable,
    /// Apache Iceberg table (`CREATE TABLE ... USING ICEBERG`).
    IcebergTable,
    /// Warehouse-managed materialized view (`CREATE MATERIALIZED VIEW`).
    MaterializedView,
    /// Databricks-specific streaming table (`CREATE STREAMING TABLE`).
    StreamingTable,
    /// Plain managed table (default warehouse format).
    #[default]
    Table,
    /// SQL view (no physical storage).
    View,
}

impl std::fmt::Display for LakehouseFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeltaTable => write!(f, "delta_table"),
            Self::IcebergTable => write!(f, "iceberg_table"),
            Self::MaterializedView => write!(f, "materialized_view"),
            Self::StreamingTable => write!(f, "streaming_table"),
            Self::Table => write!(f, "table"),
            Self::View => write!(f, "view"),
        }
    }
}

// ---------------------------------------------------------------------------
// LakehouseOptions
// ---------------------------------------------------------------------------

/// Format-specific DDL options for lakehouse materializations.
///
/// Not all options apply to every format — [`generate_lakehouse_ddl`]
/// silently ignores options that are irrelevant for the chosen format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LakehouseOptions {
    /// `PARTITIONED BY (col1, col2)` — Delta and Iceberg tables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,

    /// `CLUSTER BY (col1, col2)` — Databricks liquid clustering (Delta)
    /// or Iceberg sorted clustering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_by: Vec<String>,

    /// Arbitrary key-value table properties.
    /// Rendered as `TBLPROPERTIES ('key' = 'value', ...)` for Delta/Iceberg.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_properties: Vec<(String, String)>,

    /// Optional comment on the table/view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
}

// ---------------------------------------------------------------------------
// DDL generation
// ---------------------------------------------------------------------------

/// Generates the DDL statement(s) for a lakehouse-native materialization.
///
/// Returns one or more SQL strings that the runtime should execute in order.
/// Most formats produce a single statement; future formats that require
/// multi-step setup (e.g., `ALTER TABLE SET TBLPROPERTIES` after creation)
/// will return additional entries.
///
/// # Arguments
///
/// * `format` — The target lakehouse format.
/// * `target` — Fully-qualified target name (already formatted via `dialect.format_table_ref`).
/// * `select_sql` — The body `SELECT` statement (or full query for views/MVs).
/// * `options` — Format-specific options (partitioning, clustering, properties).
/// * `dialect` — The warehouse SQL dialect (used for `format_table_ref` validation).
///
/// # Errors
///
/// Returns [`LakehouseError`] if an identifier fails validation or the
/// format is unsupported for the given dialect context.
pub fn generate_lakehouse_ddl(
    format: &LakehouseFormat,
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
    _dialect: &dyn SqlDialect,
) -> Result<Vec<String>, LakehouseError> {
    // Validate option identifiers up front.
    validate_options(options)?;

    match format {
        LakehouseFormat::DeltaTable => generate_delta_ddl(target, select_sql, options),
        LakehouseFormat::IcebergTable => generate_iceberg_ddl(target, select_sql, options),
        LakehouseFormat::MaterializedView => {
            generate_materialized_view_ddl(target, select_sql, options)
        }
        LakehouseFormat::StreamingTable => {
            generate_streaming_table_ddl(target, select_sql, options)
        }
        LakehouseFormat::Table => generate_table_ddl(target, select_sql, options),
        LakehouseFormat::View => generate_view_ddl(target, select_sql, options),
    }
}

/// Validates all identifiers inside [`LakehouseOptions`].
fn validate_options(options: &LakehouseOptions) -> Result<(), LakehouseError> {
    for col in &options.partition_by {
        validation::validate_identifier(col)?;
    }
    for col in &options.cluster_by {
        validation::validate_identifier(col)?;
    }
    for (key, value) in &options.table_properties {
        // Keys must be safe identifiers (dot-separated namespaces are common,
        // e.g., "delta.autoOptimize.optimizeWrite"), so we validate each
        // dot-separated segment individually.
        for segment in key.split('.') {
            if segment.is_empty() {
                return Err(LakehouseError::InvalidOption(format!(
                    "table property key contains empty segment: '{key}'"
                )));
            }
            // Segments may contain alphanumeric + underscore (standard identifier chars).
            if !segment
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_')
            {
                return Err(LakehouseError::InvalidOption(format!(
                    "table property key segment contains invalid characters: '{segment}'"
                )));
            }
        }
        // Values: reject embedded single-quotes and SQL injection vectors.
        if value.contains('\'') || value.contains("--") || value.contains(';') {
            return Err(LakehouseError::InvalidOption(format!(
                "table property value contains unsafe characters: '{value}'"
            )));
        }
    }
    if let Some(comment) = &options.comment {
        if comment.contains('\'') || comment.contains("--") || comment.contains(';') {
            return Err(LakehouseError::InvalidOption(format!(
                "comment contains unsafe characters: '{comment}'"
            )));
        }
    }
    Ok(())
}

/// Renders the `TBLPROPERTIES (...)` clause, or empty string if none.
fn tblproperties_clause(options: &LakehouseOptions) -> String {
    if options.table_properties.is_empty() {
        return String::new();
    }
    let pairs: Vec<String> = options
        .table_properties
        .iter()
        .map(|(k, v)| format!("'{k}' = '{v}'"))
        .collect();
    format!("\nTBLPROPERTIES ({})", pairs.join(", "))
}

/// Renders the `COMMENT '...'` clause, or empty string if none.
fn comment_clause(options: &LakehouseOptions) -> String {
    match &options.comment {
        Some(c) => format!("\nCOMMENT '{c}'"),
        None => String::new(),
    }
}

/// Renders `PARTITIONED BY (...)` clause, or empty string.
fn partition_clause(options: &LakehouseOptions) -> String {
    if options.partition_by.is_empty() {
        return String::new();
    }
    format!("\nPARTITIONED BY ({})", options.partition_by.join(", "))
}

/// Renders `CLUSTER BY (...)` clause, or empty string.
fn cluster_clause(options: &LakehouseOptions) -> String {
    if options.cluster_by.is_empty() {
        return String::new();
    }
    format!("\nCLUSTER BY ({})", options.cluster_by.join(", "))
}

// ---------------------------------------------------------------------------
// Per-format generators
// ---------------------------------------------------------------------------

fn generate_delta_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE TABLE {target}");
    sql.push_str("\nUSING DELTA");
    sql.push_str(&partition_clause(options));
    sql.push_str(&cluster_clause(options));
    sql.push_str(&comment_clause(options));
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

fn generate_iceberg_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE TABLE {target}");
    sql.push_str("\nUSING ICEBERG");
    sql.push_str(&partition_clause(options));
    sql.push_str(&cluster_clause(options));
    sql.push_str(&comment_clause(options));
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

fn generate_materialized_view_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE MATERIALIZED VIEW {target}");
    sql.push_str(&comment_clause(options));
    // Materialized views support TBLPROPERTIES on Databricks.
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

fn generate_streaming_table_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE STREAMING TABLE {target}");
    sql.push_str(&partition_clause(options));
    sql.push_str(&cluster_clause(options));
    sql.push_str(&comment_clause(options));
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

fn generate_table_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE TABLE {target}");
    sql.push_str(&partition_clause(options));
    sql.push_str(&cluster_clause(options));
    sql.push_str(&comment_clause(options));
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

fn generate_view_ddl(
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
) -> Result<Vec<String>, LakehouseError> {
    let mut sql = format!("CREATE OR REPLACE VIEW {target}");
    sql.push_str(&comment_clause(options));
    // Views support TBLPROPERTIES on Databricks.
    sql.push_str(&tblproperties_clause(options));
    sql.push_str(&format!("\nAS\n{select_sql}"));
    Ok(vec![sql])
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ir::ColumnSelection;
    use crate::traits::{AdapterError, AdapterResult};

    /// Minimal test dialect for lakehouse DDL tests (mirrors sql_gen::tests::TestDialect).
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
            unimplemented!("not needed for lakehouse tests")
        }

        fn select_clause(
            &self,
            _columns: &ColumnSelection,
            _metadata: &[crate::ir::MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!("not needed for lakehouse tests")
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _target_ref: &str,
        ) -> AdapterResult<String> {
            unimplemented!("not needed for lakehouse tests")
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
            unimplemented!("not needed for lakehouse tests")
        }
    }

    fn dialect() -> TestDialect {
        TestDialect
    }

    fn default_opts() -> LakehouseOptions {
        LakehouseOptions::default()
    }

    // -----------------------------------------------------------------------
    // LakehouseFormat basics
    // -----------------------------------------------------------------------

    #[test]
    fn test_format_default_is_table() {
        assert_eq!(LakehouseFormat::default(), LakehouseFormat::Table);
    }

    #[test]
    fn test_format_display() {
        assert_eq!(LakehouseFormat::DeltaTable.to_string(), "delta_table");
        assert_eq!(LakehouseFormat::IcebergTable.to_string(), "iceberg_table");
        assert_eq!(
            LakehouseFormat::MaterializedView.to_string(),
            "materialized_view"
        );
        assert_eq!(
            LakehouseFormat::StreamingTable.to_string(),
            "streaming_table"
        );
        assert_eq!(LakehouseFormat::Table.to_string(), "table");
        assert_eq!(LakehouseFormat::View.to_string(), "view");
    }

    #[test]
    fn test_format_serde_roundtrip() {
        let formats = vec![
            LakehouseFormat::DeltaTable,
            LakehouseFormat::IcebergTable,
            LakehouseFormat::MaterializedView,
            LakehouseFormat::StreamingTable,
            LakehouseFormat::Table,
            LakehouseFormat::View,
        ];
        for fmt in formats {
            let json = serde_json::to_string(&fmt).unwrap();
            let deserialized: LakehouseFormat = serde_json::from_str(&json).unwrap();
            assert_eq!(fmt, deserialized);
        }
    }

    // -----------------------------------------------------------------------
    // LakehouseOptions serde
    // -----------------------------------------------------------------------

    #[test]
    fn test_options_default_is_empty() {
        let opts = LakehouseOptions::default();
        assert!(opts.partition_by.is_empty());
        assert!(opts.cluster_by.is_empty());
        assert!(opts.table_properties.is_empty());
        assert!(opts.comment.is_none());
    }

    #[test]
    fn test_options_serde_roundtrip() {
        let opts = LakehouseOptions {
            partition_by: vec!["year".into(), "month".into()],
            cluster_by: vec!["region".into()],
            table_properties: vec![("delta.autoOptimize.optimizeWrite".into(), "true".into())],
            comment: Some("Sales fact table".into()),
        };
        let json = serde_json::to_string(&opts).unwrap();
        let deserialized: LakehouseOptions = serde_json::from_str(&json).unwrap();
        assert_eq!(opts, deserialized);
    }

    // -----------------------------------------------------------------------
    // Delta table DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_delta_table_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE TABLE cat.sch.orders"));
        assert!(sql.contains("USING DELTA"));
        assert!(sql.contains("AS\nSELECT * FROM cat.raw.orders"));
        // No partition/cluster/tblproperties without options.
        assert!(!sql.contains("PARTITIONED BY"));
        assert!(!sql.contains("CLUSTER BY"));
        assert!(!sql.contains("TBLPROPERTIES"));
    }

    #[test]
    fn test_delta_table_with_partitioning() {
        let opts = LakehouseOptions {
            partition_by: vec!["year".into(), "month".into()],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains("PARTITIONED BY (year, month)"));
    }

    #[test]
    fn test_delta_table_with_liquid_clustering() {
        let opts = LakehouseOptions {
            cluster_by: vec!["region".into(), "customer_id".into()],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains("CLUSTER BY (region, customer_id)"));
    }

    #[test]
    fn test_delta_table_with_tblproperties() {
        let opts = LakehouseOptions {
            table_properties: vec![
                ("delta.autoOptimize.optimizeWrite".into(), "true".into()),
                ("delta.autoOptimize.autoCompact".into(), "true".into()),
            ],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains(
            "TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"
        ));
    }

    #[test]
    fn test_delta_table_with_comment() {
        let opts = LakehouseOptions {
            comment: Some("Sales fact table".into()),
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains("COMMENT 'Sales fact table'"));
    }

    #[test]
    fn test_delta_table_full_options() {
        let opts = LakehouseOptions {
            partition_by: vec!["year".into()],
            cluster_by: vec!["region".into()],
            table_properties: vec![("delta.enableChangeDataFeed".into(), "true".into())],
            comment: Some("Full options test".into()),
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        let sql = &stmts[0];
        assert!(sql.contains("USING DELTA"));
        assert!(sql.contains("PARTITIONED BY (year)"));
        assert!(sql.contains("CLUSTER BY (region)"));
        assert!(sql.contains("COMMENT 'Full options test'"));
        assert!(sql.contains("TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"));
    }

    // -----------------------------------------------------------------------
    // Iceberg table DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_iceberg_table_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE TABLE cat.sch.events"));
        assert!(sql.contains("USING ICEBERG"));
        assert!(sql.contains("AS\nSELECT * FROM cat.raw.events"));
    }

    #[test]
    fn test_iceberg_table_with_partitioning_and_clustering() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            cluster_by: vec!["user_id".into()],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &opts,
            &dialect(),
        )
        .unwrap();

        let sql = &stmts[0];
        assert!(sql.contains("USING ICEBERG"));
        assert!(sql.contains("PARTITIONED BY (event_date)"));
        assert!(sql.contains("CLUSTER BY (user_id)"));
    }

    // -----------------------------------------------------------------------
    // Materialized view DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_materialized_view_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::MaterializedView,
            "cat.silver.dim_accounts",
            "SELECT id, name FROM cat.raw.accounts WHERE active = true",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE MATERIALIZED VIEW cat.silver.dim_accounts"));
        assert!(sql.contains("AS\nSELECT id, name FROM cat.raw.accounts WHERE active = true"));
        // MV should not have PARTITIONED BY or CLUSTER BY.
        assert!(!sql.contains("PARTITIONED BY"));
        assert!(!sql.contains("CLUSTER BY"));
    }

    #[test]
    fn test_materialized_view_with_comment_and_properties() {
        let opts = LakehouseOptions {
            comment: Some("Active accounts dimension".into()),
            table_properties: vec![("pipelines.autoOptimize.zOrderCols".into(), "id".into())],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::MaterializedView,
            "cat.silver.dim_accounts",
            "SELECT id, name FROM cat.raw.accounts",
            &opts,
            &dialect(),
        )
        .unwrap();

        let sql = &stmts[0];
        assert!(sql.contains("COMMENT 'Active accounts dimension'"));
        assert!(sql.contains("TBLPROPERTIES"));
    }

    // -----------------------------------------------------------------------
    // Streaming table DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_streaming_table_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::StreamingTable,
            "cat.bronze.raw_clicks",
            "SELECT * FROM STREAM(cat.raw.clickstream)",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE STREAMING TABLE cat.bronze.raw_clicks"));
        assert!(sql.contains("AS\nSELECT * FROM STREAM(cat.raw.clickstream)"));
    }

    #[test]
    fn test_streaming_table_with_options() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            cluster_by: vec!["user_id".into()],
            comment: Some("Raw clickstream".into()),
            table_properties: vec![("pipelines.reset.allowed".into(), "true".into())],
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::StreamingTable,
            "cat.bronze.raw_clicks",
            "SELECT * FROM STREAM(cat.raw.clickstream)",
            &opts,
            &dialect(),
        )
        .unwrap();

        let sql = &stmts[0];
        assert!(sql.contains("STREAMING TABLE"));
        assert!(sql.contains("PARTITIONED BY (event_date)"));
        assert!(sql.contains("CLUSTER BY (user_id)"));
        assert!(sql.contains("COMMENT 'Raw clickstream'"));
        assert!(sql.contains("TBLPROPERTIES"));
    }

    // -----------------------------------------------------------------------
    // Plain table DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::Table,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE TABLE cat.sch.orders"));
        assert!(!sql.contains("USING")); // No format specifier for plain table.
        assert!(sql.contains("AS\nSELECT * FROM cat.raw.orders"));
    }

    #[test]
    fn test_table_with_clustering() {
        let opts = LakehouseOptions {
            cluster_by: vec!["order_date".into()],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::Table,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains("CLUSTER BY (order_date)"));
    }

    // -----------------------------------------------------------------------
    // View DDL
    // -----------------------------------------------------------------------

    #[test]
    fn test_view_basic() {
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::View,
            "cat.reporting.v_orders",
            "SELECT id, total FROM cat.silver.orders WHERE total > 0",
            &default_opts(),
            &dialect(),
        )
        .unwrap();

        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(sql.starts_with("CREATE OR REPLACE VIEW cat.reporting.v_orders"));
        assert!(sql.contains("AS\nSELECT id, total FROM cat.silver.orders WHERE total > 0"));
        // Views should not have physical-table clauses.
        assert!(!sql.contains("PARTITIONED BY"));
        assert!(!sql.contains("CLUSTER BY"));
    }

    #[test]
    fn test_view_with_comment() {
        let opts = LakehouseOptions {
            comment: Some("Filtered orders view".into()),
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::View,
            "cat.reporting.v_orders",
            "SELECT id, total FROM cat.silver.orders",
            &opts,
            &dialect(),
        )
        .unwrap();

        assert!(stmts[0].contains("COMMENT 'Filtered orders view'"));
    }

    // -----------------------------------------------------------------------
    // Validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_invalid_partition_column() {
        let opts = LakehouseOptions {
            partition_by: vec!["valid_col".into(), "DROP TABLE --".into()],
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_cluster_column() {
        let opts = LakehouseOptions {
            cluster_by: vec!["DROP TABLE; --".into()],
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_table_property_key() {
        let opts = LakehouseOptions {
            table_properties: vec![("key; DROP TABLE".into(), "val".into())],
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_table_property_value() {
        let opts = LakehouseOptions {
            table_properties: vec![("valid_key".into(), "val'; DROP TABLE --".into())],
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_comment() {
        let opts = LakehouseOptions {
            comment: Some("test'; DROP TABLE orders --".into()),
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_table_property_key_segment() {
        let opts = LakehouseOptions {
            table_properties: vec![("delta..optimizeWrite".into(), "true".into())],
            ..default_opts()
        };
        let result = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT 1",
            &opts,
            &dialect(),
        );
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // Clause ordering (ensure clauses appear in the right order)
    // -----------------------------------------------------------------------

    #[test]
    fn test_delta_clause_ordering() {
        let opts = LakehouseOptions {
            partition_by: vec!["year".into()],
            cluster_by: vec!["region".into()],
            comment: Some("Test".into()),
            table_properties: vec![("delta.cdf".into(), "true".into())],
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.t",
            "SELECT 1",
            &opts,
            &dialect(),
        )
        .unwrap();

        let sql = &stmts[0];
        let using_pos = sql.find("USING DELTA").unwrap();
        let part_pos = sql.find("PARTITIONED BY").unwrap();
        let clust_pos = sql.find("CLUSTER BY").unwrap();
        let comment_pos = sql.find("COMMENT").unwrap();
        let props_pos = sql.find("TBLPROPERTIES").unwrap();
        let as_pos = sql.find("\nAS\n").unwrap();

        assert!(using_pos < part_pos);
        assert!(part_pos < clust_pos);
        assert!(clust_pos < comment_pos);
        assert!(comment_pos < props_pos);
        assert!(props_pos < as_pos);
    }
}
