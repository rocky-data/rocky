//! Lakehouse-native DDL generation.
//!
//! The format/options/error types now live in `rocky_ir::lakehouse` (data
//! only). This module owns the dialect-aware DDL generator
//! [`generate_lakehouse_ddl`] and its helpers — both depend on
//! [`crate::traits::SqlDialect`], which is the runtime trait surface
//! that justifies keeping them in `rocky-core`.

use rocky_sql::validation;

use crate::traits::SqlDialect;

// Re-export the data types so existing call sites (`crate::lakehouse::X`)
// keep working without churn. External consumers should reach for
// `rocky_ir::lakehouse::*` directly.
pub use rocky_ir::lakehouse::{LakehouseError, LakehouseFormat, LakehouseOptions};

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
/// * `dialect` — The warehouse SQL dialect. Used both for `format_table_ref`
///   validation and to gate the Spark/Databricks-only `USING <format>` DDL:
///   dialects that don't advertise `supports_lakehouse_format_ddl()` fail
///   fast rather than emitting SQL their warehouse rejects.
///
/// # Errors
///
/// Returns [`LakehouseError::DialectUnsupported`] when the target dialect
/// cannot render the `format = X` DDL (every dialect except Databricks
/// today), or [`LakehouseError`] if an identifier fails validation.
pub fn generate_lakehouse_ddl(
    format: &LakehouseFormat,
    target: &str,
    select_sql: &str,
    options: &LakehouseOptions,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, LakehouseError> {
    // Fail fast on dialects that can't render the lakehouse `format = X`
    // DDL. The generator below emits the Spark/Databricks `USING DELTA |
    // ICEBERG … TBLPROPERTIES(…)` grammar, which Snowflake / BigQuery /
    // Trino / DuckDB reject at parse time. Refusing here surfaces a clear
    // known-limitation error at plan/compile time instead of a raw
    // warehouse syntax error at apply time.
    if !dialect.supports_lakehouse_format_ddl() {
        return Err(LakehouseError::DialectUnsupported {
            format: format.to_string(),
            dialect: dialect.name().to_string(),
        });
    }

    // Validate option identifiers up front.
    validate_options(options)?;

    // Reject managed-Iceberg `format_options` that the Databricks warehouse
    // rejects at execution (partition_by + cluster_by together; engine-managed
    // `write.format.*` properties). This is the run-path guard: the same
    // constraints are surfaced earlier as `rocky compile` diagnostics (E035),
    // but if compile is bypassed we still fail with a clear Rocky error here
    // instead of a raw warehouse rejection on the first run.
    if let Some(violation) = rocky_ir::lakehouse::validate_managed_iceberg_options(format, options)
        .into_iter()
        .next()
    {
        return Err(LakehouseError::ManagedIcebergUnsupported(violation.message));
    }

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
    if let Some(comment) = &options.comment
        && (comment.contains('\'') || comment.contains("--") || comment.contains(';'))
    {
        return Err(LakehouseError::InvalidOption(format!(
            "comment contains unsafe characters: '{comment}'"
        )));
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
    use crate::traits::{AdapterError, AdapterResult};
    use rocky_ir::ColumnSelection;

    /// Minimal test dialect for lakehouse DDL tests (mirrors sql_gen::tests::TestDialect).
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn name(&self) -> &'static str {
            "test"
        }

        // This stand-in represents the Databricks-flavoured dialect that
        // the lakehouse DDL generator targets, so it advertises support
        // for the `USING <format>` grammar exercised by these tests.
        fn supports_lakehouse_format_ddl(&self) -> bool {
            true
        }

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
            unimplemented!("not needed for lakehouse tests")
        }

        fn select_clause(
            &self,
            _columns: &ColumnSelection,
            _metadata: &[rocky_ir::MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!("not needed for lakehouse tests")
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
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
    fn test_iceberg_table_partition_only() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
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
        assert!(!sql.contains("CLUSTER BY"));
    }

    #[test]
    fn test_iceberg_table_cluster_only() {
        let opts = LakehouseOptions {
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
        assert!(sql.contains("CLUSTER BY (user_id)"));
        assert!(!sql.contains("PARTITIONED BY"));
    }

    // -----------------------------------------------------------------------
    // Managed-Iceberg format_options guard (FR-044)
    // -----------------------------------------------------------------------

    #[test]
    fn test_iceberg_partition_and_cluster_together_is_rejected() {
        // Databricks managed Iceberg rejects PARTITIONED BY + CLUSTER BY
        // together at the warehouse; `generate_lakehouse_ddl` must fail fast
        // with a clear error that names both options instead of emitting DDL.
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            cluster_by: vec!["user_id".into()],
            ..default_opts()
        };
        let err = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &opts,
            &dialect(),
        )
        .expect_err("partition + cluster on Iceberg must be rejected");

        match err {
            LakehouseError::ManagedIcebergUnsupported(msg) => {
                assert!(msg.contains("partition_by"), "names partition_by: {msg}");
                assert!(msg.contains("cluster_by"), "names cluster_by: {msg}");
            }
            other => panic!("expected ManagedIcebergUnsupported, got {other:?}"),
        }
    }

    #[test]
    fn test_iceberg_write_format_property_is_rejected() {
        let opts = LakehouseOptions {
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..default_opts()
        };
        let err = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &opts,
            &dialect(),
        )
        .expect_err("engine-managed write.format.* property must be rejected");

        match err {
            LakehouseError::ManagedIcebergUnsupported(msg) => {
                assert!(
                    msg.contains("write.format.default"),
                    "names the offending key: {msg}"
                );
            }
            other => panic!("expected ManagedIcebergUnsupported, got {other:?}"),
        }
    }

    #[test]
    fn test_iceberg_benign_property_is_accepted() {
        // A non-`write.format.*` property (and partition-only) must still emit.
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            table_properties: vec![("delta.enableChangeDataFeed".into(), "true".into())],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &opts,
            &dialect(),
        )
        .expect("benign Iceberg options must compile");
        assert!(stmts[0].contains("USING ICEBERG"));
    }

    #[test]
    fn test_delta_partition_and_cluster_together_is_allowed() {
        // The managed-Iceberg rules must not touch Delta — partition + cluster
        // together is a supported Delta combo.
        let opts = LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            ..default_opts()
        };
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.orders",
            "SELECT * FROM cat.raw.orders",
            &opts,
            &dialect(),
        )
        .expect("Delta partition + cluster must still compile");
        let sql = &stmts[0];
        assert!(sql.contains("PARTITIONED BY (region)"));
        assert!(sql.contains("CLUSTER BY (id)"));
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

    // -----------------------------------------------------------------------
    // Dialect gating (D1)
    // -----------------------------------------------------------------------

    /// A dialect that does NOT advertise `supports_lakehouse_format_ddl`
    /// — stands in for Snowflake / BigQuery / Trino / DuckDB, which
    /// can't render the Spark `USING <format>` grammar.
    struct NonSparkDialect;

    impl SqlDialect for NonSparkDialect {
        fn name(&self) -> &'static str {
            "non_spark"
        }

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
            unimplemented!("not needed for lakehouse gating tests")
        }

        fn select_clause(
            &self,
            _columns: &ColumnSelection,
            _metadata: &[rocky_ir::MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!("not needed for lakehouse gating tests")
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> AdapterResult<String> {
            unimplemented!("not needed for lakehouse gating tests")
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
            unimplemented!("not needed for lakehouse gating tests")
        }
    }

    #[test]
    fn test_lakehouse_format_rejected_on_non_spark_dialect() {
        // `format = "iceberg"` on a dialect that can't render the
        // `USING <format>` grammar must fail fast with the known-limitation
        // error — naming both the format and the dialect — rather than
        // emitting Spark SQL the warehouse would reject at apply time.
        let err = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &default_opts(),
            &NonSparkDialect,
        )
        .expect_err("non-Spark dialect must refuse lakehouse format DDL");

        match err {
            LakehouseError::DialectUnsupported { format, dialect } => {
                assert_eq!(format, "iceberg_table");
                assert_eq!(dialect, "non_spark");
            }
            other => panic!("expected DialectUnsupported, got {other:?}"),
        }

        // The error message must name the limitation clearly and must NOT
        // contain emitted Spark SQL.
        let msg = generate_lakehouse_ddl(
            &LakehouseFormat::DeltaTable,
            "cat.sch.events",
            "SELECT 1",
            &default_opts(),
            &NonSparkDialect,
        )
        .unwrap_err()
        .to_string();
        assert!(msg.contains("is not implemented for adapter/dialect 'non_spark'"));
        assert!(!msg.contains("USING DELTA"));
    }

    #[test]
    fn test_lakehouse_format_still_emitted_on_spark_dialect() {
        // The Databricks/Spark happy path is preserved: a supporting
        // dialect still emits the `USING <format>` DDL.
        let stmts = generate_lakehouse_ddl(
            &LakehouseFormat::IcebergTable,
            "cat.sch.events",
            "SELECT * FROM cat.raw.events",
            &default_opts(),
            &dialect(),
        )
        .expect("supporting dialect emits lakehouse DDL");
        assert!(stmts[0].contains("USING ICEBERG"));
    }

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
