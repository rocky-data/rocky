//! End-to-end tests for Rocky core pipeline using DuckDB.
//!
//! These tests verify the full pipeline flow (SQL generation -> execution -> state)
//! using an in-memory DuckDB database. No warehouse credentials required.

use rocky_core::ir::{
    ColumnInfo, ColumnSelection, DriftAction, GovernanceConfig, MaterializationStrategy,
    MetadataColumn, ReplicationPlan, SourceRef, TableRef, TargetRef, TransformationPlan,
    WatermarkState,
};
use rocky_core::state::StateStore;
use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_core::{drift, sql_gen};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test dialect (mirrors Databricks behavior for integration tests)
// ---------------------------------------------------------------------------

/// A test SQL dialect that produces Databricks-style SQL without requiring
/// the rocky-databricks crate. Used to verify SQL generation end-to-end.
struct TestDialect;

impl SqlDialect for TestDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        rocky_sql::validation::format_table_ref(catalog, schema, table).map_err(AdapterError::new)
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
                "merge strategy requires at least one unique_key column",
            ));
        }
        for key in keys {
            rocky_sql::validation::validate_identifier(key).map_err(AdapterError::new)?;
        }
        let on_clause = keys
            .iter()
            .map(|k| format!("t.{k} = s.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let update_clause = match update_cols {
            ColumnSelection::All => "UPDATE SET *".to_string(),
            ColumnSelection::Explicit(cols) => {
                let sets = cols
                    .iter()
                    .map(|c| {
                        rocky_sql::validation::validate_identifier(c).map_err(AdapterError::new)?;
                        Ok(format!("t.{c} = s.{c}"))
                    })
                    .collect::<AdapterResult<Vec<_>>>()?;
                format!("UPDATE SET {}", sets.join(", "))
            }
        };
        Ok(format!(
            "MERGE INTO {target} AS t\n\
             USING (\n{source_sql}\n) AS s\n\
             ON {on_clause}\n\
             WHEN MATCHED THEN {update_clause}\n\
             WHEN NOT MATCHED THEN INSERT *"
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let mut sql = String::from("SELECT ");
        match columns {
            ColumnSelection::All => sql.push('*'),
            ColumnSelection::Explicit(cols) => {
                for (i, col) in cols.iter().enumerate() {
                    rocky_sql::validation::validate_identifier(col).map_err(AdapterError::new)?;
                    if i > 0 {
                        sql.push_str(", ");
                    }
                    sql.push_str(col);
                }
            }
        }
        for mc in metadata {
            rocky_sql::validation::validate_identifier(&mc.name).map_err(AdapterError::new)?;
            use std::fmt::Write;
            write!(
                sql,
                ", CAST({} AS {}) AS {}",
                mc.value, mc.data_type, mc.name
            )
            .unwrap();
        }
        Ok(sql)
    }

    fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
        rocky_sql::validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        Ok(format!(
            "WHERE {timestamp_col} > (\n\
             \x20   SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01')\n\
             \x20   FROM {target_ref}\n\
             )"
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE TABLE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
        Some(
            rocky_sql::validation::validate_identifier(name)
                .map(|_| format!("CREATE CATALOG IF NOT EXISTS {name}"))
                .map_err(AdapterError::new),
        )
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        let result = (|| {
            rocky_sql::validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            rocky_sql::validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))
        })();
        Some(result)
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        Some(format!("TABLESAMPLE ({percent} PERCENT)"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        Ok(vec![format!(
            "INSERT INTO {target} REPLACE WHERE {partition_filter}\n{select_sql}"
        )])
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a temporary state store backed by redb.
fn temp_state() -> (StateStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test-state.redb");
    let store = StateStore::open(&path).unwrap();
    (store, dir)
}

/// Returns a standard replication plan for testing.
fn sample_replication_plan(strategy: MaterializationStrategy) -> ReplicationPlan {
    ReplicationPlan {
        source: SourceRef {
            catalog: "source_catalog".into(),
            schema: "src__acme__us_west__shopify".into(),
            table: "orders".into(),
        },
        target: TargetRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west__shopify".into(),
            table: "orders".into(),
        },
        strategy,
        columns: ColumnSelection::All,
        metadata_columns: vec![MetadataColumn {
            name: "_loaded_by".into(),
            data_type: "STRING".into(),
            value: "NULL".into(),
        }],
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
    }
}

// ===========================================================================
// Test 1: Full refresh SQL generation + state tracking
// ===========================================================================

#[test]
fn test_full_refresh_pipeline() {
    let (store, _dir) = temp_state();
    let dialect = TestDialect;

    let plan = sample_replication_plan(MaterializationStrategy::FullRefresh);

    // Generate CREATE TABLE AS SELECT SQL (full refresh)
    let sql = sql_gen::generate_create_table_as_sql(&plan, &dialect).unwrap();

    // Verify SQL structure
    assert!(
        sql.contains("CREATE OR REPLACE TABLE"),
        "expected CTAS, got: {sql}"
    );
    assert!(
        sql.contains("acme_warehouse.staging__us_west__shopify.orders"),
        "expected target table ref in SQL"
    );
    assert!(
        sql.contains("source_catalog.src__acme__us_west__shopify.orders"),
        "expected source table ref in SQL"
    );
    assert!(
        sql.contains("CAST(NULL AS STRING) AS _loaded_by"),
        "expected metadata column"
    );
    // Full refresh should NOT have a WHERE clause
    assert!(!sql.contains("WHERE"), "full refresh should not have WHERE");

    // Verify no watermark exists yet (fresh state store)
    let wm = store
        .get_watermark("acme_warehouse.staging__us_west__shopify.orders")
        .unwrap();
    assert!(wm.is_none(), "no watermark should exist before first run");
}

// ===========================================================================
// Test 2: Incremental SQL generation with watermark
// ===========================================================================

#[test]
fn test_incremental_pipeline_with_watermark() {
    let (store, _dir) = temp_state();
    let dialect = TestDialect;

    let table_key = "acme_warehouse.staging__us_west__shopify.orders";

    // Set a watermark (simulating a previous successful run)
    let now = chrono::Utc::now();
    store
        .set_watermark(
            table_key,
            &WatermarkState {
                last_value: now,
                updated_at: now,
            },
        )
        .unwrap();

    // Build an incremental plan using the stored watermark
    let plan = sample_replication_plan(MaterializationStrategy::Incremental {
        timestamp_column: "_fivetran_synced".into(),
        watermark: store.get_watermark(table_key).unwrap(),
    });

    // Generate INSERT INTO ... SELECT SQL (incremental append)
    let sql = sql_gen::generate_insert_sql(&plan, &dialect).unwrap();

    // Verify incremental SQL structure
    assert!(
        sql.contains("INSERT INTO"),
        "expected INSERT INTO, got: {sql}"
    );
    assert!(
        sql.contains("acme_warehouse.staging__us_west__shopify.orders"),
        "expected target ref"
    );
    assert!(
        sql.contains("_fivetran_synced"),
        "expected timestamp column in WHERE"
    );
    assert!(
        sql.contains("WHERE _fivetran_synced > ("),
        "expected watermark WHERE clause"
    );
    assert!(
        sql.contains("COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')"),
        "expected watermark subquery"
    );

    // Verify the watermark was persisted
    let stored_wm = store.get_watermark(table_key).unwrap().unwrap();
    assert_eq!(stored_wm.last_value, now);
}

// ===========================================================================
// Test 3: Schema drift detection
// ===========================================================================

#[test]
fn test_drift_detection_type_mismatch() {
    let dialect = TestDialect;

    let table = TableRef {
        catalog: "acme_warehouse".into(),
        schema: "staging".into(),
        table: "orders".into(),
    };

    // Source has incompatible types (narrowing / incompatible changes)
    let source_cols = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
        },
        ColumnInfo {
            name: "amount".into(),
            data_type: "ARRAY<STRING>".into(),
            nullable: true,
        },
    ];
    let target_cols = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "STRING".into(),
            nullable: false,
        },
        ColumnInfo {
            name: "amount".into(),
            data_type: "DOUBLE".into(),
            nullable: true,
        },
    ];

    let result = drift::detect_drift(&table, &source_cols, &target_cols);

    // Incompatible type changes trigger DropAndRecreate
    assert_eq!(
        result.action,
        DriftAction::DropAndRecreate,
        "incompatible type changes should trigger DropAndRecreate"
    );
    assert_eq!(
        result.drifted_columns.len(),
        2,
        "expected 2 drifted columns"
    );
    assert_eq!(result.drifted_columns[0].name, "id");
    assert_eq!(result.drifted_columns[0].source_type, "INT");
    assert_eq!(result.drifted_columns[0].target_type, "STRING");
    assert_eq!(result.drifted_columns[1].name, "amount");
    assert_eq!(result.drifted_columns[1].source_type, "ARRAY<STRING>");
    assert_eq!(result.drifted_columns[1].target_type, "DOUBLE");

    // Generate DROP TABLE SQL (what happens after drift detection)
    let drop_sql = drift::generate_drop_table_sql(&table, &dialect).unwrap();
    assert!(
        drop_sql.contains("DROP TABLE IF EXISTS"),
        "expected DROP TABLE: {drop_sql}"
    );
    assert!(
        drop_sql.contains("acme_warehouse.staging.orders"),
        "expected table ref in DROP"
    );

    // Generate DESCRIBE TABLE SQL (how drift was detected)
    let describe_sql = drift::generate_describe_table_sql(&table, &dialect).unwrap();
    assert!(
        describe_sql.contains("DESCRIBE TABLE acme_warehouse.staging.orders"),
        "expected DESCRIBE TABLE"
    );
}

#[test]
fn test_drift_detection_no_drift() {
    let table = TableRef {
        catalog: "acme_warehouse".into(),
        schema: "staging".into(),
        table: "orders".into(),
    };

    let cols = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
        },
        ColumnInfo {
            name: "name".into(),
            data_type: "STRING".into(),
            nullable: true,
        },
    ];

    let result = drift::detect_drift(&table, &cols, &cols);
    assert_eq!(result.action, DriftAction::Ignore);
    assert!(result.drifted_columns.is_empty());
}

#[test]
fn test_drift_new_column_not_drift() {
    let table = TableRef {
        catalog: "acme_warehouse".into(),
        schema: "staging".into(),
        table: "orders".into(),
    };

    let source_cols = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "INT".into(),
            nullable: false,
        },
        ColumnInfo {
            name: "new_col".into(),
            data_type: "STRING".into(),
            nullable: true,
        },
    ];
    let target_cols = vec![ColumnInfo {
        name: "id".into(),
        data_type: "INT".into(),
        nullable: false,
    }];

    // New columns in source are NOT drift (SELECT * picks them up)
    let result = drift::detect_drift(&table, &source_cols, &target_cols);
    assert_eq!(result.action, DriftAction::Ignore);
    assert!(result.drifted_columns.is_empty());
}

// ===========================================================================
// Test 4: Run history state tracking
// ===========================================================================

#[test]
fn test_run_history_flow() {
    use rocky_core::state::{ModelExecution, RunRecord, RunStatus, RunTrigger};

    let (store, _dir) = temp_state();

    // Record a run with multiple model executions
    let now = chrono::Utc::now();
    let run = RunRecord {
        run_id: "run-001".to_string(),
        started_at: now,
        finished_at: now,
        status: RunStatus::Success,
        models_executed: vec![
            ModelExecution {
                model_name: "orders".to_string(),
                started_at: now,
                finished_at: now,
                duration_ms: 500,
                rows_affected: Some(1000),
                status: "success".to_string(),
                sql_hash: "hash1".to_string(),
                bytes_scanned: None,
                bytes_written: None,
            },
            ModelExecution {
                model_name: "customers".to_string(),
                started_at: now,
                finished_at: now,
                duration_ms: 300,
                rows_affected: Some(500),
                status: "success".to_string(),
                sql_hash: "hash2".to_string(),
                bytes_scanned: None,
                bytes_written: None,
            },
        ],
        trigger: RunTrigger::Manual,
        config_hash: "config-abc".to_string(),
    };

    store.record_run(&run).unwrap();

    // Retrieve and verify
    let retrieved = store.get_run("run-001").unwrap().unwrap();
    assert_eq!(retrieved.run_id, "run-001");
    assert_eq!(retrieved.models_executed.len(), 2);
    assert_eq!(retrieved.models_executed[0].model_name, "orders");
    assert_eq!(retrieved.models_executed[0].duration_ms, 500);
    assert_eq!(retrieved.models_executed[0].rows_affected, Some(1000));
    assert_eq!(retrieved.models_executed[1].model_name, "customers");

    // Verify model history lookup
    let history = store.get_model_history("orders", 10).unwrap();
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].duration_ms, 500);

    // Verify average duration
    let avg = store.get_average_duration("orders", 10).unwrap().unwrap();
    assert!((avg - 500.0).abs() < 0.01);

    // Nonexistent run
    assert!(store.get_run("nonexistent").unwrap().is_none());
}

// ===========================================================================
// Test 5: Transformation SQL generation
// ===========================================================================

#[test]
fn test_transformation_full_refresh() {
    let dialect = TestDialect;

    let plan = TransformationPlan {
        sources: vec![SourceRef {
            catalog: "source".into(),
            schema: "raw".into(),
            table: "orders".into(),
        }],
        target: TargetRef {
            catalog: "analytics".into(),
            schema: "marts".into(),
            table: "order_summary".into(),
        },
        strategy: MaterializationStrategy::FullRefresh,
        sql: "SELECT customer_id, COUNT(*) as order_count FROM source.raw.orders GROUP BY customer_id".into(),
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        format: None,
        format_options: None,
    };

    let stmts = sql_gen::generate_transformation_sql(&plan, &dialect).unwrap();
    assert_eq!(stmts.len(), 1);
    let sql = &stmts[0];

    assert!(
        sql.contains("CREATE OR REPLACE TABLE"),
        "expected CTAS for full refresh: {sql}"
    );
    assert!(
        sql.contains("analytics.marts.order_summary"),
        "expected target ref"
    );
    assert!(sql.contains("COUNT(*)"), "expected aggregation");
    assert!(
        sql.contains("GROUP BY customer_id"),
        "expected GROUP BY clause"
    );
}

#[test]
fn test_transformation_incremental() {
    let dialect = TestDialect;

    let plan = TransformationPlan {
        sources: vec![SourceRef {
            catalog: "cat".into(),
            schema: "raw".into(),
            table: "events".into(),
        }],
        target: TargetRef {
            catalog: "cat".into(),
            schema: "silver".into(),
            table: "fct_events".into(),
        },
        strategy: MaterializationStrategy::Incremental {
            timestamp_column: "updated_at".into(),
            watermark: None,
        },
        sql: "SELECT * FROM cat.raw.events WHERE updated_at > '2026-01-01'".into(),
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        format: None,
        format_options: None,
    };

    let stmts = sql_gen::generate_transformation_sql(&plan, &dialect).unwrap();
    assert_eq!(stmts.len(), 1);
    let sql = &stmts[0];
    assert!(
        sql.starts_with("INSERT INTO cat.silver.fct_events"),
        "expected INSERT INTO: {sql}"
    );
}

#[test]
fn test_transformation_merge() {
    let dialect = TestDialect;

    let plan = TransformationPlan {
        sources: vec![SourceRef {
            catalog: "cat".into(),
            schema: "raw".into(),
            table: "users".into(),
        }],
        target: TargetRef {
            catalog: "cat".into(),
            schema: "silver".into(),
            table: "dim_users".into(),
        },
        strategy: MaterializationStrategy::Merge {
            unique_key: vec!["user_id".into()],
            update_columns: ColumnSelection::All,
        },
        sql: "SELECT user_id, name, email FROM cat.raw.users".into(),
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
        format: None,
        format_options: None,
    };

    let stmts = sql_gen::generate_transformation_sql(&plan, &dialect).unwrap();
    assert_eq!(stmts.len(), 1);
    let sql = &stmts[0];
    assert!(
        sql.contains("MERGE INTO cat.silver.dim_users AS t"),
        "expected MERGE INTO: {sql}"
    );
    assert!(
        sql.contains("ON t.user_id = s.user_id"),
        "expected ON clause"
    );
}

// ===========================================================================
// Test 6: Merge SQL generation (replication)
// ===========================================================================

#[test]
fn test_merge_sql_generation() {
    let dialect = TestDialect;

    let plan = ReplicationPlan {
        source: SourceRef {
            catalog: "source_catalog".into(),
            schema: "src__acme__us_west__shopify".into(),
            table: "customers".into(),
        },
        target: TargetRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west__shopify".into(),
            table: "customers".into(),
        },
        strategy: MaterializationStrategy::Merge {
            unique_key: vec!["customer_id".into()],
            update_columns: ColumnSelection::All,
        },
        columns: ColumnSelection::All,
        metadata_columns: vec![],
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
    };

    let sql = sql_gen::generate_merge_sql(&plan, &dialect).unwrap();

    assert!(sql.contains("MERGE INTO"), "expected MERGE INTO: {sql}");
    assert!(
        sql.contains("ON t.customer_id = s.customer_id"),
        "expected ON clause"
    );
    assert!(
        sql.contains("WHEN MATCHED THEN UPDATE SET *"),
        "expected UPDATE SET *"
    );
    assert!(
        sql.contains("WHEN NOT MATCHED THEN INSERT *"),
        "expected INSERT *"
    );
}

#[test]
fn test_merge_composite_key() {
    let dialect = TestDialect;

    let plan = ReplicationPlan {
        source: SourceRef {
            catalog: "source_catalog".into(),
            schema: "src__acme__us_west__shopify".into(),
            table: "order_items".into(),
        },
        target: TargetRef {
            catalog: "acme_warehouse".into(),
            schema: "staging__us_west__shopify".into(),
            table: "order_items".into(),
        },
        strategy: MaterializationStrategy::Merge {
            unique_key: vec!["order_id".into(), "line_id".into()],
            update_columns: ColumnSelection::Explicit(vec!["status".into(), "amount".into()]),
        },
        columns: ColumnSelection::All,
        metadata_columns: vec![],
        governance: GovernanceConfig {
            permissions_file: None,
            auto_create_catalogs: false,
            auto_create_schemas: false,
        },
    };

    let sql = sql_gen::generate_merge_sql(&plan, &dialect).unwrap();
    assert!(
        sql.contains("ON t.order_id = s.order_id AND t.line_id = s.line_id"),
        "expected composite ON clause: {sql}"
    );
    assert!(
        sql.contains("UPDATE SET t.status = s.status, t.amount = s.amount"),
        "expected explicit update columns: {sql}"
    );
}

// ===========================================================================
// Test 7: Watermark lifecycle (set, get, update, delete)
// ===========================================================================

#[test]
fn test_watermark_lifecycle() {
    let (store, _dir) = temp_state();
    let table_key = "acme_warehouse.staging.orders";

    // Initially no watermark
    assert!(store.get_watermark(table_key).unwrap().is_none());

    // Set initial watermark
    let t1 = chrono::Utc::now();
    store
        .set_watermark(
            table_key,
            &WatermarkState {
                last_value: t1,
                updated_at: t1,
            },
        )
        .unwrap();
    assert_eq!(
        store.get_watermark(table_key).unwrap().unwrap().last_value,
        t1
    );

    // Update watermark (simulating a second successful run)
    let t2 = chrono::Utc::now();
    store
        .set_watermark(
            table_key,
            &WatermarkState {
                last_value: t2,
                updated_at: t2,
            },
        )
        .unwrap();
    assert_eq!(
        store.get_watermark(table_key).unwrap().unwrap().last_value,
        t2
    );

    // Delete watermark (simulating a forced full refresh)
    let removed = store.delete_watermark(table_key).unwrap();
    assert!(removed);
    assert!(store.get_watermark(table_key).unwrap().is_none());

    // Delete nonexistent returns false
    assert!(!store.delete_watermark(table_key).unwrap());
}

// ===========================================================================
// Test 8: Multiple watermarks across tables
// ===========================================================================

#[test]
fn test_multiple_watermarks_isolation() {
    let (store, _dir) = temp_state();
    let now = chrono::Utc::now();

    let tables = vec![
        "acme_warehouse.staging__us_west__shopify.orders",
        "acme_warehouse.staging__us_west__shopify.customers",
        "acme_warehouse.staging__eu_west__stripe.payments",
    ];

    // Set watermarks for all tables
    for table_key in &tables {
        store
            .set_watermark(
                table_key,
                &WatermarkState {
                    last_value: now,
                    updated_at: now,
                },
            )
            .unwrap();
    }

    // Verify all watermarks are stored
    let all = store.list_watermarks().unwrap();
    assert_eq!(all.len(), 3);

    // Delete one watermark and verify others are untouched
    store.delete_watermark(tables[1]).unwrap();
    let remaining = store.list_watermarks().unwrap();
    assert_eq!(remaining.len(), 2);
    let keys: Vec<&str> = remaining.iter().map(|(k, _)| k.as_str()).collect();
    assert!(keys.contains(&tables[0]));
    assert!(!keys.contains(&tables[1]));
    assert!(keys.contains(&tables[2]));
}

// ===========================================================================
// Test 9: SQL injection prevention
// ===========================================================================

#[test]
fn test_sql_injection_prevention() {
    let dialect = TestDialect;

    // Unsafe catalog name
    let bad_plan = ReplicationPlan {
        source: SourceRef {
            catalog: "catalog; DROP TABLE users--".into(),
            schema: "schema".into(),
            table: "table".into(),
        },
        ..sample_replication_plan(MaterializationStrategy::FullRefresh)
    };
    assert!(
        sql_gen::generate_select_sql(&bad_plan, &dialect).is_err(),
        "should reject SQL injection in catalog name"
    );

    // Unsafe schema name
    let bad_plan = ReplicationPlan {
        source: SourceRef {
            catalog: "cat".into(),
            schema: "sch' OR '1'='1".into(),
            table: "tbl".into(),
        },
        ..sample_replication_plan(MaterializationStrategy::FullRefresh)
    };
    assert!(
        sql_gen::generate_select_sql(&bad_plan, &dialect).is_err(),
        "should reject SQL injection in schema name"
    );

    // Unsafe metadata column name
    let bad_plan = ReplicationPlan {
        metadata_columns: vec![MetadataColumn {
            name: "col; DROP TABLE".into(),
            data_type: "STRING".into(),
            value: "NULL".into(),
        }],
        ..sample_replication_plan(MaterializationStrategy::FullRefresh)
    };
    assert!(
        sql_gen::generate_select_sql(&bad_plan, &dialect).is_err(),
        "should reject SQL injection in metadata column name"
    );

    // Unsafe table reference in drift detection
    let bad_table = TableRef {
        catalog: "bad; DROP".into(),
        schema: "schema".into(),
        table: "table".into(),
    };
    assert!(
        drift::generate_describe_table_sql(&bad_table, &dialect).is_err(),
        "should reject bad identifier in DESCRIBE TABLE"
    );
    assert!(
        drift::generate_drop_table_sql(&bad_table, &dialect).is_err(),
        "should reject bad identifier in DROP TABLE"
    );
}

// ===========================================================================
// Test 10: Full pipeline flow (generate -> verify -> state update)
// ===========================================================================

#[test]
fn test_full_pipeline_flow_incremental_to_full_refresh() {
    let (store, _dir) = temp_state();
    let dialect = TestDialect;
    let table_key = "acme_warehouse.staging__us_west__shopify.orders";

    // --- Phase 1: First run (no watermark -> full refresh) ---
    let wm = store.get_watermark(table_key).unwrap();
    assert!(wm.is_none(), "no watermark on first run");

    let plan = sample_replication_plan(MaterializationStrategy::FullRefresh);
    let sql = sql_gen::generate_create_table_as_sql(&plan, &dialect).unwrap();
    assert!(sql.contains("CREATE OR REPLACE TABLE"));
    assert!(!sql.contains("WHERE"));

    // Simulate successful run: set watermark
    let t1 = chrono::Utc::now();
    store
        .set_watermark(
            table_key,
            &WatermarkState {
                last_value: t1,
                updated_at: t1,
            },
        )
        .unwrap();

    // --- Phase 2: Second run (watermark exists -> incremental) ---
    let wm = store.get_watermark(table_key).unwrap();
    assert!(wm.is_some(), "watermark should exist after first run");

    let plan = sample_replication_plan(MaterializationStrategy::Incremental {
        timestamp_column: "_fivetran_synced".into(),
        watermark: wm,
    });
    let sql = sql_gen::generate_insert_sql(&plan, &dialect).unwrap();
    assert!(sql.contains("INSERT INTO"));
    assert!(sql.contains("WHERE _fivetran_synced > ("));

    // Simulate successful run: update watermark
    let t2 = chrono::Utc::now();
    store
        .set_watermark(
            table_key,
            &WatermarkState {
                last_value: t2,
                updated_at: t2,
            },
        )
        .unwrap();

    // --- Phase 3: Drift detected -> delete watermark -> full refresh ---
    let source_cols = vec![ColumnInfo {
        name: "amount".into(),
        data_type: "DOUBLE".into(),
        nullable: true,
    }];
    let target_cols = vec![ColumnInfo {
        name: "amount".into(),
        data_type: "INT".into(),
        nullable: true,
    }];
    let table = TableRef {
        catalog: "acme_warehouse".into(),
        schema: "staging__us_west__shopify".into(),
        table: "orders".into(),
    };
    let drift_result = drift::detect_drift(&table, &source_cols, &target_cols);
    assert_eq!(drift_result.action, DriftAction::DropAndRecreate);

    // On drift: delete watermark to force full refresh
    store.delete_watermark(table_key).unwrap();
    assert!(store.get_watermark(table_key).unwrap().is_none());

    // Generate drop + full refresh SQL
    let drop_sql = drift::generate_drop_table_sql(&table, &dialect).unwrap();
    assert!(drop_sql.contains("DROP TABLE IF EXISTS"));

    let plan = sample_replication_plan(MaterializationStrategy::FullRefresh);
    let ctas_sql = sql_gen::generate_create_table_as_sql(&plan, &dialect).unwrap();
    assert!(ctas_sql.contains("CREATE OR REPLACE TABLE"));
}

// ===========================================================================
// Test 11: Row count anomaly detection with state store
// ===========================================================================

#[test]
fn test_anomaly_detection_with_state() {
    let (store, _dir) = temp_state();
    let table_key = "acme_warehouse.staging.orders";

    // Build up a baseline (5 runs with ~1000 rows)
    for count in [980, 1010, 995, 1020, 1005] {
        store.record_row_count(table_key, count, 10).unwrap();
    }

    let history = store.get_check_history(table_key).unwrap();
    assert_eq!(history.len(), 5);

    // Normal count: should not be anomaly
    let normal = rocky_core::state::detect_anomaly(table_key, 1015, &history, 50.0);
    assert!(!normal.is_anomaly, "1015 should be within normal range");

    // Spike: 5000 rows (way above baseline ~1002)
    let spike = rocky_core::state::detect_anomaly(table_key, 5000, &history, 50.0);
    assert!(spike.is_anomaly, "5000 should be detected as spike");
    assert!(spike.reason.contains("spiked"));

    // Drop: 0 rows
    let drop_result = rocky_core::state::detect_anomaly(table_key, 0, &history, 50.0);
    assert!(drop_result.is_anomaly, "0 should be detected as drop");
    assert!(drop_result.reason.contains("dropped"));
}

// ===========================================================================
// Test 12: Exact SQL output matches expected patterns
// ===========================================================================

#[test]
fn test_exact_incremental_sql_output() {
    let dialect = TestDialect;

    let plan = sample_replication_plan(MaterializationStrategy::Incremental {
        timestamp_column: "_fivetran_synced".into(),
        watermark: None,
    });

    let sql = sql_gen::generate_select_sql(&plan, &dialect).unwrap();

    let expected = "\
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM acme_warehouse.staging__us_west__shopify.orders
)";

    assert_eq!(sql, expected, "incremental SELECT SQL must match exactly");
}

#[test]
fn test_exact_full_refresh_sql_output() {
    let dialect = TestDialect;

    let plan = sample_replication_plan(MaterializationStrategy::FullRefresh);
    let sql = sql_gen::generate_select_sql(&plan, &dialect).unwrap();

    let expected = "\
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders";

    assert_eq!(sql, expected, "full refresh SELECT SQL must match exactly");
}

#[test]
fn test_exact_ctas_sql_output() {
    let dialect = TestDialect;

    let plan = sample_replication_plan(MaterializationStrategy::FullRefresh);
    let sql = sql_gen::generate_create_table_as_sql(&plan, &dialect).unwrap();

    let expected = "\
CREATE OR REPLACE TABLE acme_warehouse.staging__us_west__shopify.orders AS
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders";

    assert_eq!(sql, expected, "CTAS SQL must match exactly");
}

#[test]
fn test_exact_insert_sql_output() {
    let dialect = TestDialect;

    let plan = sample_replication_plan(MaterializationStrategy::Incremental {
        timestamp_column: "_fivetran_synced".into(),
        watermark: None,
    });
    let sql = sql_gen::generate_insert_sql(&plan, &dialect).unwrap();

    let expected = "\
INSERT INTO acme_warehouse.staging__us_west__shopify.orders
SELECT *, CAST(NULL AS STRING) AS _loaded_by
FROM source_catalog.src__acme__us_west__shopify.orders
WHERE _fivetran_synced > (
    SELECT COALESCE(MAX(_fivetran_synced), TIMESTAMP '1970-01-01')
    FROM acme_warehouse.staging__us_west__shopify.orders
)";

    assert_eq!(sql, expected, "INSERT SQL must match exactly");
}

// ===========================================================================
// Test 9: time_interval end-to-end against in-process DuckDB (Phase 4)
// ===========================================================================
//
// These tests exercise the FULL time_interval execution path against a real
// in-process DuckDB, using the actual rocky-duckdb dialect (not the in-file
// TestDialect — that one emits Databricks-style REPLACE WHERE which DuckDB
// doesn't support).
//
// Pattern per test:
// 1. Open an in-memory DuckDB via DuckDbWarehouseAdapter::in_memory()
// 2. Create a source table and insert seed rows spanning multiple days.
// 3. Pre-create the target table with the right schema (the time_interval
//    DELETE+INSERT path requires the table to exist; the runtime would
//    handle the bootstrap separately, but here we just CREATE first).
// 4. Build a TransformationPlan with MaterializationStrategy::TimeInterval
//    and an explicit PartitionWindow for the partition under test.
// 5. Call sql_gen::generate_transformation_sql() with the rocky-duckdb dialect.
// 6. Execute each statement in order.
// 7. Query the target and assert.

mod time_interval_e2e {
    use rocky_core::incremental::{PartitionRecord, PartitionStatus, partition_key_to_window};
    use rocky_core::ir::{
        GovernanceConfig, MaterializationStrategy, SourceRef, TargetRef, TransformationPlan,
    };
    use rocky_core::models::TimeGrain;
    use rocky_core::sql_gen;
    use rocky_core::state::StateStore;
    use rocky_core::traits::{SqlDialect, WarehouseAdapter};
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;
    use tempfile::TempDir;

    /// SQL to seed a stg_orders table with rows across 5 partitions
    /// (2026-04-04 through 2026-04-08), 2 customers, varying revenue.
    const SEED_INSERT_SQL: &str = "INSERT INTO main.stg_orders VALUES \
        ('2026-04-04 09:00:00'::TIMESTAMP, 1, 100.0), \
        ('2026-04-04 14:00:00'::TIMESTAMP, 2, 50.0),  \
        ('2026-04-05 10:00:00'::TIMESTAMP, 1, 200.0), \
        ('2026-04-06 11:00:00'::TIMESTAMP, 2, 75.0),  \
        ('2026-04-06 16:00:00'::TIMESTAMP, 2, 25.0),  \
        ('2026-04-07 08:00:00'::TIMESTAMP, 1, 300.0), \
        ('2026-04-07 13:00:00'::TIMESTAMP, 1, 150.0), \
        ('2026-04-07 18:00:00'::TIMESTAMP, 2, 80.0),  \
        ('2026-04-08 12:00:00'::TIMESTAMP, 1, 500.0)";

    /// Create a stg_orders table + insert seed data. Returns the adapter.
    async fn setup_with_seeds() -> DuckDbWarehouseAdapter {
        let adapter = DuckDbWarehouseAdapter::in_memory().expect("open in-memory duckdb");
        adapter
            .execute_statement(
                "CREATE TABLE main.stg_orders (\
                    order_at TIMESTAMP NOT NULL, \
                    customer_id INTEGER NOT NULL, \
                    amount DOUBLE NOT NULL\
                )",
            )
            .await
            .expect("create stg_orders");
        adapter
            .execute_statement(SEED_INSERT_SQL)
            .await
            .expect("insert seeds");
        // Pre-create the empty target table with the right schema. The
        // time_interval DELETE+INSERT cycle requires the table to exist;
        // the runtime would create it on first run, but here we just
        // bootstrap directly so each test starts from a clean slate.
        adapter
            .execute_statement(
                "CREATE TABLE main.fct_daily_orders AS \
                 SELECT \
                    CAST(order_at AS DATE) AS order_date, \
                    customer_id, \
                    COUNT(*) AS order_count, \
                    SUM(amount) AS revenue \
                 FROM main.stg_orders \
                 WHERE FALSE \
                 GROUP BY 1, 2",
            )
            .await
            .expect("create empty target");
        adapter
    }

    /// Build the TransformationPlan for the fct_daily_orders model with the
    /// given partition window populated.
    fn time_interval_plan(window_key: &str) -> TransformationPlan {
        let window = partition_key_to_window(TimeGrain::Day, window_key).expect("valid daily key");
        TransformationPlan {
            sources: vec![SourceRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "stg_orders".into(),
            }],
            target: TargetRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "fct_daily_orders".into(),
            },
            strategy: MaterializationStrategy::TimeInterval {
                time_column: "order_date".into(),
                granularity: TimeGrain::Day,
                window: Some(window),
            },
            // The model SQL: aggregate by date + customer, with the
            // @start_date / @end_date placeholders gating the upstream scan.
            sql: "SELECT \
                CAST(order_at AS DATE) AS order_date, \
                customer_id, \
                COUNT(*) AS order_count, \
                SUM(amount) AS revenue \
              FROM main.stg_orders \
              WHERE order_at >= @start_date AND order_at < @end_date \
              GROUP BY 1, 2"
                .into(),
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            format: None,
            format_options: None,
        }
    }

    /// Extract a JSON value as u64. Used to parse COUNT(*) results, which
    /// duckdb returns as a JSON number.
    fn as_u64(v: &serde_json::Value) -> u64 {
        v.as_u64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<u64>().ok()))
            .unwrap_or_else(|| panic!("expected u64, got {v:?}"))
    }

    /// Extract a JSON value as f64. Used to parse SUM(amount) results.
    fn as_f64(v: &serde_json::Value) -> f64 {
        v.as_f64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<f64>().ok()))
            .unwrap_or_else(|| panic!("expected f64, got {v:?}"))
    }

    /// Extract a JSON value as i64. For customer_id columns.
    fn as_i64(v: &serde_json::Value) -> i64 {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            .unwrap_or_else(|| panic!("expected i64, got {v:?}"))
    }

    /// Execute the Vec<String> from generate_transformation_sql against the
    /// adapter, in order. Returns the number of rows in the target afterward.
    async fn execute_partition(adapter: &DuckDbWarehouseAdapter, plan: &TransformationPlan) -> u64 {
        let dialect: &dyn SqlDialect = adapter.dialect();
        let stmts = sql_gen::generate_transformation_sql(plan, dialect)
            .expect("generate time_interval SQL");
        // Sanity: the duckdb dialect emits 4 statements
        // (BEGIN/DELETE/INSERT/COMMIT) per Phase 2C.
        assert_eq!(stmts.len(), 4, "duckdb dialect should emit 4 statements");
        for stmt in &stmts {
            adapter
                .execute_statement(stmt)
                .await
                .expect("execute partition statement");
        }
        let result = adapter
            .execute_query("SELECT COUNT(*) FROM main.fct_daily_orders")
            .await
            .expect("count rows");
        as_u64(&result.rows[0][0])
    }

    #[tokio::test]
    async fn test_time_interval_single_partition_writes_only_window() {
        let adapter = setup_with_seeds().await;
        let plan = time_interval_plan("2026-04-07");
        let total_rows = execute_partition(&adapter, &plan).await;

        // 04-07 has 3 raw rows: customer 1 at 08:00 + 13:00 → 1 group,
        // customer 2 at 18:00 → 1 group. So 2 output rows.
        assert_eq!(
            total_rows, 2,
            "only 04-07 partition should be present after first run"
        );

        // Verify the actual content matches.
        let revenue = adapter
            .execute_query(
                "SELECT customer_id, order_count, revenue \
                 FROM main.fct_daily_orders \
                 WHERE order_date = '2026-04-07' \
                 ORDER BY customer_id",
            )
            .await
            .expect("query");
        assert_eq!(revenue.rows.len(), 2);
        // customer 1: 2 orders, $300 + $150 = $450
        assert_eq!(as_i64(&revenue.rows[0][0]), 1);
        assert_eq!(as_i64(&revenue.rows[0][1]), 2);
        assert_eq!(as_f64(&revenue.rows[0][2]), 450.0);
        // customer 2: 1 order, $80
        assert_eq!(as_i64(&revenue.rows[1][0]), 2);
        assert_eq!(as_i64(&revenue.rows[1][1]), 1);
        assert_eq!(as_f64(&revenue.rows[1][2]), 80.0);
    }

    #[tokio::test]
    async fn test_time_interval_idempotent_rerun() {
        let adapter = setup_with_seeds().await;
        let plan = time_interval_plan("2026-04-07");

        // Run partition 04-07 twice. Idempotency: row count must NOT
        // double on the second run.
        let first_count = execute_partition(&adapter, &plan).await;
        let second_count = execute_partition(&adapter, &plan).await;
        assert_eq!(
            first_count, second_count,
            "rerun must be idempotent — got {first_count} then {second_count}"
        );
        assert_eq!(first_count, 2);
    }

    #[tokio::test]
    async fn test_time_interval_multiple_partitions_accumulate() {
        let adapter = setup_with_seeds().await;

        // Run 04-04, 04-05, 04-06, 04-07 in sequence — each adds its own
        // partition's rows without disturbing the others.
        let mut total = 0u64;
        for key in ["2026-04-04", "2026-04-05", "2026-04-06", "2026-04-07"] {
            let plan = time_interval_plan(key);
            total = execute_partition(&adapter, &plan).await;
        }

        // Expected output rows per partition:
        //   04-04 → 2 (customer 1 + customer 2)
        //   04-05 → 1 (customer 1 only)
        //   04-06 → 1 (customer 2 has 2 orders → 1 group)
        //   04-07 → 2 (customer 1 + customer 2)
        // Total = 6
        assert_eq!(total, 6, "all 4 partitions should be present");

        // Verify each partition's data is independent.
        let result = adapter
            .execute_query(
                "SELECT order_date, COUNT(*) \
                 FROM main.fct_daily_orders \
                 GROUP BY order_date \
                 ORDER BY order_date",
            )
            .await
            .expect("group by date");
        assert_eq!(result.rows.len(), 4);
    }

    #[tokio::test]
    async fn test_time_interval_rerun_with_new_upstream_data_picks_up_changes() {
        let adapter = setup_with_seeds().await;
        let plan = time_interval_plan("2026-04-07");

        // First run with the original seed → 2 output rows.
        let count_before = execute_partition(&adapter, &plan).await;
        assert_eq!(count_before, 2);

        // Late-arriving data: a new order shows up for 04-07 after the
        // partition was already computed. This is the scenario watermark-
        // incremental fails on but time_interval handles via re-run.
        adapter
            .execute_statement(
                "INSERT INTO main.stg_orders VALUES \
                 ('2026-04-07 22:00:00'::TIMESTAMP, 3, 999.0)",
            )
            .await
            .expect("insert late row");

        // Re-run the same partition. The DELETE+INSERT cycle wipes the
        // partition and recomputes it from the now-larger upstream.
        let count_after = execute_partition(&adapter, &plan).await;
        assert_eq!(
            count_after, 3,
            "re-run after late arrival should add the new customer's group"
        );

        // Verify the late row is present.
        let result = adapter
            .execute_query(
                "SELECT customer_id, revenue \
                 FROM main.fct_daily_orders \
                 WHERE order_date = '2026-04-07' AND customer_id = 3",
            )
            .await
            .expect("query late row");
        assert_eq!(result.rows.len(), 1);
        assert_eq!(as_f64(&result.rows[0][1]), 999.0);
    }

    #[tokio::test]
    async fn test_time_interval_state_store_records_lifecycle() {
        // This test does NOT execute SQL — it just verifies the state-store
        // record_partition / get_partition / list_partitions API works the
        // way the runtime executor uses it. SQL execution is covered by the
        // tests above.
        let dir = TempDir::new().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();

        // Mark a partition InProgress (start of a run).
        let mut record = PartitionRecord {
            model_name: "fct_daily_orders".into(),
            partition_key: "2026-04-07".into(),
            status: PartitionStatus::InProgress,
            computed_at: chrono::Utc::now(),
            row_count: 0,
            duration_ms: 0,
            run_id: "test-run".into(),
            checksum: None,
        };
        store.record_partition(&record).unwrap();

        // Verify InProgress visible.
        let fetched = store
            .get_partition("fct_daily_orders", "2026-04-07")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.status, PartitionStatus::InProgress);

        // Mark Computed (end of a successful run).
        record.status = PartitionStatus::Computed;
        record.row_count = 42;
        record.duration_ms = 123;
        store.record_partition(&record).unwrap();

        let fetched = store
            .get_partition("fct_daily_orders", "2026-04-07")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.status, PartitionStatus::Computed);
        assert_eq!(fetched.row_count, 42);

        // list_partitions returns the single partition.
        let listed = store.list_partitions("fct_daily_orders").unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].partition_key, "2026-04-07");
    }

    #[tokio::test]
    async fn test_time_interval_partition_window_substitutes_into_sql() {
        // Verify that the @start_date / @end_date placeholder substitution
        // produced by sql_gen actually filters the upstream correctly. This
        // is the SQL-level proof that the placeholder substitution from
        // Phase 2D works against a real warehouse.
        let adapter = setup_with_seeds().await;
        let plan = time_interval_plan("2026-04-05"); // expect 1 customer, 1 order

        let dialect: &dyn SqlDialect = adapter.dialect();
        let stmts = sql_gen::generate_transformation_sql(&plan, dialect).unwrap();

        // The INSERT statement (index 2 in the BEGIN/DELETE/INSERT/COMMIT vec)
        // should contain the substituted timestamp literals, not the
        // @start_date / @end_date tokens.
        let insert_stmt = &stmts[2];
        assert!(
            insert_stmt.contains("'2026-04-05 00:00:00'"),
            "@start_date should be substituted: {insert_stmt}"
        );
        assert!(
            insert_stmt.contains("'2026-04-06 00:00:00'"),
            "@end_date should be substituted: {insert_stmt}"
        );
        assert!(
            !insert_stmt.contains("@start_date"),
            "raw placeholder should be gone: {insert_stmt}"
        );
        assert!(
            !insert_stmt.contains("@end_date"),
            "raw placeholder should be gone: {insert_stmt}"
        );

        // Execute and verify the filter actually scoped the upstream.
        execute_partition(&adapter, &plan).await;
        let result = adapter
            .execute_query("SELECT COUNT(*) FROM main.fct_daily_orders")
            .await
            .unwrap();
        assert_eq!(
            as_u64(&result.rows[0][0]),
            1,
            "04-05 partition should produce exactly 1 row (1 customer that day)"
        );
    }

    #[tokio::test]
    async fn test_time_interval_bootstrap_creates_empty_target_then_partitions_work() {
        // Phase 7A bootstrap proof: starting from a database where the
        // target table does NOT exist, render and execute the bootstrap
        // SQL via sql_gen::generate_time_interval_bootstrap_sql, then
        // verify the table now exists with the right schema and zero
        // rows, and that a subsequent partition run populates it
        // correctly.
        let adapter = DuckDbWarehouseAdapter::in_memory().expect("open duckdb");

        // Set up the source. CRITICALLY: we do NOT pre-create the target.
        adapter
            .execute_statement(
                "CREATE TABLE main.stg_orders (\
                    order_at TIMESTAMP NOT NULL, \
                    customer_id INTEGER NOT NULL, \
                    amount DOUBLE NOT NULL\
                )",
            )
            .await
            .expect("create source");
        adapter
            .execute_statement(SEED_INSERT_SQL)
            .await
            .expect("insert seeds");

        // Step 1: render the bootstrap SQL and execute it.
        let plan = time_interval_plan("2026-04-07");
        let dialect: &dyn SqlDialect = adapter.dialect();
        let bootstrap_sql = sql_gen::generate_time_interval_bootstrap_sql(&plan, dialect)
            .expect("bootstrap render");
        adapter
            .execute_statement(&bootstrap_sql)
            .await
            .expect("execute bootstrap");

        // Step 2: target table now exists with the right schema but zero rows.
        let post_bootstrap = adapter
            .execute_query("SELECT COUNT(*) FROM main.fct_daily_orders")
            .await
            .expect("count after bootstrap");
        assert_eq!(
            as_u64(&post_bootstrap.rows[0][0]),
            0,
            "bootstrap must produce a zero-row table"
        );

        // Verify the column shape via DuckDB's information_schema (proves
        // the bootstrap CREATE TABLE captured the model's output schema).
        let cols = adapter
            .execute_query(
                "SELECT column_name FROM information_schema.columns \
                 WHERE table_schema = 'main' AND table_name = 'fct_daily_orders' \
                 ORDER BY ordinal_position",
            )
            .await
            .expect("describe columns");
        let col_names: Vec<String> = cols
            .rows
            .iter()
            .map(|r| r[0].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            col_names,
            vec!["order_date", "customer_id", "order_count", "revenue"],
            "bootstrap should capture all 4 model output columns"
        );

        // Step 3: run a partition normally — this should work end-to-end
        // because the table now exists.
        let total_rows = execute_partition(&adapter, &plan).await;
        assert_eq!(
            total_rows, 2,
            "after bootstrap + partition, should have 2 rows (the 04-07 partition)"
        );
    }

    #[tokio::test]
    async fn test_time_interval_window_metadata_is_inclusive_start_exclusive_end() {
        // Edge case: a row at exactly the start boundary should be included,
        // a row at exactly the end boundary should be excluded. This is the
        // half-open [start, end) contract from D2 of the plan.
        let adapter = setup_with_seeds().await;

        // Insert two rows at the exact boundaries of 04-07.
        adapter
            .execute_statement(
                "INSERT INTO main.stg_orders VALUES \
                 ('2026-04-07 00:00:00'::TIMESTAMP, 99, 1.0), \
                 ('2026-04-08 00:00:00'::TIMESTAMP, 99, 2.0)",
            )
            .await
            .unwrap();

        let plan = time_interval_plan("2026-04-07");
        execute_partition(&adapter, &plan).await;

        // The 04-07 00:00:00 row is INSIDE the window. The 04-08 00:00:00
        // row is OUTSIDE (exclusive end). Customer 99 should appear with
        // exactly 1 order, revenue = 1.0.
        let result = adapter
            .execute_query(
                "SELECT order_count, revenue \
                 FROM main.fct_daily_orders \
                 WHERE order_date = '2026-04-07' AND customer_id = 99",
            )
            .await
            .unwrap();
        assert_eq!(result.rows.len(), 1);
        assert_eq!(
            as_u64(&result.rows[0][0]),
            1,
            "boundary start should be included"
        );
        assert_eq!(
            as_f64(&result.rows[0][1]),
            1.0,
            "boundary end should be EXCLUDED"
        );
    }
}
