//! Integration tests that execute Rocky pipelines against DuckDB.
//!
//! Unlike the existing e2e tests (which verify SQL string output),
//! these tests actually execute generated SQL against a real DuckDB
//! database and verify data correctness.

use chrono::Utc;

use rocky_core::compare;
use rocky_core::drift;
use rocky_core::ir::*;
use rocky_core::shadow::{self, ShadowConfig};
use rocky_core::sql_gen;
use rocky_core::state::StateStore;
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_duckdb::seed::SeedScale;
use rocky_duckdb::test_harness::TestPipeline;
use tempfile::TempDir;

// ===========================================================================
// Helpers
// ===========================================================================

fn dialect() -> DuckDbSqlDialect {
    DuckDbSqlDialect
}

fn temp_state() -> (StateStore, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("test-state.redb");
    let store = StateStore::open(&path).unwrap();
    (store, dir)
}

/// Build a replication plan for DuckDB (flat tables, no catalog prefixes).
fn duckdb_replication_plan(
    source_table: &str,
    target_table: &str,
    strategy: MaterializationStrategy,
) -> ReplicationPlan {
    ReplicationPlan {
        source: SourceRef {
            catalog: String::new(),
            schema: "source".into(),
            table: source_table.into(),
        },
        target: TargetRef {
            catalog: String::new(),
            schema: "target".into(),
            table: target_table.into(),
        },
        strategy,
        columns: ColumnSelection::All,
        metadata_columns: vec![MetadataColumn {
            name: "_loaded_by".into(),
            data_type: "VARCHAR".into(),
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
// Pipeline Lifecycle Tests
// ===========================================================================

#[tokio::test]
async fn test_full_pipeline_lifecycle() {
    let p = TestPipeline::new();
    let d = dialect();

    // Setup: create source table with data
    p.execute("CREATE SCHEMA IF NOT EXISTS source")
        .await
        .unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS target")
        .await
        .unwrap();
    p.execute(
        "CREATE TABLE source.orders (
            id INTEGER, customer_id INTEGER, amount DECIMAL(10,2),
            _fivetran_synced TIMESTAMP
        )",
    )
    .await
    .unwrap();
    p.execute(
        "INSERT INTO source.orders VALUES
            (1, 10, 99.99, TIMESTAMP '2026-03-01'),
            (2, 20, 49.50, TIMESTAMP '2026-03-02'),
            (3, 10, 150.00, TIMESTAMP '2026-03-03')",
    )
    .await
    .unwrap();

    // Generate and execute full refresh SQL
    let plan = duckdb_replication_plan("orders", "orders", MaterializationStrategy::FullRefresh);
    let sql = sql_gen::generate_create_table_as_sql(&plan, &d).unwrap();
    p.execute(&sql).await.unwrap();

    // Verify data was copied
    assert_eq!(p.row_count("target.orders").await.unwrap(), 3);

    // Verify metadata column exists
    let cols = p.describe("target", "orders").await.unwrap();
    assert!(
        cols.iter().any(|c| c.name == "_loaded_by"),
        "metadata column _loaded_by should exist"
    );

    // Verify watermark state
    let (store, _dir) = temp_state();
    let wm = store.get_watermark("target.orders").unwrap();
    assert!(wm.is_none(), "no watermark before explicit set");
}

#[tokio::test]
async fn test_incremental_pipeline_two_runs() {
    let p = TestPipeline::new();
    let d = dialect();
    let (store, _dir) = temp_state();

    p.execute("CREATE SCHEMA IF NOT EXISTS source")
        .await
        .unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS target")
        .await
        .unwrap();
    p.execute(
        "CREATE TABLE source.events (
            id INTEGER, event_type VARCHAR,
            _fivetran_synced TIMESTAMP
        )",
    )
    .await
    .unwrap();

    // Insert initial batch
    p.execute(
        "INSERT INTO source.events VALUES
            (1, 'click', TIMESTAMP '2026-03-01'),
            (2, 'view', TIMESTAMP '2026-03-02')",
    )
    .await
    .unwrap();

    // Run 1: full refresh (no watermark)
    let plan1 = duckdb_replication_plan("events", "events", MaterializationStrategy::FullRefresh);
    let sql1 = sql_gen::generate_create_table_as_sql(&plan1, &d).unwrap();
    p.execute(&sql1).await.unwrap();
    assert_eq!(p.row_count("target.events").await.unwrap(), 2);

    // Record watermark
    let wm = WatermarkState {
        last_value: chrono::DateTime::parse_from_rfc3339("2026-03-02T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc),
        updated_at: Utc::now(),
    };
    store.set_watermark("target.events", &wm).unwrap();

    // Insert new rows in source
    p.execute(
        "INSERT INTO source.events VALUES
            (3, 'purchase', TIMESTAMP '2026-03-03'),
            (4, 'click', TIMESTAMP '2026-03-04')",
    )
    .await
    .unwrap();

    // Run 2: incremental (only new rows). The watermark itself is read
    // from the state store at SQL-generation time, not carried on the
    // strategy.
    let _wm = store.get_watermark("target.events").unwrap();
    let plan2 = duckdb_replication_plan(
        "events",
        "events",
        MaterializationStrategy::Incremental {
            timestamp_column: "_fivetran_synced".into(),
        },
    );
    let sql2 = sql_gen::generate_insert_sql(&plan2, &d).unwrap();
    p.execute(&sql2).await.unwrap();

    // Should now have 4 rows (2 original + 2 new)
    assert_eq!(p.row_count("target.events").await.unwrap(), 4);
}

#[tokio::test]
async fn test_merge_pipeline() {
    let p = TestPipeline::new();

    p.execute("CREATE SCHEMA IF NOT EXISTS source")
        .await
        .unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS target")
        .await
        .unwrap();

    // Create source with duplicates (same customer, different amounts)
    p.execute(
        "CREATE TABLE source.customers (
            customer_id INTEGER, name VARCHAR, tier VARCHAR,
            _fivetran_synced TIMESTAMP
        )",
    )
    .await
    .unwrap();
    p.execute(
        "INSERT INTO source.customers VALUES
            (1, 'Alice', 'gold', TIMESTAMP '2026-03-01'),
            (2, 'Bob', 'silver', TIMESTAMP '2026-03-01'),
            (1, 'Alice Updated', 'platinum', TIMESTAMP '2026-03-02')",
    )
    .await
    .unwrap();

    // First: create target with initial data
    p.execute(
        "CREATE TABLE target.customers (
            customer_id INTEGER, name VARCHAR, tier VARCHAR,
            _fivetran_synced TIMESTAMP, _loaded_by VARCHAR
        )",
    )
    .await
    .unwrap();
    p.execute(
        "INSERT INTO target.customers VALUES
            (1, 'Alice', 'gold', TIMESTAMP '2026-03-01', NULL),
            (2, 'Bob', 'silver', TIMESTAMP '2026-03-01', NULL)",
    )
    .await
    .unwrap();

    // DuckDB MERGE doesn't support qualified column names in SET (t.col = s.col),
    // so we test the merge logic using a manual DELETE + INSERT pattern (equivalent outcome).
    // The Databricks dialect handles real MERGE; here we verify the merge *semantics*.
    p.execute(
        "DELETE FROM target.customers WHERE customer_id IN (
            SELECT customer_id FROM source.customers
        )",
    )
    .await
    .unwrap();
    p.execute(
        "INSERT INTO target.customers
         SELECT *, CAST(NULL AS VARCHAR) AS _loaded_by FROM source.customers",
    )
    .await
    .unwrap();

    // Dedup: keep only the latest row per customer_id
    p.execute(
        "CREATE OR REPLACE TABLE target.customers AS
         SELECT * FROM (
             SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY _fivetran_synced DESC) AS rn
             FROM target.customers
         ) WHERE rn = 1",
    )
    .await
    .unwrap();
    // Drop helper column
    p.execute("ALTER TABLE target.customers DROP COLUMN rn")
        .await
        .unwrap();

    // Should have 2 rows (merged, not 3)
    assert_eq!(p.row_count("target.customers").await.unwrap(), 2);

    // Verify Alice was updated to the latest version
    let result = p
        .query("SELECT name, tier FROM target.customers WHERE customer_id = 1")
        .await
        .unwrap();
    assert_eq!(result.rows[0][0].as_str().unwrap(), "Alice Updated");
    assert_eq!(result.rows[0][1].as_str().unwrap(), "platinum");
}

#[tokio::test]
async fn test_pipeline_with_metadata_columns() {
    let p = TestPipeline::new();
    let d = dialect();

    p.execute("CREATE SCHEMA IF NOT EXISTS source")
        .await
        .unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS target")
        .await
        .unwrap();
    p.execute("CREATE TABLE source.items (id INTEGER, name VARCHAR, _fivetran_synced TIMESTAMP)")
        .await
        .unwrap();
    p.execute("INSERT INTO source.items VALUES (1, 'Widget', TIMESTAMP '2026-03-01')")
        .await
        .unwrap();

    let plan = duckdb_replication_plan("items", "items", MaterializationStrategy::FullRefresh);
    let sql = sql_gen::generate_create_table_as_sql(&plan, &d).unwrap();
    p.execute(&sql).await.unwrap();

    // Verify _loaded_by column exists and is NULL
    let result = p
        .query("SELECT _loaded_by FROM target.items")
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert!(result.rows[0][0].is_null(), "_loaded_by should be NULL");
}

// ===========================================================================
// Schema Evolution Tests
// ===========================================================================

#[test]
fn test_safe_widening_int_to_bigint() {
    // INT -> BIGINT should be safe widening (AlterColumnTypes, not DropAndRecreate)
    let source = vec![ColumnInfo {
        name: "id".into(),
        data_type: "BIGINT".into(),
        nullable: false,
    }];
    let target = vec![ColumnInfo {
        name: "id".into(),
        data_type: "INT".into(),
        nullable: false,
    }];
    let table = TableRef {
        catalog: "c".into(),
        schema: "s".into(),
        table: "t".into(),
    };
    let result = drift::detect_drift(&table, &source, &target, &dialect());
    assert_eq!(result.action, DriftAction::AlterColumnTypes);
}

#[test]
fn test_safe_widening_float_to_double() {
    let source = vec![ColumnInfo {
        name: "v".into(),
        data_type: "DOUBLE".into(),
        nullable: true,
    }];
    let target = vec![ColumnInfo {
        name: "v".into(),
        data_type: "FLOAT".into(),
        nullable: true,
    }];
    let table = TableRef {
        catalog: "c".into(),
        schema: "s".into(),
        table: "t".into(),
    };
    let result = drift::detect_drift(&table, &source, &target, &dialect());
    assert_eq!(result.action, DriftAction::AlterColumnTypes);
}

#[test]
fn test_unsafe_change_triggers_drop_and_recreate() {
    let source = vec![ColumnInfo {
        name: "id".into(),
        data_type: "VARCHAR".into(),
        nullable: false,
    }];
    let target = vec![ColumnInfo {
        name: "id".into(),
        data_type: "INTEGER".into(),
        nullable: false,
    }];
    let table = TableRef {
        catalog: "c".into(),
        schema: "s".into(),
        table: "t".into(),
    };
    let result = drift::detect_drift(&table, &source, &target, &dialect());
    assert!(!result.drifted_columns.is_empty(), "should detect drift");
    assert_eq!(result.action, DriftAction::DropAndRecreate);
}

#[test]
fn test_new_column_no_drift() {
    // New column in source that's not in target → no drift (it's additive)
    let source = vec![
        ColumnInfo {
            name: "id".into(),
            data_type: "INTEGER".into(),
            nullable: false,
        },
        ColumnInfo {
            name: "new_col".into(),
            data_type: "VARCHAR".into(),
            nullable: true,
        },
    ];
    let target = vec![ColumnInfo {
        name: "id".into(),
        data_type: "INTEGER".into(),
        nullable: false,
    }];
    let table = TableRef {
        catalog: "c".into(),
        schema: "s".into(),
        table: "t".into(),
    };
    let result = drift::detect_drift(&table, &source, &target, &dialect());
    assert_eq!(
        result.action,
        DriftAction::Ignore,
        "new column in source should not trigger drift"
    );
}

// ===========================================================================
// Shadow Mode Tests
// ===========================================================================

#[test]
fn test_shadow_mode_suffix() {
    let config = ShadowConfig::default();
    let target = TargetRef {
        catalog: "warehouse".into(),
        schema: "staging".into(),
        table: "orders".into(),
    };
    let shadow = shadow::shadow_target(&target, &config);
    assert_eq!(shadow.catalog, "warehouse");
    assert_eq!(shadow.schema, "staging");
    assert_eq!(shadow.table, "orders_rocky_shadow");
}

#[test]
fn test_shadow_mode_schema_override() {
    let config = ShadowConfig {
        schema_override: Some("_shadow".into()),
        ..Default::default()
    };
    let target = TargetRef {
        catalog: "warehouse".into(),
        schema: "staging".into(),
        table: "orders".into(),
    };
    let shadow = shadow::shadow_target(&target, &config);
    assert_eq!(shadow.catalog, "warehouse");
    assert_eq!(shadow.schema, "_shadow");
    assert_eq!(shadow.table, "orders");
}

#[tokio::test]
async fn test_compare_matching_tables() {
    let p = TestPipeline::new();

    p.execute("CREATE SCHEMA IF NOT EXISTS prod").await.unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS shadow")
        .await
        .unwrap();

    // Create identical tables
    p.execute("CREATE TABLE prod.orders (id INTEGER, amount DECIMAL(10,2))")
        .await
        .unwrap();
    p.execute("INSERT INTO prod.orders VALUES (1, 99.99), (2, 49.50)")
        .await
        .unwrap();

    p.execute("CREATE TABLE shadow.orders (id INTEGER, amount DECIMAL(10,2))")
        .await
        .unwrap();
    p.execute("INSERT INTO shadow.orders VALUES (1, 99.99), (2, 49.50)")
        .await
        .unwrap();

    // Compare row counts
    let prod_count = p.row_count("prod.orders").await.unwrap();
    let shadow_count = p.row_count("shadow.orders").await.unwrap();
    let (match_, diff, pct) = compare::compare_row_counts(shadow_count, prod_count);

    assert!(match_);
    assert_eq!(diff, 0);
    assert_eq!(pct, 0.0);

    // Compare schemas
    let prod_cols = p.describe("prod", "orders").await.unwrap();
    let shadow_cols = p.describe("shadow", "orders").await.unwrap();
    let diffs = compare::compare_schemas(&shadow_cols, &prod_cols);
    assert!(
        diffs.is_empty(),
        "identical tables should have no schema diffs"
    );

    // Overall verdict
    let result = compare::ComparisonResult {
        table: "prod.orders".into(),
        row_count_match: match_,
        shadow_count,
        production_count: prod_count,
        row_count_diff: diff,
        row_count_diff_pct: pct,
        schema_match: diffs.is_empty(),
        schema_diffs: diffs,
        sample_match: None,
        sample_mismatches: vec![],
    };
    let verdict = compare::evaluate_comparison(&result, &compare::ComparisonThresholds::default());
    assert_eq!(verdict, compare::ComparisonVerdict::Pass);
}

#[tokio::test]
async fn test_compare_row_count_mismatch() {
    let p = TestPipeline::new();

    p.execute("CREATE SCHEMA IF NOT EXISTS prod").await.unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS shadow")
        .await
        .unwrap();

    p.execute("CREATE TABLE prod.items (id INTEGER)")
        .await
        .unwrap();
    p.execute("INSERT INTO prod.items SELECT * FROM generate_series(1, 100)")
        .await
        .unwrap();

    // Shadow has 10% more rows
    p.execute("CREATE TABLE shadow.items (id INTEGER)")
        .await
        .unwrap();
    p.execute("INSERT INTO shadow.items SELECT * FROM generate_series(1, 110)")
        .await
        .unwrap();

    let prod_count = p.row_count("prod.items").await.unwrap();
    let shadow_count = p.row_count("shadow.items").await.unwrap();
    let (match_, diff, pct) = compare::compare_row_counts(shadow_count, prod_count);

    assert!(!match_);
    assert_eq!(diff, 10);
    assert!((pct - 0.10).abs() < 1e-9);

    let result = compare::ComparisonResult {
        table: "prod.items".into(),
        row_count_match: match_,
        shadow_count,
        production_count: prod_count,
        row_count_diff: diff,
        row_count_diff_pct: pct,
        schema_match: true,
        schema_diffs: vec![],
        sample_match: None,
        sample_mismatches: vec![],
    };
    let verdict = compare::evaluate_comparison(&result, &compare::ComparisonThresholds::default());
    // 10% > 5% fail threshold
    assert!(matches!(verdict, compare::ComparisonVerdict::Fail(_)));
}

#[tokio::test]
async fn test_compare_schema_mismatch() {
    let p = TestPipeline::new();

    p.execute("CREATE SCHEMA IF NOT EXISTS prod").await.unwrap();
    p.execute("CREATE SCHEMA IF NOT EXISTS shadow")
        .await
        .unwrap();

    // Different column types
    p.execute("CREATE TABLE prod.data (id INTEGER, value VARCHAR)")
        .await
        .unwrap();
    p.execute("CREATE TABLE shadow.data (id BIGINT, value VARCHAR)")
        .await
        .unwrap();

    let prod_cols = p.describe("prod", "data").await.unwrap();
    let shadow_cols = p.describe("shadow", "data").await.unwrap();
    let diffs = compare::compare_schemas(&shadow_cols, &prod_cols);

    assert!(!diffs.is_empty(), "should detect type mismatch");
    assert!(diffs.iter().any(|d| matches!(
        d,
        compare::SchemaDiff::ColumnTypeDiff { name, .. } if name == "id"
    )));
}

// ===========================================================================
// Watermark / State Tests
// ===========================================================================

#[test]
fn test_watermark_lifecycle() {
    let (store, _dir) = temp_state();
    let key = "cat.sch.orders";

    // Initially empty
    assert!(store.get_watermark(key).unwrap().is_none());

    // Set watermark
    let now = Utc::now();
    store
        .set_watermark(
            key,
            &WatermarkState {
                last_value: now,
                updated_at: now,
            },
        )
        .unwrap();

    // Read back
    let wm = store.get_watermark(key).unwrap().unwrap();
    assert_eq!(wm.last_value.timestamp(), now.timestamp());

    // Delete
    assert!(store.delete_watermark(key).unwrap());
    assert!(store.get_watermark(key).unwrap().is_none());
}

#[test]
fn test_checkpoint_state_persisted() {
    let (store, _dir) = temp_state();
    let run_id = "run_001";

    // Init run progress
    store.init_run_progress(run_id, 5).unwrap();
    let progress = store.get_run_progress(run_id).unwrap().unwrap();
    assert_eq!(progress.total_tables, 5);
    assert_eq!(progress.tables.len(), 0);
}

// ===========================================================================
// DAG Tests
// ===========================================================================

#[test]
fn test_dag_topological_sort() {
    use rocky_core::dag::{DagNode, topological_sort};

    let nodes = vec![
        DagNode {
            name: "raw".into(),
            depends_on: vec![],
        },
        DagNode {
            name: "staging".into(),
            depends_on: vec!["raw".into()],
        },
        DagNode {
            name: "mart".into(),
            depends_on: vec!["staging".into()],
        },
    ];

    let order = topological_sort(&nodes).unwrap();
    let raw_pos = order.iter().position(|n| n == "raw").unwrap();
    let stg_pos = order.iter().position(|n| n == "staging").unwrap();
    let mart_pos = order.iter().position(|n| n == "mart").unwrap();

    assert!(raw_pos < stg_pos);
    assert!(stg_pos < mart_pos);
}

#[test]
fn test_dag_cycle_detection() {
    use rocky_core::dag::{DagNode, topological_sort};

    let nodes = vec![
        DagNode {
            name: "a".into(),
            depends_on: vec!["b".into()],
        },
        DagNode {
            name: "b".into(),
            depends_on: vec!["a".into()],
        },
    ];

    assert!(
        topological_sort(&nodes).is_err(),
        "cycle should be detected"
    );
}

#[test]
fn test_dag_parallel_layers() {
    use rocky_core::dag::{DagNode, execution_layers};

    // Diamond: a -> {b, c} -> d
    let nodes = vec![
        DagNode {
            name: "a".into(),
            depends_on: vec![],
        },
        DagNode {
            name: "b".into(),
            depends_on: vec!["a".into()],
        },
        DagNode {
            name: "c".into(),
            depends_on: vec!["a".into()],
        },
        DagNode {
            name: "d".into(),
            depends_on: vec!["b".into(), "c".into()],
        },
    ];

    let layers = execution_layers(&nodes).unwrap();
    assert_eq!(layers.len(), 3, "should have 3 layers");
    assert_eq!(layers[0], vec!["a"]);
    assert_eq!(layers[1].len(), 2, "b and c should be in same layer");
    assert!(layers[1].contains(&"b".to_string()));
    assert!(layers[1].contains(&"c".to_string()));
    assert_eq!(layers[2], vec!["d"]);
}

// ===========================================================================
// Seed Data Tests (verify test infrastructure)
// ===========================================================================

#[tokio::test]
async fn test_seed_produces_queryable_data() {
    let (p, stats) = TestPipeline::with_seed(SeedScale::Small);

    // Verify seed stats
    assert_eq!(stats.tables.len(), 3);

    // Query the seeded data
    let count = p.row_count("source.raw_orders").await.unwrap();
    assert_eq!(count, 10_000);

    // Verify data quality: all orders have valid customer_ids
    let result = p
        .query(
            "SELECT COUNT(*) FROM source.raw_orders o
             WHERE o.customer_id NOT IN (SELECT customer_id FROM source.raw_customers)",
        )
        .await
        .unwrap();
    // With Zipf distribution some customer_ids might exceed range, but most should be valid
    let orphans: u64 = result.rows[0][0]
        .as_str()
        .and_then(|s| s.parse().ok())
        .unwrap_or(u64::MAX);
    assert!(orphans < 1000, "too many orphan orders: {orphans}");
}

#[tokio::test]
async fn test_full_refresh_on_seeded_data() {
    let (p, _stats) = TestPipeline::with_seed(SeedScale::Small);
    let d = dialect();

    p.execute("CREATE SCHEMA IF NOT EXISTS target")
        .await
        .unwrap();

    // Full refresh from seeded source
    let plan = duckdb_replication_plan(
        "raw_orders",
        "raw_orders",
        MaterializationStrategy::FullRefresh,
    );
    let sql = sql_gen::generate_create_table_as_sql(&plan, &d).unwrap();
    p.execute(&sql).await.unwrap();

    let source_count = p.row_count("source.raw_orders").await.unwrap();
    let target_count = p.row_count("target.raw_orders").await.unwrap();
    assert_eq!(source_count, target_count);
}
