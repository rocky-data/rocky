//! Golden tests for the typed IR.
//!
//! Each fixture is a hand-built `ModelIr` covering one
//! `MaterializationStrategy` × shape combination from the typed-IR spike
//! matrix. The fixture's serialized JSON, per-dialect SQL, and recipe-hash
//! are pinned; drift from the pin fails CI.
//!
//! Three properties are regression-tested per fixture:
//!
//! 1. **Byte-stable round-trip** — `serialize → deserialize → serialize`
//!    yields identical bytes. Catches non-deterministic field ordering or
//!    accidental `null` emission that would break the canonical-JSON rule
//!    documented in `engine/crates/rocky-core/SPEC.md` §6.
//!
//! 2. **Recipe-hash pin** — `ModelIr::recipe_hash()` matches the hex stored
//!    in [`FIXTURES`]. Catches silent IR shape drift (a renamed field, a
//!    flipped serde attribute) that would change the content-addressed
//!    identity of every existing model.
//!
//! 3. **Per-dialect SQL pin** — running the appropriate `sql_gen` entry
//!    against each of the four dialects (DuckDB, Databricks, BigQuery,
//!    Snowflake) yields SQL identical to the snapshot under
//!    `tests/ir-golden/<fixture>/<dialect>.sql`. Catches dialect-specific
//!    regressions that the in-process `TestDialect` in `sql_gen.rs` tests
//!    cannot see.
//!
//! ## Regenerating
//!
//! Set `REGEN_IR_GOLDENS=1` before `cargo test` to overwrite every snapshot
//! and dump the per-fixture recipe-hash to stdout. Capture the hex values
//! and paste them into the [`FIXTURES`] constant table; subsequent normal
//! runs will assert against the new pin.
//!
//! ```shell
//! REGEN_IR_GOLDENS=1 cargo test -p rocky-cli --test ir_golden -- --nocapture
//! ```

#![cfg(feature = "duckdb")]

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

use rocky_core::ir::{
    ColumnMask, ColumnSelection, GovernanceConfig, MaterializationStrategy, MetadataColumn,
    ModelIr, Plan, ReplicationPlan, SnapshotPlan, SourceRef, TargetRef, TransformationPlan,
};
use rocky_core::lakehouse::{LakehouseFormat, LakehouseOptions};
use rocky_core::lineage::{LineageEdge, QualifiedColumn};
use rocky_core::models::TimeGrain;
use rocky_core::sql_gen;
use rocky_core::traits::{MaskStrategy, SqlDialect};
use rocky_core::types::{RockyType, TypedColumn};
use rocky_sql::lineage::TransformKind;

use rocky_bigquery::dialect::BigQueryDialect;
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_snowflake::dialect::SnowflakeSqlDialect;

const REGEN_ENV: &str = "REGEN_IR_GOLDENS";
const FIXTURES_SUBDIR: &str = "tests/ir-golden";

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(FIXTURES_SUBDIR)
}

fn regen() -> bool {
    std::env::var(REGEN_ENV).is_ok_and(|v| !v.is_empty())
}

// ---------------------------------------------------------------------------
// Dialect dispatch
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum DialectKind {
    DuckDb,
    Databricks,
    BigQuery,
    Snowflake,
}

impl DialectKind {
    const ALL: &'static [DialectKind] = &[
        DialectKind::DuckDb,
        DialectKind::Databricks,
        DialectKind::BigQuery,
        DialectKind::Snowflake,
    ];

    fn name(self) -> &'static str {
        match self {
            DialectKind::DuckDb => "duckdb",
            DialectKind::Databricks => "databricks",
            DialectKind::BigQuery => "bigquery",
            DialectKind::Snowflake => "snowflake",
        }
    }

    fn instance(self) -> Box<dyn SqlDialect> {
        match self {
            DialectKind::DuckDb => Box::new(DuckDbSqlDialect),
            DialectKind::Databricks => Box::new(DatabricksSqlDialect),
            DialectKind::BigQuery => Box::new(BigQueryDialect),
            DialectKind::Snowflake => Box::new(SnowflakeSqlDialect),
        }
    }
}

// ---------------------------------------------------------------------------
// Entry dispatch
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum Entry {
    /// `generate_create_table_as_sql` — replication, FullRefresh CTAS.
    ReplicationCreateTableAs,
    /// `generate_insert_sql` — replication, Incremental INSERT.
    ReplicationInsert,
    /// `generate_merge_sql` — replication, MERGE upsert.
    ReplicationMerge,
    /// `generate_transformation_sql` — transformation, dispatches by strategy.
    Transformation,
    /// `generate_materialized_view_sql` — transformation, MV.
    MaterializedView,
    /// `generate_dynamic_table_sql` — transformation, Snowflake dynamic table.
    /// Carries inline `target_lag` / `warehouse` to keep the entry self-contained.
    DynamicTable {
        target_lag: &'static str,
        warehouse: &'static str,
    },
    /// `generate_time_interval_bootstrap_sql` — first-run zero-row CTAS.
    TimeIntervalBootstrap,
    /// `generate_snapshot_sql` — SCD2 snapshot.
    Snapshot,
}

fn run_entry(
    entry: Entry,
    ir: &ModelIr,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, sql_gen::SqlGenError> {
    match entry {
        Entry::ReplicationCreateTableAs => {
            sql_gen::generate_create_table_as_sql(ir, dialect).map(|s| vec![s])
        }
        Entry::ReplicationInsert => sql_gen::generate_insert_sql(ir, dialect).map(|s| vec![s]),
        Entry::ReplicationMerge => sql_gen::generate_merge_sql(ir, dialect).map(|s| vec![s]),
        Entry::Transformation => sql_gen::generate_transformation_sql(ir, dialect),
        Entry::MaterializedView => {
            sql_gen::generate_materialized_view_sql(ir, dialect).map(|s| vec![s])
        }
        Entry::DynamicTable {
            target_lag,
            warehouse,
        } => {
            sql_gen::generate_dynamic_table_sql(ir, target_lag, warehouse, dialect).map(|s| vec![s])
        }
        Entry::TimeIntervalBootstrap => {
            sql_gen::generate_time_interval_bootstrap_sql(ir, dialect).map(|s| vec![s])
        }
        Entry::Snapshot => sql_gen::generate_snapshot_sql(ir, dialect),
    }
}

/// Multi-statement separator used to flatten `Vec<String>` into a single
/// snapshot file. Chosen so the snapshot is human-readable and a
/// single-statement output is identical to the bare statement.
const STATEMENT_SEP: &str = "\n;\n\n-- --- next statement ---\n\n";

fn join_statements(stmts: &[String]) -> String {
    stmts.join(STATEMENT_SEP)
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// One fixture: a hand-built `ModelIr` covering a specific IR shape, the
/// SQL-gen entry that consumes it, the dialects whose SQL is snapshotted,
/// and the pinned recipe-hash hex.
struct Fixture {
    name: &'static str,
    builder: fn() -> ModelIr,
    entry: Entry,
    dialects: &'static [DialectKind],
    /// blake3 of canonical-JSON encoding of the built IR; pinned so silent
    /// IR shape drift fails noisily.
    ///
    /// To rebuild after an intentional shape change: set
    /// `REGEN_IR_GOLDENS=1`, run the test suite with `--nocapture`, and
    /// paste the printed hex back here.
    recipe_hash: &'static str,
}

const FIXTURES: &[Fixture] = &[
    Fixture {
        name: "01-replication-full-refresh",
        builder: build_01_replication_full_refresh,
        entry: Entry::ReplicationCreateTableAs,
        dialects: DialectKind::ALL,
        recipe_hash: "d61d89296aedd5328d4eb850d4958f65f8d2613bb99007b4e37eee40c63e018a",
    },
    Fixture {
        name: "02-replication-incremental",
        builder: build_02_replication_incremental,
        entry: Entry::ReplicationInsert,
        dialects: DialectKind::ALL,
        recipe_hash: "a22f9596d500cfb75368f1a157e4efce0508542318186d3335ea10dda11c182f",
    },
    Fixture {
        name: "03-replication-merge",
        builder: build_03_replication_merge,
        entry: Entry::ReplicationMerge,
        dialects: DialectKind::ALL,
        recipe_hash: "f7049584ade0c4187d3a4fffdc8d2b0c7543511378546902f7ee9268f083c03d",
    },
    Fixture {
        name: "04-transformation-full-refresh",
        builder: build_04_transformation_full_refresh,
        entry: Entry::Transformation,
        dialects: DialectKind::ALL,
        recipe_hash: "a454687a034d06191df20a23375423c39f765885ac1593ddd8f4cba05175494f",
    },
    Fixture {
        name: "05-transformation-merge",
        builder: build_05_transformation_merge,
        entry: Entry::Transformation,
        dialects: DialectKind::ALL,
        recipe_hash: "b3ee0334c2abd6c61f2e740cd2d7910c68e34258913022c0f541c1570ae5aa12",
    },
    Fixture {
        name: "06-transformation-ephemeral",
        builder: build_06_transformation_ephemeral,
        entry: Entry::Transformation,
        dialects: DialectKind::ALL,
        recipe_hash: "810ece48173ec2caf99b0a1ebba89dd9859bdc56710d7157d9ffe39399d45d8a",
    },
    Fixture {
        name: "07-transformation-materialized-view",
        builder: build_07_transformation_materialized_view,
        entry: Entry::MaterializedView,
        dialects: DialectKind::ALL,
        recipe_hash: "80307cf6df8abc20560abe3977825195a9ec4322dbce564fbe71f945ee8ad99b",
    },
    Fixture {
        name: "08-transformation-dynamic-table",
        builder: build_08_transformation_dynamic_table,
        entry: Entry::DynamicTable {
            target_lag: "1 hour",
            warehouse: "compute_wh",
        },
        dialects: DialectKind::ALL,
        recipe_hash: "c313f449a16b0b08843b0b398cd4b6e66e07a8556794cab642c77f732a7d80a1",
    },
    Fixture {
        name: "09-transformation-time-interval-bootstrap",
        builder: build_09_transformation_time_interval_bootstrap,
        entry: Entry::TimeIntervalBootstrap,
        dialects: DialectKind::ALL,
        recipe_hash: "85b537ed061ff3a33a2acab8a15bf8f80acc158edd672c211e60a52762945883",
    },
    Fixture {
        name: "10-transformation-lakehouse-delta",
        builder: build_10_transformation_lakehouse_delta,
        entry: Entry::Transformation,
        dialects: DialectKind::ALL,
        recipe_hash: "7ca110c89134a8750898db55c60d24bd7692ba98d0a434fa24937c117f5bed5b",
    },
    Fixture {
        name: "11-transformation-multi-source-join",
        builder: build_11_transformation_multi_source_join,
        entry: Entry::Transformation,
        dialects: DialectKind::ALL,
        recipe_hash: "94dc1796b72517803e3ef966a4d92e73695486def3202956fec123b247019cd0",
    },
    Fixture {
        name: "12-snapshot-scd2",
        builder: build_12_snapshot_scd2,
        entry: Entry::Snapshot,
        dialects: DialectKind::ALL,
        recipe_hash: "e3cdac244a2517c494a6e9306bcb0eea3b6b18202f69de500f6d0034ac045cd8",
    },
];

// ---------------------------------------------------------------------------
// Fixture builders
// ---------------------------------------------------------------------------

fn baseline_governance() -> GovernanceConfig {
    GovernanceConfig {
        permissions_file: None,
        auto_create_catalogs: false,
        auto_create_schemas: false,
    }
}

// Catalog names: BigQuery validates the catalog as a GCP project ID
// (6-30 chars, lowercase letters / digits / hyphens, no underscores, must
// start with a letter, no trailing hyphen). DuckDB / Databricks /
// Snowflake validate identifiers against `[a-zA-Z0-9_]+` (no hyphens).
// The intersection is lowercase letters + digits, 6-30 chars, starting
// with a letter — which gives every fixture a single catalog name that
// runs against all four dialects.
const TGT_CATALOG: &str = "tgtwarehouse";
const SRC_CATALOG: &str = "srcwarehouse";

fn raw_target(table: &str) -> TargetRef {
    TargetRef {
        catalog: TGT_CATALOG.into(),
        schema: "raw__demo".into(),
        table: table.into(),
    }
}

fn marts_target(table: &str) -> TargetRef {
    TargetRef {
        catalog: TGT_CATALOG.into(),
        schema: "marts__demo".into(),
        table: table.into(),
    }
}

fn snapshot_target(table: &str) -> TargetRef {
    TargetRef {
        catalog: TGT_CATALOG.into(),
        schema: "snapshots__demo".into(),
        table: table.into(),
    }
}

fn raw_source(table: &str) -> SourceRef {
    SourceRef {
        catalog: SRC_CATALOG.into(),
        schema: "src__demo".into(),
        table: table.into(),
    }
}

fn build_01_replication_full_refresh() -> ModelIr {
    let plan = Plan::Replication(ReplicationPlan {
        source: raw_source("orders"),
        target: raw_target("orders"),
        strategy: MaterializationStrategy::FullRefresh,
        columns: ColumnSelection::All,
        metadata_columns: vec![],
        governance: baseline_governance(),
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("orders");
    ir
}

fn build_02_replication_incremental() -> ModelIr {
    let plan = Plan::Replication(ReplicationPlan {
        source: raw_source("events"),
        target: raw_target("events"),
        strategy: MaterializationStrategy::Incremental {
            timestamp_column: "_synced_at".into(),
        },
        columns: ColumnSelection::Explicit(vec![
            Arc::from("id"),
            Arc::from("event_type"),
            Arc::from("payload"),
            Arc::from("_synced_at"),
        ]),
        metadata_columns: vec![MetadataColumn {
            name: "_loaded_by".into(),
            data_type: "STRING".into(),
            value: "'rocky'".into(),
        }],
        governance: baseline_governance(),
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("events");
    ir
}

fn build_03_replication_merge() -> ModelIr {
    // Replication with a Merge strategy: the strategy carries its own
    // unique_key on the enum; the top-level ModelIr.unique_key stays empty
    // (variant-discrimination guardrail — see SPEC §3 and the inline test
    // `plan_to_model_ir_replication_with_merge_strategy_roundtrip`).
    //
    // `update_columns` is explicit (not `ColumnSelection::All`) so the
    // DuckDB dialect produces a valid `UPDATE SET name = source.name, ...`
    // — DuckDB MERGE rejects the `UPDATE SET *` shorthand.
    let plan = Plan::Replication(ReplicationPlan {
        source: raw_source("customers"),
        target: raw_target("customers"),
        strategy: MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("customer_id")],
            update_columns: ColumnSelection::Explicit(vec![
                Arc::from("name"),
                Arc::from("email"),
                Arc::from("updated_at"),
            ]),
        },
        columns: ColumnSelection::All,
        metadata_columns: vec![],
        governance: baseline_governance(),
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("customers");
    ir
}

fn build_04_transformation_full_refresh() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "raw__demo".into(),
            table: "orders".into(),
        }],
        target: marts_target("fct_orders"),
        strategy: MaterializationStrategy::FullRefresh,
        sql: "SELECT id, customer_id, total FROM tgtwarehouse.raw__demo.orders".into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("fct_orders");
    ir.typed_columns = vec![
        TypedColumn {
            name: "id".into(),
            data_type: RockyType::Int64,
            nullable: false,
        },
        TypedColumn {
            name: "customer_id".into(),
            data_type: RockyType::Int64,
            nullable: false,
        },
        TypedColumn {
            name: "total".into(),
            data_type: RockyType::Float64,
            nullable: true,
        },
    ];
    ir
}

fn build_05_transformation_merge() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "raw__demo".into(),
            table: "customers".into(),
        }],
        target: marts_target("dim_customers"),
        strategy: MaterializationStrategy::Merge {
            unique_key: vec![Arc::from("customer_id")],
            update_columns: ColumnSelection::Explicit(vec![
                Arc::from("name"),
                Arc::from("email"),
                Arc::from("updated_at"),
            ]),
        },
        sql: "SELECT customer_id, name, email, updated_at \
              FROM tgtwarehouse.raw__demo.customers"
            .into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("dim_customers");
    // Resolved column mask: email gets HASH for the active env.
    ir.column_masks = vec![ColumnMask {
        column: Arc::from("email"),
        strategy: MaskStrategy::Hash,
    }];
    // Cross-model lineage edge (slice that targets this model).
    ir.lineage_edges = vec![LineageEdge {
        source: QualifiedColumn {
            model: Arc::from("customers"),
            column: Arc::from("email"),
        },
        target: QualifiedColumn {
            model: Arc::from("dim_customers"),
            column: Arc::from("email"),
        },
        transform: TransformKind::Direct,
    }];
    ir
}

fn build_06_transformation_ephemeral() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "marts__demo".into(),
            table: "fct_orders".into(),
        }],
        target: marts_target("active_orders"),
        strategy: MaterializationStrategy::Ephemeral,
        sql: "SELECT * FROM tgtwarehouse.marts__demo.fct_orders WHERE status = 'active'".into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("active_orders");
    ir
}

fn build_07_transformation_materialized_view() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "marts__demo".into(),
            table: "fct_orders".into(),
        }],
        target: marts_target("mv_orders_daily"),
        strategy: MaterializationStrategy::MaterializedView,
        sql: "SELECT DATE(created_at) AS day, COUNT(*) AS order_count, SUM(total) AS revenue \
              FROM tgtwarehouse.marts__demo.fct_orders GROUP BY 1"
            .into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("mv_orders_daily");
    ir
}

fn build_08_transformation_dynamic_table() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "marts__demo".into(),
            table: "fct_orders".into(),
        }],
        target: marts_target("dt_orders_recent"),
        // Note: target_lag also sits on the strategy enum, but
        // generate_dynamic_table_sql consumes it from the call args (see Entry::DynamicTable).
        strategy: MaterializationStrategy::DynamicTable {
            target_lag: "1 hour".into(),
        },
        sql: "SELECT id, customer_id, total \
              FROM tgtwarehouse.marts__demo.fct_orders \
              WHERE created_at > CURRENT_DATE - INTERVAL '7' DAY"
            .into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("dt_orders_recent");
    ir
}

fn build_09_transformation_time_interval_bootstrap() -> ModelIr {
    // Static plan emits window=None per the recipe-hash invariant
    // documented in SPEC.md §5.
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "raw__demo".into(),
            table: "events".into(),
        }],
        target: marts_target("events_daily"),
        strategy: MaterializationStrategy::TimeInterval {
            time_column: "event_date".into(),
            granularity: TimeGrain::Day,
            window: None,
        },
        sql: "SELECT event_date, COUNT(*) AS event_count \
              FROM tgtwarehouse.raw__demo.events \
              WHERE event_date >= '@start_date' AND event_date < '@end_date' \
              GROUP BY 1"
            .into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("events_daily");
    ir
}

fn build_10_transformation_lakehouse_delta() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "raw__demo".into(),
            table: "events".into(),
        }],
        target: marts_target("fct_events_delta"),
        strategy: MaterializationStrategy::FullRefresh,
        sql: "SELECT id, event_type, event_date, payload \
              FROM tgtwarehouse.raw__demo.events"
            .into(),
        governance: baseline_governance(),
        format: Some(LakehouseFormat::DeltaTable),
        format_options: Some(LakehouseOptions {
            partition_by: vec!["event_date".into()],
            ..Default::default()
        }),
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("fct_events_delta");
    ir
}

fn build_11_transformation_multi_source_join() -> ModelIr {
    let plan = Plan::Transformation(TransformationPlan {
        sources: vec![
            SourceRef {
                catalog: TGT_CATALOG.into(),
                schema: "raw__demo".into(),
                table: "orders".into(),
            },
            SourceRef {
                catalog: TGT_CATALOG.into(),
                schema: "raw__demo".into(),
                table: "customers".into(),
            },
            SourceRef {
                catalog: TGT_CATALOG.into(),
                schema: "raw__demo".into(),
                table: "products".into(),
            },
        ],
        target: marts_target("fct_order_lines"),
        strategy: MaterializationStrategy::FullRefresh,
        sql: "SELECT o.id AS order_id, c.name AS customer_name, p.sku, o.total \
              FROM tgtwarehouse.raw__demo.orders o \
              JOIN tgtwarehouse.raw__demo.customers c ON o.customer_id = c.id \
              JOIN tgtwarehouse.raw__demo.products p ON o.product_id = p.id"
            .into(),
        governance: baseline_governance(),
        format: None,
        format_options: None,
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("fct_order_lines");
    ir
}

fn build_12_snapshot_scd2() -> ModelIr {
    let plan = Plan::Snapshot(SnapshotPlan {
        source: SourceRef {
            catalog: TGT_CATALOG.into(),
            schema: "marts__demo".into(),
            table: "dim_customers".into(),
        },
        target: snapshot_target("dim_customers_history"),
        unique_key: vec![Arc::from("customer_id")],
        updated_at: "updated_at".into(),
        invalidate_hard_deletes: true,
        governance: baseline_governance(),
    });
    let mut ir = ModelIr::from(&plan);
    ir.name = Arc::from("dim_customers_history");
    ir
}

// ---------------------------------------------------------------------------
// Snapshot helpers
// ---------------------------------------------------------------------------

fn ir_json_path(fixture: &Fixture) -> PathBuf {
    fixtures_root().join(fixture.name).join("ir.json")
}

fn sql_path(fixture: &Fixture, dialect: DialectKind) -> PathBuf {
    fixtures_root()
        .join(fixture.name)
        .join(format!("{}.sql", dialect.name()))
}

fn pretty_json(ir: &ModelIr) -> String {
    let mut s = serde_json::to_string_pretty(ir).expect("ModelIr serialize");
    s.push('\n');
    s
}

fn write_snapshot(path: &PathBuf, contents: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create snapshot parent dir");
    }
    fs::write(path, contents).expect("write snapshot");
}

fn read_snapshot(path: &PathBuf) -> String {
    fs::read_to_string(path).unwrap_or_else(|e| {
        panic!(
            "snapshot missing at {}: {e}\n\
             Run with REGEN_IR_GOLDENS=1 to create it.",
            path.display()
        )
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Property: `serialize → deserialize → serialize` is byte-stable for every
/// fixture. Catches non-deterministic field order or accidental `null`
/// emission that would break the canonical-JSON rule.
#[test]
fn ir_round_trip_byte_stable() {
    for fixture in FIXTURES {
        let ir = (fixture.builder)();
        let json1 = pretty_json(&ir);
        let back: ModelIr = serde_json::from_str(&json1).expect("deserialize ModelIr");
        let json2 = pretty_json(&back);
        assert_eq!(json1, json2, "{}: round-trip not byte-stable", fixture.name);
    }
}

/// Property: each fixture's `ir.json` snapshot is byte-identical to the
/// pretty-printed serialization of the freshly built `ModelIr`. Catches
/// silent IR shape drift (a renamed field, a flipped serde attribute) at
/// review time rather than after merge.
///
/// Regen: `REGEN_IR_GOLDENS=1 cargo test -p rocky-cli --test ir_golden -- ir_json_matches_snapshot`.
#[test]
fn ir_json_matches_snapshot() {
    for fixture in FIXTURES {
        let ir = (fixture.builder)();
        let json = pretty_json(&ir);
        let path = ir_json_path(fixture);
        if regen() {
            write_snapshot(&path, &json);
            eprintln!("regen wrote {}", path.display());
        } else {
            let snapshot = read_snapshot(&path);
            assert_eq!(json, snapshot, "{}: ir.json drift", fixture.name);
        }
    }
}

/// Property: each fixture's `ModelIr::recipe_hash()` matches the hex pinned
/// in [`FIXTURES`]. Drift from the pin is the single load-bearing signal
/// that the IR's content-addressed identity has changed; treat it the same
/// as a public-API break (intentional or accidental, surface it in the PR
/// description).
///
/// Under `REGEN_IR_GOLDENS=1` this test prints each fixture's hex to
/// stderr instead of asserting; copy the output back into the [`FIXTURES`]
/// table and re-run normally.
#[test]
fn ir_recipe_hash_pinned() {
    let mut fail = false;
    for fixture in FIXTURES {
        let ir = (fixture.builder)();
        let actual = ir.recipe_hash().to_hex().to_string();
        if regen() {
            eprintln!(r#"    "{}" => "{}","#, fixture.name, actual);
            continue;
        }
        if actual != fixture.recipe_hash {
            eprintln!(
                "{}: recipe-hash drift\n  pinned: {}\n  actual: {}",
                fixture.name, fixture.recipe_hash, actual
            );
            fail = true;
        }
    }
    assert!(
        !fail,
        "one or more fixtures drifted from their pinned recipe-hash"
    );
}

/// Property: running each fixture's `Entry` against each declared dialect
/// yields SQL identical to the snapshot at `<fixture>/<dialect>.sql`.
/// Multi-statement outputs are joined with [`STATEMENT_SEP`] so the
/// snapshot is a single human-readable file; single-statement outputs are
/// the bare statement.
///
/// Regen: `REGEN_IR_GOLDENS=1 cargo test -p rocky-cli --test ir_golden -- ir_sql_per_dialect`.
#[test]
fn ir_sql_per_dialect() {
    for fixture in FIXTURES {
        let ir = (fixture.builder)();
        for &dialect in fixture.dialects {
            let stmts = run_entry(fixture.entry, &ir, &*dialect.instance()).unwrap_or_else(|e| {
                panic!(
                    "{}: sql_gen failed against {}: {e}",
                    fixture.name,
                    dialect.name()
                )
            });
            let mut joined = join_statements(&stmts);
            if !joined.ends_with('\n') {
                joined.push('\n');
            }
            let path = sql_path(fixture, dialect);
            if regen() {
                write_snapshot(&path, &joined);
                eprintln!("regen wrote {}", path.display());
            } else {
                let snapshot = read_snapshot(&path);
                assert_eq!(
                    joined,
                    snapshot,
                    "{}/{}.sql drift",
                    fixture.name,
                    dialect.name()
                );
            }
        }
    }
}
