//! Drives the skeleton adapter end-to-end against the in-memory mock
//! backend, then prints what it executed. This is what `run.sh` runs to
//! prove the trait shape compiles and behaves correctly without a live
//! warehouse.

use std::sync::Arc;

use rocky_adapter_skeleton::{Backend, MockBackend, SkeletonAdapter};
use rocky_adapter_sdk::{ColumnInfo, ColumnSelection, TableRef, WarehouseAdapter};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Arc::new(MockBackend::new());

    // Pretend a `default.events` table already exists upstream so the
    // describe_table call has something to return.
    let events = TableRef {
        catalog: String::new(),
        schema: "default".into(),
        table: "events".into(),
    };
    backend
        .install_table(
            events.clone(),
            vec![
                ColumnInfo {
                    name: "id".into(),
                    data_type: "Int64".into(),
                    nullable: false,
                },
                ColumnInfo {
                    name: "ts".into(),
                    data_type: "DateTime64(3)".into(),
                    nullable: false,
                },
            ],
        )
        .await;

    let adapter = SkeletonAdapter::new(backend.clone() as Arc<dyn Backend>);

    // 1. Show the manifest Rocky would receive on registration.
    let manifest = SkeletonAdapter::manifest();
    println!("manifest:");
    println!("  name           = {}", manifest.name);
    println!("  dialect        = {}", manifest.dialect);
    println!("  sdk_version    = {}", manifest.sdk_version);
    println!("  warehouse      = {}", manifest.capabilities.warehouse);
    println!("  merge          = {}", manifest.capabilities.merge);
    println!("  create_schema  = {}", manifest.capabilities.create_schema);

    // 2. Round-trip a CREATE TABLE statement through the dialect + backend.
    let target = adapter.dialect().format_table_ref("", "raw", "events_copy")?;
    let create =
        adapter
            .dialect()
            .create_table_as(&target, "SELECT * FROM `default`.`events`");
    adapter.execute_statement(&create).await?;

    // 3. Generate an incremental WHERE clause and an insert overwrite plan
    //    so the example covers the dialect surface a real ClickHouse
    //    adapter would care about.
    let where_clause = adapter.dialect().watermark_where("ts", &target)?;
    let select = adapter.dialect().select_clause(
        &ColumnSelection::Explicit(vec!["id".into(), "ts".into()]),
        &[],
    )?;
    let stmts = adapter.dialect().insert_overwrite_partition(
        &target,
        "`day` = '2026-01-01'",
        &format!("SELECT {select} FROM `default`.`events` WHERE {where_clause}"),
    )?;

    for s in &stmts {
        adapter.execute_statement(s).await?;
    }

    // 4. Pull the schema back so describe_table is exercised.
    let cols = adapter.describe_table(&events).await?;
    println!();
    println!("describe_table(default.events):");
    for c in &cols {
        println!("  - {} {} (nullable={})", c.name, c.data_type, c.nullable);
    }

    // 5. Print the SQL log so a reader can see exactly what the
    //    adapter would have sent to the warehouse.
    println!();
    println!("statements executed:");
    for (i, s) in backend.statement_log().await.iter().enumerate() {
        println!("  [{i}] {s}");
    }

    adapter.close().await?;
    Ok(())
}
