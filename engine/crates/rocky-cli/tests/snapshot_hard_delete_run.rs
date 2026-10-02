#![cfg(feature = "duckdb")]

use rocky_cli::commands::{DeferOptions, PartitionRunOptions, SkipRunOptions};
use rocky_core::traits::WarehouseAdapter;
use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

#[tokio::test]
async fn rocky_run_closes_a_deleted_snapshot_key() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("s.duckdb");
    let config_path = dir.path().join("rocky.toml");
    let state_path = dir.path().join("state.redb");
    {
        let adapter = DuckDbWarehouseAdapter::open(&db).unwrap();
        adapter
            .execute_statement("CREATE TABLE main.src (customer_id INTEGER, updated_at TIMESTAMP); INSERT INTO main.src VALUES (1, TIMESTAMP '2026-01-01'), (2, TIMESTAMP '2026-01-01')")
            .await
            .unwrap();
    }
    std::fs::write(
        &config_path,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.dim]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"
invalidate_hard_deletes = true

[pipeline.dim.source]
catalog = "s"
schema = "main"
table = "src"

[pipeline.dim.target]
catalog = "s"
schema = "main"
table = "history"

[pipeline.dim.target.governance]
auto_create_schemas = true
"#,
            db.display()
        ),
    )
    .unwrap();

    for run_number in 0..2 {
        if run_number == 1 {
            let adapter = DuckDbWarehouseAdapter::open(&db).unwrap();
            adapter
                .execute_statement("DELETE FROM main.src WHERE customer_id = 1")
                .await
                .unwrap();
        }
        let loaded = std::sync::Arc::new(
            rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
        );
        rocky_cli::commands::run(
            &config_path,
            loaded,
            None,
            None,
            &state_path,
            None,
            false,
            None,
            false,
            None,
            false,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            None,
            None,
            &DeferOptions::default(),
            &SkipRunOptions::default(),
            &rocky_core::run_vars::RunVars::new(),
            None,
            None,
            false,
            None,
        )
        .await
        .expect("rocky run should complete the snapshot");
    }

    let adapter = DuckDbWarehouseAdapter::open(&db).unwrap();
    let rows = adapter
        .execute_query(
            "SELECT customer_id, valid_to IS NULL FROM main.history ORDER BY customer_id",
        )
        .await
        .unwrap();
    assert_eq!(rows.rows.len(), 2, "{rows:?}");
    assert_eq!(rows.rows[0][0], "1");
    assert_eq!(
        rows.rows[0][1], "false",
        "deleted row must be closed: {rows:?}"
    );
    assert_eq!(rows.rows[1][0], "2");
    assert_eq!(
        rows.rows[1][1], "true",
        "retained row must stay open: {rows:?}"
    );
}
