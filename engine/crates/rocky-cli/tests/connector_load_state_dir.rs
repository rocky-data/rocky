//! A connector load creates the missing parent directory of its state path.
//!
//! `rocky load --connector <name> --state-path .rocky/state.redb` on a fresh
//! checkout has no `.rocky/`. The load opens the state store for the
//! idempotency claim before anything else creates that directory, so the open
//! of the state lock file used to fail with `i/o error opening state lock
//! file`.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rocky_cli::commands::{ConnectorLoadParams, execute_connector_load};
use rocky_core::config::load_rocky_config_fingerprinted;

/// Mutation: drop the `create_private_dir_all` call in `open_lock_file`
/// (rocky-core `state.rs`) and the load fails on the lock file.
#[tokio::test]
async fn a_connector_load_creates_the_missing_state_directory() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    std::fs::create_dir_all(p.join("connector")).unwrap();
    let db = p.join("warehouse.duckdb");
    std::fs::write(
        p.join("rocky.toml"),
        format!(
            r#"[adapter.default]
type = "duckdb"
path = "{db}"

[connector.demo]
manifest = "connector/connector.toml"
output_uri = "file://./landing"
target = {{ catalog = "", schema = "main", table = "events" }}
retries = 0
"#,
            db = db.display()
        ),
    )
    .unwrap();
    std::fs::write(
        p.join("connector/connector.toml"),
        "protocol_version = 0\nname = \"demo\"\ncommand = [\"./run.sh\"]\n\
         state_schema_version = 1\negress_hosts = []\n",
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter([(
        "id",
        Arc::new(Int64Array::from(vec![1_i64])) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let parquet = p.join("batch.parquet");
    let mut writer = ArrowWriter::try_new(
        std::fs::File::create(&parquet).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let script = p.join("connector/run.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\ncat > /dev/null\ncp '{}' batch.parquet\n\
             printf '{{\"protocol_version\":0,\"files\":[{{\"path\":\"batch.parquet\",\"bytes\":%s,\"rows\":1}}],\"state\":{{\"schema_version\":1,\"value\":null}}}}' \"$(wc -c < batch.parquet | tr -d ' ')\" > _rocky_result.json\n",
            parquet.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let config_path = p.join("rocky.toml");
    // Two missing levels: `.rocky/` and `.rocky/nested/`.
    let state_dir = p.join(".rocky").join("nested");
    let state_path = state_dir.join("state.redb");
    assert!(!p.join(".rocky").exists());
    let loaded = load_rocky_config_fingerprinted(&config_path).unwrap();

    let params = ConnectorLoadParams::new(&config_path, &loaded, &state_path, "demo");
    let outcome = execute_connector_load(&params).await;
    if let Err(e) = &outcome.result {
        panic!("the load failed: {e:#}");
    }

    assert!(state_path.is_file(), "the state file was created");
    for created in [p.join(".rocky"), state_dir] {
        let mode = std::fs::metadata(&created).unwrap().permissions().mode() & 0o777;
        assert_eq!(
            mode,
            0o700,
            "{} is owner-only, like the state files",
            created.display()
        );
    }
}
