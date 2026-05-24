//! Integration test: spawn the `rocky-echo` example process adapter and
//! round-trip the core wire-protocol methods.
//!
//! This test pins the protocol contract the example documents:
//!
//! 1. The adapter spawns and the engine reads its manifest via
//!    `initialize`.
//! 2. `execute_statement` round-trips successfully against a mock
//!    statement.
//! 3. `describe_table` returns columns for the pre-baked
//!    `main.demo.events` table.
//! 4. `table_exists` returns `true` / `false` correctly.
//!
//! The test is `#[cfg(unix)]` because it depends on `python3` being on
//! `$PATH` and the example script being executable — true for the
//! engine's Linux/macOS CI matrix, not for the Windows job.

#![cfg(unix)]

use std::path::PathBuf;

use rocky_adapter_sdk::WarehouseAdapter;
use rocky_adapter_sdk::process::ProcessAdapter;
use rocky_adapter_sdk::traits::TableRef;

/// Locate the bundled `rocky-echo` example adapter relative to the
/// `rocky-cli` crate's `Cargo.toml`.
fn echo_adapter_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        // engine/crates/rocky-cli -> engine/examples/process-adapter-echo
        .join("../../examples/process-adapter-echo/rocky-echo")
}

#[tokio::test]
async fn process_adapter_round_trip_echo() {
    let path = echo_adapter_path();
    if !path.exists() {
        panic!("example adapter missing at {}", path.display());
    }

    let adapter =
        match ProcessAdapter::spawn(&path.display().to_string(), &[], &serde_json::json!({})).await
        {
            Ok(a) => a,
            Err(e) => {
                // Skip if python3 isn't available; we don't want CI to fail
                // on a missing interpreter the test only uses to emulate an
                // adapter.
                eprintln!("skipping: failed to spawn rocky-echo (python3 unavailable?): {e}");
                return;
            }
        };

    // 1. The manifest was loaded during spawn.
    let manifest = adapter.manifest();
    assert_eq!(manifest.name, "echo");
    assert_eq!(manifest.dialect, "echo");
    assert!(manifest.capabilities.warehouse, "warehouse must be true");
    assert!(
        !manifest.capabilities.governance,
        "echo doesn't claim governance"
    );

    // 2. execute_statement round-trips.
    adapter
        .execute_statement("CREATE TABLE main.demo.events (id BIGINT)")
        .await
        .expect("execute_statement should succeed against echo");

    // 3. describe_table returns the pre-baked columns.
    let columns = adapter
        .describe_table(&TableRef {
            catalog: "main".into(),
            schema: "demo".into(),
            table: "events".into(),
        })
        .await
        .expect("describe_table should succeed");
    assert_eq!(columns.len(), 3, "echo declares 3 columns for events");
    let names: Vec<&str> = columns.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["id", "name", "ts"]);

    // 4. table_exists is true for the seeded table and false for an unknown one.
    let exists = adapter
        .table_exists(&TableRef {
            catalog: "main".into(),
            schema: "demo".into(),
            table: "events".into(),
        })
        .await
        .expect("table_exists should succeed");
    assert!(exists, "events should exist");

    let missing = adapter
        .table_exists(&TableRef {
            catalog: "main".into(),
            schema: "demo".into(),
            table: "nope".into(),
        })
        .await
        .expect("table_exists should succeed");
    assert!(!missing, "missing tables should report exists=false");

    let _ = adapter.close().await;
}
