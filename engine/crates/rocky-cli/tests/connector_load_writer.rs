//! A connector load refuses a state writer mode the commit would refuse,
//! BEFORE it claims or runs anything.
//!
//! The commit comes after the promote. A refusal there would leave the batch
//! loaded and the claim in flight, and every later run would load it again.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::sync::Arc;

use arrow::array::Int64Array;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rocky_cli::commands::{ConnectorLoadParams, execute_connector_load};
use rocky_core::config::{ConcurrencyControl, load_rocky_config_fingerprinted};
use rocky_core::connector::StateEnvelope;
use rocky_core::fault_store::{FaultMode, FaultOp};
use rocky_core::state_sync::{LedgerSeamSession, remote_testing};
use rocky_core::test_harness::CrossPodHarness;

#[tokio::test]
async fn an_off_writer_is_refused_before_the_claim_when_others_use_cas() {
    refused_before_the_claim(true, false).await;
}

/// The marker check cannot finish (the HEAD fails). The connector path
/// fails closed: nothing has run yet, so a false refusal costs nothing, while
/// a missed one would load the batch before the commit is refused.
/// Mutation: make `preflight_writer` non-strict and the run goes on.
#[tokio::test]
async fn an_inconclusive_marker_check_refuses_before_the_claim() {
    refused_before_the_claim(false, true).await;
}

async fn refused_before_the_claim(seed_marker: bool, fail_head: bool) {
    let _serial = remote_testing::serial_guard();
    let harness = CrossPodHarness::new_s3_like();

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

[state]
backend = "s3"
s3_bucket = "test"
concurrency_control = "off"

[state.retry]
max_retries = 0

[connector.demo]
manifest = "connector/connector.toml"
output_uri = "file://./landing"
target = {{ catalog = "", schema = "main", table = "events" }}
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
    // A connector that would write a batch, if it ever ran.
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
    let started = p.join("started");
    let script = p.join("connector/run.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\ncat > /dev/null\ntouch '{}'\ncp '{}' batch.parquet\n\
             printf '{{\"protocol_version\":0,\"files\":[{{\"path\":\"batch.parquet\",\"bytes\":%s,\"rows\":1}}],\"state\":{{\"schema_version\":1,\"value\":null}}}}' \"$(wc -c < batch.parquet | tr -d ' ')\" > _rocky_result.json\n",
            started.display(),
            parquet.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();

    let config_path = p.join("rocky.toml");
    let state_path = p.join(".rocky-state.redb");
    let loaded = load_rocky_config_fingerprinted(&config_path).unwrap();

    // Another writer commits by compare-and-swap, which creates the marker.
    let mut cas = loaded.config.state.clone();
    cas.concurrency_control = Some(ConcurrencyControl::Cas);
    if seed_marker {
        LedgerSeamSession::new(&cas, &state_path, false)
            .execute(|store, _generation| {
                let result = store
                    .connector_commit(
                        "other",
                        0,
                        &StateEnvelope {
                            schema_version: 1,
                            value: serde_json::Value::Null,
                        },
                    )
                    .map_err(rocky_core::state_sync::StateSyncError::from);
                Box::pin(async move { result })
            })
            .await
            .unwrap();
    }
    if fail_head {
        harness.faults.arm(FaultOp::Head, FaultMode::FailAll);
    }

    let params = ConnectorLoadParams::new(&config_path, &loaded, &state_path, "demo");
    let outcome = execute_connector_load(&params).await;
    let error = format!("{:#}", outcome.result.unwrap_err());
    assert!(error.contains("refuses this writer"), "{error}");
    assert!(
        outcome.output.is_none(),
        "the refusal came before the claim, so nothing is reported"
    );
    assert!(!started.exists(), "the connector never started");
    assert!(!db.exists(), "the warehouse was never opened");
}
