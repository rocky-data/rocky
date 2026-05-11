//! Live-integration test for `UniformWriter::discover()`.
//!
//! Gated on `#[ignore]` + env vars so it only fires when the operator
//! deliberately opts in. The pattern matches
//! `rocky-databricks/tests/integration.rs`.
//!
//! To run:
//!
//! ```bash
//! eval $(aws configure export-credentials --profile <profile> --format env)
//! export AWS_DEFAULT_REGION=<region>
//! export ROCKY_TEST_S3_BUCKET=<bucket>
//! export ROCKY_TEST_S3_PREFIX=<path/to/table>
//! export ROCKY_TEST_CATALOG=<catalog>
//! export ROCKY_TEST_SCHEMA=<schema>
//! export ROCKY_TEST_TABLE=<table>
//! cargo test -p rocky-iceberg --test uniform_writer_live -- --ignored
//! ```
//!
//! The target table must be an external Delta UniForm table with
//! `delta.columnMapping.mode = 'name'`, unpartitioned, no rowTracking, and
//! no deletion vectors — i.e. the shape this Phase 1 writer can serve. The
//! test asserts `discover()` returns a `UniformTableState` consistent with
//! those constraints; it does not write anything to the table.
//!
//! `object_store::AmazonS3Builder::from_env()` reads `AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`) but does NOT
//! honour `AWS_PROFILE`. The `aws configure export-credentials` step above
//! resolves a profile to those env vars.

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use rocky_iceberg::uniform_writer::{Result, SqlClient, UniformWriter, UniformWriterConfig};

/// SQL client that panics if called — discover() never touches SQL, so the
/// integration test wires this stub in place of the real warehouse client.
struct PanicSqlClient;

#[async_trait::async_trait]
impl SqlClient for PanicSqlClient {
    async fn execute(&self, _sql: &str) -> Result<()> {
        panic!("discover() must not call SqlClient::execute");
    }
}

struct LiveConfig {
    bucket: String,
    prefix: String,
    catalog: String,
    schema: String,
    table: String,
    region: String,
}

fn try_load_config() -> Option<LiveConfig> {
    Some(LiveConfig {
        bucket: std::env::var("ROCKY_TEST_S3_BUCKET").ok()?,
        prefix: std::env::var("ROCKY_TEST_S3_PREFIX").ok()?,
        catalog: std::env::var("ROCKY_TEST_CATALOG").ok()?,
        schema: std::env::var("ROCKY_TEST_SCHEMA").ok()?,
        table: std::env::var("ROCKY_TEST_TABLE").ok()?,
        region: std::env::var("AWS_DEFAULT_REGION").ok()?,
    })
}

fn build_s3_store(cfg: &LiveConfig) -> Option<Arc<dyn object_store::ObjectStore>> {
    AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        .build()
        .ok()
        .map(|s| Arc::new(s) as Arc<dyn object_store::ObjectStore>)
}

#[tokio::test]
#[ignore]
async fn discover_phase1_compatible_sandbox() {
    let Some(cfg) = try_load_config() else {
        eprintln!(
            "skipping: required env vars not set — see test docstring for the \
             full list"
        );
        return;
    };
    let Some(store) = build_s3_store(&cfg) else {
        eprintln!("skipping: failed to build S3 store from environment");
        return;
    };
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog,
            schema: cfg.schema,
            table: cfg.table,
            prefix: cfg.prefix,
            engine_info: "rocky-iceberg/discover-live-test".into(),
        },
        store,
        Arc::new(PanicSqlClient),
    );
    let state = writer.discover().await.expect("discover() must succeed");

    // Phase 1 invariants — these are what `discover()` is supposed to
    // guarantee about a table the writer accepts. We don't assert on
    // specific column UUIDs / counts because those vary per workspace.
    assert!(
        !state.physical.is_empty(),
        "table must have at least one column"
    );
    assert_eq!(
        state.physical.len(),
        state.field_id.len(),
        "PHYSICAL + FIELD_ID maps must cover the same columns"
    );
    for col in state.physical.keys() {
        assert!(
            state.field_id.contains_key(col),
            "column {col} missing from field_id map"
        );
    }
    assert!(
        state.partition_columns.is_empty(),
        "phase 1 cannot serve partitioned tables"
    );
    assert!(
        !state.row_tracking_enabled,
        "phase 1 cannot serve rowTracking-enabled tables"
    );
    assert!(
        !state.deletion_vectors_enabled,
        "phase 1 cannot serve DV-enabled tables (and UniForm forbids DV anyway)"
    );
    assert!(
        state.next_commit_version >= 1,
        "bootstrap commit is v=0; next commit must be ≥ 1; got v={}",
        state.next_commit_version,
    );
}
