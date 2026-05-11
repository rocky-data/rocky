//! Live-integration tests for `UniformWriter` against a real Databricks
//! UniForm table + S3 prefix.
//!
//! Gated on `#[ignore]` + env vars so they only fire when the operator
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
//! # Databricks creds (only needed for the round_trip test):
//! export DATABRICKS_HOST=<host>
//! export DATABRICKS_HTTP_PATH=<http_path>
//! export DATABRICKS_CLIENT_ID=<oauth_m2m_client_id>
//! export DATABRICKS_CLIENT_SECRET=<oauth_m2m_client_secret>
//! cargo test -p rocky-iceberg --test uniform_writer_live -- --ignored
//! ```
//!
//! The target table must be an external Delta UniForm table with
//! `delta.columnMapping.mode = 'name'`, unpartitioned, no rowTracking, and
//! no deletion vectors — i.e. the shape this Phase 1 writer can serve. The
//! `round_trip` test additionally requires the canonical
//! `(id BIGINT, name STRING, ts TIMESTAMP)` schema (id/name/ts).
//!
//! `object_store::AmazonS3Builder::from_env()` reads `AWS_ACCESS_KEY_ID` /
//! `AWS_SECRET_ACCESS_KEY` (+ optional `AWS_SESSION_TOKEN`) but does NOT
//! honour `AWS_PROFILE`. The `aws configure export-credentials` step above
//! resolves a profile to those env vars.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_iceberg::uniform_writer::{
    Result, SqlClient, UniformWriter, UniformWriterConfig, UniformWriterError,
};

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
    // `ETagMatch` enables RFC 9110 If-None-Match / If-Match conditional
    // headers, which is what AWS S3 supports natively (since Nov 2024 — see
    // Exp 8). Without this, `PutMode::Create` on the writer's log-commit
    // PUT returns `NotImplemented`.
    AmazonS3Builder::from_env()
        .with_bucket_name(&cfg.bucket)
        .with_region(&cfg.region)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
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

/// End-to-end live test for `write_batch()`.
///
/// Requires the same `ROCKY_TEST_*` env vars as `discover_phase1_compatible_sandbox`,
/// plus the target table must have the canonical `(id BIGINT, name STRING,
/// ts TIMESTAMP)` schema — that's the shape this test builds an
/// `arrow::RecordBatch` for. Operators pointing at a differently-shaped
/// table will see the test skip with a message.
///
/// The write is additive — it bumps the table's commit version and adds one
/// Parquet file. Re-running the test is safe (different `ts` values per run
/// produce a different blake3 hash, so the new file lands as a separate
/// commit).
#[tokio::test]
#[ignore]
async fn write_batch_phase1_compatible_sandbox() {
    let Some(cfg) = try_load_config() else {
        eprintln!("skipping: required env vars not set");
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
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/write-batch-live-test".into(),
        },
        store.clone(),
        Arc::new(PanicSqlClient),
    );
    let state = writer.discover().await.expect("discover");

    let expected_cols: std::collections::BTreeSet<&str> =
        ["id", "name", "ts"].into_iter().collect();
    let actual_cols: std::collections::BTreeSet<&str> =
        state.physical.keys().map(String::as_str).collect();
    if expected_cols != actual_cols {
        eprintln!("skipping: this test expects schema (id, name, ts); table has {actual_cols:?}");
        return;
    }

    let before_version = state.next_commit_version;

    let now_micros = chrono::Utc::now().timestamp_micros();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let ids = Int64Array::from(vec![900_001_i64, 900_002, 900_003]);
    let names = StringArray::from(vec!["live-a", "live-b", "live-c"]);
    let ts = TimestampMicrosecondArray::from(vec![now_micros, now_micros + 1, now_micros + 2])
        .with_timezone("UTC");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ts)]).unwrap();

    let result = writer
        .write_batch(batch)
        .await
        .expect("write_batch must succeed on the sandbox");

    assert_eq!(result.num_records, 3);
    assert!(result.size_bytes > 0);
    assert!(result.commit_version >= before_version);
    assert!(
        result.blake3_hash.len() == 64,
        "blake3 hex digest is 64 chars"
    );

    let state_after = writer.discover().await.expect("re-discover");
    assert!(
        state_after.next_commit_version > before_version,
        "next_commit_version must bump after write_batch (before={before_version}, after={})",
        state_after.next_commit_version,
    );
}

// ---------------------------------------------------------------------------
// Full Phase 1 round-trip: discover → write_batch → sync_iceberg_metadata →
// SELECT COUNT(*) via Photon.
// ---------------------------------------------------------------------------

/// `SqlClient` impl that wraps a real `DatabricksConnector` for use in the
/// round-trip test. Production wire-up (e.g. a Rocky CLI command) will host
/// the equivalent impl in a non-test crate; Phase 1 keeps it here so
/// `rocky-iceberg` does not need a hard dep on `rocky-databricks`.
struct DatabricksSqlClient {
    connector: DatabricksConnector,
}

#[async_trait::async_trait]
impl SqlClient for DatabricksSqlClient {
    async fn execute(&self, sql: &str) -> Result<()> {
        self.connector
            .execute_statement(sql)
            .await
            .map(|_statement_id| ())
            .map_err(|e| UniformWriterError::Sql(format!("{e}")))
    }
}

fn connector_from_env() -> Option<DatabricksConnector> {
    let host = std::env::var("DATABRICKS_HOST").ok()?;
    let http_path = std::env::var("DATABRICKS_HTTP_PATH").ok()?;
    let warehouse_id = ConnectorConfig::warehouse_id_from_http_path(&http_path)?;
    let auth = Auth::from_config(AuthConfig {
        host: host.clone(),
        token: std::env::var("DATABRICKS_TOKEN").ok(),
        client_id: std::env::var("DATABRICKS_CLIENT_ID").ok(),
        client_secret: std::env::var("DATABRICKS_CLIENT_SECRET").ok(),
    })
    .ok()?;
    Some(DatabricksConnector::new(
        ConnectorConfig {
            host,
            warehouse_id,
            timeout: std::time::Duration::from_secs(120),
            retry: Default::default(),
        },
        auth,
    ))
}

/// End-to-end Phase 1 round trip: write a content-addressed Parquet via
/// `write_batch`, trigger MSCK via `sync_iceberg_metadata`, and confirm:
///   - `SELECT COUNT(*)` via Photon bumped by the number of rows written
///   - at least one `*.metadata.json` file appears under `metadata/`
///     (proving MSCK regenerated the Iceberg side of UniForm)
///
/// Requires the same `ROCKY_TEST_*` + AWS env as the other live tests,
/// plus `DATABRICKS_HOST` / `_HTTP_PATH` / `_CLIENT_ID` / `_CLIENT_SECRET`
/// (or `DATABRICKS_TOKEN`).
#[tokio::test]
#[ignore]
async fn round_trip_phase1_compatible_sandbox() {
    let Some(cfg) = try_load_config() else {
        eprintln!("skipping: ROCKY_TEST_* env vars not set");
        return;
    };
    let Some(store) = build_s3_store(&cfg) else {
        eprintln!("skipping: failed to build S3 store from environment");
        return;
    };
    let Some(connector) = connector_from_env() else {
        eprintln!("skipping: DATABRICKS_* env vars not set");
        return;
    };

    let sql_client = Arc::new(DatabricksSqlClient { connector });
    let fqtn = format!("{}.{}.{}", cfg.catalog, cfg.schema, cfg.table);
    let count_sql = format!("SELECT COUNT(*) AS n FROM {fqtn}");

    // Count BEFORE the write — uses the connector directly because the
    // SqlClient trait only exposes execute (no row return).
    let count_before = sql_client
        .connector
        .execute_sql(&count_sql)
        .await
        .expect("count before");
    let n_before: i64 = count_before.rows[0][0]
        .as_str()
        .or_else(|| count_before.rows[0][0].as_i64().map(|_| ""))
        .and_then(|s| s.parse().ok())
        .or_else(|| count_before.rows[0][0].as_i64())
        .expect("parse count as i64");

    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            table: cfg.table.clone(),
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/round-trip-live-test".into(),
        },
        store.clone(),
        sql_client.clone() as Arc<dyn SqlClient>,
    );

    let state = writer.discover().await.expect("discover");
    let expected_cols: std::collections::BTreeSet<&str> =
        ["id", "name", "ts"].into_iter().collect();
    let actual_cols: std::collections::BTreeSet<&str> =
        state.physical.keys().map(String::as_str).collect();
    if expected_cols != actual_cols {
        eprintln!("skipping: this test expects schema (id, name, ts); table has {actual_cols:?}");
        return;
    }

    // Build a 3-row batch with a fresh ts per run so re-runs don't collide
    // on blake3.
    let now_micros = chrono::Utc::now().timestamp_micros();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
    ]));
    let ids = Int64Array::from(vec![900_101_i64, 900_102, 900_103]);
    let names = StringArray::from(vec!["round-trip-a", "round-trip-b", "round-trip-c"]);
    let ts = TimestampMicrosecondArray::from(vec![now_micros, now_micros + 1, now_micros + 2])
        .with_timezone("UTC");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ts)]).unwrap();

    let written = writer.write_batch(batch).await.expect("write_batch");
    assert_eq!(written.num_records, 3);

    writer
        .sync_iceberg_metadata()
        .await
        .expect("MSCK REPAIR must succeed");

    // Verify Photon sees the new rows.
    let count_after = sql_client
        .connector
        .execute_sql(&count_sql)
        .await
        .expect("count after");
    let n_after: i64 = count_after.rows[0][0]
        .as_str()
        .or_else(|| count_after.rows[0][0].as_i64().map(|_| ""))
        .and_then(|s| s.parse().ok())
        .or_else(|| count_after.rows[0][0].as_i64())
        .expect("parse count as i64");
    assert_eq!(
        n_after,
        n_before + 3,
        "Photon count must bump by 3 (before={n_before}, after={n_after})"
    );

    // Verify a `metadata/*.metadata.json` exists after MSCK.
    use futures::TryStreamExt;
    let metadata_prefix = object_store::path::Path::from(format!("{}/metadata", cfg.prefix));
    let mut metadata_json_count = 0_usize;
    let mut stream = store.list(Some(&metadata_prefix));
    while let Some(item) = stream.try_next().await.expect("list metadata") {
        if item.location.as_ref().ends_with(".metadata.json") {
            metadata_json_count += 1;
        }
    }
    assert!(
        metadata_json_count > 0,
        "MSCK must have written at least one *.metadata.json file under metadata/"
    );
}
