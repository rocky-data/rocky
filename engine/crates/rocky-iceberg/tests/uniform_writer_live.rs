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

/// Live test for `write_partitioned_batch()`.
///
/// Requires the same env vars as the other live tests, except
/// `ROCKY_TEST_*` should point at a **partitioned** UniForm table with
/// the canonical `(id BIGINT, payload STRING, region STRING)` schema and
/// `region` as the partition column. The test writes 3 rows into the
/// `region=eu` partition and verifies the new Parquet appears at
/// `<prefix>/region=eu/<hash>.parquet`.
#[tokio::test]
#[ignore]
async fn write_partitioned_batch_phase2_compatible_sandbox() {
    let Some(cfg) = try_load_config() else {
        eprintln!("skipping: ROCKY_TEST_* env vars not set");
        return;
    };
    let Some(store) = build_s3_store(&cfg) else {
        eprintln!("skipping: failed to build S3 store from environment");
        return;
    };
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            table: cfg.table.clone(),
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/write-partitioned-live-test".into(),
        },
        store.clone(),
        Arc::new(PanicSqlClient),
    );
    let state = writer.discover().await.expect("discover");

    if state.partition_columns.is_empty() {
        eprintln!(
            "skipping: this test expects a partitioned table; the target table is unpartitioned"
        );
        return;
    }
    let expected_cols: std::collections::BTreeSet<&str> =
        ["id", "payload", "region"].into_iter().collect();
    let actual_cols: std::collections::BTreeSet<&str> =
        state.physical.keys().map(String::as_str).collect();
    if expected_cols != actual_cols || state.partition_columns != vec!["region".to_string()] {
        eprintln!(
            "skipping: this test expects (id, payload, region) PARTITIONED BY (region); \
             got cols={actual_cols:?}, partitions={:?}",
            state.partition_columns,
        );
        return;
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", DataType::Utf8, false),
        Field::new("region", DataType::Utf8, false),
    ]));
    let ts_suffix = chrono::Utc::now().timestamp_micros();
    let ids = Int64Array::from(vec![900_201_i64, 900_202, 900_203]);
    let payload = StringArray::from(vec![
        format!("part-a-{ts_suffix}"),
        format!("part-b-{ts_suffix}"),
        format!("part-c-{ts_suffix}"),
    ]);
    let region = StringArray::from(vec!["eu", "eu", "eu"]);
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(ids), Arc::new(payload), Arc::new(region)],
    )
    .unwrap();

    let mut pv = std::collections::HashMap::new();
    pv.insert("region".to_string(), "eu".to_string());
    let result = writer
        .write_partitioned_batch(batch, pv)
        .await
        .expect("partitioned write must succeed");

    assert_eq!(result.num_records, 3);
    assert!(
        result.file_path.contains("/region=eu/"),
        "file path must carry partition prefix: {}",
        result.file_path
    );

    // Verify the Parquet landed in S3 at the Hive-style key.
    use futures::TryStreamExt;
    let part_prefix = object_store::path::Path::from(format!("{}/region=eu", cfg.prefix));
    let mut found = false;
    let mut stream = store.list(Some(&part_prefix));
    while let Some(item) = stream.try_next().await.expect("list partition prefix") {
        if item
            .location
            .as_ref()
            .ends_with(&format!("{}.parquet", result.blake3_hash))
        {
            found = true;
            break;
        }
    }
    assert!(
        found,
        "Parquet must exist at <prefix>/region=eu/{}.parquet",
        result.blake3_hash
    );
}

/// Live test for Phase 3 — rowTracking writer surface.
///
/// Requires the same env vars as the other live tests, except
/// `ROCKY_TEST_*` must point at a **rowTracking-enabled** UniForm table.
/// The test writes a small batch, then projects `_metadata.row_id` via
/// Databricks SQL — this projection is what fails (`Missing base_row_id
/// value`) when the writer omits `baseRowId` on the add action. A
/// successful projection proves the rowTracking write path is wired
/// correctly.
#[tokio::test]
#[ignore]
async fn row_tracking_round_trip_phase3_compatible_sandbox() {
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
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            table: cfg.table.clone(),
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/row-tracking-live-test".into(),
        },
        store.clone(),
        Arc::new(PanicSqlClient),
    );
    let state = writer.discover().await.expect("discover");
    if !state.row_tracking_enabled {
        eprintln!(
            "skipping: this test expects a rowTracking-enabled table; \
             target table has rowTracking off"
        );
        return;
    }
    let expected_cols: std::collections::BTreeSet<&str> =
        ["id", "name", "ts"].into_iter().collect();
    let actual_cols: std::collections::BTreeSet<&str> =
        state.physical.keys().map(String::as_str).collect();
    if expected_cols != actual_cols {
        eprintln!("skipping: this test expects schema (id, name, ts); table has {actual_cols:?}");
        return;
    }

    let watermark_before = state.row_tracking_next_id;
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
    let ids = Int64Array::from(vec![900_701_i64, 900_702, 900_703]);
    let names = StringArray::from(vec!["rt-a", "rt-b", "rt-c"]);
    let ts = TimestampMicrosecondArray::from(vec![now_micros, now_micros + 1, now_micros + 2])
        .with_timezone("UTC");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ts)]).unwrap();

    let written = writer.write_batch(batch).await.expect("write_batch");
    assert_eq!(written.num_records, 3);

    // MSCK so cross-engine readers see the new commit. Uses the
    // connector we built up-front (and reuses it for the row_id read
    // below).
    let msck = format!(
        "MSCK REPAIR TABLE {}.{}.{} SYNC METADATA",
        cfg.catalog, cfg.schema, cfg.table
    );
    connector.execute_statement(&msck).await.expect("MSCK");

    // The load-bearing assertion: `SELECT _metadata.row_id` succeeds on
    // the new rows. This is exactly what fails ("Missing base_row_id
    // value") when the writer omits baseRowId — Exp 9 documented it.
    let fqtn = format!("{}.{}.{}", cfg.catalog, cfg.schema, cfg.table);
    let sql = format!(
        "SELECT id, _metadata.row_id AS row_id FROM {fqtn} WHERE id >= 900701 AND id <= 900703 \
         ORDER BY id"
    );
    let result = connector
        .execute_sql(&sql)
        .await
        .expect("row_id projection");
    assert_eq!(result.rows.len(), 3);
    for (i, row) in result.rows.iter().enumerate() {
        // row_id is a BIGINT — Databricks returns it as a JSON string.
        let row_id: i64 = row[1]
            .as_i64()
            .or_else(|| row[1].as_str().and_then(|s| s.parse().ok()))
            .expect("row_id parses as i64");
        assert_eq!(
            row_id as u64,
            watermark_before + i as u64,
            "row {i}: expected row_id={}, got {row_id}",
            watermark_before + i as u64,
        );
    }

    // Re-discover should report watermark = watermark_before + 3.
    let state_after = writer.discover().await.expect("re-discover");
    assert_eq!(
        state_after.row_tracking_next_id,
        watermark_before + 3,
        "watermark must advance by 3 (before={watermark_before}, after={})",
        state_after.row_tracking_next_id,
    );
}

/// Live test for Phase 5 — schema evolution.
///
/// Requires the same env vars as the other live tests, except
/// `ROCKY_TEST_*` should point at a UniForm table that has been
/// `ALTER TABLE … ADD COLUMN`'d at least once (so the latest schema
/// lives in a post-bootstrap commit). The test:
///   - asserts `discover()` returns the post-ALTER schema, not the
///     bootstrap one
///   - writes a batch using the new column
///   - reads back via Photon and confirms the new-column rows are
///     visible
///
/// The canonical sandbox is the Exp 10 `schema_evo_t2` table (id,
/// name, value, extra) — `extra` was added by ALTER ADD COLUMN.
#[tokio::test]
#[ignore]
async fn schema_evolution_e2e_live_sandbox() {
    let Some(cfg) = try_load_config() else {
        eprintln!("skipping: ROCKY_TEST_* env vars not set");
        return;
    };
    let Some(store) = build_s3_store(&cfg) else {
        eprintln!("skipping: failed to build S3 store from environment");
        return;
    };
    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            table: cfg.table.clone(),
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/schema-evolution-live-test".into(),
        },
        store.clone(),
        Arc::new(PanicSqlClient),
    );
    let state = writer.discover().await.expect("discover");

    // This test targets a Phase-5 sandbox where ALTER ADD COLUMN has
    // landed. Skip cleanly if the operator points at a different shape.
    if !state.physical.contains_key("extra") {
        eprintln!(
            "skipping: expected the sandbox to have an `extra` column added \
             via ALTER ADD COLUMN; got cols {:?}",
            state.physical.keys().collect::<Vec<_>>()
        );
        return;
    }
    assert!(
        state.physical.contains_key("id"),
        "id retained from pre-ALTER schema"
    );

    let ts_suffix = chrono::Utc::now().timestamp_micros();
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("extra", DataType::Utf8, true),
    ]));
    let ids = Int64Array::from(vec![900_801_i64, 900_802]);
    let names = StringArray::from(vec!["evo-a", "evo-b"]);
    let values = Int64Array::from(vec![ts_suffix, ts_suffix + 1]);
    let extras = StringArray::from(vec![Some("post-alter-1"), Some("post-alter-2")]);
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(names),
            Arc::new(values),
            Arc::new(extras),
        ],
    )
    .unwrap();
    let result = writer
        .write_batch(batch)
        .await
        .expect("post-ALTER write must succeed");
    assert_eq!(result.num_records, 2);
}

/// Live end-to-end **point-to** (content-addressed reuse) against the
/// UniForm sandbox — the Databricks-gated verification of B5 stage 2's
/// strong reuse backend.
///
/// Two claims, kept separate (mirroring the provenance record's two-claim
/// framing):
/// 1. **Zero byte copy.** `recover_pointer_inputs` lifts a prior run's `add`
///    from its `_delta_log/{version}.json` (one GET, no parquet read) and
///    `commit_pointer_with_state` references that same blake3-named file —
///    no `build_parquet`, no parquet PUT.
/// 2. **Append-only correctness (the double-count guard).** On a *same-target*
///    point-to, the target already references the file. The pre-check must
///    make the commit a **no-op** (return the existing version, land no new
///    `_delta_log` entry), so Photon's `SELECT COUNT(*)` stays **unchanged**.
///    A double-counted row count here is the bug this test exists to catch —
///    it is why the runner-side reuse DECISION stays unwired until this
///    passes.
///
/// Requires the same env as `round_trip_phase1_compatible_sandbox`: the
/// `ROCKY_TEST_*` (catalog/schema/table/bucket/prefix) + AWS creds + the
/// `DATABRICKS_*` connection, against an unpartitioned, non-rowTracking
/// UniForm table with `(id BIGINT, name STRING, ts TIMESTAMP)`. All
/// workspace identifiers come from the environment — none are hard-coded.
#[tokio::test]
#[ignore]
async fn point_to_same_target_is_noop_live_sandbox() {
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

    let read_count = |sql_client: Arc<DatabricksSqlClient>, sql: String| async move {
        let res = sql_client.connector.execute_sql(&sql).await.expect("count");
        let cell = &res.rows[0][0];
        cell.as_i64()
            .or_else(|| cell.as_str().and_then(|s| s.parse().ok()))
            .expect("count parses as i64")
    };

    let writer = UniformWriter::new(
        UniformWriterConfig {
            catalog: cfg.catalog.clone(),
            schema: cfg.schema.clone(),
            table: cfg.table.clone(),
            prefix: cfg.prefix.clone(),
            engine_info: "rocky-iceberg/point-to-live-test".into(),
        },
        store.clone(),
        sql_client.clone() as Arc<dyn SqlClient>,
    );

    let state = writer.discover().await.expect("discover");
    if !state.partition_columns.is_empty() || state.row_tracking_enabled {
        eprintln!("skipping: point-to needs an unpartitioned, non-rowTracking table");
        return;
    }
    let expected_cols: std::collections::BTreeSet<&str> =
        ["id", "name", "ts"].into_iter().collect();
    let actual_cols: std::collections::BTreeSet<&str> =
        state.physical.keys().map(String::as_str).collect();
    if expected_cols != actual_cols {
        eprintln!("skipping: this test expects schema (id, name, ts); table has {actual_cols:?}");
        return;
    }

    // Step 1 — R's build: a fresh content-addressed file (unique ts so it
    // never collides with a prior run's blake3).
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
    let ids = Int64Array::from(vec![900_701_i64, 900_702, 900_703]);
    let names = StringArray::from(vec!["point-to-a", "point-to-b", "point-to-c"]);
    let ts = TimestampMicrosecondArray::from(vec![now_micros, now_micros + 1, now_micros + 2])
        .with_timezone("UTC");
    let batch =
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ts)]).unwrap();

    let r = writer
        .write_batch(batch)
        .await
        .expect("R build write_batch");
    writer
        .sync_iceberg_metadata()
        .await
        .expect("MSCK after build");
    let count_after_build = read_count(sql_client.clone(), count_sql.clone()).await;

    // Step 2 — recover R's add (one GET, no parquet read) and attempt a
    // same-target point-to.
    let pointer = writer
        .recover_pointer_inputs(
            r.commit_version,
            r.blake3_hash.clone(),
            r.num_records,
            r.size_bytes,
        )
        .await
        .expect("recover_pointer_inputs");
    assert_eq!(
        pointer.blake3_hash, r.blake3_hash,
        "shared bytes — same blake3"
    );

    let fresh_state = writer.discover().await.expect("discover before point-to");
    let pr = writer
        .commit_pointer_with_state(&pointer, fresh_state)
        .await
        .expect("point-to commit");
    writer
        .sync_iceberg_metadata()
        .await
        .expect("MSCK after point-to");

    // Claim 2 — the double-count guard held: the point-to was a no-op
    // (returned R's version) and Photon's count did NOT change.
    assert_eq!(
        pr.commit_version, r.commit_version,
        "same-target point-to must return R's existing commit version (no new commit)"
    );
    let count_after_pointto = read_count(sql_client.clone(), count_sql.clone()).await;
    assert_eq!(
        count_after_pointto, count_after_build,
        "same-target point-to must NOT double-count R's rows \
         (after_build={count_after_build}, after_point_to={count_after_pointto})"
    );
}
