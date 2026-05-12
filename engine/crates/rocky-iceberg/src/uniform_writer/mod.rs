//! Content-addressed writer for Delta UniForm tables.
//!
//! Reads a Delta UniForm table's `_delta_log` state, writes deterministic
//! Parquet files keyed by their blake3 hash, and emits Delta commit JSONL
//! that references those files. After each commit, callers must trigger
//! `MSCK REPAIR TABLE ... SYNC METADATA` via the warehouse SQL surface so
//! UniForm regenerates the corresponding Iceberg metadata.
//!
//! Scope today (Phase 1 + Phase 2 + Phase 3):
//! - Single writer; conditional-put on the log entry covers contention.
//! - External Delta UniForm table on an object store the writer can `PUT`.
//! - Unpartitioned tables via [`UniformWriter::write_batch`].
//! - Partitioned tables via [`UniformWriter::write_partitioned_batch`]
//!   (caller pre-groups rows per partition tuple). `add.partitionValues`
//!   is keyed by physical-name UUID, not logical name (Exp 11).
//! - Row-tracking-enabled tables: every `add` action emits
//!   `baseRowId` + `defaultRowCommitVersion`, and each commit appends a
//!   `domainMetadata` action that bumps the `rowIdHighWaterMark` (Exp 9
//!   finding — reads projecting `_metadata.row_id` fail without these).
//! - No schema evolution, no deletion vectors. The writer errors loudly
//!   if it discovers DV at table-init time; UniForm + DV is forbidden
//!   by Delta itself anyway.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arrow::array::RecordBatch;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};

pub mod commit;
pub mod discover;
pub mod errors;
pub mod parquet_builder;

pub use errors::{Result, UniformWriterError};

/// Maximum cond-put retries when racing on a `_delta_log/{N}.json` PUT.
///
/// Phase 1 is documented as single-writer, so contention should be zero in
/// the happy path — this budget exists only to absorb spurious S3 retries.
/// Exp 8 confirmed S3 `If-None-Match: *` is honoured atomically, so a real
/// race terminates after at most one retry.
const COND_PUT_RETRY_BUDGET: u32 = 5;

/// Configuration for a `UniformWriter`.
///
/// The triple `(catalog, schema, table)` identifies the table in the
/// warehouse SQL surface; `prefix` is the object-store key prefix under
/// which `_delta_log/` and the table's Parquet files live. `engine_info`
/// is recorded in every commit's `commitInfo.engineInfo` so observers can
/// trace writes back to the Rocky version that emitted them.
#[derive(Debug, Clone)]
pub struct UniformWriterConfig {
    pub catalog: String,
    pub schema: String,
    pub table: String,
    pub prefix: String,
    pub engine_info: String,
}

impl UniformWriterConfig {
    pub fn fqtn(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Snapshot of a Delta UniForm table's state as observed by `discover()`.
///
/// `physical` and `field_id` are keyed by the logical column name and
/// return the Delta column-mapping UUID + numeric id, respectively. Stats
/// and `partitionValues` in `add` actions must be keyed by these UUIDs,
/// not the logical names (see Exp 11 — column-mapped Delta tables use
/// physical UUIDs for partition key lookup, not the logical name the
/// protocol spec implies).
#[derive(Debug, Clone)]
pub struct UniformTableState {
    pub physical: HashMap<String, String>,
    pub field_id: HashMap<String, i32>,
    pub partition_columns: Vec<String>,
    pub row_tracking_enabled: bool,
    pub deletion_vectors_enabled: bool,
    pub next_commit_version: u64,
    /// Next row-id to allocate when writing to a row-tracking-enabled
    /// table (the "high water mark" + 1).
    ///
    /// Delta requires every `add` action on a rowTracking table to carry
    /// a `baseRowId` (the smallest row-id in the file), and a
    /// `domainMetadata` action that bumps `rowIdHighWaterMark` to cover
    /// the newly written rows. Without these, reads that project
    /// `_metadata.row_id` fail with `Missing base_row_id value` (see
    /// Exp 9 finding).
    ///
    /// `0` for tables without rowTracking, and for rowTracking-enabled
    /// tables that have not yet been written to.
    pub row_tracking_next_id: u64,
}

/// Result of a successful `write_batch()` call.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub file_path: String,
    pub blake3_hash: String,
    pub commit_version: u64,
    pub num_records: usize,
    pub size_bytes: u64,
}

/// Minimal SQL execution surface the writer needs.
///
/// Phase 1 only uses this for `MSCK REPAIR TABLE ... SYNC METADATA` after
/// a successful commit. Implementations live next to the existing
/// warehouse adapters (e.g. `rocky-databricks` wraps its Statement
/// Execution API behind this trait in PR 4).
#[async_trait::async_trait]
pub trait SqlClient: Send + Sync {
    async fn execute(&self, sql: &str) -> Result<()>;
}

/// Content-addressed writer for a single Delta UniForm table.
pub struct UniformWriter {
    config: UniformWriterConfig,
    store: Arc<dyn ObjectStore>,
    sql: Arc<dyn SqlClient>,
}

impl UniformWriter {
    pub fn new(
        config: UniformWriterConfig,
        store: Arc<dyn ObjectStore>,
        sql: Arc<dyn SqlClient>,
    ) -> Self {
        Self { config, store, sql }
    }

    pub fn config(&self) -> &UniformWriterConfig {
        &self.config
    }

    pub fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    pub fn sql(&self) -> &Arc<dyn SqlClient> {
        &self.sql
    }

    /// Write a single Arrow [`RecordBatch`] as a content-addressed Parquet
    /// file plus a `_delta_log/{N:020}.json` commit referencing it.
    ///
    /// **Unpartitioned tables only.** For partitioned tables, callers
    /// pre-group rows by partition tuple and invoke
    /// [`UniformWriter::write_partitioned_batch`] once per partition.
    /// Mixing this entry point with a partitioned target returns
    /// [`UniformWriterError::PartitionedUnsupported`].
    ///
    /// Calls [`UniformWriter::discover`] first to read the current table
    /// state. To skip the discover round-trip (e.g. when the caller has a
    /// fresh state from a prior call), use
    /// [`UniformWriter::write_batch_with_state`].
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<WriteResult> {
        let state = self.discover().await?;
        self.write_batch_with_state(batch, state).await
    }

    /// Write a [`RecordBatch`] against a [`UniformTableState`] the caller
    /// already obtained.
    ///
    /// On a cond-put conflict (412 — another writer claimed `next_commit_version`
    /// first), refetches the next version from the live `_delta_log/` listing
    /// and retries up to [`COND_PUT_RETRY_BUDGET`] times.
    pub async fn write_batch_with_state(
        &self,
        batch: RecordBatch,
        state: UniformTableState,
    ) -> Result<WriteResult> {
        if !state.partition_columns.is_empty() {
            return Err(UniformWriterError::PartitionedUnsupported(
                state.partition_columns.clone(),
            ));
        }
        let empty = HashMap::new();
        self.write_internal(batch, state, &empty, |hash| format!("{hash}.parquet"))
            .await
    }

    /// Write a single Arrow [`RecordBatch`] to a partitioned UniForm table.
    ///
    /// The caller has pre-grouped rows so that every row in `batch`
    /// belongs to the same partition tuple, and supplies that tuple as
    /// `partition_values` keyed by logical column name. Stringification is
    /// the caller's responsibility (Delta partition values are always
    /// strings on the wire).
    ///
    /// The Parquet file is uploaded to a Hive-style prefix
    /// (`<col1>=<val1>/<col2>=<val2>/.../<hash>.parquet`); the emitted
    /// `add.partitionValues` is keyed by **physical** column name UUID, as
    /// required by Delta column-mapping (Exp 11 finding).
    ///
    /// Errors:
    /// - the target is unpartitioned → `DeltaLog` (caller should use
    ///   [`Self::write_batch`])
    /// - `partition_values` is missing a column or carries an unknown key
    ///   → `DeltaLog`
    pub async fn write_partitioned_batch(
        &self,
        batch: RecordBatch,
        partition_values: HashMap<String, String>,
    ) -> Result<WriteResult> {
        let state = self.discover().await?;
        self.write_partitioned_batch_with_state(batch, partition_values, state)
            .await
    }

    /// [`Self::write_partitioned_batch`] against a state the caller
    /// already obtained.
    pub async fn write_partitioned_batch_with_state(
        &self,
        batch: RecordBatch,
        partition_values: HashMap<String, String>,
        state: UniformTableState,
    ) -> Result<WriteResult> {
        if state.partition_columns.is_empty() {
            return Err(UniformWriterError::DeltaLog(
                "write_partitioned_batch called against unpartitioned table; \
                 use write_batch instead"
                    .to_string(),
            ));
        }
        // Validate keys match the table's partition columns.
        let table_partitions: HashSet<&str> =
            state.partition_columns.iter().map(String::as_str).collect();
        for col in &state.partition_columns {
            if !partition_values.contains_key(col) {
                return Err(UniformWriterError::DeltaLog(format!(
                    "missing partition value for column `{col}` (table partition columns: {:?})",
                    state.partition_columns
                )));
            }
        }
        for k in partition_values.keys() {
            if !table_partitions.contains(k.as_str()) {
                return Err(UniformWriterError::DeltaLog(format!(
                    "unexpected partition value for column `{k}` (table partition columns: {:?})",
                    state.partition_columns
                )));
            }
        }

        // Pre-compute the Hive-style partition path prefix; iteration in
        // table's partition_columns order keeps it deterministic.
        let mut partition_prefix = String::new();
        for col in &state.partition_columns {
            let v = partition_values.get(col).expect("checked above");
            partition_prefix.push_str(&format!("{col}={v}/"));
        }
        let pv_for_closure = partition_values.clone();
        self.write_internal(batch, state, &pv_for_closure, move |hash| {
            format!("{partition_prefix}{hash}.parquet")
        })
        .await
    }

    /// Shared write pipeline used by both unpartitioned and partitioned
    /// entry points. `path_for_hash` produces the `add.path` (relative to
    /// the table prefix) given the blake3 hash of the Parquet bytes.
    async fn write_internal(
        &self,
        batch: RecordBatch,
        mut state: UniformTableState,
        partition_values: &HashMap<String, String>,
        path_for_hash: impl FnOnce(&str) -> String,
    ) -> Result<WriteResult> {
        // 1. Build deterministic Parquet bytes from the input batch.
        let parquet_bytes = parquet_builder::build_parquet(&batch, &state)?;
        let hash = blake3::hash(&parquet_bytes).to_hex().to_string();
        let add_file_path = path_for_hash(&hash);
        let file_size = parquet_bytes.len() as u64;
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let parquet_path = Path::from(format!("{prefix}/{add_file_path}"));

        // 2. PUT the Parquet. Same content → same hash → same key → idempotent.
        self.store
            .put(&parquet_path, PutPayload::from(Bytes::from(parquet_bytes)))
            .await?;

        // 3. Loop: build commit, PUT with `If-None-Match: *`, retry on 412.
        let modification_time_millis = chrono::Utc::now().timestamp_millis();
        let num_records = batch.num_rows();
        for attempt in 0..COND_PUT_RETRY_BUDGET {
            let target_version = state.next_commit_version;
            // For rowTracking-enabled tables, allocate a contiguous range
            // of row-ids for this commit's rows. `base` = current
            // watermark; `new_high` = watermark + num_records - 1.
            // Re-computed on every retry because state.row_tracking_next_id
            // may advance between retries if the cond-put loser refreshes.
            let row_tracking = if state.row_tracking_enabled {
                if num_records == 0 {
                    None
                } else {
                    let base = state.row_tracking_next_id;
                    let new_high = base.checked_add(num_records as u64 - 1).ok_or_else(|| {
                        UniformWriterError::DeltaLog(format!(
                            "row_tracking_next_id={base} + {num_records} rows overflows u64"
                        ))
                    })?;
                    Some(commit::RowTrackingCommit {
                        base_row_id: base,
                        default_row_commit_version: target_version,
                        new_high_water_mark: new_high,
                    })
                }
            } else {
                None
            };
            let inputs = commit::CommitInputs {
                batch: &batch,
                state: &state,
                add_file_path: &add_file_path,
                file_size,
                modification_time_millis,
                engine_info: &self.config.engine_info,
                partition_values,
                row_tracking,
            };
            let commit_body = commit::build_commit_jsonl(&inputs)?;
            let log_path = Path::from(format!("{prefix}/_delta_log/{target_version:020}.json"));
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match self
                .store
                .put_opts(&log_path, PutPayload::from(Bytes::from(commit_body)), opts)
                .await
            {
                Ok(_) => {
                    return Ok(WriteResult {
                        file_path: parquet_path.to_string(),
                        blake3_hash: hash,
                        commit_version: target_version,
                        num_records: batch.num_rows(),
                        size_bytes: file_size,
                    });
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    let observed = discover::next_commit_version(&*self.store, &prefix).await?;
                    state.next_commit_version = observed.max(target_version.saturating_add(1));
                    // The cond-put loser may also be racing on the
                    // row-tracking watermark — re-scan the log so the
                    // retry doesn't allocate row-ids that the winner
                    // already claimed.
                    if state.row_tracking_enabled {
                        state.row_tracking_next_id =
                            discover::discover_row_tracking_next_id(&*self.store, &prefix).await?;
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        previous_target = target_version,
                        new_target = state.next_commit_version,
                        new_row_tracking_next_id = state.row_tracking_next_id,
                        "cond-put 412 on _delta_log entry; retrying"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(UniformWriterError::CondPutRetryExhausted(format!(
            "exhausted {COND_PUT_RETRY_BUDGET} retries chasing next_commit_version"
        )))
    }

    /// Trigger `MSCK REPAIR TABLE <fqtn> SYNC METADATA` on the warehouse so
    /// UniForm regenerates the Iceberg metadata next to the table.
    ///
    /// Callers should run this after every successful [`Self::write_batch`]
    /// so cross-engine readers (DuckDB iceberg_scan, Iceberg-aware Trino,
    /// etc.) see the new commit. Photon reads via the Delta surface
    /// directly and does not need MSCK.
    ///
    /// Phase 1 makes this a separate call so callers can batch multiple
    /// writes and trigger one MSCK at the end. The default `write_batch`
    /// path does not call it implicitly.
    pub async fn sync_iceberg_metadata(&self) -> Result<()> {
        let sql = format!("MSCK REPAIR TABLE {} SYNC METADATA", self.config.fqtn());
        tracing::debug!(sql = %sql, "issuing MSCK REPAIR to sync iceberg metadata");
        self.sql.execute(&sql).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use serde_json::Value;

    #[test]
    fn config_builds_fqtn() {
        let c = UniformWriterConfig {
            catalog: "cat".into(),
            schema: "sch".into(),
            table: "tbl".into(),
            prefix: "p/".into(),
            engine_info: "rocky-iceberg/test".into(),
        };
        assert_eq!(c.fqtn(), "cat.sch.tbl");
    }

    struct PanicSqlClient;

    #[async_trait::async_trait]
    impl SqlClient for PanicSqlClient {
        async fn execute(&self, _sql: &str) -> Result<()> {
            panic!("write_batch must not call SqlClient::execute");
        }
    }

    /// Records every SQL statement issued for assertion in tests.
    #[derive(Default)]
    struct RecordingSqlClient {
        log: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl SqlClient for RecordingSqlClient {
        async fn execute(&self, sql: &str) -> Result<()> {
            self.log.lock().unwrap().push(sql.to_string());
            Ok(())
        }
    }

    /// Seed an `InMemory` object store with a partitioned bootstrap commit
    /// (`region` as the lone partition column).
    async fn seed_partitioned_bootstrap(store: &InMemory, prefix: &str) {
        let bootstrap = serde_json::json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"],
            }
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-id-uuid"
                        }},
                        {"name": "payload", "type": "string", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 2,
                            "delta.columnMapping.physicalName": "col-payload-uuid"
                        }},
                        {"name": "region", "type": "string", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 3,
                            "delta.columnMapping.physicalName": "col-region-uuid"
                        }}
                    ]
                })).unwrap(),
                "partitionColumns": ["region"],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true"
                },
                "createdTime": 0
            }
        });
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&bootstrap).unwrap(),
            serde_json::to_string(&metadata).unwrap(),
        );
        store
            .put(
                &object_store::path::Path::from(format!(
                    "{prefix}/_delta_log/00000000000000000000.json"
                )),
                PutPayload::from(Bytes::from(body.into_bytes())),
            )
            .await
            .unwrap();
    }

    fn make_partitioned_batch(region: &str, rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let payload = StringArray::from(
            (0..rows)
                .map(|i| format!("{region}-{i}"))
                .collect::<Vec<_>>(),
        );
        let region_col = StringArray::from(vec![region; rows]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(payload), Arc::new(region_col)],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn write_partitioned_batch_round_trip_against_in_memory_store() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "tbl";
        seed_partitioned_bootstrap(&store, prefix).await;
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );

        let batch = make_partitioned_batch("eu", 5);
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        let result = writer
            .write_partitioned_batch(batch, pv)
            .await
            .expect("partitioned write must succeed");

        assert_eq!(result.commit_version, 1);
        assert_eq!(result.num_records, 5);
        // Hive-style path includes the partition column.
        assert!(
            result.file_path.contains("/region=eu/"),
            "file path must carry partition prefix: {}",
            result.file_path
        );

        // The commit must encode partitionValues by physical UUID.
        let log_path = object_store::path::Path::from(format!(
            "{prefix}/_delta_log/00000000000000000001.json"
        ));
        let log = store.get(&log_path).await.unwrap().bytes().await.unwrap();
        let log_text = std::str::from_utf8(&log).unwrap();
        let add_line = log_text.lines().nth(1).unwrap();
        let add: Value = serde_json::from_str(add_line).unwrap();
        assert_eq!(
            add["add"]["partitionValues"],
            serde_json::json!({"col-region-uuid": "eu"})
        );
    }

    #[tokio::test]
    async fn write_batch_rejects_partitioned_table() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "tbl";
        seed_partitioned_bootstrap(&store, prefix).await;
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );
        match writer.write_batch(make_partitioned_batch("eu", 1)).await {
            Err(UniformWriterError::PartitionedUnsupported(cols)) => {
                assert_eq!(cols, vec!["region".to_string()]);
            }
            other => panic!("expected PartitionedUnsupported, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn write_partitioned_batch_rejects_unpartitioned_table() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "tbl";
        seed_bootstrap(&store, prefix).await;
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        match writer.write_partitioned_batch(make_batch(1), pv).await {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("unpartitioned"),
                    "error must mention unpartitioned table: {msg}"
                );
            }
            other => panic!("expected DeltaLog error, got {other:?}"),
        }
    }

    async fn seed_row_tracking_bootstrap(store: &InMemory, prefix: &str) {
        let bootstrap = serde_json::json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": [
                    "columnMapping", "icebergCompatV2", "invariants",
                    "appendOnly", "rowTracking", "domainMetadata"
                ],
            }
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        {"name": "id", "type": "long", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 1,
                            "delta.columnMapping.physicalName": "col-id-uuid"
                        }},
                        {"name": "name", "type": "string", "nullable": false, "metadata": {
                            "delta.columnMapping.id": 2,
                            "delta.columnMapping.physicalName": "col-name-uuid"
                        }}
                    ]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true",
                    "delta.enableRowTracking": "true"
                },
                "createdTime": 0
            }
        });
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&bootstrap).unwrap(),
            serde_json::to_string(&metadata).unwrap(),
        );
        store
            .put(
                &object_store::path::Path::from(format!(
                    "{prefix}/_delta_log/00000000000000000000.json"
                )),
                PutPayload::from(Bytes::from(body.into_bytes())),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn row_tracking_round_trip_emits_base_row_id_and_watermark() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "rt";
        seed_row_tracking_bootstrap(&store, prefix).await;
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );

        let result = writer.write_batch(make_batch(10)).await.unwrap();
        assert_eq!(result.num_records, 10);

        // Inspect the commit. Lines: commitInfo, add, domainMetadata.
        let log_path = object_store::path::Path::from(format!(
            "{prefix}/_delta_log/00000000000000000001.json"
        ));
        let log = store.get(&log_path).await.unwrap().bytes().await.unwrap();
        let log_text = std::str::from_utf8(&log).unwrap();
        let lines: Vec<&str> = log_text.lines().collect();
        assert_eq!(lines.len(), 3, "rowTracking commit must have 3 actions");

        let add: Value = serde_json::from_str(lines[1]).unwrap();
        let add_obj = add.get("add").unwrap();
        assert_eq!(add_obj["baseRowId"], 0, "first write starts at row-id 0");
        assert_eq!(add_obj["defaultRowCommitVersion"], 1);

        let dm: Value = serde_json::from_str(lines[2]).unwrap();
        assert_eq!(dm["domainMetadata"]["domain"], "delta.rowTracking");
        let cfg_raw = dm["domainMetadata"]["configuration"].as_str().unwrap();
        let cfg: Value = serde_json::from_str(cfg_raw).unwrap();
        // 10 rows starting at 0 → high water mark = 9.
        assert_eq!(cfg["rowIdHighWaterMark"], 9);

        // A second discover() should pick the watermark up.
        let state2 = writer.discover().await.unwrap();
        assert_eq!(state2.row_tracking_next_id, 10);

        // A second write should allocate row-ids 10..19.
        let result2 = writer.write_batch(make_batch(5)).await.unwrap();
        assert_eq!(result2.num_records, 5);
        let log_path2 = object_store::path::Path::from(format!(
            "{prefix}/_delta_log/00000000000000000002.json"
        ));
        let log2 = store.get(&log_path2).await.unwrap().bytes().await.unwrap();
        let lines2: Vec<&str> = std::str::from_utf8(&log2).unwrap().lines().collect();
        let add2: Value = serde_json::from_str(lines2[1]).unwrap();
        assert_eq!(add2["add"]["baseRowId"], 10);
        let dm2: Value = serde_json::from_str(lines2[2]).unwrap();
        let cfg2_raw = dm2["domainMetadata"]["configuration"].as_str().unwrap();
        let cfg2: Value = serde_json::from_str(cfg2_raw).unwrap();
        // Existing 10 rows + new 5 → high water mark = 14.
        assert_eq!(cfg2["rowIdHighWaterMark"], 14);
    }

    #[tokio::test]
    async fn sync_iceberg_metadata_issues_msck_repair() {
        let recorder = Arc::new(RecordingSqlClient::default());
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "cat".into(),
                schema: "sch".into(),
                table: "tbl".into(),
                prefix: "p".into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
            recorder.clone(),
        );
        writer.sync_iceberg_metadata().await.unwrap();
        let log = recorder.log.lock().unwrap();
        assert_eq!(log.len(), 1);
        assert_eq!(log[0], "MSCK REPAIR TABLE cat.sch.tbl SYNC METADATA");
    }

    /// Seed an `InMemory` object store with a minimal Phase-1-compatible
    /// bootstrap commit so `discover()` succeeds and `write_batch()` can
    /// emit `_delta_log/00000000000000000001.json` on top of it.
    async fn seed_bootstrap(store: &InMemory, prefix: &str) {
        let bootstrap = serde_json::json!({
            "protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "invariants", "appendOnly"],
            }
        });
        let metadata = serde_json::json!({
            "metaData": {
                "id": "00000000-0000-0000-0000-000000000000",
                "format": {"provider": "parquet", "options": {}},
                "schemaString": serde_json::to_string(&serde_json::json!({
                    "type": "struct",
                    "fields": [
                        {
                            "name": "id",
                            "type": "long",
                            "nullable": false,
                            "metadata": {
                                "delta.columnMapping.id": 1,
                                "delta.columnMapping.physicalName": "col-id-uuid"
                            }
                        },
                        {
                            "name": "name",
                            "type": "string",
                            "nullable": false,
                            "metadata": {
                                "delta.columnMapping.id": 2,
                                "delta.columnMapping.physicalName": "col-name-uuid"
                            }
                        }
                    ]
                })).unwrap(),
                "partitionColumns": [],
                "configuration": {
                    "delta.columnMapping.mode": "name",
                    "delta.universalFormat.enabledFormats": "iceberg",
                    "delta.enableIcebergCompatV2": "true"
                },
                "createdTime": 0
            }
        });
        let body = format!(
            "{}\n{}\n",
            serde_json::to_string(&bootstrap).unwrap(),
            serde_json::to_string(&metadata).unwrap(),
        );
        store
            .put(
                &object_store::path::Path::from(format!(
                    "{prefix}/_delta_log/00000000000000000000.json"
                )),
                PutPayload::from(Bytes::from(body.into_bytes())),
            )
            .await
            .unwrap();
    }

    fn make_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from((0..rows as i64).collect::<Vec<_>>());
        let names = StringArray::from((0..rows).map(|i| format!("r{i}")).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    #[tokio::test]
    async fn write_batch_round_trip_against_in_memory_store() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "tbl";
        seed_bootstrap(&store, prefix).await;

        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );

        let batch = make_batch(10);
        let result = writer
            .write_batch(batch)
            .await
            .expect("write_batch must succeed");

        assert_eq!(result.commit_version, 1);
        assert_eq!(result.num_records, 10);
        assert_eq!(
            result.size_bytes as usize,
            /* SNAPPY + 10 rows ≫ 0 */ result.size_bytes as usize
        );
        assert!(result.size_bytes > 0);
        assert!(!result.blake3_hash.is_empty());
        assert!(result.file_path.ends_with(".parquet"));

        // The Parquet file + the new commit should be there.
        let log_path = object_store::path::Path::from(format!(
            "{prefix}/_delta_log/00000000000000000001.json"
        ));
        let log_body = store.get(&log_path).await.unwrap().bytes().await.unwrap();
        let log_text = std::str::from_utf8(&log_body).unwrap();
        let lines: Vec<&str> = log_text.lines().collect();
        assert_eq!(lines.len(), 2, "commit should be commitInfo + 1 add");
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(
            add["add"]["path"],
            result.file_path.split('/').next_back().unwrap()
        );

        // Discover again — next_commit_version should now be 2.
        let state2 = writer.discover().await.expect("discover after write");
        assert_eq!(state2.next_commit_version, 2);
    }

    #[tokio::test]
    async fn write_batch_retries_on_cond_put_conflict() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let prefix = "tbl";
        seed_bootstrap(&store, prefix).await;

        // Pre-emptively park a commit at v=1 so the first write's cond-put
        // will 412. The writer must refetch and land at v=2.
        store
            .put(
                &object_store::path::Path::from(format!(
                    "{prefix}/_delta_log/00000000000000000001.json"
                )),
                PutPayload::from(Bytes::from_static(b"{\"commitInfo\":{}}\n")),
            )
            .await
            .unwrap();

        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );

        // discover() sees v=1 already exists, returns next=2.
        let state = writer.discover().await.unwrap();
        assert_eq!(state.next_commit_version, 2);

        let result = writer.write_batch(make_batch(3)).await.unwrap();
        assert_eq!(result.commit_version, 2, "writer must skip the parked v=1");
    }
}
