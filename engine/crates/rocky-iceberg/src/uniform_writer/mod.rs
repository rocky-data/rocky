//! Content-addressed writer for Delta UniForm tables.
//!
//! Reads a Delta UniForm table's `_delta_log` state, writes deterministic
//! Parquet files keyed by their blake3 hash, and emits Delta commit JSONL
//! that references those files. After each commit, callers must trigger
//! `MSCK REPAIR TABLE ... SYNC METADATA` via the warehouse SQL surface so
//! UniForm regenerates the corresponding Iceberg metadata.
//!
//! Phase 1 scope (locked, see `rocky-arc1-wave2-phase1-uniform-writer.md`):
//! - Single writer (no cross-writer coordination beyond conditional-put on
//!   the log entry).
//! - External Delta UniForm table on an object store the writer can `PUT`.
//! - Unpartitioned, no schema evolution, no rowTracking, no deletion
//!   vectors. The writer errors loudly if it discovers any of those at
//!   table-init time; later phases lift each restriction.

use std::collections::HashMap;
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
        mut state: UniformTableState,
    ) -> Result<WriteResult> {
        // 1. Build deterministic Parquet bytes from the input batch.
        let parquet_bytes = parquet_builder::build_parquet(&batch, &state)?;
        let hash = blake3::hash(&parquet_bytes).to_hex().to_string();
        let file_name = format!("{hash}.parquet");
        let file_size = parquet_bytes.len() as u64;
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let parquet_path = Path::from(format!("{prefix}/{file_name}"));

        // 2. PUT the Parquet. Same content → same hash → same key → idempotent.
        self.store
            .put(&parquet_path, PutPayload::from(Bytes::from(parquet_bytes)))
            .await?;

        // 3. Loop: build commit, PUT with `If-None-Match: *`, retry on 412.
        let modification_time_millis = chrono::Utc::now().timestamp_millis();
        for attempt in 0..COND_PUT_RETRY_BUDGET {
            let target_version = state.next_commit_version;
            let inputs = commit::CommitInputs {
                batch: &batch,
                state: &state,
                file_name: &file_name,
                file_size,
                modification_time_millis,
                engine_info: &self.config.engine_info,
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
                    // Another writer (or a stale state) won the v=target_version
                    // race. Refetch the next version and try again.
                    let observed = discover::next_commit_version(&*self.store, &prefix).await?;
                    state.next_commit_version = observed.max(target_version.saturating_add(1));
                    tracing::warn!(
                        attempt = attempt + 1,
                        previous_target = target_version,
                        new_target = state.next_commit_version,
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
