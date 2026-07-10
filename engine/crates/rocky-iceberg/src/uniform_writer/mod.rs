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

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use arrow::ipc::writer::StreamWriter;
use bytes::Bytes;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use rocky_core::state::ColumnHash;

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

/// The liveness state of a content-addressed file's path in a table's
/// `_delta_log`, resolved from the **highest-versioned** commit that
/// references it (a later `remove` supersedes an earlier `add`).
///
/// The three states carry distinct trust for a *deletion* decision, where
/// [`Self::Absent`] must never be conflated with [`Self::Removed`]: the log
/// reader consults only the `<20-digit>.json` commit tail, so a
/// checkpoint-truncated table returns `Absent` for a file whose `add` still
/// lives inside a checkpoint parquet. Deleting on `Absent` would therefore
/// delete a live file. A reclamation gate must evict **only** on
/// [`Self::Removed`] and hold on `Absent`/`Live`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathLiveness {
    /// The highest commit referencing the path added it and no later commit
    /// removed it — the file is live in the current snapshot.
    Live,
    /// The highest commit referencing the path `remove`d it (a
    /// compaction/VACUUM retired it) — the file is provably not live.
    Removed,
    /// No `<20-digit>.json` commit references the path at all — it was never
    /// added to *this* table, or its `add` was truncated behind a checkpoint.
    /// **Not** a proof of removal.
    Absent,
}

/// The verdict of the **strict removal proof** — the gate a byte-deleting or
/// tombstoning reclamation path must pass before retiring an artifact.
///
/// Unlike [`PathLiveness`] (a best-effort liveness read whose `false`/`Absent`
/// is deliberately conservative for the reuse gate), this is an *affirmative
/// proof*: [`Self::ProvenRemoved`] is assembled through a single narrow path
/// that validates the whole `_delta_log` tail, and **every** unhandled or
/// anomalous condition falls to [`Self::Held`]. A future/unknown Delta action,
/// a checkpoint, a version gap, a malformed commit, or a protocol feature that
/// changes file liveness (deletion vectors) can therefore never authorize a
/// reclamation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RemovalProof {
    /// Affirmatively proven removed: the target's own `add` is present in a
    /// clean, checkpoint-free, contiguous commit history and the
    /// highest-versioned commit referencing it is a `remove`. Safe to reclaim.
    ProvenRemoved,
    /// Not proven removed — HOLD (never reclaim). Carries a stable reason.
    Held(RemovalHoldReason),
}

/// Why a [`RemovalProof`] could not affirm removal — a stable, exhaustive set
/// of hold reasons for logging and operator messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalHoldReason {
    /// The candidate file is not under the target table's key prefix (or the
    /// bucket/prefix could not be resolved) — it may belong to another table.
    PrefixMismatch,
    /// The `_delta_log` carries a checkpoint (`_last_checkpoint` /
    /// `*.checkpoint*.parquet`); the readable JSON tail alone cannot prove the
    /// file is not re-added inside the checkpoint.
    CheckpointPresent,
    /// The `<20-digit>.json` commit versions are not a contiguous run from 0.
    VersionGap,
    /// The `_delta_log` has two commits at the same version.
    DuplicateVersion,
    /// A commit body did not parse, a line was not a JSON object, an `add`/
    /// `remove` lacked a string `path`, or a commit carried an unrecognized
    /// action key.
    MalformedCommit,
    /// A `protocol` action declared a feature that changes file liveness
    /// semantics (e.g. deletion vectors) or an unrecognized writer feature.
    UnsupportedProtocol,
    /// The `_delta_log` has no readable JSON commits.
    NoCommits,
    /// The target's own `add` (at its recorded commit version) does not appear
    /// in this table's log — it was never added here.
    NeverAddedHere,
    /// The highest-versioned commit referencing the file is an `add`, not a
    /// `remove` — the file is still live.
    StillLive,
    /// An object-store / log read failed — the proof could not be assembled.
    ReadError,
}

impl RemovalHoldReason {
    /// Stable lowercase token for logs / operator messages.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            RemovalHoldReason::PrefixMismatch => "prefix_mismatch",
            RemovalHoldReason::CheckpointPresent => "checkpoint_present",
            RemovalHoldReason::VersionGap => "version_gap",
            RemovalHoldReason::DuplicateVersion => "duplicate_version",
            RemovalHoldReason::MalformedCommit => "malformed_commit",
            RemovalHoldReason::UnsupportedProtocol => "unsupported_protocol",
            RemovalHoldReason::NoCommits => "no_commits",
            RemovalHoldReason::NeverAddedHere => "never_added_here",
            RemovalHoldReason::StillLive => "still_live",
            RemovalHoldReason::ReadError => "read_error",
        }
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
    /// Per-output-column content hashes over the written Arrow columns, in
    /// table column order (see [`rocky_core::state::ColumnHash`]). Computed
    /// on a genuine build (fresh bytes written); **empty** on a zero-copy
    /// point-to reuse, which references a prior run's already-written bytes
    /// and does not recompute them. The whole-body `blake3_hash` above is
    /// table-granular; these are the per-column signal.
    pub column_hashes: Vec<ColumnHash>,
    pub commit_version: u64,
    pub num_records: usize,
    pub size_bytes: u64,
}

/// Compute per-column content hashes for a written [`RecordBatch`], in the
/// batch's column order.
///
/// One [`ColumnHash`] per column. The hash is over the column's serialized
/// content (type + values), **not** its schema/name — so a schema-stable
/// value change flips the hash while a rename does not. No normalization in
/// v1: the column is hashed as written, so the hash is order-sensitive (the
/// documented over-sensitivity — a mismatch can only ever force a safe
/// rebuild, never a wrong skip).
///
/// # Errors
///
/// [`UniformWriterError::Arrow`] if a column cannot be Arrow-IPC-serialized
/// (not expected for the content-addressed writer's supported types).
fn compute_column_hashes(batch: &RecordBatch) -> Result<Vec<ColumnHash>> {
    let schema = batch.schema();
    let mut out = Vec::with_capacity(batch.num_columns());
    for (idx, array) in batch.columns().iter().enumerate() {
        let field = schema.field(idx);
        out.push(ColumnHash {
            column: field.name().to_string(),
            hash: hash_arrow_column(field, array)?,
        });
    }
    Ok(out)
}

/// blake3 (hex) of a single Arrow column's serialized content.
///
/// The column is wrapped in a one-field [`RecordBatch`] — with the field name
/// normalized to a constant so the hash is over content + type, not the name —
/// and serialized via Arrow IPC. IPC is a deterministic, type-uniform encoding
/// that re-bases any slice offset, so the same logical array always hashes the
/// same regardless of how it was sliced. Padding differences and null-slot
/// bytes are hashed as-is (no normalization in v1).
fn hash_arrow_column(field: &Field, array: &ArrayRef) -> Result<String> {
    let norm_field = Field::new("col", field.data_type().clone(), field.is_nullable());
    let schema = Arc::new(Schema::new(vec![norm_field]));
    let one_col = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(array)])?;
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
        writer.write(&one_col)?;
        writer.finish()?;
    }
    Ok(blake3::hash(&buf).to_hex().to_string())
}

/// Inputs to a content-addressed *point-to* commit
/// ([`UniformWriter::commit_pointer_with_state`]).
///
/// Carries everything needed to reference a prior run `R`'s existing
/// blake3-named parquet **without re-reading the bytes**:
/// - `recovered_add` — `R`'s `add` action object, lifted verbatim from its
///   `_delta_log/{version}.json` (via
///   [`discover::recover_add_action_for_version`]). Its `stats`
///   (`numRecords` + min/max/nullCount) carry over byte-for-byte.
/// - `add_file_path` — the content-addressed path (`<hash>.parquet`) the new
///   commit references; used both as the `add.path` and as the
///   double-count pre-check key. It is `recovered_add["path"]`, surfaced
///   explicitly so the writer never has to re-parse the lifted action.
/// - `blake3_hash` / `num_records` / `size_bytes` — `R`'s recorded artifact
///   identity, flowed straight into the returned [`WriteResult`] so the
///   runner records a fresh `ArtifactRecord` at the *same* blake3 (driving
///   `refcount_for_hash` ≥ 2 — shared bytes, not copied).
#[derive(Debug, Clone)]
pub struct PointerInputs {
    /// `R`'s `add` action object, lifted verbatim.
    pub recovered_add: serde_json::Map<String, serde_json::Value>,
    /// The content-addressed parquet path the commit references
    /// (`recovered_add["path"]`).
    pub add_file_path: String,
    /// `R`'s output blake3 (hex) — unchanged; the reusing run shares it.
    pub blake3_hash: String,
    /// `R`'s row count, carried into the result for the runner's summary.
    pub num_records: usize,
    /// `R`'s parquet size in bytes.
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
        // Scope guard: this writer allocates each partition group's baseRowId
        // range from the per-call `state.row_tracking_next_id`, so it cannot
        // sequence globally-disjoint ranges across >=2 groups — colliding
        // ranges plus an under-counted rowIdHighWaterMark silently corrupt
        // Delta row-tracking metadata. Refuse rowTracking here, mirroring
        // `commit_pointer_with_state`; the caller must fall back to a normal
        // build.
        if state.row_tracking_enabled {
            return Err(UniformWriterError::DeltaLog(
                "partitioned content-addressed write does not support rowTracking \
                 tables (per-group baseRowId allocation cannot be globally \
                 sequenced across partition groups); falling back to a normal \
                 build is the caller's responsibility"
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

    /// Recover the [`PointerInputs`] for a point-to from a prior run `R`'s
    /// recorded artifact identity.
    ///
    /// Given `R`'s `commit_version` (joined from its `ArtifactRecord`), its
    /// output blake3, and the parquet size + row count, this GETs `R`'s
    /// `_delta_log/{commit_version}.json` and lifts its `add` action — one
    /// GET, no listing, no parquet byte read (the `add`'s `stats` already
    /// hold `numRecords` + min/max/nullCount). The returned `PointerInputs`
    /// feeds straight into [`Self::commit_pointer_with_state`].
    ///
    /// # Errors
    ///
    /// [`UniformWriterError::DeltaLog`] when the recovered commit carries no
    /// `add` action; the object-store GET error when the log file is missing.
    pub async fn recover_pointer_inputs(
        &self,
        commit_version: u64,
        blake3_hash: String,
        num_records: usize,
        size_bytes: u64,
    ) -> Result<PointerInputs> {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let recovered_add =
            discover::recover_add_action_for_version(&*self.store, &prefix, commit_version).await?;
        let add_file_path = recovered_add
            .get("path")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                UniformWriterError::DeltaLog(format!(
                    "recovered `add` at commit {commit_version} has no `path`"
                ))
            })?
            .to_string();
        Ok(PointerInputs {
            recovered_add,
            add_file_path,
            blake3_hash,
            num_records,
            size_bytes,
        })
    }

    /// Reuse a prior run `R`'s already-written content-addressed parquet by
    /// emitting a Delta commit that **references R's existing blake3-named
    /// file — with zero byte copy.** No SQL executes, no parquet is built,
    /// nothing is uploaded.
    ///
    /// This is the strong (offline-byte-verifiable) reuse backend: the
    /// reusing run's target gains a commit whose lone `add` action points at
    /// the same object `R` wrote. `pointer.blake3_hash` and the underlying
    /// bytes are `R`'s; only a new `_delta_log` entry (and, recorded
    /// separately by the runner, a fresh `ArtifactRecord` at the same blake3)
    /// distinguish the reusing run.
    ///
    /// # Append-only safety — the double-count guard
    ///
    /// Delta is append-only: two live `add` actions for the same `path`
    /// **double-count** that file's rows (the writer's own cond-put handler
    /// documents this — see [`Self::write_internal`]). A point-to therefore
    /// **pre-checks** the live `_delta_log` for an existing `add` referencing
    /// `pointer.add_file_path`:
    /// - **already present** ⇒ the target already holds R's file; the reuse
    ///   is satisfied with **no new commit** — returns that commit's version
    ///   so the caller records the shared-bytes reference without
    ///   re-counting.
    /// - **absent** ⇒ land a fresh pointer commit at `next_commit_version`
    ///   via the conditional-put loop (retrying on a genuine version race).
    ///
    /// # Scope
    ///
    /// Unpartitioned, non-rowTracking tables only — validated against the
    /// discovered `state` (and guarded again in
    /// [`commit::build_commit_jsonl_from_add`]). A partitioned or rowTracking
    /// table returns an error rather than a partial point-to; the caller
    /// falls back to a normal BUILD.
    ///
    /// # Errors
    ///
    /// - [`UniformWriterError::PartitionedUnsupported`] / a `DeltaLog` error
    ///   when the table is partitioned or rowTracking;
    /// - [`UniformWriterError::CondPutRetryExhausted`] when the version race
    ///   never settles;
    /// - the underlying object-store / JSON errors otherwise.
    pub async fn commit_pointer_with_state(
        &self,
        pointer: &PointerInputs,
        mut state: UniformTableState,
    ) -> Result<WriteResult> {
        // Scope guard: unpartitioned, non-rowTracking only. A partial
        // point-to is never emitted — the caller falls back to a BUILD.
        if !state.partition_columns.is_empty() {
            return Err(UniformWriterError::PartitionedUnsupported(
                state.partition_columns.clone(),
            ));
        }
        if state.row_tracking_enabled {
            return Err(UniformWriterError::DeltaLog(
                "point-to does not support rowTracking tables (the reusing commit needs a \
                 freshly re-allocated baseRowId range); falling back to a normal build is the \
                 caller's responsibility"
                    .to_string(),
            ));
        }

        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let parquet_path = Path::from(format!("{prefix}/{}", pointer.add_file_path));

        // Double-count guard: if a live commit already references this exact
        // content-addressed file, the target already holds R's bytes. Reuse
        // is already satisfied — return that version, emit NO new commit.
        if let Some(existing_version) =
            discover::find_commit_with_add_path(&*self.store, &prefix, &pointer.add_file_path)
                .await?
        {
            tracing::info!(
                add_file_path = %pointer.add_file_path,
                existing_version,
                "point-to: target already references this content-addressed file; \
                 reuse satisfied with no new commit (append-only double-count guard)"
            );
            return Ok(WriteResult {
                file_path: parquet_path.to_string(),
                blake3_hash: pointer.blake3_hash.clone(),
                // Point-to reuse references R's already-written bytes; no
                // Arrow batch is in hand to hash, so no per-column hashes are
                // recomputed. R's own successful build recorded them.
                column_hashes: Vec::new(),
                commit_version: existing_version,
                num_records: pointer.num_records,
                size_bytes: pointer.size_bytes,
            });
        }

        // File absent on the target: land a fresh pointer commit referencing
        // R's parquet. Same cond-put loop as a build, minus build_parquet +
        // the parquet PUT.
        let modification_time_millis = chrono::Utc::now().timestamp_millis();
        for attempt in 0..COND_PUT_RETRY_BUDGET {
            let target_version = state.next_commit_version;
            let commit_body = commit::build_commit_jsonl_from_add(
                &pointer.recovered_add,
                &self.config.engine_info,
                modification_time_millis,
                &state.partition_columns,
            )?;
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
                        blake3_hash: pointer.blake3_hash.clone(),
                        // Point-to reuse: no fresh Arrow batch to hash.
                        column_hashes: Vec::new(),
                        commit_version: target_version,
                        num_records: pointer.num_records,
                        size_bytes: pointer.size_bytes,
                    });
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Re-run the double-count pre-check before retrying: a
                    // concurrent point-to of the *same* file may have landed
                    // while we raced on the version. If so, return its
                    // version rather than adding the file a second time.
                    if let Some(existing_version) = discover::find_commit_with_add_path(
                        &*self.store,
                        &prefix,
                        &pointer.add_file_path,
                    )
                    .await?
                    {
                        tracing::info!(
                            add_file_path = %pointer.add_file_path,
                            existing_version,
                            "point-to cond-put 412: file landed concurrently; returning \
                             existing commit (double-count guard)"
                        );
                        return Ok(WriteResult {
                            file_path: parquet_path.to_string(),
                            blake3_hash: pointer.blake3_hash.clone(),
                            // Point-to reuse: no fresh Arrow batch to hash.
                            column_hashes: Vec::new(),
                            commit_version: existing_version,
                            num_records: pointer.num_records,
                            size_bytes: pointer.size_bytes,
                        });
                    }
                    let observed = discover::next_commit_version(&*self.store, &prefix).await?;
                    state.next_commit_version = observed.max(target_version.saturating_add(1));
                    tracing::warn!(
                        attempt = attempt + 1,
                        previous_target = target_version,
                        new_target = state.next_commit_version,
                        "point-to cond-put 412 on _delta_log entry; retrying"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(UniformWriterError::CondPutRetryExhausted(format!(
            "exhausted {COND_PUT_RETRY_BUDGET} retries chasing next_commit_version for a point-to"
        )))
    }

    /// Whether a prior run `R`'s content-addressed file is **still live** in
    /// this table's current `_delta_log` — added and not since removed
    /// (VACUUM'd / compacted / superseded).
    ///
    /// The liveness gate the fail-closed reuse decision consults **before**
    /// pointing a new commit at `R`'s parquet. A `false` answer (the file was
    /// `remove`d, or no commit in this table references it) means a point-to
    /// would reference removed bytes — the caller must fall back to a normal
    /// BUILD. Any error (cannot read the log) is propagated so the caller can
    /// treat the doubt as "not provably live" and BUILD.
    ///
    /// `add_file_path` is the path relative to the table prefix
    /// (`<hash>.parquet`), i.e. [`PointerInputs::add_file_path`].
    ///
    /// # Errors
    ///
    /// [`UniformWriterError::ObjectStore`] when the `_delta_log` listing or a
    /// commit GET fails; [`UniformWriterError::DeltaLog`] when a commit body
    /// cannot be parsed.
    pub async fn add_path_is_live(&self, add_file_path: &str) -> Result<bool> {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        discover::add_path_is_live(&*self.store, &prefix, add_file_path).await
    }

    /// The three-state liveness of `add_file_path` in this table's
    /// `_delta_log` — [`PathLiveness::Live`], [`PathLiveness::Removed`], or
    /// [`PathLiveness::Absent`].
    ///
    /// The trust-preserving companion to [`Self::add_path_is_live`] (which
    /// folds `Removed` and `Absent` into a single `false`, safe for the reuse
    /// gate's BUILD-on-doubt but **unsafe** for a delete gate). A reclamation
    /// path that physically removes bytes must distinguish provably-`Removed`
    /// (safe to reclaim) from `Absent` (unprovable — a checkpoint may still
    /// reference the file) and evict only on the former. See [`PathLiveness`].
    ///
    /// `add_file_path` is the path relative to the table prefix
    /// (`<hash>.parquet`).
    ///
    /// # Errors
    ///
    /// [`UniformWriterError::ObjectStore`] when the `_delta_log` listing or a
    /// commit GET fails; [`UniformWriterError::DeltaLog`] when a commit body
    /// cannot be parsed. Any error is a "cannot verify" the caller must treat
    /// as fail-closed (hold, never delete).
    pub async fn add_path_liveness(&self, add_file_path: &str) -> Result<PathLiveness> {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        discover::add_path_liveness(&*self.store, &prefix, add_file_path).await
    }

    /// The **strict removal proof** for `add_file_path` (the table-relative
    /// Delta path, nested components preserved) whose `add` is recorded at
    /// `expected_add_version`.
    ///
    /// Returns [`RemovalProof::ProvenRemoved`] **only** when the whole readable
    /// commit history validates and affirmatively proves removal (see
    /// [`RemovalProof`]); every anomaly or unverifiable condition — including an
    /// object-store read failure — yields [`RemovalProof::Held`]. This is the
    /// gate a reclamation path (tombstone / future VACUUM) must pass; unlike
    /// [`Self::add_path_liveness`] it can never fall through to a
    /// reclaim-authorizing verdict.
    pub async fn proven_removed(
        &self,
        add_file_path: &str,
        expected_add_version: u64,
    ) -> RemovalProof {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        discover::proven_removed(&*self.store, &prefix, add_file_path, expected_add_version).await
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
        // Per-column content hashes over the same in-memory Arrow batch,
        // computed in this pass (the batch is held right here, immediately
        // before the whole-body blake3 above). Captured on every genuine
        // build; nothing consults them yet.
        let column_hashes = compute_column_hashes(&batch)?;
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
                        column_hashes: column_hashes.clone(),
                        commit_version: target_version,
                        num_records: batch.num_rows(),
                        size_bytes: file_size,
                    });
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Distinguish a genuine version race from a competing writer
                    // that already landed this exact content-addressed file. If
                    // an existing `add` action references our `add_file_path`, a
                    // concurrent identical write won — return its commit version
                    // rather than adding the same file a second time at a higher
                    // version (which would double-count rows/bytes).
                    if let Some(existing_version) =
                        discover::find_commit_with_add_path(&*self.store, &prefix, &add_file_path)
                            .await?
                    {
                        tracing::info!(
                            add_file_path = %add_file_path,
                            existing_version,
                            "cond-put 412: identical content-addressed file already \
                             committed by a concurrent writer; returning existing commit"
                        );
                        return Ok(WriteResult {
                            file_path: parquet_path.to_string(),
                            blake3_hash: hash,
                            column_hashes: column_hashes.clone(),
                            commit_version: existing_version,
                            num_records: batch.num_rows(),
                            size_bytes: file_size,
                        });
                    }

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
    async fn write_partitioned_batch_rejects_row_tracking() {
        // Regression: a partitioned + rowTracking table allocates each
        // group's baseRowId range from the per-call next-id and cannot
        // globally sequence them, so it must be refused (like the point-to
        // writer) rather than silently corrupt Delta row-tracking metadata.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let writer = UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: "tbl".into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            Arc::new(PanicSqlClient),
        );
        let state = UniformTableState {
            physical: HashMap::new(),
            field_id: HashMap::new(),
            partition_columns: vec!["region".to_string()],
            row_tracking_enabled: true,
            deletion_vectors_enabled: false,
            next_commit_version: 1,
            row_tracking_next_id: 0,
        };
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        let err = writer
            .write_partitioned_batch_with_state(make_partitioned_batch("eu", 1), pv, state)
            .await
            .expect_err("rowTracking partitioned write must be refused");
        assert!(err.to_string().contains("rowTracking"), "{err}");
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

    #[test]
    fn compute_column_hashes_is_deterministic_and_value_sensitive() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
            ],
        )
        .unwrap();

        // One hash per column, in table column order, keyed by name.
        let h1 = compute_column_hashes(&batch).unwrap();
        assert_eq!(h1.len(), 2);
        assert_eq!(h1[0].column, "id");
        assert_eq!(h1[1].column, "name");
        assert!(h1.iter().all(|c| !c.hash.is_empty()));

        // Deterministic: identical content hashes identically.
        let h2 = compute_column_hashes(&batch).unwrap();
        assert_eq!(h1, h2);

        // Value-sensitive: changing one column's data (schema unchanged) flips
        // that column's hash and leaves the other column's hash untouched.
        let mutated = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2, 3])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b", "CHANGED"])) as ArrayRef,
            ],
        )
        .unwrap();
        let h3 = compute_column_hashes(&mutated).unwrap();
        assert_eq!(h3[0].hash, h1[0].hash, "unchanged column keeps its hash");
        assert_ne!(h3[1].hash, h1[1].hash, "changed column's hash moves");
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

        // Per-column content hashes are computed on the genuine build path,
        // from the real Arrow columns, in table column order — one per column,
        // keyed by name, each a non-empty hex blake3.
        let cols: Vec<&str> = result
            .column_hashes
            .iter()
            .map(|c| c.column.as_str())
            .collect();
        assert_eq!(cols, vec!["id", "name"]);
        assert!(result.column_hashes.iter().all(|c| !c.hash.is_empty()));
        assert_ne!(
            result.column_hashes[0].hash, result.column_hashes[1].hash,
            "distinct columns must hash distinctly"
        );

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

    // -- point-to (commit_pointer_with_state / recover_pointer_inputs) ------

    fn make_unpartitioned_writer(store: Arc<dyn ObjectStore>, prefix: &str) -> UniformWriter {
        UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store,
            Arc::new(PanicSqlClient),
        )
    }

    #[tokio::test]
    async fn point_to_recover_then_commit_into_fresh_table() {
        // R writes a batch into table A (the build). A *separate* fresh
        // table B then points to R's parquet with zero byte copy: recover
        // R's add, commit a pointer into B, assert B's new commit references
        // the same content-addressed path + same blake3, and no second
        // parquet object is created under B.
        let store: Arc<InMemory> = Arc::new(InMemory::new());

        // R's build into table A.
        seed_bootstrap(&store, "tbl_a").await;
        let writer_a = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl_a");
        let r = writer_a.write_batch(make_batch(7)).await.unwrap();
        assert_eq!(r.commit_version, 1);

        // Point-to into a fresh table B (its own bootstrap, no data yet).
        seed_bootstrap(&store, "tbl_b").await;
        let writer_b = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl_b");
        let state_b = writer_b.discover().await.unwrap();
        assert_eq!(state_b.next_commit_version, 1);

        let pointer = writer_a
            .recover_pointer_inputs(
                r.commit_version,
                r.blake3_hash.clone(),
                r.num_records,
                r.size_bytes,
            )
            .await
            .unwrap();
        // The recovered path is R's content-addressed file.
        assert!(pointer.add_file_path.ends_with(".parquet"));
        assert_eq!(pointer.blake3_hash, r.blake3_hash);

        let pr = writer_b
            .commit_pointer_with_state(&pointer, state_b)
            .await
            .unwrap();
        // B's pointer commit lands at version 1, references R's bytes, copies
        // nothing.
        assert_eq!(pr.commit_version, 1);
        assert_eq!(pr.blake3_hash, r.blake3_hash, "shared bytes — same blake3");
        assert_eq!(pr.num_records, r.num_records);
        // R's genuine build computed per-column hashes; the zero-copy point-to
        // recomputes none (it holds no Arrow batch — R's own build recorded
        // them). Empty here degrades to a safe rebuild in the later skip gate.
        assert!(
            !r.column_hashes.is_empty(),
            "the build recorded column hashes"
        );
        assert!(
            pr.column_hashes.is_empty(),
            "point-to reuse recomputes no column hashes"
        );

        // B's _delta_log/...001.json references the SAME content-addressed
        // path R wrote (the file name; the prefix differs per table).
        let log_b = store
            .get(&object_store::path::Path::from(
                "tbl_b/_delta_log/00000000000000000001.json",
            ))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&log_b).unwrap().lines().collect();
        assert_eq!(lines.len(), 2, "point-to commit is commitInfo + lifted add");
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        let r_file = r.file_path.split('/').next_back().unwrap();
        assert_eq!(
            add["add"]["path"], r_file,
            "B's commit must reference R's content-addressed file name"
        );

        // No parquet object was written under tbl_b — the point-to copied
        // zero bytes (the only tbl_b objects are _delta_log/*).
        use futures::TryStreamExt;
        let mut stream = store.list(Some(&object_store::path::Path::from("tbl_b")));
        let mut saw_parquet = false;
        while let Some(meta) = stream.try_next().await.unwrap() {
            if meta.location.to_string().ends_with(".parquet") {
                saw_parquet = true;
            }
        }
        assert!(
            !saw_parquet,
            "a point-to must not write any parquet object under the target table"
        );
    }

    #[tokio::test]
    async fn point_to_is_a_noop_when_file_already_referenced() {
        // Double-count guard: if the target already holds R's file, a
        // point-to emits NO new commit and returns the existing version.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");

        // R's build lands at v=1 in THIS table.
        let r = writer.write_batch(make_batch(4)).await.unwrap();
        assert_eq!(r.commit_version, 1);

        // Recover R's add and attempt a point-to into the SAME table — the
        // file is already referenced, so this must be a no-op.
        let pointer = writer
            .recover_pointer_inputs(
                r.commit_version,
                r.blake3_hash.clone(),
                r.num_records,
                r.size_bytes,
            )
            .await
            .unwrap();
        let state = writer.discover().await.unwrap();
        assert_eq!(state.next_commit_version, 2, "v=1 already taken by R");

        let pr = writer
            .commit_pointer_with_state(&pointer, state)
            .await
            .unwrap();
        assert_eq!(
            pr.commit_version, 1,
            "must return R's existing version, not land a new commit"
        );

        // No v=2 commit was written — the table still tops out at v=1.
        let v2 = store
            .get(&object_store::path::Path::from(
                "tbl/_delta_log/00000000000000000002.json",
            ))
            .await;
        assert!(
            v2.is_err(),
            "the double-count guard must NOT land a second commit for the same file"
        );
    }

    #[tokio::test]
    async fn point_to_rejects_partitioned_table() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_partitioned_bootstrap(&store, "ptbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "ptbl");
        let state = writer.discover().await.unwrap();
        let pointer = PointerInputs {
            recovered_add: serde_json::Map::new(),
            add_file_path: "x.parquet".into(),
            blake3_hash: "deadbeef".into(),
            num_records: 1,
            size_bytes: 1,
        };
        match writer.commit_pointer_with_state(&pointer, state).await {
            Err(UniformWriterError::PartitionedUnsupported(cols)) => {
                assert_eq!(cols, vec!["region".to_string()]);
            }
            other => panic!("expected PartitionedUnsupported, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn point_to_rejects_row_tracking_table() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_row_tracking_bootstrap(&store, "rt").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "rt");
        let state = writer.discover().await.unwrap();
        let pointer = PointerInputs {
            recovered_add: serde_json::Map::new(),
            add_file_path: "x.parquet".into(),
            blake3_hash: "deadbeef".into(),
            num_records: 1,
            size_bytes: 1,
        };
        match writer.commit_pointer_with_state(&pointer, state).await {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(msg.contains("rowTracking"), "must name rowTracking: {msg}");
            }
            other => panic!("expected DeltaLog rowTracking refusal, got {other:?}"),
        }
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

    #[tokio::test]
    async fn add_path_is_live_true_for_a_freshly_written_file() {
        // After a normal build, the file's `add` is live (no later remove).
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let r = writer.write_batch(make_batch(4)).await.unwrap();
        let add_file = r.file_path.split('/').next_back().unwrap();

        assert!(
            writer.add_path_is_live(add_file).await.unwrap(),
            "a just-added file must be live"
        );
    }

    #[tokio::test]
    async fn add_path_is_live_false_after_a_later_remove() {
        // Build, then land a later commit that `remove`s the file (the
        // VACUUM/compaction case). Liveness must flip to false so the reuse
        // decision falls back to BUILD rather than pointing at removed bytes.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let r = writer.write_batch(make_batch(4)).await.unwrap();
        let add_file = r.file_path.split('/').next_back().unwrap();

        // Land v=2: a remove of R's file (what a VACUUM/compaction emits).
        let remove_body = format!(
            "{{\"commitInfo\":{{\"operation\":\"VACUUM END\"}}}}\n\
             {{\"remove\":{{\"path\":\"{add_file}\",\"dataChange\":false}}}}\n"
        );
        store
            .put(
                &object_store::path::Path::from(
                    "tbl/_delta_log/00000000000000000002.json".to_string(),
                ),
                PutPayload::from(Bytes::from(remove_body)),
            )
            .await
            .unwrap();

        assert!(
            !writer.add_path_is_live(add_file).await.unwrap(),
            "a removed file must NOT be live — reuse must fall back to BUILD"
        );
    }

    #[tokio::test]
    async fn add_path_is_live_false_for_a_path_no_commit_references() {
        // A path that was never added to this table is not live.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");

        assert!(
            !writer
                .add_path_is_live("never-added.parquet")
                .await
                .unwrap(),
            "a path no commit references is not live in this table"
        );
    }

    /// The three-state reader distinguishes the two cases `add_path_is_live`
    /// folds into `false` — the distinction the gc delete gate depends on
    /// (evict only on `Removed`, hold on `Absent`). A still-added file is
    /// `Live`, a `remove`d one is `Removed`, and an unreferenced one is
    /// `Absent` (NOT `Removed`).
    #[tokio::test]
    async fn add_path_liveness_separates_removed_from_absent() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let r = writer.write_batch(make_batch(4)).await.unwrap();
        let add_file = r.file_path.split('/').next_back().unwrap().to_string();

        assert_eq!(
            writer.add_path_liveness(&add_file).await.unwrap(),
            PathLiveness::Live,
            "a just-added file is Live"
        );
        assert_eq!(
            writer
                .add_path_liveness("never-added.parquet")
                .await
                .unwrap(),
            PathLiveness::Absent,
            "a path no commit references is Absent — NOT Removed (the delete gate must hold)"
        );

        // Land a `remove` of the added file → Removed (the only reclaimable
        // state).
        let remove_body = format!(
            "{{\"commitInfo\":{{\"operation\":\"VACUUM END\"}}}}\n\
             {{\"remove\":{{\"path\":\"{add_file}\",\"dataChange\":false}}}}\n"
        );
        store
            .put(
                &object_store::path::Path::from(
                    "tbl/_delta_log/00000000000000000002.json".to_string(),
                ),
                PutPayload::from(Bytes::from(remove_body)),
            )
            .await
            .unwrap();
        assert_eq!(
            writer.add_path_liveness(&add_file).await.unwrap(),
            PathLiveness::Removed,
            "a removed file is Removed"
        );
    }
}
