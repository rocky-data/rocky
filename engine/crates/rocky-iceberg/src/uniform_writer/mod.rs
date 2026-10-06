//! Content-addressed writer for Delta UniForm tables.
//!
//! Reads a Delta UniForm table's `_delta_log` state, writes deterministic
//! Parquet files keyed by their blake3 hash, and emits Delta commit JSONL
//! that references those files. After each commit, callers must trigger
//! `MSCK REPAIR TABLE ... SYNC METADATA` via the warehouse SQL surface so
//! UniForm regenerates the corresponding Iceberg metadata.
//!
//! Every write **replaces** the table (RV1-D8, #2269). One commit removes
//! each live file that is not in the new output and adds each new file that
//! is not live yet. An unchanged output writes no commit.
//!
//! ```text
//!   run 1 ─▶ v1: add A                live = {A}
//!   run 2 ─▶ v2: remove A, add B      live = {B}
//!   run 3 (same output as run 2) ─▶ no commit, live = {B}
//! ```
//!
//! Scope today:
//! - Single writer; a conditional put on the log entry covers contention.
//!   On a 412 the writer re-reads the live set and recomputes the removes.
//! - External Delta UniForm table on an object store the writer can `PUT`.
//! - Unpartitioned tables via [`UniformWriter::write_batch`].
//! - Partitioned tables via [`UniformWriter::write_partitioned_batches`]:
//!   the caller pre-groups rows per partition tuple, and all groups land in
//!   one commit. `add.partitionValues` is keyed by physical-name UUID, not
//!   logical name (Exp 11).
//! - Row-tracking-enabled tables (unpartitioned): every `add` action emits
//!   `baseRowId` + `defaultRowCommitVersion`, and each commit appends a
//!   `domainMetadata` action that bumps the `rowIdHighWaterMark` (Exp 9).
//! - The live set comes from the JSON commits only. A table with a Delta
//!   checkpoint refuses the write ([`UniformWriterError::CheckpointPresent`]).
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
    ///
    /// Carries the Delta `head_version` (the highest `_delta_log` JSON commit)
    /// the proof validated against, so the caller can (a) re-verify the head has
    /// not advanced just before it mutates state — the TOCTOU narrowing — and
    /// (b) version-scope the tombstone it writes.
    ProvenRemoved { head_version: u64 },
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
    /// A commit body did not parse, a line was not a single-key JSON object, an
    /// `add`/`remove` lacked a string `path` (or its action was not an object),
    /// or a commit carried an unrecognized action key.
    MalformedCommit,
    /// An `add`/`remove` path could not be resolved to a canonical `(bucket,
    /// key)` identity in this table (a foreign scheme, a cross-bucket absolute
    /// URI, or a dot-segment escaping the table root) — so relative-vs-absolute
    /// aliases of the target could not be compared safely.
    UncanonicalizablePath,
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
            RemovalHoldReason::UncanonicalizablePath => "uncanonicalizable_path",
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
    /// The commit version of this file's live `add`: the replace commit when
    /// this call added the file, or the earlier commit that added it when
    /// the file was already live (unchanged content).
    pub commit_version: u64,
    /// The table version whose snapshot is exactly this call's output: the
    /// replace commit, or the current head on a no-op.
    pub table_version: u64,
    /// `true` when this call wrote a commit; `false` on a no-op (the output
    /// was already the live set).
    pub committed: bool,
    pub num_records: usize,
    pub size_bytes: u64,
}

/// Result of one replace: the table's live set now equals the files written
/// by the call.
///
/// ```text
///   live before ─▶ remove (live − new) ; add (new − live) ─▶ live after == new
/// ```
#[derive(Debug, Clone)]
pub struct ReplaceOutcome {
    /// The table version whose snapshot is exactly this output: the new
    /// commit, or the current head when nothing changed.
    pub table_version: u64,
    /// `true` when a commit was written; `false` on a no-op.
    pub committed: bool,
    /// The `add.path` of every live file the commit removed.
    pub removed_paths: Vec<String>,
    /// One entry per output file, in the order the caller passed the groups.
    pub files: Vec<WriteResult>,
}

impl ReplaceOutcome {
    /// The single file of an unpartitioned (or single-group) replace.
    fn into_single(self) -> Result<WriteResult> {
        let n = self.files.len();
        let mut files = self.files;
        match (files.pop(), n) {
            (Some(file), 1) => Ok(file),
            _ => Err(UniformWriterError::DeltaLog(format!(
                "expected exactly one output file, got {n}"
            ))),
        }
    }
}

/// One output file, uploaded (or lifted, on a point-to) and ready to be
/// added by a replace commit.
#[derive(Debug, Clone)]
struct StagedFile {
    /// `add.path`, relative to the table prefix.
    path: String,
    /// The `add` body, without row-tracking fields.
    add: serde_json::Map<String, serde_json::Value>,
    blake3_hash: String,
    column_hashes: Vec<ColumnHash>,
    num_records: usize,
    size_bytes: u64,
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

    /// Replace the table's content with one Arrow [`RecordBatch`], written as
    /// a content-addressed Parquet file plus one replace commit.
    ///
    /// **Unpartitioned tables only.** For partitioned tables, use
    /// [`UniformWriter::write_partitioned_batches`]. Mixing this entry point
    /// with a partitioned target returns
    /// [`UniformWriterError::PartitionedUnsupported`].
    ///
    /// Calls [`UniformWriter::discover`] first to read the current table
    /// state. To skip the discover round-trip, use
    /// [`UniformWriter::write_batch_with_state`].
    pub async fn write_batch(&self, batch: RecordBatch) -> Result<WriteResult> {
        let state = self.discover().await?;
        self.write_batch_with_state(batch, state).await
    }

    /// [`Self::write_batch`] against a [`UniformTableState`] the caller
    /// already obtained.
    ///
    /// The commit removes every live file and adds the new one (see
    /// [`ReplaceOutcome`]). When the new file is already the only live file,
    /// no commit is written and the result names the current version.
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
        let outcome = self
            .replace_with_batches(vec![(HashMap::new(), batch)], state)
            .await?;
        outcome.into_single()
    }

    /// Replace a partitioned table's content with one partition group.
    ///
    /// Every row in `batch` belongs to the partition tuple
    /// `partition_values` (keyed by logical column name). This is a
    /// **replace**: every other partition's live files are removed. To write
    /// several groups, use [`Self::write_partitioned_batches`].
    pub async fn write_partitioned_batch(
        &self,
        batch: RecordBatch,
        partition_values: HashMap<String, String>,
    ) -> Result<WriteResult> {
        let state = self.discover().await?;
        self.write_partitioned_batch_with_state(batch, partition_values, state)
            .await
    }

    /// [`Self::write_partitioned_batch`] against a state the caller already
    /// obtained.
    pub async fn write_partitioned_batch_with_state(
        &self,
        batch: RecordBatch,
        partition_values: HashMap<String, String>,
        state: UniformTableState,
    ) -> Result<WriteResult> {
        self.write_partitioned_batches_with_state(vec![(partition_values, batch)], state)
            .await?
            .into_single()
    }

    /// Replace a partitioned table's content with several partition groups,
    /// in **one** commit.
    ///
    /// The caller has pre-grouped rows so that every row of a group's batch
    /// belongs to that group's partition tuple, keyed by logical column name.
    /// Stringification is the caller's job (Delta partition values are
    /// strings on the wire).
    ///
    /// Each Parquet file is uploaded to a Hive-style prefix
    /// (`<col1>=<val1>/<col2>=<val2>/.../<hash>.parquet`); each `add` keys
    /// `partitionValues` by **physical** column UUID (Exp 11 finding).
    ///
    /// Errors:
    /// - the target is unpartitioned → `DeltaLog` (use [`Self::write_batch`])
    /// - the target uses rowTracking → `DeltaLog`
    /// - a group's `partition_values` misses a column or has an unknown key
    ///   → `DeltaLog`
    pub async fn write_partitioned_batches(
        &self,
        groups: Vec<(HashMap<String, String>, RecordBatch)>,
    ) -> Result<ReplaceOutcome> {
        let state = self.discover().await?;
        self.write_partitioned_batches_with_state(groups, state)
            .await
    }

    /// [`Self::write_partitioned_batches`] against a state the caller already
    /// obtained.
    pub async fn write_partitioned_batches_with_state(
        &self,
        groups: Vec<(HashMap<String, String>, RecordBatch)>,
        state: UniformTableState,
    ) -> Result<ReplaceOutcome> {
        if state.partition_columns.is_empty() {
            return Err(UniformWriterError::DeltaLog(
                "write_partitioned_batch called against unpartitioned table; \
                 use write_batch instead"
                    .to_string(),
            ));
        }
        // Scope guard: the rowTracking + partitioned path has no live
        // verification yet, so it stays refused, like the point-to writer.
        // The caller must fall back to a normal build.
        if state.row_tracking_enabled {
            return Err(UniformWriterError::DeltaLog(
                "partitioned content-addressed write does not support rowTracking \
                 tables; falling back to a normal build is the caller's responsibility"
                    .to_string(),
            ));
        }
        let table_partitions: HashSet<&str> =
            state.partition_columns.iter().map(String::as_str).collect();
        for (partition_values, _) in &groups {
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
        }
        self.replace_with_batches(groups, state).await
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
    /// emitting a replace commit that **references R's existing blake3-named
    /// file — with zero byte copy.** No SQL executes, no parquet is built,
    /// nothing is uploaded.
    ///
    /// `pointer.blake3_hash` and the underlying bytes are `R`'s; only a new
    /// `_delta_log` entry (and, recorded separately by the runner, a fresh
    /// `ArtifactRecord` at the same blake3) distinguish the reusing run.
    ///
    /// # Replace semantics
    ///
    /// A point-to is a replace like any build (RV1-D8): after it, the live
    /// set is exactly `{R's file}`.
    /// - R's file is already the only live file ⇒ **no new commit**; the
    ///   result names the version of R's live `add`.
    /// - otherwise ⇒ one commit that removes every other live file and adds
    ///   R's file unless it is live already.
    ///
    /// # Scope
    ///
    /// Unpartitioned, non-rowTracking tables only — validated against the
    /// discovered `state` (and guarded again in
    /// [`commit::lift_add_action`]). A partitioned or rowTracking table
    /// returns an error rather than a partial point-to; the caller falls back
    /// to a normal BUILD.
    ///
    /// # Errors
    ///
    /// - [`UniformWriterError::PartitionedUnsupported`] / a `DeltaLog` error
    ///   when the table is partitioned or rowTracking;
    /// - [`UniformWriterError::CheckpointPresent`] when the log has a
    ///   checkpoint;
    /// - [`UniformWriterError::CondPutRetryExhausted`] when the version race
    ///   never settles;
    /// - the underlying object-store / JSON errors otherwise.
    pub async fn commit_pointer_with_state(
        &self,
        pointer: &PointerInputs,
        state: UniformTableState,
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
        let live = discover::read_live_set(&*self.store, &prefix, &self.config.fqtn()).await?;
        let modification_time_millis = chrono::Utc::now().timestamp_millis();
        let add = commit::lift_add_action(&pointer.recovered_add, modification_time_millis)?;
        let staged = StagedFile {
            path: pointer.add_file_path.clone(),
            add,
            blake3_hash: pointer.blake3_hash.clone(),
            // Point-to reuse references R's already-written bytes; no Arrow
            // batch is in hand to hash. R's own build recorded them.
            column_hashes: Vec::new(),
            num_records: pointer.num_records,
            size_bytes: pointer.size_bytes,
        };
        self.commit_replace(vec![staged], live, state, modification_time_millis)
            .await?
            .into_single()
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
    ///
    /// `table_bucket` is this table's object-store bucket, used to canonicalize
    /// absolute `s3://…` add/remove paths and reject cross-bucket references.
    pub async fn proven_removed(
        &self,
        table_bucket: &str,
        add_file_path: &str,
        expected_add_version: u64,
    ) -> RemovalProof {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        discover::proven_removed(
            &*self.store,
            table_bucket,
            &prefix,
            add_file_path,
            expected_add_version,
        )
        .await
    }

    /// Build and upload one Parquet file per group, then make one replace
    /// commit. Shared by the unpartitioned and partitioned entry points.
    ///
    /// The live set is read **before** any byte is uploaded, so a refusal
    /// (a checkpoint, an unreadable history) writes nothing.
    async fn replace_with_batches(
        &self,
        groups: Vec<(HashMap<String, String>, RecordBatch)>,
        state: UniformTableState,
    ) -> Result<ReplaceOutcome> {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let live = discover::read_live_set(&*self.store, &prefix, &self.config.fqtn()).await?;
        let modification_time_millis = chrono::Utc::now().timestamp_millis();

        let mut staged = Vec::with_capacity(groups.len());
        for (partition_values, batch) in &groups {
            // 1. Build deterministic Parquet bytes from the input batch.
            let parquet_bytes = parquet_builder::build_parquet(batch, &state)?;
            let hash = blake3::hash(&parquet_bytes).to_hex().to_string();
            // Per-column content hashes over the same in-memory Arrow batch.
            let column_hashes = compute_column_hashes(batch)?;
            // Hive-style partition prefix, in the table's partition_columns
            // order so the path is deterministic. Empty when unpartitioned.
            let mut add_file_path = String::new();
            for col in &state.partition_columns {
                let v = partition_values.get(col).ok_or_else(|| {
                    UniformWriterError::DeltaLog(format!(
                        "missing partition value for column `{col}`"
                    ))
                })?;
                add_file_path.push_str(&format!("{col}={v}/"));
            }
            add_file_path.push_str(&format!("{hash}.parquet"));
            let file_size = parquet_bytes.len() as u64;
            let parquet_path = Path::from(format!("{prefix}/{add_file_path}"));

            // 2. PUT the Parquet. Same content → same hash → same key →
            // idempotent. A re-PUT also restores bytes a VACUUM deleted.
            self.store
                .put(&parquet_path, PutPayload::from(Bytes::from(parquet_bytes)))
                .await?;

            let add = commit::build_add_action(&commit::AddInputs {
                batch,
                state: &state,
                add_file_path: &add_file_path,
                file_size,
                modification_time_millis,
                partition_values,
            })?;
            staged.push(StagedFile {
                path: add_file_path,
                add,
                blake3_hash: hash,
                column_hashes,
                num_records: batch.num_rows(),
                size_bytes: file_size,
            });
        }

        // 3. One replace commit.
        self.commit_replace(staged, live, state, modification_time_millis)
            .await
    }

    /// Make the live set equal `staged`, in at most one commit.
    ///
    /// ```text
    ///   live == new ──▶ no commit; table_version = current head
    ///   otherwise   ──▶ commit head+1:  remove (live − new) ; add (new − live)
    ///                     └─ 412 ──▶ re-read the live set, recompute, retry
    /// ```
    ///
    /// The conditional PUT (`If-None-Match: *`) makes the commit atomic. A
    /// 412 means another commit took the version: the live set is read again
    /// so the removes reflect the new head, and the retry may turn into a
    /// no-op when the winner already wrote this exact output.
    async fn commit_replace(
        &self,
        staged: Vec<StagedFile>,
        mut live: discover::LiveSet,
        mut state: UniformTableState,
        modification_time_millis: i64,
    ) -> Result<ReplaceOutcome> {
        use std::collections::BTreeSet;
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let new_paths: BTreeSet<&str> = staged.iter().map(|s| s.path.as_str()).collect();

        for attempt in 0..COND_PUT_RETRY_BUDGET {
            let live_paths: BTreeSet<&str> = live.files.keys().map(String::as_str).collect();
            if live_paths == new_paths {
                tracing::info!(
                    table = %self.config.fqtn(),
                    version = live.head_version,
                    "content-addressed output already live; no commit written"
                );
                return Ok(self.outcome(&staged, &live, live.head_version, false, Vec::new()));
            }

            let mut removes = Vec::new();
            let mut removed_paths = Vec::new();
            for (path, file) in &live.files {
                if !new_paths.contains(path.as_str()) {
                    removes.push(commit::build_remove_action(
                        &file.add,
                        modification_time_millis,
                    )?);
                    removed_paths.push(path.clone());
                }
            }
            if !removes.is_empty() && live.append_only {
                return Err(UniformWriterError::AppendOnlyTable {
                    table: self.config.fqtn(),
                });
            }

            let target_version = live.head_version + 1;
            // Allocate row ids for the files this commit adds, one
            // contiguous range per file, in order.
            let mut next_row_id = state.row_tracking_next_id;
            let mut high_water_mark = None;
            let mut adds = Vec::new();
            for s in staged.iter().filter(|s| !live.files.contains_key(&s.path)) {
                let mut add = s.add.clone();
                if state.row_tracking_enabled && s.num_records > 0 {
                    let base = next_row_id;
                    let high = base.checked_add(s.num_records as u64 - 1).ok_or_else(|| {
                        UniformWriterError::DeltaLog(format!(
                            "row_tracking_next_id={base} + {} rows overflows u64",
                            s.num_records
                        ))
                    })?;
                    commit::set_row_tracking(&mut add, base, target_version);
                    next_row_id = high.saturating_add(1);
                    high_water_mark = Some(high);
                }
                adds.push(add);
            }

            let body = commit::build_replace_commit_jsonl(&commit::ReplaceCommit {
                engine_info: &self.config.engine_info,
                timestamp_millis: modification_time_millis,
                read_version: live.head_version,
                partition_columns: &state.partition_columns,
                removes: &removes,
                adds: &adds,
                row_tracking_high_water_mark: high_water_mark,
            })?;
            let log_path = Path::from(format!("{prefix}/_delta_log/{target_version:020}.json"));
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match self
                .store
                .put_opts(&log_path, PutPayload::from(Bytes::from(body)), opts)
                .await
            {
                Ok(_) => {
                    return Ok(self.outcome(&staged, &live, target_version, true, removed_paths));
                }
                Err(object_store::Error::AlreadyExists { .. }) => {
                    // Another commit took `target_version`. Re-read the live
                    // set so the removes reflect the new head.
                    live =
                        discover::read_live_set(&*self.store, &prefix, &self.config.fqtn()).await?;
                    if state.row_tracking_enabled {
                        state.row_tracking_next_id =
                            discover::discover_row_tracking_next_id(&*self.store, &prefix).await?;
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        previous_target = target_version,
                        new_head = live.head_version,
                        "cond-put 412 on _delta_log entry; re-read the live set, retrying"
                    );
                }
                Err(e) => return Err(e.into()),
            }
        }
        Err(UniformWriterError::CondPutRetryExhausted(format!(
            "exhausted {COND_PUT_RETRY_BUDGET} retries chasing next_commit_version"
        )))
    }

    /// Assemble the [`ReplaceOutcome`] of a replace (or a no-op).
    ///
    /// A file that was live before the commit keeps the version of its live
    /// `add`; a file this commit added gets `table_version`.
    fn outcome(
        &self,
        staged: &[StagedFile],
        live: &discover::LiveSet,
        table_version: u64,
        committed: bool,
        removed_paths: Vec<String>,
    ) -> ReplaceOutcome {
        let prefix = self.config.prefix.trim_end_matches('/');
        let files = staged
            .iter()
            .map(|s| WriteResult {
                file_path: Path::from(format!("{prefix}/{}", s.path)).to_string(),
                blake3_hash: s.blake3_hash.clone(),
                column_hashes: s.column_hashes.clone(),
                commit_version: live.files.get(&s.path).map_or(table_version, |f| f.version),
                table_version,
                committed,
                num_records: s.num_records,
                size_bytes: s.size_bytes,
            })
            .collect();
        ReplaceOutcome {
            table_version,
            committed,
            removed_paths,
            files,
        }
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

        // A second write replaces the first and allocates row-ids 10..14.
        let result2 = writer.write_batch(make_batch(5)).await.unwrap();
        assert_eq!(result2.num_records, 5);
        let log_path2 = object_store::path::Path::from(format!(
            "{prefix}/_delta_log/00000000000000000002.json"
        ));
        let log2 = store.get(&log_path2).await.unwrap().bytes().await.unwrap();
        let lines2: Vec<&str> = std::str::from_utf8(&log2).unwrap().lines().collect();
        assert_eq!(
            lines2.len(),
            4,
            "commitInfo + remove + add + domainMetadata"
        );
        // The remove of the first file carries its row-tracking fields.
        let remove2: Value = serde_json::from_str(lines2[1]).unwrap();
        assert_eq!(remove2["remove"]["baseRowId"], 0);
        assert_eq!(remove2["remove"]["defaultRowCommitVersion"], 1);
        let add2: Value = serde_json::from_str(lines2[2]).unwrap();
        assert_eq!(add2["add"]["baseRowId"], 10);
        assert_eq!(add2["add"]["defaultRowCommitVersion"], 2);
        let dm2: Value = serde_json::from_str(lines2[3]).unwrap();
        let cfg2_raw = dm2["domainMetadata"]["configuration"].as_str().unwrap();
        let cfg2: Value = serde_json::from_str(cfg2_raw).unwrap();
        // Row ids are never reused: 10 allocated earlier + 5 new → 14.
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

    // -- replace semantics (RV1-P1a, #2269) ----------------------------------

    /// Independent Delta log replay for the tests: live = adds − removes,
    /// commit by commit, in version order. It does NOT use the writer's own
    /// reader. It also asserts the protocol rule that one commit never adds
    /// and removes the same path.
    async fn replay_live_paths(
        store: &InMemory,
        prefix: &str,
    ) -> std::collections::BTreeSet<String> {
        use futures::TryStreamExt;
        let mut commits: Vec<(u64, object_store::path::Path)> = Vec::new();
        let mut stream = store.list(Some(&object_store::path::Path::from(format!(
            "{prefix}/_delta_log"
        ))));
        while let Some(meta) = stream.try_next().await.unwrap() {
            let name = meta.location.filename().unwrap().to_string();
            if let Some(stem) = name.strip_suffix(".json")
                && stem.len() == 20
            {
                commits.push((stem.parse().unwrap(), meta.location));
            }
        }
        commits.sort_by_key(|(v, _)| *v);
        let mut live = std::collections::BTreeSet::new();
        for (v, path) in commits {
            let body = store.get(&path).await.unwrap().bytes().await.unwrap();
            let mut adds = Vec::new();
            let mut removes = Vec::new();
            for line in std::str::from_utf8(&body).unwrap().lines() {
                let value: Value = serde_json::from_str(line).unwrap();
                if let Some(p) = value["add"]["path"].as_str() {
                    adds.push(p.to_string());
                }
                if let Some(p) = value["remove"]["path"].as_str() {
                    removes.push(p.to_string());
                }
            }
            for p in &removes {
                assert!(!adds.contains(p), "commit {v} adds and removes `{p}`");
                live.remove(p);
            }
            live.extend(adds);
        }
        live
    }

    async fn commit_lines(store: &InMemory, prefix: &str, version: u64) -> Vec<Value> {
        let body = store
            .get(&object_store::path::Path::from(format!(
                "{prefix}/_delta_log/{version:020}.json"
            )))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        std::str::from_utf8(&body)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    async fn commit_exists(store: &InMemory, prefix: &str, version: u64) -> bool {
        store
            .head(&object_store::path::Path::from(format!(
                "{prefix}/_delta_log/{version:020}.json"
            )))
            .await
            .is_ok()
    }

    fn basename(file_path: &str) -> String {
        file_path.rsplit('/').next().unwrap().to_string()
    }

    fn actions<'a>(lines: &'a [Value], key: &str) -> Vec<&'a Value> {
        lines.iter().filter_map(|l| l.get(key)).collect()
    }

    #[tokio::test]
    async fn second_build_replaces_the_first() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");

        let a = writer.write_batch(make_batch(4)).await.unwrap();
        let b = writer.write_batch(make_batch(5)).await.unwrap();
        assert_eq!((a.commit_version, b.commit_version), (1, 2));
        assert!(b.committed);
        assert_eq!(b.table_version, 2);

        let lines = commit_lines(&store, "tbl", 2).await;
        let info = &lines[0]["commitInfo"];
        assert_eq!(info["operation"], "WRITE");
        assert_eq!(info["operationParameters"]["mode"], "Overwrite");
        assert_eq!(info["isBlindAppend"], false);
        let removes = actions(&lines, "remove");
        let adds = actions(&lines, "add");
        assert_eq!(removes.len(), 1, "exactly one remove");
        assert_eq!(adds.len(), 1, "exactly one add");
        assert_eq!(removes[0]["path"], basename(&a.file_path));
        assert_eq!(removes[0]["dataChange"], true);
        assert_eq!(removes[0]["extendedFileMetadata"], true);
        assert_eq!(removes[0]["size"], a.size_bytes);
        assert_eq!(adds[0]["path"], basename(&b.file_path));

        let live = replay_live_paths(&store, "tbl").await;
        assert_eq!(
            live.into_iter().collect::<Vec<_>>(),
            vec![basename(&b.file_path)],
            "the live table equals the second output only"
        );
    }

    #[tokio::test]
    async fn identical_rebuild_writes_no_commit() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");

        let first = writer.write_batch(make_batch(3)).await.unwrap();
        let again = writer.write_batch(make_batch(3)).await.unwrap();
        assert!(first.committed);
        assert!(!again.committed, "an unchanged output writes no commit");
        assert_eq!(
            again.table_version, 1,
            "the no-op names the current version"
        );
        assert_eq!(again.commit_version, 1);
        assert_eq!(again.blake3_hash, first.blake3_hash);
        assert!(!commit_exists(&store, "tbl", 2).await);
    }

    #[tokio::test]
    async fn partitioned_write_is_one_commit_and_moves_only_changed_files() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_partitioned_bootstrap(&store, "ptbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "ptbl");
        let pv = |r: &str| HashMap::from([("region".to_string(), r.to_string())]);

        // Run 1: three groups land in ONE commit with three adds.
        let run1 = writer
            .write_partitioned_batches(vec![
                (pv("eu"), make_partitioned_batch("eu", 2)),
                (pv("us"), make_partitioned_batch("us", 3)),
                (pv("ap"), make_partitioned_batch("ap", 1)),
            ])
            .await
            .unwrap();
        assert_eq!(run1.table_version, 1);
        assert!(
            !commit_exists(&store, "ptbl", 2).await,
            "one commit, not one per group"
        );
        let lines = commit_lines(&store, "ptbl", 1).await;
        assert_eq!(actions(&lines, "add").len(), 3);
        assert_eq!(actions(&lines, "remove").len(), 0);
        let eu1 = run1.files[0].file_path.clone();
        let us1 = run1.files[1].file_path.clone();

        // Run 2: eu changes, us is unchanged, ap disappears.
        let run2 = writer
            .write_partitioned_batches(vec![
                (pv("eu"), make_partitioned_batch("eu", 4)),
                (pv("us"), make_partitioned_batch("us", 3)),
            ])
            .await
            .unwrap();
        assert_eq!(run2.table_version, 2);
        assert_eq!(run2.files[1].file_path, us1, "same content, same path");
        assert_eq!(run2.files[1].commit_version, 1, "us keeps its live add");
        assert_eq!(run2.files[0].commit_version, 2);
        let lines = commit_lines(&store, "ptbl", 2).await;
        let removed: Vec<&str> = actions(&lines, "remove")
            .iter()
            .map(|r| r["path"].as_str().unwrap())
            .collect();
        let added: Vec<&str> = actions(&lines, "add")
            .iter()
            .map(|a| a["path"].as_str().unwrap())
            .collect();
        let rel = |p: &str| p.strip_prefix("ptbl/").unwrap().to_string();
        assert_eq!(removed.len(), 2, "old eu + ap: {removed:?}");
        assert!(removed.contains(&rel(&eu1).as_str()));
        assert!(
            !removed.contains(&rel(&us1).as_str()),
            "unchanged us is not removed"
        );
        assert_eq!(
            added,
            vec![rel(&run2.files[0].file_path)],
            "only the new eu is added"
        );
        // The remove keeps the partition metadata of the live add.
        let ap_remove = actions(&lines, "remove")
            .into_iter()
            .find(|r| r["path"].as_str().unwrap().starts_with("region=ap/"))
            .unwrap();
        assert_eq!(
            ap_remove["partitionValues"],
            serde_json::json!({"col-region-uuid": "ap"})
        );

        let live = replay_live_paths(&store, "ptbl").await;
        let expected: std::collections::BTreeSet<String> =
            [rel(&run2.files[0].file_path), rel(&us1)]
                .into_iter()
                .collect();
        assert_eq!(live, expected);
    }

    #[tokio::test]
    async fn a_checkpoint_refuses_the_write_and_writes_no_commit() {
        for marker in [
            "_last_checkpoint",
            "00000000000000000001.checkpoint.parquet",
        ] {
            let store: Arc<InMemory> = Arc::new(InMemory::new());
            seed_bootstrap(&store, "tbl").await;
            let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
            writer.write_batch(make_batch(2)).await.unwrap();
            store
                .put(
                    &object_store::path::Path::from(format!("tbl/_delta_log/{marker}")),
                    PutPayload::from(Bytes::from_static(b"{}")),
                )
                .await
                .unwrap();

            match writer.write_batch(make_batch(7)).await {
                Err(UniformWriterError::CheckpointPresent { table, checkpoint }) => {
                    assert_eq!(table, "c.s.t");
                    assert!(checkpoint.ends_with(marker), "{checkpoint}");
                    let msg =
                        UniformWriterError::CheckpointPresent { table, checkpoint }.to_string();
                    assert!(msg.contains("To recover"), "{msg}");
                }
                other => panic!("expected CheckpointPresent, got {other:?}"),
            }
            assert!(
                !commit_exists(&store, "tbl", 2).await,
                "no commit after a refusal"
            );
        }
    }

    #[tokio::test]
    async fn a_conflict_retry_recomputes_removes_against_the_new_head() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(2)).await.unwrap();

        // Read the live set at head 1 ({A}), then let a competitor land v2
        // (remove A, add C) before our commit.
        let state = writer.discover().await.unwrap();
        let stale = discover::read_live_set(&*store, "tbl", "c.s.t")
            .await
            .unwrap();
        let competitor = format!(
            "{{\"commitInfo\":{{}}}}\n\
             {{\"remove\":{{\"path\":\"{}\",\"dataChange\":true}}}}\n\
             {{\"add\":{{\"path\":\"c.parquet\",\"partitionValues\":{{}},\"size\":9,\
             \"modificationTime\":0,\"dataChange\":true}}}}\n",
            basename(&a.file_path)
        );
        store
            .put(
                &object_store::path::Path::from("tbl/_delta_log/00000000000000000002.json"),
                PutPayload::from(Bytes::from(competitor)),
            )
            .await
            .unwrap();

        let batch = make_batch(6);
        let pv = HashMap::new();
        let add = commit::build_add_action(&commit::AddInputs {
            batch: &batch,
            state: &state,
            add_file_path: "b.parquet",
            file_size: 11,
            modification_time_millis: 0,
            partition_values: &pv,
        })
        .unwrap();
        let staged = StagedFile {
            path: "b.parquet".into(),
            add,
            blake3_hash: "b".into(),
            column_hashes: Vec::new(),
            num_records: 6,
            size_bytes: 11,
        };
        let outcome = writer
            .commit_replace(vec![staged], stale, state, 0)
            .await
            .unwrap();
        assert_eq!(outcome.table_version, 3, "the 412 at v2 retried at v3");
        let lines = commit_lines(&store, "tbl", 3).await;
        let removed: Vec<&str> = actions(&lines, "remove")
            .iter()
            .map(|r| r["path"].as_str().unwrap())
            .collect();
        assert_eq!(
            removed,
            vec!["c.parquet"],
            "removes reflect the new head, not the stale A"
        );
        let live = replay_live_paths(&store, "tbl").await;
        assert_eq!(
            live.into_iter().collect::<Vec<_>>(),
            vec!["b.parquet".to_string()]
        );
    }

    #[tokio::test]
    async fn a_conflict_with_the_same_output_becomes_a_no_op() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(2)).await.unwrap();
        let state = writer.discover().await.unwrap();
        let stale = discover::read_live_set(&*store, "tbl", "c.s.t")
            .await
            .unwrap();
        // A competitor lands exactly our output at v2.
        let competitor = format!(
            "{{\"commitInfo\":{{}}}}\n\
             {{\"remove\":{{\"path\":\"{}\",\"dataChange\":true}}}}\n\
             {{\"add\":{{\"path\":\"b.parquet\",\"partitionValues\":{{}},\"size\":11,\
             \"modificationTime\":0,\"dataChange\":true}}}}\n",
            basename(&a.file_path)
        );
        store
            .put(
                &object_store::path::Path::from("tbl/_delta_log/00000000000000000002.json"),
                PutPayload::from(Bytes::from(competitor)),
            )
            .await
            .unwrap();
        let staged = StagedFile {
            path: "b.parquet".into(),
            add: serde_json::json!({"path": "b.parquet", "partitionValues": {}, "size": 11})
                .as_object()
                .unwrap()
                .clone(),
            blake3_hash: "b".into(),
            column_hashes: Vec::new(),
            num_records: 1,
            size_bytes: 11,
        };
        let outcome = writer
            .commit_replace(vec![staged], stale, state, 0)
            .await
            .unwrap();
        assert!(!outcome.committed);
        assert_eq!(outcome.table_version, 2);
        assert!(!commit_exists(&store, "tbl", 3).await);
    }

    #[tokio::test]
    async fn point_to_replaces_the_live_set() {
        // Build A (v1), then B (v2). A point-to back to A is a replace: it
        // removes B and re-adds A in one commit.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(3)).await.unwrap();
        let b = writer.write_batch(make_batch(8)).await.unwrap();
        let pointer = writer
            .recover_pointer_inputs(a.commit_version, a.blake3_hash.clone(), 3, a.size_bytes)
            .await
            .unwrap();
        let state = writer.discover().await.unwrap();
        let pr = writer
            .commit_pointer_with_state(&pointer, state)
            .await
            .unwrap();
        assert_eq!(pr.commit_version, 3);
        let lines = commit_lines(&store, "tbl", 3).await;
        assert_eq!(
            lines[0]["commitInfo"]["operationParameters"]["mode"],
            "Overwrite"
        );
        assert_eq!(actions(&lines, "remove")[0]["path"], basename(&b.file_path));
        assert_eq!(actions(&lines, "add")[0]["path"], basename(&a.file_path));
        let live = replay_live_paths(&store, "tbl").await;
        assert_eq!(
            live.into_iter().collect::<Vec<_>>(),
            vec![basename(&a.file_path)]
        );
    }

    #[tokio::test]
    async fn an_append_only_table_refuses_the_replace() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        // Land v2: the bootstrap metaData with delta.appendOnly=true.
        let mut md = commit_lines(&store, "tbl", 0).await[1].clone();
        md["metaData"]["configuration"]["delta.appendOnly"] = Value::from("true");
        store
            .put(
                &object_store::path::Path::from("tbl/_delta_log/00000000000000000002.json"),
                PutPayload::from(Bytes::from(format!("{md}\n"))),
            )
            .await
            .unwrap();
        match writer.write_batch(make_batch(5)).await {
            Err(UniformWriterError::AppendOnlyTable { table }) => assert_eq!(table, "c.s.t"),
            other => panic!("expected AppendOnlyTable, got {other:?}"),
        }
        assert!(!commit_exists(&store, "tbl", 3).await);
    }
}
