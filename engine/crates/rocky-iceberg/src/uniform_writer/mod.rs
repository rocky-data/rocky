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
pub mod publish;

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
    /// clean, checkpoint-free, contiguous commit history, the
    /// highest-versioned commit referencing it is a `remove`, and Delta's
    /// deleted-file retention window has passed. Safe to reclaim.
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
    /// The file is provably removed, but Delta's retention window
    /// (`deletionTimestamp + delta.deletedFileRetentionDuration`, default 7
    /// days) has not passed: time travel can still read it.
    RetentionWindowOpen,
    /// The file is provably removed, but its deletion time or the table's
    /// retention setting could not be read — the window cannot be checked.
    RetentionUnknown,
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
            RemovalHoldReason::RetentionWindowOpen => "retention_window_open",
            RemovalHoldReason::RetentionUnknown => "retention_unknown",
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
    ///
    /// Informational only. A replace commit allocates row ids from the
    /// live-set replay that also picks the commit version, never from this
    /// value: a commit can land between `discover()` and the replace.
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

/// Who calls [`UniformWriter::commit_replace`], which sets what it does when
/// the commit PUT does not succeed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplaceMode {
    /// A build. A lost conditional PUT re-reads the live set and recomputes
    /// the removes against the new head: a build replaces the table's
    /// content, so the winner's files go too.
    Build,
    /// A table publish. A lost conditional PUT refuses when the new head's
    /// live files differ from the ones the first attempt saw: a publish must
    /// not remove files it never saw. A PUT error that does not say whether
    /// the commit was stored is [`UniformWriterError::CommitOutcomeUnknown`].
    Publish,
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

/// The object-store [`Path`] of a table-relative object key under `prefix`.
///
/// The key's segments are already escaped (see [`commit::escape_path_name`]),
/// so they are parsed as they are. `Path::from` would percent-encode a `%`
/// a second time and the object would not sit at the key the log names.
fn object_path(prefix: &str, relative_key: &str) -> Result<Path> {
    // `Path::from(prefix)` spells the prefix the way every other key of this
    // table spells it; the relative key is appended verbatim.
    let joined = format!("{}/{relative_key}", Path::from(prefix));
    Path::parse(&joined)
        .map_err(|e| UniformWriterError::DeltaLog(format!("output key `{relative_key}`: {e}")))
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
    /// `add.path`, relative to the table prefix and URI-encoded as the log
    /// carries it. Its [`discover::canonical_key`] is the object key.
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
///   commit references; used both as the `add.path` and as the key the
///   replace compares against the live set. It is `recovered_add["path"]`, surfaced
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
    /// The table's object-store bucket, used to resolve absolute `s3://`
    /// paths in the log. `None` makes every absolute path refuse.
    table_bucket: Option<String>,
}

impl UniformWriter {
    pub fn new(
        config: UniformWriterConfig,
        store: Arc<dyn ObjectStore>,
        sql: Arc<dyn SqlClient>,
    ) -> Self {
        Self {
            config,
            store,
            sql,
            table_bucket: None,
        }
    }

    /// Set the table's bucket so the live-set replay can resolve absolute
    /// `s3://<bucket>/…` paths that another engine wrote into the log.
    #[must_use]
    pub fn with_table_bucket(mut self, bucket: impl Into<String>) -> Self {
        self.table_bucket = Some(bucket.into());
        self
    }

    fn bucket(&self) -> &str {
        self.table_bucket.as_deref().unwrap_or_default()
    }

    /// Read the live set, resolving paths against this table.
    async fn live_set(&self) -> Result<discover::LiveSet> {
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        discover::read_live_set(&*self.store, &prefix, &self.config.fqtn(), self.bucket()).await
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
        let live = self.live_set().await?;
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
        self.commit_replace(
            vec![staged],
            live,
            state,
            modification_time_millis,
            ReplaceMode::Build,
        )
        .await?
        .into_single()
    }

    /// Make the table serve an earlier content-addressed output again
    /// (RV1-P3 table publish). `files` are the blake3 hashes the output
    /// recorded ([`rocky_core::state::OutputVersion::ContentAddressed`]).
    ///
    /// One replace commit makes the live set exactly those files. Their
    /// `add` actions are lifted from the commits that added them, so no byte
    /// is copied or rewritten. When the files are already the live set, no
    /// commit is written.
    ///
    /// ```text
    ///   v1: add A   v2: remove A, add B   publish(A) ─▶ v3: remove B, add A
    /// ```
    ///
    /// # Scope
    ///
    /// Unpartitioned, non-rowTracking tables only, like
    /// [`Self::commit_pointer_with_state`].
    ///
    /// # Errors
    ///
    /// - [`UniformWriterError::PartitionedUnsupported`] / `DeltaLog` for a
    ///   partitioned or rowTracking table, or a malformed hash;
    /// - [`UniformWriterError::PublishSourceUnavailable`] when no commit of
    ///   this table added a file, the file was added before the latest
    ///   protocol, schema, partitioning or column-mapping-mode change, its
    ///   `add` carries a deletion vector, or its bytes are gone (for example
    ///   after a `VACUUM`);
    /// - every error of the replace commit (checkpoint, append-only, a
    ///   concurrent shape change, the retry budget).
    pub async fn restore_content_addressed(
        &self,
        files: &[String],
        state: UniformTableState,
    ) -> Result<ReplaceOutcome> {
        if !state.partition_columns.is_empty() {
            return Err(UniformWriterError::PartitionedUnsupported(
                state.partition_columns.clone(),
            ));
        }
        if state.row_tracking_enabled {
            return Err(UniformWriterError::RowTrackingUnsupported);
        }
        if files.is_empty() {
            return Err(UniformWriterError::DeltaLog(
                "a publish names no output file".to_string(),
            ));
        }
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        let unavailable = |detail: String| UniformWriterError::PublishSourceUnavailable {
            table: self.config.fqtn(),
            detail,
        };
        let live = self.live_set().await?;
        let modification_time_millis = chrono::Utc::now().timestamp_millis();
        let mut staged = Vec::with_capacity(files.len());
        for hash in files {
            if hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()) {
                return Err(UniformWriterError::DeltaLog(format!(
                    "`{hash}` is not a blake3 hex hash"
                )));
            }
            let name = format!("{hash}.parquet");
            let key = discover::canonical_key(&name, self.bucket(), &prefix)
                .ok_or_else(|| unavailable(format!("`{name}` cannot be resolved")))?;
            let Some(found) = live.ever_added.get(&key) else {
                return Err(unavailable(format!(
                    "no commit in the table's log added `{name}`"
                )));
            };
            if found.version < live.shape_version {
                return Err(unavailable(format!(
                    "`{name}` was added at commit {}, before the protocol, schema, partitioning \
                     or column mapping changed at commit {}",
                    found.version, live.shape_version
                )));
            }
            // A deletion vector would hide rows of the file. Rocky never
            // writes one, so a lifted `add` that carries one is not Rocky's
            // output as recorded.
            if found
                .add
                .get("deletionVector")
                .is_some_and(|dv| !dv.is_null())
            {
                return Err(unavailable(format!(
                    "the `add` of `{name}` at commit {} carries a deletion vector",
                    found.version
                )));
            }
            match self.store.head(&object_path(&prefix, &name)?).await {
                Ok(_) => {}
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(unavailable(format!(
                        "the bytes of `{name}` are gone (a VACUUM can remove the files of a \
                         replaced version)"
                    )));
                }
                Err(e) => return Err(e.into()),
            }
            let add = commit::lift_add_action(&found.add, modification_time_millis)?;
            let path = add
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let size_bytes = add
                .get("size")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();
            let num_records = add
                .get("stats")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                .and_then(|s| s.get("numRecords").and_then(serde_json::Value::as_u64))
                .unwrap_or_default() as usize;
            staged.push(StagedFile {
                path,
                add,
                blake3_hash: hash.clone(),
                column_hashes: Vec::new(),
                num_records,
                size_bytes,
            });
        }
        self.commit_replace(
            staged,
            live,
            state,
            modification_time_millis,
            ReplaceMode::Publish,
        )
        .await
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
        let live = self.live_set().await?;
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
            // Each segment is escaped like Spark's partition directories;
            // see `commit::escape_path_name`.
            let mut object_key = String::new();
            for col in &state.partition_columns {
                let v = partition_values.get(col).ok_or_else(|| {
                    UniformWriterError::DeltaLog(format!(
                        "missing partition value for column `{col}`"
                    ))
                })?;
                object_key.push_str(&commit::partition_dir(col, v));
                object_key.push('/');
            }
            object_key.push_str(&format!("{hash}.parquet"));
            let file_size = parquet_bytes.len() as u64;
            let parquet_path = object_path(&prefix, &object_key)?;

            // 2. PUT the Parquet. Same content → same hash → same key →
            // idempotent. A re-PUT also restores bytes a VACUUM deleted.
            self.store
                .put(&parquet_path, PutPayload::from(Bytes::from(parquet_bytes)))
                .await?;

            let add = commit::build_add_action(&commit::AddInputs {
                batch,
                state: &state,
                add_file_path: &object_key,
                file_size,
                modification_time_millis,
                partition_values,
            })?;
            // The URI-encoded `add.path`; its canonical key is `object_key`.
            let log_path = add
                .get("path")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            staged.push(StagedFile {
                path: log_path,
                add,
                blake3_hash: hash,
                column_hashes,
                num_records: batch.num_rows(),
                size_bytes: file_size,
            });
        }

        // 3. One replace commit.
        self.commit_replace(
            staged,
            live,
            state,
            modification_time_millis,
            ReplaceMode::Build,
        )
        .await
    }

    /// Make the live set equal `staged`, in at most one commit.
    ///
    /// ```text
    ///   table shape changed since discover() ──▶ refuse, no commit
    ///   live == new ──▶ no commit; table_version = current head
    ///   otherwise   ──▶ commit head+1:  remove (live − new) ; add (new − live)
    ///                     └─ 412 / 409 ──▶ re-read the live set, recompute, retry
    /// ```
    ///
    /// Files are compared by canonical key, so a live file that the log
    /// spells as an absolute or percent-encoded path still matches the new
    /// output's relative path. A `remove` copies the live file's own spelling.
    ///
    /// The conditional PUT (`If-None-Match: *`) makes the commit atomic. A
    /// conflict means another commit took the version: the live set, the
    /// protocol and the metadata are read again. The removes then reflect the
    /// new head, a schema or protocol change refuses, and the retry may turn
    /// into a no-op when the winner already wrote this exact output.
    ///
    /// With [`ReplaceMode::Publish`] (a table publish) a conflict
    /// whose winner changed the live files refuses instead: recomputing the
    /// removes against the new head would silently remove the winner's
    /// files. That includes a winner that wrote this exact output; the next
    /// publish then finds the table already current.
    async fn commit_replace(
        &self,
        staged: Vec<StagedFile>,
        mut live: discover::LiveSet,
        state: UniformTableState,
        modification_time_millis: i64,
        mode: ReplaceMode,
    ) -> Result<ReplaceOutcome> {
        use std::collections::{BTreeMap, BTreeSet};
        let prefix = self.config.prefix.trim_end_matches('/').to_string();
        // Canonical key of every staged file.
        let mut new_keys: BTreeMap<String, usize> = BTreeMap::new();
        for (i, s) in staged.iter().enumerate() {
            let key =
                discover::canonical_key(&s.path, self.bucket(), &prefix).ok_or_else(|| {
                    UniformWriterError::DeltaLog(format!(
                        "output path `{}` cannot be resolved inside the table",
                        s.path
                    ))
                })?;
            if new_keys.insert(key, i).is_some() {
                return Err(UniformWriterError::DeltaLog(format!(
                    "two output files resolve to the same path `{}`",
                    s.path
                )));
            }
        }
        let expected_shape = discover::TableShape::of_state(&state);
        let first_snapshot = (live.protocol.clone(), live.metadata.clone());
        let first_files: BTreeSet<String> = live.files.keys().cloned().collect();

        for attempt in 0..COND_PUT_RETRY_BUDGET {
            // The prepared files match the shape discover() saw. A schema,
            // partitioning or protocol change since then refuses.
            let shape = live.shape()?;
            if shape != expected_shape {
                return Err(UniformWriterError::TableChangedDuringWrite {
                    table: self.config.fqtn(),
                    what: describe_shape_change(&expected_shape, &shape),
                });
            }
            if (&live.protocol, &live.metadata) != (&first_snapshot.0, &first_snapshot.1) {
                return Err(UniformWriterError::TableChangedDuringWrite {
                    table: self.config.fqtn(),
                    what: "a later commit changed the table protocol or metadata".to_string(),
                });
            }

            let live_keys: BTreeSet<&str> = live.files.keys().map(String::as_str).collect();
            let new_key_set: BTreeSet<&str> = new_keys.keys().map(String::as_str).collect();
            if live_keys == new_key_set {
                tracing::info!(
                    table = %self.config.fqtn(),
                    version = live.head_version,
                    "content-addressed output already live; no commit written"
                );
                return Ok(self.outcome(
                    &staged,
                    &new_keys,
                    &live,
                    live.head_version,
                    false,
                    Vec::new(),
                ));
            }

            let mut removes = Vec::new();
            let mut removed_paths = Vec::new();
            for (key, file) in &live.files {
                if !new_keys.contains_key(key) {
                    let remove = commit::build_remove_action(&file.add, modification_time_millis)?;
                    removed_paths.push(
                        remove
                            .get("path")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string(),
                    );
                    removes.push(remove);
                }
            }
            if !removes.is_empty() && live.append_only {
                return Err(UniformWriterError::AppendOnlyTable {
                    table: self.config.fqtn(),
                });
            }

            let target_version = live.head_version + 1;
            // Allocate row ids for the files this commit adds, one
            // contiguous range per file, in order. The high-water mark comes
            // from the same log replay as `head_version`: a mark read in a
            // separate pass can be older than the head this commit follows,
            // and the ranges would then overlap the head's row ids.
            let mut next_row_id = if state.row_tracking_enabled {
                live.row_tracking_next_id()?
            } else {
                0
            };
            let mut high_water_mark = None;
            let mut adds = Vec::new();
            for (key, &i) in &new_keys {
                if live.files.contains_key(key) {
                    continue;
                }
                let s = &staged[i];
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
                    return Ok(self.outcome(
                        &staged,
                        &new_keys,
                        &live,
                        target_version,
                        true,
                        removed_paths,
                    ));
                }
                // object_store maps S3's 412 and its 409
                // `ConditionalRequestConflict` on a create to `AlreadyExists`;
                // `Precondition` is the other spelling some stores use.
                Err(
                    object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. },
                ) => {
                    // Another commit took `target_version`. Re-read the live
                    // set, protocol, metadata and row-id high-water mark in
                    // one replay so the next attempt reflects the new head.
                    live = self.live_set().await?;
                    match mode {
                        ReplaceMode::Build => {}
                        ReplaceMode::Publish => {
                            let now: BTreeSet<String> = live.files.keys().cloned().collect();
                            if now != first_files {
                                return Err(UniformWriterError::TableChangedDuringWrite {
                                    table: self.config.fqtn(),
                                    what: format!(
                                        "a concurrent commit (now at version {}) changed the \
                                         table's files, so this publish would remove them",
                                        live.head_version
                                    ),
                                });
                            }
                        }
                    }
                    tracing::warn!(
                        attempt = attempt + 1,
                        previous_target = target_version,
                        new_head = live.head_version,
                        "conditional put conflict on _delta_log entry; re-read the live set, retrying"
                    );
                }
                Err(e) => {
                    // These errors reject the request before anything is
                    // stored. Any other one (a timeout, a dropped connection,
                    // a 5xx) may come after the store kept the object.
                    let rejected = matches!(
                        e,
                        object_store::Error::InvalidPath { .. }
                            | object_store::Error::NotSupported { .. }
                            | object_store::Error::NotImplemented { .. }
                            | object_store::Error::PermissionDenied { .. }
                            | object_store::Error::Unauthenticated { .. }
                    );
                    return Err(match mode {
                        ReplaceMode::Publish if !rejected => {
                            UniformWriterError::CommitOutcomeUnknown {
                                table: self.config.fqtn(),
                                version: target_version,
                                detail: e.to_string(),
                            }
                        }
                        ReplaceMode::Publish | ReplaceMode::Build => e.into(),
                    });
                }
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
        new_keys: &std::collections::BTreeMap<String, usize>,
        live: &discover::LiveSet,
        table_version: u64,
        committed: bool,
        removed_paths: Vec<String>,
    ) -> ReplaceOutcome {
        let prefix = self.config.prefix.trim_end_matches('/');
        let version_of: HashMap<usize, u64> = new_keys
            .iter()
            .map(|(key, &i)| (i, live.files.get(key).map_or(table_version, |f| f.version)))
            .collect();
        // The canonical key is the decoded `add.path` under the prefix: the
        // object key, not the URI-encoded log spelling.
        let key_of: HashMap<usize, &str> =
            new_keys.iter().map(|(key, &i)| (i, key.as_str())).collect();
        let files = staged
            .iter()
            .enumerate()
            .map(|(i, s)| WriteResult {
                file_path: key_of.get(&i).map_or_else(
                    || Path::from(format!("{prefix}/{}", s.path)).to_string(),
                    |k| (*k).to_string(),
                ),
                blake3_hash: s.blake3_hash.clone(),
                column_hashes: s.column_hashes.clone(),
                commit_version: version_of.get(&i).copied().unwrap_or(table_version),
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

/// Name what differs between the shape `discover()` saw and the latest one.
fn describe_shape_change(expected: &discover::TableShape, latest: &discover::TableShape) -> String {
    let mut what = Vec::new();
    if expected.partition_columns != latest.partition_columns {
        what.push(format!(
            "partition columns {:?} → {:?}",
            expected.partition_columns, latest.partition_columns
        ));
    }
    if expected.physical != latest.physical || expected.field_id != latest.field_id {
        what.push("the schema or its column mapping changed".to_string());
    }
    if expected.row_tracking_enabled != latest.row_tracking_enabled {
        what.push(format!(
            "rowTracking {} → {}",
            expected.row_tracking_enabled, latest.row_tracking_enabled
        ));
    }
    what.join("; ")
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
        let stale = discover::read_live_set(&*store, "tbl", "c.s.t", "")
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
            .commit_replace(vec![staged], stale, state, 0, ReplaceMode::Build)
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
        let stale = discover::read_live_set(&*store, "tbl", "c.s.t", "")
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
            .commit_replace(vec![staged], stale, state, 0, ReplaceMode::Build)
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

    // -- red-team fixes: protocol, shape, canonical paths, DVs ----------------

    async fn put_commit(store: &InMemory, prefix: &str, version: u64, lines: &[Value]) {
        let body: String = lines.iter().map(|l| format!("{l}\n")).collect();
        store
            .put(
                &object_store::path::Path::from(format!("{prefix}/_delta_log/{version:020}.json")),
                PutPayload::from(Bytes::from(body)),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_feature_enabled_after_v0_refuses_the_replace() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        // v2: an `ALTER TABLE` that turns on in-commit timestamps.
        let boot = commit_lines(&store, "tbl", 0).await;
        let mut protocol = boot[0].clone();
        protocol["protocol"]["writerFeatures"]
            .as_array_mut()
            .unwrap()
            .push(Value::from("inCommitTimestamp"));
        let mut md = boot[1].clone();
        md["metaData"]["configuration"]["delta.enableInCommitTimestamps"] = Value::from("true");
        put_commit(&store, "tbl", 2, &[protocol, md]).await;

        match writer.write_batch(make_batch(5)).await {
            Err(UniformWriterError::UnsupportedTableFeature { table, feature }) => {
                assert_eq!(table, "c.s.t");
                assert!(feature.contains("inCommitTimestamp"), "{feature}");
            }
            other => panic!("expected UnsupportedTableFeature, got {other:?}"),
        }
        assert!(
            !commit_exists(&store, "tbl", 3).await,
            "no commit after a refusal"
        );
    }

    #[tokio::test]
    async fn a_config_only_feature_after_v0_refuses_the_replace() {
        // The protocol stays the same; only the table configuration turns
        // change data feed on.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        let mut md = commit_lines(&store, "tbl", 0).await[1].clone();
        md["metaData"]["configuration"]["delta.enableChangeDataFeed"] = Value::from("true");
        put_commit(&store, "tbl", 2, &[md]).await;
        match writer.write_batch(make_batch(5)).await {
            Err(UniformWriterError::UnsupportedTableFeature { feature, .. }) => {
                assert!(feature.contains("changeDataFeed"), "{feature}");
            }
            other => panic!("expected UnsupportedTableFeature, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn staged_commits_refuse_the_replace() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        store
            .put(
                &object_store::path::Path::from(
                    "tbl/_delta_log/_staged_commits/00000000000000000002.uuid.json",
                ),
                PutPayload::from(Bytes::from_static(b"{}")),
            )
            .await
            .unwrap();
        match writer.write_batch(make_batch(5)).await {
            Err(UniformWriterError::UnsupportedTableFeature { table, feature }) => {
                assert_eq!(table, "c.s.t");
                assert!(feature.contains("_staged_commits"), "{feature}");
            }
            other => panic!("expected UnsupportedTableFeature, got {other:?}"),
        }
        assert!(!commit_exists(&store, "tbl", 2).await);
    }

    #[tokio::test]
    async fn an_unknown_action_refuses_the_replace() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        put_commit(&store, "tbl", 2, &[serde_json::json!({"futureAction": {}})]).await;
        match writer.write_batch(make_batch(5)).await {
            Err(UniformWriterError::UnsupportedTableFeature { feature, .. }) => {
                assert!(feature.contains("futureAction"), "{feature}");
            }
            other => panic!("expected UnsupportedTableFeature, got {other:?}"),
        }
    }

    /// A metaData commit with one more column than the bootstrap schema.
    async fn alter_add_column(store: &InMemory, prefix: &str, version: u64) {
        let mut md = commit_lines(store, prefix, 0).await[1].clone();
        let mut schema: Value =
            serde_json::from_str(md["metaData"]["schemaString"].as_str().unwrap()).unwrap();
        schema["fields"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "name": "extra", "type": "string", "nullable": true, "metadata": {
                    "delta.columnMapping.id": 9,
                    "delta.columnMapping.physicalName": "col-extra-uuid"
                }
            }));
        md["metaData"]["schemaString"] = Value::from(schema.to_string());
        put_commit(store, prefix, version, &[md]).await;
    }

    #[tokio::test]
    async fn a_schema_change_after_discover_refuses_without_a_commit() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        let stale_state = writer.discover().await.unwrap();
        alter_add_column(&store, "tbl", 2).await;
        match writer
            .write_batch_with_state(make_batch(5), stale_state)
            .await
        {
            Err(UniformWriterError::TableChangedDuringWrite { table, what }) => {
                assert_eq!(table, "c.s.t");
                assert!(what.contains("schema"), "{what}");
            }
            other => panic!("expected TableChangedDuringWrite, got {other:?}"),
        }
        assert!(!commit_exists(&store, "tbl", 3).await);
    }

    #[tokio::test]
    async fn a_conflict_retry_rereads_the_schema_and_refuses_on_change() {
        // The live set is read at head 1. A competitor then lands v2: an
        // ALTER that adds a column. Our commit at v2 conflicts; the retry
        // re-reads the metadata, sees the change and refuses.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        writer.write_batch(make_batch(2)).await.unwrap();
        let state = writer.discover().await.unwrap();
        let stale = discover::read_live_set(&*store, "tbl", "c.s.t", "")
            .await
            .unwrap();
        alter_add_column(&store, "tbl", 2).await;
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
        match writer
            .commit_replace(vec![staged], stale, state, 0, ReplaceMode::Build)
            .await
        {
            Err(UniformWriterError::TableChangedDuringWrite { .. }) => {}
            other => panic!("expected TableChangedDuringWrite, got {other:?}"),
        }
        assert!(!commit_exists(&store, "tbl", 3).await);
    }

    /// The live `add` of the file a build produces, re-spelled by another
    /// engine: v2 removes Rocky's relative spelling, v3 adds `spelling`.
    async fn respell_live_file(store: &InMemory, prefix: &str, rel: &str, spelling: &str) {
        let v1 = commit_lines(store, prefix, 1).await;
        let add = actions(&v1, "add")[0].clone();
        put_commit(
            store,
            prefix,
            2,
            &[serde_json::json!({"remove": {"path": rel, "dataChange": true}})],
        )
        .await;
        let mut respelled = add.clone();
        respelled["path"] = Value::from(spelling);
        put_commit(store, prefix, 3, &[serde_json::json!({ "add": respelled })]).await;
    }

    #[tokio::test]
    async fn an_absolute_spelling_of_the_same_file_is_not_moved() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl")
            .with_table_bucket("bkt");
        let first = writer.write_batch(make_batch(3)).await.unwrap();
        let rel = basename(&first.file_path);
        respell_live_file(&store, "tbl", &rel, &format!("s3://bkt/tbl/{rel}")).await;

        let again = writer.write_batch(make_batch(3)).await.unwrap();
        assert!(!again.committed, "same canonical file: no remove, no add");
        assert_eq!(again.table_version, 3);
        assert_eq!(
            again.commit_version, 3,
            "the version of the live (absolute) add"
        );
        assert!(!commit_exists(&store, "tbl", 4).await);

        // A different output removes the file by its own (absolute) spelling.
        writer.write_batch(make_batch(4)).await.unwrap();
        let lines = commit_lines(&store, "tbl", 4).await;
        assert_eq!(
            actions(&lines, "remove")[0]["path"],
            format!("s3://bkt/tbl/{rel}")
        );
    }

    #[tokio::test]
    async fn a_percent_encoded_spelling_of_the_same_file_is_not_moved() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_partitioned_bootstrap(&store, "ptbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "ptbl");
        let pv = HashMap::from([("region".to_string(), "eu".to_string())]);
        let first = writer
            .write_partitioned_batch(make_partitioned_batch("eu", 2), pv.clone())
            .await
            .unwrap();
        let rel = first.file_path.strip_prefix("ptbl/").unwrap().to_string();
        let encoded = rel.replace('=', "%3D");
        assert_ne!(encoded, rel);
        respell_live_file(&store, "ptbl", &rel, &encoded).await;

        let again = writer
            .write_partitioned_batch(make_partitioned_batch("eu", 2), pv)
            .await
            .unwrap();
        assert!(!again.committed, "same canonical file: no remove, no add");
        assert!(!commit_exists(&store, "ptbl", 4).await);
    }

    #[tokio::test]
    async fn a_deletion_vector_update_refuses_as_deletion_vectors_unsupported() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(3)).await.unwrap();
        let rel = basename(&a.file_path);
        // A legal DV update: remove + add of one path with different DVs.
        let dv = serde_json::json!({"storageType": "u", "pathOrInlineDv": "x", "sizeInBytes": 1, "cardinality": 1});
        put_commit(
            &store,
            "tbl",
            2,
            &[
                serde_json::json!({"remove": {"path": rel, "dataChange": true}}),
                serde_json::json!({"add": {"path": rel, "size": 1, "partitionValues": {}, "dataChange": true, "deletionVector": dv}}),
            ],
        )
        .await;
        assert!(matches!(
            writer.write_batch(make_batch(4)).await,
            Err(UniformWriterError::DeletionVectorsUnsupported)
        ));

        // A remove that carries a deletion vector also refuses.
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(3)).await.unwrap();
        put_commit(
            &store,
            "tbl",
            2,
            &[serde_json::json!({"remove": {"path": basename(&a.file_path), "dataChange": true, "deletionVector": dv}})],
        )
        .await;
        assert!(matches!(
            writer.write_batch(make_batch(4)).await,
            Err(UniformWriterError::DeletionVectorsUnsupported)
        ));
    }

    #[tokio::test]
    async fn a_superseded_file_is_held_until_the_retention_window_passes() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "tbl");
        let a = writer.write_batch(make_batch(3)).await.unwrap();
        writer.write_batch(make_batch(4)).await.unwrap(); // removes A at v2
        let rel = basename(&a.file_path);
        let deleted_at =
            actions(&commit_lines(&store, "tbl", 2).await, "remove")[0]["deletionTimestamp"]
                .as_i64()
                .unwrap();
        let week = 7 * 24 * 60 * 60 * 1000;
        assert_eq!(
            discover::proven_removed_at(&*store, "b", "tbl", &rel, 1, deleted_at + week - 1).await,
            RemovalProof::Held(RemovalHoldReason::RetentionWindowOpen),
            "inside the default 7-day window, time travel can still read A"
        );
        assert_eq!(
            discover::proven_removed_at(&*store, "b", "tbl", &rel, 1, deleted_at + week).await,
            RemovalProof::ProvenRemoved { head_version: 2 }
        );

        // A table-level retention setting is honoured.
        let mut md = commit_lines(&store, "tbl", 0).await[1].clone();
        md["metaData"]["configuration"]["delta.deletedFileRetentionDuration"] =
            Value::from("interval 1 hours");
        put_commit(&store, "tbl", 3, &[md.clone()]).await;
        assert_eq!(
            discover::proven_removed_at(&*store, "b", "tbl", &rel, 1, deleted_at + 3_600_000).await,
            RemovalProof::ProvenRemoved { head_version: 3 }
        );
        // An unreadable retention setting holds.
        md["metaData"]["configuration"]["delta.deletedFileRetentionDuration"] =
            Value::from("seven days");
        put_commit(&store, "tbl", 4, &[md]).await;
        assert_eq!(
            discover::proven_removed_at(&*store, "b", "tbl", &rel, 1, i64::MAX / 2).await,
            RemovalProof::Held(RemovalHoldReason::RetentionUnknown)
        );
    }

    // -- add.path is URI-encoded; the object key is Spark-escaped -------------

    /// Every object key under `prefix/` that ends in `.parquet`.
    async fn parquet_keys(store: &InMemory, prefix: &str) -> Vec<String> {
        use futures::TryStreamExt;
        let mut out = Vec::new();
        let mut stream = store.list(Some(&object_store::path::Path::from(prefix)));
        while let Some(meta) = stream.try_next().await.unwrap() {
            let k = meta.location.to_string();
            if k.ends_with(".parquet") {
                out.push(k);
            }
        }
        out.sort();
        out
    }

    #[test]
    fn special_partition_values_escape_like_spark_and_encode_in_the_log() {
        // (value, object-key directory, add.path directory)
        let cases = [
            ("50%off", "region=50%25off", "region=50%2525off"),
            ("a b", "region=a b", "region=a%20b"),
            ("x#y", "region=x%23y", "region=x%2523y"),
            ("q?r", "region=q%3Fr", "region=q%253Fr"),
            ("a/b", "region=a%2Fb", "region=a%252Fb"),
            ("k=v", "region=k%3Dv", "region=k%253Dv"),
            ("a<b", "region=a%3Cb", "region=a%253Cb"),
            ("t~", "region=t%7E", "region=t%257E"),
            ("über", "region=über", "region=%C3%BCber"),
            ("eu", "region=eu", "region=eu"),
        ];
        for (value, dir, log_dir) in cases {
            let key = format!("{}/H.parquet", commit::partition_dir("region", value));
            assert_eq!(key, format!("{dir}/H.parquet"), "object key for {value:?}");
            let logged = commit::delta_log_path(&key);
            assert_eq!(
                logged,
                format!("{log_dir}/H.parquet"),
                "add.path for {value:?}"
            );
            assert_eq!(
                discover::canonical_key(&logged, "b", "tbl").as_deref(),
                Some(format!("tbl/{key}").as_str()),
                "canonical_key(add.path) is the object key for {value:?}"
            );
        }
        // An ordinary key is the same in all three forms: logs written before
        // this change still match.
        let plain = "region=eu-1_x.y/0123abcd.parquet";
        assert_eq!(commit::delta_log_path(plain), plain);
    }

    /// The object key of every ASCII value equals the key the earlier writer
    /// stored through `Path::from(raw)`, except for the four characters
    /// Spark escapes and `Path::from` did not (`'` `:` `=` and `/`, which
    /// `Path::from` split into a directory). Those four move to a new key
    /// and the transition commit removes the old one, so no object is left
    /// behind still referenced under a key the log no longer names.
    #[test]
    fn object_keys_match_the_earlier_path_from_keys() {
        for b in 1u8..0x80 {
            let c = b as char;
            if matches!(c, '\'' | ':' | '=' | '/') {
                continue;
            }
            let value = format!("a{c}b");
            let old = Path::from(format!("t/region={value}/H.parquet")).to_string();
            let new = object_path(
                "t",
                &format!("{}/H.parquet", commit::partition_dir("region", &value)),
            )
            .unwrap()
            .to_string();
            assert_eq!(new, old, "object key for byte {b:#04x}");
        }
    }

    /// A table the earlier writer built: object keys from `Path::from(raw)`,
    /// `add.path` the raw `region=<value>/…` string.
    ///
    /// ```text
    ///   v1  legacy adds (raw add.path)
    ///   v2  transition: remove every legacy spelling, add the encoded one
    ///   --  third run: no commit
    /// ```
    #[tokio::test]
    async fn a_legacy_raw_log_makes_one_transition_commit_and_keeps_shared_objects() {
        use std::collections::{BTreeMap, BTreeSet};
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_partitioned_bootstrap(&store, "ptbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "ptbl");
        let state = writer.discover().await.unwrap();
        let values = ["50%off", "x#y", "2024-01-01 00:00:00", "a<b", "eu"];
        let pv = |r: &str| HashMap::from([("region".to_string(), r.to_string())]);
        let groups = || -> Vec<_> {
            values
                .iter()
                .map(|v| (pv(v), make_partitioned_batch(v, 2)))
                .collect()
        };

        // v1, as the pre-fix writer wrote it.
        let mut legacy_lines = vec![serde_json::json!({"commitInfo": {}})];
        let mut legacy_keys = BTreeMap::new();
        for (p, batch) in groups() {
            let bytes = parquet_builder::build_parquet(&batch, &state).unwrap();
            let hash = blake3::hash(&bytes).to_hex().to_string();
            let raw = format!("region={}/{hash}.parquet", p["region"]);
            let key = Path::from(format!("ptbl/{raw}"));
            store
                .put(&key, PutPayload::from(Bytes::from(bytes.clone())))
                .await
                .unwrap();
            let mut add = commit::build_add_action(&commit::AddInputs {
                batch: &batch,
                state: &state,
                add_file_path: &raw,
                file_size: bytes.len() as u64,
                modification_time_millis: 0,
                partition_values: &p,
            })
            .unwrap();
            add.insert("path".into(), Value::from(raw.clone()));
            legacy_lines.push(serde_json::json!({ "add": add }));
            legacy_keys.insert(p["region"].clone(), (raw, key.to_string()));
        }
        put_commit(&store, "ptbl", 1, &legacy_lines).await;

        // Run 1 of the new writer: one transition commit.
        let run = writer.write_partitioned_batches(groups()).await.unwrap();
        assert_eq!(run.table_version, 2);
        assert!(!commit_exists(&store, "ptbl", 3).await, "one commit");
        let lines = commit_lines(&store, "ptbl", 2).await;
        let canon = |key: &str, action: &Value| {
            discover::canonical_key(action["path"].as_str().unwrap(), "", "ptbl")
                .unwrap_or_else(|| panic!("{key} path does not resolve"))
        };
        let removed: BTreeSet<String> = actions(&lines, "remove")
            .into_iter()
            .map(|r| canon("remove", r))
            .collect();
        let added: BTreeSet<String> = actions(&lines, "add")
            .into_iter()
            .map(|a| canon("add", a))
            .collect();
        assert!(removed.is_disjoint(&added), "{removed:?} vs {added:?}");
        // `eu` is spelled the same both ways: neither removed nor re-added.
        assert_eq!(removed.len(), 4, "{removed:?}");
        assert_eq!(added.len(), 4, "{added:?}");
        let removed_raw: BTreeSet<&str> = actions(&lines, "remove")
            .into_iter()
            .map(|r| r["path"].as_str().unwrap())
            .collect();
        for v in ["50%off", "x#y", "2024-01-01 00:00:00", "a<b"] {
            assert!(removed_raw.contains(legacy_keys[v].0.as_str()), "{v}");
        }

        // Every live file resolves to an object that exists. For `%`, `#`
        // and `<` the new add names the SAME object the legacy add stored,
        // so that object stays referenced. The timestamp's `:` now escapes,
        // so it gets a new object and the old one is removed in the log.
        let keys: BTreeSet<String> = parquet_keys(&store, "ptbl").await.into_iter().collect();
        let live = discover::read_live_set(&*store, "ptbl", "c.s.t", "")
            .await
            .unwrap();
        for k in live.files.keys() {
            assert!(keys.contains(k), "live {k} has no object");
        }
        for v in ["50%off", "x#y", "a<b", "eu"] {
            assert!(
                live.files.contains_key(&legacy_keys[v].1),
                "{v} object still live"
            );
        }
        let ts_old = &legacy_keys["2024-01-01 00:00:00"].1;
        assert!(!live.files.contains_key(ts_old));
        assert!(
            removed.contains(ts_old),
            "the old timestamp object is removed in the log"
        );

        // Run 2: stable, no commit.
        let again = writer.write_partitioned_batches(groups()).await.unwrap();
        assert!(!again.committed);
        assert!(!commit_exists(&store, "ptbl", 3).await);
    }

    #[tokio::test]
    async fn special_partition_values_round_trip_and_move_only_changed_files() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_partitioned_bootstrap(&store, "ptbl").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "ptbl");
        let pv = |r: &str| HashMap::from([("region".to_string(), r.to_string())]);
        let groups = |hash_rows: usize| {
            vec![
                (pv("50%off"), make_partitioned_batch("50%off", 2)),
                (pv("a b"), make_partitioned_batch("a b", 3)),
                (pv("x#y"), make_partitioned_batch("x#y", hash_rows)),
            ]
        };

        // Run 1: the object keys are Spark-escaped, the add paths encoded.
        let run1 = writer.write_partitioned_batches(groups(1)).await.unwrap();
        assert_eq!(run1.table_version, 1);
        let keys = parquet_keys(&store, "ptbl").await;
        let file_paths: Vec<String> = {
            let mut v: Vec<String> = run1.files.iter().map(|f| f.file_path.clone()).collect();
            v.sort();
            v
        };
        assert_eq!(keys, file_paths, "file_path names the stored object");
        let lines = commit_lines(&store, "ptbl", 1).await;
        let mut added: Vec<&str> = actions(&lines, "add")
            .iter()
            .map(|a| a["path"].as_str().unwrap())
            .collect();
        added.sort();
        let dirs: Vec<&str> = added.iter().map(|p| p.split('/').next().unwrap()).collect();
        assert_eq!(
            dirs,
            vec!["region=50%2525off", "region=a%20b", "region=x%2523y"]
        );
        let mut canon: Vec<String> = added
            .iter()
            .map(|p| discover::canonical_key(p, "", "ptbl").unwrap())
            .collect();
        canon.sort();
        assert_eq!(canon, keys, "canonical_key(add.path) == the object key");

        // Run 2: the same output finds every file live and writes no commit.
        let run2 = writer.write_partitioned_batches(groups(1)).await.unwrap();
        assert!(!run2.committed, "unchanged special-value files still match");
        assert!(!commit_exists(&store, "ptbl", 2).await);

        // Run 3: only `x#y` changes → one remove of its old file, one add.
        let x_old = run1
            .files
            .iter()
            .find(|f| f.file_path.contains("x%23y"))
            .unwrap()
            .file_path
            .clone();
        let run3 = writer.write_partitioned_batches(groups(4)).await.unwrap();
        assert_eq!(run3.table_version, 2);
        let lines = commit_lines(&store, "ptbl", 2).await;
        let removed: Vec<&str> = actions(&lines, "remove")
            .iter()
            .map(|r| r["path"].as_str().unwrap())
            .collect();
        let added: Vec<&str> = actions(&lines, "add")
            .iter()
            .map(|a| a["path"].as_str().unwrap())
            .collect();
        assert_eq!(removed.len(), 1, "{removed:?}");
        assert_eq!(
            discover::canonical_key(removed[0], "", "ptbl").unwrap(),
            x_old,
            "the remove names the old x#y object"
        );
        assert_eq!(added.len(), 1, "{added:?}");
        assert!(added[0].starts_with("region=x%2523y/"));
        let live = replay_live_paths(&store, "ptbl").await;
        assert_eq!(live.len(), 3);
    }

    // -- row ids come from the same replay as the head ------------------------

    /// A competitor commit at `version` that adds one rowTracking file and
    /// bumps `rowIdHighWaterMark` to `hwm`.
    async fn put_row_tracking_competitor(store: &InMemory, prefix: &str, version: u64, hwm: u64) {
        put_commit(
            store,
            prefix,
            version,
            &[
                serde_json::json!({"commitInfo": {}}),
                serde_json::json!({"add": {
                    "path": "competitor.parquet", "partitionValues": {}, "size": 9,
                    "modificationTime": 0, "dataChange": true,
                    "baseRowId": 0, "defaultRowCommitVersion": version
                }}),
                serde_json::json!({"domainMetadata": {
                    "domain": "delta.rowTracking",
                    "configuration": format!("{{\"rowIdHighWaterMark\":{hwm}}}"),
                    "removed": false
                }}),
            ],
        )
        .await;
    }

    /// The `baseRowId` of the only add, and the high-water mark, of `version`.
    async fn row_ids_of(store: &InMemory, prefix: &str, version: u64) -> (u64, u64) {
        let lines = commit_lines(store, prefix, version).await;
        let adds = actions(&lines, "add");
        assert_eq!(adds.len(), 1, "one add in commit {version}");
        let base = adds[0]["baseRowId"].as_u64().unwrap();
        let dm = actions(&lines, "domainMetadata");
        let cfg: Value = serde_json::from_str(dm[0]["configuration"].as_str().unwrap()).unwrap();
        (base, cfg["rowIdHighWaterMark"].as_u64().unwrap())
    }

    /// ```text
    ///   discover()          next row id 0, head 0
    ///   competitor v1       rowIdHighWaterMark 99
    ///   replace             reads head 1 ──▶ commits v2, baseRowId 100
    /// ```
    #[tokio::test]
    async fn a_commit_after_discover_moves_the_row_id_range_above_its_mark() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_row_tracking_bootstrap(&store, "rt").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "rt");
        let state = writer.discover().await.unwrap();
        assert_eq!(state.row_tracking_next_id, 0);

        put_row_tracking_competitor(&store, "rt", 1, 99).await;

        let result = writer
            .write_batch_with_state(make_batch(5), state)
            .await
            .unwrap();
        assert_eq!(result.commit_version, 2);
        assert_eq!(
            row_ids_of(&store, "rt", 2).await,
            (100, 104),
            "row ids start above the competitor's mark, not at the stale 0"
        );
    }

    /// ```text
    ///   live set read at head 0 (stale)
    ///   competitor v1       rowIdHighWaterMark 99
    ///   commit v1 ──▶ 412 ──▶ re-read ──▶ commits v2, baseRowId 100
    /// ```
    #[tokio::test]
    async fn a_conflict_retry_allocates_row_ids_from_the_new_head() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_row_tracking_bootstrap(&store, "rt").await;
        let writer = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "rt");
        let state = writer.discover().await.unwrap();
        let stale = discover::read_live_set(&*store, "rt", "c.s.t", "")
            .await
            .unwrap();
        assert_eq!(stale.head_version, 0);

        put_row_tracking_competitor(&store, "rt", 1, 99).await;

        let batch = make_batch(3);
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
            num_records: 3,
            size_bytes: 11,
        };
        let outcome = writer
            .commit_replace(vec![staged], stale, state, 0, ReplaceMode::Build)
            .await
            .unwrap();
        assert_eq!(outcome.table_version, 2, "the 412 at v1 retried at v2");
        assert_eq!(row_ids_of(&store, "rt", 2).await, (100, 102));
    }

    #[test]
    fn the_live_set_takes_the_latest_row_tracking_mark() {
        // Same rule as `discover_row_tracking_next_id`: none → 0, a negative
        // mark → 0, otherwise mark + 1; a malformed mark is an error.
        let dm = |cfg: &str| {
            Some(serde_json::json!({"domain": "delta.rowTracking", "configuration": cfg}))
        };
        let mut live = discover::LiveSet {
            head_version: 0,
            files: Default::default(),
            append_only: false,
            protocol: Value::Null,
            metadata: Value::Null,
            row_tracking_domain: None,
            ever_added: Default::default(),
            shape_version: 0,
        };
        assert_eq!(live.row_tracking_next_id().unwrap(), 0);
        live.row_tracking_domain = dm(r#"{"rowIdHighWaterMark":-1}"#);
        assert_eq!(live.row_tracking_next_id().unwrap(), 0);
        live.row_tracking_domain = dm(r#"{"rowIdHighWaterMark":41}"#);
        assert_eq!(live.row_tracking_next_id().unwrap(), 42);
        live.row_tracking_domain = dm(r#"{}"#);
        assert!(live.row_tracking_next_id().is_err());
    }

    /// Not a CI test. Writes two replace scenarios to the directory in
    /// `ROCKY_DELTA_EXPORT_DIR` so an outside Delta reader (delta-rs) can
    /// check them. See the RV1-P1a experiment record in rocky-plans.
    #[tokio::test]
    #[ignore]
    async fn export_replace_tables_for_an_external_reader() {
        let Ok(dir) = std::env::var("ROCKY_DELTA_EXPORT_DIR") else {
            eprintln!("skipping: ROCKY_DELTA_EXPORT_DIR not set");
            return;
        };
        use futures::TryStreamExt;
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "unpart").await;
        let w = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "unpart");
        w.write_batch(make_batch(4)).await.unwrap(); // v1: ids 0..3
        w.write_batch(make_batch(6)).await.unwrap(); // v2: ids 0..5
        w.write_batch(make_batch(6)).await.unwrap(); // no-op

        seed_partitioned_bootstrap(&store, "part").await;
        let w = make_unpartitioned_writer(store.clone() as Arc<dyn ObjectStore>, "part");
        let pv = |r: &str| HashMap::from([("region".to_string(), r.to_string())]);
        w.write_partitioned_batches(vec![
            (pv("eu"), make_partitioned_batch("eu", 2)),
            (pv("us"), make_partitioned_batch("us", 3)),
            (pv("ap"), make_partitioned_batch("ap", 1)),
        ])
        .await
        .unwrap(); // v1: eu 2, us 3, ap 1
        w.write_partitioned_batches(vec![
            (pv("eu"), make_partitioned_batch("eu", 4)),
            (pv("us"), make_partitioned_batch("us", 3)),
        ])
        .await
        .unwrap(); // v2: eu 4, us 3

        let mut stream = store.list(None);
        while let Some(meta) = stream.try_next().await.unwrap() {
            let bytes = store
                .get(&meta.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let out = std::path::Path::new(&dir).join(meta.location.as_ref());
            std::fs::create_dir_all(out.parent().unwrap()).unwrap();
            std::fs::write(out, &bytes).unwrap();
        }
    }

    // -- table publish (RV1-P3) ----------------------------------------------

    fn table_writer(store: &Arc<InMemory>, prefix: &str, sql: Arc<dyn SqlClient>) -> UniformWriter {
        UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: prefix.into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            store.clone() as Arc<dyn ObjectStore>,
            sql,
        )
    }

    /// Two builds, `A` then `B`. Returns their file hashes.
    async fn two_builds(writer: &UniformWriter) -> (String, String) {
        let a = writer.write_batch(make_batch(10)).await.unwrap();
        let b = writer.write_batch(make_batch(20)).await.unwrap();
        assert_eq!((a.table_version, b.table_version), (1, 2));
        (a.blake3_hash, b.blake3_hash)
    }

    async fn object_count(store: &InMemory) -> usize {
        use futures::TryStreamExt;
        store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .len()
    }

    /// Publishing the earlier output `A` writes one commit that removes `B`
    /// and adds `A` again, with A's own `add` (no byte copied). Publishing
    /// it a second time writes nothing.
    #[tokio::test]
    async fn restore_makes_an_earlier_output_live_in_one_commit() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, b) = two_builds(&writer).await;
        let objects_before = object_count(&store).await;

        let state = writer.discover().await.unwrap();
        let out = writer
            .restore_content_addressed(std::slice::from_ref(&a), state)
            .await
            .unwrap();
        assert!(out.committed);
        assert_eq!(out.table_version, 3);
        assert_eq!(out.removed_paths, vec![format!("{b}.parquet")]);
        assert_eq!(
            replay_live_paths(&store, "tbl").await,
            std::collections::BTreeSet::from([format!("{a}.parquet")])
        );
        let lines = commit_lines(&store, "tbl", 3).await;
        let add = lines.iter().find_map(|l| l.get("add")).unwrap();
        let first = commit_lines(&store, "tbl", 1).await;
        let first_add = first.iter().find_map(|l| l.get("add")).unwrap();
        assert_eq!(add["stats"], first_add["stats"], "A's stats are lifted");
        assert_eq!(add["size"], first_add["size"]);
        // Only one new object: the commit. No parquet was written.
        assert_eq!(object_count(&store).await, objects_before + 1);

        let state = writer.discover().await.unwrap();
        let again = writer
            .restore_content_addressed(std::slice::from_ref(&a), state)
            .await
            .unwrap();
        assert!(!again.committed);
        assert_eq!(again.table_version, 3);
        assert!(!commit_exists(&store, "tbl", 4).await);
    }

    /// The bytes of `A` are gone (a VACUUM after the replace): the publish
    /// refuses and writes no commit. An unknown hash refuses the same way.
    #[tokio::test]
    async fn restore_refuses_a_version_whose_bytes_are_gone_or_never_existed() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, _) = two_builds(&writer).await;
        store
            .delete(&object_store::path::Path::from(format!("tbl/{a}.parquet")))
            .await
            .unwrap();

        let state = writer.discover().await.unwrap();
        let err = writer
            .restore_content_addressed(std::slice::from_ref(&a), state)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, UniformWriterError::PublishSourceUnavailable { detail, .. } if detail.contains("are gone")),
            "{err}"
        );
        let state = writer.discover().await.unwrap();
        let err = writer
            .restore_content_addressed(&["f".repeat(64)], state)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, UniformWriterError::PublishSourceUnavailable { detail, .. } if detail.contains("no commit")),
            "{err}"
        );
        assert!(!commit_exists(&store, "tbl", 3).await);
    }

    /// A file written before a schema change is not published: its parquet
    /// and stats follow the old schema.
    #[tokio::test]
    async fn restore_refuses_a_file_written_before_a_schema_change() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, b) = two_builds(&writer).await;
        // Commit 3 changes the schema (a column becomes nullable) and keeps
        // B live.
        let v0 = commit_lines(&store, "tbl", 0).await;
        let mut meta = v0
            .iter()
            .find(|l| l.get("metaData").is_some())
            .unwrap()
            .clone();
        let schema = meta["metaData"]["schemaString"].as_str().unwrap().replacen(
            "\"nullable\":false",
            "\"nullable\":true",
            1,
        );
        meta["metaData"]["schemaString"] = Value::String(schema);
        let body = format!("{}\n", serde_json::to_string(&meta).unwrap());
        store
            .put(
                &object_store::path::Path::from("tbl/_delta_log/00000000000000000003.json"),
                PutPayload::from(Bytes::from(body.into_bytes())),
            )
            .await
            .unwrap();

        let state = writer.discover().await.unwrap();
        let err = writer
            .restore_content_addressed(std::slice::from_ref(&a), state)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, UniformWriterError::PublishSourceUnavailable { detail, .. } if detail.contains("schema")),
            "{err}"
        );
        assert!(!commit_exists(&store, "tbl", 4).await);
        // B was added before the change too: refused as well.
        let state = writer.discover().await.unwrap();
        assert!(writer.restore_content_addressed(&[b], state).await.is_err());
    }

    /// Restoring `a` is refused as a publish source, the table still
    /// discovers, and no commit is written at `next`.
    async fn assert_restore_refused(writer: &UniformWriter, store: &InMemory, a: &str, next: u64) {
        let state = writer
            .discover()
            .await
            .expect("the table itself is still writable");
        let err = writer
            .restore_content_addressed(&[a.to_string()], state)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, UniformWriterError::PublishSourceUnavailable { .. }),
            "{err}"
        );
        assert!(!commit_exists(store, "tbl", next).await);
    }

    /// A file written before a protocol change is not published: the new
    /// protocol can change how its bytes must be read.
    #[tokio::test]
    async fn restore_refuses_a_file_written_before_a_protocol_change() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, _) = two_builds(&writer).await;
        put_commit(
            &store,
            "tbl",
            3,
            &[serde_json::json!({"protocol": {
                "minReaderVersion": 2,
                "minWriterVersion": 7,
                "writerFeatures": ["columnMapping", "icebergCompatV2", "appendOnly"],
            }})],
        )
        .await;
        assert_restore_refused(&writer, &store, &a, 4).await;
    }

    /// A file written before a `delta.columnMapping.mode` change is not
    /// published, even when the schema string is unchanged.
    #[tokio::test]
    async fn restore_refuses_a_file_written_before_a_column_mapping_mode_change() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, _) = two_builds(&writer).await;
        let mut meta = commit_lines(&store, "tbl", 0)
            .await
            .into_iter()
            .find(|l| l.get("metaData").is_some())
            .unwrap();
        meta["metaData"]["configuration"]["delta.columnMapping.mode"] = Value::from("id");
        put_commit(&store, "tbl", 3, &[meta]).await;
        assert_restore_refused(&writer, &store, &a, 4).await;
    }

    /// An `add` of the target file that carries a deletion vector is not
    /// lifted: the vector would hide rows the recorded output had.
    #[tokio::test]
    async fn restore_refuses_an_add_that_carries_a_deletion_vector() {
        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let writer = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, _) = two_builds(&writer).await;
        // Commit 3 adds A again with a deletion vector; commit 4 removes it.
        let mut add = commit_lines(&store, "tbl", 1)
            .await
            .into_iter()
            .find(|l| l.get("add").is_some())
            .unwrap();
        add["add"]["deletionVector"] = serde_json::json!({
            "storageType": "u", "pathOrInlineDv": "ab", "offset": 1,
            "sizeInBytes": 36, "cardinality": 2
        });
        let path = add["add"]["path"].clone();
        put_commit(&store, "tbl", 3, &[add]).await;
        put_commit(
            &store,
            "tbl",
            4,
            &[serde_json::json!({"remove": {"path": path, "dataChange": true}})],
        )
        .await;
        assert_restore_refused(&writer, &store, &a, 5).await;
    }

    fn content_addressed(hash: &str, version: u64) -> rocky_core::state::OutputVersion {
        rocky_core::state::OutputVersion::content_addressed(
            "c.s.t".into(),
            vec![version],
            vec![hash.to_string()],
            false,
        )
    }

    /// Record two runs of `orders` (outputs `a` then `b`) in a fresh local
    /// state store and publish the first one to `prod` through `writer`.
    async fn publish_first_build(
        writer: UniformWriter,
        a: &str,
        b: &str,
    ) -> rocky_core::table_publish::TablePublishReport {
        use rocky_core::environments::{EnvironmentName, PublishRequest, PublishSource};
        use rocky_core::state::StateStore;
        use rocky_core::table_publish::publish_tables;

        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join(".rocky-state.redb");
        let state = StateStore::open(&path).unwrap();
        state
            .record_run(&rocky_core::state::run_with_output_versions(
                "r1",
                &[("orders", Some(content_addressed(a, 1)))],
            ))
            .unwrap();
        state
            .record_run(&rocky_core::state::run_with_output_versions(
                "r2",
                &[("orders", Some(content_addressed(b, 2)))],
            ))
            .unwrap();
        drop(state);
        let session = rocky_core::state_sync::LedgerSeamSession::new(
            &rocky_core::config::StateConfig::default(),
            &path,
            false,
        );
        let publisher = publish::DeltaTablePublisher::new().with_table(writer);
        let request = PublishRequest {
            environment: EnvironmentName::parse("prod").unwrap(),
            expected_head: None,
            sources: vec![PublishSource {
                model: "orders".into(),
                run_id: "r1".into(),
            }],
            principal: rocky_core::config::PrincipalRef::unnamed(),
            plan_id: None,
        };
        publish_tables(&session, &request, &publisher, Default::default())
            .await
            .unwrap()
    }

    /// End to end on the in-memory store and a local state store: publish
    /// the earlier run's output through `rocky_core::table_publish`. One
    /// Delta commit moves the table, the Iceberg sync runs once, and the
    /// publish history records the move with its commit version.
    #[tokio::test]
    async fn a_table_publish_moves_the_delta_table_and_records_the_commit() {
        use rocky_core::environments::TableMoveOutcome;

        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let sql = Arc::new(RecordingSqlClient::default());
        let writer = table_writer(&store, "tbl", sql.clone());
        let (a, b) = two_builds(&writer).await;
        let report = publish_first_build(writer, &a, &b).await;

        assert!(report.is_complete());
        assert_eq!(
            report.moves()[0].outcome,
            TableMoveOutcome::Moved {
                table: "c.s.t".into(),
                table_version: 3,
            }
        );
        assert_eq!(
            replay_live_paths(&store, "tbl").await,
            std::collections::BTreeSet::from([format!("{a}.parquet")])
        );
        assert_eq!(
            *sql.log.lock().unwrap(),
            vec!["MSCK REPAIR TABLE c.s.t SYNC METADATA".to_string()]
        );
    }

    /// A SQL client whose first statement fails. It records every one.
    #[derive(Default)]
    struct FailFirstSqlClient {
        log: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl SqlClient for FailFirstSqlClient {
        async fn execute(&self, sql: &str) -> Result<()> {
            let mut log = self.log.lock().unwrap();
            log.push(sql.to_string());
            if log.len() == 1 {
                return Err(UniformWriterError::DeltaLog("warehouse unavailable".into()));
            }
            Ok(())
        }
    }

    /// The Iceberg sync fails after the commit lands: the move is
    /// `sync_failed`, not `moved`. A retry finds the table already current
    /// and runs the sync again, which now succeeds.
    #[tokio::test]
    async fn a_failed_iceberg_sync_is_its_own_outcome_and_a_retry_runs_it_again() {
        use rocky_core::environments::{EnvPointer, PointerVersion};
        use rocky_core::table_publish::{TableMoved, TablePointerBackend};

        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let sql = Arc::new(FailFirstSqlClient::default());
        let writer = table_writer(&store, "tbl", sql.clone());
        let (a, _) = two_builds(&writer).await;
        let publisher = publish::DeltaTablePublisher::new().with_table(writer);
        let pointer = EnvPointer {
            model: "orders".into(),
            run_id: "r1".into(),
            version: PointerVersion::Known(content_addressed(&a, 1)),
        };

        let first = publisher.move_table(&pointer).await.unwrap();
        assert!(
            matches!(
                &first,
                TableMoved::SyncFailed { table_version: 3, committed: true, error, .. }
                    if error.contains("Iceberg metadata sync failed")
            ),
            "{first:?}"
        );
        let retry = publisher.move_table(&pointer).await.unwrap();
        assert_eq!(
            retry,
            TableMoved::AlreadyCurrent {
                table: "c.s.t".into(),
                table_version: 3
            }
        );
        assert_eq!(sql.log.lock().unwrap().len(), 2, "the retry synced again");
    }

    /// What [`Interposed`] does to one `_delta_log` create.
    #[derive(Debug)]
    enum Interpose {
        /// Land `body` at `version` first, as a concurrent writer would, so
        /// the create loses.
        CompetitorFirst { version: u64, body: String },
        /// Store the create at `version`, then return a transport error, as
        /// a timeout after the request reached the store would.
        LandThenFail { version: u64 },
    }

    impl Interpose {
        fn version(&self) -> u64 {
            match self {
                Self::CompetitorFirst { version, .. } | Self::LandThenFail { version } => *version,
            }
        }
    }

    /// An in-memory store that interposes once on a `_delta_log` create.
    #[derive(Debug)]
    struct Interposed {
        inner: Arc<InMemory>,
        prefix: String,
        once: std::sync::Mutex<Option<Interpose>>,
    }

    impl std::fmt::Display for Interposed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Interposed({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for Interposed {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            let hit = {
                let mut once = self.once.lock().unwrap();
                let target = once.as_ref().map(|i| {
                    Path::from(format!(
                        "{}/_delta_log/{:020}.json",
                        self.prefix,
                        i.version()
                    ))
                });
                if target.as_ref() == Some(location) {
                    once.take()
                } else {
                    None
                }
            };
            match hit {
                Some(Interpose::CompetitorFirst { body, .. }) => {
                    self.inner
                        .put(location, PutPayload::from(Bytes::from(body.into_bytes())))
                        .await?;
                    self.inner.put_opts(location, payload, opts).await
                }
                Some(Interpose::LandThenFail { .. }) => {
                    self.inner.put_opts(location, payload, opts).await?;
                    Err(object_store::Error::Generic {
                        store: "Interposed",
                        source: "operation timed out".into(),
                    })
                }
                None => self.inner.put_opts(location, payload, opts).await,
            }
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(
            &self,
            location: &Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }
        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<'static, object_store::Result<Path>>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }
        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// A writer for table `c.s.t` under `tbl` whose store interposes once.
    fn interposed_writer(store: &Arc<InMemory>, interpose: Interpose) -> UniformWriter {
        UniformWriter::new(
            UniformWriterConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: "t".into(),
                prefix: "tbl".into(),
                engine_info: "rocky-iceberg/test".into(),
            },
            Arc::new(Interposed {
                inner: store.clone(),
                prefix: "tbl".into(),
                once: std::sync::Mutex::new(Some(interpose)),
            }) as Arc<dyn ObjectStore>,
            Arc::new(RecordingSqlClient::default()),
        )
    }

    /// A concurrent append lands at the version the publish's commit
    /// targets, after the publish read the live set. The publish must not
    /// recompute its removes against the new head (that would remove the
    /// append): it fails for the table, and the append stays live.
    #[tokio::test]
    async fn a_publish_that_loses_to_a_concurrent_append_fails_and_keeps_the_append() {
        use rocky_core::environments::TableMoveOutcome;

        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let (a, b) = two_builds(&table_writer(&store, "tbl", Arc::new(PanicSqlClient))).await;
        let append = concat!(
            "{\"commitInfo\":{}}\n",
            "{\"add\":{\"path\":\"appended.parquet\",\"partitionValues\":{},",
            "\"size\":7,\"modificationTime\":0,\"dataChange\":true}}\n"
        )
        .to_string();
        let writer = interposed_writer(
            &store,
            Interpose::CompetitorFirst {
                version: 3,
                body: append,
            },
        );
        let report = publish_first_build(writer, &a, &b).await;

        assert!(!report.is_complete());
        assert!(
            matches!(
                &report.moves()[0].outcome,
                TableMoveOutcome::Failed { error } if error.contains("concurrent commit")
            ),
            "{:?}",
            report.moves()
        );
        assert_eq!(
            replay_live_paths(&store, "tbl").await,
            std::collections::BTreeSet::from([
                format!("{b}.parquet"),
                "appended.parquet".to_string()
            ]),
            "the concurrent append survives and nothing else changed"
        );
        assert!(!commit_exists(&store, "tbl", 4).await);
    }

    /// The commit PUT stores the object, then returns a transport error. The
    /// publish records the move `unknown`, not `failed`, and a second
    /// publish finds the table already serving the version.
    #[tokio::test]
    async fn a_publish_whose_commit_put_errors_after_landing_is_recorded_unknown() {
        use rocky_core::environments::TableMoveOutcome;

        let store: Arc<InMemory> = Arc::new(InMemory::new());
        seed_bootstrap(&store, "tbl").await;
        let plain = table_writer(&store, "tbl", Arc::new(PanicSqlClient));
        let (a, b) = two_builds(&plain).await;
        let writer = interposed_writer(&store, Interpose::LandThenFail { version: 3 });
        let report = publish_first_build(writer, &a, &b).await;

        assert!(
            matches!(
                &report.moves()[0].outcome,
                TableMoveOutcome::Unknown { error } if error.contains("may have landed")
            ),
            "{:?}",
            report.moves()
        );
        assert!(commit_exists(&store, "tbl", 3).await, "the commit landed");
        let state = plain.discover().await.unwrap();
        let again = plain
            .restore_content_addressed(std::slice::from_ref(&a), state)
            .await
            .unwrap();
        assert!(!again.committed, "a retry finds the table current");
        assert_eq!(again.table_version, 3);
    }

    /// The backend refuses, before anything is written, a version it cannot
    /// publish: a partitioned output, a `delta_observed` version, and a
    /// table with no writer.
    #[test]
    fn the_delta_publisher_refuses_what_it_cannot_publish() {
        use rocky_core::environments::{EnvPointer, PointerVersion};
        use rocky_core::state::OutputVersion;
        use rocky_core::table_publish::TablePointerBackend;

        let store: Arc<InMemory> = Arc::new(InMemory::new());
        let publisher = publish::DeltaTablePublisher::new().with_table(table_writer(
            &store,
            "tbl",
            Arc::new(PanicSqlClient),
        ));
        let pointer = |v: OutputVersion| EnvPointer {
            model: "orders".into(),
            run_id: "r1".into(),
            version: PointerVersion::Known(v),
        };
        let h = "a".repeat(64);
        assert!(publisher.check(&pointer(content_addressed(&h, 1))).is_ok());
        let partitioned =
            OutputVersion::content_addressed("c.s.t".into(), vec![1], vec![h.clone()], true);
        let err = publisher.check(&pointer(partitioned)).unwrap_err();
        assert!(err.contains("partitioned"), "{err}");
        let observed = OutputVersion::DeltaObserved {
            table: "c.s.t".into(),
            version: 1,
        };
        let err = publisher.check(&pointer(observed)).unwrap_err();
        assert!(err.contains("delta_observed"), "{err}");
        let other = OutputVersion::content_addressed("c.s.other".into(), vec![1], vec![h], false);
        let err = publisher.check(&pointer(other)).unwrap_err();
        assert!(err.contains("no Delta table writer"), "{err}");
    }
}
