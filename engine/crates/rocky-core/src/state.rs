use std::fs::File;
use std::path::Path;

use fs4::FileExt;
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::config::SchemaMismatchPolicy;
use crate::schema_cache::SchemaCacheEntry;
use rocky_ir::WatermarkState;

const WATERMARKS: TableDefinition<&str, &[u8]> = TableDefinition::new("watermarks");
/// Per-table opaque source change-markers recorded after each successful
/// replication copy. The `prune_unchanged` skip-unchanged pruning compares the
/// live [`WarehouseAdapter::source_change_marker`] against the recorded value
/// to decide whether a source is unchanged since the last copy. Synced to
/// remote state like [`WATERMARKS`] so a distributed run reads a consistent
/// last-copied marker.
const SOURCE_MARKERS: TableDefinition<&str, &str> = TableDefinition::new("source_markers");
const CHECK_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("check_history");
const RUN_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("run_history");
const QUALITY_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("quality_history");
const DAG_SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("dag_snapshots");
/// Per-run progress *header* (run_id / started_at / total_tables).
///
/// Key: `run_id`. Value: serialized [`RunProgress`] whose `tables` vector is
/// left empty by the current write path — the per-table entries live in
/// [`RUN_PROGRESS_ENTRIES`] so each completed table is an O(1) blind insert
/// rather than a read-modify-write of one growing blob. Reads
/// ([`StateStore::get_run_progress`] / [`StateStore::get_latest_run_progress`])
/// stitch the header together with its entries. Databases written by a
/// pre-v8 binary still carry their `tables` inline; the readers fall back to
/// that blob when the per-entry scan is empty, so old runs keep resuming.
const RUN_PROGRESS: TableDefinition<&str, &[u8]> = TableDefinition::new("run_progress");
/// Per-table progress entries for a run.
///
/// Key format: `"{run_id}|{table_key}"` where `table_key` is the
/// fully-qualified `catalog.schema.table` — the same identity the resume
/// logic dedups on, so it is unique per (run, table). Value: serialized
/// [`TableProgress`]. Written as a blind insert (no read-modify-write), which
/// makes [`StateStore::record_table_progress`] O(1) per completed table and
/// removes the lost-update race the old whole-blob rewrite carried under
/// concurrent table tasks. Reads prefix-scan `"{run_id}|"` and sort the
/// recovered entries by [`TableProgress::index`] to restore execution order.
const RUN_PROGRESS_ENTRIES: TableDefinition<&str, &[u8]> =
    TableDefinition::new("run_progress_entries");
/// Per-partition state for `time_interval` materialization.
///
/// Key format: `"{model_name}|{partition_key}"` (e.g.
/// `"fct_daily_orders|2026-04-07"`). Value: serialized `PartitionRecord`.
/// Authoritative source for "did partition X compute"; consulted by
/// `--missing` discovery.
const PARTITIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("partitions");
/// Grace-period tracking for columns dropped from the source.
///
/// Key format: `"{table_key}|{column_name}"` (e.g.
/// `"acme_warehouse.staging.orders|old_col"`). Value: serialized
/// `GracePeriodRecord`. When a column reappears in the source the record
/// is deleted; when the grace period expires the column is dropped from
/// the target and the record is removed.
const GRACE_PERIODS: TableDefinition<&str, &[u8]> = TableDefinition::new("grace_periods");
/// Per-file load tracking for the `load` pipeline type.
///
/// Key format: `"{pipeline_name}|{file_path}"` (e.g.
/// `"ingest|data/orders.csv"`). Value: serialized [`LoadedFileRecord`].
/// Used for incremental loads: files already recorded with a matching hash
/// can be skipped.
const LOADED_FILES: TableDefinition<&str, &[u8]> = TableDefinition::new("loaded_files");
/// Named virtual branches (schema-prefix branches).
///
/// Key: branch name. Value: serialized [`BranchRecord`]. A branch records the
/// `schema_prefix` applied to every model target when `rocky run --branch <name>`
/// is invoked; it's the persistent, named analogue of the ephemeral `--shadow`
/// mode. Warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake
/// zero-copy `CLONE`) are a follow-up — the schema-prefix approach ships first
/// because it works uniformly across every adapter.
const BRANCHES: TableDefinition<&str, &[u8]> = TableDefinition::new("branches");
/// Cache of `DESCRIBE TABLE` results for source warehouses.
///
/// Key format: `"<catalog>.<schema>.<table>"` (lowercased). Value: serialized
/// [`crate::schema_cache::SchemaCacheEntry`]. Read by
/// `rocky_compiler::schema_cache::load_source_schemas_from_cache` to populate
/// `CompilerConfig.source_schemas` without a live round-trip. Written
/// opportunistically by `rocky run`'s drift/materialize paths and explicitly
/// by `rocky discover --with-schemas`.
///
/// Replicates as `replicate = false` by default in `state_sync.rs` so a fresh
/// clone doesn't inherit another machine's stale types — see design doc §5.7.
const SCHEMA_CACHE: TableDefinition<&str, &[u8]> = TableDefinition::new("schema_cache");
/// Idempotency-key stamps for `rocky run --idempotency-key` dedup.
///
/// Key: the caller-supplied idempotency key (opaque UTF-8 string; stored
/// verbatim — never hash, never truncate). Value: serialized
/// [`crate::idempotency::IdempotencyEntry`]. A new entry is inserted as
/// `InFlight` when a run claims the key, updated to `Succeeded` or `Failed`
/// at run end (see [`crate::config::DedupPolicy`]).
///
/// Replicates across backends by default — the table is intentionally NOT
/// in [`LOCAL_ONLY_TABLE_NAMES`] so tiered-backend snapshots surface the
/// same stamp to other pods after upload.
const IDEMPOTENCY_KEYS: TableDefinition<&str, &[u8]> = TableDefinition::new("idempotency_keys");
/// Per-write content-addressed artifact ledger.
///
/// Key format: `"{run_id}|{model_name}|{file_path}"`. Value: serialized
/// [`ArtifactRecord`]. One row per content-addressed parquet file written
/// (so a partitioned model produces one row per partition group, an
/// unpartitioned model produces one row per run).
///
/// Records the `blake3_hash` returned by
/// `rocky_iceberg::uniform_writer::WriteResult` so the Phase 6 VACUUM
/// refcount sweep can answer "which runs touched hash X" without
/// re-reading every Delta log. Today's call site is
/// `rocky_cli::commands::run_content_addressed::execute_content_addressed_model`'s
/// success branch; refcount semantics (decrement, is-unreferenced,
/// physical removal) land in the Phase 6 PR.
///
/// Replicates across backends by default — the table is intentionally
/// NOT in [`LOCAL_ONLY_TABLE_NAMES`] so tiered-backend snapshots surface
/// the artifact ledger to other pods. Recipe-as-canonical-versioning
/// treats the artifact ledger as global ground truth, not machine-local
/// scratch.
const OUTPUT_ARTIFACTS: TableDefinition<&str, &[u8]> = TableDefinition::new("output_artifacts");
/// Pre-execution-style input-match index for auditable reuse.
///
/// Key format: the model's `input_hash` (hex) — see
/// [`crate::reuse::compute_input_hash`]. Value: serialized
/// [`InputIndexEntry`], locating the prior run that produced those inputs.
///
/// The complement of [`OUTPUT_ARTIFACTS`]: that ledger is keyed by the
/// **output** blake3 and read after execution by GC; this index is keyed by
/// the **inputs** (logic key + upstream identities + target) and is the
/// surface a future reuse decision reads *before* execution. In Stage 1 it
/// is written on a successful run and read back only by tests — nothing
/// consults it for a reuse decision yet (the index is dormant).
///
/// Replicates across backends by default — like [`OUTPUT_ARTIFACTS`], the
/// reuse spine is global ground truth, not machine-local scratch.
const INPUT_INDEX: TableDefinition<&str, &[u8]> = TableDefinition::new("input_index");
/// Offline-verifiable provenance for each indexed model build.
///
/// Key format: `"{run_id}|{model_name}"`. Value: serialized
/// [`ProvenanceRecord`], embedding the model's canonical `ModelIr` JSON plus
/// the output blake3(s), recorded `skip_hash`, and `proof_class`. An auditor
/// with the state store and object store — and no trust in the runtime — can
/// later deserialize the embedded IR, recompute its `skip_hash`, and `b3sum`
/// the recorded parquet to verify the *input-logic match* and the
/// *byte-identity of the recorded bytes*. It does **not** attest that a fresh
/// re-run would reproduce them.
///
/// State-internal: never part of any `*Output` JSON schema, so it carries no
/// codegen. Replicates across backends by default.
const INPUT_PROVENANCE: TableDefinition<&str, &[u8]> = TableDefinition::new("input_provenance");

/// Prior `rocky discover` source inventory, keyed by pipeline name →
/// JSON-encoded sorted `Vec<String>` of discovered source schemas. Read and
/// rewritten each `discover` run when `report_new_sources` is set, so the next
/// run can diff "sources present now but absent before" into `new_sources`.
const DISCOVER_SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("discover_snapshots");
/// Append-only agent-policy decision ledger.
///
/// Key format: `"{timestamp_rfc3339}|{plan_id}|{model}"`. Value: serialized
/// [`PolicyDecisionRecord`]. One row per policy *evaluation* at a mutating
/// enforcement seam (`rocky apply` / promote) — reads are never recorded
/// (they short-circuit before evaluation). Queried by `rocky audit`.
///
/// Mirrors [`OUTPUT_ARTIFACTS`]: replicates across backends by default (it is
/// intentionally NOT in [`LOCAL_ONLY_TABLE_NAMES`]) so the audit trail is
/// global ground truth, not machine-local scratch. Remote keys are
/// schema-version-qualified by `state_sync`, so a v14 ledger never mixes with
/// another schema version's.
const POLICY_DECISIONS: TableDefinition<&str, &[u8]> = TableDefinition::new("policy_decisions");
/// Long-running-job records for the `rocky serve` HTTP job model
/// (`POST /api/v1/jobs/{run|plan|apply}`). Keyed by `job_id`; the value is a
/// serialized [`PersistedJob`]. Persisting job status here (rather than
/// in-memory only) is what lets a killed-and-restarted sidecar report honest
/// status for jobs it launched instead of a blank slate.
const JOBS: TableDefinition<&str, &[u8]> = TableDefinition::new("jobs");
/// Key/value store for internal metadata (e.g. `"schema_version"`).
const METADATA: TableDefinition<&str, &str> = TableDefinition::new("metadata");

/// Tables that should NOT be replicated by `state_sync` by default.
///
/// Exposed so `state_sync::upload_state` can filter them out of the remote
/// copy when the user has not opted in via `[cache.schemas] replicate = true`.
/// Kept alongside the table definitions so the replicate-posture decision
/// travels with the table name — adding a new local-only table is a
/// one-place edit instead of "grep the whole crate for replicate lists".
pub const LOCAL_ONLY_TABLE_NAMES: &[&str] = &["schema_cache"];

/// Schema version for the current set of state tables.
///
/// Increment this constant whenever a table is added, removed, or structurally
/// changed in a way that is incompatible with an older binary. Rocky will
/// refuse to open a database whose stored version exceeds this value.
///
/// # Version history
///
/// - **v5** — idempotency keys table; pre-audit `RunRecord` shape
///   (run_id / started_at / finished_at / status / models_executed /
///   trigger / config_hash).
/// - **v6** — adds the 8-field governance audit trail to
///   [`RunRecord`] (`triggering_identity`, `session_source`, `git_commit`,
///   `git_branch`, `idempotency_key`, `target_catalog`, `hostname`,
///   `rocky_version`). v5 records forward-deserialize as v6 via
///   `#[serde(default)]` on each new field — no in-place blob rewrite
///   is required. See the `RunRecord` docs for the default-value
///   contract (`hostname = "unknown"`, `rocky_version = "<pre-audit>"`,
///   `session_source = Cli`).
/// - **v7** — adds the [`OUTPUT_ARTIFACTS`] table for the content-addressed
///   write ledger (Arc 4 / Arc 1 wave 2 Phase 6 input). Pure additive
///   schema change: v6 databases auto-create the empty table on next
///   open and stamp themselves as v7. No blob migration needed; existing
///   tables are untouched.
/// - **v8** — adds the [`RUN_PROGRESS_ENTRIES`] table so each completed
///   table's progress is an O(1) blind insert under a per-entry key
///   instead of a read-modify-write of one growing blob. The legacy
///   [`RUN_PROGRESS`] table is retained as the per-run header (run_id /
///   started_at / total_tables); reads stitch the header together with
///   the per-entry rows. Pure additive schema change: v7 databases
///   auto-create the empty table on next open and stamp themselves as v8.
///   A v7-recorded run with entries still in the header blob continues to
///   resume — the readers fall back to the header's `tables` when the
///   per-entry scan is empty.
/// - **v9** — adds the [`INPUT_INDEX`] + [`INPUT_PROVENANCE`] tables for the
///   auditable-reuse input-match spine. Pure additive schema change: v8
///   databases auto-create both empty tables on next open and stamp
///   themselves as v9. No blob migration needed; existing tables are
///   untouched. Both tables are dormant in Stage 1 — populated on a
///   successful run when `[reuse]` is enabled, never read for a reuse
///   decision yet.
/// - **v10** — adds the [`SOURCE_MARKERS`] table for replication skip-unchanged
///   pruning (`prune_unchanged`): per-table source change-markers recorded
///   after each successful copy. Pure additive schema change: v9 databases
///   auto-create the empty table on next open and stamp themselves as v10. No
///   blob migration needed; existing tables are untouched. Empty until a
///   `prune_unchanged` pipeline runs, so pre-existing state resumes unchanged.
/// - **v11** — adds the recipe-identity triple
///   (`recipe_hash` / `input_hash` / `input_proof_class` / `env_hash` /
///   `hash_scheme`) to the [`ModelExecution`] blob. Pure additive blob-field
///   change: the new fields are `#[serde(default)]`, so a v10 `RunRecord`
///   forward-deserializes with them all `None` and a v11 blob back-reads
///   cleanly on a v10 binary (serde ignores the extra keys). No new tables, no
///   in-place migration. The version bump is deliberate visibility for the
///   identity surface (mirroring the v6 audit-trail fields) rather than a
///   storage-layout requirement; a leaner precedent (`ModelExecution::tenant`)
///   added a blob field without a bump. Guarded by
///   `test_v10_model_execution_forward_deserializes_recipe_identity_none`.
/// - **v12** — adds `output_column_hashes` (per-output-column content hashes;
///   see [`ColumnHash`]) to the [`ModelExecution`] blob. Pure additive
///   blob-field change, same shape as the v11 recipe-identity fields: the new
///   field is `#[serde(default)]`, so a v11 `RunRecord` forward-deserializes
///   with it `None` and a v12 blob back-reads cleanly on a v11 binary (serde
///   ignores the extra key). No new tables, no in-place migration. Guarded by
///   `test_v11_model_execution_forward_deserializes_output_column_hashes_none`.
/// - **v13** — adds `consumed_column_hashes` (the consumer-side per-column
///   baseline; see [`ColumnHash`]) to the [`UpstreamSig`] blob carried on
///   [`ModelExecution::upstream_freshness`]. Pure additive blob-field change,
///   same shape as the v12 field: the new field is `#[serde(default)]`, so a
///   v12 `UpstreamSig` forward-deserializes with it `None` and a v13 blob
///   back-reads cleanly on a v12 binary (serde ignores the extra key). No new
///   tables, no in-place migration. Guarded by
///   `test_v12_upstream_sig_forward_deserializes_consumed_column_hashes_none`.
/// - **v14** — adds the [`POLICY_DECISIONS`] table for the agent-policy
///   decision ledger (agent-authority enforcement at `rocky apply` / promote).
///   Pure additive schema change, same shape as the v7 [`OUTPUT_ARTIFACTS`]
///   add: v13 databases auto-create the empty table on next open and stamp
///   themselves as v14. No blob migration needed; existing tables are
///   untouched. Empty until a project with a `[policy]` block applies an
///   agent-authored plan, so pre-existing state resumes unchanged. Guarded by
///   `test_v13_opens_and_creates_policy_decisions_table`.
/// - **v15** — adds the [`JOBS`] table for the `rocky serve` HTTP job model
///   (`POST /api/v1/jobs/{run|plan|apply}`). Each row is a serialized
///   [`PersistedJob`] keyed by `job_id`, letting a killed-and-restarted sidecar
///   report honest status for the jobs it launched. Pure additive schema
///   change: v14 databases auto-create the empty table on next open and stamp
///   themselves as v15. No blob migration needed; existing tables are
///   untouched. Empty until a `rocky serve` sidecar submits a job.
/// - **v16** — adds the [`ModelExecution::attempts`] classified-retry trail.
///   A pure serde-additive *field* change, not a table change: the redb table
///   set is unchanged (`EXPECTED_TABLES` is untouched). A v15 blob (which lacks
///   the field) forward-deserializes with the trail empty, guarded by
///   `test_v15_model_execution_forward_deserializes_attempts_empty`. The bump
///   exists so the on-disk version tracks the record-shape addition and the
///   `[state] on_schema_mismatch` handling engages as usual; no blob walk.
/// - **v17** — adds two serde-additive *fields* for the policy plane's
///   `verify_after` post-apply gate: [`RunRecord::check_outcomes`] (per-check
///   pass/fail captured on every run) and [`PolicyDecisionRecord::verify_after`]
///   (the named checks a verification custody row required). Neither is a table
///   change — the redb table set is unchanged (`EXPECTED_TABLES` is untouched).
///   A v16 blob (which lacks the fields) forward-deserializes with both empty,
///   guarded by `test_v16_run_record_forward_deserializes_check_outcomes_empty`.
///   The bump tracks the record-shape addition so `[state] on_schema_mismatch`
///   engages as usual; no blob walk.
/// - **v18** — adds the serde-additive [`PolicyDecisionRecord::auto_apply`]
///   custody field ([`AutoApplyCustody`]) for the governed additive-drift
///   auto-apply path: what drift the run auto-applied (or refused), its
///   classification, and a revert pointer where a rollback substrate exists.
///   Not a table change — the redb table set is unchanged (`EXPECTED_TABLES`
///   is untouched). A v17 blob (which lacks the field) forward-deserializes
///   with it `None`, guarded by
///   `test_v17_policy_decision_forward_deserializes_auto_apply_none`. The bump
///   tracks the record-shape addition so `[state] on_schema_mismatch` engages
///   as usual; no blob walk.
const CURRENT_SCHEMA_VERSION: u32 = 18;

/// Errors from the embedded redb state store.
#[derive(Debug, Error)]
pub enum StateError {
    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),

    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),

    #[error("table error: {0}")]
    Table(#[from] redb::TableError),

    #[error("transaction error: {0}")]
    Transaction(Box<redb::TransactionError>),

    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error(
        "state schema version mismatch: database has v{found}, binary expects v{expected}. \
         Delete or migrate the state file at {path} to continue. \
         See https://rocky.dev/docs/state-migration for instructions."
    )]
    SchemaMismatch {
        found: u32,
        expected: u32,
        path: String,
    },

    #[error("state schema version is corrupt: expected an integer, found {0:?}")]
    VersionParse(String),

    #[error(
        "state store at {path} is locked by another process; only one rocky writer \
         can run at a time against the same state file"
    )]
    LockHeldByOther { path: String },

    #[error(
        "state store at {path} is busy — another rocky process (likely a long-running \
         `rocky run` or a concurrent LSP read) is holding the database lock. Retry in a moment."
    )]
    Busy { path: String },

    #[error("i/o error opening state lock file at {path}: {source}")]
    LockIo {
        path: String,
        #[source]
        source: std::io::Error,
    },
}

impl From<redb::TransactionError> for StateError {
    fn from(e: redb::TransactionError) -> Self {
        StateError::Transaction(Box::new(e))
    }
}

/// Embedded state store backed by redb for tracking watermarks and run history.
pub struct StateStore {
    db: Database,
    // Held exclusive advisory lock on `<path>.lock`. `None` for read-only opens.
    // Released automatically when the `File` is dropped.
    _lock_file: Option<File>,
    // `true` when this store was opened against a forward-incompatible on-disk
    // schema under [`SchemaMismatchPolicy::Recreate`] and the local file was
    // bootstrapped fresh. The run path reads this via
    // [`StateStore::was_recreated_for_forward_incompat`] to suppress the
    // end-of-run upload, so a downgraded state is never written back over the
    // newer state that upgraded pods depend on.
    recreated_for_forward_incompat: bool,
}

/// The schema version this binary supports.
///
/// Exposed so callers (state-sync key qualification, `rocky doctor`,
/// `rocky state show`) can reason about forward/backward compatibility without
/// opening a store. See [`CURRENT_SCHEMA_VERSION`].
pub fn current_schema_version() -> u32 {
    CURRENT_SCHEMA_VERSION
}

/// Outcome of [`StateStore::init_db`].
enum InitOutcome {
    /// The version was stamped/upgraded and tables were created; the store is
    /// ready to use.
    Ready,
    /// The on-disk schema is newer than this binary supports and the policy is
    /// [`SchemaMismatchPolicy::Recreate`]; the caller must delete and recreate
    /// the file. Carries the on-disk version for the WARN message.
    RecreateForwardIncompat { found: u32 },
}

impl StateStore {
    /// Opens or creates a state store at the given path for **writing**.
    ///
    /// Takes an exclusive advisory lock on `<path>.lock`. If another process
    /// already holds the lock the call fails with
    /// [`StateError::LockHeldByOther`] — this guarantees only one writer can
    /// mutate the state file at a time, which prevents the silent-corruption
    /// race where two `rocky run` processes interleave writes.
    ///
    /// Use [`StateStore::open_read_only`] for inspection commands that must not
    /// contend with a concurrent writer.
    ///
    /// On first open the schema version is written to the `metadata` table.
    /// On subsequent opens the stored version is compared to
    /// [`CURRENT_SCHEMA_VERSION`]; if the database was written by a newer
    /// binary (stored > current) an error is returned immediately so that the
    /// caller can surface a clear message rather than silently misreading data.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        Self::open_inner(path, true, SchemaMismatchPolicy::Fail)
    }

    /// Opens or creates a state store for **writing** with an explicit
    /// forward-incompatibility policy.
    ///
    /// Identical to [`StateStore::open`] except that the caller chooses what
    /// happens when the on-disk schema is *newer* than this binary supports.
    /// [`SchemaMismatchPolicy::Fail`] (what bare [`open`][Self::open] uses)
    /// aborts with [`StateError::SchemaMismatch`]; [`SchemaMismatchPolicy::Recreate`]
    /// logs a `WARN`, bootstraps a fresh local state, and sets the
    /// [`was_recreated_for_forward_incompat`][Self::was_recreated_for_forward_incompat]
    /// flag so the caller can suppress writing the downgraded state back to a
    /// shared tier.
    ///
    /// Only the `rocky run` path threads a non-default policy here (from
    /// `[state] on_schema_mismatch`); every other caller uses the hard-fail
    /// default via [`open`][Self::open].
    pub fn open_with_policy(path: &Path, policy: SchemaMismatchPolicy) -> Result<Self, StateError> {
        Self::open_inner(path, true, policy)
    }

    /// Opens an existing state store for **read-only** access.
    ///
    /// Skips Rocky's advisory write lock so two read-only opens from the same
    /// host don't fight each other on the `.redb.lock` sentinel. Redb itself
    /// still takes an exclusive `flock` on the database file (no `redb` 2.x
    /// API bypasses it), so the underlying open call is briefly serialised
    /// with any concurrent open — read or write. To smooth that race
    /// `open_inner` retries the redb open up to 5 × 50ms via
    /// [`open_redb_with_retry`]; after exhaustion the caller sees
    /// [`StateError::Busy`] (a friendly "retry in a moment" error) rather
    /// than the raw redb message. This is the right entry point for
    /// inspection commands (`rocky state`, `rocky history`, `rocky doctor`,
    /// `rocky metrics`, `rocky optimize`, LSP schema-cache reads, server
    /// APIs) — they don't need exclusivity and the retry hides the
    /// millisecond-scale collisions the LSP creates on every debounced
    /// keystroke.
    pub fn open_read_only(path: &Path) -> Result<Self, StateError> {
        Self::open_inner(path, false, SchemaMismatchPolicy::Fail)
    }

    /// Reads the schema version stamped in an on-disk state file **without**
    /// opening it for use, stamping it, creating tables, or recreating it.
    ///
    /// Returns:
    /// - `Ok(None)` if the file does not exist, has no `metadata` table, or has
    ///   no `schema_version` key (a pre-versioning store).
    /// - `Ok(Some(v))` with the stored version otherwise.
    ///
    /// Unlike [`open`][Self::open] / [`open_read_only`][Self::open_read_only],
    /// this never fails on a forward-incompatible store — it is the primitive
    /// behind `rocky doctor --check state_schema` and the version fields in
    /// `rocky state show`, both of which must report the on-disk version
    /// precisely when it is newer than this binary supports. It takes redb's
    /// shared open path (with the same lock-contention retry) but never Rocky's
    /// advisory write lock.
    pub fn peek_schema_version(path: &Path) -> Result<Option<u32>, StateError> {
        // Side-effect-free for a missing file: do not let the redb open path
        // create an empty database as a probe artifact.
        if !path.exists() {
            return Ok(None);
        }
        let db = open_redb_with_retry(path)?;
        let txn = db.begin_read()?;
        let metadata = match txn.open_table(METADATA) {
            Ok(table) => table,
            // A store written before the metadata table existed (or an empty
            // freshly-created file) carries no version — treat as unversioned.
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(StateError::Table(e)),
        };
        match metadata.get("schema_version")? {
            Some(guard) => {
                let raw = guard.value().to_string();
                let version = raw
                    .parse::<u32>()
                    .map_err(|_| StateError::VersionParse(raw))?;
                Ok(Some(version))
            }
            None => Ok(None),
        }
    }

    /// Whether this store was bootstrapped fresh because the on-disk schema was
    /// forward-incompatible (newer than this binary) and the open policy was
    /// [`SchemaMismatchPolicy::Recreate`].
    ///
    /// When `true`, the caller must **not** persist this store back to a shared
    /// remote tier — doing so would clobber the newer state that already-upgraded
    /// pods depend on. The `rocky run` path checks this before its end-of-run
    /// `upload_state`.
    pub fn was_recreated_for_forward_incompat(&self) -> bool {
        self.recreated_for_forward_incompat
    }

    fn open_inner(
        path: &Path,
        lock: bool,
        policy: SchemaMismatchPolicy,
    ) -> Result<Self, StateError> {
        // Acquire the advisory lock BEFORE opening the database — otherwise two
        // concurrent writers could both pass `Database::create` before either
        // of them reaches the lock check.
        let lock_file = if lock {
            let lock_path = path.with_extension("redb.lock");
            let file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(false)
                .read(true)
                .write(true)
                .open(&lock_path)
                .map_err(|e| StateError::LockIo {
                    path: lock_path.display().to_string(),
                    source: e,
                })?;
            FileExt::try_lock(&file).map_err(|e| match e {
                fs4::TryLockError::WouldBlock => StateError::LockHeldByOther {
                    path: path.display().to_string(),
                },
                fs4::TryLockError::Error(source) => StateError::LockIo {
                    path: lock_path.display().to_string(),
                    source,
                },
            })?;
            Some(file)
        } else {
            None
        };

        let db = open_redb_with_retry(path)?;

        match Self::init_db(&db, path, policy)? {
            InitOutcome::Ready => Ok(StateStore {
                db,
                _lock_file: lock_file,
                recreated_for_forward_incompat: false,
            }),
            InitOutcome::RecreateForwardIncompat { found } => {
                // Forward-incompatible store under `SchemaMismatchPolicy::Recreate`.
                // Treat it as cold: drop the handle, delete the local file, and
                // bootstrap fresh. The caller (run path) reads the
                // `recreated_for_forward_incompat` flag and skips the end-of-run
                // upload, so the newer shared state is never clobbered.
                tracing::warn!(
                    target: "rocky::state",
                    found,
                    expected = CURRENT_SCHEMA_VERSION,
                    path = %path.display(),
                    "state schema version mismatch: database has v{found}, binary expects \
                     v{CURRENT_SCHEMA_VERSION}. on_schema_mismatch = recreate: bootstrapping \
                     fresh local state and running a full refresh. The newer shared state is \
                     left intact (not overwritten) for already-upgraded pods."
                );
                drop(db);
                // If we cannot remove the forward-incompatible file, reopening
                // would just re-read the same newer version and loop. Degrade to
                // the hard-fail error so the operator gets the actionable
                // "delete or migrate" message rather than a silent spin.
                if let Err(source) = std::fs::remove_file(path) {
                    tracing::warn!(
                        target: "rocky::state",
                        error = %source,
                        path = %path.display(),
                        "could not delete forward-incompatible state file to recreate it; \
                         falling back to hard failure"
                    );
                    return Err(StateError::SchemaMismatch {
                        found,
                        expected: CURRENT_SCHEMA_VERSION,
                        path: path.display().to_string(),
                    });
                }
                let db = open_redb_with_retry(path)?;
                match Self::init_db(&db, path, policy)? {
                    InitOutcome::Ready => Ok(StateStore {
                        db,
                        _lock_file: lock_file,
                        recreated_for_forward_incompat: true,
                    }),
                    // A file we just created cannot carry a newer schema version.
                    InitOutcome::RecreateForwardIncompat { found } => {
                        Err(StateError::SchemaMismatch {
                            found,
                            expected: CURRENT_SCHEMA_VERSION,
                            path: path.display().to_string(),
                        })
                    }
                }
            }
        }
    }

    /// Stamp the schema version and ensure every table exists, in a single
    /// write transaction so the version stamp and table layout always commit
    /// together.
    ///
    /// Returns [`InitOutcome::RecreateForwardIncompat`] (without committing)
    /// when the on-disk version is newer than this binary supports **and** the
    /// policy is [`SchemaMismatchPolicy::Recreate`]; under
    /// [`SchemaMismatchPolicy::Fail`] the same condition returns
    /// [`StateError::SchemaMismatch`]. Otherwise stamps/upgrades the version,
    /// creates the tables, commits, and returns [`InitOutcome::Ready`].
    fn init_db(
        db: &Database,
        path: &Path,
        policy: SchemaMismatchPolicy,
    ) -> Result<InitOutcome, StateError> {
        let txn = db.begin_write()?;
        {
            let mut metadata = txn.open_table(METADATA)?;

            // Read the stored version string, if any. The AccessGuard borrows
            // `metadata` immutably, so we copy the value out before we may
            // need to mutably insert below.
            let stored_version: Option<String> = metadata
                .get("schema_version")?
                .map(|g| g.value().to_string());

            match stored_version {
                Some(version_str) => {
                    let found = version_str
                        .parse::<u32>()
                        .map_err(|_| StateError::VersionParse(version_str.clone()))?;
                    if found > CURRENT_SCHEMA_VERSION {
                        // Dropping the uncommitted `txn` here aborts it, so no
                        // partial stamp/table write survives the early return.
                        return match policy {
                            SchemaMismatchPolicy::Fail => Err(StateError::SchemaMismatch {
                                found,
                                expected: CURRENT_SCHEMA_VERSION,
                                path: path.display().to_string(),
                            }),
                            SchemaMismatchPolicy::Recreate => {
                                Ok(InitOutcome::RecreateForwardIncompat { found })
                            }
                        };
                    }
                    // Upgrade stamp so future migrations can branch on
                    // the actual version stored on disk.
                    if found < CURRENT_SCHEMA_VERSION {
                        metadata.insert("schema_version", &*CURRENT_SCHEMA_VERSION.to_string())?;
                    }
                }
                None => {
                    // Fresh database or pre-versioning database — stamp the version.
                    metadata.insert("schema_version", &*CURRENT_SCHEMA_VERSION.to_string())?;
                }
            }

            // Eagerly open every table so a freshly-created DB materializes the
            // full set up front. This is what makes the on-disk format
            // deterministic and lets the `on_disk_state_schema_format_is_pinned`
            // golden enumerate it via `list_tables()`. When adding a table:
            // register it HERE (eager creation) *and* in that golden's
            // EXPECTED_TABLES. A table created lazily on first write would not
            // appear in a fresh DB and would silently escape the golden.
            let _table = txn.open_table(WATERMARKS)?;
            let _table = txn.open_table(SOURCE_MARKERS)?;
            let _table = txn.open_table(CHECK_HISTORY)?;
            let _table = txn.open_table(RUN_HISTORY)?;
            let _table = txn.open_table(QUALITY_HISTORY)?;
            let _table = txn.open_table(DAG_SNAPSHOTS)?;
            let _table = txn.open_table(RUN_PROGRESS)?;
            let _table = txn.open_table(RUN_PROGRESS_ENTRIES)?;
            let _table = txn.open_table(PARTITIONS)?;
            let _table = txn.open_table(GRACE_PERIODS)?;
            let _table = txn.open_table(LOADED_FILES)?;
            let _table = txn.open_table(BRANCHES)?;
            let _table = txn.open_table(SCHEMA_CACHE)?;
            let _table = txn.open_table(IDEMPOTENCY_KEYS)?;
            let _table = txn.open_table(OUTPUT_ARTIFACTS)?;
            let _table = txn.open_table(INPUT_INDEX)?;
            let _table = txn.open_table(INPUT_PROVENANCE)?;
            let _table = txn.open_table(DISCOVER_SNAPSHOTS)?;
            let _table = txn.open_table(POLICY_DECISIONS)?;
            let _table = txn.open_table(JOBS)?;
        }
        txn.commit()?;
        Ok(InitOutcome::Ready)
    }

    /// Gets the watermark for a table (keyed by `catalog.schema.table`).
    pub fn get_watermark(&self, table_key: &str) -> Result<Option<WatermarkState>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(WATERMARKS)?;

        match table.get(table_key)? {
            Some(value) => {
                let watermark: WatermarkState = serde_json::from_slice(value.value())?;
                Ok(Some(watermark))
            }
            None => Ok(None),
        }
    }

    /// Sets the watermark for a table.
    pub fn set_watermark(
        &self,
        table_key: &str,
        watermark: &WatermarkState,
    ) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(watermark)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(WATERMARKS)?;
            table.insert(table_key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Gets the recorded source change-marker for a table (keyed by
    /// `catalog.schema.table`), written after the last successful copy.
    pub fn get_source_marker(&self, table_key: &str) -> Result<Option<String>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(SOURCE_MARKERS)?;
        Ok(table.get(table_key)?.map(|v| v.value().to_string()))
    }

    /// Records the source change-marker for a table after a successful copy.
    /// Store the marker read at the copy's decision point (not a post-copy
    /// re-read): if the source advanced mid-copy the recorded marker stays
    /// behind, so the next run copies again rather than skipping a change.
    pub fn set_source_marker(&self, table_key: &str, marker: &str) -> Result<(), StateError> {
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SOURCE_MARKERS)?;
            table.insert(table_key, marker)?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Sets multiple watermarks in a single write transaction.
    ///
    /// Use this at end-of-run when promoting deferred watermarks: one
    /// `begin_write` / `commit` cycle covers every entry instead of one per
    /// watermark. On a 100-partition pipeline this saves hundreds of
    /// milliseconds of fsync overhead (redb commits flush to disk).
    ///
    /// Returns the number of watermarks written. If any entry fails to
    /// serialize or insert, the whole transaction is rolled back — partial
    /// watermark commits are not observable to subsequent reads (§P1.6).
    pub fn batch_set_watermarks(
        &self,
        entries: &[(&str, &WatermarkState)],
    ) -> Result<usize, StateError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(WATERMARKS)?;
            for (key, watermark) in entries {
                let bytes = serde_json::to_vec(*watermark)?;
                table.insert(*key, bytes.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(entries.len())
    }

    /// Deletes the watermark for a table (triggers full refresh on next run).
    pub fn delete_watermark(&self, table_key: &str) -> Result<bool, StateError> {
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(WATERMARKS)?;
            removed = table.remove(table_key)?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Lists all stored watermarks.
    pub fn list_watermarks(&self) -> Result<Vec<(String, WatermarkState)>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(WATERMARKS)?;
        let mut results = Vec::new();

        for entry in table.iter()? {
            let (key, value) = entry?;
            let watermark: WatermarkState = serde_json::from_slice(value.value())?;
            results.push((key.value().to_string(), watermark));
        }

        Ok(results)
    }

    /// Records a row count snapshot for a table (for anomaly detection).
    /// Key format: `table_key` → JSON array of `CheckSnapshot`.
    /// Keeps the last `max_history` entries.
    pub fn record_row_count(
        &self,
        table_key: &str,
        row_count: u64,
        max_history: usize,
    ) -> Result<(), StateError> {
        let snapshot = CheckSnapshot {
            timestamp: chrono::Utc::now(),
            row_count,
        };

        let mut history = self.get_check_history(table_key)?;
        history.push(snapshot);

        // Trim to max_history
        if history.len() > max_history {
            history.drain(..history.len() - max_history);
        }

        let bytes = serde_json::to_vec(&history)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(CHECK_HISTORY)?;
            table.insert(table_key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Gets the check history for a table.
    pub fn get_check_history(&self, table_key: &str) -> Result<Vec<CheckSnapshot>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(CHECK_HISTORY)?;

        match table.get(table_key)? {
            Some(value) => {
                let history: Vec<CheckSnapshot> = serde_json::from_slice(value.value())?;
                Ok(history)
            }
            None => Ok(Vec::new()),
        }
    }

    // ----- time_interval partition state -----

    /// Records (or updates) a partition's lifecycle state.
    ///
    /// Used by the runtime executor at the start of a partition compute
    /// (`InProgress`) and at the end (`Computed` or `Failed`). The key in
    /// the underlying table is `"{model_name}|{partition_key}"`.
    pub fn record_partition(
        &self,
        record: &crate::incremental::PartitionRecord,
    ) -> Result<(), StateError> {
        let key = partition_record_key(&record.model_name, &record.partition_key);
        let bytes = serde_json::to_vec(record)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(PARTITIONS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Gets the recorded state for a single `(model, partition_key)`.
    /// Returns `None` if the partition has never been computed.
    pub fn get_partition(
        &self,
        model_name: &str,
        partition_key: &str,
    ) -> Result<Option<crate::incremental::PartitionRecord>, StateError> {
        let key = partition_record_key(model_name, partition_key);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PARTITIONS)?;
        match table.get(key.as_str())? {
            Some(value) => {
                let record: crate::incremental::PartitionRecord =
                    serde_json::from_slice(value.value())?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Lists all stored partitions for a model, in arbitrary order.
    /// Used by `--missing` discovery to compute the diff between expected
    /// partitions and what's actually been computed.
    pub fn list_partitions(
        &self,
        model_name: &str,
    ) -> Result<Vec<crate::incremental::PartitionRecord>, StateError> {
        let prefix = format!("{model_name}|");
        let txn = self.db.begin_read()?;
        let table = txn.open_table(PARTITIONS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                continue;
            }
            let record: crate::incremental::PartitionRecord =
                serde_json::from_slice(value.value())?;
            results.push(record);
        }
        Ok(results)
    }

    /// Lists partitions for a model whose status is `Failed`.
    /// Used by retry support to find partitions that need to be recomputed
    /// without scanning all of `list_partitions()` and filtering at the
    /// caller.
    pub fn list_failed_partitions(
        &self,
        model_name: &str,
    ) -> Result<Vec<crate::incremental::PartitionRecord>, StateError> {
        Ok(self
            .list_partitions(model_name)?
            .into_iter()
            .filter(|r| r.status == crate::incremental::PartitionStatus::Failed)
            .collect())
    }

    /// Deletes a partition's state-store record.
    /// Used by cleanup tooling and the future `rocky archive` integration.
    /// Returns true if a row was actually removed.
    pub fn delete_partition(
        &self,
        model_name: &str,
        partition_key: &str,
    ) -> Result<bool, StateError> {
        let key = partition_record_key(model_name, partition_key);
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(PARTITIONS)?;
            removed = table.remove(key.as_str())?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }
}

/// Number of attempts (incl. the first) for [`open_redb_with_retry`].
const REDB_OPEN_RETRY_ATTEMPTS: u32 = 5;
/// Linear backoff between retry attempts.
const REDB_OPEN_RETRY_DELAY: std::time::Duration = std::time::Duration::from_millis(50);

#[cfg(test)]
thread_local! {
    /// Test-only retry observer. When an opener thread arms this, every flock
    /// collision in [`open_redb_with_retry`] bumps the counter. A test can then
    /// release a competing lock the moment it sees the retry path has been
    /// entered, instead of sleeping a fixed duration and racing the retry
    /// budget — see `open_read_only_retries_and_succeeds_after_brief_hold`.
    static REDB_RETRY_OBSERVER: std::cell::RefCell<Option<std::sync::Arc<std::sync::atomic::AtomicUsize>>> =
        const { std::cell::RefCell::new(None) };
}

/// Open the redb database file, retrying briefly on the transient
/// [`redb::DatabaseError::DatabaseAlreadyOpen`] flock collision.
///
/// Redb 2.x takes an unconditional `flock(LOCK_EX | LOCK_NB)` in
/// `FileBackend::new`, so any concurrent open from the same machine — even
/// another reader — is briefly serialised. The LSP fires a schema-cache read
/// ~300ms after every keystroke (`rocky-server::lsp::Server::did_change`),
/// which races every CLI invocation the VS Code extension launches; without
/// retry the user sees a raw "Database already open. Cannot acquire lock."
/// error from redb.
///
/// Retries are bounded (5 × 50ms ≈ 250ms total) and only kick in for the
/// flock collision. Other redb errors propagate immediately. After
/// exhaustion the caller gets [`StateError::Busy`] with a friendly message.
///
/// This does NOT fix the writer-vs-CLI race during a long `rocky run`
/// (the writer holds the lock for seconds-to-minutes); inspection commands
/// will still hit `Busy` in that scenario, but with a clear next step.
fn open_redb_with_retry(path: &Path) -> Result<Database, StateError> {
    for attempt in 0..REDB_OPEN_RETRY_ATTEMPTS {
        match Database::create(path) {
            Ok(db) => return Ok(db),
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => {
                #[cfg(test)]
                REDB_RETRY_OBSERVER.with(|o| {
                    if let Some(counter) = o.borrow().as_ref() {
                        counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                });
                if attempt + 1 < REDB_OPEN_RETRY_ATTEMPTS {
                    std::thread::sleep(REDB_OPEN_RETRY_DELAY);
                }
            }
            Err(e) => return Err(StateError::Database(e)),
        }
    }
    Err(StateError::Busy {
        path: path.display().to_string(),
    })
}

/// Compose the redb key for a `PartitionRecord`.
///
/// Pulled out as a helper so the `record_partition` / `get_partition` /
/// `delete_partition` methods can't accidentally diverge on key format.
fn partition_record_key(model_name: &str, partition_key: &str) -> String {
    format!("{model_name}|{partition_key}")
}

// ---------------------------------------------------------------------------
// Run history
// ---------------------------------------------------------------------------

/// Status of a pipeline run.
///
/// `Success` / `PartialFailure` / `Failure` cover the terminal outcomes of a
/// run that actually executed. `SkippedIdempotent` / `SkippedInFlight` are
/// the short-circuit outcomes of `rocky run --idempotency-key` — see
/// [`crate::idempotency`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum RunStatus {
    Success,
    PartialFailure,
    Failure,
    /// The run was short-circuited because an idempotency key already
    /// mapped to a prior completed run. No work was done.
    SkippedIdempotent,
    /// The run was short-circuited because another caller held the
    /// idempotency key's in-flight claim. No work was done.
    SkippedInFlight,
}

/// What triggered the run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RunTrigger {
    Manual,
    Sensor,
    Schedule,
    Ci,
}

/// Where the `rocky` invocation originated — the calling surface that
/// spawned the process.
///
/// Stamped on every [`RunRecord`] at claim time for the governance audit
/// trail (§1.4 item 2). The CLI derives it from the environment:
///
/// - [`SessionSource::Dagster`] — `DAGSTER_PIPES_CONTEXT` is set (Dagster's
///   Pipes protocol populates this when a Pipes client launches the
///   subprocess — whether or not the binary emits Pipes messages back).
/// - [`SessionSource::HttpApi`] — `ROCKY_SESSION_SOURCE=http_api` is set
///   (an HTTP caller explicitly stamps itself; Rocky's server never
///   spawns `rocky run` today, but the variant is reserved for future
///   wiring).
/// - [`SessionSource::Lsp`] — `ROCKY_SESSION_SOURCE=lsp` is set. Reserved
///   for completeness; the LSP server (`rocky lsp`) handles requests
///   in-process rather than shelling out to `rocky run`, so this is
///   effectively unreachable today.
/// - [`SessionSource::Cli`] — the fallback. A human / script / CI runner
///   invoking `rocky run` directly.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum SessionSource {
    /// Direct invocation — shell, script, or CI runner.
    Cli,
    /// Dagster Pipes / subprocess client launched the process.
    Dagster,
    /// LSP server invoked the run. Reserved; see enum-level docs.
    Lsp,
    /// HTTP API caller stamped itself via `ROCKY_SESSION_SOURCE=http_api`.
    HttpApi,
}

impl Default for SessionSource {
    /// The deserialization default for `session_source` on v5→v6 records.
    /// Pre-audit rows that predate the audit-trail field surface as
    /// [`SessionSource::Cli`] — a safe, maximally-general choice since the
    /// overwhelming majority of historical invocations ran via shell.
    fn default() -> Self {
        SessionSource::Cli
    }
}

/// Per-upstream freshness signature recorded for a model on a successful
/// build, read back by the opt-in `--skip-unchanged` gate to decide whether
/// any upstream's *data* moved since the last build.
///
/// This is a best-effort optimization input, not a correctness guarantee:
/// a matching signature means an upstream's tracked signal (latest timestamp
/// and/or row count) *looks* unchanged, not that the upstream data is
/// provably identical. State-internal — never part of any `*Output` JSON
/// schema, so it carries no codegen.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct UpstreamSig {
    /// Fully-qualified `catalog.schema.table` identity of the upstream this
    /// signature describes.
    pub upstream_key: String,
    /// Latest observed timestamp (`MAX(ts)`) for the upstream's tracked
    /// timestamp column, when one is available. `None` when the upstream has
    /// no tracked timestamp column or the value could not be read.
    #[serde(default)]
    pub max_ts: Option<chrono::DateTime<chrono::Utc>>,
    /// Observed row count (`COUNT(*)`) for the upstream, recorded only when
    /// the rowcount fallback is enabled. `None` otherwise.
    #[serde(default)]
    pub row_count: Option<u64>,

    // --- Consumer-side column baseline (schema v13) ----------------------
    /// The per-column content hashes this downstream **consumed** from this
    /// upstream at its last successful build — the consumer-side baseline
    /// (see [`ColumnHash`]). One entry per column in `consumed(D, U)` (the
    /// columns the downstream provably reads from this upstream, per the
    /// completeness guard), each carrying the upstream's producer output-column
    /// hash ([`ModelExecution::output_column_hashes`]) as observed at build
    /// time.
    ///
    /// This is **consumer-side and local**: it records what *this* model read
    /// from *this* upstream, reconstructible from two stored snapshots plus the
    /// current run — never a producer-now-vs-producer-prior inference. It
    /// extends the freshness baseline this same `UpstreamSig` already stores
    /// (`max_ts` / `row_count`) with column granularity, on the
    /// content-addressed path only.
    ///
    /// `None` when the baseline was not captured: a pre-v13 record, a
    /// non-content-addressed build, an upstream whose producer column hashes
    /// were unavailable this build, or a downstream whose consumed-column set
    /// could not be proven complete. An absent baseline can only ever force a
    /// (safe) rebuild in the later skip gate, never a wrong skip. Captured
    /// only; nothing consults it yet (the skip gate is a later phase).
    /// State-internal — carries no codegen. Serde-defaulted so a pre-v13 blob
    /// forward-deserializes with it `None`; guarded by
    /// `test_v12_upstream_sig_forward_deserializes_consumed_column_hashes_none`.
    #[serde(default)]
    pub consumed_column_hashes: Option<Vec<ColumnHash>>,
}

/// A single output column's content hash, captured on a successful
/// content-addressed build.
///
/// `hash` is `hex(blake3(...))` over the written Arrow column's serialized
/// content (values + type, in table column order) — content, not schema. Two
/// columns with byte-identical stored content share a hash; a schema-stable
/// value change (a WHERE/JOIN/CASE rewrite that keeps the type but moves the
/// data) flips it. The hash is intentionally over-sensitive: row-order or
/// encoding differences make it differ even when the logical content is the
/// same, so a mismatch can only ever cause a (safe) rebuild, never a wrong
/// skip.
///
/// State-internal — never part of any `*Output` JSON schema, so it carries no
/// codegen (matches the `skip_hash` / `upstream_freshness` / recipe-identity
/// precedent).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ColumnHash {
    /// The output column's name, in the table's column order.
    pub column: String,
    /// `hex(blake3(...))` of the column's serialized content.
    pub hash: String,
}

/// One attempt at materializing a model, recorded when the run loop's
/// classified-retry layer is active.
///
/// A model that succeeds on the first try records a single attempt; a model
/// that failed transiently and was re-run records one entry per attempt, the
/// last of which is the outcome that stuck. The trail is **execution
/// metadata** — it lives on [`ModelExecution`] alongside the identity hashes,
/// never inside them, so a retried-then-succeeded execution stays
/// byte-indistinguishable downstream from a first-try success (same
/// `recipe_hash` / `input_hash` / output hash).
///
/// Reused verbatim on `MaterializationOutput.attempts` (the run-JSON surface),
/// so it derives `JsonSchema`; that is the one attempt type on both the wire
/// and the state record.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub struct AttemptRecord {
    /// 1-based attempt index. Attempt 1 is the first try; 2+ are retries.
    pub attempt: u32,
    /// `"success"` or `"failed"`.
    pub outcome: String,
    /// The run-loop [`FailureClass`](crate::failure_class::FailureClass) label
    /// (`"transient"` / `"permanent"` / `"unknown"`) for a failed attempt;
    /// `None` on a successful attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_class: Option<String>,
    /// The transient sub-kind label (`"network"`, `"timeout"`, …) when the
    /// failure was transient; `None` otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transient_kind: Option<String>,
    /// The failure message (truncated), for a failed attempt; `None` on
    /// success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Milliseconds slept as backoff *after* this attempt before the next
    /// retry. `None` on the final (kept) attempt and on any attempt that was
    /// not followed by a retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_ms: Option<u64>,
    /// Wall-clock duration of this attempt, in milliseconds.
    pub duration_ms: u64,
}

/// Execution record for a single model within a run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ModelExecution {
    pub model_name: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub finished_at: chrono::DateTime<chrono::Utc>,
    pub duration_ms: u64,
    pub rows_affected: Option<u64>,
    pub status: String,
    pub sql_hash: String,
    /// Cosmetic-invariant logic key for the model, recorded on a successful
    /// build and read back by the opt-in `--skip-unchanged` gate (clause B2:
    /// "logic looks unchanged"). `None` for executions written by binaries
    /// predating the gate, for non-canonicalisable models, or for failed
    /// executions. A best-effort optimization input — never a proof of
    /// result-equivalence. State-internal: not part of any `*Output` JSON
    /// schema, so it carries no codegen.
    #[serde(default)]
    pub skip_hash: Option<String>,
    /// Per-upstream freshness signatures captured on a successful build,
    /// read back by the `--skip-unchanged` gate (clause B3: "upstream data
    /// looks unchanged"). `None` for pre-gate / failed executions.
    /// State-internal — carries no codegen.
    #[serde(default)]
    pub upstream_freshness: Option<Vec<UpstreamSig>>,
    /// Adapter-reported bytes figure used for cost accounting. This is
    /// the *billing-relevant* number per adapter, not literal scan
    /// volume:
    ///
    /// - **BigQuery:** `totalBytesBilled` — includes the 10 MB
    ///   per-query minimum floor; matches the BigQuery console's
    ///   "Bytes billed" field, **not** "Bytes processed".
    /// - **Databricks:** when populated, byte count from the
    ///   statement-execution manifest (`total_byte_count`); `None`
    ///   today until the manifest plumbing lands.
    /// - **Snowflake:** `None` — deferred by design (QUERY_HISTORY
    ///   round-trip cost; Snowflake cost is duration × DBU, not
    ///   bytes-driven).
    /// - **DuckDB:** `None` — no billed-bytes concept.
    ///
    /// Persisted mirror of `MaterializationOutput.bytes_scanned`.
    pub bytes_scanned: Option<u64>,
    /// Adapter-reported bytes-written figure. Currently `None` on
    /// every adapter — BigQuery doesn't expose a bytes-written figure
    /// for query jobs, and the Databricks / Snowflake paths haven't
    /// wired it yet. Reserved for adapters that produce a natural
    /// bytes-written figure via their statistics surface.
    pub bytes_written: Option<u64>,
    /// Tenant this execution is attributed to, sourced from the
    /// discover-time schema-pattern `{tenant}` component (see
    /// [`crate::schema`]). Populated only for replication pipelines
    /// whose schema pattern declares a `{tenant}` placeholder;
    /// transformation / `time_interval` models and non-tenant patterns
    /// record `None`. Read back by `rocky cost --by tenant` to roll
    /// per-model cost up to a tenant dimension.
    ///
    /// Serde-defaulted so records written before this field existed
    /// forward-deserialize to `None` — no `CURRENT_SCHEMA_VERSION` bump.
    /// Guarded by `test_pre_tenant_model_execution_forward_deserializes_to_none`.
    #[serde(default)]
    pub tenant: Option<String>,

    // --- Recipe identity (schema v11) ------------------------------------
    //
    // The `(recipe_hash, input_hash, env_hash)` triple + its scheme tag,
    // recorded on every execution so "what exact program, over what inputs,
    // in what environment produced this table version?" is answerable for
    // every model on every adapter — not just the content-addressed path.
    // All state-internal (never in a `*Output` schema, no codegen) and
    // serde-defaulted; see [`crate::recipe_identity`] for the definitions.
    /// The model's **identity** key: blake3 (hex) of the canonical
    /// [`rocky_ir::ModelIr`] JSON — [`crate::recipe_identity::recipe_hash`].
    /// The same canonical form [`ProvenanceRecord`] embeds. Distinct from
    /// [`Self::skip_hash`] (the cosmetic-invariant *skip* key): recipe_hash
    /// is whitespace-sensitive program identity, not a skip decision.
    /// `None` for failed executions and for records written before v11.
    #[serde(default)]
    pub recipe_hash: Option<String>,
    /// The model's **input** key (hex): a blake3 over the *observed* input
    /// identities via [`crate::recipe_identity::compute_input_hash`]. Present
    /// only when this run actually observed inputs (the skip gate's upstream
    /// freshness signatures, or the content-addressed reuse spine). `None` on
    /// the default run path, which observes no inputs — the declared inputs
    /// already live inside [`Self::recipe_hash`], so a bare "no upstreams"
    /// hash would add nothing and would falsely read as a strong claim.
    #[serde(default)]
    pub input_hash: Option<String>,
    /// Strength of [`Self::input_hash`]: `"strong"` (every observed upstream
    /// is a content hash) or `"heuristic"` (at least one is a freshness
    /// signature — the *weak* label the design uses for mutable sources). See
    /// [`crate::recipe_identity::ProofClass`]. `None` whenever
    /// [`Self::input_hash`] is `None`.
    #[serde(default)]
    pub input_proof_class: Option<String>,
    /// The **environment** key (hex): blake3 over
    /// [`crate::recipe_identity::EnvIdentity`] (engine version + adapter /
    /// dialect identity + execution config). Excludes hostname by
    /// construction — machine identity is on [`RunRecord::hostname`], not
    /// here. `None` for failed executions and pre-v11 records.
    #[serde(default)]
    pub env_hash: Option<String>,
    /// The [`crate::recipe_identity::HASH_SCHEME`] tag (`"v1"`) in force when
    /// the triple above was computed, so a future canonicalisation change is
    /// an explicit new scheme rather than a silent history fork. `None` for
    /// failed executions and pre-v11 records.
    #[serde(default)]
    pub hash_scheme: Option<String>,

    // --- Output column hashes (schema v12) -------------------------------
    /// Per-output-column content hashes captured on a successful
    /// content-addressed build, in the table's column order. The producer
    /// side of the per-column skip substrate: "what did each of my columns
    /// hash to when I built?" (see [`ColumnHash`]).
    ///
    /// `None` for pre-v12 executions, failed executions, non-content-addressed
    /// builds, and content-addressed builds that reused a prior run's bytes or
    /// wrote a partitioned table (both deferred — see the runner). Captured
    /// only; nothing consults it yet (the consumer-side baseline and skip gate
    /// are later phases). State-internal — carries no codegen. Serde-defaulted
    /// so a pre-v12 blob forward-deserializes with it `None`; guarded by
    /// `test_v11_model_execution_forward_deserializes_output_column_hashes_none`.
    #[serde(default)]
    pub output_column_hashes: Option<Vec<ColumnHash>>,

    // --- Attempt history (schema v16) ------------------------------------
    /// The classified-retry attempt trail for this execution: one
    /// [`AttemptRecord`] per try. Empty for records written before v16, for
    /// executions the retry layer never touched, and (by construction) for a
    /// single-attempt success where the trail carries no extra signal — the
    /// producer only stamps a trail once a retry actually occurred.
    ///
    /// **Execution metadata, never identity.** This field rides *alongside*
    /// `recipe_hash` / `input_hash` / `output_column_hashes`, never inside
    /// them, exactly like the governance audit trail rides outside `plan_id`.
    /// A model that failed once then succeeded records its retries here while
    /// producing byte-identical identity hashes to a clean first-try run, so
    /// replay, the column-skip gate, and `rocky-verify` are all unaffected.
    ///
    /// Serde-defaulted (and omitted when empty) so a pre-v16 blob
    /// forward-deserializes with it empty AND a no-retry record's ledger bytes
    /// stay byte-identical to before v16 — the common case carries no trail.
    /// Guarded by `test_v15_model_execution_forward_deserializes_attempts_empty`
    /// and the `ledger_record_serialization_pinned` golden.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<AttemptRecord>,
}

/// A complete pipeline run record.
///
/// # Governance audit trail (schema v6)
///
/// The eight `triggering_identity` / `session_source` / `git_commit` /
/// `git_branch` / `idempotency_key` / `target_catalog` / `hostname` /
/// `rocky_version` fields form the governance audit trail introduced in
/// state schema v6. They are populated at claim time in
/// `rocky_cli::commands::run::persist_run_record` via helpers in
/// `rocky_cli::commands::run_audit`.
///
/// Every new field is serde-defaulted so v5 records (which lack these
/// fields entirely) forward-deserialize cleanly:
///
/// - Option-typed fields default to `None`.
/// - [`SessionSource`] defaults to [`SessionSource::Cli`].
/// - `hostname` defaults to `"unknown"`.
/// - `rocky_version` defaults to `"<pre-audit>"` — a sentinel so
///   operators can tell a pre-audit row apart from a real binary that
///   happened to read `CARGO_PKG_VERSION` as `"unknown"`.
///
/// The redb blob format is `serde_json::to_vec(&record)`, so the upgrade
/// is zero-touch at the storage layer: no in-place migration, no blob
/// walk. A `test_v5_run_record_forward_deserializes_with_defaults` guard
/// in this file pins the contract.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub finished_at: chrono::DateTime<chrono::Utc>,
    pub status: RunStatus,
    pub models_executed: Vec<ModelExecution>,
    pub trigger: RunTrigger,
    pub config_hash: String,

    // --- Governance audit trail (schema v6 — §1.4) ---
    /// Best-effort caller identity. `$USER` on Unix, `%USERNAME%` on
    /// Windows; `None` in CI/containers without a clear human caller.
    /// SSO `sub` claim extraction (Snowflake / Databricks OAuth) is a
    /// deferred follow-up.
    #[serde(default)]
    pub triggering_identity: Option<String>,

    /// Where the `rocky` invocation originated (CLI / Dagster / LSP /
    /// HTTP). See [`SessionSource`] for the derivation rules. Defaults
    /// to [`SessionSource::Cli`] on v5 records.
    #[serde(default)]
    pub session_source: SessionSource,

    /// `git rev-parse HEAD` at claim time. `None` when the working
    /// directory is not a git checkout or `git` is not on PATH.
    #[serde(default)]
    pub git_commit: Option<String>,

    /// `git symbolic-ref --short HEAD` at claim time. `None` on detached
    /// HEAD or non-git checkouts.
    #[serde(default)]
    pub git_branch: Option<String>,

    /// The `--idempotency-key` value for this run, or `None` if the flag
    /// was not supplied. When present, matches the key stamped into the
    /// `idempotency_keys` table (PR #235).
    #[serde(default)]
    pub idempotency_key: Option<String>,

    /// Best-available target catalog for the pipeline at claim time.
    /// `None` when no catalog is resolvable (e.g. model-only run without
    /// a concrete pipeline context). For templated catalogs
    /// (`catalog_template = "{client}"`) this captures the literal
    /// template string — per-table resolutions are already recorded on
    /// the individual [`ModelExecution`] entries.
    #[serde(default)]
    pub target_catalog: Option<String>,

    /// The machine that produced the run, from the `hostname` crate.
    /// Defaults to `"unknown"` on v5 records.
    #[serde(default = "default_hostname_sentinel")]
    pub hostname: String,

    /// Compile-time `env!("CARGO_PKG_VERSION")` of the `rocky` binary.
    /// Defaults to `"<pre-audit>"` on v5 records — distinguishes a
    /// pre-audit row from a future binary that reports a literal
    /// `"unknown"` version string.
    #[serde(default = "default_rocky_version_sentinel")]
    pub rocky_version: String,

    /// Per-check pass/fail outcomes this run produced, one entry per executed
    /// data-quality check (flattened across every model). Empty on pre-v17
    /// records and on runs that ran no checks. Consumed by the policy plane's
    /// `verify_after` post-apply gate to confirm named checks passed.
    #[serde(default)]
    pub check_outcomes: Vec<CheckOutcome>,
}

/// One executed data-quality check's pass/fail outcome, captured on a
/// [`RunRecord`] so a later reader (the `verify_after` policy gate) can confirm
/// a named check ran and passed without re-executing it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CheckOutcome {
    /// The check's name (e.g. `row_count`, `not_null:email`), matching the
    /// `CheckResult.name` the run emitted and the names a `verify_after` rule
    /// lists.
    pub name: String,
    /// Whether the check passed.
    pub passed: bool,
}

/// Default-value contract for `RunRecord.hostname` on v5→v6 upgrade.
/// Kept as a top-level fn because `#[serde(default = "...")]` requires a
/// function path, not a closure or const.
fn default_hostname_sentinel() -> String {
    "unknown".to_string()
}

/// Default-value contract for `RunRecord.rocky_version` on v5→v6
/// upgrade. The `<…>` bracket syntax is intentionally not a valid
/// semver string — operators inspecting `rocky history --audit` can
/// tell a pre-audit row apart from any real binary at a glance.
fn default_rocky_version_sentinel() -> String {
    "<pre-audit>".to_string()
}

// ---------------------------------------------------------------------------
// Quality metrics
// ---------------------------------------------------------------------------

/// Quality metrics snapshot for a model.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QualitySnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub run_id: String,
    pub model_name: String,
    pub metrics: QualityMetrics,
}

/// Quality metrics for a single model.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QualityMetrics {
    pub row_count: u64,
    pub null_rates: std::collections::HashMap<String, f64>,
    pub freshness_lag_seconds: Option<u64>,
}

// ---------------------------------------------------------------------------
// DAG snapshots
// ---------------------------------------------------------------------------

/// A change in the DAG between snapshots.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DagChange {
    ModelAdded(String),
    ModelRemoved(String),
    ColumnAdded {
        model: String,
        column: String,
    },
    ColumnRemoved {
        model: String,
        column: String,
    },
    ColumnTypeChanged {
        model: String,
        column: String,
        from: String,
        to: String,
    },
    DependencyAdded {
        from: String,
        to: String,
    },
    DependencyRemoved {
        from: String,
        to: String,
    },
}

/// A snapshot of the DAG at a point in time.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DagSnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub graph_hash: String,
    pub model_count: usize,
    pub edge_count: usize,
    pub changes: Vec<DagChange>,
}

// ---------------------------------------------------------------------------
// Checkpoint / resume
// ---------------------------------------------------------------------------

/// Status of a single table within a run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub enum TableStatus {
    Success,
    Failed,
    Skipped,
    /// Table was cancelled by a SIGINT (Ctrl-C) before it completed —
    /// distinct from `Failed` so `--resume-latest` knows to retry it and
    /// JSON consumers can show "interrupted" in the UI.
    Interrupted,
}

/// Execution record for a single table within a run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TableProgress {
    /// Position in the execution plan.
    pub index: usize,
    /// Fully-qualified table key (catalog.schema.table).
    pub table_key: String,
    /// Asset key components.
    pub asset_key: Vec<String>,
    /// Execution status.
    pub status: TableStatus,
    /// Error message if failed.
    pub error: Option<String>,
    /// Execution duration in ms.
    pub duration_ms: u64,
    /// When this table completed.
    pub completed_at: chrono::DateTime<chrono::Utc>,
}

/// Progress for an entire run (collection of table progresses).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunProgress {
    pub run_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub total_tables: usize,
    pub tables: Vec<TableProgress>,
}

// ---------------------------------------------------------------------------
// State store: HTTP job records (`rocky serve` job model)
// ---------------------------------------------------------------------------

/// A persisted `rocky serve` long-running-job record.
///
/// The durable half of the HTTP job model (`POST /api/v1/jobs/{run|plan|apply}`,
/// `GET /api/v1/jobs/{id}`): written to the [`JOBS`] table so a
/// killed-and-restarted sidecar reports honest status for jobs it launched
/// rather than a blank slate (the in-memory registry that fronts it does not
/// survive a restart). The presentation type — `rocky-cli`'s `JobStatus` — is
/// built from this record at the API boundary; the two are kept separate so
/// this crate stays free of the `JsonSchema`/HTTP surface.
///
/// `state` and `kind` are stored as plain strings (not enums) for the same
/// forward-compatibility reason the [`RunRecord`] audit fields are
/// `#[serde(default)]`: a newer sidecar can introduce a state without a
/// blob-format break, and every field defaults so a partially-written record
/// still deserializes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PersistedJob {
    /// Opaque job identifier returned by `POST /api/v1/jobs/{kind}`.
    pub job_id: String,
    /// Job kind: `"run"`, `"plan"`, or `"apply"`.
    #[serde(default)]
    pub kind: String,
    /// Lifecycle state: `"queued"`, `"running"`, `"succeeded"`, or `"failed"`.
    #[serde(default)]
    pub state: String,
    /// When the job was submitted (RFC 3339).
    #[serde(default)]
    pub submitted_at: String,
    /// When execution started (RFC 3339), if it has.
    #[serde(default)]
    pub started_at: Option<String>,
    /// When the job reached a terminal state (RFC 3339), if it has.
    #[serde(default)]
    pub finished_at: Option<String>,
    /// Advisory, spoofable `X-Rocky-Principal` recorded for audit (never an
    /// authorization input under the single-shared-secret auth ceiling).
    #[serde(default)]
    pub principal: Option<String>,
    /// Failure detail when `state == "failed"`.
    #[serde(default)]
    pub error: Option<String>,
    /// The canonical `RunOutput` / `PlanOutput` / `ApplyOutput` the underlying
    /// `rocky <kind>` subprocess emitted, embedded verbatim, once terminal.
    #[serde(default)]
    pub result: Option<serde_json::Value>,
}

impl StateStore {
    /// Insert or replace a job record (keyed by `job_id`).
    ///
    /// A brief open-write-close: the caller opens the store, writes, and drops
    /// it, so the server never holds a long-lived write handle across a job's
    /// subprocess (which is itself the writer for `run`/`apply`).
    pub fn record_job(&self, job: &PersistedJob) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(job)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(JOBS)?;
            table.insert(job.job_id.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a job record by `job_id`.
    pub fn get_job(&self, job_id: &str) -> Result<Option<PersistedJob>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(JOBS)?;
        match table.get(job_id)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// List all persisted job records (unordered).
    pub fn list_jobs(&self) -> Result<Vec<PersistedJob>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(JOBS)?;
        let mut jobs = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry?;
            jobs.push(serde_json::from_slice(value.value())?);
        }
        Ok(jobs)
    }
}

// ---------------------------------------------------------------------------
// State store: run history, quality, DAG methods
// ---------------------------------------------------------------------------

impl StateStore {
    /// Record a pipeline run.
    pub fn record_run(&self, run: &RunRecord) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(run)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RUN_HISTORY)?;
            table.insert(run.run_id.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Get a run by ID.
    pub fn get_run(&self, run_id: &str) -> Result<Option<RunRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_HISTORY)?;
        match table.get(run_id)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// List recent runs (newest first).
    pub fn list_runs(&self, limit: usize) -> Result<Vec<RunRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_HISTORY)?;
        let mut runs = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry?;
            let run: RunRecord = serde_json::from_slice(value.value())?;
            runs.push(run);
        }
        // Sort by started_at descending
        runs.sort_by_key(|run| std::cmp::Reverse(run.started_at));
        runs.truncate(limit);
        Ok(runs)
    }

    /// Get execution history for a specific model across runs.
    pub fn get_model_history(
        &self,
        model_name: &str,
        limit: usize,
    ) -> Result<Vec<ModelExecution>, StateError> {
        let runs = self.list_runs(100)?; // look at last 100 runs
        let mut history: Vec<ModelExecution> = runs
            .into_iter()
            .flat_map(|run| {
                run.models_executed
                    .into_iter()
                    .filter(|m| m.model_name == model_name)
            })
            .collect();
        history.truncate(limit);
        Ok(history)
    }

    /// Get average duration for a model over recent runs.
    pub fn get_average_duration(
        &self,
        model_name: &str,
        window: usize,
    ) -> Result<Option<f64>, StateError> {
        let history = self.get_model_history(model_name, window)?;
        if history.is_empty() {
            return Ok(None);
        }
        let avg = history.iter().map(|m| m.duration_ms as f64).sum::<f64>() / history.len() as f64;
        Ok(Some(avg))
    }

    /// Record quality metrics for a model.
    pub fn record_quality(&self, snapshot: &QualitySnapshot) -> Result<(), StateError> {
        let key = format!("{}:{}", snapshot.model_name, snapshot.run_id);
        let bytes = serde_json::to_vec(snapshot)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(QUALITY_HISTORY)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Get quality trend for a model (recent snapshots).
    pub fn get_quality_trend(
        &self,
        model_name: &str,
        limit: usize,
    ) -> Result<Vec<QualitySnapshot>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(QUALITY_HISTORY)?;
        let prefix = format!("{model_name}:");
        let mut snapshots = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            if key.value().starts_with(&prefix) {
                let snapshot: QualitySnapshot = serde_json::from_slice(value.value())?;
                snapshots.push(snapshot);
            }
        }
        snapshots.sort_by_key(|snapshot| std::cmp::Reverse(snapshot.timestamp));
        snapshots.truncate(limit);
        Ok(snapshots)
    }

    /// Record a DAG snapshot.
    pub fn record_dag_snapshot(&self, snapshot: &DagSnapshot) -> Result<(), StateError> {
        let key = snapshot.timestamp.to_rfc3339();
        let bytes = serde_json::to_vec(snapshot)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(DAG_SNAPSHOTS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Get the latest DAG snapshot.
    pub fn get_latest_dag_snapshot(&self) -> Result<Option<DagSnapshot>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DAG_SNAPSHOTS)?;
        let mut latest: Option<DagSnapshot> = None;
        for entry in table.iter()? {
            let (_, value) = entry?;
            let snapshot: DagSnapshot = serde_json::from_slice(value.value())?;
            if latest
                .as_ref()
                .is_none_or(|l| snapshot.timestamp > l.timestamp)
            {
                latest = Some(snapshot);
            }
        }
        Ok(latest)
    }

    // -----------------------------------------------------------------------
    // Checkpoint / resume
    // -----------------------------------------------------------------------

    /// Initialize run progress with total table count.
    pub fn init_run_progress(&self, run_id: &str, total_tables: usize) -> Result<(), StateError> {
        let progress = RunProgress {
            run_id: run_id.to_string(),
            started_at: chrono::Utc::now(),
            total_tables,
            tables: Vec::new(),
        };
        let bytes = serde_json::to_vec(&progress)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RUN_PROGRESS)?;
            table.insert(run_id, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Record progress for a single table within a run.
    ///
    /// O(1) per call: serializes exactly this one [`TableProgress`] and blind-
    /// inserts it under the per-entry key `"{run_id}|{table_key}"` in
    /// [`RUN_PROGRESS_ENTRIES`]. No read-modify-write of a growing blob, so an
    /// N-table run does O(N) cumulative work instead of the old O(N²). The
    /// blind insert is also race-free under the concurrent table tasks that
    /// drive `rocky run`: there is no read-then-write window for a lost update.
    ///
    /// `table_key` is the same fully-qualified identity the resume logic dedups
    /// on, so distinct real tables never collide; re-recording the same table
    /// (e.g. an interrupted-then-final write) is last-write-wins, which matches
    /// resume semantics. One write transaction = one fsync per completed table,
    /// preserving per-table crash durability for `--resume-latest`.
    pub fn record_table_progress(
        &self,
        run_id: &str,
        progress: &TableProgress,
    ) -> Result<(), StateError> {
        let key = run_progress_entry_key(run_id, &progress.table_key);
        let bytes = serde_json::to_vec(progress)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RUN_PROGRESS_ENTRIES)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Read every per-table entry recorded for `run_id`, sorted by execution
    /// [`TableProgress::index`].
    ///
    /// Prefix-scans `"{run_id}|"` over [`RUN_PROGRESS_ENTRIES`]. The trailing
    /// `|` separator means `"run-1"` and `"run-12"` do not bleed into each
    /// other — `"run-12|..."` does not start with `"run-1|"`.
    fn read_progress_entries(
        table: &impl ReadableTable<&'static str, &'static [u8]>,
        run_id: &str,
    ) -> Result<Vec<TableProgress>, StateError> {
        let prefix = format!("{run_id}|");
        let mut entries = Vec::new();
        for entry in table.range(prefix.as_str()..)? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                break;
            }
            entries.push(serde_json::from_slice(value.value())?);
        }
        entries.sort_by_key(|t: &TableProgress| t.index);
        Ok(entries)
    }

    /// Get the progress for a run (header stitched with its per-table entries).
    ///
    /// The `tables` vector is reconstructed from [`RUN_PROGRESS_ENTRIES`]. For a
    /// run recorded by a pre-v8 binary (entries inline in the header blob) the
    /// per-entry scan is empty, so the header's own `tables` are kept — old
    /// runs continue to resume.
    pub fn get_run_progress(&self, run_id: &str) -> Result<Option<RunProgress>, StateError> {
        let txn = self.db.begin_read()?;
        let header_table = txn.open_table(RUN_PROGRESS)?;
        let Some(value) = header_table.get(run_id)? else {
            return Ok(None);
        };
        let mut progress: RunProgress = serde_json::from_slice(value.value())?;

        let entries_table = txn.open_table(RUN_PROGRESS_ENTRIES)?;
        let entries = Self::read_progress_entries(&entries_table, run_id)?;
        if !entries.is_empty() {
            progress.tables = entries;
        }
        Ok(Some(progress))
    }

    /// Get the most recent run progress (for `--resume-latest`).
    ///
    /// Picks the header with the latest `started_at`, then stitches in that
    /// run's per-table entries (with the same pre-v8 fallback as
    /// [`Self::get_run_progress`]) so the resume caller sees a populated
    /// `tables` vector.
    pub fn get_latest_run_progress(&self) -> Result<Option<RunProgress>, StateError> {
        let txn = self.db.begin_read()?;
        let header_table = txn.open_table(RUN_PROGRESS)?;
        let mut latest: Option<RunProgress> = None;
        for entry in header_table.iter()? {
            let (_, value) = entry?;
            let progress: RunProgress = serde_json::from_slice(value.value())?;
            if latest
                .as_ref()
                .is_none_or(|l| progress.started_at > l.started_at)
            {
                latest = Some(progress);
            }
        }

        let Some(mut progress) = latest else {
            return Ok(None);
        };
        let entries_table = txn.open_table(RUN_PROGRESS_ENTRIES)?;
        let entries = Self::read_progress_entries(&entries_table, &progress.run_id)?;
        if !entries.is_empty() {
            progress.tables = entries;
        }
        Ok(Some(progress))
    }
}

// ---------------------------------------------------------------------------
// State-store retention sweep (see `crate::retention::StateRetentionConfig`)
// ---------------------------------------------------------------------------

impl StateStore {
    /// Drop history rows that exceed the configured retention policy.
    ///
    /// Sweeps three tables — `run_history`, `dag_snapshots`,
    /// `quality_history` — when each is listed in
    /// [`StateRetentionConfig::applies_to`]. Operational tables
    /// (`schema_cache`, `watermarks`, `partitions`, `loaded_files`,
    /// `branches`, `idempotency_keys`, `grace_periods`, `run_progress`,
    /// `check_history`) are never touched: they hold live state, not
    /// history.
    ///
    /// For each domain:
    ///
    /// 1. Read every row and pair its key with the row's own timestamp
    ///    (`started_at` for runs, `timestamp` for DAG / quality
    ///    snapshots).
    /// 2. Sort by timestamp descending.
    /// 3. Always keep the most recent `min_runs_kept` rows, regardless of
    ///    age. Of the rest, delete those older than `now - max_age_days`.
    ///
    /// Each domain executes in its own redb write transaction, so a
    /// failure mid-sweep leaves earlier domains committed and the
    /// reported counts remain accurate up to the failure point.
    ///
    /// `min_runs_kept` is applied **per domain**, not as a global cap.
    pub fn sweep_retention(
        &self,
        policy: &crate::retention::StateRetentionConfig,
    ) -> Result<crate::retention::SweepReport, StateError> {
        use crate::retention::{StateRetentionDomain, SweepReport};

        let started = std::time::Instant::now();
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(i64::from(policy.max_age_days));
        let min_keep = policy.min_runs_kept as usize;

        let mut report = SweepReport::default();

        if policy.applies_to_domain(StateRetentionDomain::History) {
            let (deleted, kept) = self.sweep_run_history(cutoff, min_keep)?;
            report.runs_deleted = deleted;
            report.runs_kept = kept;
        } else {
            report.runs_kept = self.count_run_history()?;
        }

        if policy.applies_to_domain(StateRetentionDomain::Lineage) {
            let (deleted, kept) = self.sweep_dag_snapshots(cutoff, min_keep)?;
            report.lineage_deleted = deleted;
            report.lineage_kept = kept;
        } else {
            report.lineage_kept = self.count_dag_snapshots()?;
        }

        if policy.applies_to_domain(StateRetentionDomain::Audit) {
            let (deleted, kept) = self.sweep_quality_history(cutoff, min_keep)?;
            report.audit_deleted = deleted;
            report.audit_kept = kept;
        } else {
            report.audit_kept = self.count_quality_history()?;
        }

        report.duration_ms = started.elapsed().as_millis() as u64;
        Ok(report)
    }

    /// Dry-run counterpart to [`StateStore::sweep_retention`]: counts what
    /// *would* be deleted / kept, without opening a write transaction.
    pub fn sweep_retention_dry_run(
        &self,
        policy: &crate::retention::StateRetentionConfig,
    ) -> Result<crate::retention::SweepReport, StateError> {
        use crate::retention::{StateRetentionDomain, SweepReport};

        let started = std::time::Instant::now();
        let now = chrono::Utc::now();
        let cutoff = now - chrono::Duration::days(i64::from(policy.max_age_days));
        let min_keep = policy.min_runs_kept as usize;

        let mut report = SweepReport::default();

        if policy.applies_to_domain(StateRetentionDomain::History) {
            let (deleted, kept) = self.count_run_history_sweep(cutoff, min_keep)?;
            report.runs_deleted = deleted;
            report.runs_kept = kept;
        } else {
            report.runs_kept = self.count_run_history()?;
        }

        if policy.applies_to_domain(StateRetentionDomain::Lineage) {
            let (deleted, kept) = self.count_dag_snapshots_sweep(cutoff, min_keep)?;
            report.lineage_deleted = deleted;
            report.lineage_kept = kept;
        } else {
            report.lineage_kept = self.count_dag_snapshots()?;
        }

        if policy.applies_to_domain(StateRetentionDomain::Audit) {
            let (deleted, kept) = self.count_quality_history_sweep(cutoff, min_keep)?;
            report.audit_deleted = deleted;
            report.audit_kept = kept;
        } else {
            report.audit_kept = self.count_quality_history()?;
        }

        report.duration_ms = started.elapsed().as_millis() as u64;
        Ok(report)
    }

    /// Plan + apply the sweep against `run_history`.
    ///
    /// Reads every row, pairs its key with `started_at`, decides which
    /// keys to delete (older than `cutoff` and not in the most recent
    /// `min_keep`), then issues all deletes in a single write transaction.
    fn sweep_run_history(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_run_history_sweep(cutoff, min_keep)?;
        let total = self.count_run_history()?;
        let deleted = to_delete.len() as u64;

        if !to_delete.is_empty() {
            let txn = self.db.begin_write()?;
            {
                let mut table = txn.open_table(RUN_HISTORY)?;
                for key in &to_delete {
                    table.remove(key.as_str())?;
                }
            }
            txn.commit()?;
        }

        Ok((deleted, total.saturating_sub(deleted)))
    }

    /// Plan-only counterpart: returns `(would_delete, would_keep)` without
    /// touching redb. Shared by the dry-run path and the apply path so
    /// "what got deleted" never drifts from "what was planned".
    fn count_run_history_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_run_history_sweep(cutoff, min_keep)?;
        let total = self.count_run_history()?;
        let deleted = to_delete.len() as u64;
        Ok((deleted, total.saturating_sub(deleted)))
    }

    fn plan_run_history_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<Vec<String>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_HISTORY)?;
        let mut entries: Vec<(String, chrono::DateTime<chrono::Utc>)> = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let run: RunRecord = serde_json::from_slice(value.value())?;
            entries.push((key.value().to_string(), run.started_at));
        }
        // Newest first; the first `min_keep` are protected unconditionally.
        entries.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts));

        let to_delete: Vec<String> = entries
            .into_iter()
            .skip(min_keep)
            .filter(|(_, ts)| *ts < cutoff)
            .map(|(key, _)| key)
            .collect();
        Ok(to_delete)
    }

    fn count_run_history(&self) -> Result<u64, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_HISTORY)?;
        let mut n = 0u64;
        for entry in table.iter()? {
            let _ = entry?;
            n += 1;
        }
        Ok(n)
    }

    fn sweep_dag_snapshots(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_dag_snapshots_sweep(cutoff, min_keep)?;
        let total = self.count_dag_snapshots()?;
        let deleted = to_delete.len() as u64;

        if !to_delete.is_empty() {
            let txn = self.db.begin_write()?;
            {
                let mut table = txn.open_table(DAG_SNAPSHOTS)?;
                for key in &to_delete {
                    table.remove(key.as_str())?;
                }
            }
            txn.commit()?;
        }

        Ok((deleted, total.saturating_sub(deleted)))
    }

    fn count_dag_snapshots_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_dag_snapshots_sweep(cutoff, min_keep)?;
        let total = self.count_dag_snapshots()?;
        let deleted = to_delete.len() as u64;
        Ok((deleted, total.saturating_sub(deleted)))
    }

    fn plan_dag_snapshots_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<Vec<String>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DAG_SNAPSHOTS)?;
        let mut entries: Vec<(String, chrono::DateTime<chrono::Utc>)> = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let snap: DagSnapshot = serde_json::from_slice(value.value())?;
            entries.push((key.value().to_string(), snap.timestamp));
        }
        entries.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts));

        let to_delete: Vec<String> = entries
            .into_iter()
            .skip(min_keep)
            .filter(|(_, ts)| *ts < cutoff)
            .map(|(key, _)| key)
            .collect();
        Ok(to_delete)
    }

    fn count_dag_snapshots(&self) -> Result<u64, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DAG_SNAPSHOTS)?;
        let mut n = 0u64;
        for entry in table.iter()? {
            let _ = entry?;
            n += 1;
        }
        Ok(n)
    }

    fn sweep_quality_history(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_quality_history_sweep(cutoff, min_keep)?;
        let total = self.count_quality_history()?;
        let deleted = to_delete.len() as u64;

        if !to_delete.is_empty() {
            let txn = self.db.begin_write()?;
            {
                let mut table = txn.open_table(QUALITY_HISTORY)?;
                for key in &to_delete {
                    table.remove(key.as_str())?;
                }
            }
            txn.commit()?;
        }

        Ok((deleted, total.saturating_sub(deleted)))
    }

    fn count_quality_history_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<(u64, u64), StateError> {
        let to_delete = self.plan_quality_history_sweep(cutoff, min_keep)?;
        let total = self.count_quality_history()?;
        let deleted = to_delete.len() as u64;
        Ok((deleted, total.saturating_sub(deleted)))
    }

    fn plan_quality_history_sweep(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        min_keep: usize,
    ) -> Result<Vec<String>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(QUALITY_HISTORY)?;
        let mut entries: Vec<(String, chrono::DateTime<chrono::Utc>)> = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let snap: QualitySnapshot = serde_json::from_slice(value.value())?;
            entries.push((key.value().to_string(), snap.timestamp));
        }
        entries.sort_by_key(|(_, ts)| std::cmp::Reverse(*ts));

        let to_delete: Vec<String> = entries
            .into_iter()
            .skip(min_keep)
            .filter(|(_, ts)| *ts < cutoff)
            .map(|(key, _)| key)
            .collect();
        Ok(to_delete)
    }

    fn count_quality_history(&self) -> Result<u64, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(QUALITY_HISTORY)?;
        let mut n = 0u64;
        for entry in table.iter()? {
            let _ = entry?;
            n += 1;
        }
        Ok(n)
    }

    /// Read the timestamp of the most recent successful end-of-run
    /// auto-sweep, if any.
    ///
    /// Returns `Ok(None)` when the key is absent (fresh store, or older
    /// rocky binary that never wrote it). Returns `Err` only when the
    /// stored value is present but unparseable as RFC3339 — that is a
    /// genuine corruption signal, not a missing-key signal, and the
    /// caller should treat it as "do not auto-sweep this run" so the
    /// state store stays unchanged until an operator inspects it.
    ///
    /// The auto-sweep gate at end-of-run reads this value and skips
    /// sweeping when `now - last < sweep_interval_seconds`. The manual
    /// `rocky state retention sweep` subcommand never consults this
    /// field — it always runs.
    pub fn get_last_retention_sweep_at(
        &self,
    ) -> Result<Option<chrono::DateTime<chrono::Utc>>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(METADATA)?;
        let Some(value) = table.get(LAST_RETENTION_SWEEP_AT_KEY)? else {
            return Ok(None);
        };
        let parsed = chrono::DateTime::parse_from_rfc3339(value.value())
            .map_err(|e| StateError::VersionParse(format!("last_retention_sweep_at: {e}")))?
            .with_timezone(&chrono::Utc);
        Ok(Some(parsed))
    }

    /// Stamp the `last_retention_sweep_at` metadata key to `at`,
    /// serialised as RFC3339.
    ///
    /// Called by the end-of-run auto-sweep regardless of whether the
    /// sweep stayed inside its budget — the stamp tracks "when did we
    /// last attempt", not "when did we last finish quickly". That keeps
    /// the interval gate from firing every run on a state store that
    /// consistently breaches the soft budget.
    pub fn set_last_retention_sweep_at(
        &self,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StateError> {
        let formatted = at.to_rfc3339();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(METADATA)?;
            table.insert(LAST_RETENTION_SWEEP_AT_KEY, formatted.as_str())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Read the timestamp of the most recent `--since last` estate digest.
    ///
    /// Backs the `rocky brief --since last` cursor: the "everything up to
    /// here has already been briefed" watermark. `None` when no digest has
    /// ever been taken (the very first `--since last` therefore reports the
    /// full history). Returns `Err` only when a stored value is present but
    /// unparseable as RFC3339 — a genuine corruption signal, not a
    /// missing-key signal.
    ///
    /// Stored in the existing [`METADATA`] table under a dedicated key, so it
    /// adds no table and moves no schema version.
    pub fn get_last_brief_at(&self) -> Result<Option<chrono::DateTime<chrono::Utc>>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(METADATA)?;
        let Some(value) = table.get(LAST_BRIEF_AT_KEY)? else {
            return Ok(None);
        };
        let parsed = chrono::DateTime::parse_from_rfc3339(value.value())
            .map_err(|e| StateError::VersionParse(format!("last_brief_at: {e}")))?
            .with_timezone(&chrono::Utc);
        Ok(Some(parsed))
    }

    /// Advance the `last_brief_at` digest cursor to `at`, serialised as
    /// RFC3339.
    ///
    /// Called after a `rocky brief --since last` successfully renders, so the
    /// next `--since last` digest starts where this one ended. Relative
    /// windows (`--since 24h` / `--since 7d`) never touch the cursor.
    pub fn set_last_brief_at(&self, at: chrono::DateTime<chrono::Utc>) -> Result<(), StateError> {
        let formatted = at.to_rfc3339();
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(METADATA)?;
            table.insert(LAST_BRIEF_AT_KEY, formatted.as_str())?;
        }
        txn.commit()?;
        Ok(())
    }
}

/// Metadata key for the timestamp of the most recent end-of-run
/// auto-sweep (see [`StateStore::get_last_retention_sweep_at`]).
const LAST_RETENTION_SWEEP_AT_KEY: &str = "last_retention_sweep_at";

/// Metadata key for the `rocky brief --since last` digest cursor (see
/// [`StateStore::get_last_brief_at`]).
const LAST_BRIEF_AT_KEY: &str = "last_brief_at";

// ---------------------------------------------------------------------------
// Idempotency-key persistence (see `crate::idempotency`)
// ---------------------------------------------------------------------------

impl StateStore {
    /// Read an idempotency entry by key.
    pub fn idempotency_get(
        &self,
        key: &str,
    ) -> Result<Option<crate::idempotency::IdempotencyEntry>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(IDEMPOTENCY_KEYS)?;
        match table.get(key)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Insert or replace an idempotency entry. Used by the Tiered backend's
    /// best-effort redb mirror; prefer
    /// [`StateStore::idempotency_check_and_claim`] for racing callers.
    pub fn idempotency_put(
        &self,
        entry: &crate::idempotency::IdempotencyEntry,
    ) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(entry)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(IDEMPOTENCY_KEYS)?;
            table.insert(entry.key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Delete an idempotency entry. Returns `true` if the key was present.
    pub fn idempotency_delete(&self, key: &str) -> Result<bool, StateError> {
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(IDEMPOTENCY_KEYS)?;
            removed = table.remove(key)?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Atomically inspect the idempotency table and claim the key if no
    /// non-stale entry already holds it.
    ///
    /// Everything happens inside one redb write transaction — because the
    /// `StateStore` holds the advisory `state.redb.lock`, we also have
    /// per-process exclusion. Combined, the local backend provides true
    /// "check-if-absent-then-claim" semantics without a second primitive.
    pub fn idempotency_check_and_claim(
        &self,
        new_entry: &crate::idempotency::IdempotencyEntry,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<crate::idempotency::IdempotencyCheck, StateError> {
        use crate::idempotency::{IdempotencyCheck, IdempotencyEntry, classify_existing};

        let new_bytes = serde_json::to_vec(new_entry)?;
        let txn = self.db.begin_write()?;
        let result;
        {
            let mut table = txn.open_table(IDEMPOTENCY_KEYS)?;
            let prior_entry: Option<IdempotencyEntry> = match table.get(new_entry.key.as_str())? {
                Some(value) => Some(serde_json::from_slice(value.value())?),
                None => None,
            };

            result = match prior_entry {
                None => {
                    table.insert(new_entry.key.as_str(), new_bytes.as_slice())?;
                    IdempotencyCheck::Proceed
                }
                Some(prior) => {
                    let verdict = classify_existing(prior, now, new_entry);
                    match &verdict {
                        IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. } => {
                            // Overwrite the stale entry with the fresh InFlight claim.
                            table.insert(new_entry.key.as_str(), new_bytes.as_slice())?;
                        }
                        IdempotencyCheck::SkipCompleted { .. }
                        | IdempotencyCheck::SkipInFlight { .. } => {
                            // Leave the prior entry untouched.
                        }
                    }
                    verdict
                }
            };
        }
        txn.commit()?;
        Ok(result)
    }

    /// Finalize an idempotency entry — update to `Succeeded` / `Failed`, or
    /// delete the entry entirely when [`crate::config::DedupPolicy::Success`]
    /// + failure semantics mean retries should proceed.
    ///
    /// No-op (returns `Ok(())`) if the key is absent or held by a different
    /// `run_id` — another caller already finalized it, which is not an error
    /// (e.g. a `rocky replay` with `--force` that bypassed idempotency mid-run).
    pub fn idempotency_finalize(
        &self,
        key: &str,
        run_id: &str,
        outcome: crate::idempotency::FinalOutcome,
        retention_until: chrono::DateTime<chrono::Utc>,
        dedup_on: crate::config::DedupPolicy,
    ) -> Result<(), StateError> {
        use crate::config::DedupPolicy;
        use crate::idempotency::{FinalOutcome, IdempotencyEntry, IdempotencyState};

        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(IDEMPOTENCY_KEYS)?;
            let existing: Option<IdempotencyEntry> = match table.get(key)? {
                Some(value) => Some(serde_json::from_slice(value.value())?),
                None => None,
            };
            match existing {
                None => {
                    // Nothing to finalize — already swept, or claim was never
                    // written (e.g. tiered mirror race). Silently succeed.
                }
                Some(prior) if prior.run_id != run_id => {
                    // Another run owns the key now (AdoptStale path). Leave alone.
                }
                Some(prior) => {
                    let should_keep = match (outcome, dedup_on) {
                        (FinalOutcome::Succeeded, _) => true,
                        (FinalOutcome::Failed, DedupPolicy::Any) => true,
                        (FinalOutcome::Failed, DedupPolicy::Success) => false,
                    };
                    if should_keep {
                        let updated = IdempotencyEntry {
                            key: prior.key,
                            run_id: prior.run_id,
                            state: match outcome {
                                FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                                FinalOutcome::Failed => IdempotencyState::Failed,
                            },
                            stamped_at: chrono::Utc::now(),
                            expires_at: retention_until,
                            dedup_on,
                        };
                        let bytes = serde_json::to_vec(&updated)?;
                        table.insert(key, bytes.as_slice())?;
                    } else {
                        table.remove(key)?;
                    }
                }
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Sweep expired idempotency entries.
    ///
    /// Removes:
    /// - Entries whose `expires_at < now` (retention exhausted).
    /// - Entries still in `InFlight` whose `stamped_at + in_flight_ttl_hours`
    ///   is in the past (crashed-pod corpses missed by the `AdoptStale`
    ///   reader path — e.g. a claim whose run crashed and no follow-up run
    ///   ever tried the same key).
    ///
    /// Returns the count removed.
    pub fn idempotency_sweep_expired(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        in_flight_ttl_hours: u32,
    ) -> Result<usize, StateError> {
        use crate::idempotency::{IdempotencyEntry, IdempotencyState};

        let in_flight_cutoff = now - chrono::Duration::hours(in_flight_ttl_hours as i64);

        // Phase 1: collect keys to drop (can't mutate during iter).
        let to_drop: Vec<String> = {
            let txn = self.db.begin_read()?;
            let table = txn.open_table(IDEMPOTENCY_KEYS)?;
            let mut drop = Vec::new();
            for entry in table.iter()? {
                let (key, value) = entry?;
                let parsed: IdempotencyEntry = serde_json::from_slice(value.value())?;
                let stale = match parsed.state {
                    IdempotencyState::InFlight => parsed.stamped_at < in_flight_cutoff,
                    IdempotencyState::Succeeded | IdempotencyState::Failed => {
                        parsed.expires_at < now
                    }
                };
                if stale {
                    drop.push(key.value().to_string());
                }
            }
            drop
        };

        if to_drop.is_empty() {
            return Ok(0);
        }

        let txn = self.db.begin_write()?;
        let mut removed = 0;
        {
            let mut table = txn.open_table(IDEMPOTENCY_KEYS)?;
            for key in &to_drop {
                if table.remove(key.as_str())?.is_some() {
                    removed += 1;
                }
            }
        }
        txn.commit()?;
        Ok(removed)
    }
}

// ---------------------------------------------------------------------------
// Grace-period column drop tracking
// ---------------------------------------------------------------------------

/// Record for a column that exists in the target but has been dropped from
/// the source. Stored in the `GRACE_PERIODS` redb table.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct GracePeriodRecord {
    /// Fully-qualified table key (catalog.schema.table).
    pub table_key: String,
    /// Column name in the target table.
    pub column_name: String,
    /// Column data type in the target table.
    pub data_type: String,
    /// When the column was first detected as missing from the source.
    pub first_seen_at: chrono::DateTime<chrono::Utc>,
    /// When the grace period expires (first_seen_at + grace_period_days).
    pub expires_at: chrono::DateTime<chrono::Utc>,
}

/// Compose the redb key for a `GracePeriodRecord`.
fn grace_period_key(table_key: &str, column_name: &str) -> String {
    format!("{table_key}|{column_name}")
}

impl StateStore {
    /// Records a new grace-period entry for a column.
    pub fn set_grace_period(&self, record: &GracePeriodRecord) -> Result<(), StateError> {
        let key = grace_period_key(&record.table_key, &record.column_name);
        let bytes = serde_json::to_vec(record)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(GRACE_PERIODS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Gets the grace-period record for a specific table + column.
    pub fn get_grace_period(
        &self,
        table_key: &str,
        column_name: &str,
    ) -> Result<Option<GracePeriodRecord>, StateError> {
        let key = grace_period_key(table_key, column_name);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GRACE_PERIODS)?;
        match table.get(key.as_str())? {
            Some(value) => {
                let record: GracePeriodRecord = serde_json::from_slice(value.value())?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Lists all grace-period records for a given table.
    pub fn list_grace_periods(
        &self,
        table_key: &str,
    ) -> Result<Vec<GracePeriodRecord>, StateError> {
        let prefix = format!("{table_key}|");
        let txn = self.db.begin_read()?;
        let table = txn.open_table(GRACE_PERIODS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            if !key.value().starts_with(&prefix) {
                continue;
            }
            let record: GracePeriodRecord = serde_json::from_slice(value.value())?;
            results.push(record);
        }
        Ok(results)
    }

    /// Deletes the grace-period record for a column (e.g., when the column
    /// reappears in the source or after the column is actually dropped).
    /// Returns true if a record was removed.
    pub fn delete_grace_period(
        &self,
        table_key: &str,
        column_name: &str,
    ) -> Result<bool, StateError> {
        let key = grace_period_key(table_key, column_name);
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(GRACE_PERIODS)?;
            removed = table.remove(key.as_str())?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    // ------------------------------------------------------------------
    // Loaded-files tracking (load pipeline)
    // ------------------------------------------------------------------

    /// Records a successfully loaded file for a pipeline.
    ///
    /// Key: `"{pipeline}|{file_path}"`.
    pub fn record_loaded_file(
        &self,
        pipeline: &str,
        file_path: &str,
        record: &LoadedFileRecord,
    ) -> Result<(), StateError> {
        let key = loaded_file_key(pipeline, file_path);
        let bytes = serde_json::to_vec(record)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(LOADED_FILES)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Gets the record for a previously loaded file.
    pub fn get_loaded_file(
        &self,
        pipeline: &str,
        file_path: &str,
    ) -> Result<Option<LoadedFileRecord>, StateError> {
        let key = loaded_file_key(pipeline, file_path);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOADED_FILES)?;
        match table.get(key.as_str())? {
            Some(value) => {
                let record: LoadedFileRecord = serde_json::from_slice(value.value())?;
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    /// Lists all loaded files for a pipeline.
    pub fn list_loaded_files(
        &self,
        pipeline: &str,
    ) -> Result<Vec<(String, LoadedFileRecord)>, StateError> {
        let prefix = format!("{pipeline}|");
        let txn = self.db.begin_read()?;
        let table = txn.open_table(LOADED_FILES)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let key_str = key.value();
            if key_str.starts_with(&prefix) {
                let file_path = key_str[prefix.len()..].to_string();
                let record: LoadedFileRecord = serde_json::from_slice(value.value())?;
                results.push((file_path, record));
            }
        }
        Ok(results)
    }

    /// Deletes the record for a loaded file.
    /// Returns true if a record was removed.
    pub fn delete_loaded_file(&self, pipeline: &str, file_path: &str) -> Result<bool, StateError> {
        let key = loaded_file_key(pipeline, file_path);
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(LOADED_FILES)?;
            removed = table.remove(key.as_str())?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Output artifacts (content-addressed write ledger; Phase 6 input)
    // -----------------------------------------------------------------------

    /// Record one content-addressed parquet write.
    ///
    /// Called once per
    /// `rocky_iceberg::uniform_writer::WriteResult` produced by
    /// `execute_content_addressed_model` — i.e. once for an unpartitioned
    /// write and once per partition group for a partitioned write. The
    /// ledger is the source of truth for Phase 6 VACUUM refcount
    /// queries; the present method only writes, it does not decrement.
    ///
    /// Key composition: `"{run_id}|{model_name}|{file_path}"`. The triple
    /// is globally unique (`file_path` already incorporates the per-table
    /// storage prefix; same run + same model + same file_path means
    /// we're idempotently re-recording the same write, e.g. on a
    /// cond-put retry winner that overwrites a loser's stamp).
    pub fn record_artifact(&self, artifact: &ArtifactRecord) -> Result<(), StateError> {
        let key = artifact_key(&artifact.run_id, &artifact.model_name, &artifact.file_path);
        let bytes = serde_json::to_vec(artifact)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(OUTPUT_ARTIFACTS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Agent-policy decision ledger (POLICY_DECISIONS)
    // -----------------------------------------------------------------------

    /// Append a policy decision to the ledger.
    ///
    /// Called once per policy *evaluation* at a mutating enforcement seam
    /// (`rocky apply` / promote), for both `allow` and gate (`require_review`
    /// / `deny`) outcomes. Reads are never recorded — they short-circuit
    /// before evaluation. Idempotent for an identical `(timestamp, plan_id,
    /// model)` triple (re-recording overwrites the same row).
    pub fn record_policy_decision(
        &self,
        decision: &PolicyDecisionRecord,
    ) -> Result<(), StateError> {
        let key = policy_decision_key(&decision.timestamp, &decision.plan_id, &decision.model);
        let bytes = serde_json::to_vec(decision)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(POLICY_DECISIONS)?;
            table.insert(key.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Return every recorded policy decision, oldest first.
    ///
    /// Keys are `"{timestamp_rfc3339}|{plan_id}|{model}"`, which sort
    /// chronologically, so a forward table scan yields decisions in the order
    /// they were recorded. Backs `rocky audit`.
    pub fn list_policy_decisions(&self) -> Result<Vec<PolicyDecisionRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(POLICY_DECISIONS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (_key, value) = entry?;
            let record: PolicyDecisionRecord = serde_json::from_slice(value.value())?;
            results.push(record);
        }
        Ok(results)
    }

    // -----------------------------------------------------------------------
    // Input-match index + provenance (auditable-reuse spine — dormant)
    // -----------------------------------------------------------------------

    /// Record input-index entries and provenance records for a run in a
    /// single write transaction.
    ///
    /// Called once per successful run, from the persist path, when `[reuse]`
    /// is enabled. Batching both tables into one `begin_write` / `commit`
    /// keeps the cost to a single extra fsync per run regardless of model
    /// count — the same posture [`Self::batch_set_watermarks`] takes.
    ///
    /// The two slices are written independently (a model may contribute an
    /// index entry, a provenance record, or both); callers typically pass
    /// index entries and provenance records for the same set of models. An
    /// empty pair is a no-op (no transaction is opened).
    ///
    /// **Dormant in Stage 1.** This only *writes* the spine; no reuse
    /// decision reads it. The index attests an *input-logic match*, never
    /// reproduction of an output.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] if any record fails to serialize or the
    /// transaction fails. The whole batch is atomic — a partial write is
    /// never observable.
    pub fn record_reuse_spine(
        &self,
        index_entries: &[InputIndexEntry],
        provenance: &[ProvenanceRecord],
    ) -> Result<(), StateError> {
        if index_entries.is_empty() && provenance.is_empty() {
            return Ok(());
        }
        let txn = self.db.begin_write()?;
        {
            let mut index = txn.open_table(INPUT_INDEX)?;
            for entry in index_entries {
                let bytes = serde_json::to_vec(entry)?;
                index.insert(entry.input_hash.as_str(), bytes.as_slice())?;
            }
            let mut prov = txn.open_table(INPUT_PROVENANCE)?;
            for record in provenance {
                let key = provenance_key(&record.run_id, &record.model_name);
                let bytes = serde_json::to_vec(record)?;
                prov.insert(key.as_str(), bytes.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(())
    }

    /// Look up the indexed prior run for a model's `input_hash`.
    ///
    /// **Provided for Stage 2 to consume — nothing in Stage 1 calls this for
    /// a reuse decision.** Returns the [`InputIndexEntry`] (locating the
    /// prior run + its output artifact) when an entry exists for the given
    /// `input_hash`, else `None`.
    ///
    /// A hit attests an *input-logic match* against a prior build. It is a
    /// reuse *candidate* lookup only: a fail-closed Stage-2 decision must
    /// still verify the artifact and refcount before reusing any bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] when the read transaction or deserialize fails.
    pub fn get_by_input_hash(
        &self,
        input_hash: &str,
    ) -> Result<Option<InputIndexEntry>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INPUT_INDEX)?;
        match table.get(input_hash)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Fetch the provenance record for a `(run_id, model_name)` build.
    ///
    /// The input to the offline-verify recipe: an auditor reads this,
    /// deserializes the embedded canonical `ModelIr` JSON, recomputes its
    /// `skip_hash`, and `b3sum`s the recorded parquet. Returns `None` when no
    /// provenance was recorded for that build.
    ///
    /// # Errors
    ///
    /// Returns [`StateError`] when the read transaction or deserialize fails.
    pub fn get_provenance(
        &self,
        run_id: &str,
        model_name: &str,
    ) -> Result<Option<ProvenanceRecord>, StateError> {
        let key = provenance_key(run_id, model_name);
        let txn = self.db.begin_read()?;
        let table = txn.open_table(INPUT_PROVENANCE)?;
        match table.get(key.as_str())? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Return every artifact record whose `blake3_hash` matches.
    ///
    /// Phase 6 VACUUM uses this to answer "is this file still referenced
    /// by any run?" — an empty result means the parquet bytes are
    /// orphan candidates. O(N) full table scan today; if N grows past
    /// the comfortable single-digit-millisecond range a secondary
    /// `ARTIFACTS_BY_HASH` index can back this method without changing
    /// the signature.
    pub fn list_artifacts_by_hash(
        &self,
        blake3_hash: &str,
    ) -> Result<Vec<ArtifactRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(OUTPUT_ARTIFACTS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (_key, value) = entry?;
            let record: ArtifactRecord = serde_json::from_slice(value.value())?;
            if record.blake3_hash == blake3_hash {
                results.push(record);
            }
        }
        Ok(results)
    }

    /// List every artifact recorded for a given run.
    ///
    /// Walks the table keyed-prefix-style on `"{run_id}|"`. Useful for
    /// the eventual `rocky history --artifacts` surface and for tests
    /// that need to assert the per-run write count.
    pub fn list_artifacts_for_run(&self, run_id: &str) -> Result<Vec<ArtifactRecord>, StateError> {
        let prefix = format!("{run_id}|");
        let txn = self.db.begin_read()?;
        let table = txn.open_table(OUTPUT_ARTIFACTS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            if key.value().starts_with(&prefix) {
                let record: ArtifactRecord = serde_json::from_slice(value.value())?;
                results.push(record);
            }
        }
        Ok(results)
    }

    /// Total artifact count. Cheap sanity-check used by tests; future
    /// retention sweep planning will key off this too.
    pub fn count_artifacts(&self) -> Result<u64, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(OUTPUT_ARTIFACTS)?;
        let mut count: u64 = 0;
        for entry in table.iter()? {
            entry?;
            count += 1;
        }
        Ok(count)
    }

    /// Return every artifact record in the content-addressed ledger.
    ///
    /// One full table scan over [`OUTPUT_ARTIFACTS`]. Backs the read-only
    /// derivability inventory (`rocky gc --derivable --dry-run`), which needs
    /// the whole candidate universe to group by content hash, sum managed
    /// bytes, and join each artifact to its provenance + refcount. Rows are
    /// returned in the table's key order (`"{run_id}|{model_name}|…"`); the
    /// caller imposes any grouping or ordering it needs.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Db`] when the redb transaction fails to open the
    /// table or iterate, and [`StateError::Serialize`] when a ledger row fails
    /// to deserialize.
    pub fn list_all_artifacts(&self) -> Result<Vec<ArtifactRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(OUTPUT_ARTIFACTS)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (_key, value) = entry?;
            let record: ArtifactRecord = serde_json::from_slice(value.value())?;
            results.push(record);
        }
        Ok(results)
    }

    /// Count the number of ledger rows pointing at a given `blake3_hash`.
    ///
    /// This is the VACUUM refcount primitive. A content-addressed parquet
    /// file is safe to physically delete only when its hash refcount is
    /// zero — every recorded reference (a run+model pair that materialised
    /// the same bytes) must be retired first. A refcount above one means
    /// the bytes are shared (e.g. across branches or replayed runs) and
    /// deleting them would corrupt every other referer.
    ///
    /// O(N) full table scan today; if the ledger grows past the
    /// comfortable single-digit-millisecond range a secondary
    /// `ARTIFACTS_BY_HASH` index can back this method without changing
    /// the signature. The wider value here is **honest counting**, not
    /// raw speed: VACUUM consumers only need the boolean refcount==0
    /// answer, but exposing the integer lets callers log "kept N
    /// references" for ops debugging.
    ///
    /// # Errors
    ///
    /// Returns [`StateError::Db`] when the redb transaction fails to
    /// open the table or iterate, and [`StateError::Serialize`] when a
    /// ledger row fails to deserialize. Callers in a VACUUM path
    /// should treat both as "do not delete" — fail closed.
    pub fn refcount_for_hash(&self, blake3_hash: &str) -> Result<u64, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(OUTPUT_ARTIFACTS)?;
        let mut count: u64 = 0;
        for entry in table.iter()? {
            let (_key, value) = entry?;
            let record: ArtifactRecord = serde_json::from_slice(value.value())?;
            if record.blake3_hash == blake3_hash {
                count += 1;
            }
        }
        Ok(count)
    }

    // -----------------------------------------------------------------------
    // Branches (schema-prefix virtual branches)
    // -----------------------------------------------------------------------

    /// Record (create or overwrite) a branch.
    pub fn put_branch(&self, branch: &BranchRecord) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(branch)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(BRANCHES)?;
            table.insert(branch.name.as_str(), bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Fetch a branch by name.
    pub fn get_branch(&self, name: &str) -> Result<Option<BranchRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BRANCHES)?;
        match table.get(name)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// List all branches (sorted by name).
    pub fn list_branches(&self) -> Result<Vec<BranchRecord>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(BRANCHES)?;
        let mut branches = Vec::new();
        for entry in table.iter()? {
            let (_, value) = entry?;
            branches.push(serde_json::from_slice(value.value())?);
        }
        branches.sort_by(|a: &BranchRecord, b| a.name.cmp(&b.name));
        Ok(branches)
    }

    /// Delete a branch. Returns true if a record was removed.
    pub fn delete_branch(&self, name: &str) -> Result<bool, StateError> {
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(BRANCHES)?;
            removed = table.remove(name)?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    // -----------------------------------------------------------------------
    // Schema cache
    // -----------------------------------------------------------------------

    /// Read a single schema cache entry by key.
    ///
    /// Key shape is `"<catalog>.<schema>.<table>"` — see
    /// [`crate::schema_cache::schema_cache_key`]. Returns `None` when the key
    /// is absent; the compiler-side loader treats that as a miss and falls
    /// back to the existing `HashMap::new()` behaviour (which typechecks the
    /// leaf columns as `RockyType::Unknown`).
    pub fn read_schema_cache_entry(
        &self,
        key: &str,
    ) -> Result<Option<SchemaCacheEntry>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(SCHEMA_CACHE)?;
        match table.get(key)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Write a schema cache entry.
    ///
    /// Always overwrites — the wave-2 design is TTL-bounded staleness, not
    /// versioned entries. Callers who want drift detection compare the old
    /// and new entries themselves before calling (that logic belongs in the
    /// `rocky run` write tap landing in PR 2).
    pub fn write_schema_cache_entry(
        &self,
        key: &str,
        entry: &SchemaCacheEntry,
    ) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(entry)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SCHEMA_CACHE)?;
            table.insert(key, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Write multiple schema cache entries in a single write transaction.
    ///
    /// Mirrors [`Self::batch_set_watermarks`]: one `begin_write` / `commit`
    /// cycle covers every entry instead of one fsync per entry. The hot caller
    /// is `rocky discover --with-schemas`, which describes a whole source
    /// schema at a time — collapsing N per-entry commits into one transaction
    /// turns N fsyncs into one.
    ///
    /// Returns the number of entries written. If any entry fails to serialize
    /// or insert, the whole transaction is rolled back — partial cache writes
    /// are not observable to subsequent reads. (This is all-or-nothing, unlike
    /// the per-entry loop's partial-success posture; callers that want
    /// per-schema isolation should batch one schema at a time.)
    pub fn batch_write_schema_cache_entries(
        &self,
        entries: &[(&str, &SchemaCacheEntry)],
    ) -> Result<usize, StateError> {
        if entries.is_empty() {
            return Ok(0);
        }
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(SCHEMA_CACHE)?;
            for (key, entry) in entries {
                let bytes = serde_json::to_vec(*entry)?;
                table.insert(*key, bytes.as_slice())?;
            }
        }
        txn.commit()?;
        Ok(entries.len())
    }

    /// Read the prior `rocky discover` source inventory for `pipeline`.
    ///
    /// Returns the JSON-decoded `Vec<String>` of source schemas persisted by
    /// the last [`Self::put_discover_snapshot`], or `None` when this pipeline
    /// has never been discovered (the first-run baseline case).
    pub fn get_discover_snapshot(&self, pipeline: &str) -> Result<Option<Vec<String>>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(DISCOVER_SNAPSHOTS)?;
        match table.get(pipeline)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Persist the current `rocky discover` source inventory for `pipeline`,
    /// overwriting any prior snapshot. `sources` should be the deduplicated,
    /// sorted list of discovered source schemas so the stored baseline is
    /// deterministic.
    pub fn put_discover_snapshot(
        &self,
        pipeline: &str,
        sources: &[String],
    ) -> Result<(), StateError> {
        let bytes = serde_json::to_vec(sources)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(DISCOVER_SNAPSHOTS)?;
            table.insert(pipeline, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Delete a single schema cache entry. Returns `true` when a row was
    /// actually removed, `false` when the key was already absent.
    ///
    /// Used by the future `rocky state clear-schema-cache [--key ...]`
    /// subcommand (wiring in PR 4). Defined now so the read path and the
    /// admin path share one CRUD surface.
    pub fn delete_schema_cache_entry(&self, key: &str) -> Result<bool, StateError> {
        let txn = self.db.begin_write()?;
        let removed;
        {
            let mut table = txn.open_table(SCHEMA_CACHE)?;
            removed = table.remove(key)?.is_some();
        }
        txn.commit()?;
        Ok(removed)
    }

    /// Enumerate the full contents of the schema cache.
    ///
    /// Returns `(key, entry)` tuples in arbitrary order. The compiler-side
    /// loader prefers this over per-key reads because typecheck wants the
    /// whole `<schema>.<table> -> Vec<TypedColumn>` map at once, and redb's
    /// single-table scan is measurably cheaper than N point reads when the
    /// cache has more than a handful of entries.
    ///
    /// Expired entries are NOT filtered here — the TTL check lives in the
    /// compiler-side loader so that deletion (clear-schema-cache) and read
    /// (typecheck) have a single source of truth for the eviction decision.
    pub fn list_schema_cache(&self) -> Result<Vec<(String, SchemaCacheEntry)>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(SCHEMA_CACHE)?;
        let mut results = Vec::new();
        for entry in table.iter()? {
            let (key, value) = entry?;
            let cached: SchemaCacheEntry = serde_json::from_slice(value.value())?;
            results.push((key.value().to_string(), cached));
        }
        Ok(results)
    }
}

/// A named virtual branch.
///
/// Persisted in the state store. When `rocky run --branch <name>` is invoked,
/// the pipeline executes against tables whose schema has been suffixed with
/// `schema_prefix`, i.e. a named, persistent form of the existing `--shadow`
/// mode. Warehouse-native clones (Delta `SHALLOW CLONE` / Snowflake
/// zero-copy `CLONE`) are a follow-up — schema-prefix branches work uniformly
/// across every adapter without dialect-specific SQL.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BranchRecord {
    /// Branch name (CLI-facing identifier).
    pub name: String,
    /// Token appended to every target schema when the branch is active.
    ///
    /// Example: branch name `fix-price` → `schema_prefix = "branch__fix-price"`
    /// → a model targeting `prod.sales` materializes to `prod.sales__branch__fix-price`
    /// when run with `--branch fix-price`.
    pub schema_prefix: String,
    /// Creator of the branch (user name resolved from `$USER` at create time).
    pub created_by: String,
    /// Creation timestamp (UTC).
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// Optional free-form description.
    pub description: Option<String>,
}

/// Builds the key for the LOADED_FILES table.
fn loaded_file_key(pipeline: &str, file_path: &str) -> String {
    format!("{pipeline}|{file_path}")
}

/// Builds the key for the [`RUN_PROGRESS_ENTRIES`] table.
///
/// Composition: `"{run_id}|{table_key}"`. The `table_key` is the
/// fully-qualified `catalog.schema.table`, which is unique per (run, table) —
/// the same identity the resume logic dedups on. The trailing `|` separator
/// keeps prefix scans for "all entries in run X" unambiguous (`"run-1"` and
/// `"run-12"` do not collide).
fn run_progress_entry_key(run_id: &str, table_key: &str) -> String {
    format!("{run_id}|{table_key}")
}

/// Builds the key for the [`OUTPUT_ARTIFACTS`] table.
///
/// Composition: `"{run_id}|{model_name}|{file_path}"`. The triple is
/// globally unique today (`file_path` already incorporates the per-table
/// storage prefix; the `run_id` prefix anchors the row to a single run).
/// The same separator pattern as `PARTITIONS` / `LOADED_FILES` keeps
/// prefix scans cheap (`starts_with("{run_id}|")` for "all artifacts in
/// run X").
fn artifact_key(run_id: &str, model_name: &str, file_path: &str) -> String {
    format!("{run_id}|{model_name}|{file_path}")
}

/// Builds the key for the [`INPUT_PROVENANCE`] table.
///
/// Composition: `"{run_id}|{model_name}"` — unique per (run, model). The
/// trailing-`|`-free pair is unambiguous because `model_name` is a validated
/// identifier with no `|`. Mirrors the per-run anchoring of
/// [`artifact_key`].
fn provenance_key(run_id: &str, model_name: &str) -> String {
    format!("{run_id}|{model_name}")
}

/// Builds the key for the [`POLICY_DECISIONS`] table.
///
/// Composition: `"{timestamp_rfc3339}|{plan_id}|{model}"`. The RFC 3339
/// timestamp prefix sorts chronologically under redb's lexicographic key
/// order, so a forward scan replays decisions oldest-first; `plan_id` +
/// `model` disambiguate multiple decisions recorded within the same
/// timestamp tick.
fn policy_decision_key(
    timestamp: &chrono::DateTime<chrono::Utc>,
    plan_id: &str,
    model: &str,
) -> String {
    format!("{}|{plan_id}|{model}", timestamp.to_rfc3339())
}

/// One row in the content-addressed write ledger ([`OUTPUT_ARTIFACTS`]).
///
/// Recorded once per `WriteResult` from
/// `rocky_iceberg::uniform_writer` — i.e. once per unpartitioned commit,
/// once per partition group on a partitioned commit. Persists the
/// `blake3_hash` that the writer returns so Phase 6 VACUUM refcount can
/// answer "which runs touched this hash" without walking the Delta log
/// of every content-addressed table.
///
/// Shape is 1:1 with the writer's `WriteResult` plus the (`run_id`,
/// `model_name`, `written_at`) join fields needed to anchor the
/// artifact back to a `RunRecord`. Adding fields later is forward-
/// compatible via `#[serde(default)]` — same pattern that took
/// `RunRecord` from v5 to v6.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ArtifactRecord {
    /// Hex-encoded blake3 of the parquet body. The Phase 6 refcount
    /// query keys off this; mirrors `WriteResult.blake3_hash`.
    pub blake3_hash: String,
    /// Run that produced this artifact. Joins to `RunRecord.run_id`.
    pub run_id: String,
    /// Model that produced this artifact. Matches
    /// `ModelExecution.model_name` for the same `run_id`.
    pub model_name: String,
    /// Object-store path of the parquet file, including the per-table
    /// storage prefix. Globally unique across tables in the same
    /// bucket — see `uniform_writer::mod.rs::parquet_path` for the
    /// construction.
    pub file_path: String,
    /// Delta commit version this artifact was attached to. Lets an
    /// operator correlate an artifact row with the Delta `_delta_log`
    /// entry when debugging a hash mismatch.
    pub commit_version: u64,
    /// Parquet file size in bytes. Mirrors `WriteResult.size_bytes`.
    pub size_bytes: u64,
    /// Best-effort wall clock when the artifact was recorded. Not the
    /// same as the write's Delta `modification_time_millis` (that's in
    /// the log); close enough for retention sweep windows.
    pub written_at: chrono::DateTime<chrono::Utc>,
}

/// One row in the agent-policy decision ledger ([`POLICY_DECISIONS`]).
///
/// Recorded once per policy *evaluation* at a mutating enforcement seam
/// (`rocky apply` / promote). The `effect` is the resolved verdict for the
/// evaluated `model`; `rule_id` is the winning `[[policy.rules]]` index (or
/// `None` for the default posture / short-circuit). Adding fields later is
/// forward-compatible via `#[serde(default)]` — the same additive pattern the
/// artifact and run ledgers use.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct PolicyDecisionRecord {
    /// Wall clock when the decision was recorded.
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// The plan the decision governed (`plan_id`).
    pub plan_id: String,
    /// Who was acting (`human` / `agent`).
    pub principal: crate::config::PolicyPrincipal,
    /// The capability that was evaluated (the model's change-classification
    /// refinement of `apply` / `promote`, or the bare verb).
    pub capability: crate::config::PolicyCapability,
    /// The model the decision was about — the concrete scope that matched.
    pub model: String,
    /// The resolved verdict.
    pub effect: crate::config::PolicyEffect,
    /// Index of the winning `[[policy.rules]]` entry, or `None` for the
    /// default posture / short-circuit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<usize>,
    /// Human-readable explanation of how the effect was reached.
    #[serde(default)]
    pub reason: String,
    /// Named checks a `verify_after` condition required post-apply. Empty on a
    /// plain evaluation row; non-empty on a **post-apply verification custody**
    /// row, where `effect` records the verification verdict (`allow` = every
    /// named check passed, `deny` = a named check failed or was absent and the
    /// apply halted) and `reason` states the outcome and rollback status.
    #[serde(default)]
    pub verify_after: Vec<String>,
    /// Custody detail for a governed auto-apply of additive source drift.
    /// Present only on rows the auto-apply path writes; `None` on ordinary
    /// apply/promote evaluations. Serde-defaulted so a pre-v18 row
    /// forward-deserializes with it absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_apply: Option<AutoApplyCustody>,
}

/// Custody detail attached to a [`PolicyDecisionRecord`] when the run loop
/// auto-applies (or refuses) additive source drift on its own authority.
///
/// Records what the run auto-applied (or refused): a human-readable drift
/// summary, the change classification the eligibility gate resolved, whether
/// the migration was applied, and — where a rollback substrate exists — the
/// pointer a revert would restore. On backends with no rollback substrate
/// (e.g. DuckDB) `revert_pointer` is `None` and a failed post-apply
/// verification is halt-only: the mutation stands until a human reverts it.
///
/// Forward-compatible via `#[serde(default)]` on any field added later.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AutoApplyCustody {
    /// Human-readable summary of the drift (e.g. `added nullable column
    /// 'email' (VARCHAR)`).
    pub drift_summary: String,
    /// The change classification the eligibility gate resolved
    /// (e.g. `schema_change.additive`, `schema_change.breaking`).
    pub classification: String,
    /// `true` when the migration was auto-applied; `false` when it was refused
    /// and left for review.
    pub applied: bool,
    /// Pointer to the pre-mutation state a rollback could restore, when a
    /// rollback substrate exists (a content-addressed / Iceberg snapshot).
    /// `None` on substrates with no rollback — the mutation is halt-only on a
    /// verification failure.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revert_pointer: Option<String>,
}

/// One row in the input-match index ([`INPUT_INDEX`]).
///
/// Keyed by the model's `input_hash` (see
/// [`crate::reuse::compute_input_hash`]), it locates the prior run that
/// produced a build with those declared inputs. A future reuse decision
/// reads this *before* execution to find a candidate run `R`; the shipped
/// [`OUTPUT_ARTIFACTS`] ledger + [`StateStore::refcount_for_hash`] then make
/// any reuse of `R`'s bytes safe.
///
/// **Dormant in Stage 1.** This record is written on a successful run and
/// read back only by tests — nothing consults the index for a reuse
/// decision yet. It attests an *input-logic match*, never that re-running
/// the model would reproduce its output.
///
/// Forward-compatible via `#[serde(default)]` on any field added later,
/// the same pattern that carried [`ArtifactRecord`] and [`RunRecord`].
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InputIndexEntry {
    /// Hex-encoded `input_hash` this entry is keyed by. Stored redundantly
    /// inside the value so a by-value scan (e.g. for diagnostics) is
    /// self-describing.
    pub input_hash: String,
    /// Run that produced the inputs. Joins to `RunRecord.run_id`.
    pub run_id: String,
    /// Model these inputs belong to. Matches `ModelExecution.model_name`
    /// for the same `run_id`.
    pub model_name: String,
    /// Output blake3 hash(es) the run produced for this model — one per
    /// content-addressed file (one entry for an unpartitioned model). Empty
    /// when the model was not written content-addressed.
    #[serde(default)]
    pub output_blake3: Vec<String>,
    /// Object-store path(s) of the output parquet, index-aligned with
    /// [`Self::output_blake3`]. Empty when the model was not written
    /// content-addressed.
    #[serde(default)]
    pub output_path: Vec<String>,
    /// Strength of the match this entry would back: `"strong"` (every
    /// upstream identity is a content hash) or `"heuristic"` (at least one
    /// upstream is a freshness signal). See [`crate::reuse::ProofClass`].
    pub proof_class: String,
    /// Best-effort wall clock when the entry was recorded.
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

/// Offline-verifiable provenance for one indexed model build
/// ([`INPUT_PROVENANCE`]).
///
/// Keyed `"{run_id}|{model_name}"`. Embeds everything an auditor needs to
/// re-derive the claim *without trusting the runtime*:
///
/// - the canonical `ModelIr` JSON (via
///   [`rocky_ir::ModelIr::canonical_json`]) — deserialize it and recompute
///   [`rocky_ir::ModelIr::skip_hash`], compare to [`Self::skip_hash`];
/// - the resolved [`Self::upstreams`] — the exact `Vec<UpstreamIdentity>`
///   that justified [`Self::proof_class`], so an auditor can recompute
///   [`Self::input_hash`] offline (via
///   [`crate::reuse::compute_input_hash`]) and, for each `strong` upstream,
///   independently cross-check its recorded blake3 against that upstream's
///   own provenance record;
/// - the recorded output blake3(s) — re-fetch the parquet at the recorded
///   path and `b3sum` it, compare to the recorded hash (the byte-identity
///   half of the claim);
/// - the [`Self::proof_class`] label so a consumer never mistakes a
///   freshness heuristic for a byte-proof.
///
/// The claim is narrow: an *input-logic match* plus *byte-identity of the
/// recorded bytes*. It is **not** a reproducibility guarantee — re-executing
/// the model need not reproduce these bytes (non-deterministic SQL, session
/// settings, UDFs). State-internal; carries no codegen.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct ProvenanceRecord {
    /// Run that produced this build. Joins to `RunRecord.run_id`.
    pub run_id: String,
    /// Model this provenance describes.
    pub model_name: String,
    /// The model's `input_hash` (hex) — the [`INPUT_INDEX`] key this build
    /// is reachable through.
    pub input_hash: String,
    /// Hex of the model's [`rocky_ir::ModelIr::skip_hash`] at build time.
    /// The auditor recomputes this from [`Self::model_ir_canonical_json`]
    /// and compares.
    pub skip_hash: String,
    /// Canonical `ModelIr` JSON (key-sorted, whitespace-free) — the exact
    /// form [`rocky_ir::ModelIr::canonical_json`] emits, round-trippable
    /// back into a `ModelIr` for the offline `skip_hash` recompute.
    pub model_ir_canonical_json: String,
    /// The resolved immediate-upstream identities that justified
    /// [`Self::proof_class`] and were folded into [`Self::input_hash`].
    ///
    /// Persisting them is what makes the input side offline-auditable: an
    /// auditor recomputes `input_hash` via
    /// [`crate::reuse::compute_input_hash`] from [`Self::skip_hash`], the
    /// target identity (parsed back out of [`Self::model_ir_canonical_json`]),
    /// and this list — no trust in the runtime required. Each `strong`
    /// ([`crate::reuse::UpstreamIdentity::Content`]) entry carries the
    /// upstream's recorded blake3, which the auditor cross-checks against that
    /// upstream's *own* provenance record (whose `output_path` locates the
    /// bytes to `b3sum`). Empty for a model that reads nothing (vacuously
    /// strong). `#[serde(default)]` keeps pre-field records readable.
    #[serde(default)]
    pub upstreams: Vec<crate::reuse::UpstreamIdentity>,
    /// Output blake3 hash(es) for the build, one per content-addressed
    /// file. Empty when the model was not written content-addressed.
    #[serde(default)]
    pub output_blake3: Vec<String>,
    /// Object-store path(s) of the output parquet, index-aligned with
    /// [`Self::output_blake3`].
    #[serde(default)]
    pub output_path: Vec<String>,
    /// Match-strength label: `"strong"` or `"heuristic"`. See
    /// [`crate::reuse::ProofClass`].
    pub proof_class: String,
    /// Best-effort wall clock when the provenance was recorded.
    pub recorded_at: chrono::DateTime<chrono::Utc>,
}

/// Result of splitting a batch of candidate artifacts by refcount.
///
/// Produced by [`partition_vacuum_candidates`] from a batch of
/// candidate [`ArtifactRecord`]s (rows the caller has decided are
/// eligible by some other policy — e.g. age, Delta-log staleness,
/// retention window) and a refcount lookup. The split is purely
/// arithmetic: rows whose `blake3_hash` is referenced more than once
/// in the ledger land in `still_referenced`; rows referenced exactly
/// once land in `safe_to_delete` (the input row *is* the last
/// reference, and physically deleting the bytes will drop the
/// refcount to zero once the ledger row is retired).
///
/// The split intentionally never raises errors: a lookup failure on
/// any candidate moves it to `still_referenced` (fail-closed — we
/// would rather keep an orphan byte for one more sweep than delete
/// something that's still live), and the lookup error is captured in
/// `lookup_errors` so the caller can surface it. This is the
/// "graceful degradation" the Phase 6 spec calls out: a ledger
/// hiccup must not catastrophically nuke a VACUUM run, and it must
/// definitely never delete a byte we cannot prove is unreferenced.
#[derive(Debug, Clone)]
pub struct VacuumPartition {
    /// Candidates whose hash had a refcount of exactly 1 — i.e. the
    /// candidate row itself was the only reference and the bytes are
    /// now orphan-eligible. Safe to physically delete *after* the
    /// ledger row is retired in the same transaction.
    pub safe_to_delete: Vec<ArtifactRecord>,
    /// Candidates whose hash had a refcount above 1, *or* whose
    /// refcount lookup failed. Either way the bytes must be kept.
    pub still_referenced: Vec<ArtifactRecord>,
    /// Hashes whose refcount lookup failed. The corresponding records
    /// are in `still_referenced` (fail-closed). Present so callers can
    /// log a warning and decide whether to abort the sweep.
    pub lookup_errors: Vec<String>,
}

/// Partition a batch of VACUUM candidates by ledger refcount.
///
/// `refcount` is invoked once per unique candidate hash; on success
/// the integer it returns is interpreted as the total number of
/// ledger references for that hash (including the row the caller
/// has already decided to retire). A return of `Ok(0)` is treated
/// as "ledger has no record at all" → kept in `still_referenced`,
/// because absence of evidence is not evidence of absence (the
/// row may have been written by an older Rocky version that
/// pre-dates the ledger, or by a writer that crashed before the
/// `record_artifact` call).
///
/// Refcount semantics: a hash with refcount exactly 1 is safe to
/// delete because the single reference is the candidate itself
/// (the caller has, by definition, decided to retire it). Refcount
/// above 1 means at least one *other* run/model also wrote bytes
/// with the same content hash — physical deletion would corrupt
/// those references.
///
/// Designed to be transport-agnostic: `refcount` can be backed by
/// [`StateStore::refcount_for_hash`], a remote service, or a
/// pre-computed `HashMap` for batch sweeps.
pub fn partition_vacuum_candidates<F>(
    candidates: Vec<ArtifactRecord>,
    mut refcount: F,
) -> VacuumPartition
where
    F: FnMut(&str) -> Result<u64, StateError>,
{
    let mut safe_to_delete = Vec::new();
    let mut still_referenced = Vec::new();
    let mut lookup_errors: Vec<String> = Vec::new();
    // Per-batch memoisation: a partitioned commit can record many
    // rows under the same blake3_hash, and rerunning the lookup for
    // each duplicate is pure waste.
    let mut seen: std::collections::HashMap<String, Option<u64>> = std::collections::HashMap::new();

    for record in candidates {
        let count = match seen.get(&record.blake3_hash) {
            Some(cached) => *cached,
            None => {
                let result = refcount(&record.blake3_hash);
                let cached = match result {
                    Ok(n) => Some(n),
                    Err(_) => {
                        if !lookup_errors.contains(&record.blake3_hash) {
                            lookup_errors.push(record.blake3_hash.clone());
                        }
                        None
                    }
                };
                seen.insert(record.blake3_hash.clone(), cached);
                cached
            }
        };

        match count {
            Some(1) => safe_to_delete.push(record),
            _ => still_referenced.push(record),
        }
    }

    VacuumPartition {
        safe_to_delete,
        still_referenced,
        lookup_errors,
    }
}

/// Record of a successfully loaded file, stored in the state database.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LoadedFileRecord {
    /// Timestamp of the load operation.
    pub loaded_at: chrono::DateTime<chrono::Utc>,
    /// Number of rows loaded from this file.
    pub rows_loaded: u64,
    /// Size of the source file in bytes.
    pub bytes_read: u64,
    /// Duration of the load in milliseconds.
    pub duration_ms: u64,
    /// SHA-256 hash of the file content (hex-encoded).
    /// Used for change detection on incremental loads.
    pub file_hash: Option<String>,
}

/// A historical row count snapshot for anomaly detection.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CheckSnapshot {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub row_count: u64,
}

/// Anomaly detection result.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AnomalyResult {
    pub table: String,
    pub current_count: u64,
    pub baseline_avg: f64,
    pub deviation_pct: f64,
    pub is_anomaly: bool,
    pub reason: String,
}

/// Detects row count anomalies by comparing against historical baseline.
///
/// An anomaly is flagged when the current count deviates from the moving average
/// by more than `threshold_pct` (e.g., 50.0 = 50% deviation).
pub fn detect_anomaly(
    table_key: &str,
    current_count: u64,
    history: &[CheckSnapshot],
    threshold_pct: f64,
) -> AnomalyResult {
    if history.is_empty() {
        return AnomalyResult {
            table: table_key.to_string(),
            current_count,
            baseline_avg: current_count as f64,
            deviation_pct: 0.0,
            is_anomaly: false,
            reason: "first run, no baseline".to_string(),
        };
    }

    let avg: f64 = history.iter().map(|s| s.row_count as f64).sum::<f64>() / history.len() as f64;

    let deviation_pct = if avg > 0.0 {
        ((current_count as f64 - avg) / avg * 100.0).abs()
    } else if current_count == 0 {
        0.0
    } else {
        100.0 // went from 0 to non-zero
    };

    let is_anomaly = deviation_pct > threshold_pct;

    let reason = if is_anomaly {
        if (current_count as f64) < avg {
            format!(
                "row count dropped {deviation_pct:.1}% (expected ~{avg:.0}, got {current_count})"
            )
        } else {
            format!(
                "row count spiked {deviation_pct:.1}% (expected ~{avg:.0}, got {current_count})"
            )
        }
    } else {
        "within normal range".to_string()
    };

    AnomalyResult {
        table: table_key.to_string(),
        current_count,
        baseline_avg: avg,
        deviation_pct,
        is_anomaly,
        reason,
    }
}

/// Standard file name for Rocky's embedded state store.
///
/// Both the CLI (`rocky run`, `rocky state`, …) and the LSP/server
/// resolve paths ending in this name via [`resolve_state_path`]; the
/// constant lives here so the two halves of the binary can't disagree
/// on spelling.
pub const STATE_FILE_NAME: &str = ".rocky-state.redb";

/// Directory (under `models_dir`) that holds per-namespace state files.
///
/// When per-pipeline / per-client namespacing is on, each namespace gets its
/// own `<models_dir>/.rocky-state/<namespace>.redb`. The directory name is a
/// dotfile sibling of the legacy global [`STATE_FILE_NAME`] so the two modes
/// never collide on disk: `.rocky-state.redb` (global file) vs
/// `.rocky-state/` (namespaced dir).
pub const STATE_NAMESPACE_DIR: &str = ".rocky-state";

/// Resolution outcome for [`resolve_state_path`].
///
/// `path` is always the state file the caller should open. `warning` is
/// `Some` when the resolver fell back to a deprecated CWD-relative state
/// file (case 3 or 5 below); the CLI prints it to stderr so users see
/// exactly one deprecation nudge per invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedStatePath {
    pub path: std::path::PathBuf,
    pub warning: Option<String>,
}
impl ResolvedStatePath {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Resolve the canonical state-file path for a project.
///
/// Unifies the CLI and LSP defaults: a divergence (CLI wrote
/// `./rocky-state.redb` in CWD while the LSP read
/// `<models_dir>/.rocky-state.redb`) made the schema-cache write tap and
/// inlay-hint read path miss each other in every project where they
/// weren't already co-located. This resolver is the single source of
/// truth for both surfaces.
///
/// Policy (checked in order):
///
/// 1. `explicit` is `Some` — use the user's path verbatim, no warning.
///    This keeps `rocky --state-path <file>` a hard override.
/// 2. `<models_dir>/.rocky-state.redb` exists — use it, no warning.
///    New projects and users who already migrated land here.
/// 3. CWD `.rocky-state.redb` exists — use it. Emit a deprecation
///    warning when `models_dir` is a real directory (the caller can
///    act on it); stay silent for replication-only / quality-only
///    pipelines that have no `models/` directory at all (nothing to
///    migrate to, and the LSP never attaches to those projects so
///    there's no divergence to unify).
/// 4. **Both** CWD and `<models_dir>/.rocky-state.redb` exist — use the
///    CWD file (the historical default that still holds the user's
///    watermarks and branch state) and return a louder warning asking
///    the user to reconcile. Merging is lossy, so we don't attempt it.
/// 5. Neither exists — fresh project. Default to
///    `<models_dir>/.rocky-state.redb` when `models_dir` is a real
///    directory; otherwise fall back to CWD `.rocky-state.redb`. The
///    fallback keeps `rocky run` working for pipelines that legitimately
///    don't have a `models/` directory (e.g. replication-only POCs)
///    without requiring them to create one just to hold state.
pub fn resolve_state_path(explicit: Option<&Path>, models_dir: &Path) -> ResolvedStatePath {
    if let Some(p) = explicit {
        return ResolvedStatePath {
            path: p.to_path_buf(),
            warning: None,
        };
    }

    let models_state = models_dir.join(STATE_FILE_NAME);
    let cwd_state = std::path::PathBuf::from(STATE_FILE_NAME);
    let has_models_dir = models_dir.is_dir();

    // Case 4: both exist. Prefer CWD so existing watermarks/branch state
    // keep working, but shout about the ambiguity. (Only reachable when
    // `models_dir` is a real directory, since `models_state` lives
    // inside it.)
    if cwd_state.exists() && models_state.exists() {
        return ResolvedStatePath {
            path: cwd_state,
            warning: Some(format!(
                "two Rocky state files detected: `{}` (CWD, in use) and `{}` \
                 (models_dir, ignored). Merge is lossy — delete one to silence \
                 this warning. Run `rocky state` from each to decide which \
                 holds the state you want to keep.",
                STATE_FILE_NAME,
                models_state.display(),
            )),
        };
    }

    // Case 2: canonical (new default).
    if models_state.exists() {
        return ResolvedStatePath {
            path: models_state,
            warning: None,
        };
    }

    // Case 3: legacy CWD fallback. Only nudge the user to migrate when a
    // real `models_dir` exists — without one, there's nowhere to move the
    // file to and the warning would be pure noise.
    if cwd_state.exists() {
        let warning = if has_models_dir {
            Some(format!(
                "using legacy state file `{}` in the current directory. \
                 The canonical location is `{}`; move the file there to \
                 silence this warning. The CLI will keep honouring the \
                 CWD fallback for now, but it is deprecated and may be \
                 removed in a future release.",
                STATE_FILE_NAME,
                models_state.display(),
            ))
        } else {
            None
        };
        return ResolvedStatePath {
            path: cwd_state,
            warning,
        };
    }

    // Case 5: fresh project. Default to `<models_dir>/.rocky-state.redb`
    // when the models directory exists; otherwise fall back to CWD so
    // replication-only pipelines (no `models/`) still work out of the
    // box. The LSP only attaches to projects with `.rocky` files (which
    // implies a models dir), so the no-models path has no divergence to
    // unify.
    if has_models_dir {
        ResolvedStatePath {
            path: models_state,
            warning: None,
        }
    } else {
        ResolvedStatePath {
            path: cwd_state,
            warning: None,
        }
    }
}

/// Namespace-aware wrapper around [`resolve_state_path`].
///
/// redb permits exactly one writer per file (its own `flock` plus Rocky's
/// advisory `.redb.lock`), so fanning out one `rocky run` process per pipeline
/// or per client against the single global `<models_dir>/.rocky-state.redb`
/// forces those independent runs to serialize on one lock. Giving each
/// namespace its own file gives it its own lock and its own redb handle, so
/// runs on distinct namespaces proceed concurrently with zero shared
/// corruption surface — every single-writer invariant still holds *inside*
/// each file.
///
/// Resolution policy:
///
/// 1. `namespace` is `None` → delegate verbatim to [`resolve_state_path`].
///    This is the default and is **byte-identical** to today's behavior;
///    namespacing is purely opt-in.
/// 2. `namespace` is `Some(_)` **and** `explicit` is `Some(_)` → the explicit
///    `--state-path` is a hard override that **disables** namespacing for that
///    invocation, so this also delegates to [`resolve_state_path`] (which
///    honors the explicit path verbatim). This mirrors the existing
///    "`--state-path` wins" rule.
/// 3. `namespace` is `Some(ns)` and `explicit` is `None` → resolve to
///    `<models_dir>/.rocky-state/<ns>.redb`. `ns` is validated as a SQL
///    identifier (`^[a-zA-Z0-9_]+$`) by the caller before reaching here
///    (see [`validate_namespace`]), which makes path traversal impossible.
///    The `.rocky-state/` parent directory is created on resolution so the
///    later advisory-lock open (whose first filesystem op is opening
///    `<...>/<ns>.redb.lock`) doesn't fail on a missing parent. If that
///    directory can't be created — or already exists as a non-directory — this
///    falls back to the global resolution with a `rocky::state_path` warning
///    rather than composing a path that would only fail later at lock-open.
///
/// The advisory `.redb.lock` derivation in [`StateStore::open`] needs no
/// change: `path.with_extension("redb.lock")` composes with the nested path,
/// so `<...>/acme.redb` yields `<...>/acme.redb.lock` — a distinct lock per
/// namespace.
///
/// # Migration
///
/// Namespaced files start **fresh**. A pre-existing global
/// `<models_dir>/.rocky-state.redb` is never moved, deleted, or auto-seeded —
/// watermark ownership (which `catalog.schema.table` rows belong to a given
/// pipeline) is not derivable from the state file alone, so seeding can't live
/// here. Operators who want to carry watermarks forward copy the legacy file
/// to `<models_dir>/.rocky-state/<ns>.redb` once, or point `--state-path` at
/// the legacy file for the namespace's first run.
///
/// # Panics
///
/// Does not panic. If `namespace` somehow reaches this function unvalidated
/// (it shouldn't — callers validate first), [`sanitize_namespace`] is applied
/// defensively and an invalid namespace falls back to the global resolution
/// rather than composing a traversal path.
pub fn resolve_state_path_ns(
    explicit: Option<&Path>,
    models_dir: &Path,
    namespace: Option<&str>,
) -> ResolvedStatePath {
    // Cases 1 + 2: no namespace, or an explicit `--state-path` that disables
    // it. Both delegate verbatim — transparency by construction (R1).
    let Some(ns) = namespace else {
        return resolve_state_path(explicit, models_dir);
    };
    if explicit.is_some() {
        return resolve_state_path(explicit, models_dir);
    }

    // Defensive sanitize: callers validate the namespace as a SQL identifier
    // before reaching here, but if an unvalidated value slips through we must
    // never compose a path-traversal segment. An invalid namespace falls back
    // to the global resolution (it can't safely become a file name).
    let Some(sanitized) = sanitize_namespace(ns) else {
        return resolve_state_path(explicit, models_dir);
    };

    let ns_dir = models_dir.join(STATE_NAMESPACE_DIR);
    // Create the namespaced parent dir up front. If the path already exists as
    // a non-directory (e.g. a stray `.rocky-state` file from the legacy global
    // layout), or creation fails for any other reason, fall back to the global
    // resolution with a targeted warning rather than composing a path that
    // would only fail later with an opaque lock-open I/O error.
    if ns_dir.exists() && !ns_dir.is_dir() {
        tracing::warn!(
            target: "rocky::state_path",
            path = %ns_dir.display(),
            "state namespace directory path exists but is not a directory; \
             falling back to the global state file (move or remove it to enable namespacing)"
        );
        return resolve_state_path(explicit, models_dir);
    }
    if let Err(e) = std::fs::create_dir_all(&ns_dir) {
        tracing::warn!(
            target: "rocky::state_path",
            path = %ns_dir.display(),
            error = %e,
            "failed to create state namespace directory; falling back to the global state file"
        );
        return resolve_state_path(explicit, models_dir);
    }

    ResolvedStatePath {
        path: ns_dir.join(format!("{sanitized}.redb")),
        warning: None,
    }
}

/// Validate a state namespace as a SQL identifier (`^[a-zA-Z0-9_]+$`).
///
/// This is the path-traversal guard for `--state-namespace` /
/// `[state] namespacing`: the namespace becomes a path segment, so anything
/// outside the SQL-identifier alphabet (`/`, `.`, `..`, spaces, …) is rejected
/// rather than silently rewritten — silent rewriting would collide distinct
/// tenants (`acme-1` and `acme_1`) onto the same file. Reuses the same
/// validator the rest of the engine uses for catalog/schema/table identifiers.
///
/// # Errors
///
/// Returns [`rocky_sql::validation::ValidationError`] when `ns` is empty or
/// contains a character outside `[a-zA-Z0-9_]`.
pub fn validate_namespace(ns: &str) -> Result<&str, rocky_sql::validation::ValidationError> {
    rocky_sql::validation::validate_identifier(ns)
}

/// Return `Some(ns)` if `ns` is a valid state namespace, else `None`.
///
/// Used by [`resolve_state_path_ns`] as a defensive last line — the real
/// rejection happens at the CLI boundary via [`validate_namespace`], which
/// surfaces a clean error to the user.
fn sanitize_namespace(ns: &str) -> Option<&str> {
    validate_namespace(ns).ok()
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use tempfile::TempDir;

    use super::*;

    fn temp_store() -> (StateStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        (store, dir)
    }

    #[test]
    fn test_open_creates_db() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.redb");
        assert!(!path.exists());
        let _store = StateStore::open(&path).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn test_get_nonexistent_watermark() {
        let (store, _dir) = temp_store();
        let result = store.get_watermark("cat.sch.tbl").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn source_marker_round_trip_and_absent_is_none() {
        let (store, _dir) = temp_store();
        // Absent → None (so an unrecorded table always copies, never prunes).
        assert!(store.get_source_marker("cat.sch.orders").unwrap().is_none());
        // Round-trips, and overwriting reflects the latest recorded marker.
        store
            .set_source_marker("cat.sch.orders", "2026-01-01T00:00:00Z|3|2048")
            .unwrap();
        assert_eq!(
            store
                .get_source_marker("cat.sch.orders")
                .unwrap()
                .as_deref(),
            Some("2026-01-01T00:00:00Z|3|2048")
        );
        store
            .set_source_marker("cat.sch.orders", "2026-01-02T00:00:00Z|4|4096")
            .unwrap();
        assert_eq!(
            store
                .get_source_marker("cat.sch.orders")
                .unwrap()
                .as_deref(),
            Some("2026-01-02T00:00:00Z|4|4096")
        );
        // Keyed per table — a different key stays independent.
        assert!(store.get_source_marker("cat.sch.other").unwrap().is_none());
    }

    #[test]
    fn test_set_and_get_watermark() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        let watermark = WatermarkState {
            last_value: now,
            updated_at: now,
        };

        store.set_watermark("cat.sch.tbl", &watermark).unwrap();
        let retrieved = store.get_watermark("cat.sch.tbl").unwrap().unwrap();
        assert_eq!(retrieved.last_value, watermark.last_value);
    }

    #[test]
    fn test_batch_set_watermarks_writes_all_entries() {
        // §P1.6 batch_set_watermarks should commit every entry in a single
        // transaction so `get_watermark` observes the same result after one
        // call as N sequential `set_watermark` calls.
        let (store, _dir) = temp_store();
        let t = Utc::now();
        let wm1 = WatermarkState {
            last_value: t,
            updated_at: t,
        };
        let wm2 = WatermarkState {
            last_value: t,
            updated_at: t,
        };
        let wm3 = WatermarkState {
            last_value: t,
            updated_at: t,
        };
        let entries = [
            ("cat.sch.a", &wm1),
            ("cat.sch.b", &wm2),
            ("cat.sch.c", &wm3),
        ];
        let count = store.batch_set_watermarks(&entries).unwrap();
        assert_eq!(count, 3);

        assert!(store.get_watermark("cat.sch.a").unwrap().is_some());
        assert!(store.get_watermark("cat.sch.b").unwrap().is_some());
        assert!(store.get_watermark("cat.sch.c").unwrap().is_some());
    }

    #[test]
    fn test_batch_set_watermarks_empty_slice_is_noop() {
        let (store, _dir) = temp_store();
        let count = store.batch_set_watermarks(&[]).unwrap();
        assert_eq!(count, 0);
        assert!(store.get_watermark("never-set").unwrap().is_none());
    }

    #[test]
    fn test_batch_set_watermarks_overwrites_existing() {
        let (store, _dir) = temp_store();
        let t1 = Utc::now();
        let wm1 = WatermarkState {
            last_value: t1,
            updated_at: t1,
        };
        store.set_watermark("cat.sch.tbl", &wm1).unwrap();

        // Fresher timestamp goes in via the batch path.
        let t2 = Utc::now();
        let wm2 = WatermarkState {
            last_value: t2,
            updated_at: t2,
        };
        let entries = [("cat.sch.tbl", &wm2)];
        store.batch_set_watermarks(&entries).unwrap();

        let retrieved = store.get_watermark("cat.sch.tbl").unwrap().unwrap();
        assert_eq!(retrieved.last_value, wm2.last_value);
    }

    #[test]
    fn test_update_watermark() {
        let (store, _dir) = temp_store();
        let t1 = Utc::now();
        let wm1 = WatermarkState {
            last_value: t1,
            updated_at: t1,
        };
        store.set_watermark("cat.sch.tbl", &wm1).unwrap();

        let t2 = Utc::now();
        let wm2 = WatermarkState {
            last_value: t2,
            updated_at: t2,
        };
        store.set_watermark("cat.sch.tbl", &wm2).unwrap();

        let retrieved = store.get_watermark("cat.sch.tbl").unwrap().unwrap();
        assert_eq!(retrieved.last_value, wm2.last_value);
    }

    #[test]
    fn test_delete_watermark() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        let watermark = WatermarkState {
            last_value: now,
            updated_at: now,
        };
        store.set_watermark("cat.sch.tbl", &watermark).unwrap();

        let removed = store.delete_watermark("cat.sch.tbl").unwrap();
        assert!(removed);

        let result = store.get_watermark("cat.sch.tbl").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_delete_nonexistent() {
        let (store, _dir) = temp_store();
        let removed = store.delete_watermark("nonexistent").unwrap();
        assert!(!removed);
    }

    #[test]
    fn test_list_watermarks() {
        let (store, _dir) = temp_store();
        let now = Utc::now();

        for key in &["cat.sch.tbl1", "cat.sch.tbl2", "cat.sch.tbl3"] {
            let wm = WatermarkState {
                last_value: now,
                updated_at: now,
            };
            store.set_watermark(key, &wm).unwrap();
        }

        let watermarks = store.list_watermarks().unwrap();
        assert_eq!(watermarks.len(), 3);

        let keys: Vec<&str> = watermarks.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains(&"cat.sch.tbl1"));
        assert!(keys.contains(&"cat.sch.tbl2"));
        assert!(keys.contains(&"cat.sch.tbl3"));
    }

    #[test]
    fn test_list_empty() {
        let (store, _dir) = temp_store();
        let watermarks = store.list_watermarks().unwrap();
        assert!(watermarks.is_empty());
    }

    #[test]
    fn test_multiple_tables_isolated() {
        let (store, _dir) = temp_store();
        let t1 = Utc::now();
        let t2 = Utc::now();

        store
            .set_watermark(
                "cat.sch.tbl_a",
                &WatermarkState {
                    last_value: t1,
                    updated_at: t1,
                },
            )
            .unwrap();
        store
            .set_watermark(
                "cat.sch.tbl_b",
                &WatermarkState {
                    last_value: t2,
                    updated_at: t2,
                },
            )
            .unwrap();

        let a = store.get_watermark("cat.sch.tbl_a").unwrap().unwrap();
        let b = store.get_watermark("cat.sch.tbl_b").unwrap().unwrap();
        assert_eq!(a.last_value, t1);
        assert_eq!(b.last_value, t2);
    }

    // --- Check history + anomaly detection tests ---

    #[test]
    fn test_record_and_get_history() {
        let (store, _dir) = temp_store();
        store.record_row_count("tbl", 100, 10).unwrap();
        store.record_row_count("tbl", 110, 10).unwrap();
        store.record_row_count("tbl", 105, 10).unwrap();

        let history = store.get_check_history("tbl").unwrap();
        assert_eq!(history.len(), 3);
        assert_eq!(history[0].row_count, 100);
        assert_eq!(history[2].row_count, 105);
    }

    #[test]
    fn test_history_trimmed_to_max() {
        let (store, _dir) = temp_store();
        for i in 0..20 {
            store.record_row_count("tbl", i * 10, 5).unwrap();
        }
        let history = store.get_check_history("tbl").unwrap();
        assert_eq!(history.len(), 5);
        assert_eq!(history[0].row_count, 150); // last 5: 150,160,170,180,190
    }

    #[test]
    fn test_empty_history() {
        let (store, _dir) = temp_store();
        let history = store.get_check_history("nonexistent").unwrap();
        assert!(history.is_empty());
    }

    #[test]
    fn test_anomaly_no_history() {
        let result = detect_anomaly("tbl", 100, &[], 50.0);
        assert!(!result.is_anomaly);
        assert!(result.reason.contains("first run"));
    }

    #[test]
    fn test_anomaly_normal() {
        let history = vec![
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 100,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 105,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 110,
            },
        ];
        let result = detect_anomaly("tbl", 108, &history, 50.0);
        assert!(!result.is_anomaly);
    }

    #[test]
    fn test_anomaly_drop_detected() {
        let history = vec![
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 10000,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 10100,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 10050,
            },
        ];
        // Row count drops to 0 — way more than 50% deviation
        let result = detect_anomaly("tbl", 0, &history, 50.0);
        assert!(result.is_anomaly);
        assert!(result.reason.contains("dropped"));
    }

    #[test]
    fn test_anomaly_spike_detected() {
        let history = vec![
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 100,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 105,
            },
            CheckSnapshot {
                timestamp: Utc::now(),
                row_count: 110,
            },
        ];
        // Spike to 500 — avg is ~105, deviation is ~376%
        let result = detect_anomaly("tbl", 500, &history, 50.0);
        assert!(result.is_anomaly);
        assert!(result.reason.contains("spiked"));
    }

    #[test]
    fn test_anomaly_threshold_boundary() {
        let history = vec![CheckSnapshot {
            timestamp: Utc::now(),
            row_count: 100,
        }];
        // Exactly 50% increase: 100 → 150
        let result = detect_anomaly("tbl", 150, &history, 50.0);
        assert!(!result.is_anomaly); // 50% == threshold, not anomaly (> not >=)
    }

    // --- Run history tests ---

    /// Test-only builder for a minimal [`RunRecord`] with audit fields
    /// zeroed to their defaults. Centralised so adding a future audit
    /// field doesn't churn every existing test in this file.
    fn minimal_run_record(run_id: &str, models: Vec<ModelExecution>) -> RunRecord {
        RunRecord {
            run_id: run_id.to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "config-hash".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
        }
    }

    /// v16 → v17 forward-compat: a `RunRecord` blob written before the
    /// `check_outcomes` field existed (no `check_outcomes` key) must
    /// forward-deserialize with the outcomes defaulting to empty — never crash
    /// the read. Also asserts a v17 record carrying outcomes round-trips.
    #[test]
    fn test_v16_run_record_forward_deserializes_check_outcomes_empty() {
        // A full v16 record serialized with `check_outcomes` stripped.
        let mut value = serde_json::to_value(minimal_run_record("run-v16", vec![]))
            .expect("serialize run record");
        value
            .as_object_mut()
            .expect("record is an object")
            .remove("check_outcomes");
        let blob = serde_json::to_vec(&value).expect("reserialize without field");

        let record: RunRecord =
            serde_json::from_slice(&blob).expect("pre-v17 RunRecord must forward-deserialize");
        assert_eq!(record.run_id, "run-v16");
        assert!(
            record.check_outcomes.is_empty(),
            "a pre-v17 record reads back with no check outcomes"
        );

        // A v17 record carrying outcomes round-trips losslessly.
        let mut with_outcomes = minimal_run_record("run-v17", vec![]);
        with_outcomes.check_outcomes = vec![
            CheckOutcome {
                name: "row_count".to_string(),
                passed: true,
            },
            CheckOutcome {
                name: "not_null:email".to_string(),
                passed: false,
            },
        ];
        let round: RunRecord =
            serde_json::from_slice(&serde_json::to_vec(&with_outcomes).unwrap()).unwrap();
        assert_eq!(round.check_outcomes, with_outcomes.check_outcomes);
    }

    #[test]
    fn test_record_and_get_run() {
        let (store, _dir) = temp_store();
        let run = minimal_run_record(
            "run-001",
            vec![ModelExecution {
                model_name: "orders".to_string(),
                started_at: Utc::now(),
                finished_at: Utc::now(),
                duration_ms: 1234,
                rows_affected: Some(5000),
                status: "success".to_string(),
                sql_hash: "abc123".to_string(),
                skip_hash: None,
                upstream_freshness: None,
                bytes_scanned: None,
                bytes_written: None,
                tenant: None,
                recipe_hash: None,
                input_hash: None,
                input_proof_class: None,
                env_hash: None,
                hash_scheme: None,
                output_column_hashes: None,
                attempts: Vec::new(),
            }],
        );
        store.record_run(&run).unwrap();

        let retrieved = store.get_run("run-001").unwrap().unwrap();
        assert_eq!(retrieved.run_id, "run-001");
        assert_eq!(retrieved.models_executed.len(), 1);
        assert_eq!(retrieved.models_executed[0].duration_ms, 1234);
    }

    #[test]
    fn test_list_runs() {
        let (store, _dir) = temp_store();
        for i in 0..5 {
            let run = minimal_run_record(&format!("run-{i:03}"), vec![]);
            store.record_run(&run).unwrap();
        }

        let runs = store.list_runs(3).unwrap();
        assert_eq!(runs.len(), 3);
    }

    #[test]
    fn test_get_model_history() {
        let (store, _dir) = temp_store();
        let run = minimal_run_record(
            "run-001",
            vec![
                ModelExecution {
                    model_name: "orders".to_string(),
                    started_at: Utc::now(),
                    finished_at: Utc::now(),
                    duration_ms: 500,
                    rows_affected: None,
                    status: "success".to_string(),
                    sql_hash: "h1".to_string(),
                    skip_hash: None,
                    upstream_freshness: None,
                    bytes_scanned: None,
                    bytes_written: None,
                    tenant: None,
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: None,
                    env_hash: None,
                    hash_scheme: None,
                    output_column_hashes: None,
                    attempts: Vec::new(),
                },
                ModelExecution {
                    model_name: "customers".to_string(),
                    started_at: Utc::now(),
                    finished_at: Utc::now(),
                    duration_ms: 300,
                    rows_affected: None,
                    status: "success".to_string(),
                    sql_hash: "h2".to_string(),
                    skip_hash: None,
                    upstream_freshness: None,
                    bytes_scanned: None,
                    bytes_written: None,
                    tenant: None,
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: None,
                    env_hash: None,
                    hash_scheme: None,
                    output_column_hashes: None,
                    attempts: Vec::new(),
                },
            ],
        );
        store.record_run(&run).unwrap();

        let history = store.get_model_history("orders", 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].duration_ms, 500);
    }

    #[test]
    fn test_get_average_duration() {
        let (store, _dir) = temp_store();
        for (i, dur) in [100u64, 200, 300].iter().enumerate() {
            let run = minimal_run_record(
                &format!("run-{i}"),
                vec![ModelExecution {
                    model_name: "orders".to_string(),
                    started_at: Utc::now(),
                    finished_at: Utc::now(),
                    duration_ms: *dur,
                    rows_affected: None,
                    status: "success".to_string(),
                    sql_hash: format!("h{i}"),
                    skip_hash: None,
                    upstream_freshness: None,
                    bytes_scanned: None,
                    bytes_written: None,
                    tenant: None,
                    recipe_hash: None,
                    input_hash: None,
                    input_proof_class: None,
                    env_hash: None,
                    hash_scheme: None,
                    output_column_hashes: None,
                    attempts: Vec::new(),
                }],
            );
            store.record_run(&run).unwrap();
        }

        let avg = store.get_average_duration("orders", 10).unwrap().unwrap();
        assert!((avg - 200.0).abs() < 0.01);
    }

    /// Guard the v5→v6 forward-deserialization contract: a JSON blob
    /// written by a v5 binary (no audit fields at all) must deserialize
    /// cleanly with every new field populated to its documented default.
    ///
    /// The blob shape below is the exact `serde_json::to_vec(&RunRecord)`
    /// output of the pre-audit struct as of schema v5 — hand-crafted so
    /// the test fails if someone accidentally drops the serde-default
    /// on a new field, reintroducing a breaking-change in redb row
    /// compatibility.
    #[test]
    fn test_v5_run_record_forward_deserializes_with_defaults() {
        let v5_blob = br#"{
            "run_id": "run-v5-legacy",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:05:00Z",
            "status": "Success",
            "models_executed": [],
            "trigger": "Manual",
            "config_hash": "legacy-hash"
        }"#;

        let record: RunRecord =
            serde_json::from_slice(v5_blob).expect("v5 blob must forward-deserialize");

        // Original fields preserved.
        assert_eq!(record.run_id, "run-v5-legacy");
        assert_eq!(record.status, RunStatus::Success);
        assert_eq!(record.config_hash, "legacy-hash");

        // Audit fields populated to documented defaults.
        assert_eq!(record.triggering_identity, None);
        assert_eq!(record.session_source, SessionSource::Cli);
        assert_eq!(record.git_commit, None);
        assert_eq!(record.git_branch, None);
        assert_eq!(record.idempotency_key, None);
        assert_eq!(record.target_catalog, None);
        assert_eq!(record.hostname, "unknown");
        assert_eq!(record.rocky_version, "<pre-audit>");
    }

    /// A `ModelExecution` blob written before the skip-gate fields existed
    /// must forward-deserialize with `skip_hash = None` and
    /// `upstream_freshness = None`. This is the load-bearing fail-safe: a
    /// prior execution lacking a stored `skip_hash` can never satisfy gate
    /// clause D, so the next run rebuilds rather than skips.
    #[test]
    fn test_pre_skip_gate_model_execution_forward_deserializes_to_none() {
        let pre_gate_blob = br#"{
            "model_name": "orders",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:00:02Z",
            "duration_ms": 2000,
            "rows_affected": 5000,
            "status": "success",
            "sql_hash": "legacy-sql-hash"
        }"#;

        let exec: ModelExecution = serde_json::from_slice(pre_gate_blob)
            .expect("pre-skip-gate ModelExecution must forward-deserialize");

        assert_eq!(exec.model_name, "orders");
        assert_eq!(exec.sql_hash, "legacy-sql-hash");
        // The skip-gate fields default to None — never to an empty Vec or a
        // sentinel that could be misread as "no upstreams ⇒ stable".
        assert_eq!(exec.skip_hash, None);
        assert_eq!(exec.upstream_freshness, None);
        // bytes fields also default (these predate the skip fields too).
        assert_eq!(exec.bytes_scanned, None);
        assert_eq!(exec.bytes_written, None);
        // The tenant attribution field also defaults to None on a blob
        // that predates it.
        assert_eq!(exec.tenant, None);
    }

    /// A `ModelExecution` blob written before the `tenant` attribution
    /// field existed must forward-deserialize with `tenant = None`. This
    /// is the load-bearing state-schema guard for `rocky cost --by
    /// tenant`: the field is additive and serde-defaulted, so the redb
    /// blob format is unchanged and `CURRENT_SCHEMA_VERSION` does not move.
    /// A record lacking the field simply reports as unattributed.
    #[test]
    fn test_pre_tenant_model_execution_forward_deserializes_to_none() {
        // Note: this blob carries the skip/bytes fields (a record from a
        // binary after those landed) but omits `tenant` — the exact shape
        // a pre-tenant binary would have persisted.
        let pre_tenant_blob = br#"{
            "model_name": "orders",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:00:02Z",
            "duration_ms": 2000,
            "rows_affected": 5000,
            "status": "success",
            "sql_hash": "legacy-sql-hash",
            "skip_hash": "logic-key",
            "upstream_freshness": null,
            "bytes_scanned": 4096,
            "bytes_written": 2048
        }"#;

        let exec: ModelExecution = serde_json::from_slice(pre_tenant_blob)
            .expect("pre-tenant ModelExecution must forward-deserialize");

        // Original fields preserved.
        assert_eq!(exec.model_name, "orders");
        assert_eq!(exec.sql_hash, "legacy-sql-hash");
        assert_eq!(exec.skip_hash.as_deref(), Some("logic-key"));
        assert_eq!(exec.bytes_scanned, Some(4096));
        assert_eq!(exec.bytes_written, Some(2048));
        // The new attribution dimension defaults to None — a pre-tenant
        // record is treated as unattributed, never crashes the read.
        assert_eq!(exec.tenant, None);
    }

    /// v10 → v11 forward-compat: a `ModelExecution` blob written before the
    /// recipe-identity triple existed (no `recipe_hash` / `input_hash` /
    /// `input_proof_class` / `env_hash` / `hash_scheme` keys) must
    /// forward-deserialize with all five defaulting to `None`.
    #[test]
    fn test_v10_model_execution_forward_deserializes_recipe_identity_none() {
        let v10_blob = br#"{
            "model_name": "orders",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:00:02Z",
            "duration_ms": 2000,
            "rows_affected": 5000,
            "status": "success",
            "sql_hash": "legacy-sql-hash",
            "skip_hash": "logic-key",
            "upstream_freshness": null,
            "bytes_scanned": 4096,
            "bytes_written": 2048,
            "tenant": "acme"
        }"#;

        let exec: ModelExecution = serde_json::from_slice(v10_blob)
            .expect("pre-v11 ModelExecution must forward-deserialize");

        // Prior fields preserved.
        assert_eq!(exec.model_name, "orders");
        assert_eq!(exec.tenant.as_deref(), Some("acme"));
        // The recipe-identity triple defaults to None — a pre-v11 record is
        // treated as identity-unknown, never crashes the read.
        assert_eq!(exec.recipe_hash, None);
        assert_eq!(exec.input_hash, None);
        assert_eq!(exec.input_proof_class, None);
        assert_eq!(exec.env_hash, None);
        assert_eq!(exec.hash_scheme, None);
    }

    /// v11 → v12 forward-compat: a `ModelExecution` blob written before
    /// `output_column_hashes` existed (a full v11 record carrying the
    /// recipe-identity triple but no `output_column_hashes` key) must
    /// forward-deserialize with the field defaulting to `None`.
    #[test]
    fn test_v11_model_execution_forward_deserializes_output_column_hashes_none() {
        let v11_blob = br#"{
            "model_name": "orders",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:00:02Z",
            "duration_ms": 2000,
            "rows_affected": 5000,
            "status": "success",
            "sql_hash": "legacy-sql-hash",
            "skip_hash": "logic-key",
            "upstream_freshness": null,
            "bytes_scanned": 4096,
            "bytes_written": 2048,
            "tenant": "acme",
            "recipe_hash": "rh",
            "input_hash": "ih",
            "input_proof_class": "heuristic",
            "env_hash": "eh",
            "hash_scheme": "v1"
        }"#;

        let exec: ModelExecution = serde_json::from_slice(v11_blob)
            .expect("pre-v12 ModelExecution must forward-deserialize");

        // Prior fields (incl. the v11 triple) preserved.
        assert_eq!(exec.model_name, "orders");
        assert_eq!(exec.recipe_hash.as_deref(), Some("rh"));
        assert_eq!(exec.hash_scheme.as_deref(), Some("v1"));
        // The new field defaults to None — a pre-v12 record is treated as
        // "no output column hashes recorded", never crashes the read.
        assert_eq!(exec.output_column_hashes, None);
    }

    /// v15 → v16 forward-compat: a `ModelExecution` blob written before the
    /// classified-retry `attempts` field existed (a full v15 record with no
    /// `attempts` key) must forward-deserialize with the trail defaulting to
    /// empty — never crash the read. Also asserts a v16 record carrying a trail
    /// round-trips, so the attempt history survives a store→read cycle.
    #[test]
    fn test_v15_model_execution_forward_deserializes_attempts_empty() {
        let v15_blob = br#"{
            "model_name": "orders",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:00:02Z",
            "duration_ms": 2000,
            "rows_affected": 5000,
            "status": "success",
            "sql_hash": "legacy-sql-hash",
            "skip_hash": "logic-key",
            "upstream_freshness": null,
            "bytes_scanned": 4096,
            "bytes_written": 2048,
            "tenant": "acme",
            "recipe_hash": "rh",
            "input_hash": "ih",
            "input_proof_class": "heuristic",
            "env_hash": "eh",
            "hash_scheme": "v1",
            "output_column_hashes": null
        }"#;

        let exec: ModelExecution = serde_json::from_slice(v15_blob)
            .expect("pre-v16 ModelExecution must forward-deserialize");

        // Prior fields preserved; the new trail defaults to empty.
        assert_eq!(exec.model_name, "orders");
        assert_eq!(exec.recipe_hash.as_deref(), Some("rh"));
        assert!(
            exec.attempts.is_empty(),
            "a pre-v16 record reads back with no attempt trail"
        );

        // A v16 record carrying a trail round-trips losslessly.
        let mut with_trail = exec;
        with_trail.attempts = vec![
            AttemptRecord {
                attempt: 1,
                outcome: "failed".to_string(),
                failure_class: Some("transient".to_string()),
                transient_kind: Some("rate_limit".to_string()),
                error: Some("429".to_string()),
                backoff_ms: Some(500),
                duration_ms: 12,
            },
            AttemptRecord {
                attempt: 2,
                outcome: "success".to_string(),
                failure_class: None,
                transient_kind: None,
                error: None,
                backoff_ms: None,
                duration_ms: 20,
            },
        ];
        let json = serde_json::to_string(&with_trail).unwrap();
        let back: ModelExecution = serde_json::from_str(&json).unwrap();
        assert_eq!(back.attempts, with_trail.attempts);
    }

    /// v12 → v13 forward-compat: an `UpstreamSig` blob written before
    /// `consumed_column_hashes` existed (a full v12 freshness signature with no
    /// `consumed_column_hashes` key) must forward-deserialize with the field
    /// defaulting to `None`. Also asserts a v13 `UpstreamSig` carrying the new
    /// field round-trips, so the consumer baseline survives a store→read cycle.
    #[test]
    fn test_v12_upstream_sig_forward_deserializes_consumed_column_hashes_none() {
        let v12_blob = br#"{
            "upstream_key": "cat.sch.orders",
            "max_ts": "2024-01-01T00:00:00Z",
            "row_count": 42
        }"#;

        let sig: UpstreamSig =
            serde_json::from_slice(v12_blob).expect("pre-v13 UpstreamSig must forward-deserialize");

        // Prior freshness fields preserved.
        assert_eq!(sig.upstream_key, "cat.sch.orders");
        assert_eq!(sig.row_count, Some(42));
        // The new field defaults to None — a pre-v13 signature is treated as
        // "no consumer column baseline recorded", never crashes the read.
        assert_eq!(sig.consumed_column_hashes, None);

        // A v13 signature carrying the consumer baseline round-trips.
        let v13 = UpstreamSig {
            upstream_key: "cat.sch.orders".to_string(),
            max_ts: None,
            row_count: None,
            consumed_column_hashes: Some(vec![ColumnHash {
                column: "id".to_string(),
                hash: "col-hash".to_string(),
            }]),
        };
        let json = serde_json::to_string(&v13).unwrap();
        let back: UpstreamSig = serde_json::from_str(&json).unwrap();
        assert_eq!(back, v13);
    }

    /// A v11 `ModelExecution` blob (carrying the triple) must back-read on a
    /// binary that predates it: serde ignores the unknown keys, so a v10 reader
    /// never breaks on a v11 row. Emulated by deserializing the full v11 shape
    /// and confirming the round-trip is lossless.
    #[test]
    fn test_v11_model_execution_round_trips_recipe_identity() {
        let exec = ModelExecution {
            model_name: "fct_orders".to_string(),
            started_at: "2024-01-01T12:00:00Z".parse().unwrap(),
            finished_at: "2024-01-01T12:00:02Z".parse().unwrap(),
            duration_ms: 2000,
            rows_affected: Some(10),
            status: "success".to_string(),
            sql_hash: "sql".to_string(),
            skip_hash: Some("logic-key".to_string()),
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            recipe_hash: Some("rh".to_string()),
            input_hash: Some("ih".to_string()),
            input_proof_class: Some("heuristic".to_string()),
            env_hash: Some("eh".to_string()),
            hash_scheme: Some("v1".to_string()),
            output_column_hashes: Some(vec![ColumnHash {
                column: "id".to_string(),
                hash: "col-hash".to_string(),
            }]),
            attempts: Vec::new(),
        };
        let json = serde_json::to_string(&exec).unwrap();
        let back: ModelExecution = serde_json::from_str(&json).unwrap();
        assert_eq!(back.recipe_hash.as_deref(), Some("rh"));
        assert_eq!(back.input_hash.as_deref(), Some("ih"));
        assert_eq!(back.input_proof_class.as_deref(), Some("heuristic"));
        assert_eq!(back.env_hash.as_deref(), Some("eh"));
        assert_eq!(back.hash_scheme.as_deref(), Some("v1"));
        assert_eq!(
            back.output_column_hashes,
            Some(vec![ColumnHash {
                column: "id".to_string(),
                hash: "col-hash".to_string(),
            }])
        );
    }

    /// 🔴 Ledger-readability tripwire. The durable ledger's records
    /// ([`ModelExecution`] carrying the recipe-identity triple, and
    /// [`ProvenanceRecord`]) must stay byte-readable across releases: an
    /// auditor reads these blobs back long after they were written. This pins
    /// the exact serialized JSON of a fixed record of each type, so an
    /// accidental serde change (a renamed field, a flipped attribute, a
    /// reordered declaration) that would silently break an old reader fails
    /// loudly here. A legitimate wire-format change must update these pins
    /// *deliberately* and be paired with a schema-version bump — never re-pin to
    /// make the test green. Run with `REGEN_LEDGER_GOLDEN=1` to reprint.
    #[test]
    fn ledger_record_serialization_pinned() {
        let regen = std::env::var("REGEN_LEDGER_GOLDEN").is_ok_and(|v| !v.is_empty());
        let ts: chrono::DateTime<chrono::Utc> = "2024-01-01T12:00:00Z".parse().unwrap();

        let exec = ModelExecution {
            model_name: "fct_orders".to_string(),
            started_at: ts,
            finished_at: ts,
            duration_ms: 2000,
            rows_affected: Some(10),
            status: "success".to_string(),
            sql_hash: "sql".to_string(),
            skip_hash: Some("logic-key".to_string()),
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written: None,
            tenant: None,
            recipe_hash: Some("recipe-abc".to_string()),
            input_hash: Some("input-def".to_string()),
            input_proof_class: Some("heuristic".to_string()),
            env_hash: Some("env-ghi".to_string()),
            hash_scheme: Some("v1".to_string()),
            output_column_hashes: None,
            attempts: Vec::new(),
        };
        let exec_json = serde_json::to_string(&exec).unwrap();

        let prov = ProvenanceRecord {
            run_id: "run-1".to_string(),
            model_name: "fct_orders".to_string(),
            input_hash: "input-def".to_string(),
            skip_hash: "logic-key".to_string(),
            model_ir_canonical_json: r#"{"a":1}"#.to_string(),
            upstreams: vec![crate::reuse::UpstreamIdentity::Watermark {
                upstream_key: "cat.sch.src".to_string(),
                max_ts: Some("2024-01-01T00:00:00Z".to_string()),
                row_count: Some(42),
            }],
            output_blake3: vec!["out-hash".to_string()],
            output_path: vec!["s3://b/p.parquet".to_string()],
            proof_class: "heuristic".to_string(),
            recorded_at: ts,
        };
        let prov_json = serde_json::to_string(&prov).unwrap();

        if regen {
            eprintln!("EXEC_GOLDEN = {exec_json}");
            eprintln!("PROV_GOLDEN = {prov_json}");
            return;
        }

        const EXEC_GOLDEN: &str = r#"{"model_name":"fct_orders","started_at":"2024-01-01T12:00:00Z","finished_at":"2024-01-01T12:00:00Z","duration_ms":2000,"rows_affected":10,"status":"success","sql_hash":"sql","skip_hash":"logic-key","upstream_freshness":null,"bytes_scanned":null,"bytes_written":null,"tenant":null,"recipe_hash":"recipe-abc","input_hash":"input-def","input_proof_class":"heuristic","env_hash":"env-ghi","hash_scheme":"v1","output_column_hashes":null}"#;
        const PROV_GOLDEN: &str = r#"{"run_id":"run-1","model_name":"fct_orders","input_hash":"input-def","skip_hash":"logic-key","model_ir_canonical_json":"{\"a\":1}","upstreams":[{"kind":"watermark","upstream_key":"cat.sch.src","max_ts":"2024-01-01T00:00:00Z","row_count":42}],"output_blake3":["out-hash"],"output_path":["s3://b/p.parquet"],"proof_class":"heuristic","recorded_at":"2024-01-01T12:00:00Z"}"#;
        assert_eq!(exec_json, EXEC_GOLDEN, "ModelExecution wire format drifted");
        assert_eq!(
            prov_json, PROV_GOLDEN,
            "ProvenanceRecord wire format drifted"
        );
    }

    /// End-to-end variant of the forward-compat guard: write a v5 blob
    /// directly into the `run_history` redb table (bypassing
    /// `record_run`'s serialization), then read it back via the normal
    /// `get_run` / `list_runs` path. Catches both the serde-level
    /// default wiring and any downstream code path that would panic on
    /// a missing field.
    #[test]
    fn test_v5_redb_row_reads_as_v6_with_defaults() {
        let (store, _dir) = temp_store();
        let v5_blob = br#"{
            "run_id": "run-v5-via-redb",
            "started_at": "2024-01-01T12:00:00Z",
            "finished_at": "2024-01-01T12:05:00Z",
            "status": "Failure",
            "models_executed": [],
            "trigger": "Ci",
            "config_hash": "legacy-via-redb"
        }"#;

        // Insert the raw v5 bytes directly into the redb table — no
        // `record_run` roundtrip, so the blob stays byte-identical to
        // what a v5 binary would have written.
        let txn = store.db.begin_write().unwrap();
        {
            let mut table = txn.open_table(RUN_HISTORY).unwrap();
            table.insert("run-v5-via-redb", v5_blob.as_slice()).unwrap();
        }
        txn.commit().unwrap();

        let retrieved = store.get_run("run-v5-via-redb").unwrap().unwrap();
        assert_eq!(retrieved.hostname, "unknown");
        assert_eq!(retrieved.rocky_version, "<pre-audit>");
        assert!(retrieved.triggering_identity.is_none());
        assert_eq!(retrieved.session_source, SessionSource::Cli);
    }

    // --- Quality metrics tests ---

    #[test]
    fn test_record_and_get_quality() {
        let (store, _dir) = temp_store();
        let snapshot = QualitySnapshot {
            timestamp: Utc::now(),
            run_id: "run-001".to_string(),
            model_name: "orders".to_string(),
            metrics: QualityMetrics {
                row_count: 10000,
                null_rates: [("email".to_string(), 0.05)].into(),
                freshness_lag_seconds: Some(3600),
            },
        };
        store.record_quality(&snapshot).unwrap();

        let trend = store.get_quality_trend("orders", 10).unwrap();
        assert_eq!(trend.len(), 1);
        assert_eq!(trend[0].metrics.row_count, 10000);
    }

    // --- DAG snapshot tests ---

    #[test]
    fn test_record_and_get_dag_snapshot() {
        let (store, _dir) = temp_store();
        let snapshot = DagSnapshot {
            timestamp: Utc::now(),
            graph_hash: "abc123".to_string(),
            model_count: 5,
            edge_count: 8,
            changes: vec![DagChange::ModelAdded("new_model".to_string())],
        };
        store.record_dag_snapshot(&snapshot).unwrap();

        let latest = store.get_latest_dag_snapshot().unwrap().unwrap();
        assert_eq!(latest.graph_hash, "abc123");
        assert_eq!(latest.model_count, 5);
        assert_eq!(latest.changes.len(), 1);
    }

    // --- Checkpoint / resume tests ---

    #[test]
    fn test_init_and_get_run_progress() {
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 50).unwrap();

        let progress = store.get_run_progress("run-001").unwrap().unwrap();
        assert_eq!(progress.run_id, "run-001");
        assert_eq!(progress.total_tables, 50);
        assert!(progress.tables.is_empty());
    }

    #[test]
    fn test_get_run_progress_nonexistent() {
        let (store, _dir) = temp_store();
        let progress = store.get_run_progress("nonexistent").unwrap();
        assert!(progress.is_none());
    }

    #[test]
    fn test_record_table_progress() {
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 3).unwrap();

        let p1 = TableProgress {
            index: 0,
            table_key: "cat.sch.tbl_a".to_string(),
            asset_key: vec!["fivetran".into(), "acme".into(), "tbl_a".into()],
            status: TableStatus::Success,
            error: None,
            duration_ms: 120,
            completed_at: Utc::now(),
        };
        let p2 = TableProgress {
            index: 1,
            table_key: "cat.sch.tbl_b".to_string(),
            asset_key: vec!["fivetran".into(), "acme".into(), "tbl_b".into()],
            status: TableStatus::Failed,
            error: Some("connection timeout".to_string()),
            duration_ms: 5000,
            completed_at: Utc::now(),
        };
        let p3 = TableProgress {
            index: 2,
            table_key: "cat.sch.tbl_c".to_string(),
            asset_key: vec!["fivetran".into(), "acme".into(), "tbl_c".into()],
            status: TableStatus::Success,
            error: None,
            duration_ms: 85,
            completed_at: Utc::now(),
        };

        store.record_table_progress("run-001", &p1).unwrap();
        store.record_table_progress("run-001", &p2).unwrap();
        store.record_table_progress("run-001", &p3).unwrap();

        let progress = store.get_run_progress("run-001").unwrap().unwrap();
        assert_eq!(progress.tables.len(), 3);
        assert_eq!(progress.tables[0].table_key, "cat.sch.tbl_a");
        assert_eq!(progress.tables[0].status, TableStatus::Success);
        assert_eq!(progress.tables[1].table_key, "cat.sch.tbl_b");
        assert_eq!(progress.tables[1].status, TableStatus::Failed);
        assert!(progress.tables[1].error.is_some());
        assert_eq!(progress.tables[2].table_key, "cat.sch.tbl_c");
        assert_eq!(progress.tables[2].status, TableStatus::Success);
    }

    #[test]
    fn test_get_latest_run_progress() {
        let (store, _dir) = temp_store();

        // Create two runs with a small time gap to ensure ordering
        store.init_run_progress("run-001", 10).unwrap();
        // Force a slightly later timestamp for the second run
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.init_run_progress("run-002", 20).unwrap();

        let latest = store.get_latest_run_progress().unwrap().unwrap();
        assert_eq!(latest.run_id, "run-002");
        assert_eq!(latest.total_tables, 20);
    }

    #[test]
    fn test_get_latest_run_progress_empty() {
        let (store, _dir) = temp_store();
        let latest = store.get_latest_run_progress().unwrap();
        assert!(latest.is_none());
    }

    #[test]
    fn test_resume_filters_completed() {
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 5).unwrap();

        // Simulate: 3 succeeded, 1 failed, 1 never reached
        for (i, (key, status)) in [
            ("cat.sch.tbl_a", TableStatus::Success),
            ("cat.sch.tbl_b", TableStatus::Success),
            ("cat.sch.tbl_c", TableStatus::Failed),
            ("cat.sch.tbl_d", TableStatus::Success),
        ]
        .iter()
        .enumerate()
        {
            store
                .record_table_progress(
                    "run-001",
                    &TableProgress {
                        index: i,
                        table_key: key.to_string(),
                        asset_key: vec![],
                        status: status.clone(),
                        error: if *status == TableStatus::Failed {
                            Some("error".into())
                        } else {
                            None
                        },
                        duration_ms: 100,
                        completed_at: Utc::now(),
                    },
                )
                .unwrap();
        }

        let progress = store.get_run_progress("run-001").unwrap().unwrap();

        // Filter to only successful tables (what resume would skip)
        let completed: std::collections::HashSet<String> = progress
            .tables
            .iter()
            .filter(|t| t.status == TableStatus::Success)
            .map(|t| t.table_key.clone())
            .collect();

        assert_eq!(completed.len(), 3);
        assert!(completed.contains("cat.sch.tbl_a"));
        assert!(completed.contains("cat.sch.tbl_b"));
        assert!(completed.contains("cat.sch.tbl_d"));
        // Failed table should NOT be in the completed set
        assert!(!completed.contains("cat.sch.tbl_c"));
        // Never-reached table should NOT be in the completed set
        assert!(!completed.contains("cat.sch.tbl_e"));
    }

    fn progress_entry(index: usize, table_key: &str, status: TableStatus) -> TableProgress {
        TableProgress {
            index,
            table_key: table_key.to_string(),
            asset_key: vec![],
            status,
            error: None,
            duration_ms: 100,
            completed_at: Utc::now(),
        }
    }

    #[test]
    fn test_record_table_progress_is_o1_blind_insert() {
        // Each recorded table is a single per-entry row; the header blob's
        // `tables` vector is NOT grown. Reads stitch the two together.
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 3).unwrap();

        // Header still has no inline entries.
        {
            let txn = store.db.begin_read().unwrap();
            let header = txn.open_table(RUN_PROGRESS).unwrap();
            let raw: RunProgress =
                serde_json::from_slice(header.get("run-001").unwrap().unwrap().value()).unwrap();
            assert!(raw.tables.is_empty());
        }

        store
            .record_table_progress(
                "run-001",
                &progress_entry(0, "cat.sch.a", TableStatus::Success),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(1, "cat.sch.b", TableStatus::Failed),
            )
            .unwrap();

        // The header blob is still empty — entries live in the entries table.
        {
            let txn = store.db.begin_read().unwrap();
            let header = txn.open_table(RUN_PROGRESS).unwrap();
            let raw: RunProgress =
                serde_json::from_slice(header.get("run-001").unwrap().unwrap().value()).unwrap();
            assert!(
                raw.tables.is_empty(),
                "record_table_progress must not grow the header blob"
            );
        }

        // But the stitched read returns both, in index order.
        let progress = store.get_run_progress("run-001").unwrap().unwrap();
        assert_eq!(progress.total_tables, 3);
        assert_eq!(progress.tables.len(), 2);
        assert_eq!(progress.tables[0].table_key, "cat.sch.a");
        assert_eq!(progress.tables[1].table_key, "cat.sch.b");
    }

    #[test]
    fn test_record_table_progress_preserves_index_order_on_read() {
        // Insert out of index order; read must sort by execution index, not
        // by the lexical entry key.
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 3).unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(2, "cat.sch.c", TableStatus::Success),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(0, "cat.sch.a", TableStatus::Success),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(1, "cat.sch.b", TableStatus::Success),
            )
            .unwrap();

        let progress = store.get_run_progress("run-001").unwrap().unwrap();
        let order: Vec<&str> = progress
            .tables
            .iter()
            .map(|t| t.table_key.as_str())
            .collect();
        assert_eq!(order, vec!["cat.sch.a", "cat.sch.b", "cat.sch.c"]);
    }

    #[test]
    fn test_record_table_progress_same_table_key_is_last_write_wins() {
        // Re-recording the same table (interrupted -> final) overwrites the
        // one row rather than appending a duplicate.
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 1).unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(0, "cat.sch.a", TableStatus::Interrupted),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-001",
                &progress_entry(0, "cat.sch.a", TableStatus::Success),
            )
            .unwrap();

        let progress = store.get_run_progress("run-001").unwrap().unwrap();
        assert_eq!(progress.tables.len(), 1);
        assert_eq!(progress.tables[0].status, TableStatus::Success);
    }

    #[test]
    fn test_run_progress_entries_do_not_bleed_across_run_ids() {
        // "run-1" and "run-12" share a "run-1" lexical prefix but the trailing
        // "|" separator keeps their entries isolated.
        let (store, _dir) = temp_store();
        store.init_run_progress("run-1", 1).unwrap();
        store.init_run_progress("run-12", 1).unwrap();
        store
            .record_table_progress(
                "run-1",
                &progress_entry(0, "cat.sch.one", TableStatus::Success),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-12",
                &progress_entry(0, "cat.sch.twelve", TableStatus::Success),
            )
            .unwrap();

        let p1 = store.get_run_progress("run-1").unwrap().unwrap();
        assert_eq!(p1.tables.len(), 1);
        assert_eq!(p1.tables[0].table_key, "cat.sch.one");

        let p12 = store.get_run_progress("run-12").unwrap().unwrap();
        assert_eq!(p12.tables.len(), 1);
        assert_eq!(p12.tables[0].table_key, "cat.sch.twelve");
    }

    #[test]
    fn test_get_latest_run_progress_stitches_entries() {
        // --resume-latest reads `.tables` off the latest run; the latest header
        // must come back with its per-entry rows stitched in.
        let (store, _dir) = temp_store();
        store.init_run_progress("run-001", 2).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(10));
        store.init_run_progress("run-002", 2).unwrap();

        store
            .record_table_progress(
                "run-001",
                &progress_entry(0, "cat.sch.old", TableStatus::Success),
            )
            .unwrap();
        store
            .record_table_progress(
                "run-002",
                &progress_entry(0, "cat.sch.new", TableStatus::Success),
            )
            .unwrap();

        let latest = store.get_latest_run_progress().unwrap().unwrap();
        assert_eq!(latest.run_id, "run-002");
        assert_eq!(latest.tables.len(), 1);
        assert_eq!(latest.tables[0].table_key, "cat.sch.new");
    }

    #[test]
    fn test_get_run_progress_falls_back_to_inline_header_tables() {
        // A run recorded by a pre-v8 binary carries its entries inline in the
        // header blob with an empty entries table. The reader must keep the
        // inline `tables` so the old run still resumes.
        let (store, _dir) = temp_store();
        let legacy = RunProgress {
            run_id: "legacy-run".to_string(),
            started_at: Utc::now(),
            total_tables: 2,
            tables: vec![
                progress_entry(0, "cat.sch.x", TableStatus::Success),
                progress_entry(1, "cat.sch.y", TableStatus::Failed),
            ],
        };
        let bytes = serde_json::to_vec(&legacy).unwrap();
        {
            let txn = store.db.begin_write().unwrap();
            {
                let mut header = txn.open_table(RUN_PROGRESS).unwrap();
                header.insert("legacy-run", bytes.as_slice()).unwrap();
            }
            txn.commit().unwrap();
        }

        let progress = store.get_run_progress("legacy-run").unwrap().unwrap();
        assert_eq!(progress.tables.len(), 2);
        assert_eq!(progress.tables[0].table_key, "cat.sch.x");
        assert_eq!(progress.tables[1].table_key, "cat.sch.y");

        // get_latest_run_progress applies the same fallback.
        let latest = store.get_latest_run_progress().unwrap().unwrap();
        assert_eq!(latest.run_id, "legacy-run");
        assert_eq!(latest.tables.len(), 2);
    }

    // ----- time_interval partition state tests (Phase 2A) -----

    use crate::incremental::{PartitionRecord, PartitionStatus};

    fn make_partition(model: &str, key: &str, status: PartitionStatus) -> PartitionRecord {
        PartitionRecord {
            model_name: model.into(),
            partition_key: key.into(),
            status,
            computed_at: Utc::now(),
            row_count: 100,
            duration_ms: 50,
            run_id: "run-1".into(),
            checksum: None,
        }
    }

    #[test]
    fn test_record_and_get_partition() {
        let (store, _dir) = temp_store();
        let rec = make_partition("fct_orders", "2026-04-07", PartitionStatus::Computed);
        store.record_partition(&rec).unwrap();

        let retrieved = store
            .get_partition("fct_orders", "2026-04-07")
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.partition_key, "2026-04-07");
        assert_eq!(retrieved.status, PartitionStatus::Computed);
        assert_eq!(retrieved.row_count, 100);
    }

    #[test]
    fn test_get_nonexistent_partition() {
        let (store, _dir) = temp_store();
        assert!(
            store
                .get_partition("fct_orders", "2026-04-07")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_record_partition_overwrites() {
        let (store, _dir) = temp_store();
        let mut rec = make_partition("m", "2026-04-07", PartitionStatus::InProgress);
        store.record_partition(&rec).unwrap();
        rec.status = PartitionStatus::Computed;
        rec.row_count = 999;
        store.record_partition(&rec).unwrap();

        let retrieved = store.get_partition("m", "2026-04-07").unwrap().unwrap();
        assert_eq!(retrieved.status, PartitionStatus::Computed);
        assert_eq!(retrieved.row_count, 999);
    }

    #[test]
    fn test_list_partitions_filters_by_model() {
        let (store, _dir) = temp_store();
        store
            .record_partition(&make_partition(
                "model_a",
                "2026-04-07",
                PartitionStatus::Computed,
            ))
            .unwrap();
        store
            .record_partition(&make_partition(
                "model_a",
                "2026-04-08",
                PartitionStatus::Computed,
            ))
            .unwrap();
        store
            .record_partition(&make_partition(
                "model_b",
                "2026-04-07",
                PartitionStatus::Computed,
            ))
            .unwrap();

        let a = store.list_partitions("model_a").unwrap();
        assert_eq!(a.len(), 2);
        let b = store.list_partitions("model_b").unwrap();
        assert_eq!(b.len(), 1);
    }

    #[test]
    fn test_list_partitions_empty() {
        let (store, _dir) = temp_store();
        let result = store.list_partitions("never_recorded").unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn test_list_failed_partitions() {
        let (store, _dir) = temp_store();
        store
            .record_partition(&make_partition(
                "m",
                "2026-04-07",
                PartitionStatus::Computed,
            ))
            .unwrap();
        store
            .record_partition(&make_partition("m", "2026-04-08", PartitionStatus::Failed))
            .unwrap();
        store
            .record_partition(&make_partition(
                "m",
                "2026-04-09",
                PartitionStatus::InProgress,
            ))
            .unwrap();

        let failed = store.list_failed_partitions("m").unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0].partition_key, "2026-04-08");
    }

    #[test]
    fn test_delete_partition() {
        let (store, _dir) = temp_store();
        let rec = make_partition("m", "2026-04-07", PartitionStatus::Computed);
        store.record_partition(&rec).unwrap();
        assert!(store.delete_partition("m", "2026-04-07").unwrap());
        assert!(store.get_partition("m", "2026-04-07").unwrap().is_none());
        // Second delete is a no-op.
        assert!(!store.delete_partition("m", "2026-04-07").unwrap());
    }

    #[test]
    fn test_partition_key_namespacing() {
        // A partition recorded under model "fct_orders" with key "2026-04"
        // must NOT collide with model "fct" key "orders|2026-04".
        // (The | separator is the only thing protecting against this.)
        let (store, _dir) = temp_store();
        store
            .record_partition(&make_partition(
                "fct_orders",
                "2026-04",
                PartitionStatus::Computed,
            ))
            .unwrap();
        // The other model has nothing recorded.
        assert!(
            store
                .get_partition("fct", "orders|2026-04")
                .unwrap()
                .is_none()
        );
    }

    // --- Loaded-files tracking tests ---

    fn make_loaded_file_record(rows: u64, bytes: u64) -> LoadedFileRecord {
        LoadedFileRecord {
            loaded_at: Utc::now(),
            rows_loaded: rows,
            bytes_read: bytes,
            duration_ms: 100,
            file_hash: Some("abc123".to_string()),
        }
    }

    #[test]
    fn test_record_and_get_loaded_file() {
        let (store, _dir) = temp_store();
        let rec = make_loaded_file_record(1000, 50_000);

        store
            .record_loaded_file("ingest", "data/orders.csv", &rec)
            .unwrap();

        let retrieved = store
            .get_loaded_file("ingest", "data/orders.csv")
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.rows_loaded, 1000);
        assert_eq!(retrieved.bytes_read, 50_000);
        assert_eq!(retrieved.file_hash.as_deref(), Some("abc123"));
    }

    #[test]
    fn test_get_nonexistent_loaded_file() {
        let (store, _dir) = temp_store();
        assert!(
            store
                .get_loaded_file("ingest", "data/nope.csv")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_record_loaded_file_overwrites() {
        let (store, _dir) = temp_store();
        let rec1 = make_loaded_file_record(100, 5000);
        store
            .record_loaded_file("ingest", "data/orders.csv", &rec1)
            .unwrap();

        let rec2 = make_loaded_file_record(200, 10_000);
        store
            .record_loaded_file("ingest", "data/orders.csv", &rec2)
            .unwrap();

        let retrieved = store
            .get_loaded_file("ingest", "data/orders.csv")
            .unwrap()
            .unwrap();
        assert_eq!(retrieved.rows_loaded, 200);
    }

    #[test]
    fn test_list_loaded_files_filters_by_pipeline() {
        let (store, _dir) = temp_store();
        store
            .record_loaded_file("pipe_a", "a.csv", &make_loaded_file_record(10, 100))
            .unwrap();
        store
            .record_loaded_file("pipe_a", "b.csv", &make_loaded_file_record(20, 200))
            .unwrap();
        store
            .record_loaded_file("pipe_b", "c.csv", &make_loaded_file_record(30, 300))
            .unwrap();

        let a_files = store.list_loaded_files("pipe_a").unwrap();
        assert_eq!(a_files.len(), 2);

        let b_files = store.list_loaded_files("pipe_b").unwrap();
        assert_eq!(b_files.len(), 1);
        assert_eq!(b_files[0].0, "c.csv");
    }

    #[test]
    fn test_list_loaded_files_empty() {
        let (store, _dir) = temp_store();
        let files = store.list_loaded_files("nonexistent").unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn test_delete_loaded_file() {
        let (store, _dir) = temp_store();
        store
            .record_loaded_file("ingest", "data/x.csv", &make_loaded_file_record(10, 100))
            .unwrap();
        assert!(store.delete_loaded_file("ingest", "data/x.csv").unwrap());
        assert!(
            store
                .get_loaded_file("ingest", "data/x.csv")
                .unwrap()
                .is_none()
        );
        // Second delete is a no-op.
        assert!(!store.delete_loaded_file("ingest", "data/x.csv").unwrap());
    }

    #[test]
    fn test_loaded_file_key_namespacing() {
        // Files from different pipelines must not collide.
        let (store, _dir) = temp_store();
        store
            .record_loaded_file("pipe_a", "data.csv", &make_loaded_file_record(10, 100))
            .unwrap();
        assert!(
            store
                .get_loaded_file("pipe_b", "data.csv")
                .unwrap()
                .is_none()
        );
    }

    // -----------------------------------------------------------------------
    // Branches
    // -----------------------------------------------------------------------

    fn make_branch(name: &str, description: Option<&str>) -> BranchRecord {
        BranchRecord {
            name: name.to_string(),
            schema_prefix: format!("branch__{name}"),
            created_by: "hugo".to_string(),
            created_at: Utc::now(),
            description: description.map(ToString::to_string),
        }
    }

    #[test]
    fn test_branch_put_get() {
        let (store, _dir) = temp_store();
        let branch = make_branch("fix-price", Some("trial a new price join"));
        store.put_branch(&branch).unwrap();

        let fetched = store.get_branch("fix-price").unwrap().unwrap();
        assert_eq!(fetched.name, "fix-price");
        assert_eq!(fetched.schema_prefix, "branch__fix-price");
        assert_eq!(
            fetched.description.as_deref(),
            Some("trial a new price join")
        );
    }

    #[test]
    fn test_branch_get_missing() {
        let (store, _dir) = temp_store();
        assert!(store.get_branch("nope").unwrap().is_none());
    }

    #[test]
    fn test_branch_list_sorted() {
        let (store, _dir) = temp_store();
        store.put_branch(&make_branch("charlie", None)).unwrap();
        store.put_branch(&make_branch("alice", None)).unwrap();
        store.put_branch(&make_branch("bob", None)).unwrap();

        let branches = store.list_branches().unwrap();
        let names: Vec<&str> = branches.iter().map(|b| b.name.as_str()).collect();
        assert_eq!(names, vec!["alice", "bob", "charlie"]);
    }

    #[test]
    fn test_branch_delete() {
        let (store, _dir) = temp_store();
        store.put_branch(&make_branch("transient", None)).unwrap();
        assert!(store.delete_branch("transient").unwrap());
        assert!(!store.delete_branch("transient").unwrap()); // idempotent
        assert!(store.get_branch("transient").unwrap().is_none());
    }

    #[test]
    fn test_branch_put_overwrites() {
        let (store, _dir) = temp_store();
        store.put_branch(&make_branch("x", Some("v1"))).unwrap();
        store.put_branch(&make_branch("x", Some("v2"))).unwrap();

        let fetched = store.get_branch("x").unwrap().unwrap();
        assert_eq!(fetched.description.as_deref(), Some("v2"));
    }

    // -----------------------------------------------------------------------
    // Schema cache
    // -----------------------------------------------------------------------

    use crate::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};

    fn make_schema_cache_entry(columns: Vec<(&str, &str, bool)>) -> SchemaCacheEntry {
        SchemaCacheEntry {
            columns: columns
                .into_iter()
                .map(|(name, data_type, nullable)| StoredColumn {
                    name: name.to_string(),
                    data_type: data_type.to_string(),
                    nullable,
                })
                .collect(),
            cached_at: Utc::now(),
        }
    }

    #[test]
    fn test_schema_cache_write_and_read_round_trip() {
        let (store, _dir) = temp_store();
        let key = schema_cache_key("cat", "staging", "orders");
        let entry =
            make_schema_cache_entry(vec![("id", "BIGINT", false), ("email", "STRING", true)]);
        store.write_schema_cache_entry(&key, &entry).unwrap();

        let fetched = store.read_schema_cache_entry(&key).unwrap().unwrap();
        assert_eq!(fetched.columns.len(), 2);
        assert_eq!(fetched.columns[0].name, "id");
        assert_eq!(fetched.columns[0].data_type, "BIGINT");
        assert!(!fetched.columns[0].nullable);
        assert_eq!(fetched.columns[1].name, "email");
        assert!(fetched.columns[1].nullable);
    }

    #[test]
    fn test_schema_cache_read_missing_is_none() {
        let (store, _dir) = temp_store();
        let key = schema_cache_key("cat", "s", "nope");
        assert!(store.read_schema_cache_entry(&key).unwrap().is_none());
    }

    #[test]
    fn test_schema_cache_write_overwrites() {
        let (store, _dir) = temp_store();
        let key = schema_cache_key("c", "s", "t");

        let first = make_schema_cache_entry(vec![("id", "INT", false)]);
        store.write_schema_cache_entry(&key, &first).unwrap();

        let second =
            make_schema_cache_entry(vec![("id", "BIGINT", false), ("name", "STRING", true)]);
        store.write_schema_cache_entry(&key, &second).unwrap();

        let fetched = store.read_schema_cache_entry(&key).unwrap().unwrap();
        assert_eq!(fetched.columns.len(), 2);
        assert_eq!(fetched.columns[0].data_type, "BIGINT");
    }

    #[test]
    fn test_schema_cache_delete_returns_true_when_present() {
        let (store, _dir) = temp_store();
        let key = schema_cache_key("c", "s", "t");
        let entry = make_schema_cache_entry(vec![("id", "BIGINT", false)]);
        store.write_schema_cache_entry(&key, &entry).unwrap();

        assert!(store.delete_schema_cache_entry(&key).unwrap());
        assert!(store.read_schema_cache_entry(&key).unwrap().is_none());
        // Idempotent: second delete returns false.
        assert!(!store.delete_schema_cache_entry(&key).unwrap());
    }

    #[test]
    fn test_schema_cache_delete_missing_returns_false() {
        let (store, _dir) = temp_store();
        assert!(!store.delete_schema_cache_entry("never.seen.key").unwrap());
    }

    #[test]
    fn test_schema_cache_list_returns_all_entries() {
        let (store, _dir) = temp_store();
        store
            .write_schema_cache_entry(
                &schema_cache_key("c", "s", "a"),
                &make_schema_cache_entry(vec![("id", "BIGINT", false)]),
            )
            .unwrap();
        store
            .write_schema_cache_entry(
                &schema_cache_key("c", "s", "b"),
                &make_schema_cache_entry(vec![("id", "BIGINT", false)]),
            )
            .unwrap();
        store
            .write_schema_cache_entry(
                &schema_cache_key("other", "s", "c"),
                &make_schema_cache_entry(vec![("id", "BIGINT", false)]),
            )
            .unwrap();

        let all = store.list_schema_cache().unwrap();
        assert_eq!(all.len(), 3);
        let keys: std::collections::HashSet<&str> = all.iter().map(|(k, _)| k.as_str()).collect();
        assert!(keys.contains("c.s.a"));
        assert!(keys.contains("c.s.b"));
        assert!(keys.contains("other.s.c"));
    }

    #[test]
    fn test_schema_cache_list_empty() {
        let (store, _dir) = temp_store();
        assert!(store.list_schema_cache().unwrap().is_empty());
    }

    #[test]
    fn test_batch_write_schema_cache_entries_writes_all() {
        let (store, _dir) = temp_store();
        let k_a = schema_cache_key("c", "s", "a");
        let k_b = schema_cache_key("c", "s", "b");
        let k_c = schema_cache_key("other", "s", "c");
        let e_a = make_schema_cache_entry(vec![("id", "BIGINT", false)]);
        let e_b = make_schema_cache_entry(vec![("id", "BIGINT", false), ("v", "STRING", true)]);
        let e_c = make_schema_cache_entry(vec![("id", "INT", false)]);

        let count = store
            .batch_write_schema_cache_entries(&[
                (k_a.as_str(), &e_a),
                (k_b.as_str(), &e_b),
                (k_c.as_str(), &e_c),
            ])
            .unwrap();
        assert_eq!(count, 3);

        let all = store.list_schema_cache().unwrap();
        assert_eq!(all.len(), 3);
        // Round-trips: the b entry kept both its columns.
        let fetched_b = store.read_schema_cache_entry(&k_b).unwrap().unwrap();
        assert_eq!(fetched_b.columns.len(), 2);
    }

    #[test]
    fn test_batch_write_schema_cache_entries_empty_is_noop() {
        let (store, _dir) = temp_store();
        let count = store.batch_write_schema_cache_entries(&[]).unwrap();
        assert_eq!(count, 0);
        assert!(store.list_schema_cache().unwrap().is_empty());
    }

    #[test]
    fn test_batch_write_schema_cache_entries_overwrites_existing() {
        let (store, _dir) = temp_store();
        let key = schema_cache_key("c", "s", "t");
        store
            .write_schema_cache_entry(&key, &make_schema_cache_entry(vec![("id", "INT", false)]))
            .unwrap();

        let updated = make_schema_cache_entry(vec![("id", "BIGINT", false), ("n", "STRING", true)]);
        let count = store
            .batch_write_schema_cache_entries(&[(key.as_str(), &updated)])
            .unwrap();
        assert_eq!(count, 1);

        let fetched = store.read_schema_cache_entry(&key).unwrap().unwrap();
        assert_eq!(fetched.columns.len(), 2);
        assert_eq!(fetched.columns[0].data_type, "BIGINT");
    }

    #[test]
    fn test_schema_cache_entry_ttl_expiry_semantics() {
        // Deliberately test the TTL predicate at the state-store layer
        // too — a regression in `is_expired` would silently cause every
        // cache read to count as fresh, which is exactly the correctness
        // failure mode the design doc §5.1 flags.
        let now = Utc::now();
        let ttl = chrono::Duration::hours(24);

        let fresh = SchemaCacheEntry {
            columns: vec![],
            cached_at: now - chrono::Duration::hours(1),
        };
        assert!(!fresh.is_expired(now, ttl));

        let stale = SchemaCacheEntry {
            columns: vec![],
            cached_at: now - chrono::Duration::hours(25),
        };
        assert!(stale.is_expired(now, ttl));
    }

    // -----------------------------------------------------------------------
    // Idempotency StateStore integration tests
    // -----------------------------------------------------------------------

    fn mk_idemp_entry(key: &str, run_id: &str) -> crate::idempotency::IdempotencyEntry {
        let now = Utc::now();
        crate::idempotency::IdempotencyEntry {
            key: key.into(),
            run_id: run_id.into(),
            state: crate::idempotency::IdempotencyState::InFlight,
            stamped_at: now,
            expires_at: now + chrono::Duration::hours(24),
            dedup_on: crate::config::DedupPolicy::Success,
        }
    }

    #[test]
    fn idempotency_first_claim_proceeds() {
        let (store, _dir) = temp_store();
        let entry = mk_idemp_entry("fresh-key", "run-1");
        let verdict = store
            .idempotency_check_and_claim(&entry, Utc::now())
            .unwrap();
        assert!(matches!(
            verdict,
            crate::idempotency::IdempotencyCheck::Proceed
        ));

        // Entry was written.
        let got = store.idempotency_get("fresh-key").unwrap().unwrap();
        assert_eq!(got.run_id, "run-1");
        assert_eq!(got.state, crate::idempotency::IdempotencyState::InFlight);
    }

    #[test]
    fn idempotency_repeat_with_succeeded_skips() {
        let (store, _dir) = temp_store();
        // Seed a Succeeded entry.
        let now = Utc::now();
        let mut entry = mk_idemp_entry("done-key", "run-prior");
        entry.state = crate::idempotency::IdempotencyState::Succeeded;
        entry.expires_at = now + chrono::Duration::days(30);
        store.idempotency_put(&entry).unwrap();

        // New claim attempt must skip.
        let new_entry = mk_idemp_entry("done-key", "run-new");
        let verdict = store.idempotency_check_and_claim(&new_entry, now).unwrap();
        match verdict {
            crate::idempotency::IdempotencyCheck::SkipCompleted { run_id } => {
                assert_eq!(run_id, "run-prior");
            }
            other => panic!("expected SkipCompleted, got {other:?}"),
        }
        // Existing entry unchanged.
        let got = store.idempotency_get("done-key").unwrap().unwrap();
        assert_eq!(got.run_id, "run-prior");
    }

    #[test]
    fn idempotency_repeat_with_inflight_in_ttl_skips() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        let prior = crate::idempotency::IdempotencyEntry {
            key: "busy-key".into(),
            run_id: "run-busy".into(),
            state: crate::idempotency::IdempotencyState::InFlight,
            stamped_at: now - chrono::Duration::minutes(5),
            expires_at: now + chrono::Duration::hours(23),
            dedup_on: crate::config::DedupPolicy::Success,
        };
        store.idempotency_put(&prior).unwrap();

        let new_entry = mk_idemp_entry("busy-key", "run-new");
        let verdict = store.idempotency_check_and_claim(&new_entry, now).unwrap();
        match verdict {
            crate::idempotency::IdempotencyCheck::SkipInFlight { run_id } => {
                assert_eq!(run_id, "run-busy");
            }
            other => panic!("expected SkipInFlight, got {other:?}"),
        }
    }

    #[test]
    fn idempotency_stale_inflight_adopts() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        let stale = crate::idempotency::IdempotencyEntry {
            key: "stale-key".into(),
            run_id: "run-crashed".into(),
            state: crate::idempotency::IdempotencyState::InFlight,
            stamped_at: now - chrono::Duration::hours(25),
            expires_at: now - chrono::Duration::hours(1),
            dedup_on: crate::config::DedupPolicy::Success,
        };
        store.idempotency_put(&stale).unwrap();

        let new_entry = mk_idemp_entry("stale-key", "run-adopter");
        let verdict = store.idempotency_check_and_claim(&new_entry, now).unwrap();
        match verdict {
            crate::idempotency::IdempotencyCheck::AdoptStale { prior_run_id } => {
                assert_eq!(prior_run_id, "run-crashed");
            }
            other => panic!("expected AdoptStale, got {other:?}"),
        }
        // Entry overwritten with the adopter.
        let got = store.idempotency_get("stale-key").unwrap().unwrap();
        assert_eq!(got.run_id, "run-adopter");
        assert_eq!(got.state, crate::idempotency::IdempotencyState::InFlight);
    }

    #[test]
    fn idempotency_finalize_succeeded_keeps_entry() {
        let (store, _dir) = temp_store();
        let entry = mk_idemp_entry("key-a", "run-a");
        store
            .idempotency_check_and_claim(&entry, Utc::now())
            .unwrap();

        let retention = Utc::now() + chrono::Duration::days(30);
        store
            .idempotency_finalize(
                "key-a",
                "run-a",
                crate::idempotency::FinalOutcome::Succeeded,
                retention,
                crate::config::DedupPolicy::Success,
            )
            .unwrap();

        let got = store.idempotency_get("key-a").unwrap().unwrap();
        assert_eq!(got.state, crate::idempotency::IdempotencyState::Succeeded);
    }

    #[test]
    fn idempotency_finalize_failed_with_dedup_success_deletes() {
        let (store, _dir) = temp_store();
        let entry = mk_idemp_entry("key-b", "run-b");
        store
            .idempotency_check_and_claim(&entry, Utc::now())
            .unwrap();

        store
            .idempotency_finalize(
                "key-b",
                "run-b",
                crate::idempotency::FinalOutcome::Failed,
                Utc::now() + chrono::Duration::days(30),
                crate::config::DedupPolicy::Success,
            )
            .unwrap();

        assert!(store.idempotency_get("key-b").unwrap().is_none());
    }

    #[test]
    fn idempotency_finalize_failed_with_dedup_any_keeps_entry() {
        let (store, _dir) = temp_store();
        let entry = mk_idemp_entry("key-c", "run-c");
        store
            .idempotency_check_and_claim(&entry, Utc::now())
            .unwrap();

        store
            .idempotency_finalize(
                "key-c",
                "run-c",
                crate::idempotency::FinalOutcome::Failed,
                Utc::now() + chrono::Duration::days(30),
                crate::config::DedupPolicy::Any,
            )
            .unwrap();

        let got = store.idempotency_get("key-c").unwrap().unwrap();
        assert_eq!(got.state, crate::idempotency::IdempotencyState::Failed);
    }

    #[test]
    fn idempotency_finalize_mismatched_run_id_is_noop() {
        let (store, _dir) = temp_store();
        let entry = mk_idemp_entry("key-d", "run-d");
        store
            .idempotency_check_and_claim(&entry, Utc::now())
            .unwrap();

        // Pretend another run adopted the key; finalizing with a stale
        // `run_id` must not clobber the current owner.
        store
            .idempotency_finalize(
                "key-d",
                "stale-run",
                crate::idempotency::FinalOutcome::Succeeded,
                Utc::now() + chrono::Duration::days(30),
                crate::config::DedupPolicy::Success,
            )
            .unwrap();

        let got = store.idempotency_get("key-d").unwrap().unwrap();
        assert_eq!(got.run_id, "run-d");
        assert_eq!(got.state, crate::idempotency::IdempotencyState::InFlight);
    }

    #[test]
    fn idempotency_sweep_expired_removes_past_retention() {
        let (store, _dir) = temp_store();
        let now = Utc::now();

        // Fresh entry — within retention.
        let fresh = crate::idempotency::IdempotencyEntry {
            key: "fresh".into(),
            run_id: "r-fresh".into(),
            state: crate::idempotency::IdempotencyState::Succeeded,
            stamped_at: now - chrono::Duration::days(1),
            expires_at: now + chrono::Duration::days(29),
            dedup_on: crate::config::DedupPolicy::Success,
        };
        // Past retention — should be swept.
        let expired = crate::idempotency::IdempotencyEntry {
            key: "expired".into(),
            run_id: "r-expired".into(),
            state: crate::idempotency::IdempotencyState::Succeeded,
            stamped_at: now - chrono::Duration::days(60),
            expires_at: now - chrono::Duration::days(30),
            dedup_on: crate::config::DedupPolicy::Success,
        };
        // InFlight past TTL — crashed pod corpse.
        let corpse = crate::idempotency::IdempotencyEntry {
            key: "corpse".into(),
            run_id: "r-corpse".into(),
            state: crate::idempotency::IdempotencyState::InFlight,
            stamped_at: now - chrono::Duration::hours(48),
            expires_at: now - chrono::Duration::hours(24),
            dedup_on: crate::config::DedupPolicy::Success,
        };
        store.idempotency_put(&fresh).unwrap();
        store.idempotency_put(&expired).unwrap();
        store.idempotency_put(&corpse).unwrap();

        let removed = store.idempotency_sweep_expired(now, 24).unwrap();
        assert_eq!(removed, 2);
        assert!(store.idempotency_get("fresh").unwrap().is_some());
        assert!(store.idempotency_get("expired").unwrap().is_none());
        assert!(store.idempotency_get("corpse").unwrap().is_none());
    }

    #[test]
    fn idempotency_concurrent_claim_only_one_wins() {
        // Local backend atomicity: the state.redb.lock enforces one writer
        // at a time, so a second `check_and_claim` for the same key from
        // the same store instance observes the first entry and skips.
        let (store, _dir) = temp_store();
        let now = Utc::now();

        let a = mk_idemp_entry("race-key", "run-a");
        let b = mk_idemp_entry("race-key", "run-b");

        let verdict_a = store.idempotency_check_and_claim(&a, now).unwrap();
        let verdict_b = store.idempotency_check_and_claim(&b, now).unwrap();

        assert!(matches!(
            verdict_a,
            crate::idempotency::IdempotencyCheck::Proceed
        ));
        match verdict_b {
            crate::idempotency::IdempotencyCheck::SkipInFlight { run_id } => {
                assert_eq!(run_id, "run-a");
            }
            other => panic!("expected SkipInFlight for second claim, got {other:?}"),
        }
    }

    // -------- resolve_state_path --------
    //
    // These tests exercise the CLI-vs-LSP unification helper. The test
    // moves into a temp CWD (via `std::env::set_current_dir`) so that the
    // CWD probe in the resolver looks at a clean directory; no test in
    // the rocky-core suite runs in parallel with CWD-mutating cargo
    // tests today (see `#[cfg(test)]` usage across the crate).

    /// Serialise CWD-mutating tests — all resolver tests share the
    /// process-wide CWD, so running them concurrently would race.
    static CWD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn with_cwd<F: FnOnce() -> R, R>(dir: &Path, f: F) -> R {
        let _guard = CWD_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let prev = std::env::current_dir().expect("current_dir");
        std::env::set_current_dir(dir).expect("set_current_dir to temp");
        let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        std::env::set_current_dir(&prev).expect("restore cwd");
        match out {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        }
    }

    #[test]
    fn resolve_state_path_explicit_wins() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // Seed both potential locations so we can prove `explicit` beats them.
        std::fs::File::create(models.join(STATE_FILE_NAME)).unwrap();
        with_cwd(tmp.path(), || {
            std::fs::File::create(STATE_FILE_NAME).unwrap();
            let explicit = tmp.path().join("custom.redb");
            let resolved = resolve_state_path(Some(&explicit), &models);
            assert_eq!(resolved.path, explicit);
            assert!(resolved.warning.is_none());
        });
    }

    #[test]
    fn resolve_state_path_prefers_models_dir_when_only_it_exists() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::File::create(models.join(STATE_FILE_NAME)).unwrap();
        with_cwd(tmp.path(), || {
            let resolved = resolve_state_path(None, &models);
            assert_eq!(resolved.path, models.join(STATE_FILE_NAME));
            assert!(
                resolved.warning.is_none(),
                "models_dir-only layout is the canonical path; no warning expected"
            );
        });
    }

    #[test]
    fn resolve_state_path_falls_back_to_cwd_with_warning() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        with_cwd(tmp.path(), || {
            std::fs::File::create(STATE_FILE_NAME).unwrap();
            let resolved = resolve_state_path(None, &models);
            assert_eq!(resolved.path, std::path::PathBuf::from(STATE_FILE_NAME));
            let warning = resolved.warning.expect("deprecation warning expected");
            assert!(
                warning.contains("legacy state file"),
                "warning should name the legacy layout explicitly: {warning}"
            );
            assert!(
                warning.contains(STATE_FILE_NAME),
                "warning should name the file: {warning}"
            );
        });
    }

    #[test]
    fn resolve_state_path_both_prefer_cwd_but_warn_louder() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::File::create(models.join(STATE_FILE_NAME)).unwrap();
        with_cwd(tmp.path(), || {
            std::fs::File::create(STATE_FILE_NAME).unwrap();
            let resolved = resolve_state_path(None, &models);
            assert_eq!(
                resolved.path,
                std::path::PathBuf::from(STATE_FILE_NAME),
                "both-exist case must prefer CWD to preserve existing state"
            );
            let warning = resolved
                .warning
                .expect("both-exist case must warn about ambiguity");
            assert!(
                warning.contains("two Rocky state files"),
                "louder warning should call out the ambiguity: {warning}"
            );
        });
    }

    #[test]
    fn resolve_state_path_fresh_project_picks_models_dir() {
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        with_cwd(tmp.path(), || {
            let resolved = resolve_state_path(None, &models);
            assert_eq!(resolved.path, models.join(STATE_FILE_NAME));
            assert!(resolved.warning.is_none());
        });
    }

    #[test]
    fn resolve_state_path_fresh_project_without_models_dir_falls_back_to_cwd() {
        // Replication-only / quality-only pipelines (and many POCs) have
        // no `models/` directory. Case 5 must fall back to CWD rather
        // than return a path whose parent doesn't exist — otherwise the
        // next `rocky run` fails trying to open the state lock file.
        // Regression for the drift POC fixture-regen failure on PR #238.
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models"); // deliberately NOT created
        with_cwd(tmp.path(), || {
            let resolved = resolve_state_path(None, &models);
            assert_eq!(resolved.path, std::path::PathBuf::from(STATE_FILE_NAME));
            assert!(
                resolved.warning.is_none(),
                "silent fallback — no models dir means nothing to migrate to"
            );
        });
    }

    #[test]
    fn resolve_state_path_cwd_without_models_dir_is_silent() {
        // Follow-on from the case above: if a legacy CWD state file
        // already exists AND there's no `models/` directory, the
        // deprecation warning has no useful target. Stay quiet.
        let tmp = TempDir::new().unwrap();
        let models = tmp.path().join("models"); // deliberately NOT created
        with_cwd(tmp.path(), || {
            std::fs::File::create(STATE_FILE_NAME).unwrap();
            let resolved = resolve_state_path(None, &models);
            assert_eq!(resolved.path, std::path::PathBuf::from(STATE_FILE_NAME));
            assert!(
                resolved.warning.is_none(),
                "no models dir → no migration nudge"
            );
        });
    }

    // ---------- redb open retry / Busy mapping ----------

    /// When another process briefly holds the redb flock, `open_read_only`
    /// must retry and ultimately succeed instead of leaking the raw
    /// `DatabaseAlreadyOpen` error.
    ///
    /// The release is driven by *observed retry progress*, not a wall-clock
    /// sleep: the holder keeps the flock until the opener reports its first
    /// collision (via [`REDB_RETRY_OBSERVER`]), then drops it. So the opener
    /// always hits contention at least once and the lock always frees a
    /// bounded couple of attempts in — regardless of how the two threads are
    /// scheduled. The earlier version held the lock for a fixed `sleep(120ms)`
    /// and raced the opener's 5×50ms≈250ms budget; under uneven CI load the
    /// holder thread could be starved past the opener's last attempt, so the
    /// opener gave up with `Busy` and the test flaked.
    #[test]
    fn open_read_only_retries_and_succeeds_after_brief_hold() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");

        // Seed the file so `open_read_only` has something to attach to.
        StateStore::open(&path).unwrap();

        let lock_acquired = Arc::new(Barrier::new(2));
        let retries = Arc::new(AtomicUsize::new(0));

        let holder = {
            let path = path.clone();
            let lock_acquired = Arc::clone(&lock_acquired);
            let retries = Arc::clone(&retries);
            thread::spawn(move || {
                let db = Database::create(&path).unwrap();
                lock_acquired.wait();
                // Hold the flock until the opener has provably entered its
                // retry loop, then release. Bounded so a logic regression
                // fails loudly instead of hanging CI — the opener hits the
                // held lock within a millisecond or two in practice.
                let mut waited = Duration::ZERO;
                while retries.load(Ordering::SeqCst) == 0 {
                    thread::sleep(Duration::from_millis(1));
                    waited += Duration::from_millis(1);
                    assert!(
                        waited < Duration::from_secs(5),
                        "opener never hit lock contention"
                    );
                }
                drop(db);
            })
        };

        lock_acquired.wait();
        // Open while the holder still owns the flock: the first attempt
        // collides (bumping `retries`, which prompts the holder to drop), and
        // a later retry then succeeds.
        let opener = {
            let path = path.clone();
            let retries = Arc::clone(&retries);
            thread::spawn(move || {
                REDB_RETRY_OBSERVER.with(|o| *o.borrow_mut() = Some(retries));
                StateStore::open_read_only(&path)
            })
        };

        let result = opener.join().unwrap();
        holder.join().unwrap();

        assert!(
            result.is_ok(),
            "open_read_only should retry past brief redb-lock contention, got error: {}",
            result.err().map(|e| e.to_string()).unwrap_or_default()
        );
        assert!(
            retries.load(Ordering::SeqCst) >= 1,
            "the retry path should have been exercised at least once"
        );
    }

    /// When the redb flock is held longer than the retry budget,
    /// `open_read_only` must surface [`StateError::Busy`] (the friendly
    /// message) — not the raw redb `DatabaseAlreadyOpen` text.
    #[test]
    fn open_read_only_surfaces_busy_after_retry_exhaustion() {
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        StateStore::open(&path).unwrap();

        let lock_acquired = Arc::new(Barrier::new(2));
        let test_done = Arc::new(Barrier::new(2));

        let path_holder = path.clone();
        let lock_acquired_clone = Arc::clone(&lock_acquired);
        let test_done_clone = Arc::clone(&test_done);
        let holder = thread::spawn(move || {
            let db = Database::create(&path_holder).unwrap();
            lock_acquired_clone.wait();
            // Hold past the retry budget (5 × 50ms = 250ms). 600ms gives
            // CI plenty of slack so the test never flakes the other way.
            thread::sleep(Duration::from_millis(600));
            test_done_clone.wait();
            drop(db);
        });

        lock_acquired.wait();
        let result = StateStore::open_read_only(&path);
        test_done.wait();
        holder.join().unwrap();

        match result {
            Err(StateError::Busy { path: p }) => {
                assert_eq!(p, path.display().to_string());
            }
            Err(other) => panic!("expected StateError::Busy, got: {other}"),
            Ok(_) => panic!("expected StateError::Busy, got Ok"),
        }
    }

    // -----------------------------------------------------------------------
    // sweep_retention tests
    // -----------------------------------------------------------------------

    use crate::retention::{StateRetentionConfig, StateRetentionDomain};

    fn run_at(run_id: &str, started_at: chrono::DateTime<Utc>) -> RunRecord {
        let mut r = minimal_run_record(run_id, vec![]);
        r.started_at = started_at;
        r.finished_at = started_at;
        r
    }

    fn dag_snapshot_at(timestamp: chrono::DateTime<Utc>, hash: &str) -> DagSnapshot {
        DagSnapshot {
            timestamp,
            graph_hash: hash.to_string(),
            model_count: 1,
            edge_count: 0,
            changes: vec![],
        }
    }

    fn quality_at(
        model_name: &str,
        run_id: &str,
        timestamp: chrono::DateTime<Utc>,
    ) -> QualitySnapshot {
        QualitySnapshot {
            timestamp,
            run_id: run_id.to_string(),
            model_name: model_name.to_string(),
            metrics: QualityMetrics {
                row_count: 1,
                null_rates: std::collections::HashMap::new(),
                freshness_lag_seconds: None,
            },
        }
    }

    #[test]
    fn sweep_retention_all_young_runs_no_deletes() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        for i in 0..5 {
            store
                .record_run(&run_at(&format!("r{i}"), now - chrono::Duration::days(1)))
                .unwrap();
        }
        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 0,
            applies_to: vec![StateRetentionDomain::History],
            ..StateRetentionConfig::default()
        };
        let report = store.sweep_retention(&policy).unwrap();
        assert_eq!(report.runs_deleted, 0);
        assert_eq!(report.runs_kept, 5);
        assert_eq!(store.list_runs(100).unwrap().len(), 5);
    }

    #[test]
    fn sweep_retention_min_kept_protects_old_runs() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        // 5 ancient runs, all > max_age_days; min_runs_kept >= total → no deletes.
        for i in 0..5 {
            store
                .record_run(&run_at(
                    &format!("r{i}"),
                    now - chrono::Duration::days(1000 + i64::from(i)),
                ))
                .unwrap();
        }
        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 10,
            applies_to: vec![StateRetentionDomain::History],
            ..StateRetentionConfig::default()
        };
        let report = store.sweep_retention(&policy).unwrap();
        assert_eq!(report.runs_deleted, 0);
        assert_eq!(report.runs_kept, 5);
        assert_eq!(store.list_runs(100).unwrap().len(), 5);
    }

    #[test]
    fn sweep_retention_mixed_keeps_recent_5() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        // 5 recent runs (1 day old each, distinct timestamps)
        for i in 0..5 {
            store
                .record_run(&run_at(
                    &format!("recent-{i}"),
                    now - chrono::Duration::hours(1 + i64::from(i)),
                ))
                .unwrap();
        }
        // 7 old runs (2 years old)
        for i in 0..7 {
            store
                .record_run(&run_at(
                    &format!("old-{i}"),
                    now - chrono::Duration::days(700 + i64::from(i)),
                ))
                .unwrap();
        }

        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 5,
            applies_to: vec![StateRetentionDomain::History],
            ..StateRetentionConfig::default()
        };
        let report = store.sweep_retention(&policy).unwrap();
        // The 7 old runs are not in the most-recent-5 and exceed cutoff →
        // deleted. The 5 recent runs survive.
        assert_eq!(report.runs_deleted, 7);
        assert_eq!(report.runs_kept, 5);

        let remaining = store.list_runs(100).unwrap();
        assert_eq!(remaining.len(), 5);
        for r in &remaining {
            assert!(r.run_id.starts_with("recent-"));
        }
    }

    #[test]
    fn sweep_retention_dry_run_does_not_delete() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        for i in 0..3 {
            store
                .record_run(&run_at(
                    &format!("old-{i}"),
                    now - chrono::Duration::days(500 + i64::from(i)),
                ))
                .unwrap();
        }
        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 0,
            applies_to: vec![StateRetentionDomain::History],
            ..StateRetentionConfig::default()
        };

        let dry = store.sweep_retention_dry_run(&policy).unwrap();
        assert_eq!(dry.runs_deleted, 3);
        assert_eq!(dry.runs_kept, 0);
        // dry run must leave the store untouched
        assert_eq!(store.list_runs(100).unwrap().len(), 3);
    }

    #[test]
    fn sweep_retention_skips_domains_not_in_applies_to() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        // Old run + old DAG snapshot. With applies_to = [Lineage], only the
        // DAG snapshot is touched; the run survives even though it would
        // qualify for deletion.
        store
            .record_run(&run_at("old-run", now - chrono::Duration::days(500)))
            .unwrap();
        store
            .record_dag_snapshot(&dag_snapshot_at(
                now - chrono::Duration::days(500),
                "hash-old",
            ))
            .unwrap();

        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 0,
            applies_to: vec![StateRetentionDomain::Lineage],
            ..StateRetentionConfig::default()
        };
        let report = store.sweep_retention(&policy).unwrap();
        assert_eq!(report.runs_deleted, 0);
        assert_eq!(report.runs_kept, 1);
        assert_eq!(report.lineage_deleted, 1);
        assert_eq!(report.lineage_kept, 0);
    }

    #[test]
    fn sweep_retention_audit_quality_history() {
        let (store, _dir) = temp_store();
        let now = Utc::now();
        for i in 0..3 {
            store
                .record_quality(&quality_at(
                    "orders",
                    &format!("run-{i}"),
                    now - chrono::Duration::hours(1 + i64::from(i)),
                ))
                .unwrap();
        }
        for i in 0..4 {
            store
                .record_quality(&quality_at(
                    "users",
                    &format!("old-{i}"),
                    now - chrono::Duration::days(800 + i64::from(i)),
                ))
                .unwrap();
        }
        let policy = StateRetentionConfig {
            max_age_days: 30,
            min_runs_kept: 2,
            applies_to: vec![StateRetentionDomain::Audit],
            ..StateRetentionConfig::default()
        };
        let report = store.sweep_retention(&policy).unwrap();
        assert_eq!(report.audit_deleted, 4);
        assert_eq!(report.audit_kept, 3);
    }

    #[test]
    fn sweep_retention_default_policy_keeps_recent() {
        // Default policy: 365d / 100 / [history, lineage, audit]. With
        // every row inside the cutoff, the sweep is a no-op.
        let (store, _dir) = temp_store();
        let now = Utc::now();
        for i in 0..3 {
            store
                .record_run(&run_at(
                    &format!("r{i}"),
                    now - chrono::Duration::days(i64::from(i)),
                ))
                .unwrap();
        }
        let report = store
            .sweep_retention(&StateRetentionConfig::default())
            .unwrap();
        assert_eq!(report.runs_deleted, 0);
        assert_eq!(report.runs_kept, 3);
        assert_eq!(report.lineage_deleted, 0);
        assert_eq!(report.audit_deleted, 0);
    }

    #[test]
    fn last_retention_sweep_at_absent_returns_none() {
        // Fresh state store has no metadata key yet.
        let (store, _dir) = temp_store();
        let result = store.get_last_retention_sweep_at().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn last_retention_sweep_at_round_trips() {
        // Stamp a known timestamp and read it back; the chrono RFC3339
        // serializer is lossless at second precision so we round-trip
        // exactly for second-aligned inputs.
        let (store, _dir) = temp_store();
        let stamped = chrono::DateTime::parse_from_rfc3339("2026-05-02T12:34:56Z")
            .unwrap()
            .with_timezone(&Utc);
        store.set_last_retention_sweep_at(stamped).unwrap();

        let read_back = store.get_last_retention_sweep_at().unwrap();
        assert_eq!(read_back, Some(stamped));
    }

    #[test]
    fn last_retention_sweep_at_set_overwrites() {
        // Subsequent stamps replace the prior value; we don't keep a
        // history of sweep timestamps in the metadata table.
        let (store, _dir) = temp_store();
        let first = chrono::DateTime::parse_from_rfc3339("2026-05-02T01:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let second = chrono::DateTime::parse_from_rfc3339("2026-05-02T02:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        store.set_last_retention_sweep_at(first).unwrap();
        store.set_last_retention_sweep_at(second).unwrap();

        let read_back = store.get_last_retention_sweep_at().unwrap();
        assert_eq!(read_back, Some(second));
    }

    #[test]
    fn last_retention_sweep_at_persists_across_reopen() {
        // The metadata key is durable: closing and reopening the store
        // recovers the timestamp.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let stamped = chrono::DateTime::parse_from_rfc3339("2026-05-02T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        {
            let store = StateStore::open(&path).unwrap();
            store.set_last_retention_sweep_at(stamped).unwrap();
        }
        let store = StateStore::open(&path).unwrap();
        assert_eq!(store.get_last_retention_sweep_at().unwrap(), Some(stamped));
    }

    #[test]
    fn last_brief_at_absent_returns_none() {
        // Fresh state store has never taken a `--since last` digest.
        let (store, _dir) = temp_store();
        assert!(store.get_last_brief_at().unwrap().is_none());
    }

    #[test]
    fn last_brief_at_round_trips_and_overwrites() {
        // The digest cursor round-trips and advances monotonically as
        // successive `--since last` briefs are taken.
        let (store, _dir) = temp_store();
        let first = chrono::DateTime::parse_from_rfc3339("2026-07-07T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let second = chrono::DateTime::parse_from_rfc3339("2026-07-07T18:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        store.set_last_brief_at(first).unwrap();
        assert_eq!(store.get_last_brief_at().unwrap(), Some(first));
        store.set_last_brief_at(second).unwrap();
        assert_eq!(store.get_last_brief_at().unwrap(), Some(second));
    }

    #[test]
    fn last_brief_at_is_independent_of_retention_cursor() {
        // The two metadata cursors never alias — advancing one leaves the
        // other untouched.
        let (store, _dir) = temp_store();
        let brief = chrono::DateTime::parse_from_rfc3339("2026-07-07T09:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        store.set_last_brief_at(brief).unwrap();
        assert_eq!(store.get_last_brief_at().unwrap(), Some(brief));
        assert!(store.get_last_retention_sweep_at().unwrap().is_none());
    }

    // -----------------------------------------------------------------------
    // Output artifacts (Arc 4 — Phase 6 refcount input)
    // -----------------------------------------------------------------------

    fn make_artifact(
        run_id: &str,
        model: &str,
        hash: &str,
        path: &str,
        size: u64,
    ) -> ArtifactRecord {
        ArtifactRecord {
            blake3_hash: hash.to_string(),
            run_id: run_id.to_string(),
            model_name: model.to_string(),
            file_path: path.to_string(),
            commit_version: 1,
            size_bytes: size,
            written_at: Utc::now(),
        }
    }

    #[test]
    fn policy_decision_round_trip_and_ordering() {
        use crate::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};

        let (store, _dir) = temp_store();
        // Two decisions with distinct timestamps → forward scan is oldest-first.
        let earlier = PolicyDecisionRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-07-07T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            plan_id: "plan_a".to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: "raw_events".to_string(),
            effect: PolicyEffect::Allow,
            rule_id: Some(1),
            reason: "allow by rule 1".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        };
        let later = PolicyDecisionRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-07-07T11:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            plan_id: "plan_b".to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::Apply,
            model: "fct_orders".to_string(),
            effect: PolicyEffect::Deny,
            rule_id: Some(0),
            reason: "denied by rule 0 (deny overrides)".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        };
        // Insert out of order; the ledger must return them chronologically.
        store.record_policy_decision(&later).unwrap();
        store.record_policy_decision(&earlier).unwrap();

        let all = store.list_policy_decisions().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], earlier, "oldest decision first");
        assert_eq!(all[1], later);
    }

    #[test]
    fn test_v17_policy_decision_forward_deserializes_auto_apply_none() {
        use crate::config::{PolicyCapability, PolicyEffect, PolicyPrincipal};

        // A full v17 record serialized with `auto_apply` stripped.
        let record = PolicyDecisionRecord {
            timestamp: chrono::DateTime::parse_from_rfc3339("2026-07-07T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            plan_id: "plan_v17".to_string(),
            principal: PolicyPrincipal::Agent,
            capability: PolicyCapability::SchemaChangeAdditive,
            model: "raw_events".to_string(),
            effect: PolicyEffect::Allow,
            rule_id: Some(1),
            reason: "allow by rule 1".to_string(),
            verify_after: Vec::new(),
            auto_apply: None,
        };
        let mut value = serde_json::to_value(&record).expect("serialize record");
        value
            .as_object_mut()
            .expect("record is an object")
            .remove("auto_apply");
        let blob = serde_json::to_vec(&value).expect("reserialize without field");

        let read: PolicyDecisionRecord = serde_json::from_slice(&blob)
            .expect("pre-v18 PolicyDecisionRecord must forward-deserialize");
        assert_eq!(read.plan_id, "plan_v17");
        assert!(
            read.auto_apply.is_none(),
            "a pre-v18 record reads back with no auto-apply custody"
        );

        // A v18 record carrying custody round-trips losslessly.
        let mut with_custody = record.clone();
        with_custody.auto_apply = Some(AutoApplyCustody {
            drift_summary: "added nullable column 'email' (VARCHAR)".to_string(),
            classification: "schema_change.additive".to_string(),
            applied: true,
            revert_pointer: None,
        });
        let round: PolicyDecisionRecord =
            serde_json::from_slice(&serde_json::to_vec(&with_custody).unwrap()).unwrap();
        assert_eq!(round.auto_apply, with_custody.auto_apply);
    }

    #[test]
    fn test_v13_opens_and_creates_policy_decisions_table() {
        // A store freshly opened under the current binary materializes the
        // `policy_decisions` table (part of the eager EXPECTED_TABLES set) and
        // stamps itself at the current schema version. The ledger has existed
        // since v14. Reads succeed against an empty ledger.
        let (store, _dir) = temp_store();
        assert!(
            current_schema_version() >= 14,
            "policy_decisions ledger exists from schema v14 onward"
        );
        let decisions = store.list_policy_decisions().unwrap();
        assert!(
            decisions.is_empty(),
            "a fresh store has an empty policy-decision ledger"
        );
    }

    #[test]
    fn artifact_round_trip_by_hash() {
        // Record one artifact, look it up by its blake3 hash.
        let (store, _dir) = temp_store();
        let art = make_artifact(
            "run-1",
            "fct_orders",
            "blake3:abc",
            "s3://bucket/prefix/abc.parquet",
            1024,
        );
        store.record_artifact(&art).unwrap();

        let by_hash = store.list_artifacts_by_hash("blake3:abc").unwrap();
        assert_eq!(by_hash.len(), 1);
        assert_eq!(by_hash[0], art);

        // Different hash → no hit.
        let miss = store.list_artifacts_by_hash("blake3:not-there").unwrap();
        assert!(miss.is_empty());
    }

    #[test]
    fn artifact_list_for_run_filters_by_prefix() {
        // Two artifacts on run-1, one on run-2; list_for_run isolates them.
        let (store, _dir) = temp_store();
        store
            .record_artifact(&make_artifact(
                "run-1",
                "a",
                "h1",
                "s3://b/p/h1.parquet",
                100,
            ))
            .unwrap();
        store
            .record_artifact(&make_artifact(
                "run-1",
                "b",
                "h2",
                "s3://b/p/h2.parquet",
                200,
            ))
            .unwrap();
        store
            .record_artifact(&make_artifact(
                "run-2",
                "a",
                "h3",
                "s3://b/p/h3.parquet",
                300,
            ))
            .unwrap();

        let run1 = store.list_artifacts_for_run("run-1").unwrap();
        assert_eq!(run1.len(), 2);
        assert!(run1.iter().any(|a| a.blake3_hash == "h1"));
        assert!(run1.iter().any(|a| a.blake3_hash == "h2"));

        let run2 = store.list_artifacts_for_run("run-2").unwrap();
        assert_eq!(run2.len(), 1);
        assert_eq!(run2[0].blake3_hash, "h3");

        let run_none = store.list_artifacts_for_run("run-3").unwrap();
        assert!(run_none.is_empty());
    }

    #[test]
    fn artifact_list_for_run_does_not_prefix_collide() {
        // "run-1" must not match "run-10" — the `|` separator after the
        // run_id is what keeps the prefix scan safe.
        let (store, _dir) = temp_store();
        store
            .record_artifact(&make_artifact("run-1", "m", "h", "s3://b/p/a.parquet", 10))
            .unwrap();
        store
            .record_artifact(&make_artifact("run-10", "m", "h", "s3://b/p/b.parquet", 20))
            .unwrap();

        let run1 = store.list_artifacts_for_run("run-1").unwrap();
        assert_eq!(run1.len(), 1, "run-1 must not match run-10 by prefix");
        assert_eq!(run1[0].file_path, "s3://b/p/a.parquet");
    }

    #[test]
    fn artifact_count_matches_inserts() {
        let (store, _dir) = temp_store();
        assert_eq!(store.count_artifacts().unwrap(), 0);
        store
            .record_artifact(&make_artifact("r", "m", "h1", "s3://b/p/x.parquet", 1))
            .unwrap();
        store
            .record_artifact(&make_artifact("r", "m", "h2", "s3://b/p/y.parquet", 1))
            .unwrap();
        assert_eq!(store.count_artifacts().unwrap(), 2);
    }

    #[test]
    fn artifact_record_is_idempotent_on_same_key() {
        // Re-recording the same (run, model, file_path) overwrites — a
        // cond-put retry winner replacing a loser's stamp should not
        // produce a second row for Phase 6 refcount to double-count.
        let (store, _dir) = temp_store();
        let first = make_artifact("r", "m", "h", "s3://b/p/x.parquet", 100);
        store.record_artifact(&first).unwrap();
        let second = make_artifact("r", "m", "h", "s3://b/p/x.parquet", 200);
        store.record_artifact(&second).unwrap();

        assert_eq!(store.count_artifacts().unwrap(), 1);
        let fetched = store.list_artifacts_for_run("r").unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].size_bytes, 200, "second insert wins");
    }

    #[test]
    fn artifact_multiple_runs_share_hash() {
        // Two runs materialize the same content (same blake3 hash) into
        // different file paths. `list_by_hash` returns both. This is the
        // shape Phase 6 needs: "which (run, model) tuples reference
        // hash X?".
        let (store, _dir) = temp_store();
        store
            .record_artifact(&make_artifact(
                "run-a",
                "m",
                "shared-hash",
                "s3://b/p1/shared.parquet",
                1,
            ))
            .unwrap();
        store
            .record_artifact(&make_artifact(
                "run-b",
                "m",
                "shared-hash",
                "s3://b/p2/shared.parquet",
                1,
            ))
            .unwrap();

        let refs = store.list_artifacts_by_hash("shared-hash").unwrap();
        assert_eq!(refs.len(), 2);
        let run_ids: std::collections::BTreeSet<&str> =
            refs.iter().map(|a| a.run_id.as_str()).collect();
        assert!(run_ids.contains("run-a"));
        assert!(run_ids.contains("run-b"));
    }

    #[test]
    fn artifact_persists_across_reopen() {
        // Record an artifact, close the DB, reopen, look it up — proves
        // the table is durable on disk (not just in-memory between
        // method calls).
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let art = make_artifact("r", "m", "h", "s3://b/p/x.parquet", 42);
        {
            let store = StateStore::open(&path).unwrap();
            store.record_artifact(&art).unwrap();
        }
        let store = StateStore::open(&path).unwrap();
        let fetched = store.list_artifacts_by_hash("h").unwrap();
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0], art);
    }

    #[test]
    fn artifact_table_auto_created_on_v6_reopen() {
        // Older-version upgrade contract: opening a state DB previously
        // stamped at v6 must (a) succeed, (b) auto-create the tables added
        // since v6 (OUTPUT_ARTIFACTS in v7, RUN_PROGRESS_ENTRIES in v8) on
        // the existing open path, and (c) re-stamp the DB at the current
        // version so a subsequent open by an older binary fails loudly
        // rather than silently misreading. We simulate "v6 on disk" by
        // opening the DB at the current binary version, then rewriting the
        // schema_version blob to "6" via a raw redb transaction.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            // Force the metadata blob back to "6" so the next open
            // exercises the upgrade path.
            let txn = store.db.begin_write().unwrap();
            {
                let mut metadata = txn.open_table(METADATA).unwrap();
                metadata.insert("schema_version", "6").unwrap();
            }
            txn.commit().unwrap();
        }
        // Reopening the v6 DB should bump the stamp to the current version
        // and not fail even though newer tables were created.
        let store = StateStore::open(&path).unwrap();
        // The artifact table is usable.
        let art = make_artifact("r", "m", "h", "s3://b/p/x.parquet", 1);
        store.record_artifact(&art).unwrap();
        assert_eq!(store.count_artifacts().unwrap(), 1);

        // The version stamp is now the current schema version.
        let txn = store.db.begin_read().unwrap();
        let metadata = txn.open_table(METADATA).unwrap();
        let stored = metadata
            .get("schema_version")
            .unwrap()
            .map(|g| g.value().to_string())
            .unwrap();
        assert_eq!(stored, CURRENT_SCHEMA_VERSION.to_string());
    }

    // -----------------------------------------------------------------------
    // Forward-incompat / schema-mismatch policy (deploy safety)
    // -----------------------------------------------------------------------

    /// Stamp an arbitrary `schema_version` onto an existing store file, then
    /// release the lock. Used to simulate a store written by a *newer* binary.
    fn force_schema_version(path: &Path, version: &str) {
        let store = StateStore::open(path).unwrap();
        let txn = store.db.begin_write().unwrap();
        {
            let mut metadata = txn.open_table(METADATA).unwrap();
            metadata.insert("schema_version", version).unwrap();
        }
        txn.commit().unwrap();
        // `store` drops here, releasing the advisory lock.
    }

    fn sample_watermark() -> WatermarkState {
        let now = Utc::now();
        WatermarkState {
            last_value: now,
            updated_at: now,
        }
    }

    #[test]
    fn open_hard_fails_on_forward_incompat_by_default() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        drop(StateStore::open(&path).unwrap());
        let future = (CURRENT_SCHEMA_VERSION + 1).to_string();
        force_schema_version(&path, &future);

        // Bare `open` keeps today's behaviour: a hard SchemaMismatch.
        // (`StateStore` is not `Debug`, so match instead of `unwrap_err`.)
        let err = match StateStore::open(&path) {
            Ok(_) => panic!("expected SchemaMismatch on a forward-incompatible store"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StateError::SchemaMismatch { found, expected, .. }
                if found == CURRENT_SCHEMA_VERSION + 1 && expected == CURRENT_SCHEMA_VERSION),
            "got {err:?}"
        );
        // Explicit `Fail` policy is identical.
        let err = match StateStore::open_with_policy(&path, SchemaMismatchPolicy::Fail) {
            Ok(_) => panic!("expected SchemaMismatch under Fail policy"),
            Err(e) => e,
        };
        assert!(
            matches!(err, StateError::SchemaMismatch { .. }),
            "got {err:?}"
        );
        // The file still carries the newer stamp — Fail does not mutate it.
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(CURRENT_SCHEMA_VERSION + 1)
        );
    }

    #[test]
    fn open_with_policy_recreate_bootstraps_fresh_on_forward_incompat() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        // Seed a watermark, then stamp the file as written by a newer binary.
        {
            let store = StateStore::open(&path).unwrap();
            store
                .set_watermark("cat.sch.tbl", &sample_watermark())
                .unwrap();
        }
        let future = (CURRENT_SCHEMA_VERSION + 2).to_string();
        force_schema_version(&path, &future);

        // Recreate: opens fresh, flags the recreate, and the old watermark is gone.
        let store = StateStore::open_with_policy(&path, SchemaMismatchPolicy::Recreate).unwrap();
        assert!(store.was_recreated_for_forward_incompat());
        assert!(
            store.list_watermarks().unwrap().is_empty(),
            "recreate must discard the forward-incompatible state"
        );
        drop(store);
        // The file is now re-stamped at the version this binary supports.
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(CURRENT_SCHEMA_VERSION)
        );
    }

    #[test]
    fn open_with_policy_recreate_is_noop_on_compatible_store() {
        // A current-version store is never recreated; its data is preserved and
        // the flag stays false even under the Recreate policy.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            store
                .set_watermark("cat.sch.tbl", &sample_watermark())
                .unwrap();
        }
        let store = StateStore::open_with_policy(&path, SchemaMismatchPolicy::Recreate).unwrap();
        assert!(!store.was_recreated_for_forward_incompat());
        assert_eq!(store.list_watermarks().unwrap().len(), 1);
    }

    #[test]
    fn open_with_policy_recreate_upgrades_older_store_without_recreating() {
        // Backward-compat (on-disk OLDER than binary) must still auto-migrate
        // forward in place — Recreate is only for the forward case.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            store
                .set_watermark("cat.sch.tbl", &sample_watermark())
                .unwrap();
        }
        force_schema_version(&path, "6");
        let store = StateStore::open_with_policy(&path, SchemaMismatchPolicy::Recreate).unwrap();
        assert!(
            !store.was_recreated_for_forward_incompat(),
            "an older store auto-migrates, it is not recreated"
        );
        assert_eq!(store.list_watermarks().unwrap().len(), 1, "data preserved");
    }

    #[test]
    fn peek_schema_version_reports_on_disk_without_mutating() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        // Missing file → None, and peek must NOT create it as a side effect.
        assert_eq!(StateStore::peek_schema_version(&path).unwrap(), None);
        assert!(!path.exists(), "peek must not create a missing state file");

        // Fresh store → the current version.
        drop(StateStore::open(&path).unwrap());
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(CURRENT_SCHEMA_VERSION)
        );

        // A forward-stamped file is reported precisely (no error, no recreate).
        let future = CURRENT_SCHEMA_VERSION + 3;
        force_schema_version(&path, &future.to_string());
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(future)
        );
        // Peeking again leaves the stamp untouched.
        assert_eq!(
            StateStore::peek_schema_version(&path).unwrap(),
            Some(future)
        );
    }

    /// F1 golden: pin the **on-disk state-schema format** — the schema version
    /// plus the exact set of redb tables a fresh store materializes.
    ///
    /// The `state.redb` file is persisted and shared across pods (see
    /// `state_sync`), so its layout is a compatibility contract. `init_db`
    /// eagerly creates every table, so a fresh store's `list_tables()` is the
    /// authoritative on-disk table set. Adding, removing, or renaming a table
    /// — or bumping `CURRENT_SCHEMA_VERSION` — must be a deliberate, reviewed
    /// change paired with the version-mismatch migration story (the
    /// `open_with_policy_*` tests above). This test fails on any such change
    /// until the golden below is updated **in the same PR**, making "no silent
    /// breaking change to the consumed state format" mechanically true.
    #[test]
    fn on_disk_state_schema_format_is_pinned() {
        use redb::TableHandle;

        // The pinned format contract. Update DELIBERATELY: a table-set change
        // is an on-disk break that needs a `CURRENT_SCHEMA_VERSION` bump + a
        // migration path, not just an edit here.
        //
        // v14 adds the `policy_decisions` table for the agent-policy decision
        // ledger — a pure additive table add, the same shape as the v7
        // `output_artifacts` add. A v13 database auto-creates the empty table on
        // next open and stamps itself v14; the forward-compat path is the
        // existing version-mismatch handling (guarded by the `open_with_policy_*`
        // tests), not a blob migration.
        //
        // v15 adds the `jobs` table for the `rocky serve` HTTP job model. Same
        // additive shape: a v14 database auto-creates the empty `jobs` table on
        // next open and stamps itself v15, guarded by the same mismatch tests.
        //
        // v16 adds `ModelExecution.attempts` (the classified-retry trail). That
        // is a serde-additive *field* on a blob — the redb table set does NOT
        // change, so EXPECTED_TABLES below is untouched; only the version moves.
        // v17 adds the `verify_after` post-apply-gate fields (`RunRecord::
        // check_outcomes`, `PolicyDecisionRecord::verify_after`), also
        // serde-additive fields — guarded by
        // `test_v16_run_record_forward_deserializes_check_outcomes_empty`.
        // v18 adds `PolicyDecisionRecord::auto_apply` (the governed
        // additive-drift auto-apply custody), another serde-additive field —
        // the redb table set is untouched, only the version moves; guarded by
        // `test_v17_policy_decision_forward_deserializes_auto_apply_none`.
        const EXPECTED_VERSION: u32 = 18;
        const EXPECTED_TABLES: &[&str] = &[
            "branches",
            "check_history",
            "dag_snapshots",
            "discover_snapshots",
            "grace_periods",
            "idempotency_keys",
            "input_index",
            "input_provenance",
            "jobs",
            "loaded_files",
            "metadata",
            "output_artifacts",
            "partitions",
            "policy_decisions",
            "quality_history",
            "run_history",
            "run_progress",
            "run_progress_entries",
            "schema_cache",
            "source_markers",
            "watermarks",
        ];

        assert_eq!(
            CURRENT_SCHEMA_VERSION, EXPECTED_VERSION,
            "the on-disk state schema version changed. Update EXPECTED_VERSION here, and \
             confirm the version-mismatch migration path (open_with_policy_* tests) handles \
             the bump, in the same PR."
        );

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();
        let txn = store.db.begin_read().unwrap();
        let mut actual: Vec<String> = txn
            .list_tables()
            .unwrap()
            .map(|t| t.name().to_string())
            .collect();
        actual.sort_unstable();

        assert_eq!(
            actual, EXPECTED_TABLES,
            "the on-disk state table set changed: a fresh `StateStore` now materializes a \
             different set of redb tables than the pinned format contract. If intentional, \
             update EXPECTED_TABLES (and bump CURRENT_SCHEMA_VERSION + add the migration) in \
             the same PR."
        );
    }

    // -----------------------------------------------------------------------
    // JOBS table (v14 — `rocky serve` HTTP job model)
    // -----------------------------------------------------------------------

    fn sample_job(id: &str, state: &str) -> PersistedJob {
        PersistedJob {
            job_id: id.to_string(),
            kind: "run".to_string(),
            state: state.to_string(),
            submitted_at: "2026-07-07T00:00:00Z".to_string(),
            started_at: Some("2026-07-07T00:00:01Z".to_string()),
            finished_at: None,
            principal: Some("ci@example.com".to_string()),
            error: None,
            result: None,
        }
    }

    /// A recorded job round-trips through the store, a terminal update replaces
    /// the prior record, and `list_jobs` sees every row.
    #[test]
    fn job_record_roundtrips_and_updates() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");
        let store = StateStore::open(&path).unwrap();

        assert!(store.get_job("job-1").unwrap().is_none());
        store.record_job(&sample_job("job-1", "running")).unwrap();

        let got = store.get_job("job-1").unwrap().expect("job present");
        assert_eq!(got.state, "running");
        assert_eq!(got.kind, "run");

        // Terminal update replaces the running record under the same key.
        let mut done = sample_job("job-1", "succeeded");
        done.finished_at = Some("2026-07-07T00:00:05Z".to_string());
        done.result = Some(serde_json::json!({ "version": "1", "command": "run" }));
        store.record_job(&done).unwrap();

        let got = store.get_job("job-1").unwrap().unwrap();
        assert_eq!(got.state, "succeeded");
        assert_eq!(got.result.unwrap()["command"], "run");

        store.record_job(&sample_job("job-2", "failed")).unwrap();
        let mut ids: Vec<String> = store
            .list_jobs()
            .unwrap()
            .into_iter()
            .map(|j| j.job_id)
            .collect();
        ids.sort();
        assert_eq!(ids, vec!["job-1".to_string(), "job-2".to_string()]);
    }

    /// A minimal blob (only `job_id`) forward-deserializes with every other
    /// field defaulted — the additive-field contract for the record.
    #[test]
    fn persisted_job_forward_deserializes_with_defaults() {
        let job: PersistedJob = serde_json::from_str(r#"{"job_id":"j"}"#).unwrap();
        assert_eq!(job.job_id, "j");
        assert_eq!(job.state, "");
        assert_eq!(job.kind, "");
        assert!(job.result.is_none());
        assert!(job.principal.is_none());
    }

    // -----------------------------------------------------------------------
    // VACUUM refcount (Phase 6) — refcount_for_hash + partition_vacuum_candidates
    // -----------------------------------------------------------------------

    #[test]
    fn refcount_for_hash_counts_ledger_rows() {
        // Sanity: refcount_for_hash matches list_artifacts_by_hash().len(),
        // and isolated hashes don't bleed into one another's counts.
        let (store, _dir) = temp_store();
        store
            .record_artifact(&make_artifact(
                "run-a",
                "m",
                "shared",
                "s3://b/p1/shared.parquet",
                1,
            ))
            .unwrap();
        store
            .record_artifact(&make_artifact(
                "run-b",
                "m",
                "shared",
                "s3://b/p2/shared.parquet",
                1,
            ))
            .unwrap();
        store
            .record_artifact(&make_artifact(
                "run-c",
                "m",
                "solo",
                "s3://b/p3/solo.parquet",
                1,
            ))
            .unwrap();

        assert_eq!(store.refcount_for_hash("shared").unwrap(), 2);
        assert_eq!(store.refcount_for_hash("solo").unwrap(), 1);
        assert_eq!(store.refcount_for_hash("never-written").unwrap(), 0);
    }

    #[test]
    fn vacuum_partition_single_reference_is_safe_to_delete() {
        // Baseline regression: when the candidate is the *only* recorded
        // reference, the partition routes it into safe_to_delete. This is
        // the path today's (pre-Phase-6, implicit-refcount=1) VACUUM
        // effectively walks — proving it still works confirms backward
        // compat is preserved.
        let (store, _dir) = temp_store();
        let art = make_artifact("run-1", "m", "h-solo", "s3://b/p/solo.parquet", 1);
        store.record_artifact(&art).unwrap();

        let partition =
            partition_vacuum_candidates(vec![art.clone()], |h| store.refcount_for_hash(h));

        assert_eq!(partition.safe_to_delete.len(), 1);
        assert_eq!(partition.safe_to_delete[0].blake3_hash, "h-solo");
        assert!(partition.still_referenced.is_empty());
        assert!(partition.lookup_errors.is_empty());
    }

    #[test]
    fn vacuum_partition_multi_reference_is_preserved() {
        // The cleanup invariant: two runs reference the same hash; even
        // though only run-a's artifact is in the candidate batch, the
        // bytes must not be deleted because run-b's row still points at
        // them.
        let (store, _dir) = temp_store();
        let art_a = make_artifact("run-a", "m", "h-shared", "s3://b/p1/shared.parquet", 1);
        let art_b = make_artifact("run-b", "m", "h-shared", "s3://b/p2/shared.parquet", 1);
        store.record_artifact(&art_a).unwrap();
        store.record_artifact(&art_b).unwrap();

        let partition =
            partition_vacuum_candidates(vec![art_a.clone()], |h| store.refcount_for_hash(h));

        assert!(partition.safe_to_delete.is_empty());
        assert_eq!(partition.still_referenced.len(), 1);
        assert_eq!(partition.still_referenced[0].run_id, "run-a");
        assert!(partition.lookup_errors.is_empty());
    }

    #[test]
    fn vacuum_partition_refcount_zero_is_preserved_fail_closed() {
        // A refcount of 0 means the ledger has *no* record of this hash —
        // either pre-ledger bytes or a writer that crashed before
        // recording. We don't pretend that's evidence of absence; the
        // row stays in still_referenced so an operator can investigate
        // before the byte is physically removed. This is "fail closed".
        let art = make_artifact("run-x", "m", "h-orphan", "s3://b/p/orphan.parquet", 1);

        let partition = partition_vacuum_candidates(vec![art.clone()], |_| Ok(0));

        assert!(partition.safe_to_delete.is_empty());
        assert_eq!(partition.still_referenced.len(), 1);
        assert_eq!(partition.still_referenced[0].blake3_hash, "h-orphan");
        assert!(partition.lookup_errors.is_empty());
    }

    #[test]
    fn vacuum_partition_lookup_error_does_not_catastrophically_fail() {
        // Graceful degradation: when refcount lookup errors for a hash,
        // the partition routes the candidate to still_referenced (fail
        // closed — never delete what we can't prove unreferenced) and
        // records the hash in lookup_errors so the caller can log it.
        // The function itself still returns Ok-equivalent (a populated
        // VacuumPartition), not a panic / bubble-up, so a single bad
        // row cannot nuke a sweep over thousands of healthy rows.
        let art_bad = make_artifact("run-x", "m", "h-bad", "s3://b/p/bad.parquet", 1);
        let art_good = make_artifact("run-y", "m", "h-good", "s3://b/p/good.parquet", 1);

        let partition = partition_vacuum_candidates(vec![art_bad.clone(), art_good.clone()], |h| {
            if h == "h-bad" {
                // Any StateError variant works for the test; Busy is
                // a realistic "ledger is locked" surface.
                Err(StateError::Busy {
                    path: "/tmp/state.redb".to_string(),
                })
            } else {
                Ok(1)
            }
        });

        assert_eq!(partition.safe_to_delete.len(), 1);
        assert_eq!(partition.safe_to_delete[0].blake3_hash, "h-good");
        assert_eq!(partition.still_referenced.len(), 1);
        assert_eq!(partition.still_referenced[0].blake3_hash, "h-bad");
        assert_eq!(partition.lookup_errors, vec!["h-bad".to_string()]);
    }

    #[test]
    fn vacuum_partition_memoises_lookup_per_hash() {
        // A partitioned commit can record many rows under the same hash.
        // We don't want to fire one ledger scan per row when one scan
        // per unique hash suffices. Wire a counter into the closure and
        // assert it fires exactly once per unique hash.
        let art1 = make_artifact("r", "m", "h-dup", "s3://b/p/a.parquet", 1);
        let art2 = make_artifact("r", "m", "h-dup", "s3://b/p/b.parquet", 1);
        let art3 = make_artifact("r", "m", "h-other", "s3://b/p/c.parquet", 1);

        let mut hits = 0u32;
        let partition = partition_vacuum_candidates(vec![art1, art2, art3], |_| {
            hits += 1;
            Ok(1)
        });

        // Two unique hashes ("h-dup", "h-other") → exactly two lookups.
        assert_eq!(hits, 2);
        assert_eq!(partition.safe_to_delete.len(), 3);
    }

    // ---------------------------------------------------------------------
    // Auditable-reuse spine: INPUT_INDEX + INPUT_PROVENANCE round-trips
    // ---------------------------------------------------------------------

    fn make_index_entry(input_hash: &str, run_id: &str, model: &str) -> InputIndexEntry {
        InputIndexEntry {
            input_hash: input_hash.to_string(),
            run_id: run_id.to_string(),
            model_name: model.to_string(),
            output_blake3: vec!["blake3:out".to_string()],
            output_path: vec!["s3://bucket/prefix/out.parquet".to_string()],
            proof_class: "strong".to_string(),
            recorded_at: Utc::now(),
        }
    }

    /// A real, canonicalisable `ModelIr` for the offline-recompute test.
    fn sample_model_ir() -> rocky_ir::ModelIr {
        let mut model_ir = rocky_ir::ModelIr::transformation(
            rocky_ir::TargetRef {
                catalog: "analytics".to_string(),
                schema: "marts".to_string(),
                table: "fct_orders".to_string(),
            },
            rocky_ir::MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT id, amount FROM raw.orders".to_string(),
            rocky_ir::GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model_ir.typed_columns = vec![
            rocky_ir::TypedColumn {
                name: "id".into(),
                data_type: rocky_ir::RockyType::Int64,
                nullable: false,
            },
            rocky_ir::TypedColumn {
                name: "amount".into(),
                data_type: rocky_ir::RockyType::Float64,
                nullable: true,
            },
        ];
        model_ir
    }

    #[test]
    fn input_index_put_get_round_trip() {
        let (store, _dir) = temp_store();
        let entry = make_index_entry("ih-abc", "run-1", "fct_orders");
        store
            .record_reuse_spine(std::slice::from_ref(&entry), &[])
            .unwrap();

        let got = store.get_by_input_hash("ih-abc").unwrap();
        assert_eq!(got.as_ref(), Some(&entry), "index entry must round-trip");

        // A different input_hash → no hit (the index is dormant: lookup only).
        assert!(store.get_by_input_hash("ih-not-there").unwrap().is_none());
    }

    #[test]
    fn provenance_put_get_round_trip() {
        let (store, _dir) = temp_store();
        let model_ir = sample_model_ir();
        let skip_hash = model_ir.skip_hash().expect("sample model canonicalises");
        let record = ProvenanceRecord {
            run_id: "run-7".to_string(),
            model_name: "fct_orders".to_string(),
            input_hash: "ih-xyz".to_string(),
            skip_hash: skip_hash.to_hex().to_string(),
            model_ir_canonical_json: model_ir.canonical_json(),
            upstreams: vec![crate::reuse::UpstreamIdentity::Content {
                upstream_key: "analytics.marts.dim_x".to_string(),
                blake3_hash: "up-hash".to_string(),
            }],
            output_blake3: vec!["blake3:out".to_string()],
            output_path: vec!["s3://bucket/prefix/out.parquet".to_string()],
            proof_class: "strong".to_string(),
            recorded_at: Utc::now(),
        };
        store
            .record_reuse_spine(&[], std::slice::from_ref(&record))
            .unwrap();

        let got = store.get_provenance("run-7", "fct_orders").unwrap();
        assert_eq!(got.as_ref(), Some(&record), "provenance must round-trip");

        // Wrong key → no hit.
        assert!(
            store
                .get_provenance("run-7", "other_model")
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn provenance_offline_ir_hash_recompute_matches() {
        // The offline-verify recipe's step 1: an auditor deserializes the
        // embedded canonical ModelIr JSON, recomputes its skip_hash, and
        // compares against the recorded value. This must match what produced
        // the record — without re-running the model.
        let (store, _dir) = temp_store();
        let model_ir = sample_model_ir();
        let recorded_skip_hash = model_ir
            .skip_hash()
            .expect("canonicalises")
            .to_hex()
            .to_string();
        let record = ProvenanceRecord {
            run_id: "run-9".to_string(),
            model_name: "fct_orders".to_string(),
            input_hash: "ih-9".to_string(),
            skip_hash: recorded_skip_hash.clone(),
            model_ir_canonical_json: model_ir.canonical_json(),
            upstreams: Vec::new(),
            output_blake3: vec!["blake3:out".to_string()],
            output_path: vec!["s3://bucket/prefix/out.parquet".to_string()],
            proof_class: "strong".to_string(),
            recorded_at: Utc::now(),
        };
        store
            .record_reuse_spine(&[], std::slice::from_ref(&record))
            .unwrap();

        // Read the record back and recompute the IR hash from the embedded
        // canonical JSON exactly as an offline verifier would.
        let got = store
            .get_provenance("run-9", "fct_orders")
            .unwrap()
            .unwrap();
        let reparsed: rocky_ir::ModelIr =
            serde_json::from_str(&got.model_ir_canonical_json).expect("canonical JSON round-trips");
        let recomputed = reparsed.skip_hash().expect("reparsed model canonicalises");
        assert_eq!(
            recomputed.to_hex().to_string(),
            got.skip_hash,
            "offline skip_hash recompute must equal the recorded value"
        );
        assert_eq!(got.skip_hash, recorded_skip_hash);
    }

    #[test]
    fn reuse_spine_empty_batch_is_noop() {
        let (store, _dir) = temp_store();
        // No transaction should be opened; nothing recorded.
        store.record_reuse_spine(&[], &[]).unwrap();
        assert!(store.get_by_input_hash("anything").unwrap().is_none());
    }

    #[test]
    fn input_index_entry_forward_deserializes_without_optional_fields() {
        // A v9 reader must accept a record written without the
        // `#[serde(default)]` output_* arrays (the minimum a future heuristic
        // entry might carry). Forward-deserialize safety for new fields.
        let minimal = serde_json::json!({
            "input_hash": "ih-min",
            "run_id": "run-x",
            "model_name": "m",
            "proof_class": "heuristic",
            "recorded_at": Utc::now(),
        });
        let parsed: InputIndexEntry =
            serde_json::from_value(minimal).expect("optional output_* fields default to empty");
        assert!(parsed.output_blake3.is_empty());
        assert!(parsed.output_path.is_empty());
        assert_eq!(parsed.proof_class, "heuristic");
    }

    #[test]
    fn provenance_record_forward_deserializes_without_optional_fields() {
        // Symmetric to the index-entry forward test: a v9 reader must accept a
        // provenance record written without the `#[serde(default)]` output_*
        // arrays. Forward-deserialize safety for new fields.
        let minimal = serde_json::json!({
            "run_id": "run-x",
            "model_name": "m",
            "input_hash": "ih",
            "skip_hash": "sh",
            "model_ir_canonical_json": "{}",
            "proof_class": "strong",
            "recorded_at": Utc::now(),
        });
        let parsed: ProvenanceRecord =
            serde_json::from_value(minimal).expect("optional output_* fields default to empty");
        assert!(parsed.output_blake3.is_empty());
        assert!(parsed.output_path.is_empty());
        assert!(
            parsed.upstreams.is_empty(),
            "a pre-field record must deserialize with an empty upstreams list"
        );
        assert_eq!(parsed.proof_class, "strong");
    }

    #[test]
    fn provenance_offline_input_hash_recompute_matches() {
        // The offline-verify recipe's input-side step: an auditor holding ONLY
        // a persisted ProvenanceRecord recomputes the model's input_hash from
        // the bytes in that record — skip_hash, the target identity parsed back
        // out of the canonical ModelIr JSON, and the persisted upstreams — and
        // it must equal the recorded input_hash. Nothing here touches the live
        // model: every input is derived from the deserialized record.
        let (store, _dir) = temp_store();
        let model_ir = sample_model_ir();
        let skip_hash = model_ir
            .skip_hash()
            .expect("canonicalises")
            .to_hex()
            .to_string();
        let upstreams = vec![crate::reuse::UpstreamIdentity::Content {
            upstream_key: "analytics.marts.dim_x".to_string(),
            blake3_hash: "up-hash".to_string(),
        }];
        let target_identity = format!(
            "{}.{}.{}",
            model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
        );
        let input_hash = crate::reuse::compute_input_hash(&skip_hash, &target_identity, &upstreams)
            .to_hex()
            .to_string();
        let record = ProvenanceRecord {
            run_id: "run-11".to_string(),
            model_name: "fct_orders".to_string(),
            input_hash: input_hash.clone(),
            skip_hash,
            model_ir_canonical_json: model_ir.canonical_json(),
            upstreams,
            output_blake3: vec!["blake3:out".to_string()],
            output_path: vec!["s3://bucket/prefix/out.parquet".to_string()],
            proof_class: "strong".to_string(),
            recorded_at: Utc::now(),
        };
        store
            .record_reuse_spine(&[], std::slice::from_ref(&record))
            .unwrap();

        // Read the record back and recompute input_hash purely from its bytes.
        let got = store
            .get_provenance("run-11", "fct_orders")
            .unwrap()
            .unwrap();
        assert_eq!(
            got.upstreams.len(),
            1,
            "the persisted upstreams must round-trip"
        );
        let reparsed: rocky_ir::ModelIr =
            serde_json::from_str(&got.model_ir_canonical_json).expect("canonical JSON round-trips");
        let recomputed_target = format!(
            "{}.{}.{}",
            reparsed.target.catalog, reparsed.target.schema, reparsed.target.table
        );
        let recomputed =
            crate::reuse::compute_input_hash(&got.skip_hash, &recomputed_target, &got.upstreams)
                .to_hex()
                .to_string();
        assert_eq!(
            recomputed, got.input_hash,
            "offline input_hash recompute from the persisted record must equal the recorded value"
        );
        assert_eq!(got.input_hash, input_hash);
    }
}
