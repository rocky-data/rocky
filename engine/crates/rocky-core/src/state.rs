use std::fs::File;
use std::path::Path;

use fs4::FileExt;
use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::ir::WatermarkState;
use crate::schema_cache::SchemaCacheEntry;

const WATERMARKS: TableDefinition<&str, &[u8]> = TableDefinition::new("watermarks");
const CHECK_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("check_history");
const RUN_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("run_history");
const QUALITY_HISTORY: TableDefinition<&str, &[u8]> = TableDefinition::new("quality_history");
const DAG_SNAPSHOTS: TableDefinition<&str, &[u8]> = TableDefinition::new("dag_snapshots");
const RUN_PROGRESS: TableDefinition<&str, &[u8]> = TableDefinition::new("run_progress");
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
/// Cache of `DESCRIBE TABLE` results for source warehouses (Arc 7 wave 2
/// wave-2).
///
/// Key format: `"<catalog>.<schema>.<table>"` (lowercased). Value: serialized
/// [`crate::schema_cache::SchemaCacheEntry`]. Read by
/// `rocky_compiler::schema_cache::load_source_schemas_from_cache` to populate
/// `CompilerConfig.source_schemas` without a live round-trip. Written
/// opportunistically by `rocky run`'s drift/materialize paths and explicitly
/// by `rocky discover --with-schemas` (wiring lands in PR 1b / PR 2 / PR 3).
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
const CURRENT_SCHEMA_VERSION: u32 = 6;

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
        Self::open_inner(path, true)
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
        Self::open_inner(path, false)
    }

    fn open_inner(path: &Path, lock: bool) -> Result<Self, StateError> {
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

        // Single write transaction: check/write schema version AND ensure all
        // tables exist. Committing them together means the version stamp and
        // the table layout are always consistent.
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
                        return Err(StateError::SchemaMismatch {
                            found,
                            expected: CURRENT_SCHEMA_VERSION,
                            path: path.display().to_string(),
                        });
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

            let _table = txn.open_table(WATERMARKS)?;
            let _table = txn.open_table(CHECK_HISTORY)?;
            let _table = txn.open_table(RUN_HISTORY)?;
            let _table = txn.open_table(QUALITY_HISTORY)?;
            let _table = txn.open_table(DAG_SNAPSHOTS)?;
            let _table = txn.open_table(RUN_PROGRESS)?;
            let _table = txn.open_table(PARTITIONS)?;
            let _table = txn.open_table(GRACE_PERIODS)?;
            let _table = txn.open_table(LOADED_FILES)?;
            let _table = txn.open_table(BRANCHES)?;
            let _table = txn.open_table(SCHEMA_CACHE)?;
            let _table = txn.open_table(IDEMPOTENCY_KEYS)?;
        }
        txn.commit()?;

        Ok(StateStore {
            db,
            _lock_file: lock_file,
        })
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
    pub fn record_table_progress(
        &self,
        run_id: &str,
        progress: &TableProgress,
    ) -> Result<(), StateError> {
        let mut run_progress = self
            .get_run_progress(run_id)?
            .unwrap_or_else(|| RunProgress {
                run_id: run_id.to_string(),
                started_at: chrono::Utc::now(),
                total_tables: 0,
                tables: Vec::new(),
            });
        run_progress.tables.push(progress.clone());

        let bytes = serde_json::to_vec(&run_progress)?;
        let txn = self.db.begin_write()?;
        {
            let mut table = txn.open_table(RUN_PROGRESS)?;
            table.insert(run_id, bytes.as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Get the progress for a run.
    pub fn get_run_progress(&self, run_id: &str) -> Result<Option<RunProgress>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_PROGRESS)?;
        match table.get(run_id)? {
            Some(value) => Ok(Some(serde_json::from_slice(value.value())?)),
            None => Ok(None),
        }
    }

    /// Get the most recent run progress (for --resume-latest).
    pub fn get_latest_run_progress(&self) -> Result<Option<RunProgress>, StateError> {
        let txn = self.db.begin_read()?;
        let table = txn.open_table(RUN_PROGRESS)?;
        let mut latest: Option<RunProgress> = None;
        for entry in table.iter()? {
            let (_, value) = entry?;
            let progress: RunProgress = serde_json::from_slice(value.value())?;
            if latest
                .as_ref()
                .is_none_or(|l| progress.started_at > l.started_at)
            {
                latest = Some(progress);
            }
        }
        Ok(latest)
    }
}

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
    // Schema cache (Arc 7 wave 2 wave-2)
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
/// Unifies the CLI and LSP defaults that diverged through Arc 7 wave 2
/// wave-2: the CLI wrote `./rocky-state.redb` in CWD while the LSP read
/// `<models_dir>/.rocky-state.redb`, so the schema-cache write tap and
/// inlay-hint read path missed each other in every project where they
/// weren't already co-located.
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
        }
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
                bytes_scanned: None,
                bytes_written: None,
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
                    bytes_scanned: None,
                    bytes_written: None,
                },
                ModelExecution {
                    model_name: "customers".to_string(),
                    started_at: Utc::now(),
                    finished_at: Utc::now(),
                    duration_ms: 300,
                    rows_affected: None,
                    status: "success".to_string(),
                    sql_hash: "h2".to_string(),
                    bytes_scanned: None,
                    bytes_written: None,
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
                    bytes_scanned: None,
                    bytes_written: None,
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
    // Schema cache (Arc 7 wave 2 wave-2)
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
    /// `DatabaseAlreadyOpen` error. Coordinated with a barrier (no
    /// time-based race), per the advisor's "no sleep-and-pray" rule.
    #[test]
    fn open_read_only_retries_and_succeeds_after_brief_hold() {
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        let dir = TempDir::new().unwrap();
        let path = dir.path().join("state.redb");

        // Seed the file so `open_read_only` has something to attach to.
        StateStore::open(&path).unwrap();

        let lock_acquired = Arc::new(Barrier::new(2));
        let release_now = Arc::new(Barrier::new(2));

        let path_holder = path.clone();
        let lock_acquired_clone = Arc::clone(&lock_acquired);
        let release_now_clone = Arc::clone(&release_now);
        let holder = thread::spawn(move || {
            let db = Database::create(&path_holder).unwrap();
            lock_acquired_clone.wait();
            // Hold for ~120ms — well within the 250ms retry budget but
            // long enough that the first 1–2 retry attempts will fail.
            thread::sleep(Duration::from_millis(120));
            release_now_clone.wait();
            drop(db);
        });

        lock_acquired.wait();
        // Open while the holder is still mid-sleep. Retry should mask the
        // collision and eventually succeed once the holder drops.
        let opener_path = path.clone();
        let opener = thread::spawn(move || StateStore::open_read_only(&opener_path));
        // Tell the holder it can drop after a beat — the opener is now
        // looping through its retries.
        release_now.wait();

        let result = opener.join().unwrap();
        holder.join().unwrap();

        assert!(
            result.is_ok(),
            "open_read_only should retry past brief redb-lock contention, got error: {}",
            result.err().map(|e| e.to_string()).unwrap_or_default()
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
}
