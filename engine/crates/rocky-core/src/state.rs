use std::path::Path;

use redb::{Database, ReadableTable, TableDefinition};
use thiserror::Error;

use crate::ir::WatermarkState;

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
/// Key/value store for internal metadata (e.g. `"schema_version"`).
const METADATA: TableDefinition<&str, &str> = TableDefinition::new("metadata");

/// Schema version for the current set of state tables.
///
/// Increment this constant whenever a table is added, removed, or structurally
/// changed in a way that is incompatible with an older binary. Rocky will
/// refuse to open a database whose stored version exceeds this value.
const CURRENT_SCHEMA_VERSION: u32 = 3;

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
}

impl From<redb::TransactionError> for StateError {
    fn from(e: redb::TransactionError) -> Self {
        StateError::Transaction(Box::new(e))
    }
}

/// Embedded state store backed by redb for tracking watermarks and run history.
pub struct StateStore {
    db: Database,
}

impl StateStore {
    /// Opens or creates a state store at the given path.
    ///
    /// On first open the schema version is written to the `metadata` table.
    /// On subsequent opens the stored version is compared to
    /// [`CURRENT_SCHEMA_VERSION`]; if the database was written by a newer
    /// binary (stored > current) an error is returned immediately so that the
    /// caller can surface a clear message rather than silently misreading data.
    pub fn open(path: &Path) -> Result<Self, StateError> {
        let db = Database::create(path)?;

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
                    metadata.insert("schema_version", "3")?;
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
        }
        txn.commit()?;

        Ok(StateStore { db })
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RunStatus {
    Success,
    PartialFailure,
    Failure,
}

/// What triggered the run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RunTrigger {
    Manual,
    Sensor,
    Schedule,
    Ci,
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
    pub bytes_scanned: Option<u64>,
    pub bytes_written: Option<u64>,
}

/// A complete pipeline run record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub finished_at: chrono::DateTime<chrono::Utc>,
    pub status: RunStatus,
    pub models_executed: Vec<ModelExecution>,
    pub trigger: RunTrigger,
    pub config_hash: String,
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

    #[test]
    fn test_record_and_get_run() {
        let (store, _dir) = temp_store();
        let run = RunRecord {
            run_id: "run-001".to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            status: RunStatus::Success,
            models_executed: vec![ModelExecution {
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
            trigger: RunTrigger::Manual,
            config_hash: "config-hash".to_string(),
        };
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
            let run = RunRecord {
                run_id: format!("run-{i:03}"),
                started_at: Utc::now(),
                finished_at: Utc::now(),
                status: RunStatus::Success,
                models_executed: vec![],
                trigger: RunTrigger::Manual,
                config_hash: "hash".to_string(),
            };
            store.record_run(&run).unwrap();
        }

        let runs = store.list_runs(3).unwrap();
        assert_eq!(runs.len(), 3);
    }

    #[test]
    fn test_get_model_history() {
        let (store, _dir) = temp_store();
        let run = RunRecord {
            run_id: "run-001".to_string(),
            started_at: Utc::now(),
            finished_at: Utc::now(),
            status: RunStatus::Success,
            models_executed: vec![
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
            trigger: RunTrigger::Manual,
            config_hash: "hash".to_string(),
        };
        store.record_run(&run).unwrap();

        let history = store.get_model_history("orders", 10).unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].duration_ms, 500);
    }

    #[test]
    fn test_get_average_duration() {
        let (store, _dir) = temp_store();
        for (i, dur) in [100u64, 200, 300].iter().enumerate() {
            let run = RunRecord {
                run_id: format!("run-{i}"),
                started_at: Utc::now(),
                finished_at: Utc::now(),
                status: RunStatus::Success,
                models_executed: vec![ModelExecution {
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
                trigger: RunTrigger::Manual,
                config_hash: "hash".to_string(),
            };
            store.record_run(&run).unwrap();
        }

        let avg = store.get_average_duration("orders", 10).unwrap().unwrap();
        assert!((avg - 200.0).abs() < 0.01);
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
}
