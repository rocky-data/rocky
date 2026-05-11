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

use object_store::ObjectStore;

pub mod errors;

pub use errors::{Result, UniformWriterError};

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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
