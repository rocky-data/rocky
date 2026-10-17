//! The stored bytes each model execution records.
//!
//! Each execution site calls one of these right after its write. The result
//! rides on `MaterializationOutput::stored_bytes` (never serialized) and
//! `RunOutput::to_run_record` copies it onto the persisted
//! [`rocky_core::state::ModelExecution::stored_bytes`]. `rocky cost` shows it.
//!
//! ```text
//!   strategy stores a whole table this write measures? ──no──▶ None
//!            │ yes
//!   stored_table_bytes(t) ── Ok(Some(b)) ─▶ Some(b)
//!                         ── Ok(None) ────▶ None   (adapter cannot measure)
//!                         ── Err ─────────▶ None   (logged at debug)
//!
//!   content-addressed write ──▶ Some(sum of written file sizes, data_files)
//! ```
//!
//! `None` always means unknown. A failed read never fails the run: the data
//! is already written. Replicated tables are not measured, so replication
//! cost is unchanged.

use rocky_core::state::{StoredBytes, StoredBytesBasis};
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::{MaterializationStrategy, TableRef};
use tracing::debug;

use super::run_content_addressed::ContentAddressedRunSummary;

/// Whether one write of this strategy leaves a table whose stored bytes
/// belong to this execution.
///
/// Exhaustive on purpose (no `_ =>`): a new strategy fails to compile here
/// until someone decides whether its output is measured.
pub(crate) fn measures_stored_bytes(strategy: &MaterializationStrategy) -> bool {
    match strategy {
        // No stored table, or a warehouse-managed one Rocky does not write.
        MaterializationStrategy::View
        | MaterializationStrategy::Ephemeral
        | MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. } => false,
        // One execution per partition: the whole table's size would repeat
        // on every partition row, so it is not recorded per partition.
        MaterializationStrategy::TimeInterval { .. } => false,
        // Content-addressed writes record their file sizes through
        // `content_addressed_stored_bytes` instead.
        MaterializationStrategy::ContentAddressed { .. }
        | MaterializationStrategy::FullRefresh
        | MaterializationStrategy::Incremental { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::Microbatch { .. }
        | MaterializationStrategy::Snapshot(_) => true,
    }
}

/// The bytes a written table stores, as the adapter measures them, or
/// `None` when the strategy is not measured or the adapter cannot say.
pub(crate) async fn observe_stored_bytes(
    warehouse: &dyn WarehouseAdapter,
    strategy: &MaterializationStrategy,
    table: &TableRef,
) -> Option<StoredBytes> {
    if !measures_stored_bytes(strategy) {
        return None;
    }
    observe_table_stored_bytes(warehouse, table).await
}

/// The bytes a written table stores, as the adapter measures them, or
/// `None` when the adapter cannot say or the read fails.
pub(crate) async fn observe_table_stored_bytes(
    warehouse: &dyn WarehouseAdapter,
    table: &TableRef,
) -> Option<StoredBytes> {
    match warehouse.stored_table_bytes(table).await {
        Ok(bytes) => bytes,
        Err(e) => {
            debug!(
                table = %format!("{}.{}.{}", table.catalog, table.schema, table.table),
                error = %e,
                "could not measure the stored bytes after the write; \
                 recording unknown (the run is not affected)"
            );
            None
        }
    }
}

/// The exact stored bytes of a content-addressed output: the summed size of
/// every live Parquet data file the run committed or referenced.
pub(crate) fn content_addressed_stored_bytes(summary: &ContentAddressedRunSummary) -> StoredBytes {
    StoredBytes {
        bytes: summary
            .written_files
            .iter()
            .fold(0_u64, |acc, f| acc.saturating_add(f.size_bytes)),
        basis: StoredBytesBasis::DataFiles,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exact basis sums every live file, partition groups included.
    #[test]
    fn content_addressed_sums_every_written_file() {
        use super::super::run_content_addressed::WrittenFile;
        let file = |size_bytes| WrittenFile {
            blake3_hash: "h".into(),
            file_path: "f.parquet".into(),
            commit_version: 1,
            size_bytes,
        };
        let summary = ContentAddressedRunSummary {
            num_rows: 3,
            blake3_hash: "h".into(),
            commit_version: 1,
            file_path: "f.parquet".into(),
            size_bytes: 7,
            output_column_hashes: Vec::new(),
            reused_from: None,
            written_files: vec![file(100), file(7)],
        };
        assert_eq!(
            content_addressed_stored_bytes(&summary),
            StoredBytes {
                bytes: 107,
                basis: StoredBytesBasis::DataFiles
            }
        );
    }

    #[cfg(feature = "duckdb")]
    mod through_run {
        use super::super::super::run_output_version::tests::through_run::{run_project, strategy};
        use rocky_core::state::{StoredBytes, StoredBytesBasis};

        /// DuckDB's default block size, verified on DuckDB 1.5.6.
        const BLOCK_SIZE: u64 = 262_144;

        /// A file-backed DuckDB run records a non-zero allocated-blocks
        /// figure for each stored table, and nothing for a view. The tables
        /// are small enough to sit in the WAL after the write, so this also
        /// proves the checkpoint runs before the count.
        #[tokio::test]
        async fn duckdb_file_records_stored_bytes_for_tables_only() {
            let full_refresh = strategy("full_refresh");
            let view = strategy("view");
            let models = [
                (
                    "fr",
                    "SELECT range AS x, 'row ' || range AS label FROM range(1000)\n",
                    full_refresh.as_str(),
                ),
                ("v", "SELECT 2 AS y\n", view.as_str()),
            ];
            let db = tempfile::TempDir::new().unwrap();
            let adapter = format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n",
                db.path().join("w.duckdb").display()
            );
            let (result, execs, _, _tmp) = run_project(&adapter, &models, |_| {}).await;
            result.expect("the DuckDB run must succeed");
            let Some(StoredBytes { bytes, basis }) = execs["fr"].stored_bytes else {
                panic!("a stored table must record stored bytes: {:?}", execs["fr"]);
            };
            assert_eq!(basis, StoredBytesBasis::AllocatedBlocks);
            assert!(bytes > 0, "a 1000-row table occupies at least one block");
            assert_eq!(bytes % BLOCK_SIZE, 0, "whole blocks: {bytes}");
            assert_eq!(execs["v"].stored_bytes, None, "a view stores no data");
        }

        /// An in-memory DuckDB database has no file blocks: unknown, not 0.
        #[tokio::test]
        async fn duckdb_in_memory_records_unknown() {
            let full_refresh = strategy("full_refresh");
            let models = [("fr", "SELECT 1 AS x\n", full_refresh.as_str())];
            let (result, execs, _, _tmp) =
                run_project("[adapter]\ntype = \"duckdb\"\n", &models, |_| {}).await;
            result.expect("the DuckDB run must succeed");
            assert_eq!(execs["fr"].status, "success");
            assert_eq!(execs["fr"].stored_bytes, None);
        }

        /// An adapter that cannot measure stored bytes (the trait default)
        /// records unknown.
        #[tokio::test]
        async fn an_unmeasurable_adapter_records_unknown() {
            let full_refresh = strategy("full_refresh");
            let models = [("fr", "SELECT 1 AS x\n", full_refresh.as_str())];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"observe-version\"\n";
            let (result, execs, _, _tmp) = run_project(adapter, &models, |_| {}).await;
            result.expect("the run must succeed");
            assert_eq!(execs["fr"].status, "success");
            assert_eq!(execs["fr"].stored_bytes, None);
        }
    }
}
