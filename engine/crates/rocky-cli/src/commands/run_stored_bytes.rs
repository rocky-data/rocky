//! The stored bytes each model execution records.
//!
//! The figure rides on `MaterializationOutput::stored_bytes` (never
//! serialized) and `RunOutput::to_run_record` copies it onto the persisted
//! [`rocky_core::state::ModelExecution::stored_bytes`]. `rocky cost` shows it.
//!
//! The model loop only notes which outputs to measure. One batch measures
//! them all after the last model finished, so the adapter's work (on DuckDB,
//! one `CHECKPOINT` per database) happens once, with no model's write in
//! flight:
//!
//! ```text
//!   model write ok ── strategy measured? ──no──▶ None
//!                            │ yes
//!                  PendingStoredBytes::note(index, table)
//!                            ⋮  (every model of the phase)
//!   PendingStoredBytes::measure ─ cancelled ─────────▶ all None (no read)
//!        │ stored_tables_bytes(all tables)
//!        ├─ Ok(per table) ─▶ Some(b) or None per table
//!        └─ Err ───────────▶ all None (logged at debug)
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
use tracing::{debug, warn};

use super::run_content_addressed::ContentAddressedRunSummary;
use crate::output::MaterializationOutput;

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
        // Content-addressed writes record their exact file sizes through
        // `content_addressed_stored_bytes`, never through the adapter.
        MaterializationStrategy::ContentAddressed { .. } => false,
        MaterializationStrategy::FullRefresh
        | MaterializationStrategy::Incremental { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::Microbatch { .. }
        | MaterializationStrategy::Snapshot(_) => true,
    }
}

/// The outputs of one model execution phase whose stored bytes are measured
/// after its last model.
#[derive(Debug, Default)]
pub(crate) struct PendingStoredBytes {
    /// `(index into RunOutput::materializations, the table it wrote)`.
    entries: Vec<(usize, TableRef)>,
}

impl PendingStoredBytes {
    /// Note the materialization just pushed at `index` if its strategy
    /// stores a table this write measures. `asset_key` is the
    /// `[catalog, schema, table]` the execution site wrote.
    pub(crate) fn note(
        &mut self,
        strategy: &MaterializationStrategy,
        index: usize,
        asset_key: &[String],
    ) {
        if !measures_stored_bytes(strategy) {
            return;
        }
        let [catalog, schema, table] = asset_key else {
            debug!(?asset_key, "not a three-part target; stored bytes unknown");
            return;
        };
        self.entries.push((
            index,
            TableRef {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.clone(),
            },
        ));
    }

    /// Measure every noted output in one adapter call and record each
    /// figure on its materialization. Drains the list, so a second call does
    /// nothing.
    ///
    /// Best-effort: a cancelled run skips the read, and any failure leaves
    /// the figure unknown. It never fails the run and never changes a
    /// model's status.
    pub(crate) async fn measure(
        &mut self,
        warehouse: &dyn WarehouseAdapter,
        materializations: &mut [MaterializationOutput],
        cancel: &tokio_util::sync::CancellationToken,
    ) {
        let entries = std::mem::take(&mut self.entries);
        if entries.is_empty() {
            return;
        }
        if cancel.is_cancelled() {
            debug!(
                tables = entries.len(),
                "run cancelled; stored bytes not measured (recorded as unknown)"
            );
            return;
        }
        let tables: Vec<TableRef> = entries.iter().map(|(_, t)| t.clone()).collect();
        let measured = match warehouse.stored_tables_bytes(&tables).await {
            Ok(measured) if measured.len() == tables.len() => measured,
            Ok(measured) => {
                warn!(
                    asked = tables.len(),
                    answered = measured.len(),
                    "the adapter answered a different number of stored-bytes \
                     figures; recording unknown (the run is not affected)"
                );
                return;
            }
            Err(e) => {
                // Debug, not warn: the figure is advisory, and a read-only
                // attached database would otherwise warn on every run.
                debug!(
                    error = %e,
                    "could not measure the stored bytes after the run; \
                     recording unknown (the run is not affected)"
                );
                return;
            }
        };
        for ((index, table), bytes) in entries.into_iter().zip(measured) {
            let Some(mat) = materializations.get_mut(index) else {
                continue;
            };
            if mat.asset_key.as_slice()
                != [
                    table.catalog.as_str(),
                    table.schema.as_str(),
                    table.table.as_str(),
                ]
            {
                debug!(index, "materialization moved; stored bytes unknown");
                continue;
            }
            mat.stored_bytes = bytes;
        }
    }
}

/// The bytes one written table stores, measured on its own (one adapter
/// call, so on DuckDB one checkpoint). For a run that writes exactly one
/// table. `None` when the adapter cannot say or the read fails.
pub(crate) async fn measure_single_table(
    warehouse: &dyn WarehouseAdapter,
    table: &TableRef,
) -> Option<StoredBytes> {
    match warehouse
        .stored_tables_bytes(std::slice::from_ref(table))
        .await
    {
        Ok(measured) => measured.into_iter().next().flatten(),
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
        /// figure for each stored table, and nothing for a view, with ONE
        /// `CHECKPOINT` for the whole run. The tables are small enough to sit
        /// in the WAL after their writes, so non-zero whole blocks also prove
        /// the checkpoint ran before the counts.
        #[tokio::test]
        async fn duckdb_file_records_stored_bytes_with_one_checkpoint() {
            let full_refresh = strategy("full_refresh");
            let view = strategy("view");
            let models = [
                (
                    "fr",
                    "SELECT range AS x, 'row ' || range AS label FROM range(1000)\n",
                    full_refresh.as_str(),
                ),
                (
                    "fr2",
                    "SELECT range AS id FROM range(50)\n",
                    full_refresh.as_str(),
                ),
                (
                    "fr3",
                    "SELECT range AS id, range * 2 AS twice FROM range(200)\n",
                    full_refresh.as_str(),
                ),
                ("v", "SELECT 2 AS y\n", view.as_str()),
            ];
            let db = tempfile::TempDir::new().unwrap();
            let db_path = db.path().join("w.duckdb");
            let adapter = format!(
                "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n",
                db_path.display()
            );
            let (result, execs, _, _tmp) = run_project(&adapter, &models, |_| {}).await;
            result.expect("the DuckDB run must succeed");
            for model in ["fr", "fr2", "fr3"] {
                let Some(StoredBytes { bytes, basis }) = execs[model].stored_bytes else {
                    panic!(
                        "a stored table must record stored bytes: {:?}",
                        execs[model]
                    );
                };
                assert_eq!(basis, StoredBytesBasis::AllocatedBlocks, "{model}");
                assert!(bytes > 0, "{model} occupies at least one block");
                assert_eq!(bytes % BLOCK_SIZE, 0, "{model}: whole blocks: {bytes}");
            }
            assert_eq!(execs["v"].stored_bytes, None, "a view stores no data");
            assert_eq!(
                rocky_duckdb::adapter::checkpoints_issued_for_test(&db_path),
                1,
                "one checkpoint per run, not one per model"
            );
        }

        /// A failed measurement records unknown for every table and leaves
        /// the run and each model successful.
        #[tokio::test]
        async fn a_failed_measurement_records_unknown_and_the_run_succeeds() {
            let full_refresh = strategy("full_refresh");
            let models = [
                ("fr", "SELECT 1 AS x\n", full_refresh.as_str()),
                ("fr2", "SELECT 2 AS y\n", full_refresh.as_str()),
            ];
            let adapter = "[adapter]\ntype = \"test-fail-write\"\npath = \"stored-bytes-fail\"\n";
            let (result, execs, _, _tmp) = run_project(adapter, &models, |_| {}).await;
            result.expect("a failed measurement must not fail the run");
            for model in ["fr", "fr2"] {
                assert_eq!(execs[model].status, "success", "{model}");
                assert_eq!(execs[model].stored_bytes, None, "{model}");
            }
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
