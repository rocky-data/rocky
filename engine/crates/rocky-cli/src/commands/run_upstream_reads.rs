//! The upstream reads each model build records.
//!
//! For a production model build on an adapter that can profile (DuckDB), the
//! run reads what each statement scanned from each upstream table. The
//! figures ride on `MaterializationOutput::upstream_reads` and are persisted
//! on `ModelExecution::upstream_reads`. This records measurements only: no
//! cost, no split across consumers.
//!
//! ```text
//!   model build ── enabled? ──no──▶ plain statements, upstream_reads None
//!                     │ yes (production run, DuckDB, table-building strategy)
//!                     ▼
//!   each statement ─▶ warehouse.execute_statement_with_upstream_reads
//!        ├─ Some(reads) ─▶ merged into the model's total
//!        └─ None ────────▶ the whole model's reads are lost (never a partial sum)
//!   finish ─▶ drop the model's own target ─▶ Some(total) | None
//! ```
//!
//! `None` always means not measured. A failed measurement never fails the
//! model.

use rocky_core::state::UpstreamReads;
use rocky_core::traits::{AdapterResult, ExecutionStats, WarehouseAdapter};
use rocky_ir::MaterializationStrategy;

/// Whether the build of this strategy reads its upstream tables while the
/// statements run.
///
/// Exhaustive on purpose (no `_ =>`): a new strategy fails to compile here
/// until someone decides whether its build reads data.
pub(crate) fn reads_upstream_at_build(strategy: &MaterializationStrategy) -> bool {
    match strategy {
        // A view, an ephemeral model and a warehouse-managed object read
        // nothing when they are created: the reads happen later.
        MaterializationStrategy::View
        | MaterializationStrategy::Ephemeral
        | MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. } => false,
        // One execution per partition, a Rocky-owned write, and the snapshot
        // path each have their own executor: not recorded.
        MaterializationStrategy::TimeInterval { .. }
        | MaterializationStrategy::ContentAddressed { .. }
        | MaterializationStrategy::Snapshot(_) => false,
        MaterializationStrategy::FullRefresh
        | MaterializationStrategy::Incremental { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::Microbatch { .. } => true,
    }
}

/// Whether a run's mode allows recording: a production run only. A shadow or
/// branch run writes elsewhere, and a backfill (`model_set`) rebuilds
/// selected models.
pub(crate) fn run_records_upstream_reads(
    shadow_config: Option<&rocky_core::shadow::ShadowConfig>,
    model_set: Option<&std::collections::BTreeSet<String>>,
) -> bool {
    shadow_config.is_none() && model_set.is_none()
}

/// The reads of one model build, collected statement by statement.
#[derive(Debug)]
pub(crate) enum UpstreamReadsAcc {
    /// Not recording: statements run the ordinary way.
    Off,
    /// Recording; every statement so far was measured.
    Collecting(UpstreamReads),
    /// A statement was not measured, so the model's total is unknown.
    Lost,
}

impl UpstreamReadsAcc {
    /// Record when `record` (the run mode allows it), the strategy reads at
    /// build, and the adapter can profile.
    pub(crate) fn new(
        record: bool,
        strategy: &MaterializationStrategy,
        warehouse: &dyn WarehouseAdapter,
    ) -> Self {
        if record && reads_upstream_at_build(strategy) && warehouse.measures_upstream_reads() {
            Self::Collecting(UpstreamReads::default())
        } else {
            Self::Off
        }
    }

    fn absorb(&mut self, measured: Option<UpstreamReads>) {
        match (std::mem::replace(self, Self::Lost), measured) {
            (Self::Off, _) => *self = Self::Off,
            (Self::Collecting(mut total), Some(reads)) => {
                total.merge(reads);
                *self = Self::Collecting(total);
            }
            (Self::Collecting(_) | Self::Lost, None) | (Self::Lost, Some(_)) => {}
        }
    }

    /// A statement of this build ran outside [`run_model_statement`] (the
    /// atomic DROP and CREATE of a kind switch), so the total is unknown.
    /// Call this for every such statement: an unmeasured statement must never
    /// look like "read nothing".
    pub(crate) fn mark_unmeasured(&mut self) {
        if !matches!(self, Self::Off) {
            *self = Self::Lost;
        }
    }

    /// The model's reads without its own target (an incremental load reads
    /// its target for the watermark; that is not an upstream). `None` when
    /// not recording or when any statement was not measured.
    pub(crate) fn finish(
        self,
        target: &rocky_ir::TargetRef,
        default_catalog: Option<&str>,
    ) -> Option<UpstreamReads> {
        let Self::Collecting(mut total) = self else {
            return None;
        };
        total
            .reads
            .retain(|read| !is_own_target(&read.table, target, default_catalog));
        Some(total)
    }
}

/// Whether `read_table` (`catalog.schema.table`, as the warehouse reports
/// it) is the model's own target. A target with no catalog resolves against
/// the adapter's default catalog; with none known, schema and table decide.
/// DuckDB folds identifier case, so names compare case-insensitively.
fn is_own_target(
    read_table: &str,
    target: &rocky_ir::TargetRef,
    default_catalog: Option<&str>,
) -> bool {
    let mut parts = read_table.splitn(3, '.');
    let (Some(catalog), Some(schema), Some(table)) = (parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    let target_catalog = if target.catalog.is_empty() {
        default_catalog
    } else {
        Some(target.catalog.as_str())
    };
    schema.eq_ignore_ascii_case(&target.schema)
        && table.eq_ignore_ascii_case(&target.table)
        && target_catalog.is_none_or(|expected| expected.eq_ignore_ascii_case(catalog))
}

/// Run one model statement. When `acc` is recording the statement is
/// profiled and its reads join the total; otherwise it runs as before.
pub(crate) async fn run_model_statement(
    warehouse: &dyn WarehouseAdapter,
    sql: &str,
    acc: &mut UpstreamReadsAcc,
) -> AdapterResult<ExecutionStats> {
    if matches!(acc, UpstreamReadsAcc::Off) {
        return warehouse.execute_statement_with_stats(sql).await;
    }
    let (stats, measured) = warehouse.execute_statement_with_upstream_reads(sql).await?;
    acc.absorb(measured);
    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::state::UpstreamRead;

    fn target(catalog: &str, schema: &str, table: &str) -> rocky_ir::TargetRef {
        rocky_ir::TargetRef {
            catalog: catalog.into(),
            schema: schema.into(),
            table: table.into(),
        }
    }

    fn read(table: &str) -> UpstreamReads {
        UpstreamReads {
            reads: vec![UpstreamRead {
                table: table.into(),
                rows_scanned: 1,
                projected_columns: Vec::new(),
                est_bytes: None,
            }],
            unattributed_scans: 0,
        }
    }

    #[test]
    fn the_models_own_target_is_not_an_upstream() {
        let mut acc = UpstreamReadsAcc::Collecting(UpstreamReads::default());
        acc.absorb(Some(read("memory.main.src")));
        acc.absorb(Some(read("memory.main.OUT")));
        let got = acc
            .finish(&target("", "main", "out"), Some("memory"))
            .unwrap();
        assert_eq!(got.reads.len(), 1);
        assert_eq!(got.reads[0].table, "memory.main.src");
    }

    #[test]
    fn another_catalogs_table_of_the_same_name_is_kept() {
        let mut acc = UpstreamReadsAcc::Collecting(UpstreamReads::default());
        acc.absorb(Some(read("other.main.out")));
        let got = acc
            .finish(&target("", "main", "out"), Some("memory"))
            .unwrap();
        assert_eq!(got.reads.len(), 1);
    }

    #[test]
    fn one_unmeasured_statement_loses_the_whole_model() {
        let mut acc = UpstreamReadsAcc::Collecting(UpstreamReads::default());
        acc.absorb(Some(read("memory.main.src")));
        acc.absorb(None);
        acc.absorb(Some(read("memory.main.src")));
        assert_eq!(acc.finish(&target("", "main", "out"), None), None);
    }

    #[test]
    fn an_unmeasured_statement_outside_the_measured_call_loses_the_model() {
        let mut acc = UpstreamReadsAcc::Collecting(UpstreamReads::default());
        acc.mark_unmeasured();
        assert_eq!(acc.finish(&target("", "main", "out"), None), None);
        let mut off = UpstreamReadsAcc::Off;
        off.mark_unmeasured();
        assert!(matches!(off, UpstreamReadsAcc::Off));
    }

    #[test]
    fn an_off_accumulator_stays_off() {
        let mut acc = UpstreamReadsAcc::Off;
        acc.absorb(Some(read("memory.main.src")));
        assert_eq!(acc.finish(&target("", "main", "out"), None), None);
    }

    #[test]
    fn only_a_production_run_records() {
        assert!(run_records_upstream_reads(None, None));
        let set = std::collections::BTreeSet::from(["m".to_string()]);
        assert!(!run_records_upstream_reads(None, Some(&set)));
    }
}
