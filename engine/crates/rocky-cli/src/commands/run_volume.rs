//! The product volume check `rocky run` makes after a production build.
//!
//! A product may declare `[product.output.volume]`: a row-count range one
//! load of its output must hold. Right after a successful production build
//! of that output, the run counts the load once and judges the count. What
//! "the load" is depends on the output model's strategy
//! ([`VolumeBasis`]):
//!
//! ```text
//!   build ok ── production? ──no──▶ nothing counted (no `volume` field)
//!                   │ yes
//!          volume declared for the model? ──no──▶ nothing counted
//!                   │ yes
//!          a counted strategy? ──no──▶ not_evaluated (never a failure)
//!                   │ yes
//!          full_refresh ── SELECT COUNT(*) ─────────────────── total_rows
//!          incremental ─── the INSERT's reported rows ──────── rows_added
//!                          (the count after - before is the fallback; the two
//!                          disagreeing is not_evaluated)
//!          merge ───────── the MERGE's reported row count ──── rows_touched
//!          time_interval ─ COUNT(*) per partition written ───── partition_rows
//!                   │ error / not reported ──────────▶ not_evaluated
//!                   │ count                                (never a failure, but
//!                   │                                      a `volume_warnings` line)
//!          in range ─────────────────▶ in_range
//!          out of range, warning ────▶ a `volume_warnings` line; delivery as usual
//!          out of range, error ──────▶ the model fails after the fact:
//!                                      downstream withheld as for any failure,
//!                                      run partial, no `due_at`
//! ```
//!
//! A time-interval output is judged once per written partition, each against
//! the whole range, and the check sits on that partition's own
//! materialization. One partition out of range at `error` fails the model;
//! only the breaching partitions are marked failed in the run record. A
//! partition run that itself failed still judges the partitions it wrote.
//!
//! What an `error` breach does:
//!
//! 1. It fails the model for the run that wrote the rows. The rows are not
//!    rolled back.
//! 2. It opens a breach marker for the model in the replicated state store
//!    ([`rocky_core::product::breach::VolumeBreachMarker`]), written right
//!    after the judgment. A time-interval marker lists the breached
//!    partition keys.
//! 3. While a marker is open, every run that builds the model fails it,
//!    even when that run's load is in range, so downstream stays withheld.
//!    The load still writes its rows. Only the verdict is failed.
//! 4. A rebuild clears the marker, once it is judged in range. For
//!    incremental, merge and full-refresh outputs that is a build that
//!    created or replaced the whole table (a first run, `--full-refresh`, a
//!    drift rebuild). For a time-interval output it is a rewrite of each
//!    breached partition (`--missing`, `--partition`, `--from`/`--to`). The
//!    marker goes when its last partition is clear.
//! 5. `rocky product clear-breach` deletes the marker by hand.
//! 6. `rocky product status` lists the open breaches.
//!
//! ```text
//!   error breach ──▶ marker opened, model fails
//!   marker open, in-range append ──▶ rows written, model still fails
//!   marker open, rebuild in range ──▶ marker deleted, model passes
//! ```
//!
//! A marker is written per model to the local store, right after its
//! judgment, so a crash later in the run does not lose it locally. Other
//! machines see it when the run uploads its state (under the run's
//! `on_upload_failure` policy). A failed write of a new marker fails the model
//! (closed). A failed delete only warns: the marker stays and the model
//! stays failed. A run with no state store, and a non-production run,
//! neither reads nor writes markers. A run that does not build a held
//! output (`rocky run --model <downstream>`) withholds the output's
//! descendants instead ([`held_outputs_not_built`]).
//!
//! A cancel does not skip the count: the check runs after the build
//! committed its rows, so a breach must still open its marker.
//!
//! Counting assumes this run is the table's only writer. A reported count
//! needs no such assumption, but the cross-check against the table's own
//! count does: another writer makes the two disagree and the verdict
//! `not_evaluated`.
//!
//! A build that created the table (a first run) or replaced it whole
//! (`--full-refresh`) counts every row in it as this load's, on any basis.

use std::path::Path;

use rocky_core::product::breach::VolumeBreachMarker;
use rocky_core::product::spec::{VolumeBasis, VolumeSeverity, VolumeVerdict};
use rocky_core::traits::{SqlDialect, WarehouseAdapter};
use rocky_ir::MaterializationStrategy;
use tracing::warn;

use super::product::ProductVolume;
use crate::output::{MaterializationOutput, RunOutput, VolumeCheckOutput};

/// The volume ranges this run checks, by output model.
#[derive(Debug, Default)]
pub(crate) struct ProductVolumes {
    ranges: Vec<ProductVolume>,
}

impl ProductVolumes {
    pub(crate) fn from_ranges(ranges: Vec<ProductVolume>) -> Self {
        Self { ranges }
    }

    fn for_model(&self, model: &str) -> Option<&ProductVolume> {
        self.ranges.iter().find(|range| range.output_model == model)
    }

    /// True when a product declares a range for `model`.
    pub(crate) fn declares(&self, model: &str) -> bool {
        self.for_model(model).is_some()
    }
}

/// The state store the volume check keeps breach markers in, and the run
/// that writes them. With no store a run reads and writes no marker.
#[derive(Clone, Copy)]
pub(crate) struct BreachLedger<'a> {
    pub(crate) store: Option<&'a rocky_core::state::StateStore>,
    pub(crate) run_id: &'a str,
}

/// What the marker ledger says about one model after this run's judgment.
#[derive(Debug, PartialEq, Eq)]
enum Standing {
    /// No marker holds the model.
    Clear,
    /// An open marker holds the model failed; the sentence says why.
    Held(String),
    /// A marker could not be read or written; the sentence says what. The
    /// model fails.
    StoreFailed(String),
}

/// Add `text` to a check's reason, keeping what it already says (a
/// `not_evaluated` check keeps why it was not counted).
fn append_reason(reason: &mut Option<String>, text: &str) {
    *reason = Some(match reason.take() {
        Some(own) if !own.is_empty() => format!("{own} {text}"),
        _ => text.to_string(),
    });
}

fn held_message(marker: &VolumeBreachMarker, partitions: &[String]) -> String {
    let (product, model) = (&marker.product, &marker.model);
    let clear = format!("`rocky product clear-breach {product} --model {model}` clears it by hand");
    let how = if partitions.is_empty() {
        format!("Rebuild the whole output with `rocky run --full-refresh`, or {clear}.")
    } else {
        format!(
            "Still breached: partition(s) {}. Rewrite them with `rocky run --missing` or \
             `rocky run --partition <key>`, or {clear}.",
            partitions.join(", ")
        )
    };
    let was = if marker.reason.is_empty() {
        String::new()
    } else {
        format!(" ({})", marker.reason)
    };
    format!(
        "model '{model}' is held failed by an open volume breach of product '{product}', \
         opened {}{was}. {how}",
        marker.opened_at.to_rfc3339()
    )
}

impl BreachLedger<'_> {
    fn read(&self, product: &str, model: &str) -> Result<Option<VolumeBreachMarker>, Standing> {
        let Some(store) = self.store else {
            return Ok(None);
        };
        store.volume_breach_get(product, model).map_err(|e| {
            Standing::StoreFailed(format!(
                "model '{model}' could not be checked for an open volume breach: the state \
                 store read failed ({e}); failing closed"
            ))
        })
    }

    fn newest(
        &self,
        range: &ProductVolume,
        model: &str,
        existing: Option<&VolumeBreachMarker>,
        check: &VolumeCheckOutput,
        reason: &str,
        partitions: Vec<String>,
    ) -> VolumeBreachMarker {
        VolumeBreachMarker {
            product: range.product.clone(),
            model: model.to_string(),
            opened_at: existing.map_or_else(chrono::Utc::now, |m| m.opened_at),
            run_id: existing
                .and_then(|m| m.run_id.clone())
                .or_else(|| Some(self.run_id.to_string())),
            partitions,
            rows: check.row_count,
            min_rows: check.min_rows,
            max_rows: check.max_rows,
            reason: reason.to_string(),
        }
    }

    fn open(&self, marker: &VolumeBreachMarker) -> Standing {
        let Some(store) = self.store else {
            return Standing::Clear;
        };
        match store.volume_breach_put(marker) {
            Ok(()) => Standing::Clear,
            Err(e) => Standing::StoreFailed(format!(
                "the breach could not be recorded in the state store ({e}), so the next run \
                 will not hold model '{}' failed",
                marker.model
            )),
        }
    }

    /// Settle the marker of a model that is judged as a whole: full refresh,
    /// incremental or merge. `breach` is the sentence of an `error` breach
    /// this load made. `rebuilt_in_range` is true when this build created or
    /// replaced the whole table and the count is in range.
    fn settle_whole(
        &self,
        range: &ProductVolume,
        model: &str,
        check: &VolumeCheckOutput,
        breach: Option<&str>,
        rebuilt_in_range: bool,
    ) -> Standing {
        let existing = match self.read(&range.product, model) {
            Ok(existing) => existing,
            Err(failed) => return failed,
        };
        if let Some(line) = breach {
            return self.open(&self.newest(range, model, existing.as_ref(), check, line, vec![]));
        }
        let Some(existing) = existing else {
            return Standing::Clear;
        };
        if rebuilt_in_range && let Some(store) = self.store {
            return match store.volume_breach_delete(&range.product, model) {
                Ok(_) => Standing::Clear,
                Err(e) => {
                    warn!(model, error = %e, "could not clear the volume breach marker");
                    Standing::Held(format!(
                        "{} The rebuild was in range, but the marker could not be cleared \
                         ({e}): it stays open and the model stays failed.",
                        held_message(&existing, &existing.partitions)
                    ))
                }
            };
        }
        Standing::Held(held_message(&existing, &existing.partitions))
    }

    /// Settle the marker of a `time_interval` model after its partitions were
    /// judged. `breaching` holds each partition key an `error` breach covered
    /// (with the newest breach's check and sentence), `in_range` each key
    /// this run rewrote and judged in range.
    fn settle_partitions(
        &self,
        range: &ProductVolume,
        model: &str,
        breaching: &[String],
        newest: Option<(&VolumeCheckOutput, &str)>,
        in_range: &[String],
    ) -> Standing {
        let existing = match self.read(&range.product, model) {
            Ok(existing) => existing,
            Err(failed) => return failed,
        };
        if let Some((check, line)) = newest {
            let mut partitions = existing
                .as_ref()
                .filter(|m| !m.is_whole_output())
                .map(|m| m.partitions.clone())
                .unwrap_or_default();
            if existing.as_ref().is_none_or(|m| !m.is_whole_output()) {
                partitions.retain(|key| !in_range.contains(key));
                partitions.extend(breaching.iter().cloned());
                partitions.sort();
                partitions.dedup();
            }
            return self.open(&self.newest(
                range,
                model,
                existing.as_ref(),
                check,
                line,
                partitions,
            ));
        }
        let Some(existing) = existing else {
            return Standing::Clear;
        };
        let Some(store) = self.store else {
            return Standing::Clear;
        };
        if existing.is_whole_output() {
            return Standing::Held(held_message(&existing, &[]));
        }
        let remaining: Vec<String> = existing
            .partitions
            .iter()
            .filter(|key| !in_range.contains(key))
            .cloned()
            .collect();
        if remaining.len() == existing.partitions.len() {
            return Standing::Held(held_message(&existing, &remaining));
        }
        let saved = if remaining.is_empty() {
            store
                .volume_breach_delete(&range.product, model)
                .map(|_| ())
        } else {
            store.volume_breach_put(&VolumeBreachMarker {
                partitions: remaining.clone(),
                ..existing.clone()
            })
        };
        match saved {
            Ok(()) if remaining.is_empty() => Standing::Clear,
            Ok(()) => Standing::Held(held_message(&existing, &remaining)),
            Err(e) => {
                warn!(model, error = %e, "could not update the volume breach marker");
                Standing::Held(format!(
                    "{} The rewrite was in range, but the marker could not be updated ({e}): \
                     it stays as it was and the model stays failed.",
                    held_message(&existing, &existing.partitions)
                ))
            }
        }
    }
}

/// True when an open breach marker (or a marker read that failed) holds
/// `model`. The skip gate builds such a model, so the volume check judges
/// it instead of an "unchanged" skip passing it.
pub(crate) fn held_by_marker(
    volumes: &ProductVolumes,
    ledger: &BreachLedger<'_>,
    model: &str,
) -> bool {
    volumes
        .for_model(model)
        .is_some_and(|range| !matches!(ledger.read(&range.product, model), Ok(None)))
}

/// The outputs an open breach holds failed that this run does not build,
/// by output model, each with the sentence why.
///
/// Read once at the start of the model phase. An output this run builds is
/// judged by [`check_volume`] or [`check_partition_volume`] instead, and its
/// failure withholds its descendants as any failure does. An output this
/// run does not build (`rocky run --model <downstream>`) would otherwise let
/// its descendants build over the breached rows, so the caller withholds
/// them. A marker that cannot be read holds the output (fail closed).
pub(crate) fn held_outputs_not_built(
    volumes: &ProductVolumes,
    ledger: &BreachLedger<'_>,
    builds: impl Fn(&str) -> bool,
) -> std::collections::BTreeMap<String, String> {
    let mut held = std::collections::BTreeMap::new();
    for range in &volumes.ranges {
        let model = range.output_model.as_str();
        if builds(model) {
            continue;
        }
        match ledger.read(&range.product, model) {
            Ok(None) => {}
            Ok(Some(marker)) => {
                held.insert(model.to_string(), held_message(&marker, &marker.partitions));
            }
            Err(Standing::StoreFailed(text) | Standing::Held(text)) => {
                held.insert(model.to_string(), text);
            }
            Err(Standing::Clear) => {}
        }
    }
    held
}

/// The output and source ranges this run checks, read in one scan of the
/// project's `products/`.
///
/// ```text
///   project root? ──no──▶ nothing checked (a backfill, tests)
///        │ yes
///   production? ──no──▶ nothing checked; a spec that does not load is a warning
///        │ yes
///   every spec loads? ──no──▶ Err: the model phase refuses before any write
///        │ yes
///   the declared ranges
/// ```
///
/// A shadow or branch run counts nothing, as it stamps no `due_at`.
pub(crate) fn load_ranges(
    root: Option<&Path>,
    production: bool,
) -> anyhow::Result<(ProductVolumes, super::run_source_volume::SourceVolumes)> {
    let Some(root) = root else {
        return Ok(Default::default());
    };
    if !production {
        for problem in super::product::product_spec_problems_in(root) {
            warn!("{problem}; this run checks no product range, but a production run refuses it");
        }
        return Ok(Default::default());
    }
    let ranges = super::product::product_ranges_in(root).map_err(|e| anyhow::anyhow!(e))?;
    Ok((
        ProductVolumes::from_ranges(ranges.output),
        super::run_source_volume::SourceVolumes::from_ranges(ranges.source),
    ))
}

/// The reason an output whose strategy is not counted is `not_evaluated`.
fn unsupported_reason() -> String {
    "[volume-strategy-unsupported] the output model's strategy (a view, ephemeral, \
     materialized view, dynamic table, delete_insert, snapshot or content-addressed \
     model) has no count that is one load's volume; not counted"
        .to_string()
}

/// Count and judge `model`'s output when a product declares a volume for it,
/// then settle its breach marker.
///
/// For a model built by the plain transformation path: full refresh,
/// incremental, merge (`time_interval` goes through
/// [`check_partition_volume`]). Records the check on
/// `materialization.volume` and, for a `warning` breach, a line in
/// `output.volume_warnings`. Returns the error the caller must treat as
/// this model's failure for an `error` breach, and `None` otherwise. A
/// count that cannot be taken is `not_evaluated`, never an error.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_volume(
    volumes: &ProductVolumes,
    ledger: &BreachLedger<'_>,
    warehouse: &dyn WarehouseAdapter,
    model: &str,
    strategy: &MaterializationStrategy,
    materialization: &mut MaterializationOutput,
    output: &mut RunOutput,
) -> Option<anyhow::Error> {
    let range = volumes.for_model(model)?;
    let basis = VolumeBasis::of(strategy);
    let counted = match basis {
        None => Err(unsupported_reason()),
        // A cancel does not skip the count: the build already committed its
        // rows, so a breach must still open its marker.
        Some(basis) => count_load(warehouse, basis, materialization).await,
    };
    let mut judged = judge(range, model, basis, None, counted);
    let rebuilt = materialization
        .write_evidence
        .as_ref()
        .is_some_and(|e| e.created_or_replaced);
    let breach = judged.failure.as_ref().map(|e| format!("{e:#}"));
    let standing = ledger.settle_whole(
        range,
        model,
        &judged.check,
        breach.as_deref(),
        rebuilt && judged.check.verdict == VolumeVerdict::InRange,
    );
    judged.apply_standing(standing);
    materialization.volume = Some(judged.check.clone());
    judged.settle(output)
}

/// Count and judge a `time_interval` model's output, one partition at a
/// time: `output.materializations[first..]` holds one entry per partition
/// this run wrote.
///
/// Each partition is counted with the predicate its overwrite used
/// ([`rocky_core::sql_gen::time_interval_partition_filter`]), in one
/// `COUNT(*)` of its own, and judged against the whole range. The check is
/// recorded on that partition's own materialization. A run that wrote no
/// partition records no check. Returns an error when any partition breaches
/// at `error`, naming each breaching partition; otherwise `None`. Only the
/// breaching partitions are marked failed in the run record
/// ([`crate::output::RunOutput::to_run_record`]) and, through
/// [`mark_breaching_partitions_failed`], in the partition state.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn check_partition_volume(
    volumes: &ProductVolumes,
    ledger: &BreachLedger<'_>,
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
    model: &rocky_core::models::Model,
    output: &mut RunOutput,
    first: usize,
) -> Option<anyhow::Error> {
    let model_name = model.config.name.as_str();
    let range = volumes.for_model(model_name)?;
    let strategy = model.to_model_ir().materialization;
    let basis = VolumeBasis::of(&strategy);
    let mut failures: Vec<String> = Vec::new();
    // The keys an `error` breach covered, the keys judged in range, and the
    // newest breach, for the marker.
    let mut breaching: Vec<String> = Vec::new();
    let mut in_range: Vec<String> = Vec::new();
    let mut newest: Option<(VolumeCheckOutput, String)> = None;
    for index in first..output.materializations.len() {
        let (asset_key, partition) = {
            let mat = &output.materializations[index];
            (mat.asset_key.clone(), mat.partition.clone())
        };
        let counted = match (&strategy, basis) {
            (_, None) => Err(unsupported_reason()),
            // Each entry here is a partition this run wrote. A cancel does not
            // skip its count: the rows are committed, so a breach must still
            // open its marker.
            (MaterializationStrategy::TimeInterval { time_column, .. }, Some(_)) => {
                match &partition {
                    None => Err("this partition recorded no time window; not counted".to_string()),
                    Some(partition) => {
                        let window = rocky_ir::PartitionWindow {
                            key: partition.key.clone(),
                            start: partition.start,
                            end: partition.end,
                        };
                        match rocky_core::sql_gen::time_interval_partition_filter(
                            time_column,
                            &window,
                            dialect,
                        ) {
                            Ok(filter) => count_rows(warehouse, &asset_key, Some(&filter)).await,
                            Err(e) => Err(format!("could not render the partition filter: {e}")),
                        }
                    }
                }
            }
            (_, Some(_)) => Err(
                "the model is not a time_interval model, so its partitions are not counted".into(),
            ),
        };
        let key = partition.as_ref().map(|partition| partition.key.as_str());
        let judged = judge(range, model_name, basis, key, counted);
        let covered = partition.iter().flat_map(|partition| {
            std::iter::once(partition.key.clone()).chain(partition.batched_with.iter().cloned())
        });
        if judged.check.fails_the_model() {
            breaching.extend(covered);
            let line = judged.failure.as_ref().map(|e| format!("{e:#}"));
            newest = line.map(|line| (judged.check.clone(), line));
        } else if judged.check.verdict == VolumeVerdict::InRange {
            in_range.extend(covered);
        }
        output.materializations[index].volume = Some(judged.check.clone());
        if let Some(failure) = judged.settle(output) {
            failures.push(format!("{failure:#}"));
        }
    }
    let standing = ledger.settle_partitions(
        range,
        model_name,
        &breaching,
        newest.as_ref().map(|(check, line)| (check, line.as_str())),
        &in_range,
    );
    let held_text = match &standing {
        Standing::Held(text) => {
            for mat in &mut output.materializations[first..] {
                if let Some(volume) = mat.volume.as_mut()
                    && !volume.fails_the_model()
                {
                    volume.held_by_open_breach = true;
                    append_reason(&mut volume.reason, text);
                }
            }
            Some(text.clone())
        }
        Standing::StoreFailed(text) if failures.is_empty() => {
            for mat in &mut output.materializations[first..] {
                if let Some(volume) = mat.volume.as_mut() {
                    volume.held_by_open_breach = true;
                    append_reason(&mut volume.reason, text);
                }
            }
            Some(text.clone())
        }
        Standing::StoreFailed(text) => {
            failures.push(text.clone());
            None
        }
        Standing::Clear => None,
    };
    match failures.len() {
        0 => held_text.map(|text| anyhow::anyhow!("{text}")),
        1 => failures.pop().map(|line| anyhow::anyhow!("{line}")),
        n => Some(anyhow::anyhow!(
            "{n} partitions of model '{model_name}' breach the volume range: {}",
            failures.join("; ")
        )),
    }
}

/// Record the partitions that breached at `error` as `Failed` in the
/// partition state, so `--missing` plans them again and judges them again.
///
/// `output.materializations[first..]` are the partitions this run wrote. A
/// partition that is in range, or that breached at `warning`, or whose count
/// was not taken keeps its `Computed` state. A batched partition marks every
/// key it covers. A state write that fails only warns: the verdict stands.
pub(crate) fn mark_breaching_partitions_failed(
    store: &rocky_core::state::StateStore,
    model: &str,
    output: &RunOutput,
    first: usize,
) {
    use rocky_core::incremental::PartitionStatus;
    let mut failed = Vec::new();
    for mat in &output.materializations[first..] {
        let (Some(check), Some(partition)) = (&mat.volume, &mat.partition) else {
            continue;
        };
        if !check.fails_the_model() {
            continue;
        }
        for key in std::iter::once(&partition.key).chain(&partition.batched_with) {
            match store.get_partition(model, key) {
                Ok(Some(mut record)) => {
                    record.status = PartitionStatus::Failed;
                    failed.push(record);
                }
                Ok(None) => {}
                Err(e) => warn!(
                    model,
                    partition = key.as_str(),
                    error = %e,
                    "could not read the partition state to mark a volume breach failed"
                ),
            }
        }
    }
    if failed.is_empty() {
        return;
    }
    if let Err(e) = store.record_partitions(&failed) {
        warn!(
            model,
            error = %e,
            "could not mark the breaching partitions failed; they stay computed and --missing \
             will not rewrite them"
        );
    }
}

/// The rows one build's write changed, from what each statement it ran
/// reported. Only a build that ran exactly one statement has a count, and
/// only when that statement reported: summing several would count a
/// statement that is not the write.
pub(crate) fn single_statement_rows(reported: &[Option<u64>]) -> Option<u64> {
    match reported {
        [only] => *only,
        _ => None,
    }
}

/// The table's growth over one write, as far as it could be read.
enum Growth {
    /// The count after the write minus the count before.
    Rows(u64),
    /// The table holds fewer rows after the write than before it.
    Shrank { before: u64, after: u64 },
    /// A count was not taken or failed; the reason.
    Unavailable(String),
}

/// The count for one plain build on `basis`, or why it was not taken.
///
/// - `rows_added`: the rows the adapter reported for the INSERT. The table's
///   growth (count after minus count before) is the fallback when nothing
///   was reported, and a cross-check when both exist: if they disagree,
///   something else wrote to the table and nothing is counted.
/// - `rows_touched`: the rows the adapter reported for the single MERGE.
async fn count_load(
    warehouse: &dyn WarehouseAdapter,
    basis: VolumeBasis,
    materialization: &MaterializationOutput,
) -> Result<u64, String> {
    let evidence = materialization.write_evidence.as_ref();
    let created_or_replaced = evidence.is_some_and(|e| e.created_or_replaced);
    match basis {
        VolumeBasis::TotalRows => count_rows(warehouse, &materialization.asset_key, None).await,
        // A table this build created or replaced holds only this build's rows.
        VolumeBasis::RowsAdded | VolumeBasis::RowsTouched if created_or_replaced => {
            count_rows(warehouse, &materialization.asset_key, None).await
        }
        VolumeBasis::RowsAdded => {
            let reported = evidence.and_then(|e| e.rows_reported);
            let growth = match evidence.and_then(|e| e.rows_before.as_ref()) {
                Some(Ok(before)) => {
                    match count_rows(warehouse, &materialization.asset_key, None).await {
                        Ok(after) => match after.checked_sub(*before) {
                            Some(rows) => Growth::Rows(rows),
                            None => Growth::Shrank {
                                before: *before,
                                after,
                            },
                        },
                        Err(reason) => Growth::Unavailable(reason),
                    }
                }
                Some(Err(reason)) => {
                    Growth::Unavailable(format!("the count before the write failed: {reason}"))
                }
                None => Growth::Unavailable("no row count was taken before the write".into()),
            };
            match (reported, growth) {
                (Some(reported), Growth::Rows(grew)) if reported != grew => Err(format!(
                    "the warehouse reported {reported} inserted row(s) but the table grew by \
                     {grew}, so the two disagree (another writer?); not counted"
                )),
                (Some(reported), Growth::Shrank { before, after }) => Err(format!(
                    "the warehouse reported {reported} inserted row(s) but the table holds \
                     fewer rows after the write ({after}) than before it ({before}), so the \
                     two disagree (another writer?); not counted"
                )),
                (Some(reported), _) => Ok(reported),
                (None, Growth::Rows(grew)) => Ok(grew),
                (None, Growth::Shrank { before, after }) => Err(format!(
                    "the table holds fewer rows after the write ({after}) than before it \
                     ({before}), so something else changed it during the run; not counted"
                )),
                (None, Growth::Unavailable(reason)) => Err(reason),
            }
        }
        VolumeBasis::RowsTouched => evidence.and_then(|e| e.rows_reported).ok_or_else(|| {
            "the warehouse adapter did not report how many rows the one MERGE inserted or \
             updated, and a row count cannot see an update; not counted"
                .to_string()
        }),
        VolumeBasis::PartitionRows => Err(
            "a time_interval output is counted over the partitions its run wrote, not per \
             build; not counted"
                .into(),
        ),
    }
}

/// One judged check, and what it does to the run.
struct Judged {
    check: VolumeCheckOutput,
    /// The `volume_warnings` line of a `warning` breach.
    warning: Option<String>,
    /// The model's failure for an `error` breach.
    failure: Option<anyhow::Error>,
}

impl Judged {
    /// Fold the marker ledger's standing into the judgment. A model held by
    /// an open marker fails even when this load was in range. A model that
    /// already failed on its own breach keeps that failure and gains the
    /// ledger's note.
    fn apply_standing(&mut self, standing: Standing) {
        let text = match standing {
            Standing::Clear => return,
            Standing::Held(text) | Standing::StoreFailed(text) => text,
        };
        match self.failure.take() {
            Some(own) => self.failure = Some(anyhow::anyhow!("{own:#}; {text}")),
            None => {
                self.check.held_by_open_breach = true;
                append_reason(&mut self.check.reason, &text);
                self.failure = Some(anyhow::anyhow!("{text}"));
            }
        }
    }

    /// Record the warning, if any, and hand back the failure.
    fn settle(self, output: &mut RunOutput) -> Option<anyhow::Error> {
        if let Some(line) = self.warning {
            output.volume_warnings.push(line);
        }
        self.failure
    }
}

/// Judge `counted` against `range`. A count not taken (`Err`) is
/// `not_evaluated`, with the error as its reason, and still earns a
/// `volume_warnings` line: it never fails the run, and it is never silent.
/// `partition` names the partition of a `time_interval` model.
fn judge(
    range: &ProductVolume,
    model: &str,
    basis: Option<VolumeBasis>,
    partition: Option<&str>,
    counted: Result<u64, String>,
) -> Judged {
    let (row_count, reason) = match counted {
        Ok(count) => (Some(count), None),
        Err(reason) => (None, Some(reason)),
    };
    let verdict = row_count.map_or(VolumeVerdict::NotEvaluated, |count| {
        range.volume.judge(count)
    });
    let severity = range.volume.severity();
    let mut check = VolumeCheckOutput {
        product: range.product.clone(),
        model: model.to_string(),
        basis,
        row_count,
        min_rows: range.volume.min_rows,
        max_rows: range.volume.max_rows,
        severity,
        verdict,
        reason,
        held_by_open_breach: false,
    };
    let (warning, failure) = match (
        describe_breach(&check, partition),
        describe_not_evaluated(&check, partition),
        severity,
    ) {
        (None, None, _) => (None, None),
        (None, Some(line), _) | (Some(line), _, VolumeSeverity::Warning) => {
            warn!(model, product = %check.product, "{line}");
            (Some(line), None)
        }
        (Some(line), _, VolumeSeverity::Error) => {
            check.reason = Some(
                if partition.is_some() {
                    "the partition's rows are written, but this partition and the model are \
                 marked failed: downstream models are withheld and the partition is not \
                 recorded as a delivery"
                } else {
                    "the table is written, but the model is marked failed: downstream models \
                 are withheld and the build is not recorded as a delivery"
                }
                .to_string(),
            );
            (None, Some(anyhow::anyhow!("{line}")))
        }
    };
    Judged {
        check,
        warning,
        failure,
    }
}

/// What names the model, and the partition when there is one.
fn subject(model: &str, partition: Option<&str>) -> String {
    match partition {
        Some(key) => format!("model '{model}', partition '{key}'"),
        None => format!("model '{model}'"),
    }
}

/// The sentence for a range that was not evaluated, or `None` when it was.
fn describe_not_evaluated(check: &VolumeCheckOutput, partition: Option<&str>) -> Option<String> {
    if check.verdict != VolumeVerdict::NotEvaluated {
        return None;
    }
    Some(format!(
        "{}: the volume range was not evaluated, so nothing was checked: {} \
         (product '{}', [product.output.volume] severity = \"{}\"; this never fails the run)",
        subject(&check.model, partition),
        check.reason.as_deref().unwrap_or("no reason recorded"),
        check.product,
        check.severity.as_str()
    ))
}

/// The sentence for an out-of-range count, or `None` when the count is in
/// range or was not taken.
fn describe_breach(check: &VolumeCheckOutput, partition: Option<&str>) -> Option<String> {
    let count = check.row_count?;
    let bound = match check.verdict {
        VolumeVerdict::BelowMin => format!("below min_rows = {}", check.min_rows?),
        VolumeVerdict::AboveMax => format!("above max_rows = {}", check.max_rows?),
        VolumeVerdict::InRange | VolumeVerdict::NotEvaluated => return None,
    };
    let subject = subject(&check.model, partition);
    let counted = match check.basis {
        Some(VolumeBasis::TotalRows) | None => format!("{subject} has {count} row(s)"),
        Some(VolumeBasis::RowsAdded) => {
            format!("{subject}: this run added {count} row(s)")
        }
        Some(VolumeBasis::RowsTouched) => {
            format!("{subject}: this run's MERGE inserted or updated {count} row(s)")
        }
        Some(VolumeBasis::PartitionRows) => {
            format!("{subject}: the partition this run wrote holds {count} row(s)")
        }
    };
    Some(format!(
        "{counted}, {bound} (product '{}', [product.output.volume] severity = \"{}\")",
        check.product,
        check.severity.as_str()
    ))
}

/// `SELECT COUNT(*)` on a written table, over the rows `filter` selects
/// when given. The error is the reason the verdict is `not_evaluated`.
pub(crate) async fn count_rows(
    warehouse: &dyn WarehouseAdapter,
    asset_key: &[String],
    filter: Option<&str>,
) -> Result<u64, String> {
    let [catalog, schema, table] = asset_key else {
        return Err(format!(
            "the output is not a three-part table ({}); not counted",
            asset_key.join(".")
        ));
    };
    let table_ref = warehouse
        .dialect()
        .format_table_ref(catalog, schema, table)
        .map_err(|e| format!("could not address the table: {e}"))?;
    let sql = match filter {
        Some(filter) => format!("SELECT COUNT(*) FROM {table_ref} WHERE {filter}"),
        None => format!("SELECT COUNT(*) FROM {table_ref}"),
    };
    let result = warehouse
        .execute_query(&sql)
        .await
        .map_err(|e| format!("the count failed: {e}"))?;
    rocky_core::checks::cell_as_u64(result.rows.first().and_then(|row| row.first()))
        .ok_or_else(|| "the count returned no readable number".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::product::spec::VolumeSpec;

    fn check(verdict: VolumeVerdict, severity: VolumeSeverity) -> VolumeCheckOutput {
        VolumeCheckOutput {
            product: "p".into(),
            model: "m".into(),
            basis: Some(VolumeBasis::TotalRows),
            row_count: Some(5),
            min_rows: Some(10),
            max_rows: Some(20),
            severity,
            verdict,
            reason: None,
            held_by_open_breach: false,
        }
    }

    #[test]
    fn only_an_out_of_range_error_fails_the_model() {
        assert!(check(VolumeVerdict::BelowMin, VolumeSeverity::Error).fails_the_model());
        assert!(check(VolumeVerdict::AboveMax, VolumeSeverity::Error).fails_the_model());
        assert!(!check(VolumeVerdict::BelowMin, VolumeSeverity::Warning).fails_the_model());
        assert!(!check(VolumeVerdict::InRange, VolumeSeverity::Error).fails_the_model());
        assert!(!check(VolumeVerdict::NotEvaluated, VolumeSeverity::Error).fails_the_model());
    }

    #[test]
    fn a_breach_names_the_bound_it_crossed() {
        let below = describe_breach(
            &check(VolumeVerdict::BelowMin, VolumeSeverity::Warning),
            None,
        )
        .expect("a breach");
        assert!(below.contains("5 row(s), below min_rows = 10"), "{below}");
        assert!(
            describe_breach(
                &check(VolumeVerdict::InRange, VolumeSeverity::Warning),
                None
            )
            .is_none()
        );
        let mut uncounted = check(VolumeVerdict::NotEvaluated, VolumeSeverity::Error);
        uncounted.row_count = None;
        assert!(describe_breach(&uncounted, None).is_none());
    }

    /// The breach sentence says what was counted.
    #[test]
    fn a_breach_names_what_was_counted() {
        let mut breach = check(VolumeVerdict::BelowMin, VolumeSeverity::Warning);
        for (basis, words) in [
            (VolumeBasis::TotalRows, "model 'm' has 5 row(s), below"),
            (VolumeBasis::RowsAdded, "this run added 5 row(s), below"),
            (
                VolumeBasis::RowsTouched,
                "this run's MERGE inserted or updated 5 row(s), below",
            ),
            (
                VolumeBasis::PartitionRows,
                "the partition this run wrote holds 5 row(s), below",
            ),
        ] {
            breach.basis = Some(basis);
            let line = describe_breach(&breach, None).expect("a breach");
            assert!(line.contains(words), "{basis:?}: {line}");
        }
    }

    /// Only a build that ran exactly one statement has a reported count.
    #[test]
    fn a_reported_count_comes_from_exactly_one_statement() {
        assert_eq!(single_statement_rows(&[Some(4)]), Some(4));
        assert_eq!(single_statement_rows(&[Some(0)]), Some(0));
        assert_eq!(single_statement_rows(&[None]), None);
        assert_eq!(single_statement_rows(&[]), None);
        assert_eq!(
            single_statement_rows(&[Some(1), Some(4)]),
            None,
            "two statements are not one MERGE"
        );
        assert_eq!(single_statement_rows(&[None, Some(4)]), None);
    }

    fn volume_range(severity: Option<VolumeSeverity>) -> ProductVolume {
        ProductVolume {
            product: "p".into(),
            output_model: "m".into(),
            volume: VolumeSpec {
                min_rows: Some(2),
                max_rows: Some(3),
                severity,
            },
        }
    }

    /// Every `not_evaluated` leaves a `volume_warnings` line with its reason,
    /// at either severity, and never fails the model.
    #[test]
    fn a_range_that_was_not_evaluated_warns_and_never_fails() {
        for severity in [None, Some(VolumeSeverity::Error)] {
            let judged = judge(
                &volume_range(severity),
                "m",
                Some(VolumeBasis::RowsTouched),
                None,
                Err("the adapter did not report".into()),
            );
            assert_eq!(judged.check.verdict, VolumeVerdict::NotEvaluated);
            assert!(judged.failure.is_none(), "not_evaluated never fails");
            let line = judged.warning.as_deref().expect("a warning line");
            assert!(line.contains("not evaluated"), "{line}");
            assert!(line.contains("the adapter did not report"), "{line}");
            assert!(line.contains("model 'm'"), "{line}");
            let mut output = RunOutput::new(String::new(), 1, 1);
            assert!(judged.settle(&mut output).is_none());
            assert_eq!(
                output.volume_warnings.len(),
                1,
                "{:?}",
                output.volume_warnings
            );
        }
        // An in-range count says nothing.
        let judged = judge(&volume_range(None), "m", None, None, Ok(2));
        assert!(judged.warning.is_none() && judged.failure.is_none());
    }

    /// A partition's line names the partition, and an `error` breach marks
    /// that partition failed.
    #[test]
    fn a_partition_breach_names_the_partition() {
        let judged = judge(
            &volume_range(Some(VolumeSeverity::Error)),
            "m",
            Some(VolumeBasis::PartitionRows),
            Some("2024-01-02"),
            Ok(9),
        );
        let failure = judged.failure.expect("an error breach fails").to_string();
        assert!(failure.contains("partition '2024-01-02'"), "{failure}");
        assert!(
            failure.contains("holds 9 row(s), above max_rows = 3"),
            "{failure}"
        );
        assert!(judged.check.fails_the_model());
    }

    /// `count_load` against a DuckDB table of 100 rows, for each basis and
    /// each way the evidence can fall short.
    #[cfg(feature = "duckdb")]
    #[tokio::test]
    async fn the_load_count_follows_the_basis_and_never_guesses() {
        use crate::output::WriteEvidence;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let warehouse = DuckDbWarehouseAdapter::in_memory().unwrap();
        warehouse
            .execute_statement("CREATE TABLE main.t AS SELECT range AS x FROM range(100)")
            .await
            .unwrap();
        let mat = |write_evidence: Option<WriteEvidence>| MaterializationOutput {
            volume: None,
            write_evidence,
            asset_key: vec!["memory".into(), "main".into(), "t".into()],
            notes: vec![],
            rows_copied: None,
            duration_ms: 1,
            started_at: chrono::Utc::now(),
            metadata: crate::output::MaterializationMetadata {
                strategy: "incremental".into(),
                watermark: None,
                target_table_full_name: None,
                sql_hash: None,
                column_count: None,
                compile_time_ms: None,
            },
            partition: None,
            cost_usd: None,
            bytes_scanned: None,
            bytes_written: None,
            egress_bytes: None,
            tenant: None,
            job_ids: vec![],
            attempts: Vec::new(),
            skip_internal: None,
            recipe_identity: None,
            output_column_hashes: None,
            consumed_column_baseline: None,
            output_version: None,
            stored_bytes: None,
            upstream_reads: None,
            output_target: None,
        };
        let appended = |before: Option<Result<u64, String>>| {
            mat(Some(WriteEvidence {
                created_or_replaced: false,
                rows_before: before,
                rows_reported: None,
            }))
        };
        let count = |basis, mat: MaterializationOutput| {
            let warehouse = &warehouse;
            async move { count_load(warehouse, basis, &mat).await }
        };

        // The key case: a total of 100 with 95 before the write is 5 added.
        assert_eq!(
            count(VolumeBasis::RowsAdded, appended(Some(Ok(95)))).await,
            Ok(5)
        );
        assert_eq!(
            count(VolumeBasis::TotalRows, appended(Some(Ok(95)))).await,
            Ok(100)
        );
        let no_before = count(VolumeBasis::RowsAdded, appended(None))
            .await
            .expect_err("no count before");
        assert!(no_before.contains("no row count was taken"), "{no_before}");
        let failed_before = count(VolumeBasis::RowsAdded, appended(Some(Err("boom".into()))))
            .await
            .expect_err("a failed count before");
        assert!(failed_before.contains("boom"), "{failed_before}");
        let shrank = count(VolumeBasis::RowsAdded, appended(Some(Ok(120))))
            .await
            .expect_err("fewer rows after than before");
        assert!(shrank.contains("fewer rows after"), "{shrank}");
        assert!(
            count(VolumeBasis::RowsAdded, mat(None)).await.is_err(),
            "no evidence at all is not a count"
        );

        // An INSERT's reported rows are the count. The table's growth is the
        // fallback, and a cross-check when both exist.
        let reported = |before: Option<Result<u64, String>>, rows: u64| {
            mat(Some(WriteEvidence {
                created_or_replaced: false,
                rows_before: before,
                rows_reported: Some(rows),
            }))
        };
        assert_eq!(
            count(VolumeBasis::RowsAdded, reported(Some(Ok(95)), 5)).await,
            Ok(5),
            "the report and the growth agree"
        );
        assert_eq!(
            count(VolumeBasis::RowsAdded, reported(None, 5)).await,
            Ok(5),
            "a report needs no count before the write"
        );
        assert_eq!(
            count(
                VolumeBasis::RowsAdded,
                reported(Some(Err("boom".into())), 5)
            )
            .await,
            Ok(5),
            "a failed count before leaves the report standing"
        );
        let grew_more = count(VolumeBasis::RowsAdded, reported(Some(Ok(90)), 5))
            .await
            .expect_err("reported 5, grew 10");
        assert!(
            grew_more.contains("reported 5") && grew_more.contains("grew by 10"),
            "{grew_more}"
        );
        assert!(grew_more.contains("disagree"), "{grew_more}");
        let shrank_reported = count(VolumeBasis::RowsAdded, reported(Some(Ok(120)), 5))
            .await
            .expect_err("reported 5, table shrank");
        assert!(shrank_reported.contains("disagree"), "{shrank_reported}");

        // A merge counts what the warehouse reported, never a total.
        let merged = |reported| {
            mat(Some(WriteEvidence {
                created_or_replaced: false,
                rows_before: None,
                rows_reported: reported,
            }))
        };
        assert_eq!(
            count(VolumeBasis::RowsTouched, merged(Some(7))).await,
            Ok(7)
        );
        let unreported = count(VolumeBasis::RowsTouched, merged(None))
            .await
            .expect_err("not reported");
        assert!(unreported.contains("did not report"), "{unreported}");

        // A build that created the table counts every row in it.
        let created = || {
            mat(Some(WriteEvidence {
                created_or_replaced: true,
                rows_before: None,
                rows_reported: Some(3),
            }))
        };
        assert_eq!(count(VolumeBasis::RowsAdded, created()).await, Ok(100));
        assert_eq!(count(VolumeBasis::RowsTouched, created()).await, Ok(100));
    }

    const SPEC_WITH_RANGES: &str = "[product]\nname = \"p\"\nintent = \"x\"\n\n\
         [product.source]\ntables = [\"c.s.t\"]\n\n\
         [[product.source.volume]]\ntable = \"c.s.t\"\nmin_rows = 1\n\n\
         [product.output]\ngrain = [\"x\"]\n\
         columns = [{ name = \"x\", type = \"Int32\", nullable = false }]\n\n\
         [product.trust]\nagent = \"propose_only\"\n";

    fn project_with(specs: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("products")).unwrap();
        for (name, body) in specs {
            std::fs::write(dir.path().join("products").join(name), body).unwrap();
        }
        dir
    }

    /// N26: a production run refuses when a spec does not parse. The spec
    /// could declare an `error` range, so skipping it would deliver an
    /// output nobody checked. The error names the file and the fix.
    #[test]
    fn a_production_run_refuses_a_spec_that_does_not_parse() {
        let dir = project_with(&[("p.toml", SPEC_WITH_RANGES), ("q.toml", "[product\n")]);
        let err = load_ranges(Some(dir.path()), true)
            .expect_err("a broken spec must refuse a production run")
            .to_string();
        assert!(err.starts_with("[product-spec-unloadable] "), "{err}");
        assert!(err.contains("products/q.toml does not load"), "{err}");
        assert!(err.contains("rocky product verify q"), "{err}");
        assert!(
            !err.contains("products/p.toml"),
            "the good spec is not named: {err}"
        );
    }

    /// A spec that cannot be read (here a directory named like one) refuses
    /// a production run too. A file is used, not a mode-000 file, because
    /// a test running as root reads through permissions.
    #[test]
    fn a_production_run_refuses_a_spec_that_cannot_be_read() {
        let dir = project_with(&[]);
        std::fs::create_dir_all(dir.path().join("products/r.toml")).unwrap();
        let err = load_ranges(Some(dir.path()), true)
            .expect_err("an unreadable spec must refuse a production run")
            .to_string();
        assert!(err.contains("products/r.toml does not load"), "{err}");
        assert!(err.contains("rocky product verify r"), "{err}");
    }

    /// A `products` that cannot be listed refuses, and points at
    /// `rocky validate`: there is no spec name to verify.
    #[test]
    fn a_production_run_refuses_a_products_dir_it_cannot_list() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("products"), "not a directory").unwrap();
        let err = load_ranges(Some(dir.path()), true)
            .expect_err("an unlistable products dir must refuse a production run")
            .to_string();
        assert!(err.contains("cannot read the products directory"), "{err}");
        assert!(err.contains("run again"), "{err}");
    }

    /// A dangling `products` symlink (an unmounted share) is not "no
    /// products": its `NotFound` proves nothing, so the run refuses.
    #[cfg(unix)]
    #[test]
    fn a_production_run_refuses_a_dangling_products_symlink() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::os::unix::fs::symlink(dir.path().join("gone"), dir.path().join("products")).unwrap();
        let err = load_ranges(Some(dir.path()), true)
            .expect_err("a dangling products link must refuse a production run")
            .to_string();
        assert!(err.contains("[product-spec-unloadable]"), "{err}");
        assert!(err.contains("cannot read the products directory"), "{err}");
    }

    /// The refusal is a configuration error, never classified as transient:
    /// an orchestrator must not retry it.
    #[test]
    fn the_refusal_is_not_classified_as_retryable() {
        let dir = project_with(&[("q.toml", "[product\n")]);
        let err = load_ranges(Some(dir.path()), true).expect_err("refuses");
        let (kind, cooldown) = crate::output::classify_anyhow_error_with_cooldown(&err);
        assert_eq!(kind, crate::output::FailureKind::Unknown);
        assert_eq!(cooldown, None);
    }

    /// No `products/` declares nothing: a project without products runs.
    #[test]
    fn a_production_run_without_products_checks_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (output, source) = load_ranges(Some(dir.path()), true).expect("no products dir");
        assert!(output.ranges.is_empty());
        assert!(format!("{source:?}").contains("ranges: []"));
    }

    /// A shadow or branch run with a broken spec still runs: it checks no
    /// range at all, so there is nothing to refuse. It warns instead.
    #[test]
    fn a_non_production_run_does_not_refuse_a_broken_spec() {
        let dir = project_with(&[("q.toml", "[product\n")]);
        load_ranges(Some(dir.path()), false).expect("a shadow run checks nothing");
        load_ranges(None, true).expect("no project root checks nothing");
    }

    /// Both range kinds come from the same scan.
    #[test]
    fn a_production_run_loads_both_range_kinds() {
        let dir = project_with(&[("p.toml", SPEC_WITH_RANGES)]);
        let (output, source) = load_ranges(Some(dir.path()), true).expect("loads");
        assert!(output.ranges.is_empty(), "p declares no output range");
        assert!(
            format!("{source:?}").contains("output_model: \"p\""),
            "{source:?}"
        );
    }

    #[test]
    fn a_non_production_run_loads_no_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            load_ranges(Some(dir.path()), false)
                .expect("no products")
                .0
                .ranges
                .is_empty()
        );
        let volumes = ProductVolumes::from_ranges(vec![ProductVolume {
            product: "p".into(),
            output_model: "m".into(),
            volume: VolumeSpec {
                min_rows: Some(1),
                max_rows: None,
                severity: None,
            },
        }]);
        assert!(volumes.for_model("m").is_some());
        assert!(volumes.for_model("other").is_none());
    }

    /// `rocky product verify` refuses a volume on an output that is not a
    /// full-refresh table, with the strategy read the way the compiler
    /// reads it (here from `_defaults.toml`, absent from the sidecar).
    #[test]
    fn verify_refuses_a_volume_on_a_non_full_refresh_output() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let write = |rel: &str, text: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write(
            "products/vol_product.toml",
            "[product]\nname = \"vol_product\"\nintent = \"x\"\n\n\
             [product.source]\ntables = [\"c.s.t\"]\n\n\
             [product.output]\nmodel = \"vol\"\ngrain = [\"x\"]\n\
             columns = [{ name = \"x\", type = \"Int32\", nullable = false }]\n\n\
             [product.output.volume]\nmin_rows = 1\n\n\
             [product.trust]\nagent = \"propose_only\"\n",
        );
        let parsed = super::super::product::load_spec(root, "vol_product").expect("spec");

        // Not drafted yet: nothing to check.
        super::super::product::check_volume_strategy(root, &parsed).expect("no model yet");

        write("models/vol.sql", "SELECT 1 AS x\n");
        write("models/vol.toml", "name = \"vol\"\nintent = \"x\"\n");
        write(
            "models/_defaults.toml",
            "[strategy]\ntype = \"view\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        );
        let reject = super::super::product::check_volume_strategy(root, &parsed)
            .expect_err("a view cannot carry a volume");
        assert_eq!(reject.code, "volume-strategy-unsupported");

        write(
            "models/_defaults.toml",
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        );
        super::super::product::check_volume_strategy(root, &parsed).expect("full refresh");

        // A `.rocky` DSL model is judged the same way as a `.sql` one.
        std::fs::remove_file(root.join("models/vol.sql")).unwrap();
        write(
            "models/vol.rocky",
            "from raw_orders\ngroup customer_id {\n    n: count()\n}\n",
        );
        write(
            "models/_defaults.toml",
            "[strategy]\ntype = \"view\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        );
        let reject = super::super::product::check_volume_strategy(root, &parsed)
            .expect_err("a .rocky view cannot carry a volume");
        assert_eq!(reject.code, "volume-strategy-unsupported");
        write(
            "models/_defaults.toml",
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\n",
        );
        super::super::product::check_volume_strategy(root, &parsed).expect("a .rocky full refresh");
    }

    /// A merge-basis range at `error` is refused on an adapter that reports
    /// no MERGE count, and only then.
    #[test]
    fn verify_refuses_an_error_range_on_a_merge_the_adapter_cannot_count() {
        use super::super::product::check_volume_merge_adapter;
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let write = |rel: &str, text: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        let spec = |volume: &str| {
            write(
                "products/vol_product.toml",
                &format!(
                    "[product]\nname = \"vol_product\"\nintent = \"x\"\n\n\
                     [product.source]\ntables = [\"c.s.t\"]\n\n\
                     [product.output]\nmodel = \"vol\"\ngrain = [\"x\"]\n\
                     columns = [{{ name = \"x\", type = \"Int32\", nullable = false }}]\n\n\
                     [product.output.volume]\n{volume}\n\n\
                     [product.trust]\nagent = \"propose_only\"\n"
                ),
            );
            super::super::product::load_spec(root, "vol_product").expect("spec")
        };
        let model = |sidecar: &str| {
            write("models/vol.sql", "SELECT 1 AS x\n");
            write(
                "models/vol.toml",
                &format!("name = \"vol\"\n{sidecar}\n[target]\ncatalog = \"c\"\nschema = \"s\"\n"),
            );
        };
        const MERGE: &str = "[strategy]\ntype = \"merge\"\nunique_key = [\"x\"]\n";
        let config = |adapters: &str, pipelines: &str| {
            write("rocky.toml", &format!("{adapters}\n{pipelines}"));
            rocky_core::config::parse_rocky_config(&root.join("rocky.toml")).expect("config")
        };
        let one_pipeline = |kind: &str| {
            config(
                &format!("[adapter.default]\ntype = \"{kind}\"\n"),
                "[pipeline.tx]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 [pipeline.tx.target]\nadapter = \"default\"\n",
            )
        };
        let error = "min_rows = 1\nseverity = \"error\"";

        // Not drafted: nothing to judge.
        let parsed = spec(error);
        assert_eq!(
            check_volume_merge_adapter(root, &one_pipeline("databricks"), &parsed).unwrap(),
            None
        );

        model(MERGE);
        for kind in ["databricks", "snowflake", "bigquery", "trino", "redshift"] {
            let reject =
                check_volume_merge_adapter(root, &one_pipeline(kind), &parsed).expect_err(kind);
            assert_eq!(reject.code, "volume-merge-count-unavailable", "{kind}");
            assert!(reject.message.contains(kind), "{}", reject.message);
        }
        for kind in ["duckdb", "postgres", "sqlserver", "clickhouse"] {
            assert_eq!(
                check_volume_merge_adapter(root, &one_pipeline(kind), &parsed).unwrap(),
                None,
                "{kind} reports MERGE counts"
            );
        }

        // At `warning` the range only informs: it passes.
        let warning = spec("min_rows = 1");
        assert_eq!(
            check_volume_merge_adapter(root, &one_pipeline("databricks"), &warning).unwrap(),
            None
        );

        // An incremental model with a unique_key MERGEs too; without one it
        // INSERTs, and the table's growth is a fallback count.
        model(
            "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\nunique_key = [\"x\"]\n",
        );
        assert!(check_volume_merge_adapter(root, &one_pipeline("snowflake"), &parsed).is_err());
        model("[strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\n");
        assert_eq!(
            check_volume_merge_adapter(root, &one_pipeline("snowflake"), &parsed).unwrap(),
            None
        );
        model("[strategy]\ntype = \"full_refresh\"\n");
        assert_eq!(
            check_volume_merge_adapter(root, &one_pipeline("snowflake"), &parsed).unwrap(),
            None
        );

        // The model's own `adapter` wins over its pipeline's.
        model(&format!("adapter = \"wh\"\n{MERGE}"));
        let overridden = config(
            "[adapter.default]\ntype = \"duckdb\"\n\n[adapter.wh]\ntype = \"databricks\"\n",
            "[pipeline.tx]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             [pipeline.tx.target]\nadapter = \"default\"\n",
        );
        assert!(check_volume_merge_adapter(root, &overridden, &parsed).is_err());

        // The pipeline whose `models` glob matches the file decides.
        model(MERGE);
        let two = |first: &str| {
            config(
                "[adapter.default]\ntype = \"duckdb\"\n\n[adapter.wh]\ntype = \"databricks\"\n",
                &format!(
                    "[pipeline.a]\ntype = \"transformation\"\nmodels = \"{first}\"\n\
                     [pipeline.a.target]\nadapter = \"wh\"\n\n\
                     [pipeline.b]\ntype = \"transformation\"\nmodels = \"elsewhere/**\"\n\
                     [pipeline.b.target]\nadapter = \"default\"\n"
                ),
            )
        };
        assert!(check_volume_merge_adapter(root, &two("models/**"), &parsed).is_err());

        // No pipeline matches and the pipelines disagree: the adapter cannot
        // be told, and verify says so instead of guessing.
        let warning_text = check_volume_merge_adapter(root, &two("nowhere/**"), &parsed)
            .expect("an unknown adapter is a warning, not a refusal")
            .expect("a warning");
        assert!(
            warning_text.contains("volume-merge-count-unknown-adapter"),
            "{warning_text}"
        );
    }

    /// `rocky product verify` accepts a volume on an incremental, merge or
    /// time-interval output and still refuses one `rocky run` cannot count.
    /// Phase B lowers a whole-table test only for a full-refresh output,
    /// and refuses when it cannot read the strategy.
    #[test]
    fn verify_accepts_the_counted_strategies_and_phase_b_lowers_by_basis() {
        use rocky_core::product::lowering::VolumeLowering;
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        let write = |rel: &str, text: &str| {
            let path = root.join(rel);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
        };
        write(
            "products/vol_product.toml",
            "[product]\nname = \"vol_product\"\nintent = \"x\"\n\n\
             [product.source]\ntables = [\"c.s.t\"]\n\n\
             [product.output]\nmodel = \"vol\"\ngrain = [\"x\"]\n\
             columns = [{ name = \"x\", type = \"Int32\", nullable = false }]\n\n\
             [product.output.volume]\nmin_rows = 1\n\n\
             [product.trust]\nagent = \"propose_only\"\n",
        );
        let parsed = super::super::product::load_spec(root, "vol_product").expect("spec");
        let lowering = || super::super::product::phase_b_volume_lowering(root, &parsed);

        let refused = lowering().expect_err("no model to read the strategy from");
        assert_eq!(refused.code, "volume-strategy-unresolved");

        write("models/vol.sql", "SELECT 1 AS x, CURRENT_DATE AS d\n");
        let cases = [
            (
                "[strategy]\ntype = \"full_refresh\"\n",
                Some(VolumeLowering::TotalRowsTest),
            ),
            (
                "[strategy]\ntype = \"incremental\"\ntimestamp_column = \"d\"\n",
                Some(VolumeLowering::RunOnly),
            ),
            (
                "[strategy]\ntype = \"merge\"\nunique_key = [\"x\"]\n",
                Some(VolumeLowering::RunOnly),
            ),
            (
                "[strategy]\ntype = \"time_interval\"\ntime_column = \"d\"\ngranularity = \"day\"\n",
                Some(VolumeLowering::RunOnly),
            ),
            (
                "[strategy]\ntype = \"delete_insert\"\npartition_by = [\"d\"]\n",
                None,
            ),
            ("[strategy]\ntype = \"view\"\n", None),
        ];
        for (strategy, lowers) in cases {
            write(
                "models/vol.toml",
                &format!(
                    "name = \"vol\"\nintent = \"x\"\n\n{strategy}\n\
                     [target]\ncatalog = \"c\"\nschema = \"s\"\n"
                ),
            );
            let verified = super::super::product::check_volume_strategy(root, &parsed);
            match lowers {
                Some(expected) => {
                    verified.unwrap_or_else(|e| panic!("{strategy}: {e}"));
                    assert_eq!(lowering().expect("lowers"), expected, "{strategy}");
                }
                None => {
                    let reject = verified.expect_err(strategy);
                    assert_eq!(reject.code, "volume-strategy-unsupported", "{strategy}");
                    assert!(
                        reject.message.contains("full_refresh, incremental, merge"),
                        "{reject}"
                    );
                }
            }
        }

        write("models/vol.toml", "name = [broken\n");
        let refused = lowering().expect_err("a model that does not load");
        assert_eq!(refused.code, "volume-strategy-unresolved");
    }

    /// `rocky run` end to end on DuckDB (no credentials): a product output
    /// with a volume range, counted after its production build.
    #[cfg(feature = "duckdb")]
    mod through_run {
        use std::collections::BTreeMap;

        use rocky_core::product::spec::{VolumeSeverity, VolumeVerdict};
        use rocky_core::state::{ModelExecution, RunRecord, RunStatus, StateStore};

        use super::super::super::run::{DeferOptions, PartitionRunOptions, SkipRunOptions, run};

        const FULL_REFRESH: &str =
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n";
        const VIEW: &str =
            "[strategy]\ntype = \"view\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n";
        /// Three rows.
        const VOL_SQL: &str = "SELECT x FROM (VALUES (1), (2), (3)) AS t(x)\n";

        /// The product spec for output model `vol`, with a daily deadline so
        /// a delivery is stamped (`due_at`) on a successful build.
        fn spec(volume: &str) -> String {
            format!(
                "[product]\nname = \"vol_product\"\nintent = \"three rows\"\n\
                 deliver_by = \"0 6 * * *\"\n\n\
                 [product.source]\ntables = [\"poc.raw.t\"]\n\n\
                 [product.output]\nmodel = \"vol\"\ngrain = [\"x\"]\n\
                 columns = [{{ name = \"x\", type = \"Int32\", nullable = false }}]\n\n\
                 [product.output.volume]\n{volume}\n\n\
                 [product.trust]\nagent = \"propose_only\"\n"
            )
        }

        struct Ran {
            result: anyhow::Result<()>,
            record: RunRecord,
            output: serde_json::Value,
            _tmp: tempfile::TempDir,
        }

        impl Ran {
            fn executions(&self, model: &str) -> Vec<&ModelExecution> {
                self.record
                    .models_executed
                    .iter()
                    .filter(|e| e.rocky_model_name() == model)
                    .collect()
            }

            fn volume(&self, table: &str) -> serde_json::Value {
                self.output["materializations"]
                    .as_array()
                    .expect("materializations")
                    .iter()
                    .find(|m| {
                        m["asset_key"].as_array().and_then(|k| k.last()) == Some(&table.into())
                    })
                    .map(|m| m["volume"].clone())
                    .unwrap_or(serde_json::Value::Null)
            }
        }

        async fn run_with(
            spec_text: &str,
            models: &[(&str, &str, &str)],
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
        ) -> Ran {
            run_with_config(spec_text, models, shadow, "").await
        }

        async fn run_with_config(
            spec_text: &str,
            models: &[(&str, &str, &str)],
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
            extra_config: &str,
        ) -> Ran {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(tmp.path(), spec_text, models, extra_config);
            let (result, record, output) =
                run_in(tmp.path(), shadow, &PartitionRunOptions::default()).await;
            Ran {
                result,
                record,
                output,
                _tmp: tmp,
            }
        }

        /// Write (or overwrite) the project: the models, the product spec
        /// and `rocky.toml`. A second call between runs changes the data a
        /// later run reads, on the same warehouse and state store.
        fn write_project(
            dir: &std::path::Path,
            spec_text: &str,
            models: &[(&str, &str, &str)],
            extra_config: &str,
        ) {
            let models_dir = dir.join("models");
            std::fs::create_dir_all(&models_dir).unwrap();
            for (name, sql, toml) in models {
                std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
                std::fs::write(models_dir.join(format!("{name}.toml")), toml).unwrap();
            }
            std::fs::create_dir_all(dir.join("products")).unwrap();
            std::fs::write(dir.join("products/vol_product.toml"), spec_text).unwrap();
            std::fs::write(
                dir.join("rocky.toml"),
                format!(
                    "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                     [state]\nbackend = \"local\"\n\n\
                     [pipeline.tx]\ntype = \"transformation\"\nmodels = '{}'\n\n\
                     [pipeline.tx.target]\nadapter = \"default\"\n\n{extra_config}",
                    dir.join("w.duckdb").display(),
                    models_dir.join("**").display(),
                ),
            )
            .unwrap();
        }

        /// `rocky run` on the project in `dir`: the result, the run's
        /// record and its JSON output.
        async fn run_in(
            dir: &std::path::Path,
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
            partitions: &PartitionRunOptions,
        ) -> (anyhow::Result<()>, RunRecord, serde_json::Value) {
            run_in_with(dir, shadow, partitions, &SkipRunOptions::default()).await
        }

        /// [`run_in`] with the run's skip options, for `--full-refresh`.
        async fn run_in_with(
            dir: &std::path::Path,
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
            partitions: &PartitionRunOptions,
            skip: &SkipRunOptions,
        ) -> (anyhow::Result<()>, RunRecord, serde_json::Value) {
            run_in_selecting(dir, shadow, partitions, skip, None).await
        }

        /// [`run_in_with`] with `--model <model>`.
        async fn run_in_selecting(
            dir: &std::path::Path,
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
            partitions: &PartitionRunOptions,
            skip: &SkipRunOptions,
            model: Option<&str>,
        ) -> (anyhow::Result<()>, RunRecord, serde_json::Value) {
            let config_path = dir.join("rocky.toml");
            let state_path = dir.join("state.redb");
            let run_id = format!("volume-{}", uuid::Uuid::new_v4());
            let loaded = std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            );
            let result = run(
                &config_path,
                loaded,
                None,
                None,
                &state_path,
                None,
                true,
                None,
                false,
                None,
                false,
                shadow,
                partitions,
                model,
                None,
                None,
                None,
                &DeferOptions::default(),
                skip,
                &rocky_core::run_vars::RunVars::new(),
                Some(&run_id),
                None,
                false,
                None,
                &rocky_core::config::PrincipalRef::unnamed(),
            )
            .await
            .map(|_| ());
            let output = super::super::super::run::CAPTURED_RUN_OUTPUT_FOR_TEST
                .lock()
                .unwrap()
                .remove(&run_id)
                .unwrap_or(serde_json::Value::Null);
            let state = StateStore::open(&state_path).unwrap();
            let record = state.list_runs(1).unwrap().remove(0);
            drop(state);
            (result, record, output)
        }

        fn one_model() -> Vec<(&'static str, &'static str, &'static str)> {
            vec![("vol", VOL_SQL, FULL_REFRESH)]
        }

        #[tokio::test]
        async fn an_in_range_count_is_recorded_and_delivers() {
            let ran = run_with(&spec("min_rows = 1\nmax_rows = 10"), &one_model(), None).await;
            ran.result.as_ref().expect("the run succeeds");
            assert_eq!(ran.record.status, RunStatus::Success);
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1, "{executions:?}");
            assert_eq!(executions[0].status, "success");
            assert_eq!(executions[0].row_count, Some(3));
            assert!(executions[0].due_at.is_some(), "a delivery is stamped");
            let volume = ran.volume("vol");
            assert_eq!(volume["verdict"], "in_range", "{volume}");
            assert_eq!(volume["row_count"], 3);
            assert_eq!(volume["severity"], "warning");
            assert!(
                ran.output.get("volume_warnings").is_none(),
                "{}",
                ran.output
            );
        }

        #[tokio::test]
        async fn a_count_below_min_warns_and_still_delivers() {
            let ran = run_with(&spec("min_rows = 4"), &one_model(), None).await;
            ran.result.as_ref().expect("a warning never fails the run");
            assert_eq!(ran.record.status, RunStatus::Success);
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].status, "success");
            assert_eq!(executions[0].row_count, Some(3));
            assert!(
                executions[0].due_at.is_some(),
                "delivery is recorded as usual"
            );
            assert_eq!(ran.volume("vol")["verdict"], "below_min");
            let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            let line = warnings[0].as_str().unwrap();
            assert!(line.contains("3 row(s), below min_rows = 4"), "{line}");
            assert!(line.contains("vol_product"), "{line}");
            assert!(
                ran.output["errors"].as_array().is_none_or(Vec::is_empty),
                "{}",
                ran.output
            );
        }

        #[tokio::test]
        async fn a_count_above_max_at_error_fails_the_model_after_its_write() {
            let models = vec![
                ("vol", VOL_SQL, FULL_REFRESH),
                (
                    "down",
                    "SELECT x FROM main.vol\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ];
            let ran = run_with(&spec("max_rows = 2\nseverity = \"error\""), &models, None).await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            let executions = ran.executions("vol");
            assert_eq!(
                executions.len(),
                1,
                "one execution, not two: {executions:?}"
            );
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].row_count, Some(3), "the count is kept");
            assert_eq!(
                executions[0].due_at, None,
                "a bad-volume build is no delivery"
            );
            assert!(
                ran.executions("down").iter().all(|e| e.status != "success"),
                "the downstream model is withheld: {:?}",
                ran.record.models_executed
            );
            let volume = ran.volume("vol");
            assert_eq!(volume["verdict"], "above_max", "{volume}");
            assert_eq!(volume["severity"], "error");
            let errors = ran.output["errors"].as_array().expect("errors");
            assert!(
                errors.iter().any(|e| e["error"]
                    .as_str()
                    .is_some_and(|m| m.contains("3 row(s), above max_rows = 2"))),
                "{errors:?}"
            );
            assert_eq!(ran.output["status"], "PartialFailure");
            assert!(
                ran.output["materializations"]
                    .as_array()
                    .expect("mats")
                    .iter()
                    .all(|m| m["asset_key"].as_array().and_then(|k| k.last())
                        != Some(&"down".into())),
                "the downstream model never built: {}",
                ran.output["materializations"]
            );
        }

        /// Under `[resilience] contain_failures`, the breach is contained
        /// like a failed build: the downstream model is withheld and the
        /// model still records exactly one (failed) execution.
        #[tokio::test]
        async fn a_contained_error_breach_withholds_downstream_with_one_execution() {
            let models = vec![
                ("vol", VOL_SQL, FULL_REFRESH),
                (
                    "down",
                    "SELECT x FROM main.vol\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ];
            let ran = run_with_config(
                &spec("max_rows = 2\nseverity = \"error\""),
                &models,
                None,
                "[resilience]\ncontain_failures = true\n",
            )
            .await;
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            let executions = ran.executions("vol");
            assert_eq!(
                executions.len(),
                1,
                "one execution, not two: {executions:?}"
            );
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].due_at, None);
            let contained = ran.output["contained"].as_array().expect("contained");
            assert!(
                contained.iter().any(|c| c["model"] == "down"),
                "the downstream model is withheld: {contained:?}"
            );
        }

        /// `vol` writes table `vol_tbl`, so the materialization (named by
        /// table) and the model differ; `down` reads `vol_tbl`.
        fn renamed_models() -> Vec<(&'static str, &'static str, &'static str)> {
            vec![
                (
                    "vol",
                    VOL_SQL,
                    "[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"vol_tbl\"\n",
                ),
                (
                    "down",
                    "SELECT x FROM main.vol_tbl\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ]
        }

        /// The one execution recorded for `vol` when its table is
        /// `vol_tbl`: exactly one, failed, with the count, and no `due_at`.
        fn assert_one_failed_renamed_execution(ran: &Ran) {
            let executions = ran.executions("vol");
            assert_eq!(
                executions.len(),
                1,
                "one execution, not two: {:?}",
                ran.record.models_executed
            );
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].model_name, "vol_tbl", "{executions:?}");
            assert_eq!(executions[0].row_count, Some(3));
            assert_eq!(
                executions[0].due_at, None,
                "a bad-volume build is no delivery"
            );
            assert_eq!(ran.volume("vol_tbl")["verdict"], "above_max");
            assert!(
                ran.executions("down").iter().all(|e| e.status != "success"),
                "{:?}",
                ran.record.models_executed
            );
        }

        /// Control for the two tests below: the same renamed table, in
        /// range, IS stamped as a delivery, so their `due_at = None` is the
        /// breach's doing and not the rename's.
        #[tokio::test]
        async fn a_renamed_output_in_range_delivers() {
            let ran = run_with(&spec("max_rows = 10"), &renamed_models(), None).await;
            ran.result.as_ref().expect("the run succeeds");
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1, "{executions:?}");
            assert_eq!(executions[0].status, "success");
            assert_eq!(executions[0].row_count, Some(3));
            assert!(executions[0].due_at.is_some(), "{executions:?}");
        }

        #[tokio::test]
        async fn a_renamed_output_error_breach_fails_once_on_fail_fast() {
            let ran = run_with(
                &spec("max_rows = 2\nseverity = \"error\""),
                &renamed_models(),
                None,
            )
            .await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            assert_one_failed_renamed_execution(&ran);
        }

        #[tokio::test]
        async fn a_renamed_output_error_breach_fails_once_when_contained() {
            let ran = run_with_config(
                &spec("max_rows = 2\nseverity = \"error\""),
                &renamed_models(),
                None,
                "[resilience]\ncontain_failures = true\n",
            )
            .await;
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            assert_one_failed_renamed_execution(&ran);
            // `rocky product status` finds the count under the model name.
            let root = ran._tmp.path();
            let status = super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status");
            let volume = status.volume.expect("a volume status");
            assert_eq!(volume.row_count, Some(3));
            assert_eq!(volume.verdict, VolumeVerdict::AboveMax);
        }

        #[tokio::test]
        async fn a_shadow_run_counts_nothing() {
            let shadow = rocky_core::shadow::ShadowConfig::default();
            let ran = run_with(
                &spec("max_rows = 2\nseverity = \"error\""),
                &one_model(),
                Some(&shadow),
            )
            .await;
            ran.result.as_ref().expect("a shadow run is not judged");
            let executions = ran.executions("vol_rocky_shadow");
            let all: BTreeMap<&str, Option<u64>> = ran
                .record
                .models_executed
                .iter()
                .map(|e| (e.model_name.as_str(), e.row_count))
                .collect();
            assert!(
                all.values().all(Option::is_none),
                "no count on a shadow run: {all:?}"
            );
            assert!(executions.iter().all(|e| e.status == "success"), "{all:?}");
            for mat in ran.output["materializations"].as_array().expect("mats") {
                assert!(mat.get("volume").is_none(), "{mat}");
            }
        }

        #[tokio::test]
        async fn a_non_full_refresh_output_is_not_evaluated() {
            let ran = run_with(
                &spec("max_rows = 2\nseverity = \"error\""),
                &[("vol", VOL_SQL, VIEW)],
                None,
            )
            .await;
            ran.result
                .as_ref()
                .expect("not_evaluated is never a failure");
            let volume = ran.volume("vol");
            assert_eq!(volume["verdict"], "not_evaluated", "{volume}");
            assert!(
                volume["reason"]
                    .as_str()
                    .is_some_and(|r| r.contains("volume-strategy-unsupported")),
                "{volume}"
            );
            assert!(volume.get("row_count").is_none());
            let executions = ran.executions("vol");
            assert_eq!(executions[0].status, "success");
            assert_eq!(executions[0].row_count, None);
            // Never a failure, but never silent either.
            let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            let line = warnings[0].as_str().unwrap();
            assert!(line.contains("not evaluated"), "{line}");
            assert!(line.contains("volume-strategy-unsupported"), "{line}");
            // The run record keeps the verdict and its reason.
            let [stored] = ran.record.output_volumes.as_slice() else {
                panic!("one stored check: {:?}", ran.record.output_volumes);
            };
            assert_eq!(stored.verdict, VolumeVerdict::NotEvaluated);
            assert_eq!(stored.row_count, None);
            assert!(
                stored
                    .reason
                    .as_deref()
                    .is_some_and(|r| r.contains("volume-strategy-unsupported")),
                "{stored:?}"
            );
        }

        /// `rocky product status` reads the latest count back with its
        /// verdict, including from a build an error breach marked failed.
        #[tokio::test]
        async fn product_status_shows_the_latest_count_and_verdict() {
            let ran = run_with(
                &spec("max_rows = 2\nseverity = \"error\""),
                &one_model(),
                None,
            )
            .await;
            let root = ran._tmp.path();
            let status = super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status");
            let volume = status.volume.expect("a volume status");
            assert_eq!(volume.row_count, Some(3));
            assert_eq!(volume.max_rows, Some(2));
            assert_eq!(volume.verdict, VolumeVerdict::AboveMax);
            assert_eq!(volume.severity, VolumeSeverity::Error);
            assert_eq!(volume.run_id.as_deref(), Some(ran.record.run_id.as_str()));
        }

        fn volume_status_of(
            root: &std::path::Path,
        ) -> super::super::super::product::ProductVolumeStatus {
            super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status")
            .volume
            .expect("a volume status")
        }

        /// An older count never stands in for a newer build that took none:
        /// `status` reports the newest production build's verdict.
        #[tokio::test]
        async fn product_status_reports_the_newest_build_not_an_older_count() {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(
                tmp.path(),
                &spec("min_rows = 1\nmax_rows = 10"),
                &one_model(),
                "",
            );
            let (first, first_record, _) =
                run_in(tmp.path(), None, &PartitionRunOptions::default()).await;
            first.expect("in range");
            let counted = volume_status_of(tmp.path());
            assert_eq!(counted.verdict, VolumeVerdict::InRange);
            assert_eq!(counted.row_count, Some(3));
            assert_eq!(
                counted.basis,
                Some(rocky_core::product::spec::VolumeBasis::TotalRows)
            );

            // A newer production build of the output that took no count.
            let hour = chrono::Duration::hours(1);
            let mut newer = first_record.clone();
            newer.run_id = "newer-build".into();
            newer.started_at += hour;
            newer.finished_at += hour;
            for execution in &mut newer.models_executed {
                execution.started_at += hour;
                execution.finished_at += hour;
                execution.row_count = None;
            }
            newer.output_volumes = vec![rocky_core::state::OutputVolumeRecord {
                product: "vol_product".into(),
                model: "vol".into(),
                partition: None,
                basis: Some(rocky_core::product::spec::VolumeBasis::RowsTouched),
                row_count: None,
                min_rows: Some(1),
                max_rows: Some(10),
                severity: VolumeSeverity::Warning,
                verdict: VolumeVerdict::NotEvaluated,
                reason: Some("the adapter did not report".into()),
            }];
            StateStore::open(&tmp.path().join("state.redb"))
                .unwrap()
                .record_run(&newer)
                .unwrap();
            let status = volume_status_of(tmp.path());
            assert_eq!(status.verdict, VolumeVerdict::NotEvaluated, "{status:?}");
            assert_eq!(status.row_count, None, "the older count is not shown");
            assert_eq!(status.run_id.as_deref(), Some("newer-build"));
            assert_eq!(status.reason.as_deref(), Some("the adapter did not report"));
        }

        /// A stored count is judged only on the basis it was taken on. Once
        /// the output model counts on another basis, the stored count says
        /// nothing about the range.
        #[tokio::test]
        async fn product_status_does_not_judge_a_count_on_another_basis() {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(tmp.path(), &spec("max_rows = 2"), &one_model(), "");
            let (first, _, _) = run_in(tmp.path(), None, &PartitionRunOptions::default()).await;
            first.expect("a warning never fails the run");
            let counted = volume_status_of(tmp.path());
            assert_eq!(counted.verdict, VolumeVerdict::AboveMax, "{counted:?}");

            // The model is now a merge: its range counts MERGE rows.
            std::fs::write(
                tmp.path().join("models/vol.toml"),
                "[strategy]\ntype = \"merge\"\nunique_key = [\"x\"]\n\n\
                 [target]\ncatalog = \"\"\nschema = \"main\"\n",
            )
            .unwrap();
            let status = volume_status_of(tmp.path());
            assert_eq!(status.verdict, VolumeVerdict::NotEvaluated, "{status:?}");
            assert_eq!(status.row_count, None);
            assert!(
                status
                    .reason
                    .as_deref()
                    .is_some_and(|r| r.contains("basis")),
                "{status:?}"
            );
        }

        /// A record written before checks stored a basis holds a total-rows
        /// count: `status` still reads it, as total rows.
        #[tokio::test]
        async fn product_status_reads_a_record_from_before_the_basis_was_stored() {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(tmp.path(), &spec("max_rows = 2"), &one_model(), "");
            let (first, mut record, _) =
                run_in(tmp.path(), None, &PartitionRunOptions::default()).await;
            first.expect("a warning never fails the run");
            record.output_volumes.clear();
            StateStore::open(&tmp.path().join("state.redb"))
                .unwrap()
                .record_run(&record)
                .unwrap();
            let status = volume_status_of(tmp.path());
            assert_eq!(status.row_count, Some(3), "{status:?}");
            assert_eq!(
                status.basis,
                Some(rocky_core::product::spec::VolumeBasis::TotalRows)
            );
            assert_eq!(status.verdict, VolumeVerdict::AboveMax);
        }

        /// The intra-layer concurrent path (`--parallel`) applies the same
        /// rule: `vol` and `side` share layer 0, `down` reads `vol` in
        /// layer 1. An error breach on `vol` is the layer's failure.
        async fn run_concurrent(
            contain_failures: bool,
        ) -> (
            crate::output::RunOutput,
            anyhow::Result<super::super::super::run::GovernanceSnapshot>,
            tempfile::TempDir,
        ) {
            let tmp = tempfile::TempDir::new().unwrap();
            let (output, result) = run_layers(
                tmp.path(),
                4,
                contain_failures,
                "max_rows = 2\nseverity = \"error\"",
                None,
            )
            .await;
            (output, result, tmp)
        }

        /// `execute_models` over `vol`, `side` (layer 0) and `down` (reads
        /// `vol`, layer 1), at `parallel` (1 is the serial path), with an
        /// optional hook registry fired under pipeline `tx`.
        async fn run_layers(
            root: &std::path::Path,
            parallel: usize,
            contain_failures: bool,
            volume: &str,
            hooks: Option<&rocky_core::hooks::HookRegistry>,
        ) -> (
            crate::output::RunOutput,
            anyhow::Result<super::super::super::run::GovernanceSnapshot>,
        ) {
            use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

            let models_dir = root.join("models");
            std::fs::create_dir_all(&models_dir).unwrap();
            for (name, sql, toml) in [
                ("vol", VOL_SQL, FULL_REFRESH),
                ("side", "SELECT 7 AS y\n", FULL_REFRESH),
                (
                    "down",
                    "SELECT x FROM main.vol\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ] {
                std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
                std::fs::write(models_dir.join(format!("{name}.toml")), toml).unwrap();
            }
            std::fs::create_dir_all(root.join("products")).unwrap();
            std::fs::write(root.join("products/vol_product.toml"), spec(volume)).unwrap();
            let adapter = DuckDbWarehouseAdapter::open(&root.join("w.duckdb"))
                .expect("open duckdb")
                .with_concurrent_for_test(true);
            let opts = PartitionRunOptions {
                parallel: u32::try_from(parallel).expect("small"),
                ..Default::default()
            };
            let mut output = crate::output::RunOutput::new(String::new(), 0, parallel);
            let resilience = rocky_core::config::ResilienceConfig {
                contain_failures,
                ..Default::default()
            };
            let defer = DeferOptions {
                project: Some(rocky_compiler::compile::ProjectContext {
                    root: root.to_path_buf(),
                    model_names: Default::default(),
                }),
                ..DeferOptions::default()
            };
            let result = super::super::super::run::execute_models(
                &models_dir,
                None,
                &adapter as &dyn rocky_core::traits::WarehouseAdapter,
                None,
                &opts,
                "volume-concurrent",
                None,
                None,
                &mut output,
                hooks,
                hooks.map(|_| "tx"),
                &rocky_core::config::SchemaCacheConfig::default(),
                false,
                None,
                &defer,
                super::super::super::run::SkipGateConfig::off(),
                false,
                false,
                &rocky_core::run_vars::RunVars::new(),
                resilience,
                false,
                true,
                None,
                None,
                false,
                false,
            )
            .await;
            (output, result)
        }

        fn built(output: &crate::output::RunOutput, table: &str) -> bool {
            output
                .materializations
                .iter()
                .any(|m| m.asset_key.last().map(String::as_str) == Some(table))
        }

        #[tokio::test]
        async fn the_concurrent_path_fails_the_layer_on_an_error_breach() {
            let (output, result, _tmp) = run_concurrent(false).await;
            let error = result.expect_err("the breach is the layer's first error");
            assert!(
                format!("{error:#}").contains("3 row(s), above max_rows = 2"),
                "{error:#}"
            );
            assert!(built(&output, "vol") && built(&output, "side"));
            assert!(!built(&output, "down"), "no later layer starts");
            let volume = output
                .materializations
                .iter()
                .find_map(|m| m.volume.as_ref())
                .expect("a volume check");
            assert!(volume.fails_the_model(), "{volume:?}");
        }

        /// #2470: a fail-fast error breach records the model once, as its
        /// failed materialization with that materialization's time. The run's
        /// one error entry (`<runtime>` here) stands for the same model, so
        /// it adds no second, zero-time record under the placeholder. Serial
        /// and concurrent paths.
        #[tokio::test]
        async fn a_fail_fast_breach_is_one_record_with_its_time() {
            for parallel in [1, 4] {
                let tmp = tempfile::TempDir::new().unwrap();
                let (mut output, result) = run_layers(
                    tmp.path(),
                    parallel,
                    false,
                    "max_rows = 2\nseverity = \"error\"",
                    None,
                )
                .await;
                assert!(result.is_err(), "parallel {parallel}");
                // Plant a distinctive time so a real sub-millisecond zero
                // cannot satisfy the check.
                output
                    .materializations
                    .iter_mut()
                    .find(|m| m.asset_key.last().map(String::as_str) == Some("vol"))
                    .expect("vol built")
                    .duration_ms = 1234;
                // What the entry point does with the `Err`.
                output.tables_failed += 1;
                output.push_fail_fast_error(crate::output::TableErrorOutput {
                    asset_key: vec!["<runtime>".to_string()],
                    error: "breach".to_string(),
                    failure_kind: crate::output::FailureKind::Unknown,
                    cooldown_seconds: None,
                });
                let started = chrono::Utc::now();
                let record = output.to_run_record(
                    "run-2470",
                    started,
                    started,
                    String::new(),
                    rocky_core::state::RunTrigger::Manual,
                    output.derive_run_status(),
                    crate::output::RunRecordAudit::test_sentinels(),
                );
                let failed: Vec<_> = record
                    .models_executed
                    .iter()
                    .filter(|m| m.status == "failed")
                    .collect();
                assert_eq!(failed.len(), 1, "parallel {parallel}: {failed:?}");
                assert_eq!(failed[0].model_name, "vol", "parallel {parallel}");
                assert_eq!(failed[0].duration_ms, 1234, "parallel {parallel}");
            }
        }

        #[tokio::test]
        async fn the_concurrent_path_contains_an_error_breach() {
            let (output, result, _tmp) = run_concurrent(true).await;
            result.expect("a contained failure returns Ok");
            assert_eq!(output.tables_failed, 1);
            assert!(
                output.errors.iter().any(|e| e.asset_key == ["vol"]),
                "{:?}",
                output.errors
            );
            assert!(output.contained.iter().any(|c| c.model == "down"));
            assert!(built(&output, "side"), "a disjoint model still builds");
            assert!(!built(&output, "down"));
        }

        /// The models each hook event fired for, read from the JSON
        /// context the command hooks appended (one JSON value per fire).
        struct Fired {
            after_model_run: Vec<String>,
            model_error: Vec<String>,
        }

        /// Run the layers with command hooks on `on_after_model_run` and
        /// `on_model_error` that append their stdin context to a file. A
        /// command hook is the only way to observe a fire from here; an
        /// empty registry would prove nothing.
        async fn run_with_hooks(parallel: usize, volume: &str) -> Fired {
            let tmp = tempfile::TempDir::new().unwrap();
            let root = tmp.path();
            let mut hooks_cfg = rocky_core::hooks::HooksConfig::default();
            for event in ["on_after_model_run", "on_model_error"] {
                let log = root.join(format!("{event}.jsonl"));
                hooks_cfg.hooks.insert(
                    event.to_string(),
                    rocky_core::hooks::HookConfigOrList::Single(rocky_core::hooks::HookConfig {
                        command: format!("cat >> '{0}'; echo >> '{0}'", log.display()),
                        timeout_ms: 5_000,
                        on_failure: Default::default(),
                        env: Default::default(),
                    }),
                );
            }
            let registry = rocky_core::hooks::HookRegistry::from_config(&hooks_cfg);
            let (_output, _result) =
                run_layers(root, parallel, true, volume, Some(&registry)).await;
            let models = |event: &str| -> Vec<String> {
                let text = std::fs::read_to_string(root.join(format!("{event}.jsonl")))
                    .unwrap_or_default();
                serde_json::Deserializer::from_str(&text)
                    .into_iter::<serde_json::Value>()
                    .map(|ctx| {
                        ctx.expect("hook context JSON")["model"]
                            .as_str()
                            .unwrap_or_default()
                            .to_string()
                    })
                    .collect()
            };
            Fired {
                after_model_run: models("on_after_model_run"),
                model_error: models("on_model_error"),
            }
        }

        /// An error breach fires `model_error` and NOT the success hook, on
        /// the serial path (`parallel = 1`) and the concurrent one. The
        /// disjoint `side` model proves the success hook is wired.
        #[tokio::test]
        async fn an_error_breach_fires_only_model_error() {
            for parallel in [1, 4] {
                let fired = run_with_hooks(parallel, "max_rows = 2\nseverity = \"error\"").await;
                assert!(
                    fired.after_model_run.iter().any(|m| m == "side"),
                    "parallel {parallel}: the success hook is wired: {:?}",
                    fired.after_model_run
                );
                assert!(
                    !fired.after_model_run.iter().any(|m| m == "vol"),
                    "parallel {parallel}: a breached model is no success: {:?}",
                    fired.after_model_run
                );
                assert_eq!(
                    fired.model_error,
                    vec!["vol".to_string()],
                    "parallel {parallel}: model_error fires once, for vol"
                );
            }
        }

        /// A warning breach and an in-range count fire the success hook as
        /// before, and no `model_error`.
        #[tokio::test]
        async fn a_warning_breach_or_in_range_fires_the_success_hook() {
            for (parallel, volume) in [
                (1, "max_rows = 2"),
                (4, "max_rows = 2"),
                (1, "max_rows = 10\nseverity = \"error\""),
                (4, "max_rows = 10\nseverity = \"error\""),
            ] {
                let fired = run_with_hooks(parallel, volume).await;
                assert!(
                    fired.after_model_run.iter().any(|m| m == "vol"),
                    "parallel {parallel}, {volume:?}: {:?}",
                    fired.after_model_run
                );
                assert!(
                    fired.model_error.is_empty(),
                    "parallel {parallel}, {volume:?}: {:?}",
                    fired.model_error
                );
            }
        }

        // ----- incremental, merge and time-interval outputs (T-6) -----

        /// `src`: `n` rows with strictly increasing timestamps, so an
        /// incremental run past the target's `MAX(ts)` loads exactly the new
        /// ones.
        fn src_rows(n: u32) -> String {
            format!(
                "SELECT range AS x, TIMESTAMP '2024-01-01 00:00:00' + to_minutes(range) AS ts \
                 FROM range({n})\n"
            )
        }

        const SRC_TOML: &str =
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n";
        const INCREMENTAL_TOML: &str = "depends_on = [\"src\"]\n\n\
             [strategy]\ntype = \"incremental\"\ntimestamp_column = \"ts\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\n";
        const INCREMENTAL_SQL: &str = "SELECT x, ts FROM main.src WHERE @incremental_filter\n";
        const DOWN_TOML: &str = "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\n";

        fn incremental_models(src: &str) -> Vec<(&'static str, String, &'static str)> {
            vec![
                ("src", src.to_string(), SRC_TOML),
                ("vol", INCREMENTAL_SQL.to_string(), INCREMENTAL_TOML),
                ("down", "SELECT x FROM main.vol\n".to_string(), DOWN_TOML),
            ]
        }

        fn borrowed<'a>(
            models: &'a [(&'static str, String, &'static str)],
        ) -> Vec<(&'a str, &'a str, &'a str)> {
            models
                .iter()
                .map(|(name, sql, toml)| (*name, sql.as_str(), *toml))
                .collect()
        }

        /// Ran twice on one project: the first run with `first` rows and an
        /// always-in-range spec, the second with `second` rows and `volume`.
        async fn two_runs(
            models: impl Fn(u32) -> Vec<(&'static str, String, &'static str)>,
            first: u32,
            second: u32,
            volume: &str,
            partitions: [&PartitionRunOptions; 2],
        ) -> (Ran, serde_json::Value) {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(
                tmp.path(),
                &spec("min_rows = 0"),
                &borrowed(&models(first)),
                "",
            );
            let (first_result, _, first_output) = run_in(tmp.path(), None, partitions[0]).await;
            first_result.expect("the first run succeeds");
            write_project(tmp.path(), &spec(volume), &borrowed(&models(second)), "");
            let (result, record, output) = run_in(tmp.path(), None, partitions[1]).await;
            (
                Ran {
                    result,
                    record,
                    output,
                    _tmp: tmp,
                },
                first_output,
            )
        }

        fn table_total(ran: &Ran, table: &str) -> u64 {
            let db = rocky_duckdb::DuckDbConnector::open(&ran._tmp.path().join("w.duckdb"))
                .expect("open the warehouse");
            let rows = db
                .execute_sql(&format!("SELECT COUNT(*) FROM main.{table}"))
                .expect("count")
                .rows;
            rocky_core::checks::cell_as_u64(rows[0].first()).expect("a count")
        }

        /// The key case: the table holds 100 rows, of which this run added
        /// 5. The range judges 5, not 100. The first run created the table,
        /// so all its 95 rows were that run's.
        #[tokio::test]
        async fn an_incremental_output_judges_only_the_rows_this_run_added() {
            let (ran, first) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                "min_rows = 1\nmax_rows = 10",
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            let first_volume = first["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .map(|m| m["volume"].clone())
                .unwrap();
            assert_eq!(first_volume["basis"], "rows_added", "{first_volume}");
            assert_eq!(
                first_volume["row_count"], 95,
                "the first run created the table"
            );

            ran.result.as_ref().expect("in range");
            assert_eq!(
                table_total(&ran, "vol"),
                100,
                "the table total is not judged"
            );
            let volume = ran.volume("vol");
            assert_eq!(volume["basis"], "rows_added", "{volume}");
            assert_eq!(volume["row_count"], 5, "{volume}");
            assert_eq!(volume["verdict"], "in_range", "{volume}");
            let executions = ran.executions("vol");
            assert_eq!(executions[0].status, "success");
            assert_eq!(executions[0].row_count, Some(5));
            assert!(executions[0].due_at.is_some(), "a delivery is stamped");
        }

        #[tokio::test]
        async fn an_incremental_output_below_min_warns_on_the_added_rows() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                "min_rows = 6",
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            ran.result.as_ref().expect("a warning never fails the run");
            assert_eq!(ran.volume("vol")["verdict"], "below_min");
            let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
            let line = warnings[0].as_str().unwrap();
            assert!(
                line.contains("this run added 5 row(s), below min_rows = 6"),
                "{line}"
            );
        }

        /// Above max at `error`: the model is failed after its write, the
        /// downstream model is withheld, the run is partial and no delivery
        /// is stamped.
        #[tokio::test]
        async fn an_incremental_output_above_max_at_error_fails_the_model() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                "max_rows = 4\nseverity = \"error\"",
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1, "{executions:?}");
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].row_count, Some(5));
            assert_eq!(executions[0].due_at, None);
            assert_eq!(table_total(&ran, "vol"), 100, "the rows are written");
            let volume = ran.volume("vol");
            assert_eq!(volume["verdict"], "above_max", "{volume}");
            assert!(
                ran.executions("down").iter().all(|e| e.status != "success"),
                "the downstream model is withheld: {:?}",
                ran.record.models_executed
            );
        }

        /// `src` for a merge: keys `from..to` with a value that changes on
        /// every run (`v = x * 10 + run`).
        fn keyed_src(from: u32, to: u32, run: u32) -> String {
            format!("SELECT range AS x, range * 10 + {run} AS v FROM range({from}, {to})\n")
        }

        fn merge_models(src: String) -> Vec<(&'static str, String, &'static str)> {
            vec![
                ("src", src, SRC_TOML),
                (
                    "vol",
                    "SELECT x, v FROM main.src\n".to_string(),
                    "depends_on = [\"src\"]\n\n\
                     [strategy]\ntype = \"merge\"\nunique_key = [\"x\"]\nupdate_columns = [\"v\"]\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ]
        }

        /// A merge counts what its MERGE touched: the second run updates
        /// keys 2 and 3 and inserts 4 and 5, so 4, though the table grows
        /// by only 2 and holds 6.
        #[tokio::test]
        async fn a_merge_output_judges_the_rows_its_merge_touched() {
            let (ran, first) = two_runs(
                |run| {
                    if run == 1 {
                        merge_models(keyed_src(0, 4, 1))
                    } else {
                        merge_models(keyed_src(2, 6, 2))
                    }
                },
                1,
                2,
                "min_rows = 3\nmax_rows = 4\nseverity = \"error\"",
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            let first_volume = first["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .find(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .map(|m| m["volume"].clone())
                .unwrap();
            assert_eq!(first_volume["basis"], "rows_touched", "{first_volume}");
            assert_eq!(
                first_volume["row_count"], 4,
                "the first run created the table"
            );

            ran.result.as_ref().expect("4 touched is in range");
            assert_eq!(table_total(&ran, "vol"), 6);
            let volume = ran.volume("vol");
            assert_eq!(volume["basis"], "rows_touched", "{volume}");
            assert_eq!(volume["row_count"], 4, "{volume}");
            assert_eq!(volume["verdict"], "in_range");
        }

        #[tokio::test]
        async fn a_merge_output_out_of_range_names_the_touched_rows() {
            for (volume, verdict, words) in [
                (
                    "min_rows = 5",
                    "below_min",
                    "MERGE inserted or updated 4 row(s), below min_rows = 5",
                ),
                (
                    "max_rows = 3",
                    "above_max",
                    "MERGE inserted or updated 4 row(s), above max_rows = 3",
                ),
            ] {
                let (ran, _) = two_runs(
                    |run| {
                        if run == 1 {
                            merge_models(keyed_src(0, 4, 1))
                        } else {
                            merge_models(keyed_src(2, 6, 2))
                        }
                    },
                    1,
                    2,
                    volume,
                    [&PartitionRunOptions::default(); 2],
                )
                .await;
                ran.result.as_ref().expect("a warning never fails the run");
                assert_eq!(ran.volume("vol")["verdict"], verdict, "{volume}");
                let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
                assert!(
                    warnings[0].as_str().unwrap().contains(words),
                    "{volume}: {warnings:?}"
                );
            }
        }

        /// Rows on three days: 2, 3 and 4.
        const DAYS_SRC: &str = "SELECT CAST(d AS DATE) AS d, x FROM (VALUES \
             ('2024-01-01', 1), ('2024-01-01', 2), \
             ('2024-01-02', 3), ('2024-01-02', 4), ('2024-01-02', 5), \
             ('2024-01-03', 6), ('2024-01-03', 7), ('2024-01-03', 8), ('2024-01-03', 9)) \
             AS t(d, x)\n";

        fn partitioned_models(_: u32) -> Vec<(&'static str, String, &'static str)> {
            vec![
                ("src", DAYS_SRC.to_string(), SRC_TOML),
                (
                    "vol",
                    "SELECT d, x FROM main.src WHERE d >= @start_date AND d < @end_date\n"
                        .to_string(),
                    "depends_on = [\"src\"]\n\n\
                     [strategy]\ntype = \"time_interval\"\ntime_column = \"d\"\n\
                     granularity = \"day\"\nfirst_partition = \"2024-01-01\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
                ("down", "SELECT x FROM main.vol\n".to_string(), DOWN_TOML),
            ]
        }

        /// `partitioned_models`, except that the SQL fails for the rows of
        /// 2024-01-03, so that partition fails to run.
        fn failing_partition_models() -> Vec<(&'static str, String, &'static str)> {
            let mut models = partitioned_models(0);
            models[1].1 = "SELECT d, CASE WHEN d = DATE '2024-01-03' \
                 THEN CAST(CAST(x AS VARCHAR) || 'z' AS INTEGER) ELSE x END AS x \
                 FROM main.src WHERE d >= @start_date AND d < @end_date\n"
                .to_string();
            models
        }

        fn days(from: &str, to: &str) -> PartitionRunOptions {
            PartitionRunOptions {
                from: Some(from.into()),
                to: Some(to.into()),
                parallel: 1,
                ..Default::default()
            }
        }

        /// The first run writes 2024-01-03 (4 rows); the second writes
        /// 2024-01-01 and 2024-01-02 (2 + 3 rows). The second judges 5, not
        /// the table's 9.
        async fn two_partition_runs(volume: &str) -> Ran {
            let (ran, first) = two_runs(
                partitioned_models,
                1,
                2,
                volume,
                [
                    &days("2024-01-03", "2024-01-03"),
                    &days("2024-01-01", "2024-01-02"),
                ],
            )
            .await;
            let first_volume = &first["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .rfind(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .unwrap()["volume"];
            assert_eq!(first_volume["row_count"], 4, "{first_volume}");
            ran
        }

        fn partition_volumes(ran: &Ran) -> Vec<serde_json::Value> {
            ran.output["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .map(|m| m["volume"].clone())
                .collect()
        }

        /// The one error entry the run output holds for the model.
        fn model_error(ran: &Ran) -> String {
            let errors = ran.output["errors"].as_array().expect("errors");
            assert_eq!(errors.len(), 1, "{errors:?}");
            errors[0]["error"]
                .as_str()
                .expect("an error text")
                .to_string()
        }

        fn executions_by_partition(ran: &Ran) -> Vec<(&ModelExecution, serde_json::Value)> {
            // Executions and materializations are in the same order.
            let executions = ran.executions("vol");
            let volumes = partition_volumes(ran);
            assert_eq!(executions.len(), volumes.len());
            executions.into_iter().zip(volumes).collect()
        }

        /// Each partition is counted and judged on its own. The partitions
        /// hold 2 and 3 rows: both are in range 2..=3, though their sum (5)
        /// is not. Each carries its own check, and both deliver.
        #[tokio::test]
        async fn a_time_interval_output_judges_each_partition_on_its_own() {
            let ran = two_partition_runs("min_rows = 2\nmax_rows = 3\nseverity = \"error\"").await;
            ran.result
                .as_ref()
                .expect("2 and 3 rows are each in range, though 5 is not");
            assert_eq!(table_total(&ran, "vol"), 9);
            let volumes = partition_volumes(&ran);
            assert_eq!(volumes.len(), 2, "two partitions written: {volumes:?}");
            let counts: Vec<_> = volumes.iter().map(|v| v["row_count"].clone()).collect();
            assert_eq!(counts, [2, 3], "{volumes:?}");
            for volume in &volumes {
                assert_eq!(volume["basis"], "partition_rows", "{volume}");
                assert_eq!(volume["verdict"], "in_range", "{volume}");
            }
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 2, "{executions:?}");
            assert!(executions.iter().all(|e| e.status == "success"));
            assert!(executions.iter().all(|e| e.due_at.is_some()));
            assert_eq!(
                executions.iter().map(|e| e.row_count).collect::<Vec<_>>(),
                [Some(2), Some(3)],
                "each execution records its own partition's count"
            );
            assert_eq!(
                ran.record
                    .output_volumes
                    .iter()
                    .map(|v| (v.partition.clone(), v.row_count))
                    .collect::<Vec<_>>(),
                [
                    (Some("2024-01-01".to_string()), Some(2)),
                    (Some("2024-01-02".to_string()), Some(3))
                ]
            );
            // `rocky product status` judges the newest build: in range.
            let root = ran._tmp.path();
            let status = super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status");
            let volume = status.volume.expect("volume");
            assert_eq!(volume.verdict, VolumeVerdict::InRange);
            assert_eq!(
                volume.basis,
                Some(rocky_core::product::spec::VolumeBasis::PartitionRows)
            );
        }

        /// One partition (2 rows) is below min 3; the other (3 rows) is not.
        /// The sum (5) would pass. One warning, naming the partition.
        #[tokio::test]
        async fn a_time_interval_output_below_min_warns_for_that_partition_only() {
            let ran = two_partition_runs("min_rows = 3").await;
            ran.result.as_ref().expect("a warning never fails the run");
            let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            let line = warnings[0].as_str().unwrap();
            assert!(line.contains("partition '2024-01-01'"), "{line}");
            assert!(
                line.contains("the partition this run wrote holds 2 row(s), below min_rows = 3"),
                "{line}"
            );
            let verdicts: Vec<_> = partition_volumes(&ran)
                .iter()
                .map(|v| v["verdict"].clone())
                .collect();
            assert_eq!(verdicts, ["below_min", "in_range"]);
        }

        /// Above max at `error` for one partition (3 rows over max 2): the
        /// model fails, the run is partial, downstream is withheld. Only the
        /// breaching partition is marked failed; the other stays a success
        /// as a record, but the model delivered nothing, so neither carries
        /// a `due_at`. The breaching partition is `Failed` in the partition
        /// state, so `--missing` rewrites it; the others stay `Computed`.
        #[tokio::test]
        async fn a_time_interval_output_above_max_at_error_fails_only_the_breaching_partition() {
            let ran = two_partition_runs("max_rows = 2\nseverity = \"error\"").await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            let err = model_error(&ran);
            assert!(err.contains("partition '2024-01-02'"), "{err}");
            assert!(!err.contains("partition '2024-01-01'"), "{err}");
            let by_partition = executions_by_partition(&ran);
            assert_eq!(
                by_partition.len(),
                2,
                "no extra failed record: {by_partition:?}"
            );
            let statuses: Vec<_> = by_partition
                .iter()
                .map(|(e, _)| e.status.as_str())
                .collect();
            assert_eq!(statuses, ["success", "failed"], "{by_partition:?}");
            assert_eq!(by_partition[0].1["verdict"], "in_range");
            assert_eq!(by_partition[1].1["verdict"], "above_max");
            assert!(by_partition[1].0.due_at.is_none());
            assert!(
                by_partition[0].0.due_at.is_none(),
                "a passing partition of a failed model is not a delivery"
            );
            assert!(
                ran.executions("down").iter().all(|e| e.status != "success"),
                "the downstream model is withheld: {:?}",
                ran.record.models_executed
            );
            let state = StateStore::open(&ran._tmp.path().join("state.redb")).unwrap();
            let partitions = state.list_partitions("vol").unwrap();
            assert_eq!(partitions.len(), 3, "{partitions:?}");
            let failed: Vec<_> = state
                .list_failed_partitions("vol")
                .unwrap()
                .into_iter()
                .map(|p| p.partition_key)
                .collect();
            assert_eq!(failed, ["2024-01-02"], "only the breaching partition");
        }

        /// A breach leaves its partition `Failed`, so a later run that names
        /// it writes it again and judges it again: still out of range stays
        /// failed, and in range is `Computed` once more.
        #[tokio::test]
        async fn a_breaching_partition_is_rewritten_and_judged_again() {
            let ran = two_partition_runs("max_rows = 2\nseverity = \"error\"").await;
            assert!(ran.result.is_err());
            let root = ran._tmp.path();
            let state = StateStore::open(&root.join("state.redb")).unwrap();
            let failed = |state: &StateStore| -> Vec<String> {
                state
                    .list_failed_partitions("vol")
                    .unwrap()
                    .into_iter()
                    .map(|p| p.partition_key)
                    .collect()
            };
            assert_eq!(failed(&state), ["2024-01-02"]);
            drop(state);

            let one = PartitionRunOptions {
                partition: Some("2024-01-02".into()),
                parallel: 1,
                ..Default::default()
            };
            // Still above max: judged again, still failed.
            let (result, record, output) = run_in(root, None, &one).await;
            assert!(result.is_err(), "the rewrite breaches again");
            let rejudged: Vec<_> = output["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .map(|m| m["volume"]["verdict"].clone())
                .collect();
            assert_eq!(rejudged, ["above_max"]);
            assert_eq!(record.status, RunStatus::PartialFailure);
            let state = StateStore::open(&root.join("state.redb")).unwrap();
            assert_eq!(failed(&state), ["2024-01-02"]);
            drop(state);

            // The range now admits it: the rewrite passes and heals it.
            write_project(
                root,
                &spec("max_rows = 3\nseverity = \"error\""),
                &borrowed(&partitioned_models(0)),
                "",
            );
            let (result, _, _) = run_in(root, None, &one).await;
            result.expect("3 rows are in range now");
            let state = StateStore::open(&root.join("state.redb")).unwrap();
            assert!(failed(&state).is_empty());
        }

        /// A partition that fails to run does not stop the partitions that
        /// did write from being judged: the breaching one is recorded and
        /// marked failed. Here 2024-01-01 (2 rows) is in range and
        /// 2024-01-02 (3 rows) breaches max 2, while 2024-01-03 fails to
        /// run.
        #[tokio::test]
        async fn written_partitions_are_judged_when_another_partition_fails() {
            let tmp = tempfile::TempDir::new().unwrap();
            write_project(
                tmp.path(),
                &spec("max_rows = 2\nseverity = \"error\""),
                &borrowed(&failing_partition_models()),
                "",
            );
            let (result, record, output) =
                run_in(tmp.path(), None, &days("2024-01-01", "2024-01-03")).await;
            assert!(result.is_err());
            let verdicts: Vec<_> = output["materializations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&"vol".into()))
                .map(|m| m["volume"]["verdict"].clone())
                .collect();
            assert!(
                verdicts.contains(&serde_json::json!("in_range")),
                "the partitions written before the failure are judged: {verdicts:?}"
            );
            assert!(
                record.output_volumes.iter().any(|v| v.partition.is_some()),
                "{:?}",
                record.output_volumes
            );
        }

        /// Every partition over max at `error` fails: all are marked failed
        /// and the one error names both.
        #[tokio::test]
        async fn a_time_interval_output_marks_every_breaching_partition_failed() {
            let ran = two_partition_runs("max_rows = 1\nseverity = \"error\"").await;
            let err = model_error(&ran);
            assert!(err.contains("2 partitions"), "{err}");
            assert!(
                err.contains("2024-01-01") && err.contains("2024-01-02"),
                "{err}"
            );
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 2, "{executions:?}");
            assert!(
                executions.iter().all(|e| e.status == "failed"),
                "{executions:?}"
            );
        }

        // ----- the open breach marker (#2581) -----

        const ERROR_MAX_4: &str = "max_rows = 4\nseverity = \"error\"";

        fn markers(root: &std::path::Path) -> Vec<rocky_core::product::breach::VolumeBreachMarker> {
            StateStore::open(&root.join("state.redb"))
                .unwrap()
                .volume_breach_list()
                .unwrap()
        }

        fn executions_of<'a>(record: &'a RunRecord, model: &str) -> Vec<&'a ModelExecution> {
            record
                .models_executed
                .iter()
                .filter(|e| e.rocky_model_name() == model)
                .collect()
        }

        /// Every error text of a run's JSON output, joined.
        fn error_text(output: &serde_json::Value) -> String {
            output["errors"]
                .as_array()
                .map(|errors| {
                    errors
                        .iter()
                        .filter_map(|e| e["error"].as_str())
                        .collect::<Vec<_>>()
                        .join("; ")
                })
                .unwrap_or_default()
        }

        fn volume_of(output: &serde_json::Value, table: &str) -> serde_json::Value {
            output["materializations"]
                .as_array()
                .expect("materializations")
                .iter()
                .rfind(|m| m["asset_key"].as_array().and_then(|k| k.last()) == Some(&table.into()))
                .map(|m| m["volume"].clone())
                .unwrap_or(serde_json::Value::Null)
        }

        /// The incremental project again with `rows` source rows and the
        /// given range, optionally as `--full-refresh`.
        async fn incremental_again(
            root: &std::path::Path,
            rows: u32,
            volume: &str,
            full_refresh: bool,
        ) -> (anyhow::Result<()>, RunRecord, serde_json::Value) {
            write_project(
                root,
                &spec(volume),
                &borrowed(&incremental_models(&src_rows(rows))),
                "",
            );
            run_in_with(
                root,
                None,
                &PartitionRunOptions::default(),
                &SkipRunOptions {
                    full_refresh,
                    ..Default::default()
                },
            )
            .await
        }

        /// A breach opens the marker. The next in-range append writes its
        /// rows but the model stays failed and downstream stays withheld. A
        /// `--full-refresh` that is in range clears the marker and passes.
        /// Mutation: skip the marker read in `settle_whole` and the second
        /// run passes.
        #[tokio::test]
        async fn an_open_breach_keeps_an_incremental_output_failed_until_a_full_refresh() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                ERROR_MAX_4,
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            assert!(ran.result.is_err(), "5 added rows breach max 4");
            let root = ran._tmp.path();
            let open = markers(root);
            assert_eq!(open.len(), 1, "{open:?}");
            assert_eq!(
                (open[0].product.as_str(), open[0].model.as_str()),
                ("vol_product", "vol")
            );
            assert!(open[0].partitions.is_empty(), "the whole output");
            assert_eq!(open[0].rows, Some(5));
            assert_eq!(open[0].max_rows, Some(4));
            let opened_at = open[0].opened_at;

            // 3 more rows: in range, written, still failed.
            let (result, record, output) = incremental_again(root, 103, ERROR_MAX_4, false).await;
            assert!(result.is_err(), "an open breach keeps the model failed");
            let err = error_text(&output);
            assert!(err.contains("open volume breach"), "{err}");
            assert!(err.contains("rocky run --full-refresh"), "{err}");
            assert!(
                err.contains("rocky product clear-breach vol_product --model vol"),
                "{err}"
            );
            assert_eq!(
                table_total(&ran, "vol"),
                103,
                "the load still writes its rows"
            );
            let volume = volume_of(&output, "vol");
            assert_eq!(volume["verdict"], "in_range", "{volume}");
            assert_eq!(volume["row_count"], 3, "{volume}");
            assert_eq!(volume["held_by_open_breach"], true, "{volume}");
            let executions = executions_of(&record, "vol");
            assert_eq!(executions.len(), 1, "{executions:?}");
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].due_at, None, "no delivery over a breach");
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .all(|e| e.status != "success"),
                "downstream is withheld: {:?}",
                record.models_executed
            );
            let still = markers(root);
            assert_eq!(still.len(), 1);
            assert_eq!(
                still[0].opened_at, opened_at,
                "the marker keeps its first date"
            );

            // A full refresh that is in range clears it.
            let (result, record, output) =
                incremental_again(root, 103, "max_rows = 200\nseverity = \"error\"", true).await;
            result.expect("an in-range rebuild clears the breach");
            assert!(markers(root).is_empty());
            assert!(
                volume_of(&output, "vol")
                    .get("held_by_open_breach")
                    .is_none()
            );
            let executions = executions_of(&record, "vol");
            assert_eq!(executions[0].status, "success");
            assert!(executions[0].due_at.is_some());
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .any(|e| e.status == "success"),
                "downstream builds again"
            );
        }

        /// A full refresh that is itself out of range keeps the marker.
        #[tokio::test]
        async fn a_rebuild_that_is_still_out_of_range_keeps_the_breach_open() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                ERROR_MAX_4,
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            let root = ran._tmp.path();
            let (result, _, _) = incremental_again(root, 103, ERROR_MAX_4, true).await;
            assert!(result.is_err(), "103 rows breach max 4");
            assert_eq!(markers(root).len(), 1);
        }

        /// A `warning` breach never opens a marker.
        #[tokio::test]
        async fn a_warning_breach_opens_no_marker() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                "max_rows = 4",
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            ran.result.as_ref().expect("a warning never fails the run");
            assert_eq!(ran.volume("vol")["verdict"], "above_max");
            assert!(markers(ran._tmp.path()).is_empty());
        }

        /// A shadow run neither reads nor writes markers: it is not held by
        /// the open breach and does not change it.
        #[tokio::test]
        async fn a_non_production_run_touches_no_marker() {
            let ran = run_with(
                &spec("max_rows = 2\nseverity = \"error\""),
                &one_model(),
                None,
            )
            .await;
            assert!(ran.result.is_err(), "3 rows breach max 2");
            let root = ran._tmp.path();
            let before = markers(root);
            assert_eq!(before.len(), 1);
            let shadow = rocky_core::shadow::ShadowConfig::default();
            let (result, _, _) = run_in(root, Some(&shadow), &PartitionRunOptions::default()).await;
            result.expect("a shadow run is not held by a production breach");
            assert_eq!(markers(root), before);
        }

        /// `rocky product clear-breach` deletes the marker and says so; a
        /// second call changes nothing. `rocky product status` lists the
        /// open breach until then.
        #[tokio::test]
        async fn clear_breach_deletes_the_marker_and_status_lists_it() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                ERROR_MAX_4,
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            let root = ran._tmp.path();
            let state_path = root.join("state.redb");
            let status = super::super::super::product::product_status_in(
                root,
                Some(&state_path),
                "vol_product",
            )
            .expect("status");
            assert_eq!(status.open_volume_breaches.len(), 1, "{status:?}");
            let breach = &status.open_volume_breaches[0];
            assert_eq!(breach.model, "vol");
            assert_eq!(breach.rows, Some(5));
            assert_eq!(breach.max_rows, Some(4));
            assert!(
                breach.reason.contains("added 5 row(s)"),
                "{}",
                breach.reason
            );

            let cleared = super::super::super::product::product_clear_breach(
                &state_path,
                "vol_product",
                "vol",
            )
            .expect("clear");
            assert!(cleared.was_open);
            assert_eq!(
                cleared.cleared.as_ref().map(|c| c.model.as_str()),
                Some("vol")
            );
            assert!(markers(root).is_empty());
            let again = super::super::super::product::product_clear_breach(
                &state_path,
                "vol_product",
                "vol",
            )
            .expect("clear again");
            assert!(!again.was_open, "idempotent");
            assert!(again.cleared.is_none());
            let status = super::super::super::product::product_status_in(
                root,
                Some(&state_path),
                "vol_product",
            )
            .expect("status");
            assert!(status.open_volume_breaches.is_empty());

            // With the marker cleared, the next in-range load passes.
            let (result, _, _) = incremental_again(root, 103, ERROR_MAX_4, false).await;
            result.expect("a cleared breach no longer holds the model");
        }

        /// Time interval: the breaching partition opens the marker. A run of
        /// another, in-range partition keeps the model failed. Rewriting the
        /// breaching partition in range clears it.
        /// Mutation: skip the marker read in `settle_partitions` and the
        /// second run passes.
        #[tokio::test]
        async fn an_open_breach_keeps_a_time_interval_output_failed_until_the_partition_is_rewritten()
         {
            let ran = two_partition_runs("max_rows = 2\nseverity = \"error\"").await;
            assert!(ran.result.is_err());
            let root = ran._tmp.path();
            let open = markers(root);
            assert_eq!(open.len(), 1, "{open:?}");
            assert_eq!(open[0].partitions, ["2024-01-02"]);

            // Another partition, in range: written, but the model fails.
            let other = PartitionRunOptions {
                partition: Some("2024-01-01".into()),
                parallel: 1,
                ..Default::default()
            };
            let (result, record, output) = run_in(root, None, &other).await;
            assert!(result.is_err(), "the open breach holds the model");
            let err = error_text(&output);
            assert!(err.contains("open volume breach"), "{err}");
            assert!(err.contains("2024-01-02"), "{err}");
            assert!(err.contains("rocky run --missing"), "{err}");
            assert_eq!(volume_of(&output, "vol")["verdict"], "in_range");
            assert_eq!(volume_of(&output, "vol")["held_by_open_breach"], true);
            assert!(
                executions_of(&record, "vol")
                    .iter()
                    .all(|e| e.status == "failed"),
                "{:?}",
                record.models_executed
            );
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .all(|e| e.status != "success"),
                "downstream is withheld"
            );
            assert_eq!(markers(root)[0].partitions, ["2024-01-02"]);

            // The range admits the breaching partition; rewriting it clears.
            write_project(
                root,
                &spec("max_rows = 3\nseverity = \"error\""),
                &borrowed(&partitioned_models(0)),
                "",
            );
            let breaching = PartitionRunOptions {
                partition: Some("2024-01-02".into()),
                parallel: 1,
                ..Default::default()
            };
            let (result, record, _) = run_in(root, None, &breaching).await;
            result.expect("the rewrite is in range and clears the breach");
            assert!(markers(root).is_empty());
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .any(|e| e.status == "success"),
                "downstream builds again"
            );
        }

        /// A rewrite of one of two breached partitions clears only that one.
        #[tokio::test]
        async fn a_partial_rewrite_narrows_the_breach_to_the_partitions_left() {
            let ran = two_partition_runs("max_rows = 1\nseverity = \"error\"").await;
            assert!(ran.result.is_err());
            let root = ran._tmp.path();
            assert_eq!(markers(root)[0].partitions, ["2024-01-01", "2024-01-02"]);
            // Day 1 has 2 rows: in range of max 2. Day 2 has 3.
            write_project(
                root,
                &spec("max_rows = 2\nseverity = \"error\""),
                &borrowed(&partitioned_models(0)),
                "",
            );
            let first = PartitionRunOptions {
                partition: Some("2024-01-01".into()),
                parallel: 1,
                ..Default::default()
            };
            let (result, _, output) = run_in(root, None, &first).await;
            assert!(result.is_err(), "one partition is still breached");
            let err = error_text(&output);
            assert!(
                err.contains("Still breached: partition(s) 2024-01-02."),
                "{err}"
            );
            assert_eq!(markers(root)[0].partitions, ["2024-01-02"]);
        }

        /// A time-interval `warning` breach opens no marker.
        #[tokio::test]
        async fn a_time_interval_warning_breach_opens_no_marker() {
            let ran = two_partition_runs("min_rows = 3").await;
            ran.result.as_ref().expect("a warning never fails the run");
            assert!(markers(ran._tmp.path()).is_empty());
        }

        /// `rocky run --model down` does not build `vol`, so the volume
        /// check never sees it. The open breach on `vol` still withholds
        /// `down`: it must not build over the breached rows. Once the
        /// breach is cleared, the same run builds `down`.
        /// Mutation: drop the `held_outputs_not_built` withholding in the
        /// model phase and the first `--model down` run builds `down`.
        #[tokio::test]
        async fn a_held_output_withholds_a_downstream_model_run_alone() {
            let (ran, _) = two_runs(
                |n| incremental_models(&src_rows(n)),
                95,
                100,
                ERROR_MAX_4,
                [&PartitionRunOptions::default(); 2],
            )
            .await;
            assert!(ran.result.is_err(), "5 added rows breach max 4");
            let root = ran._tmp.path();
            assert_eq!(markers(root).len(), 1);

            let (result, record, output) = run_in_selecting(
                root,
                None,
                &PartitionRunOptions::default(),
                &SkipRunOptions::default(),
                Some("down"),
            )
            .await;
            let err = error_text(&output);
            assert!(err.contains("model 'down' was withheld"), "{err}");
            assert!(err.contains("open volume breach"), "{err}");
            assert!(
                err.contains("rocky product clear-breach vol_product --model vol"),
                "{err}"
            );
            assert!(result.is_err() || output["tables_failed"] != 0, "{output}");
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .all(|e| e.status != "success"),
                "down is withheld: {:?}",
                record.models_executed
            );
            assert!(executions_of(&record, "vol").is_empty(), "vol is not run");

            super::super::super::product::product_clear_breach(
                &root.join("state.redb"),
                "vol_product",
                "vol",
            )
            .expect("clear");
            let (result, record, _) = run_in_selecting(
                root,
                None,
                &PartitionRunOptions::default(),
                &SkipRunOptions::default(),
                Some("down"),
            )
            .await;
            result.expect("with the breach cleared, down builds");
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .any(|e| e.status == "success"),
                "{:?}",
                record.models_executed
            );
        }

        /// A time-interval output held by an open breach, in a run that
        /// writes no partition (`--missing` with nothing missing), still
        /// records the model failed.
        #[tokio::test]
        async fn a_held_time_interval_run_that_writes_no_partition_still_fails() {
            let today = chrono::Utc::now().date_naive();
            let yesterday = today - chrono::Duration::days(1);
            let src = format!(
                "SELECT CAST(d AS DATE) AS d, x FROM (VALUES \
                 ('{yesterday}', 1), ('{yesterday}', 2), \
                 ('{today}', 3), ('{today}', 4), ('{today}', 5)) AS t(d, x)\n"
            );
            let vol_toml = format!(
                "depends_on = [\"src\"]\n\n\
                 [strategy]\ntype = \"time_interval\"\ntime_column = \"d\"\n\
                 granularity = \"day\"\nfirst_partition = \"{yesterday}\"\n\n\
                 [target]\ncatalog = \"\"\nschema = \"main\"\n"
            );
            let models: Vec<(&str, &str, &str)> = vec![
                ("src", src.as_str(), SRC_TOML),
                (
                    "vol",
                    "SELECT d, x FROM main.src WHERE d >= @start_date AND d < @end_date\n",
                    vol_toml.as_str(),
                ),
                ("down", "SELECT x FROM main.vol\n", DOWN_TOML),
            ];
            let tmp = tempfile::TempDir::new().unwrap();
            let root = tmp.path();
            write_project(
                root,
                &spec("max_rows = 2\nseverity = \"error\""),
                &models,
                "",
            );
            let (result, _, _) = run_in(
                root,
                None,
                &days(&yesterday.to_string(), &today.to_string()),
            )
            .await;
            assert!(result.is_err(), "today's 3 rows breach max 2");
            assert_eq!(markers(root)[0].partitions, [today.to_string()]);

            // Every partition is recorded computed (say, a lost `Failed`
            // mark), so `--missing` plans nothing.
            {
                let store = StateStore::open(&root.join("state.redb")).unwrap();
                let records: Vec<_> = store
                    .list_partitions("vol")
                    .unwrap()
                    .into_iter()
                    .map(|mut record| {
                        record.status = rocky_core::incremental::PartitionStatus::Computed;
                        record
                    })
                    .collect();
                assert_eq!(records.len(), 2, "{records:?}");
                store.record_partitions(&records).unwrap();
            }
            let missing = PartitionRunOptions {
                missing: true,
                parallel: 1,
                ..Default::default()
            };
            let (result, record, output) = run_in(root, None, &missing).await;
            assert!(result.is_err(), "the open breach holds the model");
            let err = error_text(&output);
            assert!(err.contains("open volume breach"), "{err}");
            let vol = executions_of(&record, "vol");
            assert!(
                !vol.is_empty(),
                "a failure is recorded: {:?}",
                record.models_executed
            );
            assert!(vol.iter().all(|e| e.status == "failed"), "{vol:?}");
            assert!(
                executions_of(&record, "down")
                    .iter()
                    .all(|e| e.status != "success"),
                "downstream is withheld"
            );
        }
    }
}
