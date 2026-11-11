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
//!          incremental ─── COUNT(*) after - COUNT(*) before ── rows_added
//!          merge ───────── the MERGE's reported row count ──── rows_touched
//!          time_interval ─ COUNT(*) in the partitions written ─ partition_rows
//!                   │ cancelled / error / not reported ──▶ not_evaluated
//!                   │ count
//!          in range ─────────────────▶ in_range
//!          out of range, warning ────▶ a `volume_warnings` line; delivery as usual
//!          out of range, error ──────▶ the model fails after the fact:
//!                                      downstream withheld as for any failure,
//!                                      run partial, no `due_at`
//! ```
//!
//! A build that created the table (a first run) or replaced it whole
//! (`--full-refresh`) counts every row in it as this load's, on any basis.

use std::path::Path;

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

/// Count and judge `model`'s output when a product declares a volume for it.
///
/// For a model built by the plain transformation path: full refresh,
/// incremental, merge (`time_interval` goes through
/// [`check_partition_volume`]). Records the check on
/// `materialization.volume` and, for a `warning` breach, a line in
/// `output.volume_warnings`. Returns the error the caller must treat as
/// this model's failure for an `error` breach, and `None` otherwise. A
/// count that cannot be taken is `not_evaluated`, never an error.
pub(crate) async fn check_volume(
    volumes: &ProductVolumes,
    warehouse: &dyn WarehouseAdapter,
    model: &str,
    strategy: &MaterializationStrategy,
    materialization: &mut MaterializationOutput,
    output: &mut RunOutput,
    cancel: &tokio_util::sync::CancellationToken,
) -> Option<anyhow::Error> {
    let range = volumes.for_model(model)?;
    let basis = VolumeBasis::of(strategy);
    let counted = match basis {
        None => Err(unsupported_reason()),
        Some(_) if cancel.is_cancelled() => Err("the run was cancelled before the count".into()),
        Some(basis) => count_load(warehouse, basis, materialization).await,
    };
    let judged = judge(range, model, basis, counted);
    materialization.volume = Some(judged.check);
    judged.settle(output)
}

/// Count and judge a `time_interval` model's output: the rows in the
/// partitions this run wrote, `output.materializations[first..]` (one per
/// written partition). The check is recorded on the last of them.
///
/// Each partition is counted with the predicate its overwrite used
/// ([`rocky_core::sql_gen::time_interval_partition_filter`]), in one
/// `COUNT(*)` over all of them. A run that wrote no partition records no
/// check. Same returns as [`check_volume`].
pub(crate) async fn check_partition_volume(
    volumes: &ProductVolumes,
    warehouse: &dyn WarehouseAdapter,
    dialect: &dyn SqlDialect,
    model: &rocky_core::models::Model,
    output: &mut RunOutput,
    first: usize,
    cancel: &tokio_util::sync::CancellationToken,
) -> Option<anyhow::Error> {
    let model_name = model.config.name.as_str();
    let range = volumes.for_model(model_name)?;
    let written = output.materializations.get(first..).unwrap_or_default();
    let last = written.last()?;
    let asset_key = last.asset_key.clone();
    let windows: Vec<rocky_ir::PartitionWindow> = written
        .iter()
        .filter_map(|mat| mat.partition.as_ref())
        .map(|partition| rocky_ir::PartitionWindow {
            key: partition.key.clone(),
            start: partition.start,
            end: partition.end,
        })
        .collect();
    let strategy = model.to_model_ir().materialization;
    let basis = VolumeBasis::of(&strategy);
    let counted = match (&strategy, basis) {
        (_, None) => Err(unsupported_reason()),
        _ if cancel.is_cancelled() => Err(
            "the run was cancelled before every partition ran or before the count".into(),
        ),
        (MaterializationStrategy::TimeInterval { time_column, .. }, Some(_)) => {
            if windows.len() != written.len() {
                Err("a written partition recorded no time window; not counted".to_string())
            } else {
                match windows
                    .iter()
                    .map(|window| {
                        rocky_core::sql_gen::time_interval_partition_filter(
                            time_column,
                            window,
                            dialect,
                        )
                        .map(|filter| format!("({filter})"))
                    })
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(filters) => {
                        count_rows(warehouse, &asset_key, Some(&filters.join(" OR "))).await
                    }
                    Err(e) => Err(format!("could not render the partition filter: {e}")),
                }
            }
        }
        (_, Some(_)) => Err(
            "the model is not a time_interval model, so its partitions are not counted".into(),
        ),
    };
    let judged = judge(range, model_name, basis, counted);
    if let Some(last) = output.materializations.last_mut() {
        last.volume = Some(judged.check);
    }
    judged.settle(output)
}

/// The count for one plain build on `basis`, or why it was not taken.
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
            let before = match evidence.and_then(|e| e.rows_before.as_ref()) {
                Some(Ok(before)) => *before,
                Some(Err(reason)) => {
                    return Err(format!("the count before the write failed: {reason}"));
                }
                None => return Err("no row count was taken before the write".into()),
            };
            let after = count_rows(warehouse, &materialization.asset_key, None).await?;
            after.checked_sub(before).ok_or_else(|| {
                format!(
                    "the table holds fewer rows after the write ({after}) than before it \
                     ({before}), so something else changed it during the run; not counted"
                )
            })
        }
        VolumeBasis::RowsTouched => evidence.and_then(|e| e.rows_reported).ok_or_else(|| {
            "the warehouse adapter did not report how many rows the MERGE inserted or \
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
    /// Record the warning, if any, and hand back the failure.
    fn settle(self, output: &mut RunOutput) -> Option<anyhow::Error> {
        if let Some(line) = self.warning {
            output.volume_warnings.push(line);
        }
        self.failure
    }
}

/// Judge `counted` against `range`. A count not taken (`Err`) is
/// `not_evaluated`, with the error as its reason.
fn judge(
    range: &ProductVolume,
    model: &str,
    basis: Option<VolumeBasis>,
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
    };
    let (warning, failure) = match (describe_breach(&check), severity) {
        (None, _) => (None, None),
        (Some(line), VolumeSeverity::Warning) => {
            warn!(model, product = %check.product, "{line}");
            (Some(line), None)
        }
        (Some(line), VolumeSeverity::Error) => {
            check.reason = Some(
                "the table is written, but the model is marked failed: downstream models \
                 are withheld and the build is not recorded as a delivery"
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

/// The sentence for an out-of-range count, or `None` when the count is in
/// range or was not taken.
fn describe_breach(check: &VolumeCheckOutput) -> Option<String> {
    let count = check.row_count?;
    let bound = match check.verdict {
        VolumeVerdict::BelowMin => format!("below min_rows = {}", check.min_rows?),
        VolumeVerdict::AboveMax => format!("above max_rows = {}", check.max_rows?),
        VolumeVerdict::InRange | VolumeVerdict::NotEvaluated => return None,
    };
    let model = &check.model;
    let counted = match check.basis {
        Some(VolumeBasis::TotalRows) | None => format!("model '{model}' has {count} row(s)"),
        Some(VolumeBasis::RowsAdded) => {
            format!("model '{model}': this run added {count} row(s)")
        }
        Some(VolumeBasis::RowsTouched) => {
            format!("model '{model}': this run's MERGE touched {count} row(s)")
        }
        Some(VolumeBasis::PartitionRows) => {
            format!("model '{model}': the partitions this run wrote hold {count} row(s)")
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
        let below = describe_breach(&check(VolumeVerdict::BelowMin, VolumeSeverity::Warning))
            .expect("a breach");
        assert!(below.contains("5 row(s), below min_rows = 10"), "{below}");
        assert!(describe_breach(&check(VolumeVerdict::InRange, VolumeSeverity::Warning)).is_none());
        let mut uncounted = check(VolumeVerdict::NotEvaluated, VolumeSeverity::Error);
        uncounted.row_count = None;
        assert!(describe_breach(&uncounted).is_none());
    }

    /// The breach sentence says what was counted.
    #[test]
    fn a_breach_names_what_was_counted() {
        let mut breach = check(VolumeVerdict::BelowMin, VolumeSeverity::Warning);
        for (basis, words) in [
            (VolumeBasis::TotalRows, "model 'm' has 5 row(s), below"),
            (VolumeBasis::RowsAdded, "this run added 5 row(s), below"),
            (VolumeBasis::RowsTouched, "this run's MERGE touched 5 row(s), below"),
            (
                VolumeBasis::PartitionRows,
                "the partitions this run wrote hold 5 row(s), below",
            ),
        ] {
            breach.basis = Some(basis);
            let line = describe_breach(&breach).expect("a breach");
            assert!(line.contains(words), "{basis:?}: {line}");
        }
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
                None,
                None,
                None,
                None,
                &DeferOptions::default(),
                &SkipRunOptions::default(),
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

        fn borrowed(models: &[(&'static str, String, &'static str)]) -> Vec<(&str, &str, &str)> {
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
            rows[0][0].as_u64().expect("a count")
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
            assert_eq!(first_volume["row_count"], 95, "the first run created the table");

            ran.result.as_ref().expect("in range");
            assert_eq!(table_total(&ran, "vol"), 100, "the table total is not judged");
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
            assert_eq!(first_volume["row_count"], 4, "the first run created the table");

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
                ("min_rows = 5", "below_min", "MERGE touched 4 row(s), below min_rows = 5"),
                ("max_rows = 3", "above_max", "MERGE touched 4 row(s), above max_rows = 3"),
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

        #[tokio::test]
        async fn a_time_interval_output_judges_only_the_partitions_this_run_wrote() {
            let ran = two_partition_runs("min_rows = 5\nmax_rows = 5\nseverity = \"error\"").await;
            ran.result.as_ref().expect("5 rows is in range");
            assert_eq!(table_total(&ran, "vol"), 9);
            let volumes = partition_volumes(&ran);
            assert_eq!(volumes.len(), 2, "two partitions written: {volumes:?}");
            assert!(volumes[0].is_null(), "the check rides on the last: {volumes:?}");
            let volume = &volumes[1];
            assert_eq!(volume["basis"], "partition_rows", "{volume}");
            assert_eq!(volume["row_count"], 5, "{volume}");
            assert_eq!(volume["verdict"], "in_range");
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 2, "{executions:?}");
            assert!(executions.iter().all(|e| e.status == "success"));
            assert!(executions.iter().all(|e| e.due_at.is_some()));
            // `rocky product status` reads the count from the last partition.
            let root = ran._tmp.path();
            let status = super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status");
            assert_eq!(status.volume.expect("volume").row_count, Some(5));
        }

        #[tokio::test]
        async fn a_time_interval_output_below_min_warns() {
            let ran = two_partition_runs("min_rows = 6").await;
            ran.result.as_ref().expect("a warning never fails the run");
            let line = ran.output["volume_warnings"][0].as_str().unwrap().to_string();
            assert!(
                line.contains("the partitions this run wrote hold 5 row(s), below min_rows = 6"),
                "{line}"
            );
        }

        /// Above max at `error`: every execution of the model this run is
        /// failed and none is a delivery, `model_error` is the model's one
        /// failure, the downstream model is withheld, and the partitions
        /// stay `Computed`: their rows are in the table.
        #[tokio::test]
        async fn a_time_interval_output_above_max_at_error_fails_the_model() {
            let ran = two_partition_runs("max_rows = 4\nseverity = \"error\"").await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 2, "no extra failed record: {executions:?}");
            assert!(
                executions.iter().all(|e| e.status == "failed"),
                "{executions:?}"
            );
            assert!(executions.iter().all(|e| e.due_at.is_none()));
            let volume = &partition_volumes(&ran)[1];
            assert_eq!(volume["verdict"], "above_max", "{volume}");
            assert!(
                ran.executions("down").iter().all(|e| e.status != "success"),
                "the downstream model is withheld: {:?}",
                ran.record.models_executed
            );
            let state = StateStore::open(&ran._tmp.path().join("state.redb")).unwrap();
            let partitions = state.list_partitions("vol").unwrap();
            assert_eq!(partitions.len(), 3, "{partitions:?}");
            assert!(
                partitions
                    .iter()
                    .all(|p| p.status == rocky_core::incremental::PartitionStatus::Computed),
                "{partitions:?}"
            );
        }
    }
}
