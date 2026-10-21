//! The product volume check `rocky run` makes after a production build.
//!
//! A product may declare `[product.output.volume]`: a row-count range its
//! output table must hold after a load. Right after a successful production
//! build of that output, the run counts the table's rows once and judges the
//! count:
//!
//! ```text
//!   build ok ── production? ──no──▶ nothing counted (no `volume` field)
//!                   │ yes
//!          volume declared for the model? ──no──▶ nothing counted
//!                   │ yes
//!          full-refresh strategy? ──no──▶ not_evaluated (never a failure)
//!                   │ yes
//!          SELECT COUNT(*) ── cancelled / error / unreadable ──▶ not_evaluated
//!                   │ count
//!          in range ─────────────────▶ in_range
//!          out of range, warning ────▶ a `volume_warnings` line; delivery as usual
//!          out of range, error ──────▶ the model fails after the fact:
//!                                      downstream withheld as for any failure,
//!                                      run partial, no `due_at`
//! ```
//!
//! The count is the total rows, not the rows this load wrote, which is why
//! only a full-refresh output is counted: a total on an incremental or merge
//! table only grows.

use std::path::Path;

use rocky_core::product::spec::{VolumeSeverity, VolumeVerdict};
use rocky_core::traits::WarehouseAdapter;
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
    /// The ranges the products under `root` declare, or none.
    ///
    /// Empty when `production` is false (a shadow or branch run counts
    /// nothing, as it stamps no `due_at`) or when the run has no project
    /// root to read `products/` from.
    pub(crate) fn load(root: Option<&Path>, production: bool) -> Self {
        let ranges = match root {
            Some(root) if production => super::product::product_volumes_in(root),
            Some(_) | None => Vec::new(),
        };
        Self { ranges }
    }

    #[cfg(test)]
    pub(crate) fn from_ranges(ranges: Vec<ProductVolume>) -> Self {
        Self { ranges }
    }

    fn for_model(&self, model: &str) -> Option<&ProductVolume> {
        self.ranges.iter().find(|range| range.output_model == model)
    }
}

/// Whether a total-row count on one write of this strategy says something
/// about that write.
///
/// Exhaustive on purpose (no `_ =>`): a new strategy fails to compile here
/// until someone decides whether its total row count is a load's volume.
pub(crate) fn counts_total_rows(strategy: &MaterializationStrategy) -> bool {
    match strategy {
        // The whole table is this load.
        MaterializationStrategy::FullRefresh => true,
        // No stored table, or a warehouse-managed one Rocky does not write.
        MaterializationStrategy::View
        | MaterializationStrategy::Ephemeral
        | MaterializationStrategy::MaterializedView
        | MaterializationStrategy::DynamicTable { .. } => false,
        // The table accumulates across loads, so its total only grows.
        MaterializationStrategy::Incremental { .. }
        | MaterializationStrategy::Merge { .. }
        | MaterializationStrategy::DeleteInsert { .. }
        | MaterializationStrategy::Microbatch { .. }
        | MaterializationStrategy::TimeInterval { .. }
        | MaterializationStrategy::Snapshot(_)
        | MaterializationStrategy::ContentAddressed { .. } => false,
    }
}

/// Count and judge `model`'s output when a product declares a volume for it.
///
/// Records the check on `materialization.volume` and, for a `warning`
/// breach, a line in `output.volume_warnings`. Returns the error the caller
/// must treat as this model's failure for an `error` breach, and `None`
/// otherwise. A count that cannot be taken is `not_evaluated`, never an
/// error.
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
    let (row_count, reason) = if !counts_total_rows(strategy) {
        (
            None,
            Some(
                "[volume-strategy-unsupported] the output model is not a full-refresh table, \
                 so its total row count is not a load's volume; not counted"
                    .to_string(),
            ),
        )
    } else if cancel.is_cancelled() {
        (
            None,
            Some("the run was cancelled before the count".to_string()),
        )
    } else {
        match count_rows(warehouse, &materialization.asset_key).await {
            Ok(count) => (Some(count), None),
            Err(reason) => (None, Some(reason)),
        }
    };
    let verdict = row_count.map_or(VolumeVerdict::NotEvaluated, |count| {
        range.volume.judge(count)
    });
    let severity = range.volume.severity();
    let mut check = VolumeCheckOutput {
        product: range.product.clone(),
        model: model.to_string(),
        row_count,
        min_rows: range.volume.min_rows,
        max_rows: range.volume.max_rows,
        severity,
        verdict,
        reason,
    };
    let breach = describe_breach(&check);
    let failure = match (breach, severity) {
        (None, _) => None,
        (Some(line), VolumeSeverity::Warning) => {
            warn!(model, product = %check.product, "{line}");
            output.volume_warnings.push(line);
            None
        }
        (Some(line), VolumeSeverity::Error) => {
            check.reason = Some(
                "the table is written, but the model is marked failed: downstream models \
                 are withheld and the build is not recorded as a delivery"
                    .to_string(),
            );
            Some(anyhow::anyhow!("{line}"))
        }
    };
    materialization.volume = Some(check);
    failure
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
    Some(format!(
        "model '{}' has {count} row(s), {bound} (product '{}', \
         [product.output.volume] severity = \"{}\")",
        check.model,
        check.product,
        check.severity.as_str()
    ))
}

/// `SELECT COUNT(*)` on the written table. The error is the reason the
/// verdict is `not_evaluated`.
async fn count_rows(warehouse: &dyn WarehouseAdapter, asset_key: &[String]) -> Result<u64, String> {
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
    let result = warehouse
        .execute_query(&format!("SELECT COUNT(*) FROM {table_ref}"))
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

    #[test]
    fn only_full_refresh_counts_total_rows() {
        assert!(counts_total_rows(&MaterializationStrategy::FullRefresh));
        assert!(!counts_total_rows(&MaterializationStrategy::View));
        assert!(!counts_total_rows(&MaterializationStrategy::Ephemeral));
    }

    #[test]
    fn a_non_production_run_loads_no_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            ProductVolumes::load(Some(dir.path()), false)
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
            let dir = tmp.path();
            let models_dir = dir.join("models");
            std::fs::create_dir_all(&models_dir).unwrap();
            for (name, sql, toml) in models {
                std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
                std::fs::write(models_dir.join(format!("{name}.toml")), toml).unwrap();
            }
            std::fs::create_dir_all(dir.join("products")).unwrap();
            std::fs::write(dir.join("products/vol_product.toml"), spec_text).unwrap();
            let config_path = dir.join("rocky.toml");
            std::fs::write(
                &config_path,
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
                &PartitionRunOptions::default(),
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
            Ran {
                result,
                record,
                output,
                _tmp: tmp,
            }
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
    }
}
