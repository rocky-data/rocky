//! The source volume check `rocky run` makes when a production run starts.
//!
//! A product may declare `[[product.source.volume]]`: a row-count range on
//! one of its source tables. Before the model phase writes anything, the run
//! counts each such table once and judges the count:
//!
//! ```text
//!   production run, project root? ──no──▶ nothing counted
//!            │ yes
//!   product output in the selection? ──no──▶ that range is not counted
//!            │ yes
//!   SELECT COUNT(*) (one per distinct table)
//!            │
//!   cancelled before the count ─────────▶ not_evaluated, nothing refused
//!   in range ───────────────────────────▶ in_range (recorded)
//!   out of range / count failed:
//!      warning ─────────────────────────▶ a `volume_warnings` line; build as usual
//!      error ───────────────────────────▶ the OUTPUT model is refused at its dispatch
//! ```
//!
//! Only the product's output model is refused. Upstream models in the chain
//! still build: the check protects the delivered output, not the chain.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use rocky_core::product::spec::{VolumeSeverity, VolumeVerdict};
use rocky_core::traits::WarehouseAdapter;
use tracing::warn;

use super::product::ProductSourceVolume;
use crate::output::{RunOutput, SourceVolumeCheckOutput};

/// The source ranges this run judges at its start.
#[derive(Debug, Default)]
pub(crate) struct SourceVolumes {
    ranges: Vec<ProductSourceVolume>,
}

impl SourceVolumes {
    /// The ranges the products under `root` declare, or none.
    ///
    /// The same gate as [`super::run_volume::ProductVolumes::load`]: empty
    /// when `production` is false (a shadow or branch run) or when the run
    /// has no project root to read `products/` from (a backfill).
    pub(crate) fn load(root: Option<&Path>, production: bool) -> Self {
        let ranges = match root {
            Some(root) if production => super::product::product_source_volumes_in(root),
            Some(_) | None => Vec::new(),
        };
        Self { ranges }
    }

    #[cfg(test)]
    pub(crate) fn from_ranges(ranges: Vec<ProductSourceVolume>) -> Self {
        Self { ranges }
    }

    /// Every range whose product output is `model`, across all products.
    #[cfg(test)]
    fn for_model<'a>(&'a self, model: &'a str) -> impl Iterator<Item = &'a ProductSourceVolume> {
        self.ranges
            .iter()
            .filter(move |range| range.output_model == model)
    }
}

/// One output model refused by an `error` source range.
#[derive(Debug, Clone)]
pub(crate) struct SourceRefusal {
    /// Every breach that refuses the model, one sentence each.
    lines: Vec<String>,
    /// The time the counts behind those breaches took, in milliseconds.
    pub(crate) duration_ms: u64,
}

impl SourceRefusal {
    /// The error the run records as the model's failure.
    pub(crate) fn error(&self) -> anyhow::Error {
        anyhow::anyhow!("{}", self.lines.join("; "))
    }
}

/// The output models refused at their dispatch point, by model name.
#[derive(Debug, Default)]
pub(crate) struct SourceRefusals {
    by_model: BTreeMap<String, SourceRefusal>,
}

impl SourceRefusals {
    /// The refusal for `model`, when an `error` source range refuses it.
    pub(crate) fn get(&self, model: &str) -> Option<&SourceRefusal> {
        self.by_model.get(model)
    }
}

/// What one `COUNT(*)` produced, kept so a table two ranges name is counted
/// once.
#[derive(Debug, Clone)]
struct Counted {
    result: Result<u64, String>,
    duration_ms: u64,
}

/// Count and judge every range whose output model `selected` accepts.
///
/// Pushes one [`SourceVolumeCheckOutput`] per judged range onto
/// `output.source_volumes` and one `output.volume_warnings` line per
/// `warning` breach. Returns the output models an `error` breach refuses.
/// Each distinct table is counted at most once (memoized by its formatted
/// reference), serially.
pub(crate) async fn count_sources(
    volumes: &SourceVolumes,
    selected: impl Fn(&str) -> bool,
    warehouse: &dyn WarehouseAdapter,
    output: &mut RunOutput,
    cancel: &tokio_util::sync::CancellationToken,
) -> SourceRefusals {
    let mut counted: BTreeMap<String, Counted> = BTreeMap::new();
    let mut refusals = SourceRefusals::default();
    for entry in volumes
        .ranges
        .iter()
        .filter(|entry| selected(&entry.output_model))
    {
        let range = &entry.range;
        let severity = range.severity();
        let taken = if cancel.is_cancelled() {
            None
        } else {
            count_once(warehouse, &range.table, &mut counted, cancel).await
        };
        let cancelled = taken.is_none();
        let (row_count, reason, duration_ms) = match taken {
            None => (
                None,
                Some("the run was cancelled before the count finished".to_string()),
                0,
            ),
            Some(Counted {
                result: Ok(rows),
                duration_ms,
            }) => (Some(rows), None, duration_ms),
            Some(Counted {
                result: Err(reason),
                duration_ms,
            }) => (None, Some(reason), duration_ms),
        };
        let verdict = row_count.map_or(VolumeVerdict::NotEvaluated, |rows| range.judge(rows));
        let mut check = SourceVolumeCheckOutput {
            product: entry.product.clone(),
            model: entry.output_model.clone(),
            table: range.table.clone(),
            row_count,
            min_rows: range.min_rows,
            max_rows: range.max_rows,
            severity,
            verdict,
            reason,
        };
        let breach = if cancelled {
            None
        } else {
            describe_breach(&check)
        };
        match (breach, severity) {
            (None, _) => {}
            (Some(line), VolumeSeverity::Warning) => {
                warn!(model = %check.model, product = %check.product, "{line}");
                output.volume_warnings.push(line);
            }
            (Some(line), VolumeSeverity::Error) => {
                let why = check.reason.take();
                check.reason = Some(match why {
                    Some(why) => format!(
                        "{why}; the output model '{}' is refused: it is not built this run",
                        check.model
                    ),
                    None => format!(
                        "the output model '{}' is refused: it is not built this run",
                        check.model
                    ),
                });
                let refusal =
                    refusals
                        .by_model
                        .entry(check.model.clone())
                        .or_insert(SourceRefusal {
                            lines: Vec::new(),
                            duration_ms: 0,
                        });
                refusal.lines.push(line);
                refusal.duration_ms = refusal.duration_ms.saturating_add(duration_ms);
            }
        }
        output.source_volumes.push(check);
    }
    refusals
}

/// Count `table` once per run: a table already counted reuses its result.
///
/// `None` when the run is cancelled while the count runs: the query is
/// dropped, and nothing is memoized.
async fn count_once(
    warehouse: &dyn WarehouseAdapter,
    table: &str,
    counted: &mut BTreeMap<String, Counted>,
    cancel: &tokio_util::sync::CancellationToken,
) -> Option<Counted> {
    let table_ref = format_source_ref(warehouse, table);
    // A reference the dialect refused is keyed by the name as written, so
    // the same refusal is not asked twice either.
    let key = match &table_ref {
        Ok(table_ref) => table_ref.clone(),
        Err(_) => format!("<unformatted>{table}"),
    };
    if let Some(found) = counted.get(&key) {
        return Some(found.clone());
    }
    let started = Instant::now();
    let result = match table_ref {
        Ok(table_ref) => tokio::select! {
            result = count_rows(warehouse, &table_ref) => result,
            () = cancel.cancelled() => return None,
        },
        Err(reason) => Err(reason),
    };
    let found = Counted {
        result,
        duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
    };
    counted.insert(key, found.clone());
    Some(found)
}

/// The dialect's quoted reference for a `catalog.schema.table` source.
fn format_source_ref(warehouse: &dyn WarehouseAdapter, table: &str) -> Result<String, String> {
    let parts: Vec<&str> = table.split('.').collect();
    let [catalog, schema, name] = parts.as_slice() else {
        return Err(format!(
            "the source '{table}' is not a three-part table; not counted"
        ));
    };
    warehouse
        .dialect()
        .format_table_ref(catalog, schema, name)
        .map_err(|e| format!("could not address the source table: {e}"))
}

/// `SELECT COUNT(*)` on a formatted reference. The error is the reason the
/// verdict is `not_evaluated`.
async fn count_rows(warehouse: &dyn WarehouseAdapter, table_ref: &str) -> Result<u64, String> {
    let result = warehouse
        .execute_query(&format!("SELECT COUNT(*) FROM {table_ref}"))
        .await
        .map_err(|e| format!("the count failed: {e}"))?;
    rocky_core::checks::cell_as_u64(result.rows.first().and_then(|row| row.first()))
        .ok_or_else(|| "the count returned no readable number".to_string())
}

/// The sentence for an out-of-range count or a count that could not be
/// taken, or `None` when the count is in range.
fn describe_breach(check: &SourceVolumeCheckOutput) -> Option<String> {
    let what = match (check.verdict, check.row_count) {
        (VolumeVerdict::InRange, _) => return None,
        (VolumeVerdict::BelowMin, Some(count)) => {
            format!("has {count} row(s), below min_rows = {}", check.min_rows?)
        }
        (VolumeVerdict::AboveMax, Some(count)) => {
            format!("has {count} row(s), above max_rows = {}", check.max_rows?)
        }
        (VolumeVerdict::BelowMin | VolumeVerdict::AboveMax, None) => return None,
        (VolumeVerdict::NotEvaluated, _) => format!(
            "could not be counted ({})",
            check.reason.as_deref().unwrap_or("no reason recorded")
        ),
    };
    Some(format!(
        "source table '{}' {what} at run start (product '{}', output model '{}', \
         [[product.source.volume]] severity = \"{}\")",
        check.table,
        check.product,
        check.model,
        check.severity.as_str()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{AdapterCall, RecordingWarehouseAdapter};
    use rocky_core::product::spec::SourceVolumeSpec;

    type Bounds = (Option<u64>, Option<u64>, Option<VolumeSeverity>);

    fn range(product: &str, model: &str, table: &str, bounds: Bounds) -> ProductSourceVolume {
        ProductSourceVolume {
            product: product.into(),
            output_model: model.into(),
            range: SourceVolumeSpec {
                table: table.into(),
                min_rows: bounds.0,
                max_rows: bounds.1,
                severity: bounds.2,
            },
        }
    }

    fn counts(key: &str) -> Vec<String> {
        crate::testing::recorder(key)
            .calls()
            .into_iter()
            .filter_map(|call| match call {
                AdapterCall::ExecuteQuery(sql) if sql.starts_with("SELECT COUNT(*)") => Some(sql),
                AdapterCall::ExecuteQuery(_)
                | AdapterCall::ExecuteStatement(_)
                | AdapterCall::DescribeTable(_)
                | AdapterCall::ListTables { .. } => None,
            })
            .collect()
    }

    /// Two products naming one source table: exactly one `COUNT(*)`, and a
    /// row for each range. The recording adapter refuses every query, so
    /// the count is unreadable: refused at `error`, a warning at `warning`.
    #[tokio::test]
    async fn one_count_per_distinct_source_and_an_unreadable_count_follows_severity() {
        let key = format!("source-volume-{}", uuid::Uuid::new_v4());
        let warehouse = RecordingWarehouseAdapter::new(&key);
        let volumes = SourceVolumes::from_ranges(vec![
            range(
                "p1",
                "out1",
                "c.s.orders",
                (Some(1), None, Some(VolumeSeverity::Error)),
            ),
            range("p2", "out2", "c.s.orders", (Some(1), None, None)),
        ]);
        let mut output = RunOutput::new(String::new(), 0, 1);
        let refusals = count_sources(
            &volumes,
            |_| true,
            &warehouse,
            &mut output,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;

        assert_eq!(counts(&key).len(), 1, "{:?}", counts(&key));
        assert_eq!(output.source_volumes.len(), 2);
        for row in &output.source_volumes {
            assert_eq!(row.verdict, VolumeVerdict::NotEvaluated, "{row:?}");
            assert!(row.row_count.is_none());
            assert!(
                row.reason
                    .as_deref()
                    .is_some_and(|r| r.contains("the count failed")),
                "{row:?}"
            );
        }
        let refused = refusals.get("out1").expect("an error range refuses");
        let message = format!("{:#}", refused.error());
        assert!(message.contains("could not be counted"), "{message}");
        assert!(message.contains("c.s.orders"), "{message}");
        assert!(
            refusals.get("out2").is_none(),
            "a warning range never refuses"
        );
        assert_eq!(
            output.volume_warnings.len(),
            1,
            "{:?}",
            output.volume_warnings
        );
        assert!(output.volume_warnings[0].contains("p2"));
    }

    #[tokio::test]
    async fn a_cancelled_run_counts_nothing_and_refuses_nothing() {
        let key = format!("source-volume-{}", uuid::Uuid::new_v4());
        let warehouse = RecordingWarehouseAdapter::new(&key);
        let volumes = SourceVolumes::from_ranges(vec![range(
            "p",
            "out",
            "c.s.t",
            (Some(1), None, Some(VolumeSeverity::Error)),
        )]);
        let cancel = tokio_util::sync::CancellationToken::new();
        cancel.cancel();
        let mut output = RunOutput::new(String::new(), 0, 1);
        let refusals = count_sources(&volumes, |_| true, &warehouse, &mut output, &cancel).await;
        assert!(counts(&key).is_empty());
        assert!(refusals.get("out").is_none());
        assert!(output.volume_warnings.is_empty());
        assert_eq!(output.source_volumes.len(), 1);
        assert_eq!(
            output.source_volumes[0].verdict,
            VolumeVerdict::NotEvaluated
        );
    }

    #[tokio::test]
    async fn an_unselected_output_is_not_counted() {
        let key = format!("source-volume-{}", uuid::Uuid::new_v4());
        let warehouse = RecordingWarehouseAdapter::new(&key);
        let volumes = SourceVolumes::from_ranges(vec![range(
            "p",
            "out",
            "c.s.t",
            (Some(1), None, Some(VolumeSeverity::Error)),
        )]);
        let mut output = RunOutput::new(String::new(), 0, 1);
        let refusals = count_sources(
            &volumes,
            |model| model != "out",
            &warehouse,
            &mut output,
            &tokio_util::sync::CancellationToken::new(),
        )
        .await;
        assert!(counts(&key).is_empty());
        assert!(refusals.get("out").is_none());
        assert!(output.source_volumes.is_empty());
    }

    #[test]
    fn the_lookup_returns_every_range_for_a_model() {
        let volumes = SourceVolumes::from_ranges(vec![
            range("p1", "out", "c.s.a", (Some(1), None, None)),
            range("p2", "out", "c.s.b", (None, Some(9), None)),
            range("p3", "other", "c.s.a", (Some(1), None, None)),
        ]);
        let tables: Vec<&str> = volumes
            .for_model("out")
            .map(|r| r.range.table.as_str())
            .collect();
        assert_eq!(tables, ["c.s.a", "c.s.b"]);
    }

    #[test]
    fn a_non_production_run_loads_no_range() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir_all(dir.path().join("products")).unwrap();
        std::fs::write(
            dir.path().join("products/p.toml"),
            "[product]\nname = \"p\"\nintent = \"x\"\n\n\
             [product.source]\ntables = [\"c.s.t\"]\n\n\
             [[product.source.volume]]\ntable = \"c.s.t\"\nmin_rows = 1\n\n\
             [product.output]\ngrain = [\"x\"]\n\
             columns = [{ name = \"x\", type = \"Int32\", nullable = false }]\n\n\
             [product.trust]\nagent = \"propose_only\"\n",
        )
        .unwrap();
        assert!(
            SourceVolumes::load(Some(dir.path()), false)
                .ranges
                .is_empty()
        );
        assert!(SourceVolumes::load(None, true).ranges.is_empty());
        let loaded = SourceVolumes::load(Some(dir.path()), true);
        assert_eq!(loaded.ranges.len(), 1);
        assert_eq!(loaded.ranges[0].output_model, "p");
    }

    /// `rocky run` end to end on DuckDB (no credentials). The source table
    /// is `w.main.src` (the database file is `w.duckdb`).
    #[cfg(feature = "duckdb")]
    mod through_run {
        use rocky_core::product::spec::VolumeVerdict;
        use rocky_core::state::{ModelExecution, RunRecord, RunStatus, StateStore};
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        use super::super::super::run::{
            DeferOptions, PartitionRunOptions, SkipGateConfig, SkipRunOptions, run,
        };

        const FULL_REFRESH: &str =
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n";
        /// Three rows, read from no source.
        const VOL_SQL: &str = "SELECT x FROM (VALUES (1), (2), (3)) AS t(x)\n";
        /// Three source rows.
        const SEED_SRC: &str =
            "CREATE TABLE main.src AS SELECT * FROM (VALUES (1), (2), (3)) AS t(id)";
        /// A source big enough that its count takes measurable time.
        const SEED_SLOW_SRC: &str = "CREATE VIEW main.src AS SELECT range AS id \
             FROM range(30000000) WHERE hash(range) % 7 <> 0";

        /// The product spec for output model `vol` with one source range on
        /// `w.main.src`, and a daily deadline so a delivery is stamped.
        fn spec(range: &str) -> String {
            format!(
                "[product]\nname = \"vol_product\"\nintent = \"three rows\"\n\
                 deliver_by = \"0 6 * * *\"\n\n\
                 [product.source]\ntables = [\"w.main.src\"]\n\n\
                 [[product.source.volume]]\ntable = \"w.main.src\"\n{range}\n\n\
                 [product.output]\nmodel = \"vol\"\ngrain = [\"x\"]\n\
                 columns = [{{ name = \"x\", type = \"Int32\", nullable = false }}]\n\n\
                 [product.trust]\nagent = \"propose_only\"\n"
            )
        }

        fn chain() -> Vec<(&'static str, &'static str, &'static str)> {
            vec![
                ("stage", VOL_SQL, FULL_REFRESH),
                (
                    "vol",
                    "SELECT x FROM main.stage\n",
                    "depends_on = [\"stage\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
                (
                    "down",
                    "SELECT x FROM main.vol\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ]
        }

        struct Ran {
            result: anyhow::Result<()>,
            record: RunRecord,
            output: serde_json::Value,
            tmp: tempfile::TempDir,
        }

        impl Ran {
            fn executions(&self, model: &str) -> Vec<&ModelExecution> {
                self.record
                    .models_executed
                    .iter()
                    .filter(|e| e.rocky_model_name() == model)
                    .collect()
            }

            fn built(&self, table: &str) -> bool {
                self.output["materializations"]
                    .as_array()
                    .is_some_and(|mats| {
                        mats.iter().any(|m| {
                            m["asset_key"].as_array().and_then(|k| k.last()) == Some(&table.into())
                        })
                    })
            }

            fn source_rows(&self) -> Vec<serde_json::Value> {
                self.output["source_volumes"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
            }

            async fn query_i64(&self, sql: &str) -> Vec<i64> {
                let adapter =
                    DuckDbWarehouseAdapter::open(&self.tmp.path().join("w.duckdb")).unwrap();
                adapter
                    .execute_query(sql)
                    .await
                    .unwrap()
                    .rows
                    .iter()
                    .map(|row| {
                        row[0]
                            .as_i64()
                            .or_else(|| row[0].as_str().and_then(|s| s.parse().ok()))
                            .unwrap()
                    })
                    .collect()
            }
        }

        async fn seed(db: &std::path::Path, statements: &[&str]) {
            let adapter = DuckDbWarehouseAdapter::open(db).expect("seed open");
            for sql in statements {
                adapter.execute_statement(sql).await.expect(sql);
            }
        }

        async fn run_with(
            spec_text: &str,
            models: &[(&str, &str, &str)],
            seeds: &[&str],
            shadow: Option<&rocky_core::shadow::ShadowConfig>,
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
            seed(&dir.join("w.duckdb"), seeds).await;
            let config_path = dir.join("rocky.toml");
            std::fs::write(
                &config_path,
                format!(
                    "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                     [state]\nbackend = \"local\"\n\n\
                     [pipeline.tx]\ntype = \"transformation\"\nmodels = '{}'\n\n\
                     [pipeline.tx.target]\nadapter = \"default\"\n",
                    dir.join("w.duckdb").display(),
                    models_dir.join("**").display(),
                ),
            )
            .unwrap();
            let state_path = dir.join("state.redb");
            let run_id = format!("source-volume-{}", uuid::Uuid::new_v4());
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
                tmp,
            }
        }

        #[tokio::test]
        async fn an_in_range_source_is_recorded_and_the_output_delivers() {
            let ran = run_with(
                &spec("min_rows = 1\nmax_rows = 10\nseverity = \"error\""),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[SEED_SRC],
                None,
            )
            .await;
            ran.result.as_ref().expect("the run succeeds");
            assert_eq!(ran.record.status, RunStatus::Success);
            let rows = ran.source_rows();
            assert_eq!(rows.len(), 1, "{rows:?}");
            assert_eq!(rows[0]["table"], "w.main.src");
            assert_eq!(rows[0]["product"], "vol_product");
            assert_eq!(rows[0]["model"], "vol");
            assert_eq!(rows[0]["row_count"], 3);
            assert_eq!(rows[0]["verdict"], "in_range");
            assert_eq!(rows[0]["severity"], "error");
            assert_eq!(ran.record.source_volumes.len(), 1);
            assert_eq!(ran.record.source_volumes[0].row_count, Some(3));
            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1);
            assert_eq!(executions[0].status, "success");
            assert!(executions[0].due_at.is_some(), "a delivery is stamped");
            assert!(
                ran.output.get("volume_warnings").is_none(),
                "{}",
                ran.output
            );
        }

        #[tokio::test]
        async fn a_warning_breach_warns_and_the_output_still_delivers() {
            let ran = run_with(
                &spec("min_rows = 4"),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[SEED_SRC],
                None,
            )
            .await;
            ran.result.as_ref().expect("a warning never fails the run");
            assert_eq!(ran.record.status, RunStatus::Success);
            let warnings = ran.output["volume_warnings"].as_array().expect("warnings");
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            let line = warnings[0].as_str().unwrap();
            assert!(
                line.contains("'w.main.src' has 3 row(s), below min_rows = 4"),
                "{line}"
            );
            assert_eq!(ran.source_rows()[0]["verdict"], "below_min");
            let executions = ran.executions("vol");
            assert_eq!(executions[0].status, "success");
            assert!(
                executions[0].due_at.is_some(),
                "delivery is recorded as usual"
            );
        }

        /// An `error` breach refuses the output model before any write: the
        /// table keeps its old rows, no materialization is pushed, one
        /// failed execution is recorded with the count's time, the
        /// downstream model is withheld, and the upstream model still
        /// builds.
        #[tokio::test]
        async fn an_error_breach_refuses_the_output_before_its_write() {
            let ran = run_with(
                &spec("min_rows = 1_000_000_000\nseverity = \"error\""),
                &chain(),
                &[SEED_SLOW_SRC, "CREATE TABLE main.vol AS SELECT 99 AS x"],
                None,
            )
            .await;
            assert!(ran.result.is_err(), "an error breach fails the run");
            let errors = ran.output["errors"].as_array().expect("errors");
            assert_eq!(errors.len(), 1, "{errors:?}");
            assert!(
                errors[0]["error"]
                    .as_str()
                    .is_some_and(|m| m.contains("'w.main.src' has")
                        && m.contains("below min_rows = 1000000000")),
                "{errors:?}"
            );
            assert_eq!(ran.record.status, RunStatus::PartialFailure);
            assert_eq!(ran.output["status"], "PartialFailure");

            assert!(ran.built("stage"), "the upstream model still builds");
            assert!(
                !ran.built("vol"),
                "no materialization for the refused output"
            );
            assert!(!ran.built("down"), "the downstream model is withheld");
            assert_eq!(
                ran.query_i64("SELECT x FROM main.vol").await,
                vec![99],
                "the output table is unchanged"
            );

            let executions = ran.executions("vol");
            assert_eq!(executions.len(), 1, "{:?}", ran.record.models_executed);
            assert_eq!(executions[0].status, "failed");
            assert_eq!(executions[0].due_at, None, "a refusal is no delivery");
            assert!(
                executions[0].duration_ms > 0,
                "the refusal carries the count's time: {:?}",
                executions[0]
            );
            assert!(ran.executions("down").iter().all(|e| e.status != "success"));

            let rows = ran.source_rows();
            assert_eq!(rows[0]["verdict"], "below_min", "{rows:?}");
            assert!(
                rows[0]["reason"]
                    .as_str()
                    .is_some_and(|r| r.contains("is refused")),
                "{rows:?}"
            );
            assert_eq!(ran.record.source_volumes.len(), 1);
            assert_eq!(
                ran.record.source_volumes[0].verdict,
                VolumeVerdict::BelowMin
            );
            assert!(ran.output.get("volume_warnings").is_none());
        }

        /// A source that does not exist cannot be counted: at `error` the
        /// output is refused, at `warning` it is a warning and the output
        /// builds.
        #[tokio::test]
        async fn an_unreadable_source_refuses_at_error_and_warns_at_warning() {
            let refused = run_with(
                &spec("min_rows = 0\nseverity = \"error\""),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[],
                None,
            )
            .await;
            assert!(refused.result.is_err());
            assert!(!refused.built("vol"));
            let rows = refused.source_rows();
            assert_eq!(rows[0]["verdict"], "not_evaluated", "{rows:?}");
            assert!(rows[0].get("row_count").is_none());
            assert!(
                rows[0]["reason"]
                    .as_str()
                    .is_some_and(|r| r.contains("the count failed")),
                "{rows:?}"
            );
            assert_eq!(refused.executions("vol")[0].status, "failed");

            let warned = run_with(
                &spec("min_rows = 0"),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[],
                None,
            )
            .await;
            warned
                .result
                .as_ref()
                .expect("a warning never fails the run");
            assert!(warned.built("vol"));
            let warnings = warned.output["volume_warnings"]
                .as_array()
                .expect("warnings");
            assert!(
                warnings[0]
                    .as_str()
                    .is_some_and(|w| w.contains("could not be counted")),
                "{warnings:?}"
            );
        }

        #[tokio::test]
        async fn a_shadow_run_counts_nothing() {
            let shadow = rocky_core::shadow::ShadowConfig::default();
            let ran = run_with(
                &spec("min_rows = 1_000\nseverity = \"error\""),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[SEED_SRC],
                Some(&shadow),
            )
            .await;
            ran.result.as_ref().expect("a shadow run is not judged");
            assert!(ran.output.get("source_volumes").is_none(), "{}", ran.output);
            assert!(ran.output.get("volume_warnings").is_none());
            assert!(ran.record.source_volumes.is_empty());
        }

        /// `rocky product status` reads the source count back from the run
        /// record, judged against the range declared now.
        #[tokio::test]
        async fn product_status_reads_the_source_count_from_the_run_record() {
            let ran = run_with(
                &spec("min_rows = 1_000\nseverity = \"error\""),
                &[("vol", VOL_SQL, FULL_REFRESH)],
                &[SEED_SRC],
                None,
            )
            .await;
            assert!(ran.result.is_err());
            let root = ran.tmp.path();
            let status = super::super::super::product::product_status_in(
                root,
                Some(&root.join("state.redb")),
                "vol_product",
            )
            .expect("status");
            assert_eq!(status.source_volumes.len(), 1, "{status:?}");
            let source = &status.source_volumes[0];
            assert_eq!(source.table, "w.main.src");
            assert_eq!(source.row_count, Some(3));
            assert_eq!(source.min_rows, Some(1_000));
            assert_eq!(source.verdict, VolumeVerdict::BelowMin);
            assert_eq!(source.run_id.as_deref(), Some(ran.record.run_id.as_str()));
            assert!(status.volume.is_none(), "no output range is declared");
        }

        // ----- through `execute_models` -----

        /// The models of a layer test: `a`, `vol` and `b` share layer 0 and
        /// `down` reads `vol` in layer 1.
        fn layer_models() -> Vec<(&'static str, &'static str, &'static str)> {
            vec![
                ("a", "SELECT 1 AS y\n", FULL_REFRESH),
                ("vol", VOL_SQL, FULL_REFRESH),
                ("b", "SELECT 2 AS z\n", FULL_REFRESH),
                (
                    "down",
                    "SELECT x FROM main.vol\n",
                    "depends_on = [\"vol\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                     [target]\ncatalog = \"\"\nschema = \"main\"\n",
                ),
            ]
        }

        struct Layered {
            output: crate::output::RunOutput,
            result: anyhow::Result<super::super::super::run::GovernanceSnapshot>,
        }

        struct LayerRun<'a> {
            models: &'a [(&'a str, &'a str, &'a str)],
            range: &'a str,
            parallel: usize,
            contain_failures: bool,
            hooks: Option<&'a rocky_core::hooks::HookRegistry>,
            gate: SkipGateConfig,
            state: Option<&'a StateStore>,
        }

        async fn run_layers(root: &std::path::Path, opts: LayerRun<'_>) -> Layered {
            let models_dir = root.join("models");
            std::fs::create_dir_all(&models_dir).unwrap();
            for (name, sql, toml) in opts.models {
                std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
                std::fs::write(models_dir.join(format!("{name}.toml")), toml).unwrap();
            }
            std::fs::create_dir_all(root.join("products")).unwrap();
            std::fs::write(root.join("products/vol_product.toml"), spec(opts.range)).unwrap();
            let db = root.join("w.duckdb");
            if !db.exists() {
                seed(&db, &[SEED_SRC]).await;
            }
            let adapter = DuckDbWarehouseAdapter::open(&db)
                .expect("open duckdb")
                .with_concurrent_for_test(true);
            let partition_opts = PartitionRunOptions {
                parallel: u32::try_from(opts.parallel).expect("small"),
                ..Default::default()
            };
            let mut output = crate::output::RunOutput::new(String::new(), 0, opts.parallel);
            let resilience = rocky_core::config::ResilienceConfig {
                contain_failures: opts.contain_failures,
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
                opts.state,
                &partition_opts,
                "source-volume-layers",
                None,
                None,
                &mut output,
                opts.hooks,
                opts.hooks.map(|_| "tx"),
                &rocky_core::config::SchemaCacheConfig::default(),
                false,
                None,
                &defer,
                opts.gate,
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
            Layered { output, result }
        }

        fn built(output: &crate::output::RunOutput, table: &str) -> bool {
            output
                .materializations
                .iter()
                .any(|m| m.asset_key.last().map(String::as_str) == Some(table))
        }

        const ERROR_BREACH: &str = "min_rows = 1_000\nseverity = \"error\"";

        /// A refused model in the middle of a concurrent layer: the other
        /// models of the layer build and report as usual; the refused one is
        /// the layer's failure.
        #[tokio::test]
        async fn the_concurrent_path_refuses_a_model_mid_layer() {
            for contain_failures in [false, true] {
                let tmp = tempfile::TempDir::new().unwrap();
                let models = layer_models();
                let ran = run_layers(
                    tmp.path(),
                    LayerRun {
                        models: &models,
                        range: ERROR_BREACH,
                        parallel: 4,
                        contain_failures,
                        hooks: None,
                        gate: SkipGateConfig::off(),
                        state: None,
                    },
                )
                .await;
                let output = &ran.output;
                let tables: Vec<&str> = output
                    .materializations
                    .iter()
                    .filter_map(|m| m.asset_key.last().map(String::as_str))
                    .collect();
                assert_eq!(
                    tables,
                    ["a", "b"],
                    "the others build, in index order: {contain_failures}"
                );
                assert!(
                    output.failed_attempts.contains_key("vol"),
                    "the refusal keeps its time: {contain_failures}"
                );
                if contain_failures {
                    ran.result.as_ref().expect("a contained failure returns Ok");
                    assert_eq!(output.tables_failed, 1);
                    assert!(output.errors.iter().any(|e| e.asset_key == ["vol"]));
                    assert!(output.contained.iter().any(|c| c.model == "down"));
                } else {
                    let error = ran.result.as_ref().expect_err("the refusal stops the run");
                    assert!(
                        format!("{error:#}").contains("below min_rows = 1000"),
                        "{error:#}"
                    );
                    assert_eq!(output.fail_fast_model.as_deref(), Some("vol"));
                }
            }
        }

        struct Fired {
            before_model_run: Vec<String>,
            after_model_run: Vec<String>,
            model_error: Vec<String>,
        }

        async fn run_with_hooks(parallel: usize, range: &str) -> Fired {
            let tmp = tempfile::TempDir::new().unwrap();
            let root = tmp.path();
            let mut hooks_cfg = rocky_core::hooks::HooksConfig::default();
            for event in [
                "on_before_model_run",
                "on_after_model_run",
                "on_model_error",
            ] {
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
            let models = layer_models();
            let _ = run_layers(
                root,
                LayerRun {
                    models: &models,
                    range,
                    parallel,
                    contain_failures: true,
                    hooks: Some(&registry),
                    gate: SkipGateConfig::off(),
                    state: None,
                },
            )
            .await;
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
                before_model_run: models("on_before_model_run"),
                after_model_run: models("on_after_model_run"),
                model_error: models("on_model_error"),
            }
        }

        /// A refusal fires `model_error` once and neither model-run hook, on
        /// the serial path and the concurrent one. `a` proves both hooks are
        /// wired.
        #[tokio::test]
        async fn a_refusal_fires_only_model_error() {
            for parallel in [1, 4] {
                let fired = run_with_hooks(parallel, ERROR_BREACH).await;
                for (event, models) in [
                    ("before", &fired.before_model_run),
                    ("after", &fired.after_model_run),
                ] {
                    assert!(
                        models.iter().any(|m| m == "a"),
                        "parallel {parallel}: the {event} hook is wired: {models:?}"
                    );
                    assert!(
                        !models.iter().any(|m| m == "vol"),
                        "parallel {parallel}: {event} fired for a refused model: {models:?}"
                    );
                }
                assert_eq!(
                    fired.model_error,
                    vec!["vol".to_string()],
                    "parallel {parallel}: model_error fires once, for vol"
                );
            }
        }

        /// The skip gate skips the output on the second run; its source is
        /// still counted and recorded.
        #[tokio::test]
        async fn a_skipped_output_keeps_its_source_count() {
            let tmp = tempfile::TempDir::new().unwrap();
            let state = StateStore::open(&tmp.path().join("state.redb")).unwrap();
            let gate = SkipGateConfig {
                feature_enabled: true,
                force_rebuild: false,
                rowcount_fallback: true,
                lag_tolerance_seconds: 0,
                shadow_or_branch: false,
                full_refresh: false,
            };
            let models = [("vol", "SELECT id AS x FROM main.src\n", FULL_REFRESH)];
            let layer_run = || LayerRun {
                models: &models,
                range: "min_rows = 1",
                parallel: 1,
                contain_failures: false,
                hooks: None,
                gate,
                state: Some(&state),
            };
            let first = run_layers(tmp.path(), layer_run()).await;
            first.result.as_ref().expect("the first run builds");
            assert!(built(&first.output, "vol"));
            let started = chrono::DateTime::<chrono::Utc>::from_timestamp(1_700_000_000, 0)
                .expect("valid timestamp");
            let record = first.output.to_run_record(
                "run-1",
                started,
                started,
                String::new(),
                rocky_core::state::RunTrigger::Manual,
                RunStatus::Success,
                crate::output::RunRecordAudit::test_sentinels(),
            );
            state.record_run(&record).expect("record run");

            let second = run_layers(tmp.path(), layer_run()).await;
            second.result.as_ref().expect("the second run skips");
            assert!(!built(&second.output, "vol"), "the gate skips the output");
            assert_eq!(second.output.tables_skipped, 1);
            assert_eq!(second.output.source_volumes.len(), 1);
            assert_eq!(second.output.source_volumes[0].row_count, Some(3));
            assert_eq!(
                second.output.source_volumes[0].verdict,
                VolumeVerdict::InRange
            );
        }
    }
}
