//! Whether a pipeline is safe to run a second time.
//!
//! A pipeline is *rerun-safe* when a second run of the same work cannot
//! corrupt the target, **even while a first run may still be writing to the
//! warehouse**. A sequential re-run after the first run stopped is the easy
//! case; two overlapping runs are the bar. The scheduler will retry, re-run
//! and fail over only pipelines this module classifies as
//! [`RerunSafety::Safe`].
//!
//! The answer depends on the pipeline type, each materialization strategy it
//! writes with, the target warehouse and, for upserts, whether a unique key
//! exists. Anything this module cannot prove safe is
//! [`RerunSafety::NotSafe`] with a reason: the classifier fails closed.
//!
//! The classification judges the steady-state statements a run executes. It
//! does not audit the first-run bootstrap or the drift rebuild paths.

use serde::{Deserialize, Serialize};

use rocky_ir::MaterializationStrategy;

use crate::config::{AdapterConfig, PipelineConfig, QuarantineConfig, QuarantineMode};
use crate::cost::WarehouseType;

/// The rerun-safety class of one pipeline or one materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum RerunSafety {
    /// A second run is safe, even while a first run is still writing.
    #[serde(rename = "rerun_safe")]
    Safe,
    /// A second run may duplicate, lose or corrupt data. `reason` says why.
    #[serde(rename = "not_rerun_safe")]
    NotSafe {
        /// Why the second run is not safe, in one sentence.
        reason: String,
    },
}

impl RerunSafety {
    fn not_safe(reason: impl Into<String>) -> Self {
        Self::NotSafe {
            reason: reason.into(),
        }
    }

    /// The class name as it appears in output: `rerun_safe` or
    /// `not_rerun_safe`.
    #[must_use]
    pub fn class_name(&self) -> &'static str {
        match self {
            Self::Safe => "rerun_safe",
            Self::NotSafe { .. } => "not_rerun_safe",
        }
    }

    /// Whether this is [`RerunSafety::Safe`].
    #[must_use]
    pub fn is_safe(&self) -> bool {
        match self {
            Self::Safe => true,
            Self::NotSafe { .. } => false,
        }
    }
}

/// The pipeline type a materialization is written by. It matters for
/// [`MaterializationStrategy::Incremental`]: a replication pipeline keeps its
/// watermark in the Rocky state store, a transformation model reads it from
/// the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriterPipeline {
    /// A `type = "replication"` pipeline.
    Replication,
    /// A model of a `type = "transformation"` pipeline.
    Transformation,
}

/// The target warehouse, as far as rerun safety is concerned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetWarehouse {
    /// DuckDB on a local file or in memory, with no attached catalog. Only
    /// one process can open a DuckDB file for writing, so a second run
    /// cannot overlap the first one: it fails to open the database.
    DuckDbSingleWriter,
    /// DuckDB that attaches other catalogs (Iceberg, DuckLake) or connects
    /// to MotherDuck (`md:`). More than one process can write there.
    DuckDbShared,
    /// Databricks (Delta Lake, optimistic concurrency control).
    Databricks,
    /// Snowflake.
    Snowflake,
    /// BigQuery.
    BigQuery,
    /// Any other adapter type, or a target adapter that is not declared.
    /// Rerun safety is not verified there, so every write is not safe.
    Unverified {
        /// The adapter `type`, or a description of what is missing.
        adapter_type: String,
    },
}

impl TargetWarehouse {
    /// Classify the `[adapter.*]` block a pipeline writes to. `None` (the
    /// pipeline names an adapter that is not declared) is
    /// [`TargetWarehouse::Unverified`].
    #[must_use]
    pub fn from_adapter(adapter: Option<&AdapterConfig>) -> Self {
        let Some(adapter) = adapter else {
            return Self::Unverified {
                adapter_type: "an undeclared adapter".to_string(),
            };
        };
        let Some(warehouse) = WarehouseType::from_adapter_type(&adapter.adapter_type) else {
            return Self::Unverified {
                adapter_type: adapter.adapter_type.clone(),
            };
        };
        match warehouse {
            WarehouseType::DuckDb => {
                let motherduck = adapter.path.as_ref().is_some_and(|path| {
                    let path = path.expose().trim_start();
                    path.starts_with("md:") || path.starts_with("motherduck:")
                });
                if motherduck || !adapter.attach.is_empty() {
                    Self::DuckDbShared
                } else {
                    Self::DuckDbSingleWriter
                }
            }
            WarehouseType::Databricks => Self::Databricks,
            WarehouseType::Snowflake => Self::Snowflake,
            WarehouseType::BigQuery => Self::BigQuery,
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::DuckDbSingleWriter | Self::DuckDbShared => "DuckDB",
            Self::Databricks => "Databricks",
            Self::Snowflake => "Snowflake",
            Self::BigQuery => "BigQuery",
            Self::Unverified { adapter_type } => adapter_type,
        }
    }
}

/// What a pipeline writes, resolved by the caller from files and settings
/// this crate does not read.
#[derive(Debug, Clone)]
pub enum ResolvedWrites {
    /// A replication pipeline: every strategy a table can resolve to (the
    /// pipeline default and each `table_overrides` strategy). An `Err` is a
    /// strategy the run refuses or does not recognize.
    Strategies(Vec<Result<MaterializationStrategy, String>>),
    /// A transformation pipeline: each model it builds, by name.
    Models(Vec<(String, MaterializationStrategy)>),
    /// The writes could not be resolved, for example because the models
    /// directory is missing or a model fails to load.
    Unresolved {
        /// Why the writes are unknown.
        reason: String,
    },
    /// The pipeline type needs nothing resolved (quality, snapshot, load).
    NotNeeded,
}

/// Classify a pipeline. A pipeline is safe only when every materialization
/// it writes is safe; a transformation pipeline that builds no models, and
/// any write that cannot be resolved, is not safe.
#[must_use]
pub fn classify_pipeline(
    pipeline: &PipelineConfig,
    target: &TargetWarehouse,
    writes: &ResolvedWrites,
) -> RerunSafety {
    match pipeline {
        PipelineConfig::Replication(_) => match writes {
            ResolvedWrites::Strategies(strategies) => {
                classify_strategies(strategies, WriterPipeline::Replication, target)
            }
            ResolvedWrites::Unresolved { reason } => RerunSafety::not_safe(reason.clone()),
            ResolvedWrites::Models(_) | ResolvedWrites::NotNeeded => {
                RerunSafety::not_safe("the replication strategies were not resolved")
            }
        },
        PipelineConfig::Transformation(_) => match writes {
            ResolvedWrites::Models(models) => classify_models(models, target),
            ResolvedWrites::Unresolved { reason } => RerunSafety::not_safe(reason.clone()),
            ResolvedWrites::Strategies(_) | ResolvedWrites::NotNeeded => {
                RerunSafety::not_safe("the transformation models were not resolved")
            }
        },
        PipelineConfig::Quality(quality) => classify_quality(quality.checks.quarantine.as_ref()),
        PipelineConfig::Snapshot(_) => RerunSafety::not_safe(
            "a snapshot pipeline closes old versions and inserts new ones in separate \
             statements with no transaction, so two runs can record a change twice",
        ),
        PipelineConfig::Load(_) => RerunSafety::not_safe(
            "a load pipeline re-ingests every file on each run, so a second run \
             duplicates the rows",
        ),
    }
}

fn classify_strategies(
    strategies: &[Result<MaterializationStrategy, String>],
    pipeline: WriterPipeline,
    target: &TargetWarehouse,
) -> RerunSafety {
    if strategies.is_empty() {
        return RerunSafety::not_safe("no replication strategy was resolved");
    }
    for strategy in strategies {
        let class = match strategy {
            Ok(strategy) => classify_strategy(strategy, pipeline, target),
            Err(reason) => RerunSafety::not_safe(reason.clone()),
        };
        if !class.is_safe() {
            return class;
        }
    }
    RerunSafety::Safe
}

fn classify_models(
    models: &[(String, MaterializationStrategy)],
    target: &TargetWarehouse,
) -> RerunSafety {
    if models.is_empty() {
        return RerunSafety::not_safe("the pipeline builds no models");
    }
    for (name, strategy) in models {
        if let RerunSafety::NotSafe { reason } =
            classify_strategy(strategy, WriterPipeline::Transformation, target)
        {
            return RerunSafety::not_safe(format!("model '{name}': {reason}"));
        }
    }
    RerunSafety::Safe
}

fn classify_quality(quarantine: Option<&QuarantineConfig>) -> RerunSafety {
    let Some(quarantine) = quarantine.filter(|q| q.enabled) else {
        return RerunSafety::Safe;
    };
    match quarantine.mode {
        QuarantineMode::Tag => {
            RerunSafety::not_safe("quarantine mode \"tag\" rewrites the checked table in place")
        }
        QuarantineMode::Split | QuarantineMode::Drop => RerunSafety::not_safe(
            "quarantine writes its tables in several statements, and overlapping runs \
             are not verified",
        ),
    }
}

/// Classify one materialization written by `pipeline` to `target`.
#[must_use]
pub fn classify_strategy(
    strategy: &MaterializationStrategy,
    pipeline: WriterPipeline,
    target: &TargetWarehouse,
) -> RerunSafety {
    if let TargetWarehouse::Unverified { adapter_type } = target {
        return match strategy {
            MaterializationStrategy::Ephemeral => RerunSafety::Safe,
            MaterializationStrategy::FullRefresh
            | MaterializationStrategy::Incremental { .. }
            | MaterializationStrategy::Merge { .. }
            | MaterializationStrategy::View
            | MaterializationStrategy::MaterializedView
            | MaterializationStrategy::DynamicTable { .. }
            | MaterializationStrategy::TimeInterval { .. }
            | MaterializationStrategy::DeleteInsert { .. }
            | MaterializationStrategy::Microbatch { .. }
            | MaterializationStrategy::ContentAddressed { .. }
            | MaterializationStrategy::Snapshot(_) => RerunSafety::not_safe(format!(
                "rerun safety is not verified on adapter type '{adapter_type}'"
            )),
        };
    }
    let warehouse = target.label();
    match strategy {
        MaterializationStrategy::FullRefresh => RerunSafety::Safe,
        MaterializationStrategy::View => RerunSafety::Safe,
        MaterializationStrategy::Ephemeral => RerunSafety::Safe,
        MaterializationStrategy::MaterializedView => match target {
            TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery => RerunSafety::Safe,
            TargetWarehouse::DuckDbSingleWriter
            | TargetWarehouse::DuckDbShared
            | TargetWarehouse::Unverified { .. } => unsupported("a materialized view", warehouse),
        },
        MaterializationStrategy::DynamicTable { .. } => match target {
            TargetWarehouse::Snowflake => RerunSafety::Safe,
            TargetWarehouse::DuckDbSingleWriter
            | TargetWarehouse::DuckDbShared
            | TargetWarehouse::Databricks
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => unsupported("a dynamic table", warehouse),
        },
        MaterializationStrategy::Incremental { unique_key, .. } => match pipeline {
            WriterPipeline::Replication => RerunSafety::not_safe(
                "replication incremental reads its watermark from the Rocky state store, \
                 so a second run before the first records it copies the same rows again",
            ),
            WriterPipeline::Transformation if unique_key.is_empty() => match target {
                TargetWarehouse::DuckDbSingleWriter => RerunSafety::Safe,
                TargetWarehouse::DuckDbShared
                | TargetWarehouse::Databricks
                | TargetWarehouse::Snowflake
                | TargetWarehouse::BigQuery
                | TargetWarehouse::Unverified { .. } => RerunSafety::not_safe(format!(
                    "incremental append with no unique_key: two overlapping runs read the \
                     same MAX of the watermark and both append the new rows on {warehouse}"
                )),
            },
            WriterPipeline::Transformation => classify_upsert(target, warehouse),
        },
        MaterializationStrategy::Merge { unique_key, .. } => {
            if unique_key.is_empty() {
                RerunSafety::not_safe("merge with no unique_key")
            } else {
                classify_upsert(target, warehouse)
            }
        }
        MaterializationStrategy::TimeInterval { .. } => match target {
            TargetWarehouse::DuckDbSingleWriter | TargetWarehouse::Databricks => RerunSafety::Safe,
            TargetWarehouse::DuckDbShared
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => RerunSafety::not_safe(format!(
                "partition replacement by two overlapping runs is not verified on {warehouse}"
            )),
        },
        MaterializationStrategy::DeleteInsert { .. } => match target {
            TargetWarehouse::DuckDbSingleWriter => RerunSafety::Safe,
            TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => RerunSafety::not_safe(format!(
                "delete_insert runs the DELETE and the INSERT as separate statements on \
                 {warehouse}, so two runs can both insert the rows"
            )),
            TargetWarehouse::DuckDbShared => RerunSafety::not_safe(
                "delete_insert by two overlapping runs on a shared DuckDB catalog is not \
                 verified",
            ),
        },
        MaterializationStrategy::Microbatch { .. } => RerunSafety::not_safe(
            "microbatch has no run path of its own; a loaded model runs as time_interval",
        ),
        MaterializationStrategy::ContentAddressed { .. } => RerunSafety::Safe,
        MaterializationStrategy::Snapshot(_) => RerunSafety::not_safe(
            "a snapshot model closes old versions and inserts new ones in separate \
             statements with no transaction, so two runs can record a change twice",
        ),
    }
}

/// A `MERGE` on a unique key. Delta Lake fails one of two overlapping MERGEs
/// that touch the same files; a second MERGE of the same rows after the
/// first is a no-op. On a single-writer DuckDB file the runs cannot overlap.
fn classify_upsert(target: &TargetWarehouse, warehouse: &str) -> RerunSafety {
    match target {
        TargetWarehouse::DuckDbSingleWriter | TargetWarehouse::Databricks => RerunSafety::Safe,
        TargetWarehouse::DuckDbShared
        | TargetWarehouse::Snowflake
        | TargetWarehouse::BigQuery
        | TargetWarehouse::Unverified { .. } => RerunSafety::not_safe(format!(
            "a MERGE by two overlapping runs is not verified on {warehouse}"
        )),
    }
}

fn unsupported(what: &str, warehouse: &str) -> RerunSafety {
    RerunSafety::not_safe(format!("{warehouse} does not support {what}"))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocky_ir::{
        ColumnSelection, SnapshotChangeStrategy, SnapshotHardDeletes, SnapshotMetaColumns,
        SnapshotSpec, TimeGrain,
    };

    use super::*;

    const SOURCE: &str = include_str!("rerun_safety.rs");

    /// The warehouses each table row lists, in column order.
    fn warehouses() -> [TargetWarehouse; 6] {
        [
            TargetWarehouse::DuckDbSingleWriter,
            TargetWarehouse::DuckDbShared,
            TargetWarehouse::Databricks,
            TargetWarehouse::Snowflake,
            TargetWarehouse::BigQuery,
            TargetWarehouse::Unverified {
                adapter_type: "trino".to_string(),
            },
        ]
    }

    fn key() -> Vec<Arc<str>> {
        vec![Arc::from("id")]
    }

    fn incremental(unique_key: Vec<Arc<str>>) -> MaterializationStrategy {
        MaterializationStrategy::Incremental {
            timestamp_column: "updated_at".to_string(),
            unique_key,
            lookback: None,
            filter_column: None,
        }
    }

    fn merge(unique_key: Vec<Arc<str>>) -> MaterializationStrategy {
        MaterializationStrategy::Merge {
            unique_key,
            update_columns: ColumnSelection::All,
        }
    }

    fn snapshot() -> MaterializationStrategy {
        MaterializationStrategy::Snapshot(Box::new(SnapshotSpec {
            unique_key: key(),
            change: SnapshotChangeStrategy::Timestamp {
                updated_at: Arc::from("updated_at"),
            },
            hard_deletes: SnapshotHardDeletes::default(),
            meta_columns: SnapshotMetaColumns::default(),
            valid_to_current: None,
        }))
    }

    /// One value of every strategy. The match is exhaustive, so a new
    /// variant fails to compile here until the table below covers it.
    fn witness(strategy: &MaterializationStrategy) -> &'static str {
        match strategy {
            MaterializationStrategy::FullRefresh => "full_refresh",
            MaterializationStrategy::Incremental { .. } => "incremental",
            MaterializationStrategy::Merge { .. } => "merge",
            MaterializationStrategy::View => "view",
            MaterializationStrategy::MaterializedView => "materialized_view",
            MaterializationStrategy::DynamicTable { .. } => "dynamic_table",
            MaterializationStrategy::TimeInterval { .. } => "time_interval",
            MaterializationStrategy::Ephemeral => "ephemeral",
            MaterializationStrategy::DeleteInsert { .. } => "delete_insert",
            MaterializationStrategy::Microbatch { .. } => "microbatch",
            MaterializationStrategy::ContentAddressed { .. } => "content_addressed",
            MaterializationStrategy::Snapshot(_) => "snapshot",
        }
    }

    const S: bool = true;
    const N: bool = false;

    /// Expected class per row, in [`warehouses`] column order:
    /// DuckDB single writer, DuckDB shared, Databricks, Snowflake, BigQuery,
    /// unverified adapter.
    fn table() -> Vec<(MaterializationStrategy, WriterPipeline, [bool; 6])> {
        use WriterPipeline::{Replication as R, Transformation as T};
        let interval = MaterializationStrategy::TimeInterval {
            time_column: "day".to_string(),
            granularity: TimeGrain::Day,
            window: None,
        };
        let delete_insert = MaterializationStrategy::DeleteInsert {
            partition_by: vec![Arc::from("day")],
        };
        let microbatch = MaterializationStrategy::Microbatch {
            timestamp_column: "ts".to_string(),
            granularity: TimeGrain::Hour,
        };
        let content = MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://bucket/t".to_string(),
            partition_columns: Vec::new(),
        };
        let dynamic = MaterializationStrategy::DynamicTable {
            target_lag: "1 hour".to_string(),
        };
        let mut rows = Vec::new();
        for pipeline in [R, T] {
            rows.extend([
                (
                    MaterializationStrategy::FullRefresh,
                    pipeline,
                    [S, S, S, S, S, N],
                ),
                (MaterializationStrategy::View, pipeline, [S, S, S, S, S, N]),
                (
                    MaterializationStrategy::Ephemeral,
                    pipeline,
                    [S, S, S, S, S, S],
                ),
                (
                    MaterializationStrategy::MaterializedView,
                    pipeline,
                    [N, N, S, S, S, N],
                ),
                (dynamic.clone(), pipeline, [N, N, N, S, N, N]),
                (merge(key()), pipeline, [S, N, S, N, N, N]),
                (merge(Vec::new()), pipeline, [N, N, N, N, N, N]),
                (interval.clone(), pipeline, [S, N, S, N, N, N]),
                (delete_insert.clone(), pipeline, [S, N, N, N, N, N]),
                (microbatch.clone(), pipeline, [N, N, N, N, N, N]),
                (content.clone(), pipeline, [S, S, S, S, S, N]),
                (snapshot(), pipeline, [N, N, N, N, N, N]),
            ]);
        }
        rows.extend([
            (incremental(Vec::new()), R, [N, N, N, N, N, N]),
            (incremental(key()), R, [N, N, N, N, N, N]),
            (incremental(Vec::new()), T, [S, N, N, N, N, N]),
            (incremental(key()), T, [S, N, S, N, N, N]),
        ]);
        rows
    }

    #[test]
    fn every_strategy_pipeline_and_warehouse_has_the_expected_class() {
        let mut failures = Vec::new();
        for (strategy, pipeline, expected) in table() {
            for (target, expected) in warehouses().iter().zip(expected) {
                let class = classify_strategy(&strategy, pipeline, target);
                if class.is_safe() != expected {
                    failures.push(format!(
                        "{} / {pipeline:?} / {target:?}: got {class:?}",
                        witness(&strategy)
                    ));
                }
                if let RerunSafety::NotSafe { reason } = &class {
                    assert!(!reason.is_empty(), "a NotSafe class carries a reason");
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    #[test]
    fn the_table_covers_every_strategy_for_both_pipeline_types() {
        let names = [
            "full_refresh",
            "incremental",
            "merge",
            "view",
            "materialized_view",
            "dynamic_table",
            "time_interval",
            "ephemeral",
            "delete_insert",
            "microbatch",
            "content_addressed",
            "snapshot",
        ];
        for pipeline in [WriterPipeline::Replication, WriterPipeline::Transformation] {
            let covered: std::collections::BTreeSet<&str> = table()
                .iter()
                .filter(|(_, p, _)| *p == pipeline)
                .map(|(s, _, _)| witness(s))
                .collect();
            assert_eq!(covered.len(), names.len(), "{pipeline:?}: {covered:?}");
            for name in names {
                assert!(covered.contains(name), "{pipeline:?} misses {name}");
            }
        }
    }

    /// The exhaustive matches are what make a new enum variant fail to
    /// compile. A wildcard arm would let it through silently, so the
    /// non-test part of this file must contain none.
    #[test]
    fn the_classifier_has_no_wildcard_arm() {
        let Some((code, _)) = SOURCE.split_once("#[cfg(test)]") else {
            panic!("the test module marker is missing");
        };
        assert!(!code.contains("_ =>"), "a wildcard match arm crept in");
        assert!(!code.contains("_ if"), "a guarded wildcard arm crept in");
    }

    fn pipeline(toml: &str) -> PipelineConfig {
        toml::from_str(toml).expect("pipeline config parses")
    }

    fn transformation() -> PipelineConfig {
        pipeline("type = \"transformation\"\n[target]\nadapter = \"wh\"\n")
    }

    #[test]
    fn a_transformation_pipeline_is_safe_only_when_every_model_is() {
        let models = ResolvedWrites::Models(vec![
            ("a".to_string(), MaterializationStrategy::FullRefresh),
            ("b".to_string(), incremental(Vec::new())),
        ]);
        let duckdb = classify_pipeline(
            &transformation(),
            &TargetWarehouse::DuckDbSingleWriter,
            &models,
        );
        assert_eq!(duckdb, RerunSafety::Safe);

        let databricks =
            classify_pipeline(&transformation(), &TargetWarehouse::Databricks, &models);
        let RerunSafety::NotSafe { reason } = databricks else {
            panic!("an append model on Databricks is not safe");
        };
        assert!(reason.starts_with("model 'b': "), "{reason}");
    }

    #[test]
    fn unknown_writes_are_not_safe() {
        let target = TargetWarehouse::DuckDbSingleWriter;
        for writes in [
            ResolvedWrites::Models(Vec::new()),
            ResolvedWrites::Unresolved {
                reason: "models directory is missing".to_string(),
            },
            ResolvedWrites::NotNeeded,
            ResolvedWrites::Strategies(vec![Ok(MaterializationStrategy::FullRefresh)]),
        ] {
            let class = classify_pipeline(&transformation(), &target, &writes);
            assert!(!class.is_safe(), "{writes:?}: {class:?}");
        }
    }

    fn replication() -> PipelineConfig {
        pipeline(
            "type = \"replication\"\n\
             [source]\nadapter = \"src\"\n\
             [source.schema_pattern]\nprefix = \"src__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
             [target]\nadapter = \"wh\"\ncatalog_template = \"c\"\nschema_template = \"s\"\n",
        )
    }

    #[test]
    fn a_replication_pipeline_is_safe_only_when_every_strategy_is() {
        let target = TargetWarehouse::Databricks;
        let safe = ResolvedWrites::Strategies(vec![
            Ok(MaterializationStrategy::FullRefresh),
            Ok(merge(key())),
        ]);
        assert_eq!(
            classify_pipeline(&replication(), &target, &safe),
            RerunSafety::Safe
        );
        for writes in [
            ResolvedWrites::Strategies(vec![
                Ok(MaterializationStrategy::FullRefresh),
                Ok(incremental(Vec::new())),
            ]),
            ResolvedWrites::Strategies(vec![Err("unrecognized".to_string())]),
            ResolvedWrites::Strategies(Vec::new()),
            ResolvedWrites::NotNeeded,
        ] {
            let class = classify_pipeline(&replication(), &target, &writes);
            assert!(!class.is_safe(), "{writes:?}: {class:?}");
        }
    }

    #[test]
    fn quality_is_safe_unless_quarantine_writes() {
        let base = "type = \"quality\"\n[target]\nadapter = \"wh\"\n[checks]\nenabled = true\n";
        let target = TargetWarehouse::Databricks;
        let no_writes = ResolvedWrites::NotNeeded;
        assert_eq!(
            classify_pipeline(&pipeline(base), &target, &no_writes),
            RerunSafety::Safe
        );
        let off = format!("{base}[checks.quarantine]\nenabled = false\nmode = \"tag\"\n");
        assert_eq!(
            classify_pipeline(&pipeline(&off), &target, &no_writes),
            RerunSafety::Safe
        );
        for mode in ["split", "tag", "drop"] {
            let on = format!("{base}[checks.quarantine]\nenabled = true\nmode = \"{mode}\"\n");
            let class = classify_pipeline(&pipeline(&on), &target, &no_writes);
            assert!(!class.is_safe(), "{mode}: {class:?}");
        }
    }

    #[test]
    fn snapshot_and_load_pipelines_are_not_safe() {
        let snapshot = pipeline(
            "type = \"snapshot\"\nunique_key = [\"id\"]\nupdated_at = \"u\"\n\
             [source]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\ntable = \"t\"\n\
             [target]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\ntable = \"h\"\n",
        );
        let load = pipeline(
            "type = \"load\"\nsource_dir = \"data\"\n\
             [target]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\n",
        );
        for pc in [snapshot, load] {
            for target in warehouses() {
                let class = classify_pipeline(&pc, &target, &ResolvedWrites::NotNeeded);
                assert!(!class.is_safe(), "{}: {class:?}", pc.pipeline_type_str());
            }
        }
    }

    fn adapter(toml: &str) -> AdapterConfig {
        toml::from_str(toml).expect("adapter config parses")
    }

    #[test]
    fn the_target_warehouse_comes_from_the_adapter_block() {
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter("type = \"duckdb\"\npath = \"x.db\"\n"))),
            TargetWarehouse::DuckDbSingleWriter
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter("type = \"duckdb\"\n"))),
            TargetWarehouse::DuckDbSingleWriter
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter(
                "type = \"duckdb\"\npath = \"md:prod\"\n"
            ))),
            TargetWarehouse::DuckDbShared
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter(
                "type = \"duckdb\"\npath = \"x.db\"\n[[attach]]\nalias = \"dl\"\ntype = \"ducklake\"\n\
                 metadata_path = \"m.ducklake\"\ndata_path = \"d/\"\n"
            ))),
            TargetWarehouse::DuckDbShared
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter("type = \"databricks\"\n"))),
            TargetWarehouse::Databricks
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter("type = \"trino\"\n"))),
            TargetWarehouse::Unverified {
                adapter_type: "trino".to_string()
            }
        );
        assert!(matches!(
            TargetWarehouse::from_adapter(None),
            TargetWarehouse::Unverified { .. }
        ));
    }

    #[test]
    fn the_class_serializes_with_its_reason() {
        let json = serde_json::to_value(RerunSafety::not_safe("why")).expect("serializes");
        assert_eq!(
            json,
            serde_json::json!({"class": "not_rerun_safe", "reason": "why"})
        );
        let json = serde_json::to_value(RerunSafety::Safe).expect("serializes");
        assert_eq!(json, serde_json::json!({"class": "rerun_safe"}));
    }
}
