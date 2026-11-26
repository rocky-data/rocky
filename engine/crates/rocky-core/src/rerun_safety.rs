//! Whether a pipeline is safe to run a second time.
//!
//! A pipeline is *rerun-safe* when a second run, sequential or overlapping
//! the first, leaves the warehouse exactly as one run would. The scheduler
//! will retry, re-run and fail over only pipelines this module classifies as
//! [`RerunSafety::Safe`]. A false `Safe` writes data twice, so the classifier
//! fails closed.
//!
//! The rule is small on purpose: **a pipeline is `Safe` only when it writes
//! nothing to the warehouse.** Four rounds of review each found another
//! write that looked safe and was not (a keyless lookback, a NULL merge key,
//! a row outside a time window, a model that reads another model of its own
//! pipeline). So every write is `NotSafe`, with a reason that records the
//! counterexample. A later change may prove more strategies safe.
//!
//! Two kinds of pipeline write nothing:
//!
//! 1. A transformation pipeline whose models are all `ephemeral` (inlined
//!    into their consumers, never built), and that none of these touch: a
//!    project function (`CREATE OR REPLACE FUNCTION`) or `[governance.tags]`
//!    (`ALTER ... SET TAGS`). A run creates the functions an ephemeral model
//!    calls and tags every model, ephemeral ones included.
//! 2. A quality pipeline with no quarantine and no `[[checks.custom]]`: it
//!    only reads.
//!
//! A `[hook.*]` shell command can write anything, so [`with_hooks`] turns a
//! `Safe` class into `NotSafe` when one is configured. A webhook sends an
//! HTTP request and issues no warehouse statement, so it does not change the
//! class.
//!
//! The classification judges the steady-state statements a run executes.

use serde::{Deserialize, Serialize};

use rocky_ir::MaterializationStrategy;

use crate::config::{AdapterConfig, ChecksConfig, PipelineConfig, QuarantineMode};
use crate::cost::WarehouseType;

/// The rerun-safety class of one pipeline or one materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum RerunSafety {
    /// A second run, sequential or overlapping, leaves the warehouse as one
    /// run would.
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

/// The target warehouse, as far as rerun safety is concerned. It does not
/// change a verdict: every write is `NotSafe` on every adapter. It only
/// names the adapter in the reason when Rocky has not verified it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetWarehouse {
    /// DuckDB.
    DuckDb,
    /// Databricks (Delta Lake).
    Databricks,
    /// Snowflake.
    Snowflake,
    /// BigQuery.
    BigQuery,
    /// Any other adapter type, or a target adapter that is not declared.
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
        match WarehouseType::from_adapter_type(&adapter.adapter_type) {
            Some(WarehouseType::DuckDb) => Self::DuckDb,
            Some(WarehouseType::Databricks) => Self::Databricks,
            Some(WarehouseType::Snowflake) => Self::Snowflake,
            Some(WarehouseType::BigQuery) => Self::BigQuery,
            None => Self::Unverified {
                adapter_type: adapter.adapter_type.clone(),
            },
        }
    }
}

/// One model of a transformation pipeline, with the warehouse writes a run
/// makes for it besides building it.
#[derive(Debug, Clone)]
pub struct ResolvedModel {
    /// The model name.
    pub name: String,
    /// The materialization the run uses.
    pub strategy: MaterializationStrategy,
    /// The project functions (`functions/`) the model's SQL calls. A run
    /// creates each one with `CREATE OR REPLACE FUNCTION`, for an ephemeral
    /// model too.
    pub calls_functions: Vec<String>,
    /// Whether the model sets `[governance.tags]`. A run applies them to the
    /// model's target, for an ephemeral model too.
    pub has_governance_tags: bool,
}

/// What a pipeline writes, resolved by the caller from files this crate does
/// not read.
#[derive(Debug, Clone)]
pub enum ResolvedWrites {
    /// A transformation pipeline: each model it builds.
    Models(Vec<ResolvedModel>),
    /// The writes could not be resolved, for example because the models
    /// directory is missing or a model fails to load.
    Unresolved {
        /// Why the writes are unknown.
        reason: String,
    },
    /// The pipeline type needs nothing resolved (replication, quality,
    /// snapshot, load).
    NotNeeded,
}

const CUSTOM_CHECK_REASON: &str =
    "a [[checks.custom]] check runs its SQL as written, so Rocky cannot prove it only reads";

const REPLICATION_REASON: &str = "a replication pipeline writes its target tables on every run, \
     with any strategy; on Databricks it also sets TBLPROPERTIES run_id after the copy";

/// Classify a pipeline. A pipeline is safe only when it writes nothing to
/// the warehouse: a transformation pipeline whose models are all
/// `ephemeral`, or a quality pipeline with no quarantine and no custom check.
/// Everything else, and any write that cannot be resolved, is not safe.
#[must_use]
pub fn classify_pipeline(
    pipeline: &PipelineConfig,
    target: &TargetWarehouse,
    writes: &ResolvedWrites,
) -> RerunSafety {
    match pipeline {
        PipelineConfig::Replication(replication) => {
            if replication.checks.custom.is_empty() {
                RerunSafety::not_safe(REPLICATION_REASON)
            } else {
                RerunSafety::not_safe(format!("{REPLICATION_REASON}; also, {CUSTOM_CHECK_REASON}"))
            }
        }
        PipelineConfig::Transformation(_) => match writes {
            ResolvedWrites::Models(models) => classify_models(models, target),
            ResolvedWrites::Unresolved { reason } => RerunSafety::not_safe(reason.clone()),
            ResolvedWrites::NotNeeded => {
                RerunSafety::not_safe("the transformation models were not resolved")
            }
        },
        PipelineConfig::Quality(quality) => classify_quality(&quality.checks),
        PipelineConfig::Snapshot(_) => RerunSafety::not_safe(
            "a snapshot pipeline closes old versions and inserts new ones in separate \
             statements, so a second run can record a change twice",
        ),
        PipelineConfig::Load(_) => RerunSafety::not_safe(
            "a load pipeline re-ingests every file on each run, so a second run \
             duplicates the rows",
        ),
    }
}

/// Make a `Safe` class `NotSafe` when the project has `shell_hooks` `[hook.*]`
/// shell commands. A hook command can write to the warehouse, and Rocky does
/// not read it. A webhook is not counted: it sends an HTTP request and runs
/// no warehouse statement.
#[must_use]
pub fn with_hooks(class: RerunSafety, shell_hooks: usize) -> RerunSafety {
    match class {
        RerunSafety::Safe if shell_hooks > 0 => RerunSafety::not_safe(
            "a [hook.*] shell command can write to the warehouse, and Rocky does not classify it",
        ),
        RerunSafety::Safe | RerunSafety::NotSafe { .. } => class,
    }
}

fn classify_models(models: &[ResolvedModel], target: &TargetWarehouse) -> RerunSafety {
    if models.is_empty() {
        return RerunSafety::not_safe("the pipeline builds no models");
    }
    for model in models {
        if let RerunSafety::NotSafe { reason } = classify_strategy(&model.strategy, target) {
            return RerunSafety::not_safe(format!("model '{}': {reason}", model.name));
        }
        if let Some(function) = model.calls_functions.first() {
            return RerunSafety::not_safe(format!(
                "model '{}' calls the project function '{function}', which a run creates in \
                 the warehouse with CREATE OR REPLACE FUNCTION, even for an ephemeral model",
                model.name
            ));
        }
        if model.has_governance_tags {
            return RerunSafety::not_safe(format!(
                "model '{}' sets [governance.tags], which a run applies to its target in the \
                 warehouse, even for an ephemeral model",
                model.name
            ));
        }
    }
    RerunSafety::Safe
}

/// A quality pipeline reads the warehouse and writes nothing there, unless
/// quarantine is on or a custom check runs SQL Rocky cannot see into. That
/// holds on every adapter, so the target does not matter here.
fn classify_quality(checks: &ChecksConfig) -> RerunSafety {
    if let Some(quarantine) = checks.quarantine.as_ref().filter(|q| q.enabled) {
        return match quarantine.mode {
            QuarantineMode::Tag => {
                RerunSafety::not_safe("quarantine mode \"tag\" rewrites the checked table in place")
            }
            QuarantineMode::Split | QuarantineMode::Drop => RerunSafety::not_safe(
                "quarantine writes its tables in several statements, not one replace",
            ),
        };
    }
    if !checks.custom.is_empty() {
        return RerunSafety::not_safe(CUSTOM_CHECK_REASON);
    }
    RerunSafety::Safe
}

/// Classify one model's materialization. Only `ephemeral` is safe: Rocky
/// inlines it into its consumers as a CTE and builds nothing. Every other
/// strategy writes, so it is not safe on every adapter. The reason records
/// the counterexample the reviews found.
#[must_use]
pub fn classify_strategy(
    strategy: &MaterializationStrategy,
    target: &TargetWarehouse,
) -> RerunSafety {
    let reason = match strategy {
        MaterializationStrategy::Ephemeral => return RerunSafety::Safe,
        MaterializationStrategy::FullRefresh => {
            "full_refresh: a model that reads another model of the same pipeline can mix two \
             runs' inputs under overlap; lineage cannot prove no self-read"
        }
        MaterializationStrategy::Merge { .. } => "merge: a NULL key re-inserts on every run",
        MaterializationStrategy::Incremental { .. } => {
            "incremental: a lookback or a filter on another column re-appends rows"
        }
        MaterializationStrategy::TimeInterval { .. } => {
            "time_interval: a row outside the window, or a filter on another column, duplicates"
        }
        MaterializationStrategy::Microbatch { .. } => {
            "microbatch: runs as time_interval, so a row outside the window, or a filter on \
             another column, duplicates"
        }
        MaterializationStrategy::DeleteInsert { .. } => {
            "delete_insert: it deletes the partitions and inserts the rows in separate \
             statements, so an overlapping run can see or leave a half-written table"
        }
        MaterializationStrategy::Snapshot(_) => {
            "snapshot: it closes old versions and inserts new ones in separate statements, \
             so a second run can record a change twice"
        }
        MaterializationStrategy::View => {
            "view: it replaces a shared object; an overlapping run can read a half-built input"
        }
        MaterializationStrategy::MaterializedView => {
            "materialized_view: it replaces a shared object; an overlapping run can read a \
             half-built input"
        }
        MaterializationStrategy::DynamicTable { .. } => {
            "dynamic_table: it replaces a shared object; an overlapping run can read a \
             half-built input"
        }
        MaterializationStrategy::ContentAddressed { .. } => {
            "content_addressed: it commits to the Delta log and then syncs the table with \
             MSCK REPAIR TABLE, a second write after the commit"
        }
    };
    match target {
        TargetWarehouse::Unverified { adapter_type } => RerunSafety::not_safe(format!(
            "{reason} (rerun safety is not verified on adapter type '{adapter_type}')"
        )),
        TargetWarehouse::DuckDb
        | TargetWarehouse::Databricks
        | TargetWarehouse::Snowflake
        | TargetWarehouse::BigQuery => RerunSafety::not_safe(reason),
    }
}

/// The lines of `source` that hold a catch-all match arm: a bare binding
/// (`other =>`, `name if guard =>`), `_` or `[..]`. Classifier tests run it
/// over this file and over the `rocky validate` call site, so a new enum
/// variant cannot slip through a catch-all arm without a compile error.
#[doc(hidden)]
#[must_use]
pub fn catch_all_match_arms(source: &str) -> Vec<String> {
    source
        .lines()
        .filter(|line| {
            let line = line.trim_start();
            let line = line.strip_prefix("| ").unwrap_or(line);
            if line.starts_with("//") {
                return false;
            }
            let Some((token, rest)) = line.split_once(' ') else {
                return false;
            };
            let bare_binding = token.starts_with(|c: char| c.is_ascii_lowercase() || c == '_')
                && token.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
                && !["true", "false", "else"].contains(&token);
            (bare_binding || token == "[..]") && (rest.starts_with("=>") || rest.starts_with("if "))
        })
        .map(str::to_string)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocky_ir::{
        ColumnSelection, IncrementalLookback, SnapshotChangeStrategy, SnapshotHardDeletes,
        SnapshotMetaColumns, SnapshotSpec, TimeGrain,
    };

    use super::*;

    const SOURCE: &str = include_str!("rerun_safety.rs");

    fn warehouses() -> [TargetWarehouse; 5] {
        [
            TargetWarehouse::DuckDb,
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

    fn incremental(unique_key: Vec<Arc<str>>, lookback: Option<&str>) -> MaterializationStrategy {
        MaterializationStrategy::Incremental {
            timestamp_column: "updated_at".to_string(),
            unique_key,
            lookback: lookback.map(|l| l.parse::<IncrementalLookback>().expect("lookback parses")),
            filter_column: None,
        }
    }

    fn merge(unique_key: Vec<Arc<str>>) -> MaterializationStrategy {
        MaterializationStrategy::Merge {
            unique_key,
            update_columns: ColumnSelection::All,
        }
    }

    fn time_interval() -> MaterializationStrategy {
        MaterializationStrategy::TimeInterval {
            time_column: "day".to_string(),
            granularity: TimeGrain::Day,
            window: None,
        }
    }

    fn microbatch() -> MaterializationStrategy {
        MaterializationStrategy::Microbatch {
            timestamp_column: "ts".to_string(),
            granularity: TimeGrain::Hour,
        }
    }

    fn delete_insert() -> MaterializationStrategy {
        MaterializationStrategy::DeleteInsert {
            partition_by: vec![Arc::from("day")],
        }
    }

    fn content_addressed() -> MaterializationStrategy {
        MaterializationStrategy::ContentAddressed {
            storage_prefix: "s3://bucket/t".to_string(),
            partition_columns: Vec::new(),
        }
    }

    fn dynamic_table() -> MaterializationStrategy {
        MaterializationStrategy::DynamicTable {
            target_lag: "1 hour".to_string(),
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

    /// Every strategy, with the variants that carry options listed more than
    /// once.
    fn strategies() -> Vec<MaterializationStrategy> {
        vec![
            MaterializationStrategy::FullRefresh,
            MaterializationStrategy::View,
            MaterializationStrategy::Ephemeral,
            MaterializationStrategy::MaterializedView,
            dynamic_table(),
            merge(key()),
            merge(Vec::new()),
            time_interval(),
            microbatch(),
            delete_insert(),
            content_addressed(),
            snapshot(),
            incremental(Vec::new(), None),
            incremental(key(), None),
            incremental(Vec::new(), Some("3 days")),
            incremental(key(), Some("3 days")),
        ]
    }

    fn pipeline(toml: &str) -> PipelineConfig {
        toml::from_str(toml).expect("pipeline config parses")
    }

    fn transformation() -> PipelineConfig {
        pipeline("type = \"transformation\"\n[target]\nadapter = \"wh\"\n")
    }

    fn replication_toml(extra: &str) -> PipelineConfig {
        pipeline(&format!(
            "type = \"replication\"\n\
             [source]\nadapter = \"src\"\n\
             [source.schema_pattern]\nprefix = \"src__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
             [target]\nadapter = \"wh\"\ncatalog_template = \"c\"\nschema_template = \"s\"\n{extra}"
        ))
    }

    fn quality(extra: &str) -> PipelineConfig {
        pipeline(&format!(
            "type = \"quality\"\n[target]\nadapter = \"wh\"\n[checks]\nenabled = true\n{extra}"
        ))
    }

    fn snapshot_pipeline() -> PipelineConfig {
        pipeline(
            "type = \"snapshot\"\nunique_key = [\"id\"]\nupdated_at = \"u\"\n\
             [source]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\ntable = \"t\"\n\
             [target]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\ntable = \"h\"\n",
        )
    }

    fn load_pipeline() -> PipelineConfig {
        pipeline(
            "type = \"load\"\nsource_dir = \"data\"\n\
             [target]\nadapter = \"wh\"\ncatalog = \"c\"\nschema = \"s\"\n",
        )
    }

    fn model(name: &str, strategy: MaterializationStrategy) -> ResolvedModel {
        ResolvedModel {
            name: name.to_string(),
            strategy,
            calls_functions: Vec::new(),
            has_governance_tags: false,
        }
    }

    fn one_model(strategy: MaterializationStrategy) -> ResolvedWrites {
        ResolvedWrites::Models(vec![model("m", strategy)])
    }

    fn not_safe_reason(class: RerunSafety) -> String {
        let RerunSafety::NotSafe { reason } = class else {
            panic!("expected not_rerun_safe");
        };
        assert!(!reason.is_empty());
        reason
    }

    /// The rule, for every adapter: only a pipeline that writes nothing is
    /// safe. One row per strategy for a transformation model, then one row
    /// per other pipeline type.
    #[test]
    fn only_a_pipeline_that_writes_nothing_is_safe_on_every_adapter() {
        let mut rows: Vec<(String, PipelineConfig, ResolvedWrites, bool)> = strategies()
            .into_iter()
            .map(|strategy| {
                let safe = matches!(strategy, MaterializationStrategy::Ephemeral);
                (
                    format!("transformation / {}", witness(&strategy)),
                    transformation(),
                    one_model(strategy),
                    safe,
                )
            })
            .collect();
        let none = ResolvedWrites::NotNeeded;
        rows.extend([
            (
                "replication".to_string(),
                replication_toml(""),
                none.clone(),
                false,
            ),
            (
                "replication + custom check".to_string(),
                replication_toml("[[checks.custom]]\nname = \"c\"\nsql = \"SELECT 1\"\n"),
                none.clone(),
                false,
            ),
            ("quality".to_string(), quality(""), none.clone(), true),
            (
                "quality, quarantine off".to_string(),
                quality("[checks.quarantine]\nenabled = false\nmode = \"tag\"\n"),
                none.clone(),
                true,
            ),
            (
                "quality + quarantine".to_string(),
                quality("[checks.quarantine]\nenabled = true\nmode = \"split\"\n"),
                none.clone(),
                false,
            ),
            (
                "quality + custom check".to_string(),
                quality("[[checks.custom]]\nname = \"c\"\nsql = \"SELECT 1\"\n"),
                none.clone(),
                false,
            ),
            (
                "snapshot".to_string(),
                snapshot_pipeline(),
                none.clone(),
                false,
            ),
            ("load".to_string(), load_pipeline(), none, false),
        ]);
        let mut failures = Vec::new();
        for target in warehouses() {
            for (label, pipeline, writes, expected) in &rows {
                let class = classify_pipeline(pipeline, &target, writes);
                if class.is_safe() != *expected {
                    failures.push(format!("{label} / {target:?}: got {class:?}"));
                }
                if let RerunSafety::NotSafe { reason } = &class {
                    assert!(!reason.is_empty(), "a NotSafe class carries a reason");
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    #[test]
    fn the_table_covers_every_strategy() {
        let covered: std::collections::BTreeSet<&str> = strategies().iter().map(witness).collect();
        assert_eq!(covered.len(), 12, "{covered:?}");
    }

    /// Each strategy's reason records the counterexample the reviews found.
    #[test]
    fn each_writing_strategy_names_its_counterexample() {
        let cases: Vec<(MaterializationStrategy, &str)> = vec![
            (
                MaterializationStrategy::FullRefresh,
                "can mix two runs' inputs under overlap; lineage cannot prove no self-read",
            ),
            (merge(key()), "a NULL key re-inserts on every run"),
            (merge(Vec::new()), "a NULL key re-inserts on every run"),
            (
                incremental(Vec::new(), Some("3 days")),
                "a lookback or a filter on another column re-appends rows",
            ),
            (
                incremental(key(), None),
                "a lookback or a filter on another column re-appends rows",
            ),
            (
                time_interval(),
                "a row outside the window, or a filter on another column, duplicates",
            ),
            (
                microbatch(),
                "a row outside the window, or a filter on another column, duplicates",
            ),
            (delete_insert(), "in separate statements"),
            (snapshot(), "in separate statements"),
            (
                MaterializationStrategy::View,
                "replaces a shared object; an overlapping run can read a half-built input",
            ),
            (
                MaterializationStrategy::MaterializedView,
                "replaces a shared object; an overlapping run can read a half-built input",
            ),
            (
                dynamic_table(),
                "replaces a shared object; an overlapping run can read a half-built input",
            ),
            (content_addressed(), "MSCK REPAIR TABLE"),
        ];
        for target in warehouses() {
            for (strategy, counterexample) in &cases {
                let reason = not_safe_reason(classify_strategy(strategy, &target));
                assert!(
                    reason.starts_with(witness(strategy)) && reason.contains(counterexample),
                    "{} / {target:?}: {reason}",
                    witness(strategy)
                );
            }
        }
    }

    #[test]
    fn an_unverified_adapter_is_named_in_the_reason() {
        let target = TargetWarehouse::Unverified {
            adapter_type: "trino".to_string(),
        };
        let reason = not_safe_reason(classify_strategy(
            &MaterializationStrategy::FullRefresh,
            &target,
        ));
        assert!(
            reason.contains("not verified on adapter type 'trino'"),
            "{reason}"
        );
    }

    #[test]
    fn a_transformation_pipeline_is_safe_only_when_every_model_is_ephemeral() {
        let target = TargetWarehouse::DuckDb;
        let all_ephemeral = ResolvedWrites::Models(vec![
            model("a", MaterializationStrategy::Ephemeral),
            model("b", MaterializationStrategy::Ephemeral),
        ]);
        assert_eq!(
            classify_pipeline(&transformation(), &target, &all_ephemeral),
            RerunSafety::Safe
        );
        let mixed = ResolvedWrites::Models(vec![
            model("a", MaterializationStrategy::Ephemeral),
            model("b", MaterializationStrategy::FullRefresh),
        ]);
        let reason = not_safe_reason(classify_pipeline(&transformation(), &target, &mixed));
        assert!(reason.starts_with("model 'b': full_refresh"), "{reason}");
    }

    /// A run creates the functions an ephemeral model calls and tags every
    /// model, so an ephemeral model that does either is not a write of
    /// nothing.
    #[test]
    fn an_ephemeral_model_with_a_function_or_tags_is_not_safe() {
        let target = TargetWarehouse::DuckDb;
        let mut calls = model("a", MaterializationStrategy::Ephemeral);
        calls.calls_functions = vec!["clean_name".to_string()];
        let reason = not_safe_reason(classify_pipeline(
            &transformation(),
            &target,
            &ResolvedWrites::Models(vec![calls]),
        ));
        assert!(
            reason.contains("project function 'clean_name'")
                && reason.contains("CREATE OR REPLACE FUNCTION"),
            "{reason}"
        );
        let mut tagged = model("a", MaterializationStrategy::Ephemeral);
        tagged.has_governance_tags = true;
        let reason = not_safe_reason(classify_pipeline(
            &transformation(),
            &target,
            &ResolvedWrites::Models(vec![tagged]),
        ));
        assert!(reason.contains("[governance.tags]"), "{reason}");
    }

    #[test]
    fn unknown_writes_are_not_safe() {
        for writes in [
            ResolvedWrites::Models(Vec::new()),
            ResolvedWrites::Unresolved {
                reason: "models directory is missing".to_string(),
            },
            ResolvedWrites::NotNeeded,
        ] {
            let class = classify_pipeline(&transformation(), &TargetWarehouse::DuckDb, &writes);
            assert!(!class.is_safe(), "{writes:?}: {class:?}");
        }
    }

    #[test]
    fn a_replication_pipeline_names_why_it_is_not_safe() {
        let target = TargetWarehouse::Databricks;
        let reason = not_safe_reason(classify_pipeline(
            &replication_toml(""),
            &target,
            &ResolvedWrites::NotNeeded,
        ));
        assert!(
            reason.contains("every run") && reason.contains("TBLPROPERTIES run_id"),
            "{reason}"
        );
        assert!(!reason.contains("checks.custom"), "{reason}");
    }

    /// The custom-check guard: the reason names the check SQL only when the
    /// pipeline has one.
    #[test]
    fn a_replication_pipeline_with_a_custom_check_names_the_check() {
        let with_custom =
            replication_toml("[[checks.custom]]\nname = \"c\"\nsql = \"DELETE FROM t\"\n");
        let reason = not_safe_reason(classify_pipeline(
            &with_custom,
            &TargetWarehouse::Databricks,
            &ResolvedWrites::NotNeeded,
        ));
        assert!(
            reason.contains("[[checks.custom]]") && reason.contains("TBLPROPERTIES run_id"),
            "{reason}"
        );
    }

    #[test]
    fn quality_is_safe_unless_quarantine_or_custom_sql_writes() {
        let target = TargetWarehouse::Databricks;
        let none = ResolvedWrites::NotNeeded;
        assert_eq!(
            classify_pipeline(&quality(""), &target, &none),
            RerunSafety::Safe
        );
        for mode in ["split", "tag", "drop"] {
            let on = quality(&format!(
                "[checks.quarantine]\nenabled = true\nmode = \"{mode}\"\n"
            ));
            let reason = not_safe_reason(classify_pipeline(&on, &target, &none));
            assert!(reason.contains("quarantine"), "{mode}: {reason}");
        }
        let custom = quality("[[checks.custom]]\nname = \"c\"\nsql = \"DELETE FROM t\"\n");
        let reason = not_safe_reason(classify_pipeline(&custom, &target, &none));
        assert!(reason.contains("checks.custom"), "{reason}");
    }

    #[test]
    fn a_shell_hook_makes_a_safe_class_not_safe() {
        assert_eq!(with_hooks(RerunSafety::Safe, 0), RerunSafety::Safe);
        let reason = not_safe_reason(with_hooks(RerunSafety::Safe, 1));
        assert!(reason.contains("[hook.*] shell command"), "{reason}");
        let already = RerunSafety::not_safe("why");
        assert_eq!(with_hooks(already.clone(), 3), already);
    }

    /// The exhaustive matches are what make a new enum variant fail to
    /// compile. A wildcard arm would let it through silently, so the
    /// non-test part of this file must contain none.
    #[test]
    fn the_classifier_has_no_wildcard_arm() {
        let Some((code, _)) = SOURCE.split_once("#[cfg(test)]") else {
            panic!("the test module marker is missing");
        };
        let arms = catch_all_match_arms(code);
        assert!(arms.is_empty(), "a catch-all match arm crept in: {arms:#?}");
    }

    #[test]
    fn the_catch_all_detector_finds_every_catch_all_shape() {
        for arm in [
            "        _ => RerunSafety::Safe,",
            "        other => other,",
            "        | other => x,",
            "        x if cond => y,",
            "        _ if cond => y,",
            "        [..] => path,",
        ] {
            assert_eq!(catch_all_match_arms(arm).len(), 1, "missed: {arm}");
        }
        for line in [
            "        Ok(x) => x,",
            "        Foo::Bar if guard => y,",
            "        Self::Safe => true,",
            "        true => 1,",
            "        // other => commented out",
            "        let other = 1;",
            "        } else if x {",
        ] {
            assert!(catch_all_match_arms(line).is_empty(), "false hit: {line}");
        }
    }

    fn adapter(toml: &str) -> AdapterConfig {
        toml::from_str(toml).expect("adapter config parses")
    }

    #[test]
    fn the_target_warehouse_comes_from_the_adapter_block() {
        for (toml, expected) in [
            (
                "type = \"duckdb\"\npath = \"x.db\"\n",
                TargetWarehouse::DuckDb,
            ),
            (
                "type = \"duckdb\"\npath = \"md:prod\"\n",
                TargetWarehouse::DuckDb,
            ),
            ("type = \"databricks\"\n", TargetWarehouse::Databricks),
            (
                "type = \"trino\"\n",
                TargetWarehouse::Unverified {
                    adapter_type: "trino".to_string(),
                },
            ),
        ] {
            assert_eq!(
                TargetWarehouse::from_adapter(Some(&adapter(toml))),
                expected
            );
        }
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
