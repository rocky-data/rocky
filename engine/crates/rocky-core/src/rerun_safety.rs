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
//! A verdict is `Safe` only on one of three grounds, each proven from Rocky's
//! own code:
//!
//! - The steady-state write is **one statement that replaces the whole
//!   object** from the model's query (`CREATE OR REPLACE ...`). Two
//!   overlapping runs each leave a complete object.
//! - The target is a **single-writer DuckDB file**. Every scheduled run is a
//!   separate `rocky` process (`SubprocessSpawner`), and DuckDB lets only one
//!   process open a file to write, so the runs cannot overlap.
//! - The writer itself handles contention (`content_addressed`: a
//!   conditional PUT on the Delta log entry, then a re-read on conflict).
//!
//! Warehouse concurrency semantics that Rocky has not proven, such as how
//! Delta Lake resolves two overlapping `MERGE`s, are not a ground.
//!
//! The classification judges the steady-state statements a run executes. It
//! does not audit the first-run bootstrap or the drift rebuild paths.

use serde::{Deserialize, Serialize};

use rocky_ir::MaterializationStrategy;

use crate::config::{AdapterConfig, ChecksConfig, PipelineConfig, QuarantineMode};
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
    /// DuckDB on a plain local file path, or in memory, with no attached
    /// catalog. Only one process can open a DuckDB file for writing, so a
    /// second run cannot overlap the first one: it fails to open the
    /// database. A memory database is private to its process.
    DuckDbSingleWriter,
    /// Any other DuckDB target: an attached catalog (Iceberg, DuckLake), or a
    /// `path` with a `scheme:` prefix (`md:`, `ducklake:`, ...) that DuckDB
    /// may route to a store other processes can write.
    DuckDbShared,
    /// Databricks (Delta Lake).
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
                // The registry opens a set `path` verbatim with
                // `Connection::open` and no `path` in memory.
                let single_path = adapter
                    .path
                    .as_ref()
                    .is_none_or(|path| duckdb_path_is_single_writer(path.expose()));
                if single_path && adapter.attach.is_empty() {
                    Self::DuckDbSingleWriter
                } else {
                    Self::DuckDbShared
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

/// Whether a DuckDB `path` names a single-writer database. An allowlist: only
/// `:memory:` and a plain filesystem path count. DuckDB reads a `scheme:`
/// prefix (`md:`, `motherduck:`, `ducklake:`, an extension's own prefix) as a
/// route to another store, which other processes may write. So any `:` makes
/// the path shared, except a Windows drive letter (`C:\`, `C:/`) at the
/// start. An empty path is shared too: it is not a file Rocky can name.
///
/// `:memory:` is single-writer because each scheduled run is its own `rocky`
/// process, and a memory database lives and dies inside that process.
fn duckdb_path_is_single_writer(path: &str) -> bool {
    if path == ":memory:" {
        return true;
    }
    if path.is_empty() {
        return false;
    }
    let bytes = path.as_bytes();
    let drive_letter = bytes.len() >= 3
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes[2] == b'\\' || bytes[2] == b'/');
    let rest = if drive_letter { &path[2..] } else { path };
    !rest.contains(':')
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

const CUSTOM_CHECK_REASON: &str =
    "a [[checks.custom]] check runs its SQL as written, so Rocky cannot prove it only reads";

/// Classify a pipeline. A pipeline is safe only when every materialization
/// it writes is safe; a transformation pipeline that builds no models, and
/// any write that cannot be resolved, is not safe.
#[must_use]
pub fn classify_pipeline(
    pipeline: &PipelineConfig,
    target: &TargetWarehouse,
    writes: &ResolvedWrites,
) -> RerunSafety {
    // A replication run also runs `[[checks.custom]]`, whose SQL goes to the
    // warehouse as written. A transformation run does not run pipeline checks.
    if let PipelineConfig::Replication(replication) = pipeline
        && !replication.checks.custom.is_empty()
    {
        return RerunSafety::not_safe(CUSTOM_CHECK_REASON);
    }
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
        PipelineConfig::Quality(quality) => classify_quality(&quality.checks),
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
                "quarantine writes its tables in several statements, and overlapping runs \
                 are not verified",
            ),
        };
    }
    if !checks.custom.is_empty() {
        return RerunSafety::not_safe(CUSTOM_CHECK_REASON);
    }
    RerunSafety::Safe
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
            // An ephemeral model is inlined into its consumers as a CTE and
            // writes nothing of its own (`sql_gen` refuses to emit SQL for it).
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
        // One `CREATE OR REPLACE TABLE ... AS` (`SqlDialect::create_table_as`
        // on DuckDB, Databricks, Snowflake and BigQuery; the lakehouse
        // `format` DDL is one statement too). On an attached Iceberg catalog
        // a full refresh is a multi-statement in-place overwrite
        // (`sql_gen::in_place_overwrite_statements`), so shared DuckDB fails
        // closed.
        MaterializationStrategy::FullRefresh => match target {
            TargetWarehouse::DuckDbSingleWriter
            | TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery => RerunSafety::Safe,
            TargetWarehouse::DuckDbShared | TargetWarehouse::Unverified { .. } => {
                not_proven("a full refresh", warehouse)
            }
        },
        // One `CREATE OR REPLACE VIEW` (`SqlDialect::view_ddl`).
        MaterializationStrategy::View => RerunSafety::Safe,
        // Inlined into its consumers; it writes nothing of its own.
        MaterializationStrategy::Ephemeral => RerunSafety::Safe,
        // One `CREATE OR REPLACE MATERIALIZED VIEW` (each dialect's
        // `materialized_view_ddl`); the warehouse maintains the data.
        MaterializationStrategy::MaterializedView => match target {
            TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery => RerunSafety::Safe,
            TargetWarehouse::DuckDbSingleWriter
            | TargetWarehouse::DuckDbShared
            | TargetWarehouse::Unverified { .. } => unsupported("a materialized view", warehouse),
        },
        // One `CREATE OR REPLACE DYNAMIC TABLE` (Snowflake `dynamic_table_ddl`).
        MaterializationStrategy::DynamicTable { .. } => match target {
            TargetWarehouse::Snowflake => RerunSafety::Safe,
            TargetWarehouse::DuckDbSingleWriter
            | TargetWarehouse::DuckDbShared
            | TargetWarehouse::Databricks
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => unsupported("a dynamic table", warehouse),
        },
        MaterializationStrategy::Incremental {
            unique_key,
            lookback,
            ..
        } => match pipeline {
            WriterPipeline::Replication => RerunSafety::not_safe(
                "replication incremental reads its watermark from the Rocky state store, \
                 so a second run before the first records it copies the same rows again",
            ),
            // With no key the run is a plain INSERT of the rows past
            // `MAX(watermark) - lookback`, so every run appends the lookback
            // window again (W046), even a sequential one on a DuckDB file.
            WriterPipeline::Transformation
                if unique_key.is_empty() && lookback.is_some_and(|l| l.amount > 0) =>
            {
                RerunSafety::not_safe(
                    "incremental append with a lookback and no unique_key: every run appends \
                     the rows of the lookback window again",
                )
            }
            // Even on a DuckDB file a keyless append is not proven: with the
            // `@incremental_filter` placeholder or a `filter_column`, a
            // mis-specified model re-appends rows on every sequential run.
            WriterPipeline::Transformation if unique_key.is_empty() => {
                RerunSafety::not_safe(format!(
                    "incremental append with no unique_key: a second run can append the same \
                     rows again on {warehouse}"
                ))
            }
            WriterPipeline::Transformation => classify_upsert(target, warehouse),
        },
        MaterializationStrategy::Merge { unique_key, .. } => {
            if unique_key.is_empty() {
                RerunSafety::not_safe("merge with no unique_key")
            } else {
                classify_upsert(target, warehouse)
            }
        }
        // `INSERT ... REPLACE WHERE` on Databricks
        // (`DatabricksSqlDialect::insert_overwrite_partition`). Whether Delta
        // fails one of two overlapping replaces of the same window, also an
        // empty one, is not proven. The test that would prove it is a live
        // two-session test: two runs replace the same window at once, and the
        // table must hold the window once.
        MaterializationStrategy::TimeInterval { .. } => match target {
            TargetWarehouse::DuckDbSingleWriter => RerunSafety::Safe,
            TargetWarehouse::DuckDbShared
            | TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => not_proven("partition replacement", warehouse),
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
            TargetWarehouse::DuckDbShared => not_proven("delete_insert", warehouse),
        },
        // Unreachable from a loaded model: the loader lowers a declared
        // `microbatch` to `time_interval` (`models::normalize_transformation_strategy`),
        // so a microbatch model gets the time_interval class. This arm covers
        // hand-built IR only, and fails closed.
        MaterializationStrategy::Microbatch { .. } => RerunSafety::not_safe(
            "microbatch has no run path of its own; a loaded model runs as time_interval",
        ),
        // The uniform writer replaces the whole table in one Delta commit,
        // guarded by a conditional PUT; on a conflict it re-reads the live
        // set and recomputes (`rocky_iceberg::uniform_writer`).
        MaterializationStrategy::ContentAddressed { .. } => RerunSafety::Safe,
        MaterializationStrategy::Snapshot(_) => RerunSafety::not_safe(
            "a snapshot model closes old versions and inserts new ones in separate \
             statements with no transaction, so two runs can record a change twice",
        ),
    }
}

/// A `MERGE` on a unique key. A second MERGE of the same rows after the first
/// is a no-op, so on a single-writer DuckDB file, where the runs cannot
/// overlap, it is safe. Everywhere else two overlapping MERGEs that both
/// insert the same new key can both succeed: on Databricks, row-level
/// concurrency (DBR 14.3+, deletion vectors) need not report a conflict. The
/// test that would prove Databricks safe is a live two-session test: two runs
/// MERGE the same new key at once, and the table must hold the key once.
fn classify_upsert(target: &TargetWarehouse, warehouse: &str) -> RerunSafety {
    match target {
        TargetWarehouse::DuckDbSingleWriter => RerunSafety::Safe,
        TargetWarehouse::DuckDbShared
        | TargetWarehouse::Databricks
        | TargetWarehouse::Snowflake
        | TargetWarehouse::BigQuery
        | TargetWarehouse::Unverified { .. } => not_proven("a MERGE", warehouse),
    }
}

fn not_proven(what: &str, warehouse: &str) -> RerunSafety {
    RerunSafety::not_safe(format!(
        "{what} on {warehouse} is not proven under concurrent writers"
    ))
}

fn unsupported(what: &str, warehouse: &str) -> RerunSafety {
    RerunSafety::not_safe(format!("{warehouse} does not support {what}"))
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
        incremental_lookback(unique_key, None)
    }

    fn incremental_lookback(
        unique_key: Vec<Arc<str>>,
        lookback: Option<&str>,
    ) -> MaterializationStrategy {
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
                    [S, N, S, S, S, N],
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
                (merge(key()), pipeline, [S, N, N, N, N, N]),
                (merge(Vec::new()), pipeline, [N, N, N, N, N, N]),
                (interval.clone(), pipeline, [S, N, N, N, N, N]),
                (delete_insert.clone(), pipeline, [S, N, N, N, N, N]),
                (microbatch.clone(), pipeline, [N, N, N, N, N, N]),
                (content.clone(), pipeline, [S, S, S, S, S, N]),
                (snapshot(), pipeline, [N, N, N, N, N, N]),
            ]);
        }
        rows.extend([
            (incremental(Vec::new()), R, [N, N, N, N, N, N]),
            (incremental(key()), R, [N, N, N, N, N, N]),
            (incremental(Vec::new()), T, [N, N, N, N, N, N]),
            (
                incremental_lookback(Vec::new(), Some("0 days")),
                T,
                [N, N, N, N, N, N],
            ),
            (
                incremental_lookback(Vec::new(), Some("3 days")),
                T,
                [N, N, N, N, N, N],
            ),
            (incremental(key()), T, [S, N, N, N, N, N]),
            (
                incremental_lookback(key(), Some("3 days")),
                T,
                [S, N, N, N, N, N],
            ),
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

    #[test]
    fn databricks_upserts_and_partition_replacement_are_not_proven() {
        let interval = MaterializationStrategy::TimeInterval {
            time_column: "day".to_string(),
            granularity: TimeGrain::Day,
            window: None,
        };
        for strategy in [merge(key()), incremental(key()), interval] {
            let class = classify_strategy(
                &strategy,
                WriterPipeline::Transformation,
                &TargetWarehouse::Databricks,
            );
            let RerunSafety::NotSafe { reason } = class else {
                panic!("{}: Databricks is not proven", witness(&strategy));
            };
            assert!(
                reason.contains("not proven under concurrent writers"),
                "{reason}"
            );
        }
    }

    #[test]
    fn a_keyless_lookback_is_not_safe_even_on_a_duckdb_file() {
        let class = classify_strategy(
            &incremental_lookback(Vec::new(), Some("1 hour")),
            WriterPipeline::Transformation,
            &TargetWarehouse::DuckDbSingleWriter,
        );
        let RerunSafety::NotSafe { reason } = class else {
            panic!("a keyless lookback re-appends its window");
        };
        assert!(reason.contains("lookback"), "{reason}");
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
            ("b".to_string(), incremental(key())),
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
            panic!("an upsert model on Databricks is not safe");
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
            Ok(MaterializationStrategy::View),
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
            ResolvedWrites::Strategies(vec![
                Ok(MaterializationStrategy::FullRefresh),
                Ok(merge(key())),
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
    fn a_replication_pipeline_with_a_custom_check_is_not_safe() {
        let with_custom = pipeline(&format!(
            "type = \"replication\"\n\
             [source]\nadapter = \"src\"\n\
             [source.schema_pattern]\nprefix = \"src__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
             [target]\nadapter = \"wh\"\ncatalog_template = \"c\"\nschema_template = \"s\"\n\
             [[checks.custom]]\nname = \"c\"\nsql = \"DELETE FROM t\"\n"
        ));
        let safe = ResolvedWrites::Strategies(vec![Ok(MaterializationStrategy::FullRefresh)]);
        let class = classify_pipeline(&with_custom, &TargetWarehouse::Databricks, &safe);
        let RerunSafety::NotSafe { reason } = class else {
            panic!("custom check SQL runs as written");
        };
        assert!(reason.contains("checks.custom"), "{reason}");
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
        let custom = format!("{base}[[checks.custom]]\nname = \"c\"\nsql = \"DELETE FROM t\"\n");
        let class = classify_pipeline(&pipeline(&custom), &target, &no_writes);
        assert!(!class.is_safe(), "custom SQL: {class:?}");
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
        let duckdb_path = |path: &str| {
            let path = path.replace('\\', "\\\\");
            TargetWarehouse::from_adapter(Some(&adapter(&format!(
                "type = \"duckdb\"\npath = \"{path}\"\n"
            ))))
        };
        for single in [
            "x.db",
            "./data/warehouse.duckdb",
            "/var/lib/rocky/wh.duckdb",
            ":memory:",
            "C:\\data\\wh.duckdb",
            "d:/data/wh.duckdb",
        ] {
            assert_eq!(
                duckdb_path(single),
                TargetWarehouse::DuckDbSingleWriter,
                "{single}"
            );
        }
        for shared in [
            "md:prod",
            "motherduck:prod",
            "ducklake:meta.ducklake",
            "postgres:dbname=x",
            "sqlite:x.db",
            " md:prod",
            ":memory:shared",
            "C:",
            "c:wh.duckdb",
            "data/a:b.db",
            "",
        ] {
            assert_eq!(
                duckdb_path(shared),
                TargetWarehouse::DuckDbShared,
                "{shared:?}"
            );
        }
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
