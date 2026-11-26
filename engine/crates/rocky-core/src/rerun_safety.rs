//! Whether a pipeline is safe to run a second time.
//!
//! A pipeline is *rerun-safe* when a second run, sequential or overlapping
//! the first, leaves the target exactly as one run would. The scheduler will
//! retry, re-run and fail over only pipelines this module classifies as
//! [`RerunSafety::Safe`]. A false `Safe` writes data twice, so the
//! classifier fails closed: anything it cannot prove is
//! [`RerunSafety::NotSafe`] with a reason.
//!
//! A write is `Safe` only when both of these hold:
//!
//! 1. The SQL Rocky generates makes a second run a no-op by construction. The
//!    proof must not rest on the user's data (non-null keys, a model that
//!    filters on the right column) or on how the warehouse resolves
//!    concurrent writers.
//! 2. The write is **one statement that replaces the whole object**, or it
//!    writes nothing.
//!
//! So the safe set is small: a `full_refresh` that is one
//! `CREATE OR REPLACE TABLE ... AS`, a `view`, a `materialized_view` or a
//! `dynamic_table` that is one `CREATE OR REPLACE`, a `content_addressed`
//! model (one replace commit to the Delta log), an `ephemeral` model (it
//! writes nothing), and a quality pipeline that only reads. Every upsert,
//! append, partition replace and snapshot is `NotSafe` on every adapter,
//! and the reason says what would make it provable.
//!
//! A whole-object replace is a no-op on a second run only if it does not
//! read what it writes. A model that reads its own target, directly, through
//! an ephemeral model or through another model of the same pipeline, builds
//! on the previous run, so it is `NotSafe` too ([`classify_pipeline`] checks
//! this from [`ResolvedModel::reads`]).
//!
//! The classification judges the steady-state statements a run executes. It
//! does not audit the first-run bootstrap or the drift rebuild paths. It
//! does not classify `[hook.*]` shell commands or webhooks.

use serde::{Deserialize, Serialize};

use rocky_ir::{MaterializationStrategy, ModelIr, TargetRef};

use crate::config::{AdapterConfig, ChecksConfig, PipelineConfig, QuarantineMode};
use crate::cost::WarehouseType;

/// The rerun-safety class of one pipeline or one materialization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "class", rename_all = "snake_case")]
pub enum RerunSafety {
    /// A second run, sequential or overlapping, leaves the target as one
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
    /// DuckDB with no attached catalog, on a plain local file path, in
    /// memory, or with no `path`. A full refresh there is one
    /// `CREATE OR REPLACE TABLE ... AS`.
    DuckDbPlain,
    /// Any other DuckDB target: an attached catalog (Iceberg, DuckLake), or a
    /// `path` with a `scheme:` prefix (`md:`, `ducklake:`, ...) that DuckDB
    /// routes to another store. A full refresh into an attached Iceberg
    /// catalog is several statements (`sql_gen::in_place_overwrite_statements`),
    /// and Rocky does not resolve which catalog each model writes here.
    DuckDbAttached,
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
                let plain_path = adapter
                    .path
                    .as_ref()
                    .is_none_or(|path| duckdb_path_is_plain(path.expose()));
                if plain_path && adapter.attach.is_empty() {
                    Self::DuckDbPlain
                } else {
                    Self::DuckDbAttached
                }
            }
            WarehouseType::Databricks => Self::Databricks,
            WarehouseType::Snowflake => Self::Snowflake,
            WarehouseType::BigQuery => Self::BigQuery,
        }
    }

    fn label(&self) -> &str {
        match self {
            Self::DuckDbPlain | Self::DuckDbAttached => "DuckDB",
            Self::Databricks => "Databricks",
            Self::Snowflake => "Snowflake",
            Self::BigQuery => "BigQuery",
            Self::Unverified { adapter_type } => adapter_type,
        }
    }
}

/// Whether a DuckDB `path` names a plain database: one whose only catalog is
/// its own, so a full refresh is one `CREATE OR REPLACE`. An allowlist: only
/// `:memory:` and a plain filesystem path count. DuckDB reads a `scheme:`
/// prefix (`md:`, `motherduck:`, `ducklake:`, an extension's own prefix) as a
/// route to another store, whose writes Rocky has not verified. So any `:`
/// makes the path not plain, except a Windows drive letter (`C:\`, `C:/`) at
/// the start. An empty path is not plain either: it is not a file Rocky can
/// name.
fn duckdb_path_is_plain(path: &str) -> bool {
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

/// One model of a transformation pipeline, with what its SQL reads.
#[derive(Debug, Clone)]
pub struct ResolvedModel {
    /// The model name.
    pub name: String,
    /// The materialization the run uses.
    pub strategy: MaterializationStrategy,
    /// The table the model writes.
    pub target: TargetRef,
    /// The tables the model's SQL reads, lower-cased, CTE names dropped
    /// (`rocky_sql::lineage::referenced_tables`). `Err` when the SQL could
    /// not be parsed.
    pub reads: Result<Vec<String>, String>,
}

impl ResolvedModel {
    /// Resolve a model from its IR, reading its SQL with
    /// `rocky_sql::lineage::referenced_tables`.
    #[must_use]
    pub fn from_ir(ir: &ModelIr) -> Self {
        Self {
            name: ir.name.to_string(),
            strategy: ir.materialization.clone(),
            target: ir.target.clone(),
            reads: rocky_sql::lineage::referenced_tables(&ir.sql),
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
    /// A transformation pipeline: each model it builds.
    Models(Vec<ResolvedModel>),
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

/// What every non-replace reason ends with.
const NOT_PROVABLE: &str = "not provable from the generated SQL alone";

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
        PipelineConfig::Snapshot(_) => RerunSafety::not_safe(format!(
            "a snapshot pipeline closes old versions and inserts new ones in separate \
             statements, so a second run can record a change twice; {NOT_PROVABLE}"
        )),
        PipelineConfig::Load(_) => RerunSafety::not_safe(format!(
            "a load pipeline re-ingests every file on each run, so a second run \
             duplicates the rows; {NOT_PROVABLE}"
        )),
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

fn classify_models(models: &[ResolvedModel], target: &TargetWarehouse) -> RerunSafety {
    if models.is_empty() {
        return RerunSafety::not_safe("the pipeline builds no models");
    }
    for model in models {
        if let RerunSafety::NotSafe { reason } =
            classify_strategy(&model.strategy, WriterPipeline::Transformation, target)
        {
            return RerunSafety::not_safe(format!("model '{}': {reason}", model.name));
        }
    }
    for model in models {
        if let Err(e) = &model.reads {
            return RerunSafety::not_safe(format!(
                "model '{}': its SQL could not be parsed, so Rocky cannot check that it does \
                 not read its own output ({e})",
                model.name
            ));
        }
    }
    if let Some(model) = first_model_reading_its_own_output(models) {
        return RerunSafety::not_safe(format!(
            "model '{}' reads its own output, directly or through other models of the \
             pipeline, so each run builds on the last one",
            model.name
        ));
    }
    RerunSafety::Safe
}

/// The first non-ephemeral model that can reach its own target through the
/// reads of the pipeline's models. A read reaches a model when it names the
/// model's target table (see [`read_names_model`]) or, for an ephemeral
/// model, its name. An ephemeral model is inlined into its consumers, so
/// what it reads, they read.
fn first_model_reading_its_own_output(models: &[ResolvedModel]) -> Option<&ResolvedModel> {
    let edges: Vec<Vec<usize>> = models
        .iter()
        .map(|reader| {
            let reads = reader.reads.as_deref().unwrap_or_default();
            (0..models.len())
                .filter(|&j| reads.iter().any(|read| read_names_model(read, &models[j])))
                .collect()
        })
        .collect();
    models.iter().enumerate().find_map(|(start, model)| {
        if matches!(model.strategy, MaterializationStrategy::Ephemeral) {
            return None;
        }
        let mut seen = vec![false; models.len()];
        let mut stack = edges[start].clone();
        while let Some(next) = stack.pop() {
            if next == start {
                return Some(model);
            }
            if !std::mem::replace(&mut seen[next], true) {
                stack.extend(edges[next].iter().copied());
            }
        }
        None
    })
}

/// Whether `read` (a dotted, lower-cased name from the model's SQL) may name
/// `model`'s output. The match errs toward yes. Parts compare
/// case-insensitively with quotes dropped, and an empty target part matches
/// any part. A bare name matches the target table in any schema. One
/// qualifier matches when it is the target's schema or its catalog
/// (`schema.table` or `catalog.table`). Two or more qualifiers match when
/// the last two are the target's catalog and schema.
fn read_names_model(read: &str, model: &ResolvedModel) -> bool {
    let fold = |part: &str| {
        part.trim_matches(|c| matches!(c, '"' | '`' | '[' | ']'))
            .to_ascii_lowercase()
    };
    if matches!(model.strategy, MaterializationStrategy::Ephemeral)
        && fold(read) == fold(&model.name)
    {
        return true;
    }
    let parts: Vec<String> = read.split('.').map(fold).collect();
    let Some((table, qualifiers)) = parts.split_last() else {
        return false;
    };
    if *table != fold(&model.target.table) {
        return false;
    }
    let schema = fold(&model.target.schema);
    let catalog = fold(&model.target.catalog);
    let is = |part: &String, expected: &str| expected.is_empty() || part == expected;
    let mut rev = qualifiers.iter().rev();
    let Some(last) = rev.next() else {
        return true;
    };
    let Some(before) = rev.next() else {
        return is(last, &schema) || is(last, &catalog);
    };
    is(last, &schema) && is(before, &catalog)
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

/// Classify one materialization written by `pipeline` to `target`, from the
/// strategy alone. [`classify_pipeline`] also checks that a transformation
/// model does not read its own output.
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
            | MaterializationStrategy::Snapshot(_) => unverified(adapter_type),
        };
    }
    let warehouse = target.label();
    match strategy {
        // One `CREATE OR REPLACE TABLE ... AS` (`SqlDialect::create_table_as`
        // on DuckDB, Databricks, Snowflake and BigQuery, for a transformation
        // model and for a replication table; the lakehouse `format` DDL is
        // one statement too). On an attached Iceberg catalog a full refresh
        // is a multi-statement in-place overwrite
        // (`sql_gen::in_place_overwrite_statements`).
        MaterializationStrategy::FullRefresh => match target {
            TargetWarehouse::DuckDbPlain
            | TargetWarehouse::Databricks
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery => RerunSafety::Safe,
            TargetWarehouse::DuckDbAttached => RerunSafety::not_safe(
                "a full refresh on a DuckDB target with an attached catalog or a `scheme:` \
                 path can be several statements (an attached Iceberg catalog is overwritten \
                 in place), not one replace",
            ),
            TargetWarehouse::Unverified { adapter_type } => unverified(adapter_type),
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
            TargetWarehouse::DuckDbPlain
            | TargetWarehouse::DuckDbAttached
            | TargetWarehouse::Unverified { .. } => unsupported("a materialized view", warehouse),
        },
        // One `CREATE OR REPLACE DYNAMIC TABLE` (Snowflake `dynamic_table_ddl`).
        MaterializationStrategy::DynamicTable { .. } => match target {
            TargetWarehouse::Snowflake => RerunSafety::Safe,
            TargetWarehouse::DuckDbPlain
            | TargetWarehouse::DuckDbAttached
            | TargetWarehouse::Databricks
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => unsupported("a dynamic table", warehouse),
        },
        MaterializationStrategy::Incremental { unique_key, .. } => match pipeline {
            WriterPipeline::Replication => RerunSafety::not_safe(format!(
                "replication incremental appends the rows past the watermark in the Rocky \
                 state store, so a second run before the first records it copies the same \
                 rows again; {NOT_PROVABLE}"
            )),
            // A plain INSERT of the rows the model's filter lets through
            // (`MAX(watermark) - lookback`, `@incremental_filter` or a
            // `filter_column`). Whether a row comes through twice depends on
            // the model and the data.
            WriterPipeline::Transformation if unique_key.is_empty() => {
                RerunSafety::not_safe(format!(
                    "incremental with no unique_key appends every row its filter lets \
                     through, so a row the filter passes twice, such as one in the lookback \
                     window, is appended twice; {NOT_PROVABLE}"
                ))
            }
            // A MERGE on the key, with the same NULL-key hole as `merge`.
            WriterPipeline::Transformation => RerunSafety::not_safe(format!(
                "incremental with a unique_key is a MERGE: a row with a NULL key matches no \
                 target row, so every run inserts it again; {NOT_PROVABLE}"
            )),
        },
        // `MERGE ... ON t.k = s.k`: a NULL key never matches, so it is
        // inserted again on every run. Nothing rejects a nullable key.
        MaterializationStrategy::Merge { unique_key, .. } => {
            if unique_key.is_empty() {
                RerunSafety::not_safe(format!(
                    "merge with no unique_key matches no target row, so every run inserts \
                     its rows again; {NOT_PROVABLE}"
                ))
            } else {
                RerunSafety::not_safe(format!(
                    "merge: a row with a NULL unique_key matches no target row, so every run \
                     inserts it again; {NOT_PROVABLE}"
                ))
            }
        }
        // Delete the window on `time_column`, then insert the model's rows.
        // A row the model returns outside the window is not deleted by the
        // next run, and nothing checks that the model filters on
        // `time_column`.
        MaterializationStrategy::TimeInterval { .. } => RerunSafety::not_safe(format!(
            "time_interval deletes the window on time_column and inserts the model's rows, \
             so a row the model returns outside the window is inserted again on every run; \
             {NOT_PROVABLE}"
        )),
        MaterializationStrategy::DeleteInsert { .. } => RerunSafety::not_safe(format!(
            "delete_insert deletes the partitions it rewrites and inserts the rows in \
             separate statements on {warehouse}, not one replace of the table; {NOT_PROVABLE}"
        )),
        // Unreachable from a loaded model: the loader lowers a declared
        // `microbatch` to `time_interval` (`models::normalize_transformation_strategy`).
        // This arm covers hand-built IR only.
        MaterializationStrategy::Microbatch { .. } => RerunSafety::not_safe(format!(
            "microbatch runs as time_interval, which deletes a window and inserts the \
             model's rows, so a row outside the window is inserted again on every run; \
             {NOT_PROVABLE}"
        )),
        // The uniform writer replaces the whole table in one Delta commit, a
        // conditional PUT of the log entry. On a conflict it re-reads the
        // live set and recomputes; an unchanged output writes no commit
        // (`rocky_iceberg::uniform_writer`). The run then syncs the table
        // with `MSCK REPAIR TABLE ... SYNC METADATA` on every run, a
        // Databricks statement; other adapters are not verified.
        MaterializationStrategy::ContentAddressed { .. } => match target {
            TargetWarehouse::Databricks => RerunSafety::Safe,
            TargetWarehouse::DuckDbPlain
            | TargetWarehouse::DuckDbAttached
            | TargetWarehouse::Snowflake
            | TargetWarehouse::BigQuery
            | TargetWarehouse::Unverified { .. } => RerunSafety::not_safe(format!(
                "content_addressed is verified on Databricks only: the run syncs its table \
                 with MSCK REPAIR TABLE, not verified on {warehouse}"
            )),
        },
        MaterializationStrategy::Snapshot(_) => RerunSafety::not_safe(format!(
            "a snapshot model closes old versions and inserts new ones in separate \
             statements, so a second run can record a change twice; {NOT_PROVABLE}"
        )),
    }
}

fn unverified(adapter_type: &str) -> RerunSafety {
    RerunSafety::not_safe(format!(
        "rerun safety is not verified on adapter type '{adapter_type}'"
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
            TargetWarehouse::DuckDbPlain,
            TargetWarehouse::DuckDbAttached,
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

    fn interval() -> MaterializationStrategy {
        MaterializationStrategy::TimeInterval {
            time_column: "day".to_string(),
            granularity: TimeGrain::Day,
            window: None,
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
    /// plain DuckDB, attached DuckDB, Databricks, Snowflake, BigQuery,
    /// unverified adapter. Only whole-object replaces and writes of nothing
    /// are safe; every upsert, append and partition replace is not, on every
    /// adapter.
    fn table() -> Vec<(MaterializationStrategy, WriterPipeline, [bool; 6])> {
        use WriterPipeline::{Replication as R, Transformation as T};
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
                (merge(key()), pipeline, [N, N, N, N, N, N]),
                (merge(Vec::new()), pipeline, [N, N, N, N, N, N]),
                (interval(), pipeline, [N, N, N, N, N, N]),
                (delete_insert.clone(), pipeline, [N, N, N, N, N, N]),
                (microbatch.clone(), pipeline, [N, N, N, N, N, N]),
                (content.clone(), pipeline, [N, N, S, N, N, N]),
                (snapshot(), pipeline, [N, N, N, N, N, N]),
            ]);
        }
        for pipeline in [R, T] {
            rows.extend([
                (incremental(Vec::new()), pipeline, [N, N, N, N, N, N]),
                (incremental(key()), pipeline, [N, N, N, N, N, N]),
                (
                    incremental_lookback(Vec::new(), Some("0 days")),
                    pipeline,
                    [N, N, N, N, N, N],
                ),
                (
                    incremental_lookback(Vec::new(), Some("3 days")),
                    pipeline,
                    [N, N, N, N, N, N],
                ),
                (
                    incremental_lookback(key(), Some("3 days")),
                    pipeline,
                    [N, N, N, N, N, N],
                ),
            ]);
        }
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

    /// Every upsert, append and partition replace says why it is not safe
    /// and that the generated SQL alone does not prove it, on every adapter
    /// Rocky verifies, a plain DuckDB file included.
    #[test]
    fn non_replace_writes_name_the_hole_on_every_adapter() {
        let cases = [
            (
                merge(key()),
                WriterPipeline::Transformation,
                "NULL unique_key",
            ),
            (merge(key()), WriterPipeline::Replication, "NULL unique_key"),
            (
                merge(Vec::new()),
                WriterPipeline::Transformation,
                "no unique_key",
            ),
            (
                incremental(key()),
                WriterPipeline::Transformation,
                "NULL key",
            ),
            (
                incremental_lookback(key(), Some("1 hour")),
                WriterPipeline::Transformation,
                "NULL key",
            ),
            (
                incremental_lookback(Vec::new(), Some("1 hour")),
                WriterPipeline::Transformation,
                "lookback",
            ),
            (
                incremental(Vec::new()),
                WriterPipeline::Replication,
                "watermark",
            ),
            (
                interval(),
                WriterPipeline::Transformation,
                "outside the window",
            ),
            (
                MaterializationStrategy::DeleteInsert {
                    partition_by: vec![Arc::from("day")],
                },
                WriterPipeline::Transformation,
                "separate statements",
            ),
        ];
        let verified = warehouses()
            .into_iter()
            .filter(|w| !matches!(w, TargetWarehouse::Unverified { .. }));
        for target in verified {
            for (strategy, pipeline, hole) in &cases {
                let class = classify_strategy(strategy, *pipeline, &target);
                let RerunSafety::NotSafe { reason } = class else {
                    panic!("{} / {pipeline:?} / {target:?} is safe", witness(strategy));
                };
                assert!(
                    reason.contains(hole) && reason.ends_with(NOT_PROVABLE),
                    "{} / {target:?}: {reason}",
                    witness(strategy)
                );
            }
        }
    }

    fn pipeline(toml: &str) -> PipelineConfig {
        toml::from_str(toml).expect("pipeline config parses")
    }

    fn transformation() -> PipelineConfig {
        pipeline("type = \"transformation\"\n[target]\nadapter = \"wh\"\n")
    }

    /// A model writing `c.s.<table>` and reading `reads`.
    fn model(
        name: &str,
        table: &str,
        strategy: MaterializationStrategy,
        reads: &[&str],
    ) -> ResolvedModel {
        ResolvedModel {
            name: name.to_string(),
            strategy,
            target: TargetRef {
                catalog: "c".to_string(),
                schema: "s".to_string(),
                table: table.to_string(),
            },
            reads: Ok(reads.iter().map(|r| (*r).to_string()).collect()),
        }
    }

    fn models(models: Vec<ResolvedModel>) -> ResolvedWrites {
        ResolvedWrites::Models(models)
    }

    #[test]
    fn a_transformation_pipeline_is_safe_only_when_every_model_is() {
        let full = MaterializationStrategy::FullRefresh;
        let writes = models(vec![
            model("a", "a", full.clone(), &["raw.a"]),
            model("b", "b", MaterializationStrategy::View, &["a"]),
        ]);
        let class = classify_pipeline(&transformation(), &TargetWarehouse::Databricks, &writes);
        assert_eq!(class, RerunSafety::Safe);

        let writes = models(vec![
            model("a", "a", full, &["raw.a"]),
            model("b", "b", merge(key()), &["a"]),
        ]);
        let class = classify_pipeline(&transformation(), &TargetWarehouse::DuckDbPlain, &writes);
        let RerunSafety::NotSafe { reason } = class else {
            panic!("a merge model is not safe");
        };
        assert!(reason.starts_with("model 'b': merge"), "{reason}");
    }

    #[test]
    fn a_model_that_reads_its_own_output_is_not_safe() {
        let full = MaterializationStrategy::FullRefresh;
        let target = TargetWarehouse::DuckDbPlain;
        let not_safe = |writes: &ResolvedWrites, name: &str| {
            let class = classify_pipeline(&transformation(), &target, writes);
            let RerunSafety::NotSafe { reason } = class else {
                panic!("{writes:?} reads its own output");
            };
            assert!(
                reason.starts_with(&format!("model '{name}' reads its own output")),
                "{reason}"
            );
        };
        // Directly: bare, schema- or catalog-qualified, fully qualified, quoted.
        for read in [
            "orders",
            "s.orders",
            "c.orders",
            "c.s.orders",
            "x.c.s.orders",
            "\"c\".\"S\".\"Orders\"",
        ] {
            not_safe(
                &models(vec![model("o", "orders", full.clone(), &[read])]),
                "o",
            );
        }
        // A target part that is not set matches any qualifier.
        let mut no_schema = model("o", "orders", full.clone(), &["c.anything.orders"]);
        no_schema.target.schema = String::new();
        not_safe(&models(vec![no_schema]), "o");
        // Through an ephemeral model, which its consumer inlines.
        not_safe(
            &models(vec![
                model("e", "e", MaterializationStrategy::Ephemeral, &["orders"]),
                model("o", "orders", full.clone(), &["e"]),
            ]),
            "o",
        );
        // Through another model: a qualified read derives no DAG edge, so
        // two models can each read the other's output.
        not_safe(
            &models(vec![
                model("a", "a", full.clone(), &["c.s.b"]),
                model("b", "b", MaterializationStrategy::View, &["c.s.a"]),
            ]),
            "a",
        );
        // A same-named table in another schema is not the target.
        for read in ["raw.orders", "other.s.orders", "c.raw.orders"] {
            let writes = models(vec![model("o", "orders", full.clone(), &[read])]);
            assert_eq!(
                classify_pipeline(&transformation(), &target, &writes),
                RerunSafety::Safe,
                "{read}"
            );
        }
        // SQL the parser cannot read is not safe either.
        let mut unparsed = model("o", "orders", full, &[]);
        unparsed.reads = Err("unexpected token".to_string());
        let class = classify_pipeline(&transformation(), &target, &models(vec![unparsed]));
        let RerunSafety::NotSafe { reason } = class else {
            panic!("unparsed SQL is not safe");
        };
        assert!(reason.contains("could not be parsed"), "{reason}");
    }

    #[test]
    fn a_resolved_model_reads_its_sql_with_the_lineage_parser() {
        let ir = ModelIr::transformation(
            TargetRef {
                catalog: "c".to_string(),
                schema: "s".to_string(),
                table: "orders".to_string(),
            },
            MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT id FROM raw.orders WHERE id NOT IN (SELECT id FROM c.s.orders)".to_string(),
            rocky_ir::GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        let resolved = ResolvedModel::from_ir(&ir);
        let reads = resolved.reads.clone().expect("the SQL parses");
        assert!(reads.contains(&"c.s.orders".to_string()), "{reads:?}");
        let class = classify_pipeline(
            &transformation(),
            &TargetWarehouse::Databricks,
            &models(vec![resolved]),
        );
        assert!(!class.is_safe(), "{class:?}");
    }

    #[test]
    fn unknown_writes_are_not_safe() {
        let target = TargetWarehouse::DuckDbPlain;
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
        let with_custom = pipeline(
            "type = \"replication\"\n\
             [source]\nadapter = \"src\"\n\
             [source.schema_pattern]\nprefix = \"src__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
             [target]\nadapter = \"wh\"\ncatalog_template = \"c\"\nschema_template = \"s\"\n\
             [[checks.custom]]\nname = \"c\"\nsql = \"DELETE FROM t\"\n",
        );
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
                let RerunSafety::NotSafe { reason } = class else {
                    panic!("{} is not safe", pc.pipeline_type_str());
                };
                assert!(reason.ends_with(NOT_PROVABLE), "{reason}");
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
            TargetWarehouse::DuckDbPlain
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter("type = \"duckdb\"\n"))),
            TargetWarehouse::DuckDbPlain
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter(
                "type = \"duckdb\"\npath = \"md:prod\"\n"
            ))),
            TargetWarehouse::DuckDbAttached
        );
        assert_eq!(
            TargetWarehouse::from_adapter(Some(&adapter(
                "type = \"duckdb\"\npath = \"x.db\"\n[[attach]]\nalias = \"dl\"\ntype = \"ducklake\"\n\
                 metadata_path = \"m.ducklake\"\ndata_path = \"d/\"\n"
            ))),
            TargetWarehouse::DuckDbAttached
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
        for plain in [
            "x.db",
            "./data/warehouse.duckdb",
            "/var/lib/rocky/wh.duckdb",
            ":memory:",
            "C:\\data\\wh.duckdb",
            "d:/data/wh.duckdb",
        ] {
            assert_eq!(duckdb_path(plain), TargetWarehouse::DuckDbPlain, "{plain}");
        }
        for attached in [
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
                duckdb_path(attached),
                TargetWarehouse::DuckDbAttached,
                "{attached:?}"
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
