//! SCD Type 2 snapshots of transformation models (`type = "snapshot"`).
//!
//! The `snapshot` *pipeline* ([`crate::snapshots`]) historizes one source
//! table. This module historizes a model's SELECT, the way a dbt snapshot
//! does, so a snapshot runs inside the normal model DAG and downstream models
//! can read it.
//!
//! It reuses the SCD2 building blocks the pipeline generator uses: the
//! dialect's snapshot hooks for column resolution and quoting
//! ([`SqlDialect::snapshot_source_reference`],
//! [`SqlDialect::snapshot_insert_columns`]), its correlated-UPDATE target
//! ([`SqlDialect::snapshot_update_target`]), its NULL-safe inequality and its
//! dbt-compatible hash ([`SqlDialect::surrogate_key_expr`]).
//!
//! # Statement order and reruns
//!
//! A run is a few separate statements. Rocky does not wrap them in a
//! transaction, because not every warehouse can (Databricks has no
//! multi-statement transactions). Instead the order is chosen so that a run
//! that stops part-way can simply be run again:
//!
//! ```text
//! 1. MERGE   close the current version of every changed key
//! 2. INSERT  a new current version for every key with no current version
//!            (new keys, keys closed by step 1, revived keys)
//! 3. hard deletes (optional)
//!    invalidate: UPDATE close current versions whose key left the result
//!    new_record: INSERT a deletion marker (skipped if one is current),
//!                then UPDATE close the version it replaces
//! ```
//!
//! Step 2 only inserts where no current version exists, so repeating it is
//! harmless. A stop between steps 1 and 2 leaves closed keys with no current
//! version; the next run's step 2 opens them. A second run over an unchanged
//! source matches nothing in any step, so it writes nothing.
//!
//! Every statement in one run uses one timestamp literal for "now", so a
//! check-strategy version closed in step 1 and its successor inserted in step
//! 2 meet exactly (`valid_to` = next `valid_from`).

use chrono::{DateTime, Utc};
use rocky_ir::{
    SnapshotChangeStrategy, SnapshotCheckColumns, SnapshotHardDeletes, SnapshotMetaColumns,
    SnapshotSpec,
};
use rocky_sql::validation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::sql_gen::SqlGenError;
use crate::traits::SqlDialect;

// ---------------------------------------------------------------------------
// Sidecar TOML shapes
// ---------------------------------------------------------------------------

/// `unique_key` accepts one column name or a list (dbt parity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SnapshotUniqueKey {
    One(String),
    Many(Vec<String>),
}

impl SnapshotUniqueKey {
    /// The key columns in declaration order.
    pub fn columns(&self) -> Vec<String> {
        match self {
            Self::One(c) => vec![c.clone()],
            Self::Many(cs) => cs.clone(),
        }
    }
}

/// The `strategy` key inside a snapshot `[strategy]` block.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStrategyKind {
    Timestamp,
    Check,
}

/// `check_cols` accepts a list of columns or the string `"all"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SnapshotCheckColsConfig {
    Keyword(String),
    List(Vec<String>),
}

/// The sidecar fields of a `type = "snapshot"` strategy, borrowed from
/// [`crate::models::StrategyConfig::Snapshot`].
#[derive(Debug, Clone, Copy)]
pub struct SnapshotConfigFields<'a> {
    pub unique_key: Option<&'a SnapshotUniqueKey>,
    pub strategy: Option<SnapshotStrategyKind>,
    pub updated_at: Option<&'a str>,
    pub check_cols: Option<&'a SnapshotCheckColsConfig>,
    pub hard_deletes: Option<SnapshotHardDeletes>,
    pub invalidate_hard_deletes: Option<bool>,
    pub meta_columns: Option<&'a SnapshotMetaColumns>,
    pub valid_to_current: Option<&'a str>,
}

/// A lowered snapshot config plus the problems lowering found.
#[derive(Debug, Clone)]
pub struct LoweredSnapshot {
    pub spec: SnapshotSpec,
    /// Config errors the IR cannot represent (an unknown `check_cols`
    /// keyword, conflicting `hard_deletes` settings), plus
    /// [`SnapshotSpec::problems`]. Non-empty means the model must not run;
    /// the compiler reports each one as E049.
    pub problems: Vec<String>,
}

/// Lower the sidecar fields to the IR spec. Never fails: anything invalid is
/// recorded in [`LoweredSnapshot::problems`] and represented in the spec by
/// an empty value that SQL generation refuses.
pub fn lower_snapshot_config(f: SnapshotConfigFields<'_>) -> LoweredSnapshot {
    let mut problems = Vec::new();

    let unique_key = f
        .unique_key
        .map(|k| {
            k.columns()
                .into_iter()
                .map(|c| std::sync::Arc::from(c.trim()))
                .collect()
        })
        .unwrap_or_default();

    // dbt requires `strategy`; infer it when exactly one of the two
    // strategy-specific keys is present, so a minimal sidecar still reads.
    let kind = match (f.strategy, f.updated_at, f.check_cols) {
        (Some(k), _, _) => k,
        (None, Some(_), None) => SnapshotStrategyKind::Timestamp,
        (None, None, Some(_)) => SnapshotStrategyKind::Check,
        (None, Some(_), Some(_)) => {
            problems.push(
                "`strategy` is missing and both `updated_at` and `check_cols` are set; \
                 set `strategy = \"timestamp\"` or `strategy = \"check\"`"
                    .to_string(),
            );
            SnapshotStrategyKind::Timestamp
        }
        (None, None, None) => {
            problems.push(
                "`strategy` is required: `\"timestamp\"` (with `updated_at`) or `\"check\"` \
                 (with `check_cols`)"
                    .to_string(),
            );
            SnapshotStrategyKind::Timestamp
        }
    };

    let change = match kind {
        SnapshotStrategyKind::Timestamp => SnapshotChangeStrategy::Timestamp {
            updated_at: std::sync::Arc::from(f.updated_at.unwrap_or("").trim()),
        },
        SnapshotStrategyKind::Check => {
            let check_cols = match f.check_cols {
                None => SnapshotCheckColumns::Explicit(Vec::new()),
                Some(SnapshotCheckColsConfig::Keyword(word)) => {
                    if word.trim().eq_ignore_ascii_case("all") {
                        SnapshotCheckColumns::All
                    } else {
                        problems.push(format!(
                            "`check_cols = \"{word}\"` is not valid; use a list of columns or \
                             \"all\""
                        ));
                        SnapshotCheckColumns::Explicit(Vec::new())
                    }
                }
                Some(SnapshotCheckColsConfig::List(cols)) => SnapshotCheckColumns::Explicit(
                    cols.iter()
                        .map(|c| std::sync::Arc::from(c.trim()))
                        .collect(),
                ),
            };
            SnapshotChangeStrategy::Check {
                check_cols,
                updated_at: f
                    .updated_at
                    .map(|u| std::sync::Arc::from(u.trim()))
                    .filter(|u: &std::sync::Arc<str>| !u.is_empty()),
            }
        }
    };

    // `invalidate_hard_deletes` is dbt's legacy spelling of
    // `hard_deletes = "invalidate"`. Refuse a combination that disagrees.
    let hard_deletes = match (f.hard_deletes, f.invalidate_hard_deletes) {
        (Some(mode), None) => mode,
        (None, Some(true)) => SnapshotHardDeletes::Invalidate,
        (None, Some(false)) | (None, None) => SnapshotHardDeletes::Ignore,
        (Some(mode), Some(legacy)) => {
            let agrees = (mode == SnapshotHardDeletes::Invalidate) == legacy;
            if !agrees {
                problems.push(format!(
                    "`hard_deletes = \"{}\"` conflicts with `invalidate_hard_deletes = {legacy}`; \
                     keep only `hard_deletes`",
                    mode.as_str()
                ));
            }
            mode
        }
    };

    let spec = SnapshotSpec {
        unique_key,
        change,
        hard_deletes,
        meta_columns: f.meta_columns.cloned().unwrap_or_default(),
        valid_to_current: f.valid_to_current.map(str::to_string),
    };
    problems.extend(spec.problems());
    LoweredSnapshot { spec, problems }
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// The one "now" every statement of a run shares, as a SQL literal.
pub fn run_timestamp_literal(now: DateTime<Utc>) -> String {
    format!(
        "CAST('{}' AS TIMESTAMP)",
        now.format("%Y-%m-%d %H:%M:%S%.6f")
    )
}

fn refuse_invalid(spec: &SnapshotSpec) -> Result<(), SqlGenError> {
    let problems = spec.problems();
    if problems.is_empty() {
        Ok(())
    } else {
        Err(SqlGenError::InvalidRequest(format!(
            "invalid snapshot config (E049): {}",
            problems.join("; ")
        )))
    }
}

fn validated(name: &str) -> Result<&str, SqlGenError> {
    validation::validate_identifier(name)?;
    Ok(name)
}

/// The `valid_to` value a current version carries.
fn current_valid_to(spec: &SnapshotSpec) -> String {
    match &spec.valid_to_current {
        Some(expr) => format!("CAST({} AS TIMESTAMP)", expr.trim()),
        None => "CAST(NULL AS TIMESTAMP)".to_string(),
    }
}

/// `valid_from` of a version built from `alias`'s row.
fn version_timestamp(alias: &str, updated_at_ref: Option<&str>, now: &str) -> String {
    match updated_at_ref {
        Some(u) => format!("CAST({alias}.{u} AS TIMESTAMP)"),
        None => now.to_string(),
    }
}

/// The metadata `(name, value)` pairs for a newly inserted version.
fn new_version_metadata(
    spec: &SnapshotSpec,
    dialect: &dyn SqlDialect,
    key_refs: &[String],
    alias: &str,
    valid_from: &str,
    is_deleted: bool,
) -> Vec<(String, String)> {
    let meta = &spec.meta_columns;
    let mut hash_inputs: Vec<String> = key_refs.iter().map(|k| format!("{alias}.{k}")).collect();
    hash_inputs.push(valid_from.to_string());
    let hash_refs: Vec<&str> = hash_inputs.iter().map(String::as_str).collect();
    let mut pairs = vec![
        (meta.valid_from.clone(), valid_from.to_string()),
        (meta.valid_to.clone(), current_valid_to(spec)),
    ];
    if let Some(flag) = meta.is_current.name() {
        pairs.push((flag.to_string(), "TRUE".to_string()));
    }
    pairs.push((meta.scd_id.clone(), dialect.surrogate_key_expr(&hash_refs)));
    if let Some(updated_at) = &meta.updated_at {
        pairs.push((updated_at.clone(), valid_from.to_string()));
    }
    if spec.hard_deletes == SnapshotHardDeletes::NewRecord {
        pairs.push((
            meta.is_deleted.clone(),
            if is_deleted { "TRUE" } else { "FALSE" }.to_string(),
        ));
    }
    pairs
}

/// The first-run CTAS body: the model's rows, each as its first current
/// version. The caller wraps it in the dialect's non-replacing
/// `CREATE TABLE ... AS` (honoring any lakehouse format).
///
/// Key and `updated_at` references use the configured names unquoted (they
/// are validated identifiers): the column list is not known before the
/// target exists, and an unquoted name resolves the way the model's own
/// unquoted projection did.
pub fn generate_snapshot_bootstrap_select(
    spec: &SnapshotSpec,
    model_sql: &str,
    dialect: &dyn SqlDialect,
    now: DateTime<Utc>,
) -> Result<String, SqlGenError> {
    refuse_invalid(spec)?;
    let now = run_timestamp_literal(now);
    let keys = spec
        .unique_key
        .iter()
        .map(|k| validated(k).map(str::to_string))
        .collect::<Result<Vec<_>, _>>()?;
    let updated_at = spec.change.version_column().map(validated).transpose()?;
    let valid_from = version_timestamp("source", updated_at, &now);
    let meta = new_version_metadata(spec, dialect, &keys, "source", &valid_from, false)
        .into_iter()
        .map(|(name, value)| {
            validated(&name)?;
            Ok(format!(
                "{value} AS {}",
                dialect.snapshot_metadata_identifier(&name)
            ))
        })
        .collect::<Result<Vec<_>, SqlGenError>>()?;
    let body = model_sql.trim().trim_end_matches(';');
    // A NULL key never matches its own previous version, so a NULL-key row
    // would be re-inserted on every run. Such rows are not snapshotted.
    let keys_present = keys
        .iter()
        .map(|k| format!("source.{k} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    Ok(format!(
        "SELECT source.*, {}\nFROM (\n{body}\n) AS source\nWHERE {keys_present}",
        meta.join(", ")
    ))
}

/// Split a described target's columns into the model's columns (in table
/// order) and refuse a target that is not a snapshot of this spec.
///
/// `target_columns` is the target's `describe_table` output. Every metadata
/// column this spec relies on must be present, except `is_deleted` under
/// `new_record`, which the runner adds when missing (see
/// [`missing_is_deleted_column`]).
pub fn snapshot_source_columns(
    spec: &SnapshotSpec,
    target_columns: &[String],
) -> Result<Vec<String>, SqlGenError> {
    snapshot_source_columns_with(spec, target_columns, false)
}

/// [`snapshot_source_columns`], also treating the `is_deleted` column as
/// metadata when `marker_column_is_metadata` (see [`ExistingMarkers`]).
pub fn snapshot_source_columns_with(
    spec: &SnapshotSpec,
    target_columns: &[String],
    marker_column_is_metadata: bool,
) -> Result<Vec<String>, SqlGenError> {
    let meta = &spec.meta_columns;
    let mut required = vec![meta.valid_from.as_str(), meta.valid_to.as_str()];
    if let Some(flag) = meta.is_current.name() {
        required.push(flag);
    }
    required.push(meta.scd_id.as_str());
    if let Some(u) = &meta.updated_at {
        required.push(u.as_str());
    }
    for name in required {
        if !target_columns.iter().any(|c| c.eq_ignore_ascii_case(name)) {
            return Err(SqlGenError::InvalidRequest(format!(
                "the snapshot target has no '{name}' column, so it was not created by this \
                 snapshot config. Drop the target to rebuild it from the current rows, or set \
                 `snapshot_meta_column_names` to the columns it already has"
            )));
        }
    }
    // Only the columns this mode writes are metadata. Under `ignore` or
    // `invalidate` a model column named like `is_deleted` is ordinary data
    // and must stay in the history.
    let mut written = meta.written(spec.hard_deletes);
    if marker_column_is_metadata {
        written.push(meta.is_deleted.as_str());
    }
    Ok(target_columns
        .iter()
        .filter(|c| !written.iter().any(|r| c.eq_ignore_ascii_case(r)))
        .cloned()
        .collect())
}

/// `true` when `hard_deletes = "new_record"` but the target has no
/// `is_deleted` column (a snapshot that switched mode). The runner then adds
/// it with [`add_is_deleted_column_sql`]; existing rows read as not deleted
/// because every predicate wraps it in `COALESCE(.., FALSE)`.
pub fn missing_is_deleted_column(spec: &SnapshotSpec, target_columns: &[String]) -> bool {
    spec.hard_deletes == SnapshotHardDeletes::NewRecord
        && !target_columns
            .iter()
            .any(|c| c.eq_ignore_ascii_case(&spec.meta_columns.is_deleted))
}

/// `ALTER TABLE <target> ADD COLUMN <is_deleted> BOOLEAN`.
pub fn add_is_deleted_column_sql(
    spec: &SnapshotSpec,
    target: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let name = validated(&spec.meta_columns.is_deleted)?;
    Ok(format!(
        "ALTER TABLE {target} ADD COLUMN {} BOOLEAN",
        dialect.snapshot_metadata_identifier(name)
    ))
}

/// Generate the steady-state statements for one snapshot run against an
/// existing target. `target` is the formatted target reference;
/// `source_columns` the model's output columns in warehouse spelling (from
/// [`snapshot_source_columns`], or the typed columns for a preview).
pub fn generate_snapshot_model_sql(
    spec: &SnapshotSpec,
    target: &str,
    model_sql: &str,
    dialect: &dyn SqlDialect,
    source_columns: &[String],
    now: DateTime<Utc>,
) -> Result<Vec<String>, SqlGenError> {
    generate_snapshot_model_sql_with(
        spec,
        target,
        model_sql,
        dialect,
        source_columns,
        now,
        ExistingMarkers::FromMode,
    )
}

/// Whether the target may hold deletion markers the current mode did not
/// write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExistingMarkers {
    /// Only `hard_deletes = "new_record"` has markers.
    FromMode,
    /// The target carries the `is_deleted` metadata column from an earlier
    /// `new_record` run, though the mode changed since. A key whose current
    /// version is such a marker must still reopen when it comes back.
    Present,
}

/// [`generate_snapshot_model_sql`] with an explicit [`ExistingMarkers`].
pub fn generate_snapshot_model_sql_with(
    spec: &SnapshotSpec,
    target: &str,
    model_sql: &str,
    dialect: &dyn SqlDialect,
    source_columns: &[String],
    now: DateTime<Utc>,
    markers: ExistingMarkers,
) -> Result<Vec<String>, SqlGenError> {
    refuse_invalid(spec)?;
    let now = run_timestamp_literal(now);
    let body = model_sql.trim().trim_end_matches(';');
    let model = format!("(\n{body}\n)");
    let meta = &spec.meta_columns;
    for name in meta.reserved() {
        validated(name)?;
    }
    let ident = |name: &str| dialect.snapshot_metadata_identifier(name);
    let vt = ident(&meta.valid_to);
    let ic = meta.is_current.name().map(ident);
    // "Is the current version": the flag column when there is one, else
    // `valid_to` still holding its current-version value (dbt's own test).
    let current = |alias: &str| match &ic {
        Some(ic) => format!("{alias}.{ic} = TRUE"),
        None => match &spec.valid_to_current {
            Some(_) => format!(
                "({alias}.{vt} IS NULL OR {alias}.{vt} = {})",
                current_valid_to(spec)
            ),
            None => format!("{alias}.{vt} IS NULL"),
        },
    };
    let close = |at: &str| match &ic {
        Some(ic) => format!("{vt} = {at}, {ic} = FALSE"),
        None => format!("{vt} = {at}"),
    };
    let del = ident(&meta.is_deleted);

    let keys = spec
        .unique_key
        .iter()
        .map(|k| dialect.snapshot_source_reference(k, source_columns))
        .collect::<Result<Vec<_>, _>>()?;
    let join = |left: &str, right: &str| {
        keys.iter()
            .map(|k| format!("{left}.{k} = {right}.{k}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    let updated_at_ref = spec
        .change
        .version_column()
        .map(|u| dialect.snapshot_source_reference(u, source_columns))
        .transpose()?;

    let new_record = spec.hard_deletes == SnapshotHardDeletes::NewRecord;
    let is_deleted = |alias: &str| format!("COALESCE({alias}.{del}, FALSE)");

    // Change detection against the current version.
    let mut changed = match &spec.change {
        SnapshotChangeStrategy::Timestamp { .. } => {
            let u = updated_at_ref.as_deref().unwrap_or_default();
            // dbt semantics: only a strictly later `updated_at` is a change.
            // A NULL → value transition also counts, so a row whose stored
            // `updated_at` was NULL is not stuck forever.
            format!("source.{u} > target.{u} OR (target.{u} IS NULL AND source.{u} IS NOT NULL)")
        }
        SnapshotChangeStrategy::Check { check_cols, .. } => {
            let cols: Vec<String> = match check_cols {
                SnapshotCheckColumns::All => source_columns
                    .iter()
                    .filter(|c| {
                        !spec
                            .unique_key
                            .iter()
                            .any(|k| c.eq_ignore_ascii_case(k.as_ref()))
                    })
                    .map(|c| dialect.snapshot_column_identifier(c))
                    .collect(),
                SnapshotCheckColumns::Explicit(cols) => cols
                    .iter()
                    .map(|c| dialect.snapshot_source_reference(c, source_columns))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            if cols.is_empty() {
                return Err(SqlGenError::InvalidRequest(
                    "check strategy has no columns to compare: every output column is part \
                     of the unique_key"
                        .to_string(),
                ));
            }
            cols.iter()
                .map(|c| dialect.null_safe_neq(&format!("source.{c}"), &format!("target.{c}")))
                .collect::<Vec<_>>()
                .join(" OR ")
        }
    };
    let revive_markers = new_record || markers == ExistingMarkers::Present;
    if revive_markers {
        // A key whose current version is a deletion marker came back.
        changed = format!("{changed} OR {} = TRUE", is_deleted("target"));
    }

    let mut stmts = Vec::new();

    // 1. Close the current version of every changed key. A revived deletion
    //    marker is closed at the run time, not at the source's updated_at,
    //    which may predate the deletion.
    let version_from_source = version_timestamp("source", updated_at_ref.as_deref(), &now);
    let close_at = if revive_markers {
        format!(
            "CASE WHEN {} = TRUE THEN {now} ELSE {version_from_source} END",
            is_deleted("target")
        )
    } else {
        version_from_source.clone()
    };
    stmts.push(format!(
        "MERGE INTO {target} AS target\n\
         USING {model} AS source\n\
         ON {on} AND {is_current}\n\
         WHEN MATCHED AND ({changed}) THEN UPDATE SET {set}",
        on = join("target", "source"),
        is_current = current("target"),
        set = close(&close_at),
    ));

    // 2. Open a current version for every key that has none.
    let metadata =
        new_version_metadata(spec, dialect, &keys, "source", &version_from_source, false);
    let metadata_refs: Vec<(&str, &str)> = metadata
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let (names, values) = dialect.snapshot_insert_columns(source_columns, &metadata_refs)?;
    stmts.push(format!(
        "INSERT INTO {target} ({names})\n\
         SELECT {values}\n\
         FROM {model} AS source\n\
         WHERE {keys_present} AND NOT EXISTS (\
         SELECT 1 FROM {target} AS existing WHERE {on} AND {is_current})",
        names = names.join(", "),
        values = values.join(", "),
        keys_present = keys
            .iter()
            .map(|k| format!("source.{k} IS NOT NULL"))
            .collect::<Vec<_>>()
            .join(" AND "),
        on = join("existing", "source"),
        is_current = current("existing"),
    ));

    // 3. Hard deletes.
    let (update_target, q) = dialect.snapshot_update_target(target);
    let absent_from_model = format!(
        "NOT EXISTS (SELECT 1 FROM {model} AS source WHERE {})",
        join(&q, "source")
    );
    match spec.hard_deletes {
        SnapshotHardDeletes::Ignore => {}
        SnapshotHardDeletes::Invalidate => {
            stmts.push(format!(
                "UPDATE {update_target} SET {set}\n\
                 WHERE {is_current} AND {absent_from_model}",
                set = close(&now),
                is_current = current(&q),
            ));
        }
        SnapshotHardDeletes::NewRecord => {
            // 3a. A deletion marker copies the vanished key's last values.
            //     The target is aliased `source` so the dialect's insert
            //     column list (`source.<col>`) reads from it.
            let marker_meta = new_version_metadata(spec, dialect, &keys, "source", &now, true);
            let marker_refs: Vec<(&str, &str)> = marker_meta
                .iter()
                .map(|(n, v)| (n.as_str(), v.as_str()))
                .collect();
            let (names, values) = dialect.snapshot_insert_columns(source_columns, &marker_refs)?;
            stmts.push(format!(
                "INSERT INTO {target} ({names})\n\
                 SELECT {values}\n\
                 FROM {target} AS source\n\
                 WHERE {source_current} AND {not_deleted} = FALSE\n\
                 AND NOT EXISTS (SELECT 1 FROM {model} AS incoming WHERE {incoming_on})\n\
                 AND NOT EXISTS (SELECT 1 FROM {target} AS marker WHERE {marker_on} \
                 AND {marker_current} AND {marker_deleted} = TRUE)",
                source_current = current("source"),
                marker_current = current("marker"),
                names = names.join(", "),
                values = values.join(", "),
                not_deleted = is_deleted("source"),
                incoming_on = join("incoming", "source"),
                marker_on = join("marker", "source"),
                marker_deleted = is_deleted("marker"),
            ));
            // 3b. Close the version the marker replaced.
            stmts.push(format!(
                "UPDATE {update_target} SET {set}\n\
                 WHERE {is_current} AND {not_deleted} = FALSE AND {absent_from_model}",
                set = close(&now),
                is_current = current(&q),
                not_deleted = is_deleted(&q),
            ));
        }
    }

    Ok(stmts)
}

/// Statements for a preview (`rocky plan`, `emit-sql`) when the target's
/// columns are not known: the model's typed output columns stand in for
/// them. Errors when the compiler resolved no columns.
pub fn preview_snapshot_model_sql(
    spec: &SnapshotSpec,
    target: &str,
    model_sql: &str,
    dialect: &dyn SqlDialect,
    typed_column_names: &[String],
    now: DateTime<Utc>,
) -> Result<Vec<String>, SqlGenError> {
    if typed_column_names.is_empty() {
        return Err(SqlGenError::InvalidRequest(
            "snapshot SQL needs the model's output columns; they were not resolved at compile \
             time (`rocky run` reads them from the target instead)"
                .to_string(),
        ));
    }
    generate_snapshot_model_sql(spec, target, model_sql, dialect, typed_column_names, now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::{AdapterError, AdapterResult};
    use chrono::TimeZone;
    use rocky_ir::{ColumnSelection, MetadataColumn};

    struct D;
    impl SqlDialect for D {
        fn name(&self) -> &'static str {
            "test"
        }
        fn literal_escape(&self) -> crate::traits::LiteralEscape {
            crate::traits::LiteralEscape::Standard
        }
        fn format_table_ref(&self, c: &str, s: &str, t: &str) -> AdapterResult<String> {
            rocky_sql::validation::format_table_ref(c, s, t).map_err(AdapterError::new)
        }
        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }
        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target}\n{select_sql}")
        }
        fn merge_into(
            &self,
            _: &str,
            _: &str,
            _: &[std::sync::Arc<str>],
            _: &ColumnSelection,
        ) -> AdapterResult<String> {
            unimplemented!()
        }
        fn select_clause(
            &self,
            _: &ColumnSelection,
            _: &[MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!()
        }
        fn watermark_where(
            &self,
            _: &str,
            _: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> AdapterResult<String> {
            unimplemented!()
        }
        fn describe_table_sql(&self, t: &str) -> String {
            format!("DESCRIBE {t}")
        }
        fn drop_table_sql(&self, t: &str) -> String {
            format!("DROP TABLE IF EXISTS {t}")
        }
        fn create_catalog_sql(&self, _: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn create_schema_sql(&self, _: &str, _: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn tablesample_clause(&self, _: u32) -> Option<String> {
            None
        }
        fn insert_overwrite_partition(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> AdapterResult<Vec<String>> {
            unimplemented!()
        }
    }

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 4, 12, 0, 0).unwrap()
    }

    fn spec(change: SnapshotChangeStrategy, hard: SnapshotHardDeletes) -> SnapshotSpec {
        SnapshotSpec {
            unique_key: vec!["id".into()],
            change,
            hard_deletes: hard,
            meta_columns: SnapshotMetaColumns::default(),
            valid_to_current: None,
        }
    }

    fn ts() -> SnapshotChangeStrategy {
        SnapshotChangeStrategy::Timestamp {
            updated_at: "updated_at".into(),
        }
    }

    fn cols() -> Vec<String> {
        vec!["id".into(), "name".into(), "updated_at".into()]
    }

    #[test]
    fn lowering_infers_strategy_and_maps_legacy_invalidate() {
        let key = SnapshotUniqueKey::One("id".into());
        let lowered = lower_snapshot_config(SnapshotConfigFields {
            unique_key: Some(&key),
            strategy: None,
            updated_at: Some("updated_at"),
            check_cols: None,
            hard_deletes: None,
            invalidate_hard_deletes: Some(true),
            meta_columns: None,
            valid_to_current: None,
        });
        assert!(lowered.problems.is_empty(), "{:?}", lowered.problems);
        assert_eq!(lowered.spec.change, ts());
        assert_eq!(lowered.spec.hard_deletes, SnapshotHardDeletes::Invalidate);
    }

    #[test]
    fn lowering_reports_missing_key_strategy_and_bad_keyword() {
        let bad = SnapshotCheckColsConfig::Keyword("everything".into());
        let lowered = lower_snapshot_config(SnapshotConfigFields {
            unique_key: None,
            strategy: Some(SnapshotStrategyKind::Check),
            updated_at: None,
            check_cols: Some(&bad),
            hard_deletes: Some(SnapshotHardDeletes::NewRecord),
            invalidate_hard_deletes: Some(true),
            meta_columns: None,
            valid_to_current: None,
        });
        let p = lowered.problems.join("\n");
        assert!(p.contains("unique_key"), "{p}");
        assert!(p.contains("everything"), "{p}");
        assert!(p.contains("conflicts"), "{p}");
    }

    #[test]
    fn bootstrap_adds_metadata_with_updated_at_as_valid_from() {
        let sql = generate_snapshot_bootstrap_select(
            &spec(ts(), SnapshotHardDeletes::Ignore),
            "SELECT id, name, updated_at FROM raw.customers;",
            &D,
            now(),
        )
        .unwrap();
        assert!(sql.starts_with("SELECT source.*, CAST(source.updated_at AS TIMESTAMP) AS \"valid_from\", CAST(NULL AS TIMESTAMP) AS \"valid_to\", TRUE AS \"is_current\", md5("), "{sql}");
        assert!(
            sql.ends_with(
                "FROM (\nSELECT id, name, updated_at FROM raw.customers\n) AS source\nWHERE source.id IS NOT NULL"
            ),
            "{sql}"
        );
        assert!(!sql.contains("is_deleted"), "{sql}");
    }

    #[test]
    fn bootstrap_check_strategy_uses_run_time_and_writes_is_deleted_for_new_record() {
        let mut s = spec(
            SnapshotChangeStrategy::Check {
                check_cols: SnapshotCheckColumns::All,
                updated_at: None,
            },
            SnapshotHardDeletes::NewRecord,
        );
        s.valid_to_current = Some("'9999-12-31'".into());
        let sql = generate_snapshot_bootstrap_select(&s, "SELECT 1 AS id", &D, now()).unwrap();
        assert!(
            sql.contains("CAST('2026-10-04 12:00:00.000000' AS TIMESTAMP) AS \"valid_from\""),
            "{sql}"
        );
        assert!(
            sql.contains("CAST('9999-12-31' AS TIMESTAMP) AS \"valid_to\""),
            "{sql}"
        );
        assert!(sql.contains("FALSE AS \"is_deleted\""), "{sql}");
    }

    #[test]
    fn timestamp_merge_closes_only_strictly_newer_rows() {
        let stmts = generate_snapshot_model_sql(
            &spec(ts(), SnapshotHardDeletes::Ignore),
            "a.b.snap",
            "SELECT * FROM src",
            &D,
            &cols(),
            now(),
        )
        .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(
            stmts[0].starts_with("MERGE INTO a.b.snap AS target"),
            "{}",
            stmts[0]
        );
        assert!(
            stmts[0].contains("ON target.\"id\" = source.\"id\" AND target.\"is_current\" = TRUE")
        );
        assert!(stmts[0].contains("source.\"updated_at\" > target.\"updated_at\""));
        assert!(stmts[0].contains("UPDATE SET \"valid_to\" = CAST(source.\"updated_at\" AS TIMESTAMP), \"is_current\" = FALSE"));
        assert!(!stmts[0].contains("WHEN NOT MATCHED"), "{}", stmts[0]);
        assert!(
            stmts[1].starts_with(
                "INSERT INTO a.b.snap (\"id\", \"name\", \"updated_at\", \"valid_from\""
            ),
            "{}",
            stmts[1]
        );
        assert!(stmts[1].contains("WHERE source.\"id\" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM a.b.snap AS existing WHERE existing.\"id\" = source.\"id\" AND existing.\"is_current\" = TRUE)"), "{}", stmts[1]);
    }

    #[test]
    fn check_all_compares_every_non_key_column_null_safely() {
        let stmts = generate_snapshot_model_sql(
            &spec(
                SnapshotChangeStrategy::Check {
                    check_cols: SnapshotCheckColumns::All,
                    updated_at: None,
                },
                SnapshotHardDeletes::Ignore,
            ),
            "a.b.snap",
            "SELECT * FROM src",
            &D,
            &cols(),
            now(),
        )
        .unwrap();
        assert!(stmts[0].contains("source.\"name\" IS DISTINCT FROM target.\"name\" OR source.\"updated_at\" IS DISTINCT FROM target.\"updated_at\""), "{}", stmts[0]);
        assert!(
            !stmts[0].contains("source.\"id\" IS DISTINCT FROM"),
            "{}",
            stmts[0]
        );
        // Close and open share one timestamp.
        assert!(
            stmts[0].contains("\"valid_to\" = CAST('2026-10-04 12:00:00.000000' AS TIMESTAMP)")
        );
        assert!(stmts[1].contains("CAST('2026-10-04 12:00:00.000000' AS TIMESTAMP)"));
    }

    #[test]
    fn composite_keys_join_on_every_column() {
        let mut s = spec(ts(), SnapshotHardDeletes::Invalidate);
        s.unique_key = vec!["id".into(), "region".into()];
        let mut c = cols();
        c.push("region".into());
        let stmts = generate_snapshot_model_sql(&s, "a.b.snap", "SELECT 1", &D, &c, now()).unwrap();
        assert!(
            stmts[0].contains(
                "target.\"id\" = source.\"id\" AND target.\"region\" = source.\"region\""
            )
        );
        assert_eq!(stmts.len(), 3);
        assert!(
            stmts[2].starts_with("UPDATE a.b.snap AS target SET \"valid_to\" = CAST('2026"),
            "{}",
            stmts[2]
        );
        assert!(stmts[2].contains("WHERE target.\"is_current\" = TRUE AND NOT EXISTS (SELECT 1 FROM (\nSELECT 1\n) AS source WHERE target.\"id\" = source.\"id\" AND target.\"region\" = source.\"region\")"), "{}", stmts[2]);
    }

    #[test]
    fn new_record_inserts_guarded_marker_then_closes() {
        let stmts = generate_snapshot_model_sql(
            &spec(ts(), SnapshotHardDeletes::NewRecord),
            "a.b.snap",
            "SELECT * FROM src",
            &D,
            &cols(),
            now(),
        )
        .unwrap();
        assert_eq!(stmts.len(), 4);
        assert!(
            stmts[0].contains("OR COALESCE(target.\"is_deleted\", FALSE) = TRUE"),
            "{}",
            stmts[0]
        );
        assert!(stmts[1].contains(", FALSE\nFROM"), "{}", stmts[1]);
        let marker = &stmts[2];
        assert!(marker.contains("FROM a.b.snap AS source"), "{marker}");
        assert!(
            marker.contains(
                "AND marker.\"is_current\" = TRUE AND COALESCE(marker.\"is_deleted\", FALSE) = TRUE"
            ),
            "{marker}"
        );
        assert!(
            marker.contains(", TRUE\nFROM a.b.snap AS source"),
            "{marker}"
        );
        assert!(
            stmts[3].contains("COALESCE(target.\"is_deleted\", FALSE) = FALSE AND NOT EXISTS"),
            "{}",
            stmts[3]
        );
    }

    #[test]
    fn missing_columns_and_foreign_targets_are_refused() {
        let s = spec(ts(), SnapshotHardDeletes::Ignore);
        let err = generate_snapshot_model_sql(
            &s,
            "a.b.snap",
            "SELECT 1",
            &D,
            &["id".into(), "name".into()],
            now(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("updated_at"), "{err}");
        let err = snapshot_source_columns(&s, &["id".into(), "name".into()]).unwrap_err();
        assert!(err.to_string().contains("valid_from"), "{err}");
        let src = snapshot_source_columns(
            &s,
            &[
                "id".into(),
                "VALID_FROM".into(),
                "valid_to".into(),
                "is_current".into(),
                "snapshot_id".into(),
                "name".into(),
            ],
        )
        .unwrap();
        assert_eq!(src, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn invalid_spec_is_refused_before_sql() {
        let mut s = spec(ts(), SnapshotHardDeletes::Ignore);
        s.unique_key.clear();
        let err = generate_snapshot_model_sql(&s, "a.b.snap", "SELECT 1", &D, &cols(), now())
            .unwrap_err();
        assert!(err.to_string().contains("E049"), "{err}");
        assert!(generate_snapshot_bootstrap_select(&s, "SELECT 1", &D, now()).is_err());
    }

    #[test]
    fn without_a_flag_column_current_means_open_valid_to() {
        let mut s = spec(ts(), SnapshotHardDeletes::Invalidate);
        s.meta_columns.is_current = rocky_ir::SnapshotFlagColumn::Enabled(false);
        let stmts =
            generate_snapshot_model_sql(&s, "a.b.snap", "SELECT 1", &D, &cols(), now()).unwrap();
        assert!(
            stmts[0].contains("AND target.\"valid_to\" IS NULL\n"),
            "{}",
            stmts[0]
        );
        assert!(
            stmts[0]
                .contains("UPDATE SET \"valid_to\" = CAST(source.\"updated_at\" AS TIMESTAMP)\n")
                || stmts[0].ends_with(
                    "UPDATE SET \"valid_to\" = CAST(source.\"updated_at\" AS TIMESTAMP)"
                ),
            "{}",
            stmts[0]
        );
        assert!(!stmts.iter().any(|q| q.contains("is_current")), "{stmts:?}");
        s.valid_to_current = Some("'9999-12-31'".into());
        let stmts =
            generate_snapshot_model_sql(&s, "a.b.snap", "SELECT 1", &D, &cols(), now()).unwrap();
        assert!(stmts[1].contains("(existing.\"valid_to\" IS NULL OR existing.\"valid_to\" = CAST('9999-12-31' AS TIMESTAMP))"), "{}", stmts[1]);
    }

    #[test]
    fn markers_from_an_earlier_new_record_run_still_reopen() {
        let s = spec(ts(), SnapshotHardDeletes::Invalidate);
        let stmts = generate_snapshot_model_sql_with(
            &s,
            "a.b.snap",
            "SELECT 1",
            &D,
            &cols(),
            now(),
            ExistingMarkers::Present,
        )
        .unwrap();
        assert!(
            stmts[0].contains("OR COALESCE(target.\"is_deleted\", FALSE) = TRUE"),
            "{}",
            stmts[0]
        );
        // The mode does not write markers, so the new version leaves it NULL.
        assert!(!stmts[1].contains("is_deleted"), "{}", stmts[1]);
        let described: Vec<String> = [
            "id",
            "valid_from",
            "valid_to",
            "is_current",
            "snapshot_id",
            "is_deleted",
        ]
        .iter()
        .map(|c| (*c).to_string())
        .collect();
        assert_eq!(
            snapshot_source_columns(&s, &described).unwrap(),
            vec!["id", "is_deleted"]
        );
        assert_eq!(
            snapshot_source_columns_with(&s, &described, true).unwrap(),
            vec!["id"]
        );
    }

    #[test]
    fn renamed_meta_columns_flow_through() {
        let mut s = spec(ts(), SnapshotHardDeletes::Ignore);
        s.meta_columns.valid_from = "dbt_valid_from".into();
        s.meta_columns.valid_to = "dbt_valid_to".into();
        s.meta_columns.scd_id = "dbt_scd_id".into();
        s.meta_columns.updated_at = Some("dbt_updated_at".into());
        let stmts =
            generate_snapshot_model_sql(&s, "a.b.snap", "SELECT 1", &D, &cols(), now()).unwrap();
        assert!(stmts[0].contains("UPDATE SET \"dbt_valid_to\""));
        assert!(stmts[1].contains("\"dbt_valid_from\", \"dbt_valid_to\", \"is_current\", \"dbt_scd_id\", \"dbt_updated_at\""), "{}", stmts[1]);
    }
}
