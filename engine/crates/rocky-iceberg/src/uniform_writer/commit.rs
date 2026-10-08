//! Build Delta `_delta_log/N.json` commit bodies for the content-addressed
//! writer.
//!
//! Every run makes **one replace commit** (RV1-D8, #2269). It removes each
//! live file that is not part of the new output, and adds each new file that
//! is not live yet, in one atomic commit:
//!
//! ```text
//!   commitInfo   operation WRITE, mode Overwrite, isBlindAppend false
//!   remove × R   every live file that is not in the new output
//!   add    × A   every new file that is not live yet
//!   domainMetadata   delta.rowTracking high-water mark (rowTracking only)
//! ```
//!
//! A file that is live and also in the new output is neither removed nor
//! re-added. So one commit never adds and removes the same path.
//!
//! Stats inside an `add` action's `stats` JSON string are keyed by
//! **physical-name UUID**, not the logical column name. Delta column-mapped
//! tables stat-prune on the physical UUID. Partitioned tables also key
//! `partitionValues` by physical UUID (Exp 11 finding): callers pass a map
//! keyed by logical name, and this module translates it.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde_json::{Map, Value, json};

use super::{Result, UniformTableState, UniformWriterError};

// -- paths: the object key and the log path ---------------------------------
//
//   partition value  ──escape_path_name──▶  object key   `region=50%25off/H.parquet`
//   object key       ──delta_log_path────▶  add.path     `region=50%2525off/H.parquet`
//   add.path         ──canonical_key─────▶  object key   (one percent-decode)
//
// The object key follows Spark: a Hive-style `<col>=<value>` directory with
// each name and value escaped as `ExternalCatalogUtils.escapePathName` does,
// plus the few characters `object_store` also encodes (see
// `escape_path_name`). The
// Delta protocol requires `add.path` to be URI-encoded, so the log carries the
// key URI-encoded once more, as Spark's Delta writer does. For ordinary values
// (letters, digits, `-`, `_`, `.`) all three forms are the same string.

/// Escape one partition column name or value for a Hive-style directory
/// name: Spark's `ExternalCatalogUtils.escapePathName`, widened to also cover
/// every character `object_store`'s `Path::from` percent-encodes.
///
/// Escaped as `%XX` (upper-case hex):
///
/// | Source | Characters |
/// |---|---|
/// | Spark | ASCII controls, DEL, `"` `#` `%` `'` `*` `/` `:` `=` `?` `\` `{` `[` `]` `^` |
/// | `Path::from` only | `}` `` ` `` `<` `>` `\|` `~` |
///
/// The second row keeps the stored object key of every value the same as
/// the key the earlier writer produced with `Path::from(raw)`, so no earlier
/// object is left behind under an old key. Spark itself leaves those six
/// characters (and NUL) as they are; a Spark reader still decodes them,
/// because unescaping decodes any `%XX`. Space and non-ASCII stay as they are.
pub fn escape_path_name(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for c in raw.chars() {
        let escape = c.is_ascii_control()
            || matches!(
                c,
                '"' | '#'
                    | '%'
                    | '\''
                    | '*'
                    | '/'
                    | ':'
                    | '='
                    | '?'
                    | '\\'
                    | '{'
                    | '['
                    | ']'
                    | '^'
                    | '}'
                    | '`'
                    | '<'
                    | '>'
                    | '|'
                    | '~'
            );
        if escape {
            out.push_str(&format!("%{:02X}", c as u32));
        } else {
            out.push(c);
        }
    }
    out
}

/// The Hive-style directory segment `<col>=<value>` for one partition value,
/// both sides escaped with [`escape_path_name`].
pub fn partition_dir(column: &str, value: &str) -> String {
    format!("{}={}", escape_path_name(column), escape_path_name(value))
}

/// Characters a URI path segment keeps as they are: RFC 3986 `unreserved`
/// plus the `sub-delims` and `@`. Every other byte is percent-encoded, `%`,
/// space, `#`, `?`, `:` and all non-ASCII bytes included.
const LOG_PATH_SEGMENT: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~')
    .remove(b'!')
    .remove(b'$')
    .remove(b'&')
    .remove(b'\'')
    .remove(b'(')
    .remove(b')')
    .remove(b'*')
    .remove(b'+')
    .remove(b',')
    .remove(b';')
    .remove(b'=')
    .remove(b'@');

/// The Delta `add.path` for a table-relative object key: each `/`-separated
/// segment URI-encoded, the `/` separators kept.
///
/// The Delta protocol requires `path` to be URI-encoded. A reader decodes it
/// once to find the object, so [`super::discover::canonical_key`] of the
/// result is the object key again.
pub fn delta_log_path(relative_key: &str) -> String {
    relative_key
        .split('/')
        .map(|seg| percent_encoding::utf8_percent_encode(seg, LOG_PATH_SEGMENT).to_string())
        .collect::<Vec<_>>()
        .join("/")
}

/// Inputs to [`build_add_action`] for one freshly built parquet file.
///
/// For unpartitioned tables, pass `partition_values: &HashMap::new()` and
/// `add_file_path: "<hash>.parquet"`. For partitioned tables, pass a map
/// keyed by logical partition-column name with stringified values and an
/// `add_file_path` like `<col>=<value>/<hash>.parquet` (each segment built
/// with [`partition_dir`]).
///
/// `add_file_path` is the table-relative **object key**, not yet
/// URI-encoded. [`build_add_action`] writes it to `add.path` through
/// [`delta_log_path`].
#[derive(Debug, Clone, Copy)]
pub struct AddInputs<'a> {
    pub batch: &'a RecordBatch,
    pub state: &'a UniformTableState,
    pub add_file_path: &'a str,
    pub file_size: u64,
    pub modification_time_millis: i64,
    /// Logical-name → stringified partition value. Empty for unpartitioned
    /// tables.
    pub partition_values: &'a HashMap<String, String>,
}

/// Build the body of an `add` action (the inner object of `{"add": {...}}`)
/// for a freshly built parquet file, with stats computed from `batch`.
///
/// Row-tracking fields are not set here: the writer assigns `baseRowId` and
/// `defaultRowCommitVersion` at commit time with [`set_row_tracking`],
/// because they depend on the commit version and on the other files in the
/// same commit.
pub fn build_add_action(inputs: &AddInputs) -> Result<Map<String, Value>> {
    let stats = compute_stats(inputs.batch, inputs.state)?;
    let stats_json = serde_json::to_string(&stats)?;
    // Translate logical-name → physical-UUID for partitionValues. Sanity-
    // check that the provided keys cover exactly the table's partition
    // columns and nothing else.
    let partition_values_physical = translate_partition_values(inputs)?;

    let mut add_obj = Map::new();
    add_obj.insert(
        "path".into(),
        Value::String(delta_log_path(inputs.add_file_path)),
    );
    add_obj.insert(
        "partitionValues".into(),
        Value::Object(partition_values_physical),
    );
    add_obj.insert("size".into(), Value::from(inputs.file_size));
    add_obj.insert(
        "modificationTime".into(),
        Value::from(inputs.modification_time_millis),
    );
    add_obj.insert("dataChange".into(), Value::Bool(true));
    add_obj.insert("stats".into(), Value::String(stats_json));
    Ok(add_obj)
}

/// Set the row-tracking fields on an `add` body.
///
/// Exp 9 finding: Delta rowTracking requires every `add` action to carry
/// `baseRowId` + `defaultRowCommitVersion`. Reads that project
/// `_metadata.row_id` fail with `Missing base_row_id value` otherwise. Values
/// are i64 on the wire.
pub fn set_row_tracking(add: &mut Map<String, Value>, base_row_id: u64, commit_version: u64) {
    add.insert("baseRowId".into(), Value::from(base_row_id as i64));
    add.insert(
        "defaultRowCommitVersion".into(),
        Value::from(commit_version as i64),
    );
}

/// Lift a prior run's `add` action for a *point-to* commit — **no live
/// batch, no recomputed stats, no byte copy.**
///
/// `recovered_add` is the prior run `R`'s `add` body, obtained from `R`'s
/// `_delta_log/{version}.json` via
/// [`super::discover::recover_add_action_for_version`]. Its `path`, `size`,
/// `stats` and `dataChange` carry over byte-for-byte; only
/// `modificationTime` is refreshed to the reusing commit's wall clock.
///
/// # Scope — unpartitioned, non-rowTracking only (a hard second guard)
///
/// Returns [`UniformWriterError::DeltaLog`] for:
/// - a non-empty `partitionValues` ⇒ the partitioned point-to (deferred);
/// - a present `baseRowId` ⇒ the rowTracking point-to (the reusing commit
///   needs a *freshly re-allocated* `baseRowId` range; `R`'s cannot be lifted
///   verbatim — deferred).
///
/// Returns [`UniformWriterError::DeletionVectorsUnsupported`] when the `add`
/// carries a deletion vector: the vector would hide rows of the file, and
/// Rocky never writes one, so the `add` is not Rocky's output as recorded.
/// The protocol and column-mapping checks need the table's log, so they sit
/// in [`super::UniformWriter::commit_pointer_with_state`].
///
/// The runner's decision gate already restricts point-to to unpartitioned,
/// non-rowTracking tables; this refusal is the defence-in-depth guard so a
/// mis-routed call can never silently emit a structurally wrong commit.
pub fn lift_add_action(
    recovered_add: &Map<String, Value>,
    modification_time_millis: i64,
) -> Result<Map<String, Value>> {
    if let Some(Value::Object(pv)) = recovered_add.get("partitionValues")
        && !pv.is_empty()
    {
        return Err(UniformWriterError::DeltaLog(format!(
            "point-to refuses a partitioned `add` (partitionValues={pv:?}); \
             partitioned point-to is a deferred follow-up"
        )));
    }
    if recovered_add.contains_key("baseRowId")
        || recovered_add.contains_key("defaultRowCommitVersion")
    {
        return Err(UniformWriterError::DeltaLog(
            "point-to refuses a rowTracking `add` (baseRowId/defaultRowCommitVersion present); \
             rowTracking point-to is a deferred follow-up"
                .to_string(),
        ));
    }
    if recovered_add
        .get("deletionVector")
        .is_some_and(|dv| !dv.is_null())
    {
        return Err(UniformWriterError::DeletionVectorsUnsupported);
    }
    let mut add_obj = recovered_add.clone();
    add_obj.insert(
        "modificationTime".into(),
        Value::from(modification_time_millis),
    );
    Ok(add_obj)
}

/// Build the body of a `remove` action that retires the live file whose
/// `add` body is `live_add`.
///
/// The remove carries `path`, `deletionTimestamp`, `dataChange: true`, and
/// `extendedFileMetadata: true` with the `partitionValues` and `size` copied
/// from the live `add`, so UniForm's Iceberg conversion has the full file
/// metadata. On a rowTracking table it also copies `baseRowId` and
/// `defaultRowCommitVersion` when the live `add` has them.
///
/// # Errors
///
/// - [`UniformWriterError::DeletionVectorsUnsupported`] when the live `add`
///   carries a deletion vector (Rocky does not write them; UniForm forbids
///   them).
/// - `DeltaLog` when the live `add` has no string `path`, no integer `size`,
///   or no object `partitionValues`.
pub fn build_remove_action(
    live_add: &Map<String, Value>,
    deletion_timestamp_millis: i64,
) -> Result<Map<String, Value>> {
    if live_add
        .get("deletionVector")
        .is_some_and(|dv| !dv.is_null())
    {
        return Err(UniformWriterError::DeletionVectorsUnsupported);
    }
    let path = live_add
        .get("path")
        .and_then(Value::as_str)
        .ok_or_else(|| UniformWriterError::DeltaLog("live `add` has no string `path`".into()))?;
    let size = live_add
        .get("size")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            UniformWriterError::DeltaLog(format!("live `add` for `{path}` has no integer `size`"))
        })?;
    let partition_values = match live_add.get("partitionValues") {
        Some(Value::Object(pv)) => pv.clone(),
        None => Map::new(),
        Some(other) => {
            return Err(UniformWriterError::DeltaLog(format!(
                "live `add` for `{path}` has a non-object `partitionValues`: {other}"
            )));
        }
    };
    let mut remove = Map::new();
    remove.insert("path".into(), Value::String(path.to_string()));
    remove.insert(
        "deletionTimestamp".into(),
        Value::from(deletion_timestamp_millis),
    );
    remove.insert("dataChange".into(), Value::Bool(true));
    remove.insert("extendedFileMetadata".into(), Value::Bool(true));
    remove.insert("partitionValues".into(), Value::Object(partition_values));
    remove.insert("size".into(), Value::from(size));
    for key in ["baseRowId", "defaultRowCommitVersion"] {
        if let Some(v) = live_add.get(key) {
            remove.insert(key.into(), v.clone());
        }
    }
    Ok(remove)
}

/// Inputs to [`build_replace_commit_jsonl`].
#[derive(Debug, Clone, Copy)]
pub struct ReplaceCommit<'a> {
    pub engine_info: &'a str,
    pub timestamp_millis: i64,
    /// The table version the live set was read at (`commitInfo.readVersion`).
    pub read_version: u64,
    pub partition_columns: &'a [String],
    /// `remove` bodies (see [`build_remove_action`]).
    pub removes: &'a [Map<String, Value>],
    /// `add` bodies (see [`build_add_action`] / [`lift_add_action`]).
    pub adds: &'a [Map<String, Value>],
    /// `Some(hwm)` on a rowTracking table whose commit allocates row ids: the
    /// new `rowIdHighWaterMark` (the largest allocated row id, inclusive).
    pub row_tracking_high_water_mark: Option<u64>,
}

/// Serialize one replace commit as JSONL bytes ready to PUT at
/// `_delta_log/{N:020}.json`.
///
/// Lines, in order: `commitInfo` (`WRITE`, `mode: Overwrite`,
/// `isBlindAppend: false`), every `remove`, every `add`, then the
/// `delta.rowTracking` `domainMetadata` when
/// `row_tracking_high_water_mark` is `Some`.
///
/// # Errors
///
/// `DeltaLog` when one path appears both as a `remove` and as an `add`, or
/// twice among the adds. The Delta protocol forbids both, so the writer
/// never emits them.
pub fn build_replace_commit_jsonl(commit: &ReplaceCommit) -> Result<Vec<u8>> {
    let path_of = |m: &Map<String, Value>| -> Result<String> {
        m.get("path")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| UniformWriterError::DeltaLog("action has no string `path`".into()))
    };
    let mut removed: HashSet<String> = HashSet::new();
    for r in commit.removes {
        removed.insert(path_of(r)?);
    }
    let mut added: HashSet<String> = HashSet::new();
    for a in commit.adds {
        let p = path_of(a)?;
        if removed.contains(&p) {
            return Err(UniformWriterError::DeltaLog(format!(
                "a replace commit must not add and remove the same path `{p}`"
            )));
        }
        if !added.insert(p.clone()) {
            return Err(UniformWriterError::DeltaLog(format!(
                "a replace commit must not add the same path `{p}` twice"
            )));
        }
    }

    let partition_by_json = serde_json::to_string(commit.partition_columns)?;
    let commit_info = json!({
        "commitInfo": {
            "timestamp": commit.timestamp_millis,
            "operation": "WRITE",
            "operationParameters": {"mode": "Overwrite", "partitionBy": partition_by_json},
            "readVersion": commit.read_version,
            "isolationLevel": "Serializable",
            "isBlindAppend": false,
            "engineInfo": commit.engine_info,
        }
    });

    let mut out: Vec<u8> = Vec::new();
    let mut push = |v: &Value| -> Result<()> {
        out.extend_from_slice(serde_json::to_string(v)?.as_bytes());
        out.push(b'\n');
        Ok(())
    };
    push(&commit_info)?;
    for r in commit.removes {
        push(&json!({ "remove": Value::Object(r.clone()) }))?;
    }
    for a in commit.adds {
        push(&json!({ "add": Value::Object(a.clone()) }))?;
    }
    if let Some(hwm) = commit.row_tracking_high_water_mark {
        // Bump the rowIdHighWaterMark so later writes (and Delta's own
        // materialised row-id column) see the right next id.
        let cfg = json!({ "rowIdHighWaterMark": hwm as i64 });
        push(&json!({
            "domainMetadata": {
                "domain": "delta.rowTracking",
                "configuration": serde_json::to_string(&cfg)?,
                "removed": false,
            }
        }))?;
    }
    Ok(out)
}

fn translate_partition_values(inputs: &AddInputs) -> Result<Map<String, Value>> {
    let table_partitions: HashSet<&str> = inputs
        .state
        .partition_columns
        .iter()
        .map(String::as_str)
        .collect();

    // Caller must provide values for exactly the table's partition columns.
    for col in &inputs.state.partition_columns {
        if !inputs.partition_values.contains_key(col) {
            return Err(UniformWriterError::DeltaLog(format!(
                "missing partition value for column `{col}` (table partition columns: {:?})",
                inputs.state.partition_columns
            )));
        }
    }
    for col in inputs.partition_values.keys() {
        if !table_partitions.contains(col.as_str()) {
            return Err(UniformWriterError::DeltaLog(format!(
                "unexpected partition value for column `{col}` (table partition columns: {:?})",
                inputs.state.partition_columns
            )));
        }
    }

    let mut out: Map<String, Value> = Map::new();
    for col in &inputs.state.partition_columns {
        let phys = inputs.state.physical.get(col).ok_or_else(|| {
            UniformWriterError::DeltaLog(format!("partition column `{col}` is not in PHYSICAL map"))
        })?;
        let v = inputs.partition_values.get(col).expect("checked above");
        out.insert(phys.clone(), Value::String(v.clone()));
    }
    Ok(out)
}

/// Compute Delta `add.stats` for the supported primitive types.
///
/// Supported types: `Int64`, `Utf8` (string), `Timestamp(Microsecond, UTC)`.
/// For any other column type the writer still emits `nullCount` but omits
/// min/max — Delta tolerates missing min/max stats; only correctness on
/// counts matters.
///
/// All map keys are the **physical-name UUID** of the column, not the
/// logical name.
fn compute_stats(batch: &RecordBatch, state: &UniformTableState) -> Result<Value> {
    let mut min_values: Map<String, Value> = Map::new();
    let mut max_values: Map<String, Value> = Map::new();
    let mut null_counts: BTreeMap<String, i64> = BTreeMap::new();
    let partitions: HashSet<&str> = state.partition_columns.iter().map(String::as_str).collect();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        // Partition columns are NOT in the Parquet file, so they don't
        // belong in `stats` either.
        if partitions.contains(field.name().as_str()) {
            continue;
        }
        let phys = state.physical.get(field.name()).ok_or_else(|| {
            UniformWriterError::DeltaLog(format!(
                "column `{}` not in discovered table schema",
                field.name()
            ))
        })?;
        let array = batch.column(i);
        null_counts.insert(phys.clone(), array.null_count() as i64);

        if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
            if let Some(m) = arrow::compute::min(arr) {
                min_values.insert(phys.clone(), Value::from(m));
            }
            if let Some(m) = arrow::compute::max(arr) {
                max_values.insert(phys.clone(), Value::from(m));
            }
        } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
            if let Some(m) = arrow::compute::min_string(arr) {
                min_values.insert(phys.clone(), Value::from(m));
            }
            if let Some(m) = arrow::compute::max_string(arr) {
                max_values.insert(phys.clone(), Value::from(m));
            }
        } else if let Some(arr) = array.as_any().downcast_ref::<TimestampMicrosecondArray>() {
            if let Some(m) = arrow::compute::min(arr) {
                min_values.insert(phys.clone(), Value::String(micros_to_iso(m)));
            }
            if let Some(m) = arrow::compute::max(arr) {
                max_values.insert(phys.clone(), Value::String(micros_to_iso(m)));
            }
        }
        // Other types: nullCount only.
    }

    // Materialise null_counts as serde_json::Map preserving deterministic order.
    let mut null_map = Map::new();
    for (k, v) in null_counts {
        null_map.insert(k, Value::from(v));
    }
    Ok(json!({
        "numRecords": batch.num_rows(),
        "minValues": min_values,
        "maxValues": max_values,
        "nullCount": null_map,
    }))
}

fn micros_to_iso(micros: i64) -> String {
    let dt: DateTime<Utc> = Utc
        .timestamp_micros(micros)
        .single()
        .unwrap_or_else(Utc::now);
    // Match the Delta convention used by Databricks-written add actions:
    // ISO 8601 with microsecond precision and trailing 'Z'.
    dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state_3col() -> UniformTableState {
        let mut physical = HashMap::new();
        physical.insert("id".to_string(), "col-id".to_string());
        physical.insert("name".to_string(), "col-name".to_string());
        physical.insert("ts".to_string(), "col-ts".to_string());
        let mut field_id = HashMap::new();
        field_id.insert("id".to_string(), 1);
        field_id.insert("name".to_string(), 2);
        field_id.insert("ts".to_string(), 3);
        UniformTableState {
            physical,
            field_id,
            partition_columns: Vec::new(),
            row_tracking_enabled: false,
            deletion_vectors_enabled: false,
            next_commit_version: 1,
            row_tracking_next_id: 0,
        }
    }

    fn make_batch_3col() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
        ]));
        let ids = Int64Array::from(vec![0_i64, 1, 2]);
        let names = StringArray::from(vec!["a", "b", "c"]);
        let ts = TimestampMicrosecondArray::from(vec![1_000_000_i64, 2_000_000, 3_000_000])
            .with_timezone("UTC");
        RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(names), Arc::new(ts)]).unwrap()
    }

    fn add_for(path: &str) -> Map<String, Value> {
        let state = make_state_3col();
        let batch = make_batch_3col();
        let pv = HashMap::new();
        build_add_action(&AddInputs {
            batch: &batch,
            state: &state,
            add_file_path: path,
            file_size: 123,
            modification_time_millis: 1_000,
            partition_values: &pv,
        })
        .unwrap()
    }

    fn lines_of(body: &[u8]) -> Vec<Value> {
        std::str::from_utf8(body)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    fn commit<'a>(
        removes: &'a [Map<String, Value>],
        adds: &'a [Map<String, Value>],
        hwm: Option<u64>,
    ) -> ReplaceCommit<'a> {
        ReplaceCommit {
            engine_info: "rocky-iceberg/test",
            timestamp_millis: 42,
            read_version: 7,
            partition_columns: &[],
            removes,
            adds,
            row_tracking_high_water_mark: hwm,
        }
    }

    #[test]
    fn add_action_has_expected_keys() {
        let add = add_for("abc.parquet");
        assert_eq!(add["path"], "abc.parquet");
        assert_eq!(add["size"], 123);
        assert_eq!(add["partitionValues"], json!({}));
        assert_eq!(add["dataChange"], true);
        assert!(!add.contains_key("baseRowId"));
        assert!(!add.contains_key("defaultRowCommitVersion"));
    }

    #[test]
    fn replace_commit_is_overwrite_not_blind_append() {
        let live = add_for("a.parquet");
        let removes = vec![build_remove_action(&live, 42).unwrap()];
        let adds = vec![add_for("b.parquet")];
        let body = build_replace_commit_jsonl(&commit(&removes, &adds, None)).unwrap();
        let lines = lines_of(&body);
        assert_eq!(lines.len(), 3, "commitInfo + 1 remove + 1 add");
        let info = &lines[0]["commitInfo"];
        assert_eq!(info["operation"], "WRITE");
        assert_eq!(info["operationParameters"]["mode"], "Overwrite");
        assert_eq!(info["isBlindAppend"], false);
        assert_eq!(info["readVersion"], 7);
        assert_eq!(lines[1]["remove"]["path"], "a.parquet");
        assert_eq!(lines[2]["add"]["path"], "b.parquet");
    }

    #[test]
    fn remove_action_copies_size_and_partition_values() {
        let mut live = add_for("region=eu/a.parquet");
        live.insert("partitionValues".into(), json!({"col-region": "eu"}));
        let r = build_remove_action(&live, 99).unwrap();
        assert_eq!(r["path"], "region=eu/a.parquet");
        assert_eq!(r["deletionTimestamp"], 99);
        assert_eq!(r["dataChange"], true);
        assert_eq!(r["extendedFileMetadata"], true);
        assert_eq!(r["size"], 123);
        assert_eq!(r["partitionValues"], json!({"col-region": "eu"}));
        assert!(!r.contains_key("baseRowId"));
        assert!(!r.contains_key("stats"));
    }

    #[test]
    fn remove_action_copies_row_tracking_fields() {
        let mut live = add_for("a.parquet");
        set_row_tracking(&mut live, 100, 3);
        let r = build_remove_action(&live, 0).unwrap();
        assert_eq!(r["baseRowId"], 100);
        assert_eq!(r["defaultRowCommitVersion"], 3);
    }

    #[test]
    fn remove_action_refuses_deletion_vector_and_missing_size() {
        let mut live = add_for("a.parquet");
        live.insert("deletionVector".into(), json!({"storageType": "u"}));
        assert!(matches!(
            build_remove_action(&live, 0),
            Err(UniformWriterError::DeletionVectorsUnsupported)
        ));
        let mut live = add_for("a.parquet");
        live.remove("size");
        match build_remove_action(&live, 0) {
            Err(UniformWriterError::DeltaLog(msg)) => assert!(msg.contains("size"), "{msg}"),
            other => panic!("expected DeltaLog, got {other:?}"),
        }
    }

    #[test]
    fn replace_commit_refuses_add_and_remove_of_one_path() {
        let live = add_for("same.parquet");
        let removes = vec![build_remove_action(&live, 0).unwrap()];
        let adds = vec![add_for("same.parquet")];
        match build_replace_commit_jsonl(&commit(&removes, &adds, None)) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(msg.contains("same.parquet"), "{msg}")
            }
            other => panic!("expected DeltaLog refusal, got {other:?}"),
        }
        let adds = vec![add_for("dup.parquet"), add_for("dup.parquet")];
        assert!(build_replace_commit_jsonl(&commit(&[], &adds, None)).is_err());
    }

    #[test]
    fn replace_commit_with_row_tracking_emits_domain_metadata_last() {
        let mut add = add_for("a.parquet");
        set_row_tracking(&mut add, 100, 7);
        let adds = vec![add];
        let body = build_replace_commit_jsonl(&commit(&[], &adds, Some(102))).unwrap();
        let lines = lines_of(&body);
        assert_eq!(lines.len(), 3, "commitInfo + add + domainMetadata");
        // Exp 9 finding: both fields required on every add action.
        assert_eq!(lines[1]["add"]["baseRowId"], 100);
        assert_eq!(lines[1]["add"]["defaultRowCommitVersion"], 7);
        let dm = &lines[2]["domainMetadata"];
        assert_eq!(dm["domain"], "delta.rowTracking");
        assert_eq!(dm["removed"], false);
        let cfg: Value = serde_json::from_str(dm["configuration"].as_str().unwrap()).unwrap();
        assert_eq!(cfg["rowIdHighWaterMark"], 102);
    }

    fn make_partitioned_state() -> UniformTableState {
        let mut physical = HashMap::new();
        physical.insert("id".to_string(), "col-id".to_string());
        physical.insert("payload".to_string(), "col-payload".to_string());
        physical.insert("region".to_string(), "col-region".to_string());
        let mut field_id = HashMap::new();
        field_id.insert("id".to_string(), 1);
        field_id.insert("payload".to_string(), 2);
        field_id.insert("region".to_string(), 3);
        UniformTableState {
            physical,
            field_id,
            partition_columns: vec!["region".to_string()],
            row_tracking_enabled: false,
            deletion_vectors_enabled: false,
            next_commit_version: 1,
            row_tracking_next_id: 0,
        }
    }

    fn make_partitioned_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids = Int64Array::from(vec![0_i64, 1, 2]);
        let payload = StringArray::from(vec!["a", "b", "c"]);
        let region = StringArray::from(vec!["eu", "eu", "eu"]);
        RecordBatch::try_new(
            schema,
            vec![Arc::new(ids), Arc::new(payload), Arc::new(region)],
        )
        .unwrap()
    }

    fn partitioned_add(pv: &HashMap<String, String>) -> Result<Map<String, Value>> {
        let state = make_partitioned_state();
        let batch = make_partitioned_batch();
        build_add_action(&AddInputs {
            batch: &batch,
            state: &state,
            add_file_path: "region=eu/abc.parquet",
            file_size: 100,
            modification_time_millis: 0,
            partition_values: pv,
        })
    }

    #[test]
    fn partitioned_add_keys_partition_values_by_physical_uuid() {
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        let add = partitioned_add(&pv).unwrap();
        // Exp 11 — partitionValues MUST be keyed by physical UUID, not logical name.
        assert_eq!(add["partitionValues"], json!({"col-region": "eu"}));
        assert_eq!(add["path"], "region=eu/abc.parquet");
    }

    #[test]
    fn partitioned_stats_omit_partition_column() {
        let state = make_partitioned_state();
        let batch = make_partitioned_batch();
        let stats = compute_stats(&batch, &state).unwrap();
        for k in stats["minValues"]
            .as_object()
            .unwrap()
            .keys()
            .chain(stats["maxValues"].as_object().unwrap().keys())
            .chain(stats["nullCount"].as_object().unwrap().keys())
        {
            assert_ne!(k, "col-region", "partition column must not appear in stats");
        }
    }

    #[test]
    fn partitioned_add_rejects_missing_partition_value() {
        match partitioned_add(&HashMap::new()) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("`region`"),
                    "must name the missing column: {msg}"
                );
            }
            other => panic!("expected DeltaLog error, got {other:?}"),
        }
    }

    #[test]
    fn partitioned_add_rejects_extra_partition_value() {
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        pv.insert("unknown".to_string(), "x".to_string());
        match partitioned_add(&pv) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("`unknown`"),
                    "must name the extra column: {msg}"
                );
            }
            other => panic!("expected DeltaLog error, got {other:?}"),
        }
    }

    #[test]
    fn stats_are_keyed_by_physical_uuid() {
        let state = make_state_3col();
        let batch = make_batch_3col();
        let stats = compute_stats(&batch, &state).unwrap();

        let min = stats["minValues"].as_object().unwrap();
        let max = stats["maxValues"].as_object().unwrap();
        let nc = stats["nullCount"].as_object().unwrap();

        // All keys must be physical UUIDs ("col-id", "col-name", "col-ts"),
        // never the logical names ("id", "name", "ts").
        for k in min.keys().chain(max.keys()).chain(nc.keys()) {
            assert!(
                k.starts_with("col-"),
                "stats key `{k}` is a logical name, must be physical UUID"
            );
        }
        assert_eq!(min["col-id"], 0);
        assert_eq!(max["col-id"], 2);
        assert_eq!(min["col-name"], "a");
        assert_eq!(max["col-name"], "c");
        assert_eq!(nc["col-id"], 0);
    }

    // -- point-to (lift_add_action) -------------------------------------------

    /// A realistic unpartitioned, non-rowTracking `add` action as a prior
    /// run would have written it (stats keyed by physical UUID).
    fn recovered_unpartitioned_add() -> Map<String, Value> {
        let stats = json!({
            "numRecords": 3,
            "minValues": {"col-id": 0},
            "maxValues": {"col-id": 2},
            "nullCount": {"col-id": 0},
        });
        let mut add = Map::new();
        add.insert("path".into(), Value::String("abc123.parquet".into()));
        add.insert("partitionValues".into(), json!({}));
        add.insert("size".into(), Value::from(4096_u64));
        add.insert(
            "modificationTime".into(),
            Value::from(1_000_000_000_000_i64),
        );
        add.insert("dataChange".into(), Value::Bool(true));
        add.insert(
            "stats".into(),
            Value::String(serde_json::to_string(&stats).unwrap()),
        );
        add
    }

    #[test]
    fn lift_add_keeps_fields_and_refreshes_modification_time() {
        let add = recovered_unpartitioned_add();
        let new_mod_time = 1_700_000_000_123_i64;
        let lifted = lift_add_action(&add, new_mod_time).unwrap();
        // path / size / dataChange carry over byte-for-byte.
        assert_eq!(lifted["path"], "abc123.parquet");
        assert_eq!(lifted["size"], 4096);
        assert_eq!(lifted["dataChange"], true);
        // modificationTime is the ONLY field refreshed.
        assert_eq!(lifted["modificationTime"], new_mod_time);
        assert_eq!(lifted["stats"], add["stats"]);
    }

    #[test]
    fn lift_add_refuses_partitioned_add() {
        let mut add = recovered_unpartitioned_add();
        add.insert("partitionValues".into(), json!({"col-region": "eu"}));
        match lift_add_action(&add, 0) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(msg.contains("partitioned"), "{msg}");
            }
            other => panic!("expected DeltaLog refusal, got {other:?}"),
        }
    }

    #[test]
    fn lift_add_refuses_row_tracking_add() {
        let mut add = recovered_unpartitioned_add();
        add.insert("baseRowId".into(), Value::from(100_i64));
        add.insert("defaultRowCommitVersion".into(), Value::from(7_i64));
        match lift_add_action(&add, 0) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(msg.contains("rowTracking"), "{msg}");
            }
            other => panic!("expected DeltaLog refusal, got {other:?}"),
        }
    }

    #[test]
    fn lift_add_refuses_an_add_with_a_deletion_vector() {
        let mut add = recovered_unpartitioned_add();
        add.insert(
            "deletionVector".into(),
            serde_json::json!({"storageType": "u", "pathOrInlineDv": "ab", "offset": 1,
                               "sizeInBytes": 36, "cardinality": 2}),
        );
        assert!(matches!(
            lift_add_action(&add, 0),
            Err(UniformWriterError::DeletionVectorsUnsupported)
        ));
        add.insert("deletionVector".into(), Value::Null);
        assert!(
            lift_add_action(&add, 0).is_ok(),
            "a null vector is no vector"
        );
    }

    #[test]
    fn timestamp_min_max_are_iso8601_z() {
        let state = make_state_3col();
        let batch = make_batch_3col();
        let stats = compute_stats(&batch, &state).unwrap();
        let min_ts = stats["minValues"]["col-ts"].as_str().unwrap();
        let max_ts = stats["maxValues"]["col-ts"].as_str().unwrap();
        assert_eq!(min_ts, "1970-01-01T00:00:01.000000Z");
        assert_eq!(max_ts, "1970-01-01T00:00:03.000000Z");
    }
}
