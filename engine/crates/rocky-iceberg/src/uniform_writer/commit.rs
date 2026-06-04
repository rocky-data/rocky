//! Build Delta `_delta_log/N.json` commit bodies for the content-addressed
//! writer.
//!
//! Each commit emits two actions: a `commitInfo` (operation metadata) and a
//! single `add` (the new file). Stats inside the add action's `stats` JSON
//! string are keyed by **physical-name UUID**, not the logical column name —
//! Delta column-mapped tables stat-prune on the physical UUID, and feeding
//! logical names there breaks stats-based file skipping.
//!
//! Partition tables also key `partitionValues` by physical UUID (Exp 11
//! finding). Callers writing to a partitioned table pass a non-empty
//! `partition_values` map keyed by logical name; this module translates to
//! physical UUIDs before emitting the `add` action.

use std::collections::{BTreeMap, HashMap, HashSet};

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde_json::{Map, Value, json};

use super::{Result, UniformTableState, UniformWriterError};

/// Inputs to `build_commit_jsonl`. Borrowed so callers can build many
/// commits from the same `RecordBatch` without cloning.
///
/// For unpartitioned tables, pass `partition_values: &HashMap::new()` and
/// `add_file_path: "<hash>.parquet"`. For partitioned tables, pass a map
/// keyed by logical partition-column name with stringified values and an
/// `add_file_path` like `<col>=<value>/<hash>.parquet`.
#[derive(Debug, Clone, Copy)]
pub struct CommitInputs<'a> {
    pub batch: &'a RecordBatch,
    pub state: &'a UniformTableState,
    pub add_file_path: &'a str,
    pub file_size: u64,
    pub modification_time_millis: i64,
    pub engine_info: &'a str,
    /// Logical-name → stringified partition value. Empty for unpartitioned
    /// tables.
    pub partition_values: &'a HashMap<String, String>,
    /// Row-tracking commit fields. `Some(..)` when the table has
    /// `delta.enableRowTracking=true`; `None` otherwise. Without it,
    /// reads that project `_metadata.row_id` against a rowTracking table
    /// fail with `Missing base_row_id value` (Exp 9 finding).
    pub row_tracking: Option<RowTrackingCommit>,
}

/// Row-tracking fields required on every `add` action of a
/// rowTracking-enabled Delta table, plus the next watermark value the
/// commit advances to.
#[derive(Debug, Clone, Copy)]
pub struct RowTrackingCommit {
    /// The smallest row-id in the file written by this commit.
    pub base_row_id: u64,
    /// The commit version being written (for `defaultRowCommitVersion`).
    pub default_row_commit_version: u64,
    /// The new `rowIdHighWaterMark` after this commit — the largest
    /// row-id allocated so far (inclusive).
    pub new_high_water_mark: u64,
}

/// Serialize a Delta commit as JSONL bytes ready to PUT at
/// `_delta_log/{N:020}.json`.
///
/// Lines emitted in order:
/// 1. `commitInfo` (always)
/// 2. `add` action (always — the new content-addressed Parquet file)
/// 3. `domainMetadata` for `delta.rowTracking` (only when
///    `inputs.row_tracking` is `Some`)
///
/// Multi-`add` commits (e.g. writing several partitions atomically) are
/// not in scope — callers issue one commit per partition write.
pub fn build_commit_jsonl(inputs: &CommitInputs) -> Result<Vec<u8>> {
    let stats = compute_stats(inputs.batch, inputs.state)?;
    let stats_json = serde_json::to_string(&stats)?;

    // Translate logical-name → physical-UUID for partitionValues. Sanity-
    // check that the provided keys cover exactly the table's partition
    // columns and nothing else.
    let partition_values_physical = translate_partition_values(inputs)?;
    let partition_by_json = serde_json::to_string(&inputs.state.partition_columns)?;

    let commit_info = json!({
        "commitInfo": {
            "timestamp": inputs.modification_time_millis,
            "operation": "WRITE",
            "operationParameters": {"mode": "Append", "partitionBy": partition_by_json},
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "engineInfo": inputs.engine_info,
        }
    });
    let mut add_obj = serde_json::Map::new();
    add_obj.insert("path".into(), Value::String(inputs.add_file_path.into()));
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
    if let Some(rt) = inputs.row_tracking {
        // Exp 9 finding: Delta rowTracking requires every add action to
        // carry baseRowId + defaultRowCommitVersion. Reads that project
        // _metadata.row_id fail with `Missing base_row_id value`
        // otherwise. Values are i64 on the wire.
        add_obj.insert("baseRowId".into(), Value::from(rt.base_row_id as i64));
        add_obj.insert(
            "defaultRowCommitVersion".into(),
            Value::from(rt.default_row_commit_version as i64),
        );
    }
    let add_action = json!({ "add": Value::Object(add_obj) });

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(serde_json::to_string(&commit_info)?.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(serde_json::to_string(&add_action)?.as_bytes());
    out.push(b'\n');

    if let Some(rt) = inputs.row_tracking {
        // Bump the rowIdHighWaterMark so subsequent writes (and Delta's
        // own materialised row-id column) see the right next-id.
        let cfg = json!({ "rowIdHighWaterMark": rt.new_high_water_mark as i64 });
        let domain = json!({
            "domainMetadata": {
                "domain": "delta.rowTracking",
                "configuration": serde_json::to_string(&cfg)?,
                "removed": false,
            }
        });
        out.extend_from_slice(serde_json::to_string(&domain)?.as_bytes());
        out.push(b'\n');
    }

    Ok(out)
}

/// Build a point-to commit body that lifts a prior run's `add` action
/// verbatim — **no live batch, no recomputed stats, no byte copy.**
///
/// This is the commit half of an Iceberg content-addressed *point-to*: the
/// reusing run references a prior run `R`'s already-written, blake3-named
/// parquet file instead of executing SQL and re-writing bytes. `recovered_add`
/// is `R`'s `add` action object (the inner body of the `{"add": {...}}` line),
/// obtained from `R`'s `_delta_log/{version}.json` via
/// [`super::discover::recover_add_action_for_version`]. Its `path`, `size`,
/// `stats` (with `numRecords` + min/max/nullCount keyed by physical UUID), and
/// `dataChange` carry over byte-for-byte; only `modificationTime` is refreshed
/// to the reusing commit's wall clock.
///
/// Lines emitted, in order:
/// 1. `commitInfo` (operation `WRITE`, blind append) — fresh for this commit.
/// 2. the lifted `add` action — `R`'s, with `modificationTime` refreshed.
///
/// # Scope — unpartitioned, non-rowTracking only (a hard second guard)
///
/// This entry point refuses to lift a partitioned or rowTracking `add` and
/// returns [`UniformWriterError::DeltaLog`]:
/// - a non-empty `partitionValues` ⇒ the partitioned point-to (last-group
///   ledger is incomplete — deferred follow-up);
/// - a present `baseRowId` ⇒ the rowTracking point-to (the reusing commit
///   needs a *freshly re-allocated* `baseRowId` range; `R`'s cannot be lifted
///   verbatim — deferred follow-up).
///
/// The runner's decision gate already restricts point-to to unpartitioned,
/// non-rowTracking tables; this refusal is the defence-in-depth guard so a
/// mis-routed call can never silently emit a structurally wrong commit.
pub fn build_commit_jsonl_from_add(
    recovered_add: &Map<String, Value>,
    engine_info: &str,
    modification_time_millis: i64,
    partition_columns: &[String],
) -> Result<Vec<u8>> {
    // Guard 1: refuse a partitioned `add`. A non-empty partitionValues means
    // this file belongs to a partition group; the partitioned point-to is a
    // deferred follow-up (the ledger records only the last group's hash).
    if let Some(Value::Object(pv)) = recovered_add.get("partitionValues")
        && !pv.is_empty()
    {
        return Err(UniformWriterError::DeltaLog(format!(
            "point-to refuses a partitioned `add` (partitionValues={pv:?}); \
             partitioned point-to is a deferred follow-up"
        )));
    }
    // Guard 2: refuse a rowTracking `add`. A present baseRowId means the
    // reusing commit would need a freshly re-allocated row-id range; lifting
    // R's verbatim would collide in row-id space. Deferred follow-up.
    if recovered_add.contains_key("baseRowId")
        || recovered_add.contains_key("defaultRowCommitVersion")
    {
        return Err(UniformWriterError::DeltaLog(
            "point-to refuses a rowTracking `add` (baseRowId/defaultRowCommitVersion present); \
             rowTracking point-to is a deferred follow-up"
                .to_string(),
        ));
    }

    let partition_by_json = serde_json::to_string(partition_columns)?;
    let commit_info = json!({
        "commitInfo": {
            "timestamp": modification_time_millis,
            "operation": "WRITE",
            "operationParameters": {"mode": "Append", "partitionBy": partition_by_json},
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "engineInfo": engine_info,
        }
    });

    // Lift the recovered `add` verbatim, refreshing only modificationTime so
    // the reusing commit's add carries this run's wall clock (path, size,
    // stats, dataChange all carry over byte-for-byte from R).
    let mut add_obj = recovered_add.clone();
    add_obj.insert(
        "modificationTime".into(),
        Value::from(modification_time_millis),
    );
    let add_action = json!({ "add": Value::Object(add_obj) });

    let mut out: Vec<u8> = Vec::new();
    out.extend_from_slice(serde_json::to_string(&commit_info)?.as_bytes());
    out.push(b'\n');
    out.extend_from_slice(serde_json::to_string(&add_action)?.as_bytes());
    out.push(b'\n');
    Ok(out)
}

fn translate_partition_values(inputs: &CommitInputs) -> Result<Map<String, Value>> {
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

    #[test]
    fn commit_jsonl_has_two_lines_with_expected_keys() {
        let state = make_state_3col();
        let batch = make_batch_3col();
        let pv = HashMap::new();
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "abc.parquet",
            file_size: 123,
            modification_time_millis: 1_000_000_000_000,
            engine_info: "rocky-iceberg/test",
            partition_values: &pv,
            row_tracking: None,
        };
        let body = build_commit_jsonl(&inputs).unwrap();
        let s = std::str::from_utf8(&body).unwrap();
        let lines: Vec<&str> = s.lines().collect();
        assert_eq!(lines.len(), 2, "commit must have commitInfo + 1 add");
        let info: Value = serde_json::from_str(lines[0]).unwrap();
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        assert!(info.get("commitInfo").is_some());
        let add_obj = add.get("add").unwrap();
        assert_eq!(add_obj["path"], "abc.parquet");
        assert_eq!(add_obj["size"], 123);
        assert_eq!(add_obj["partitionValues"], json!({}));
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

    #[test]
    fn partitioned_commit_keys_partition_values_by_physical_uuid() {
        let state = make_partitioned_state();
        let batch = make_partitioned_batch();
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "region=eu/abc.parquet",
            file_size: 100,
            modification_time_millis: 0,
            engine_info: "rocky-iceberg/test",
            partition_values: &pv,
            row_tracking: None,
        };
        let body = build_commit_jsonl(&inputs).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&body).unwrap().lines().collect();
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        let add_obj = add.get("add").unwrap();
        // Exp 11 — partitionValues MUST be keyed by physical UUID, not logical name.
        assert_eq!(add_obj["partitionValues"], json!({"col-region": "eu"}));
        assert_eq!(add_obj["path"], "region=eu/abc.parquet");
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
    fn partitioned_commit_rejects_missing_partition_value() {
        let state = make_partitioned_state();
        let batch = make_partitioned_batch();
        let pv = HashMap::new();
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "x.parquet",
            file_size: 0,
            modification_time_millis: 0,
            engine_info: "t",
            partition_values: &pv,
            row_tracking: None,
        };
        match build_commit_jsonl(&inputs) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("`region`"),
                    "must name the missing column: {msg}"
                );
            }
            other => panic!("expected DeltaLog error, got {other:?}"),
        }
    }

    fn make_rt_state() -> UniformTableState {
        let mut state = make_state_3col();
        state.row_tracking_enabled = true;
        state.row_tracking_next_id = 100;
        state
    }

    #[test]
    fn row_tracking_commit_adds_base_row_id_and_watermark_action() {
        let state = make_rt_state();
        let batch = make_batch_3col();
        let pv = HashMap::new();
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "abc.parquet",
            file_size: 123,
            modification_time_millis: 0,
            engine_info: "t",
            partition_values: &pv,
            row_tracking: Some(RowTrackingCommit {
                base_row_id: 100,
                default_row_commit_version: 7,
                new_high_water_mark: 102,
            }),
        };
        let body = build_commit_jsonl(&inputs).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&body).unwrap().lines().collect();
        assert_eq!(
            lines.len(),
            3,
            "rowTracking commit is commitInfo + add + domainMetadata"
        );
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        let add_obj = add.get("add").unwrap();
        // Exp 9 finding: both fields required on every add action.
        assert_eq!(add_obj["baseRowId"], 100);
        assert_eq!(add_obj["defaultRowCommitVersion"], 7);

        let dm: Value = serde_json::from_str(lines[2]).unwrap();
        let dm_obj = dm.get("domainMetadata").unwrap();
        assert_eq!(dm_obj["domain"], "delta.rowTracking");
        assert_eq!(dm_obj["removed"], false);
        // configuration is a JSON-encoded string holding rowIdHighWaterMark.
        let cfg_raw = dm_obj["configuration"].as_str().unwrap();
        let cfg: Value = serde_json::from_str(cfg_raw).unwrap();
        assert_eq!(cfg["rowIdHighWaterMark"], 102);
    }

    #[test]
    fn unpartitioned_commit_with_no_row_tracking_emits_two_lines() {
        // Regression: a non-rowTracking commit on an unpartitioned table
        // still emits exactly commitInfo + add (no trailing
        // domainMetadata).
        let state = make_state_3col();
        let batch = make_batch_3col();
        let pv = HashMap::new();
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "abc.parquet",
            file_size: 1,
            modification_time_millis: 0,
            engine_info: "t",
            partition_values: &pv,
            row_tracking: None,
        };
        let body = build_commit_jsonl(&inputs).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&body).unwrap().lines().collect();
        assert_eq!(lines.len(), 2);
        let add: Value = serde_json::from_str(lines[1]).unwrap();
        let add_obj = add.get("add").unwrap();
        assert!(!add_obj.as_object().unwrap().contains_key("baseRowId"));
        assert!(
            !add_obj
                .as_object()
                .unwrap()
                .contains_key("defaultRowCommitVersion")
        );
    }

    #[test]
    fn partitioned_commit_rejects_extra_partition_value() {
        let state = make_partitioned_state();
        let batch = make_partitioned_batch();
        let mut pv = HashMap::new();
        pv.insert("region".to_string(), "eu".to_string());
        pv.insert("unknown".to_string(), "x".to_string());
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            add_file_path: "x.parquet",
            file_size: 0,
            modification_time_millis: 0,
            engine_info: "t",
            partition_values: &pv,
            row_tracking: None,
        };
        match build_commit_jsonl(&inputs) {
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

    // -- point-to (build_commit_jsonl_from_add) -----------------------------

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
    fn point_to_lifts_add_verbatim_and_refreshes_modification_time() {
        let add = recovered_unpartitioned_add();
        let new_mod_time = 1_700_000_000_123_i64;
        let body =
            build_commit_jsonl_from_add(&add, "rocky-iceberg/test", new_mod_time, &[]).unwrap();
        let lines: Vec<&str> = std::str::from_utf8(&body).unwrap().lines().collect();
        assert_eq!(lines.len(), 2, "point-to emits commitInfo + the lifted add");

        let info: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(info["commitInfo"]["operation"], "WRITE");
        assert_eq!(info["commitInfo"]["timestamp"], new_mod_time);
        assert_eq!(info["commitInfo"]["engineInfo"], "rocky-iceberg/test");

        let lifted: Value = serde_json::from_str(lines[1]).unwrap();
        let lifted_add = lifted.get("add").unwrap();
        // path / size / dataChange carry over byte-for-byte.
        assert_eq!(lifted_add["path"], "abc123.parquet");
        assert_eq!(lifted_add["size"], 4096);
        assert_eq!(lifted_add["dataChange"], true);
        // modificationTime is the ONLY field refreshed.
        assert_eq!(lifted_add["modificationTime"], new_mod_time);
        // stats (numRecords + min/max/nullCount keyed by physical UUID) lift
        // verbatim — no recompute, no live batch.
        let stats: Value = serde_json::from_str(lifted_add["stats"].as_str().unwrap()).unwrap();
        assert_eq!(stats["numRecords"], 3);
        assert_eq!(stats["minValues"]["col-id"], 0);
        assert_eq!(stats["maxValues"]["col-id"], 2);
        assert_eq!(stats["nullCount"]["col-id"], 0);
    }

    #[test]
    fn point_to_refuses_partitioned_add() {
        let mut add = recovered_unpartitioned_add();
        add.insert("partitionValues".into(), json!({"col-region": "eu"}));
        add.insert("path".into(), Value::String("region=eu/abc.parquet".into()));
        match build_commit_jsonl_from_add(&add, "t", 0, &["region".to_string()]) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("partitioned"),
                    "must refuse a partitioned add: {msg}"
                );
            }
            other => panic!("expected DeltaLog refusal, got {other:?}"),
        }
    }

    #[test]
    fn point_to_refuses_row_tracking_add() {
        let mut add = recovered_unpartitioned_add();
        add.insert("baseRowId".into(), Value::from(100_i64));
        add.insert("defaultRowCommitVersion".into(), Value::from(7_i64));
        match build_commit_jsonl_from_add(&add, "t", 0, &[]) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("rowTracking"),
                    "must refuse a rowTracking add: {msg}"
                );
            }
            other => panic!("expected DeltaLog refusal, got {other:?}"),
        }
    }

    #[test]
    fn point_to_emits_no_extra_actions() {
        // The point-to is exactly 2 lines — no domainMetadata, no second add.
        let add = recovered_unpartitioned_add();
        let body = build_commit_jsonl_from_add(&add, "t", 42, &[]).unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap().lines().count(),
            2,
            "a point-to commit is commitInfo + one add only"
        );
    }

    #[test]
    fn timestamp_min_max_are_iso8601_z() {
        let state = make_state_3col();
        let batch = make_batch_3col();
        let stats = compute_stats(&batch, &state).unwrap();

        let min_ts = stats["minValues"]["col-ts"].as_str().unwrap();
        let max_ts = stats["maxValues"]["col-ts"].as_str().unwrap();
        assert!(
            min_ts.ends_with('Z'),
            "min ts must end with Z, got {min_ts}"
        );
        assert!(
            max_ts.ends_with('Z'),
            "max ts must end with Z, got {max_ts}"
        );
        assert_eq!(min_ts, "1970-01-01T00:00:01.000000Z");
        assert_eq!(max_ts, "1970-01-01T00:00:03.000000Z");
    }
}
