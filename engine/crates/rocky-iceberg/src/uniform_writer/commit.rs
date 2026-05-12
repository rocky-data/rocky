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
}

/// Serialize a Delta commit as JSONL bytes ready to PUT at
/// `_delta_log/{N:020}.json`.
///
/// Single `commitInfo` + single `add` action. Multi-`add` commits (e.g.
/// writing several partitions atomically) are not Phase 2 — callers issue
/// one commit per partition write.
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
    let add_action = json!({
        "add": {
            "path": inputs.add_file_path,
            "partitionValues": partition_values_physical,
            "size": inputs.file_size,
            "modificationTime": inputs.modification_time_millis,
            "dataChange": true,
            "stats": stats_json,
        }
    });

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
