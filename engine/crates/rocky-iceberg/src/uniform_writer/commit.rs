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
//! finding); Phase 1 doesn't ship partitioned-table support so this module
//! always emits an empty map there.

use std::collections::BTreeMap;

use arrow::array::{Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use chrono::DateTime;
use chrono::TimeZone;
use chrono::Utc;
use serde_json::{Map, Value, json};

use super::{Result, UniformTableState, UniformWriterError};

/// Inputs to `build_commit_jsonl`. Borrowed so callers can build many
/// commits from the same `RecordBatch` without cloning.
#[derive(Debug, Clone, Copy)]
pub struct CommitInputs<'a> {
    pub batch: &'a RecordBatch,
    pub state: &'a UniformTableState,
    pub file_name: &'a str,
    pub file_size: u64,
    pub modification_time_millis: i64,
    pub engine_info: &'a str,
}

/// Serialize a Delta commit as JSONL bytes ready to PUT at
/// `_delta_log/{N:020}.json`.
pub fn build_commit_jsonl(inputs: &CommitInputs) -> Result<Vec<u8>> {
    let stats = compute_stats(inputs.batch, inputs.state)?;
    // BTreeMap so the stats payload is key-stable (matters for downstream
    // tooling diffing log entries — also makes the test output stable).
    let stats_json = serde_json::to_string(&stats)?;

    let commit_info = json!({
        "commitInfo": {
            "timestamp": inputs.modification_time_millis,
            "operation": "WRITE",
            "operationParameters": {"mode": "Append", "partitionBy": "[]"},
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "engineInfo": inputs.engine_info,
        }
    });
    let add_action = json!({
        "add": {
            "path": inputs.file_name,
            "partitionValues": {},
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

    for (i, field) in batch.schema().fields().iter().enumerate() {
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
        let inputs = CommitInputs {
            batch: &batch,
            state: &state,
            file_name: "abc.parquet",
            file_size: 123,
            modification_time_millis: 1_000_000_000_000,
            engine_info: "rocky-iceberg/test",
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
