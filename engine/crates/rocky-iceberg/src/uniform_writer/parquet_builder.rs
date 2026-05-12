//! Build deterministic Parquet bytes from an Arrow `RecordBatch`.
//!
//! Settings are pinned to maximise byte-stability across runs of the same
//! Rocky version on the same input:
//! - `WriterVersion::PARQUET_2_0` (data page v2 — pyarrow's `version="2.6"`)
//! - SNAPPY compression
//! - 1 MiB data page size
//! - dictionary encoding disabled
//! - statistics at column-chunk granularity
//! - Arrow schema written to file metadata (`store_schema=true`)
//!
//! Cross-language byte-identity (Rust vs pyarrow) is NOT a goal — the
//! recipe-hash protocol only requires identical bytes from the SAME Rocky
//! version on the SAME inputs.
//!
//! The input batch carries logical column names matching the table's
//! schema; this module rebuilds the schema with each column renamed to its
//! `delta.columnMapping.physicalName` UUID + a `PARQUET:field_id` metadata
//! entry, then writes that batch to a buffer.
//!
//! Partition columns (members of `state.partition_columns`) are skipped —
//! they are NOT physically present in the Parquet file. Delta + Iceberg
//! reconstruct the partition column from `add.partitionValues` + the file's
//! Hive-style path prefix; storing them in the file would be redundant and
//! breaks the partition-aware reader contract.

use std::collections::HashMap;
use std::sync::Arc;

use std::collections::HashSet;

use arrow::array::{ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion};

use super::{Result, UniformTableState, UniformWriterError};

/// Project + rename the batch: drop partition columns, rename the remaining
/// columns to their physical UUIDs, attach `PARQUET:field_id` metadata.
fn project_to_physical(
    batch: &RecordBatch,
    state: &UniformTableState,
) -> Result<(Arc<Schema>, Vec<ArrayRef>)> {
    let partitions: HashSet<&str> = state.partition_columns.iter().map(String::as_str).collect();
    let logical_schema = batch.schema();
    let mut fields: Vec<Field> = Vec::with_capacity(logical_schema.fields().len());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(logical_schema.fields().len());
    for (i, f) in logical_schema.fields().iter().enumerate() {
        if partitions.contains(f.name().as_str()) {
            continue;
        }
        let physical = state.physical.get(f.name()).ok_or_else(|| {
            UniformWriterError::DeltaLog(format!(
                "column `{}` not in discovered table schema",
                f.name()
            ))
        })?;
        let field_id = state.field_id.get(f.name()).copied().ok_or_else(|| {
            UniformWriterError::DeltaLog(format!(
                "column `{}` missing field_id in discovered table schema",
                f.name()
            ))
        })?;
        let mut metadata = HashMap::new();
        metadata.insert("PARQUET:field_id".to_string(), field_id.to_string());
        fields.push(
            Field::new(physical, f.data_type().clone(), f.is_nullable()).with_metadata(metadata),
        );
        columns.push(batch.column(i).clone());
    }
    Ok((Arc::new(Schema::new(fields)), columns))
}

/// Build Parquet bytes from an Arrow `RecordBatch`.
///
/// Any columns whose name is in `state.partition_columns` are dropped from
/// the Parquet output (partition values live in `add.partitionValues` +
/// the file's Hive-style path prefix, not the file itself).
pub fn build_parquet(batch: &RecordBatch, state: &UniformTableState) -> Result<Vec<u8>> {
    let (new_schema, columns) = project_to_physical(batch, state)?;
    let new_batch = RecordBatch::try_new(new_schema.clone(), columns)?;

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_compression(Compression::SNAPPY)
        .set_data_page_size_limit(1 << 20)
        .set_dictionary_enabled(false)
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let mut buf: Vec<u8> = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, new_schema, Some(props))?;
    writer.write(&new_batch)?;
    writer.close()?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use blake3::Hash;

    fn make_state() -> UniformTableState {
        let mut physical = HashMap::new();
        physical.insert("id".to_string(), "col-id".to_string());
        physical.insert("name".to_string(), "col-name".to_string());
        let mut field_id = HashMap::new();
        field_id.insert("id".to_string(), 1);
        field_id.insert("name".to_string(), 2);
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

    fn make_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Int64Array::from(vec![1_i64, 2, 3]);
        let name_array = StringArray::from(vec!["a", "b", "c"]);
        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)]).unwrap()
    }

    #[test]
    fn build_parquet_is_byte_stable_across_runs() {
        let state = make_state();
        let bytes1 = build_parquet(&make_batch(), &state).unwrap();
        let bytes2 = build_parquet(&make_batch(), &state).unwrap();
        assert_eq!(
            Hash::from_bytes(blake3::hash(&bytes1).into()),
            Hash::from_bytes(blake3::hash(&bytes2).into()),
            "build_parquet must be byte-stable across runs of the same Rocky version"
        );
    }

    #[test]
    fn build_parquet_renames_columns_to_physical_uuids() {
        let state = make_state();
        let bytes = build_parquet(&make_batch(), &state).unwrap();

        // Round-trip: read the Parquet back and confirm physical names landed.
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batch: Vec<_> = reader.collect::<std::result::Result<_, _>>().unwrap();
        let schema = batch[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["col-id", "col-name"]);

        for f in schema.fields() {
            let fid = f.metadata().get("PARQUET:field_id").map(String::as_str);
            assert!(fid.is_some(), "field {} missing PARQUET:field_id", f.name());
        }
    }

    #[test]
    fn build_parquet_errors_on_unknown_column() {
        let mut state = make_state();
        state.physical.remove("name");
        match build_parquet(&make_batch(), &state) {
            Err(UniformWriterError::DeltaLog(msg)) => {
                assert!(
                    msg.contains("`name`"),
                    "error must name the missing column: {msg}"
                );
            }
            other => panic!("expected DeltaLog error, got {other:?}"),
        }
    }
}
