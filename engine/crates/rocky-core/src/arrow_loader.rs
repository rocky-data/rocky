//! Batch CSV reader and INSERT SQL generator for generic file loading.
//!
//! Provides a lightweight alternative to the `arrow` crate for warehouses
//! that lack native bulk-load (COPY) support (e.g., Databricks REST API).
//! Instead of materializing full Arrow `RecordBatch` objects, this module
//! reads CSV files in configurable-size row batches ([`RowBatch`]) and
//! generates batched `INSERT INTO ... VALUES` SQL statements that can be
//! executed via any [`WarehouseAdapter::execute_statement`] implementation.
//!
//! ## Design rationale
//!
//! DuckDB has its own [`LoaderAdapter`] backed by native `COPY` commands,
//! which is always faster. This module is the **fallback** path: any
//! warehouse adapter that can execute SQL statements can use it to load
//! CSV data without a dedicated bulk-load API.
//!
//! ## Usage
//!
//! ```rust,no_run
//! use std::path::Path;
//! use rocky_core::arrow_loader::{CsvBatchReader, generate_batch_insert_sql};
//! # struct FakeDialect;
//! # // (SqlDialect impl omitted for brevity)
//!
//! let reader = CsvBatchReader::new(Path::new("data.csv"), 1000).unwrap();
//! for batch in reader {
//!     let batch = batch.unwrap();
//!     // generate_batch_insert_sql(&batch, "catalog.schema.table", &dialect);
//! }
//! ```
//!
//! [`LoaderAdapter`]: rocky_adapter_sdk::LoaderAdapter
//! [`WarehouseAdapter::execute_statement`]: crate::traits::WarehouseAdapter::execute_statement

use std::path::Path;

use thiserror::Error;

use crate::traits::SqlDialect;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from batch CSV reading and SQL generation.
#[derive(Debug, Error)]
pub enum BatchLoaderError {
    #[error("failed to open CSV file {path}: {source}")]
    OpenFile { path: String, source: csv::Error },

    #[error("failed to read CSV record in {path}: {source}")]
    ReadRecord { path: String, source: csv::Error },

    #[error("CSV file has no header row: {0}")]
    NoHeader(String),

    #[error("batch is empty — cannot generate INSERT SQL")]
    EmptyBatch,
}

// ---------------------------------------------------------------------------
// RowBatch
// ---------------------------------------------------------------------------

/// A batch of rows read from a CSV file.
///
/// Each row is a `Vec<String>` of cell values (empty strings represent
/// NULL/missing values). The column order matches [`column_names`](Self::column_names).
#[derive(Debug, Clone)]
pub struct RowBatch {
    /// Column names from the CSV header.
    pub column_names: Vec<String>,
    /// Row data — each inner Vec has the same length as `column_names`.
    pub rows: Vec<Vec<String>>,
    /// Number of rows in this batch (same as `rows.len()`).
    pub row_count: usize,
}

// ---------------------------------------------------------------------------
// ColumnTypeDef
// ---------------------------------------------------------------------------

/// A column name paired with its inferred SQL type.
///
/// Produced by [`infer_column_types`] using the same heuristic as
/// [`crate::seeds::infer_schema_from_csv`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnTypeDef {
    pub name: String,
    pub data_type: String,
}

// ---------------------------------------------------------------------------
// CsvBatchReader
// ---------------------------------------------------------------------------

/// Streaming CSV reader that yields [`RowBatch`] chunks of configurable size.
///
/// Constructed via [`CsvBatchReader::new`] or [`CsvBatchReader::with_delimiter`].
/// Implements [`Iterator`] so it can be used in `for` loops.
///
/// `Debug` is implemented manually because `csv::Reader` does not derive it.
pub struct CsvBatchReader {
    reader: csv::Reader<std::fs::File>,
    column_names: Vec<String>,
    batch_size: usize,
    path: String,
    done: bool,
}

impl std::fmt::Debug for CsvBatchReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CsvBatchReader")
            .field("column_names", &self.column_names)
            .field("batch_size", &self.batch_size)
            .field("path", &self.path)
            .field("done", &self.done)
            .finish()
    }
}

impl CsvBatchReader {
    /// Open a CSV file for batched reading with the default comma delimiter.
    pub fn new(path: &Path, batch_size: usize) -> Result<Self, BatchLoaderError> {
        Self::with_delimiter(path, batch_size, b',')
    }

    /// Open a CSV file for batched reading with a custom delimiter.
    pub fn with_delimiter(
        path: &Path,
        batch_size: usize,
        delimiter: u8,
    ) -> Result<Self, BatchLoaderError> {
        let batch_size = if batch_size == 0 { 1 } else { batch_size };

        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .delimiter(delimiter)
            .from_path(path)
            .map_err(|e| BatchLoaderError::OpenFile {
                path: path.display().to_string(),
                source: e,
            })?;

        let column_names: Vec<String> = reader
            .headers()
            .map_err(|e| BatchLoaderError::OpenFile {
                path: path.display().to_string(),
                source: e,
            })?
            .iter()
            .map(|h| h.trim().to_string())
            .collect();

        if column_names.is_empty() {
            return Err(BatchLoaderError::NoHeader(path.display().to_string()));
        }

        Ok(Self {
            reader,
            column_names,
            batch_size,
            path: path.display().to_string(),
            done: false,
        })
    }

    /// Returns the column names parsed from the CSV header.
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }
}

impl Iterator for CsvBatchReader {
    type Item = Result<RowBatch, BatchLoaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut rows = Vec::with_capacity(self.batch_size);

        for _ in 0..self.batch_size {
            match self.reader.records().next() {
                Some(Ok(record)) => {
                    let row: Vec<String> = record
                        .iter()
                        .map(std::string::ToString::to_string)
                        .collect();
                    rows.push(row);
                }
                Some(Err(e)) => {
                    self.done = true;
                    return Some(Err(BatchLoaderError::ReadRecord {
                        path: self.path.clone(),
                        source: e,
                    }));
                }
                None => {
                    self.done = true;
                    break;
                }
            }
        }

        if rows.is_empty() {
            return None;
        }

        let row_count = rows.len();
        Some(Ok(RowBatch {
            column_names: self.column_names.clone(),
            rows,
            row_count,
        }))
    }
}

// ---------------------------------------------------------------------------
// Convenience: read all batches at once
// ---------------------------------------------------------------------------

/// Read an entire CSV file into row batches of the given size.
///
/// This is a convenience wrapper around [`CsvBatchReader`] that collects
/// all batches into a `Vec`. For large files, prefer iterating over the
/// reader directly to avoid holding the entire dataset in memory.
pub fn read_csv_batches(path: &Path, batch_size: usize) -> Result<Vec<RowBatch>, BatchLoaderError> {
    let reader = CsvBatchReader::new(path, batch_size)?;
    reader.collect()
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Generate an `INSERT INTO <target> (<cols>) VALUES (...), (...)` statement
/// from a [`RowBatch`].
///
/// Cell values are formatted using the same rules as [`crate::seeds`]:
/// - Empty cells become `NULL`.
/// - Numeric-looking values are emitted bare.
/// - Everything else is single-quoted with internal quotes escaped.
///
/// The `dialect` parameter is used to format the target table reference via
/// [`SqlDialect`]; if the caller already has a fully-qualified target string,
/// pass it directly — this function accepts `&str` for the target.
pub fn generate_batch_insert_sql(
    batch: &RowBatch,
    target: &str,
    _dialect: &dyn SqlDialect,
) -> Result<String, BatchLoaderError> {
    if batch.rows.is_empty() {
        return Err(BatchLoaderError::EmptyBatch);
    }

    let col_list = batch.column_names.join(", ");

    let value_rows: Vec<String> = batch
        .rows
        .iter()
        .map(|row| {
            let cells: Vec<String> = row.iter().map(|cell| format_cell(cell)).collect();
            format!("({})", cells.join(", "))
        })
        .collect();

    Ok(format!(
        "INSERT INTO {target} ({col_list}) VALUES\n{values}",
        values = value_rows.join(",\n")
    ))
}

/// Format a single cell value for a SQL VALUES clause.
///
/// Rules:
/// - Empty/whitespace-only → `NULL`
/// - Parseable as `i64` → bare integer
/// - Parseable as `f64` → bare float
/// - Boolean (`true`/`false`, case-insensitive) → bare boolean
/// - Everything else → single-quoted with `'` escaped to `''`
fn format_cell(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "NULL".to_string();
    }

    // Boolean
    let lower = trimmed.to_lowercase();
    if lower == "true" || lower == "false" {
        return lower;
    }

    // Integer
    if trimmed.parse::<i64>().is_ok() {
        return trimmed.to_string();
    }

    // Float
    if trimmed.parse::<f64>().is_ok() {
        return trimmed.to_string();
    }

    // Default: single-quoted string
    format!("'{}'", trimmed.replace('\'', "''"))
}

// ---------------------------------------------------------------------------
// Type inference
// ---------------------------------------------------------------------------

/// Maximum number of rows to sample for type inference.
const INFER_SAMPLE_ROWS: usize = 1000;

/// Infer SQL column types from a slice of [`RowBatch`] values.
///
/// Uses the same detection heuristic as [`crate::seeds::infer_schema_from_csv`]:
/// BOOLEAN > INTEGER > FLOAT > TIMESTAMP > STRING. Cross-family values widen
/// to STRING (e.g., a column with both integers and timestamps becomes STRING).
///
/// Only the first [`INFER_SAMPLE_ROWS`] rows (across all batches) are sampled.
pub fn infer_column_types(batches: &[RowBatch]) -> Vec<ColumnTypeDef> {
    let Some(first) = batches.first() else {
        return Vec::new();
    };

    let col_count = first.column_names.len();
    let mut inferred: Vec<Option<InferredType>> = vec![None; col_count];
    let mut rows_sampled = 0usize;

    'outer: for batch in batches {
        for row in &batch.rows {
            if rows_sampled >= INFER_SAMPLE_ROWS {
                break 'outer;
            }
            for (i, cell) in row.iter().enumerate() {
                if i >= col_count {
                    break;
                }
                let trimmed = cell.trim();
                if trimmed.is_empty() {
                    continue; // NULL — don't narrow the type
                }
                let detected = detect_type(trimmed);
                inferred[i] = Some(match inferred[i] {
                    None => detected,
                    Some(prev) => widen(prev, detected),
                });
            }
            rows_sampled += 1;
        }
    }

    first
        .column_names
        .iter()
        .enumerate()
        .map(|(i, name)| ColumnTypeDef {
            name: name.clone(),
            data_type: inferred[i]
                .unwrap_or(InferredType::String)
                .as_sql_type()
                .to_string(),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Type detection internals (mirrors seeds.rs)
// ---------------------------------------------------------------------------

/// Inferred column types, ordered from most restrictive to least.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InferredType {
    Boolean,
    Integer,
    Float,
    Timestamp,
    String,
}

impl InferredType {
    fn as_sql_type(self) -> &'static str {
        match self {
            Self::Boolean => "BOOLEAN",
            Self::Integer => "BIGINT",
            Self::Float => "DOUBLE",
            Self::Timestamp => "TIMESTAMP",
            Self::String => "STRING",
        }
    }
}

/// Detect the most specific type for a single non-empty cell value.
fn detect_type(value: &str) -> InferredType {
    let lower = value.to_lowercase();
    if lower == "true" || lower == "false" {
        return InferredType::Boolean;
    }

    if value.parse::<i64>().is_ok() {
        return InferredType::Integer;
    }

    if value.parse::<f64>().is_ok() {
        return InferredType::Float;
    }

    if is_timestamp_like(value) {
        return InferredType::Timestamp;
    }

    InferredType::String
}

/// Heuristic check for timestamp-like strings.
///
/// Matches patterns like `2024-01-15`, `2024-01-15T10:30:00`,
/// `2024-01-15 10:30:00`, `2024-01-15T10:30:00Z`.
fn is_timestamp_like(value: &str) -> bool {
    if value.len() < 10 {
        return false;
    }
    let bytes = value.as_bytes();
    bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

/// Widen two types to their common supertype.
///
/// INTEGER + FLOAT → FLOAT. All other cross-family combinations → STRING.
fn widen(a: InferredType, b: InferredType) -> InferredType {
    if a == b {
        return a;
    }
    match (a, b) {
        (InferredType::Integer, InferredType::Float)
        | (InferredType::Float, InferredType::Integer) => InferredType::Float,
        _ => InferredType::String,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Helper to create a temp CSV file and return its path.
    fn write_temp_csv(dir: &Path, name: &str, content: &str) -> std::path::PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    /// Minimal dialect for testing SQL generation.
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> crate::traits::AdapterResult<String> {
            Ok(format!("{catalog}.{schema}.{table}"))
        }

        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS {select_sql}")
        }

        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target} {select_sql}")
        }

        fn merge_into(
            &self,
            _target: &str,
            _source_sql: &str,
            _keys: &[String],
            _update_cols: &crate::ir::ColumnSelection,
        ) -> crate::traits::AdapterResult<String> {
            Ok(String::new())
        }

        fn select_clause(
            &self,
            _columns: &crate::ir::ColumnSelection,
            _metadata: &[crate::ir::MetadataColumn],
        ) -> crate::traits::AdapterResult<String> {
            Ok(String::new())
        }

        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _target_ref: &str,
        ) -> crate::traits::AdapterResult<String> {
            Ok(String::new())
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE TABLE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }

        fn create_catalog_sql(&self, _name: &str) -> Option<crate::traits::AdapterResult<String>> {
            None
        }

        fn create_schema_sql(
            &self,
            _catalog: &str,
            _schema: &str,
        ) -> Option<crate::traits::AdapterResult<String>> {
            None
        }

        fn tablesample_clause(&self, _percent: u32) -> Option<String> {
            None
        }

        fn insert_overwrite_partition(
            &self,
            _target: &str,
            _partition_filter: &str,
            _select_sql: &str,
        ) -> crate::traits::AdapterResult<Vec<String>> {
            Ok(vec![])
        }
    }

    // -- CsvBatchReader -------------------------------------------------------

    #[test]
    fn batch_reader_single_batch() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "id,name\n1,Alice\n2,Bob\n3,Carol\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv);

        let batches: Vec<RowBatch> = CsvBatchReader::new(&path, 100)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].row_count, 3);
        assert_eq!(batches[0].column_names, vec!["id", "name"]);
        assert_eq!(batches[0].rows[0], vec!["1", "Alice"]);
        assert_eq!(batches[0].rows[2], vec!["3", "Carol"]);
    }

    #[test]
    fn batch_reader_multiple_batches() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "id,val\n1,a\n2,b\n3,c\n4,d\n5,e\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv);

        let batches: Vec<RowBatch> = CsvBatchReader::new(&path, 2)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(batches.len(), 3); // 2 + 2 + 1
        assert_eq!(batches[0].row_count, 2);
        assert_eq!(batches[1].row_count, 2);
        assert_eq!(batches[2].row_count, 1);
    }

    #[test]
    fn batch_reader_zero_batch_size_coerced_to_one() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "id\n1\n2\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv);

        let batches: Vec<RowBatch> = CsvBatchReader::new(&path, 0)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        // batch_size=0 coerced to 1 → two batches of 1 row each
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].row_count, 1);
    }

    #[test]
    fn batch_reader_empty_file_no_rows() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "id,name\n"; // header only, no rows
        let path = write_temp_csv(dir.path(), "test.csv", csv);

        let batches: Vec<RowBatch> = CsvBatchReader::new(&path, 100)
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert!(batches.is_empty());
    }

    #[test]
    fn batch_reader_no_header_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = write_temp_csv(dir.path(), "empty.csv", "");

        let result = CsvBatchReader::new(&path, 100);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("no header") || err.contains("No header"));
    }

    #[test]
    fn batch_reader_tsv_delimiter() {
        let dir = tempfile::tempdir().unwrap();
        let tsv = "id\tname\n1\tAlice\n2\tBob\n";
        let path = write_temp_csv(dir.path(), "test.tsv", tsv);

        let batches: Vec<RowBatch> = CsvBatchReader::with_delimiter(&path, 100, b'\t')
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].column_names, vec!["id", "name"]);
        assert_eq!(batches[0].rows[0], vec!["1", "Alice"]);
    }

    // -- read_csv_batches convenience -----------------------------------------

    #[test]
    fn read_csv_batches_convenience() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "x,y\n1,2\n3,4\n5,6\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv);

        let batches = read_csv_batches(&path, 2).unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].row_count, 2);
        assert_eq!(batches[1].row_count, 1);
    }

    // -- format_cell ----------------------------------------------------------

    #[test]
    fn format_cell_empty_is_null() {
        assert_eq!(format_cell(""), "NULL");
        assert_eq!(format_cell("   "), "NULL");
    }

    #[test]
    fn format_cell_integer() {
        assert_eq!(format_cell("42"), "42");
        assert_eq!(format_cell("-7"), "-7");
    }

    #[test]
    fn format_cell_float() {
        assert_eq!(format_cell("3.14"), "3.14");
        assert_eq!(format_cell("-0.5"), "-0.5");
    }

    #[test]
    fn format_cell_boolean() {
        assert_eq!(format_cell("true"), "true");
        assert_eq!(format_cell("False"), "false");
    }

    #[test]
    fn format_cell_string_escaping() {
        assert_eq!(format_cell("hello"), "'hello'");
        assert_eq!(format_cell("it's"), "'it''s'");
    }

    // -- generate_batch_insert_sql --------------------------------------------

    #[test]
    fn insert_sql_basic() {
        let batch = RowBatch {
            column_names: vec!["id".into(), "name".into()],
            rows: vec![
                vec!["1".into(), "Alice".into()],
                vec!["2".into(), "Bob".into()],
            ],
            row_count: 2,
        };

        let sql = generate_batch_insert_sql(&batch, "cat.sch.tbl", &TestDialect).unwrap();

        assert!(sql.starts_with("INSERT INTO cat.sch.tbl (id, name) VALUES"));
        assert!(sql.contains("(1, 'Alice')"));
        assert!(sql.contains("(2, 'Bob')"));
    }

    #[test]
    fn insert_sql_with_nulls_and_quotes() {
        let batch = RowBatch {
            column_names: vec!["id".into(), "note".into()],
            rows: vec![
                vec!["1".into(), "it's fine".into()],
                vec!["2".into(), "".into()],
            ],
            row_count: 2,
        };

        let sql = generate_batch_insert_sql(&batch, "t", &TestDialect).unwrap();

        assert!(sql.contains("'it''s fine'"));
        assert!(sql.contains("NULL"));
    }

    #[test]
    fn insert_sql_empty_batch_errors() {
        let batch = RowBatch {
            column_names: vec!["id".into()],
            rows: vec![],
            row_count: 0,
        };

        let result = generate_batch_insert_sql(&batch, "t", &TestDialect);
        assert!(result.is_err());
    }

    #[test]
    fn insert_sql_mixed_types() {
        let batch = RowBatch {
            column_names: vec!["a".into(), "b".into(), "c".into(), "d".into()],
            rows: vec![vec![
                "42".into(),
                "3.14".into(),
                "true".into(),
                "hello world".into(),
            ]],
            row_count: 1,
        };

        let sql = generate_batch_insert_sql(&batch, "t", &TestDialect).unwrap();

        assert!(sql.contains("42"));
        assert!(sql.contains("3.14"));
        assert!(sql.contains("true"));
        assert!(sql.contains("'hello world'"));
    }

    // -- infer_column_types ---------------------------------------------------

    #[test]
    fn infer_types_basic() {
        let batch = RowBatch {
            column_names: vec![
                "id".into(),
                "name".into(),
                "amount".into(),
                "active".into(),
                "created_at".into(),
            ],
            rows: vec![
                vec![
                    "1".into(),
                    "Alice".into(),
                    "29.99".into(),
                    "true".into(),
                    "2024-01-15".into(),
                ],
                vec![
                    "2".into(),
                    "Bob".into(),
                    "49.50".into(),
                    "false".into(),
                    "2024-02-20".into(),
                ],
            ],
            row_count: 2,
        };

        let types = infer_column_types(&[batch]);

        assert_eq!(types.len(), 5);
        assert_eq!(types[0].data_type, "BIGINT");
        assert_eq!(types[1].data_type, "STRING");
        assert_eq!(types[2].data_type, "DOUBLE");
        assert_eq!(types[3].data_type, "BOOLEAN");
        assert_eq!(types[4].data_type, "TIMESTAMP");
    }

    #[test]
    fn infer_types_widening() {
        let batch = RowBatch {
            column_names: vec!["val".into()],
            rows: vec![
                vec!["42".into()],   // INTEGER
                vec!["3.14".into()], // FLOAT → widens to FLOAT
            ],
            row_count: 2,
        };

        let types = infer_column_types(&[batch]);
        assert_eq!(types[0].data_type, "DOUBLE");
    }

    #[test]
    fn infer_types_cross_family_widens_to_string() {
        let batch = RowBatch {
            column_names: vec!["val".into()],
            rows: vec![
                vec!["42".into()],         // INTEGER
                vec!["2024-01-15".into()], // TIMESTAMP → cross-family → STRING
            ],
            row_count: 2,
        };

        let types = infer_column_types(&[batch]);
        assert_eq!(types[0].data_type, "STRING");
    }

    #[test]
    fn infer_types_all_empty_defaults_to_string() {
        let batch = RowBatch {
            column_names: vec!["col".into()],
            rows: vec![vec!["".into()], vec!["".into()]],
            row_count: 2,
        };

        let types = infer_column_types(&[batch]);
        assert_eq!(types[0].data_type, "STRING");
    }

    #[test]
    fn infer_types_across_multiple_batches() {
        let batch1 = RowBatch {
            column_names: vec!["val".into()],
            rows: vec![vec!["1".into()], vec!["2".into()]],
            row_count: 2,
        };
        let batch2 = RowBatch {
            column_names: vec!["val".into()],
            rows: vec![vec!["3.5".into()]],
            row_count: 1,
        };

        let types = infer_column_types(&[batch1, batch2]);
        // INTEGER widened by FLOAT → DOUBLE
        assert_eq!(types[0].data_type, "DOUBLE");
    }

    #[test]
    fn infer_types_empty_batches() {
        let types = infer_column_types(&[]);
        assert!(types.is_empty());
    }

    // -- Type detection unit tests --------------------------------------------

    #[test]
    fn detect_type_boolean() {
        assert_eq!(detect_type("true"), InferredType::Boolean);
        assert_eq!(detect_type("false"), InferredType::Boolean);
        assert_eq!(detect_type("True"), InferredType::Boolean);
    }

    #[test]
    fn detect_type_integer() {
        assert_eq!(detect_type("42"), InferredType::Integer);
        assert_eq!(detect_type("-1"), InferredType::Integer);
        assert_eq!(detect_type("0"), InferredType::Integer);
    }

    #[test]
    fn detect_type_float() {
        assert_eq!(detect_type("3.14"), InferredType::Float);
        assert_eq!(detect_type("1e10"), InferredType::Float);
    }

    #[test]
    fn detect_type_timestamp() {
        assert_eq!(detect_type("2024-01-15"), InferredType::Timestamp);
        assert_eq!(detect_type("2024-01-15T10:30:00"), InferredType::Timestamp);
    }

    #[test]
    fn detect_type_string() {
        assert_eq!(detect_type("hello"), InferredType::String);
        assert_eq!(detect_type("123-abc"), InferredType::String);
    }

    // -- End-to-end: read + infer + generate ----------------------------------

    #[test]
    fn end_to_end_csv_to_insert_sql() {
        let dir = tempfile::tempdir().unwrap();
        let csv = "id,name,score\n1,Alice,95.5\n2,Bob,87.0\n3,Carol,\n";
        let path = write_temp_csv(dir.path(), "scores.csv", csv);

        let batches = read_csv_batches(&path, 100).unwrap();
        assert_eq!(batches.len(), 1);

        let types = infer_column_types(&batches);
        assert_eq!(types[0].data_type, "BIGINT");
        assert_eq!(types[1].data_type, "STRING");
        assert_eq!(types[2].data_type, "DOUBLE");

        let sql = generate_batch_insert_sql(&batches[0], "warehouse.staging.scores", &TestDialect)
            .unwrap();

        assert!(sql.contains("INSERT INTO warehouse.staging.scores"));
        assert!(sql.contains("(1, 'Alice', 95.5)"));
        assert!(sql.contains("(2, 'Bob', 87.0)"));
        assert!(sql.contains("(3, 'Carol', NULL)"));
    }
}
