//! CSV seed loader for static reference data.
//!
//! Discovers `.csv` files in a `seeds/` directory, infers column types from
//! the data, and generates `CREATE TABLE` + batched `INSERT INTO VALUES`
//! SQL via the [`SqlDialect`] trait. This is the "dbt seed" equivalent for
//! Rocky: load dimension tables, country codes, date spines, etc. from
//! checked-in CSV files.
//!
//! ## Convention
//!
//! ```text
//! seeds/
//!   dim_date.csv          -> table "dim_date"
//!   dim_date.toml         -> optional sidecar (overrides target, types)
//!   country_codes.csv     -> table "country_codes"
//! ```
//!
//! ## Limitations (foundation)
//!
//! - CSV only (no Parquet, Arrow, or COPY INTO).
//! - INSERT INTO VALUES batching (no bulk-load path).
//! - Type inference is heuristic; use a `.toml` sidecar to pin types.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::traits::SqlDialect;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors from seed discovery, parsing, and SQL generation.
#[derive(Debug, Error)]
pub enum SeedError {
    #[error("failed to read seed file {path}: {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    #[error("failed to parse CSV in {path}: {source}")]
    CsvParse { path: String, source: csv::Error },

    #[error("CSV file {path} has no header row")]
    NoHeader { path: String },

    #[error("failed to parse sidecar TOML {path}: {source}")]
    SidecarParse {
        path: String,
        source: toml::de::Error,
    },

    #[error("seed directory does not exist: {0}")]
    DirNotFound(String),
}

// ---------------------------------------------------------------------------
// Configuration types
// ---------------------------------------------------------------------------

/// How to load the seed into the target table.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedStrategy {
    /// Drop the table and recreate it from scratch (default).
    #[default]
    DropAndRecreate,
}

/// File format for seed data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SeedFormat {
    #[default]
    Csv,
    Tsv,
}

/// Target coordinates for a seed table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedTarget {
    #[serde(default)]
    pub catalog: Option<String>,
    pub schema: String,
    #[serde(default)]
    pub table: Option<String>,
}

/// Configuration for a single seed, loaded from an optional `.toml` sidecar.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeedConfig {
    /// Logical name (defaults to file stem).
    #[serde(default)]
    pub name: Option<String>,
    /// Target table coordinates.
    #[serde(default)]
    pub target: Option<SeedTarget>,
    /// Load strategy.
    #[serde(default)]
    pub strategy: SeedStrategy,
    /// Explicit column type overrides. Keys are column names, values are
    /// SQL type strings (e.g., `"INTEGER"`, `"TIMESTAMP"`).
    #[serde(default)]
    pub column_types: std::collections::HashMap<String, String>,
}

/// A discovered seed file with its configuration.
#[derive(Debug, Clone)]
pub struct SeedFile {
    /// Logical name (file stem, or overridden by sidecar).
    pub name: String,
    /// Absolute path to the CSV file.
    pub file_path: PathBuf,
    /// File format.
    pub format: SeedFormat,
    /// Merged configuration (sidecar + defaults).
    pub config: SeedConfig,
}

// ---------------------------------------------------------------------------
// Column type inference
// ---------------------------------------------------------------------------

/// A column definition inferred from CSV data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: String,
}

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
    /// Returns the SQL type string for this inferred type.
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

/// Maximum number of rows to sample for type inference.
const INFER_SAMPLE_ROWS: usize = 1000;

/// Infer column types from a CSV file by sampling the first N rows.
///
/// Empty cells are ignored during inference (treated as nullable).
/// If all sampled cells for a column are empty, the column defaults to STRING.
///
/// Type detection priority: BOOLEAN > INTEGER > FLOAT > TIMESTAMP > STRING.
pub fn infer_schema_from_csv(path: &Path) -> Result<Vec<ColumnDef>, SeedError> {
    infer_schema_from_csv_with_overrides(path, &std::collections::HashMap::new())
}

/// Infer column types with explicit overrides from a sidecar config.
///
/// Columns listed in `overrides` skip inference and use the provided SQL type.
pub fn infer_schema_from_csv_with_overrides(
    path: &Path,
    overrides: &std::collections::HashMap<String, String>,
) -> Result<Vec<ColumnDef>, SeedError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(path)
        .map_err(|e| SeedError::CsvParse {
            path: path.display().to_string(),
            source: e,
        })?;

    let headers: Vec<String> = reader
        .headers()
        .map_err(|e| SeedError::CsvParse {
            path: path.display().to_string(),
            source: e,
        })?
        .iter()
        .map(|h| h.trim().to_string())
        .collect();

    if headers.is_empty() {
        return Err(SeedError::NoHeader {
            path: path.display().to_string(),
        });
    }

    // Track the widest (least restrictive) type seen per column.
    let mut inferred: Vec<Option<InferredType>> = vec![None; headers.len()];

    for (rows_sampled, result) in reader.records().enumerate() {
        if rows_sampled >= INFER_SAMPLE_ROWS {
            break;
        }
        let record = result.map_err(|e| SeedError::CsvParse {
            path: path.display().to_string(),
            source: e,
        })?;

        for (i, field) in record.iter().enumerate() {
            if i >= headers.len() {
                break;
            }
            // Check if this column has an explicit override — skip inference.
            if overrides.contains_key(&headers[i]) {
                continue;
            }
            let trimmed = field.trim();
            if trimmed.is_empty() {
                continue; // NULL / empty — don't narrow the type
            }
            let detected = detect_type(trimmed);
            inferred[i] = Some(match inferred[i] {
                None => detected,
                Some(prev) => widen(prev, detected),
            });
        }
    }

    let columns = headers
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            let data_type = if let Some(override_type) = overrides.get(&name) {
                override_type.clone()
            } else {
                inferred[i]
                    .unwrap_or(InferredType::String)
                    .as_sql_type()
                    .to_string()
            };
            ColumnDef { name, data_type }
        })
        .collect();

    Ok(columns)
}

/// Detect the most specific type for a single non-empty cell value.
fn detect_type(value: &str) -> InferredType {
    // Boolean
    let lower = value.to_lowercase();
    if lower == "true" || lower == "false" {
        return InferredType::Boolean;
    }

    // Integer (may have leading sign)
    if value.parse::<i64>().is_ok() {
        return InferredType::Integer;
    }

    // Float (decimal point or scientific notation)
    if value.parse::<f64>().is_ok() {
        return InferredType::Float;
    }

    // Timestamp — common ISO 8601 patterns
    if is_timestamp_like(value) {
        return InferredType::Timestamp;
    }

    InferredType::String
}

/// Heuristic check for timestamp-like strings.
///
/// Matches patterns like:
/// - `2024-01-15`
/// - `2024-01-15T10:30:00`
/// - `2024-01-15 10:30:00`
/// - `2024-01-15T10:30:00Z`
/// - `2024-01-15T10:30:00+00:00`
fn is_timestamp_like(value: &str) -> bool {
    // Must start with a 4-digit year
    if value.len() < 10 {
        return false;
    }
    let bytes = value.as_bytes();
    // YYYY-MM-DD
    bytes[0..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

/// Widen two types to their common supertype.
///
/// The lattice is: BOOLEAN < INTEGER < FLOAT < TIMESTAMP -> STRING.
/// Cross-family jumps (e.g., INTEGER + TIMESTAMP) widen to STRING.
fn widen(a: InferredType, b: InferredType) -> InferredType {
    use InferredType::*;
    if a == b {
        return a;
    }
    match (a, b) {
        // Numeric widening
        (Integer, Float) | (Float, Integer) => Float,
        // Everything else widens to STRING
        _ => String,
    }
}

// ---------------------------------------------------------------------------
// Seed discovery
// ---------------------------------------------------------------------------

/// Walk a `seeds/` directory and discover all `.csv` (and `.tsv`) files.
///
/// For each CSV file, looks for an optional `.toml` sidecar with the same
/// stem (e.g., `dim_date.toml` for `dim_date.csv`). Non-recursive — only
/// top-level files are discovered.
pub fn discover_seeds(dir: &Path) -> Result<Vec<SeedFile>, SeedError> {
    if !dir.is_dir() {
        return Err(SeedError::DirNotFound(dir.display().to_string()));
    }

    let mut seeds = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .map_err(|e| SeedError::ReadFile {
            path: dir.display().to_string(),
            source: e,
        })?
        .filter_map(std::result::Result::ok)
        .collect();

    // Sort for deterministic ordering.
    entries.sort_by_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let path = entry.path();
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_lowercase();

        let format = match ext.as_str() {
            "csv" => SeedFormat::Csv,
            "tsv" => SeedFormat::Tsv,
            _ => continue,
        };

        let stem = match path.file_stem().and_then(|s| s.to_str()) {
            Some(s) => s.to_string(),
            None => continue,
        };

        // Look for optional sidecar TOML.
        let sidecar_path = dir.join(format!("{stem}.toml"));
        let config = if sidecar_path.is_file() {
            let content =
                std::fs::read_to_string(&sidecar_path).map_err(|e| SeedError::ReadFile {
                    path: sidecar_path.display().to_string(),
                    source: e,
                })?;
            toml::from_str::<SeedConfig>(&content).map_err(|e| SeedError::SidecarParse {
                path: sidecar_path.display().to_string(),
                source: e,
            })?
        } else {
            SeedConfig {
                name: None,
                target: None,
                strategy: SeedStrategy::default(),
                column_types: std::collections::HashMap::new(),
            }
        };

        let name = config.name.clone().unwrap_or_else(|| stem.clone());

        seeds.push(SeedFile {
            name,
            file_path: path,
            format,
            config,
        });
    }

    Ok(seeds)
}

// ---------------------------------------------------------------------------
// SQL generation
// ---------------------------------------------------------------------------

/// Default batch size for INSERT INTO VALUES statements.
const DEFAULT_BATCH_SIZE: usize = 500;

/// Generate a CREATE TABLE statement for a seed.
///
/// Uses `DROP TABLE IF EXISTS` + `CREATE TABLE` when the strategy is
/// `DropAndRecreate` (the only strategy in the foundation).
pub fn generate_create_table_sql(
    seed: &SeedFile,
    schema: &[ColumnDef],
    dialect: &dyn SqlDialect,
    catalog: &str,
    target_schema: &str,
) -> Result<Vec<String>, SeedError> {
    let table_name = seed
        .config
        .target
        .as_ref()
        .and_then(|t| t.table.as_deref())
        .unwrap_or(&seed.name);

    let table_ref = dialect
        .format_table_ref(catalog, target_schema, table_name)
        .map_err(|e| SeedError::ReadFile {
            path: seed.file_path.display().to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;

    let col_defs: Vec<String> = schema
        .iter()
        .map(|c| format!("  {} {}", c.name, c.data_type))
        .collect();

    let mut statements = Vec::new();

    match seed.config.strategy {
        SeedStrategy::DropAndRecreate => {
            statements.push(dialect.drop_table_sql(&table_ref));
            statements.push(format!(
                "CREATE TABLE {} (\n{}\n)",
                table_ref,
                col_defs.join(",\n")
            ));
        }
    }

    Ok(statements)
}

/// Read all rows from a CSV and generate batched INSERT INTO VALUES statements.
///
/// Returns a vector of SQL strings, each inserting up to `batch_size` rows.
pub fn generate_insert_sql(
    seed: &SeedFile,
    dialect: &dyn SqlDialect,
    catalog: &str,
    target_schema: &str,
    schema: &[ColumnDef],
    batch_size: Option<usize>,
) -> Result<Vec<String>, SeedError> {
    let table_name = seed
        .config
        .target
        .as_ref()
        .and_then(|t| t.table.as_deref())
        .unwrap_or(&seed.name);

    let table_ref = dialect
        .format_table_ref(catalog, target_schema, table_name)
        .map_err(|e| SeedError::ReadFile {
            path: seed.file_path.display().to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;

    let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);

    // Read all rows from CSV.
    let delimiter = match seed.format {
        SeedFormat::Csv => b',',
        SeedFormat::Tsv => b'\t',
    };

    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .from_path(&seed.file_path)
        .map_err(|e| SeedError::CsvParse {
            path: seed.file_path.display().to_string(),
            source: e,
        })?;

    let col_names: Vec<&str> = schema.iter().map(|c| c.name.as_str()).collect();
    let col_list = col_names.join(", ");

    let mut statements = Vec::new();
    let mut batch_values: Vec<String> = Vec::with_capacity(batch_size);

    for result in reader.records() {
        let record = result.map_err(|e| SeedError::CsvParse {
            path: seed.file_path.display().to_string(),
            source: e,
        })?;

        let row_values: Vec<String> = record
            .iter()
            .enumerate()
            .map(|(i, field)| {
                let trimmed = field.trim();
                if trimmed.is_empty() {
                    return "NULL".to_string();
                }
                let col_type = schema
                    .get(i)
                    .map(|c| c.data_type.as_str())
                    .unwrap_or("STRING");
                format_value(trimmed, col_type)
            })
            .collect();

        batch_values.push(format!("({})", row_values.join(", ")));

        if batch_values.len() >= batch_size {
            let sql = format!(
                "INSERT INTO {} ({}) VALUES\n{}",
                table_ref,
                col_list,
                batch_values.join(",\n")
            );
            statements.push(sql);
            batch_values.clear();
        }
    }

    // Flush remaining rows.
    if !batch_values.is_empty() {
        let sql = format!(
            "INSERT INTO {} ({}) VALUES\n{}",
            table_ref,
            col_list,
            batch_values.join(",\n")
        );
        statements.push(sql);
    }

    Ok(statements)
}

/// Format a cell value for a SQL VALUES clause.
///
/// Numeric and boolean types are emitted bare; everything else is
/// single-quoted with internal quotes escaped.
fn format_value(value: &str, col_type: &str) -> String {
    let upper = col_type.to_uppercase();
    if upper == "BIGINT"
        || upper == "INTEGER"
        || upper == "INT"
        || upper == "DOUBLE"
        || upper == "FLOAT"
    {
        // Numeric — emit bare (validated by inference or override).
        // Guard against injection: if it doesn't parse as a number, quote it.
        if value.parse::<f64>().is_ok() {
            return value.to_string();
        }
    }
    if upper == "BOOLEAN" {
        let lower = value.to_lowercase();
        if lower == "true" || lower == "false" {
            return lower;
        }
    }
    // Default: single-quoted string with escaped quotes.
    format!("'{}'", value.replace('\'', "''"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Helper to create a temp CSV and return its path.
    fn write_temp_csv(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut f = std::fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    // -- Type detection -------------------------------------------------------

    #[test]
    fn detect_type_boolean() {
        assert_eq!(detect_type("true"), InferredType::Boolean);
        assert_eq!(detect_type("false"), InferredType::Boolean);
        assert_eq!(detect_type("True"), InferredType::Boolean);
        assert_eq!(detect_type("FALSE"), InferredType::Boolean);
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
        assert_eq!(detect_type("-0.5"), InferredType::Float);
        assert_eq!(detect_type("1e10"), InferredType::Float);
    }

    #[test]
    fn detect_type_timestamp() {
        assert_eq!(detect_type("2024-01-15"), InferredType::Timestamp);
        assert_eq!(detect_type("2024-01-15T10:30:00"), InferredType::Timestamp);
        assert_eq!(detect_type("2024-01-15 10:30:00"), InferredType::Timestamp);
    }

    #[test]
    fn detect_type_string() {
        assert_eq!(detect_type("hello"), InferredType::String);
        assert_eq!(detect_type("US"), InferredType::String);
        assert_eq!(detect_type("123-abc"), InferredType::String);
    }

    // -- Type widening --------------------------------------------------------

    #[test]
    fn widen_same_type() {
        assert_eq!(
            widen(InferredType::Integer, InferredType::Integer),
            InferredType::Integer
        );
    }

    #[test]
    fn widen_int_to_float() {
        assert_eq!(
            widen(InferredType::Integer, InferredType::Float),
            InferredType::Float
        );
        assert_eq!(
            widen(InferredType::Float, InferredType::Integer),
            InferredType::Float
        );
    }

    #[test]
    fn widen_cross_family_to_string() {
        assert_eq!(
            widen(InferredType::Integer, InferredType::Timestamp),
            InferredType::String
        );
        assert_eq!(
            widen(InferredType::Boolean, InferredType::Float),
            InferredType::String
        );
    }

    // -- Schema inference -----------------------------------------------------

    #[test]
    fn infer_schema_basic_types() {
        let dir = tempfile::tempdir().unwrap();
        let csv_content = "id,name,amount,active,created_at\n\
                           1,Alice,29.99,true,2024-01-15\n\
                           2,Bob,49.50,false,2024-02-20\n\
                           3,Carol,15.00,true,2024-03-10\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv_content);

        let schema = infer_schema_from_csv(&path).unwrap();

        assert_eq!(schema.len(), 5);
        assert_eq!(
            schema[0],
            ColumnDef {
                name: "id".into(),
                data_type: "BIGINT".into()
            }
        );
        assert_eq!(
            schema[1],
            ColumnDef {
                name: "name".into(),
                data_type: "STRING".into()
            }
        );
        assert_eq!(
            schema[2],
            ColumnDef {
                name: "amount".into(),
                data_type: "DOUBLE".into()
            }
        );
        assert_eq!(
            schema[3],
            ColumnDef {
                name: "active".into(),
                data_type: "BOOLEAN".into()
            }
        );
        assert_eq!(
            schema[4],
            ColumnDef {
                name: "created_at".into(),
                data_type: "TIMESTAMP".into()
            }
        );
    }

    #[test]
    fn infer_schema_with_nulls() {
        let dir = tempfile::tempdir().unwrap();
        let csv_content = "id,value\n\
                           1,\n\
                           2,42\n\
                           3,\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv_content);

        let schema = infer_schema_from_csv(&path).unwrap();

        assert_eq!(schema[0].data_type, "BIGINT");
        // value column: only non-empty cell is "42" -> INTEGER
        assert_eq!(schema[1].data_type, "BIGINT");
    }

    #[test]
    fn infer_schema_all_empty_defaults_to_string() {
        let dir = tempfile::tempdir().unwrap();
        let csv_content = "col_a\n\n\n\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv_content);

        let schema = infer_schema_from_csv(&path).unwrap();
        assert_eq!(schema[0].data_type, "STRING");
    }

    #[test]
    fn infer_schema_with_overrides() {
        let dir = tempfile::tempdir().unwrap();
        let csv_content = "id,code\n1,US\n2,UK\n";
        let path = write_temp_csv(dir.path(), "test.csv", csv_content);

        let mut overrides = std::collections::HashMap::new();
        overrides.insert("id".into(), "INTEGER".into());

        let schema = infer_schema_from_csv_with_overrides(&path, &overrides).unwrap();
        assert_eq!(
            schema[0],
            ColumnDef {
                name: "id".into(),
                data_type: "INTEGER".into()
            }
        );
        assert_eq!(
            schema[1],
            ColumnDef {
                name: "code".into(),
                data_type: "STRING".into()
            }
        );
    }

    // -- Seed discovery -------------------------------------------------------

    #[test]
    fn discover_seeds_finds_csv_files() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_csv(
            dir.path(),
            "dim_date.csv",
            "date_key,date\n20240101,2024-01-01\n",
        );
        write_temp_csv(dir.path(), "countries.csv", "code,name\nUS,United States\n");
        // Non-CSV file should be ignored.
        std::fs::write(dir.path().join("readme.txt"), "ignore me").unwrap();

        let seeds = discover_seeds(dir.path()).unwrap();

        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].name, "countries");
        assert_eq!(seeds[1].name, "dim_date");
    }

    #[test]
    fn discover_seeds_loads_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_csv(dir.path(), "codes.csv", "id,code\n1,US\n");
        std::fs::write(
            dir.path().join("codes.toml"),
            r#"
name = "country_codes"
strategy = "drop_and_recreate"

[target]
schema = "reference"

[column_types]
id = "INTEGER"
"#,
        )
        .unwrap();

        let seeds = discover_seeds(dir.path()).unwrap();

        assert_eq!(seeds.len(), 1);
        assert_eq!(seeds[0].name, "country_codes");
        assert_eq!(seeds[0].config.target.as_ref().unwrap().schema, "reference");
        assert_eq!(
            seeds[0].config.column_types.get("id").map(String::as_str),
            Some("INTEGER")
        );
    }

    #[test]
    fn discover_seeds_dir_not_found() {
        let result = discover_seeds(Path::new("/nonexistent/seeds"));
        assert!(result.is_err());
    }

    // -- SQL generation -------------------------------------------------------

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
            _keys: &[std::sync::Arc<str>],
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

    #[test]
    fn generate_create_table_sql_drop_and_recreate() {
        let seed = SeedFile {
            name: "dim_date".into(),
            file_path: PathBuf::from("seeds/dim_date.csv"),
            format: SeedFormat::Csv,
            config: SeedConfig {
                name: None,
                target: None,
                strategy: SeedStrategy::DropAndRecreate,
                column_types: std::collections::HashMap::new(),
            },
        };
        let schema = vec![
            ColumnDef {
                name: "date_key".into(),
                data_type: "BIGINT".into(),
            },
            ColumnDef {
                name: "date".into(),
                data_type: "TIMESTAMP".into(),
            },
        ];

        let stmts =
            generate_create_table_sql(&seed, &schema, &TestDialect, "my_catalog", "seeds").unwrap();

        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("DROP TABLE IF EXISTS my_catalog.seeds.dim_date"));
        assert!(stmts[1].contains("CREATE TABLE my_catalog.seeds.dim_date"));
        assert!(stmts[1].contains("date_key BIGINT"));
        assert!(stmts[1].contains("date TIMESTAMP"));
    }

    #[test]
    fn generate_insert_sql_batching() {
        let dir = tempfile::tempdir().unwrap();
        // Write a CSV with 3 rows.
        let csv_content = "id,name\n1,Alice\n2,Bob\n3,Carol\n";
        let csv_path = write_temp_csv(dir.path(), "users.csv", csv_content);

        let seed = SeedFile {
            name: "users".into(),
            file_path: csv_path,
            format: SeedFormat::Csv,
            config: SeedConfig {
                name: None,
                target: None,
                strategy: SeedStrategy::DropAndRecreate,
                column_types: std::collections::HashMap::new(),
            },
        };
        let schema = vec![
            ColumnDef {
                name: "id".into(),
                data_type: "BIGINT".into(),
            },
            ColumnDef {
                name: "name".into(),
                data_type: "STRING".into(),
            },
        ];

        // Batch size of 2 -> two INSERT statements.
        let stmts =
            generate_insert_sql(&seed, &TestDialect, "cat", "sch", &schema, Some(2)).unwrap();

        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("INSERT INTO cat.sch.users"));
        assert!(stmts[0].contains("(1, 'Alice')"));
        assert!(stmts[0].contains("(2, 'Bob')"));
        assert!(stmts[1].contains("(3, 'Carol')"));
    }

    #[test]
    fn generate_insert_sql_handles_nulls_and_quotes() {
        let dir = tempfile::tempdir().unwrap();
        let csv_content = "id,note\n1,it's fine\n2,\n";
        let csv_path = write_temp_csv(dir.path(), "notes.csv", csv_content);

        let seed = SeedFile {
            name: "notes".into(),
            file_path: csv_path,
            format: SeedFormat::Csv,
            config: SeedConfig {
                name: None,
                target: None,
                strategy: SeedStrategy::DropAndRecreate,
                column_types: std::collections::HashMap::new(),
            },
        };
        let schema = vec![
            ColumnDef {
                name: "id".into(),
                data_type: "BIGINT".into(),
            },
            ColumnDef {
                name: "note".into(),
                data_type: "STRING".into(),
            },
        ];

        let stmts = generate_insert_sql(&seed, &TestDialect, "c", "s", &schema, None).unwrap();

        assert_eq!(stmts.len(), 1);
        // Single quotes are escaped.
        assert!(stmts[0].contains("'it''s fine'"));
        // Empty field -> NULL.
        assert!(stmts[0].contains("NULL"));
    }

    #[test]
    fn format_value_numerics_bare() {
        assert_eq!(format_value("42", "BIGINT"), "42");
        assert_eq!(format_value("3.14", "DOUBLE"), "3.14");
        assert_eq!(format_value("-1", "INTEGER"), "-1");
    }

    #[test]
    fn format_value_boolean() {
        assert_eq!(format_value("true", "BOOLEAN"), "true");
        assert_eq!(format_value("False", "BOOLEAN"), "false");
    }

    #[test]
    fn format_value_string_escaping() {
        assert_eq!(format_value("hello", "STRING"), "'hello'");
        assert_eq!(format_value("it's", "STRING"), "'it''s'");
    }

    #[test]
    fn format_value_non_numeric_in_numeric_col_is_quoted() {
        // If a "numeric" column has a non-parseable value, quote it safely.
        assert_eq!(format_value("abc", "BIGINT"), "'abc'");
    }
}
