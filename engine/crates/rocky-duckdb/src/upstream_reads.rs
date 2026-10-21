//! What one DuckDB statement read from each upstream table.
//!
//! The statement runs with the JSON profiler on. Each `TABLE_SCAN` node of
//! the profile names the table (`extra_info.Table`, as
//! `catalog.schema.table`), the rows it scanned (`operator_rows_scanned`)
//! and the columns it read (`extra_info.Projections`). Those become one
//! [`UpstreamRead`] per table.
//!
//! ```text
//!   statement ──▶ profile JSON ──▶ TABLE_SCAN nodes
//!                                    ├─ has Table ─▶ UpstreamRead (summed per table)
//!                                    └─ no Table  ─▶ unattributed_scans += 1
//!                                       (a file, or a function such as range)
//! ```
//!
//! Any failure to profile or to read the profile gives `None` (not
//! measured). It never fails the statement and never yields zeros.

use std::collections::BTreeMap;

use rocky_core::state::{UpstreamRead, UpstreamReads};
use serde_json::Value;
use tracing::debug;

use crate::{DuckDbConnector, DuckDbError};

/// One `TABLE_SCAN` node, before the column types are known.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawScan {
    /// `catalog.schema.table`.
    table: String,
    rows_scanned: u64,
    projections: Vec<String>,
}

/// The scans of one profile.
#[derive(Debug, Default, PartialEq, Eq)]
struct RawScans {
    scans: Vec<RawScan>,
    unattributed: u64,
}

/// Collect the `TABLE_SCAN` nodes of a profile tree.
///
/// `None` when the profile has no `operator_type` anywhere, so it is not a
/// profile of the shape this reads (the caller records "not measured").
fn parse_profile(root: &Value) -> Option<RawScans> {
    fn walk(node: &Value, out: &mut RawScans, saw_operator: &mut bool) {
        if node.get("operator_type").is_some() {
            *saw_operator = true;
        }
        if node.get("operator_type").and_then(Value::as_str) == Some("TABLE_SCAN") {
            let table = node
                .get("extra_info")
                .and_then(|e| e.get("Table"))
                .and_then(Value::as_str)
                .filter(|t| !t.is_empty());
            let rows = node.get("operator_rows_scanned").and_then(Value::as_u64);
            match (table, rows) {
                (Some(table), Some(rows_scanned)) => out.scans.push(RawScan {
                    table: table.to_string(),
                    rows_scanned,
                    projections: projections(node),
                }),
                // A file or a table function names no table. A table scan
                // with no row count cannot be summed: count it as
                // unattributed rather than drop it silently.
                (None, _) | (Some(_), None) => out.unattributed += 1,
            }
        }
        if let Some(children) = node.get("children").and_then(Value::as_array) {
            for child in children {
                walk(child, out, saw_operator);
            }
        }
    }
    let mut out = RawScans::default();
    let mut saw_operator = false;
    walk(root, &mut out, &mut saw_operator);
    saw_operator.then_some(out)
}

/// The columns a scan read. DuckDB writes `Projections` as an array for two
/// or more columns, a bare string for one, and an empty string for none
/// (`count(*)`).
fn projections(node: &Value) -> Vec<String> {
    match node.get("extra_info").and_then(|e| e.get("Projections")) {
        Some(Value::Array(items)) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        Some(Value::String(one)) if !one.is_empty() => vec![one.clone()],
        Some(_) | None => Vec::new(),
    }
}

/// The in-memory width in bytes of a fixed-width DuckDB type, from the
/// `data_type` text of `duckdb_columns()`. `None` for variable-width types
/// (text, blob, list, struct, map, enum, bit, union) and anything unknown.
fn fixed_width(data_type: &str) -> Option<u64> {
    let t = data_type.trim().to_ascii_uppercase();
    if let Some(args) = t.strip_prefix("DECIMAL(") {
        let precision: u32 = args.split(',').next()?.trim().parse().ok()?;
        return Some(match precision {
            0..=4 => 2,
            5..=9 => 4,
            10..=18 => 8,
            _ => 16,
        });
    }
    match t.as_str() {
        "BOOLEAN" | "TINYINT" | "UTINYINT" => Some(1),
        "SMALLINT" | "USMALLINT" => Some(2),
        "INTEGER" | "UINTEGER" | "FLOAT" | "DATE" => Some(4),
        "BIGINT"
        | "UBIGINT"
        | "DOUBLE"
        | "TIME"
        | "TIMESTAMP"
        | "TIMESTAMP_S"
        | "TIMESTAMP_MS"
        | "TIMESTAMP_NS"
        | "TIMESTAMP WITH TIME ZONE"
        | "TIME WITH TIME ZONE" => Some(8),
        "HUGEINT" | "UHUGEINT" | "UUID" | "INTERVAL" => Some(16),
        _ => None,
    }
}

/// `rows_scanned` times the summed width of `projections`, or `None` when
/// there are no projections or any one is missing from `types` or is not
/// fixed-width.
fn estimate_bytes(
    rows_scanned: u64,
    projections: &[String],
    types: &BTreeMap<String, String>,
) -> Option<u64> {
    if projections.is_empty() {
        return None;
    }
    let mut width = 0_u64;
    for column in projections {
        let data_type = types.get(&column.to_ascii_lowercase())?;
        width = width.checked_add(fixed_width(data_type)?)?;
    }
    Some(rows_scanned.saturating_mul(width))
}

/// Turn raw scans into per-table reads, summing the scans of one table.
/// `types_of` gives a table's column types (lower-cased name to type text).
fn summarize(
    raw: RawScans,
    mut types_of: impl FnMut(&str) -> BTreeMap<String, String>,
) -> UpstreamReads {
    let mut reads = UpstreamReads {
        reads: Vec::new(),
        unattributed_scans: raw.unattributed,
    };
    let mut cache: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    for scan in raw.scans {
        let types = cache
            .entry(scan.table.clone())
            .or_insert_with(|| types_of(&scan.table));
        let est_bytes = estimate_bytes(scan.rows_scanned, &scan.projections, types);
        reads.merge(UpstreamReads {
            reads: vec![UpstreamRead {
                table: scan.table,
                rows_scanned: scan.rows_scanned,
                projected_columns: scan.projections,
                est_bytes,
            }],
            unattributed_scans: 0,
        });
    }
    reads
}

/// Escape a path for a single-quoted SQL string.
fn sql_quote(text: &str) -> String {
    text.replace('\'', "''")
}

/// True for a statement the profile can describe on its own: one statement.
/// A batch leaves only its last query in the profile output, so a batch is
/// not profiled.
fn is_single_statement(sql: &str) -> bool {
    let trimmed = sql.trim().trim_end_matches(';');
    !trimmed.contains(';')
}

impl DuckDbConnector {
    /// Run `sql` and report what it read from each upstream table.
    ///
    /// Returns `Err` only when the statement itself fails. The `Option` is
    /// `None` when the statement was a batch, the profiler could not be
    /// started, or the profile could not be read: not measured.
    pub fn execute_statement_with_upstream_reads(
        &self,
        sql: &str,
    ) -> Result<Option<UpstreamReads>, DuckDbError> {
        if !is_single_statement(sql) {
            self.execute_statement(sql)?;
            return Ok(None);
        }
        // DuckDB picks the profile format from the file extension.
        let file = match tempfile::Builder::new().suffix(".json").tempfile() {
            Ok(file) => file,
            Err(e) => {
                debug!(error = %e, "no temp file for the profile; upstream reads not measured");
                self.execute_statement(sql)?;
                return Ok(None);
            }
        };
        let enable = format!(
            "PRAGMA enable_profiling = 'json'; PRAGMA profiling_output = '{}';",
            sql_quote(&file.path().to_string_lossy())
        );
        if let Err(e) = self.conn.execute_batch(&enable) {
            debug!(error = %e, "could not enable profiling; upstream reads not measured");
            // Whatever half-enabled, turn it off before the statement runs.
            let _ = self.conn.execute_batch("PRAGMA disable_profiling;");
            self.execute_statement(sql)?;
            return Ok(None);
        }
        let result = self.execute_statement(sql);
        let disabled = self.conn.execute_batch("PRAGMA disable_profiling;");
        result?;
        if let Err(e) = disabled {
            debug!(error = %e, "could not disable profiling; upstream reads not measured");
            return Ok(None);
        }
        Ok(self.read_profile(file.path()))
    }

    fn read_profile(&self, path: &std::path::Path) -> Option<UpstreamReads> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| debug!(error = %e, "could not read the profile"))
            .ok()?;
        let root: Value = serde_json::from_str(&text)
            .map_err(|e| debug!(error = %e, "the profile is not JSON"))
            .ok()?;
        let raw = parse_profile(&root)?;
        Some(summarize(raw, |table| self.column_types(table)))
    }

    /// Column types of `catalog.schema.table`, lower-cased name to type text.
    /// Empty when the table cannot be read (dropped by the statement, a
    /// temp object): the estimate is then `None`.
    fn column_types(&self, qualified: &str) -> BTreeMap<String, String> {
        let mut parts = qualified.splitn(3, '.');
        let (Some(catalog), Some(schema), Some(table)) = (parts.next(), parts.next(), parts.next())
        else {
            return BTreeMap::new();
        };
        let query = || -> Result<BTreeMap<String, String>, duckdb::Error> {
            let mut stmt = self.conn.prepare(
                "SELECT column_name, data_type FROM duckdb_columns() \
                 WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            )?;
            let rows = stmt.query_map(duckdb::params![catalog, schema, table], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            let mut types = BTreeMap::new();
            for row in rows {
                let (name, data_type) = row?;
                types.insert(name.to_ascii_lowercase(), data_type);
            }
            Ok(types)
        };
        query().unwrap_or_else(|e| {
            debug!(table = qualified, error = %e, "could not read the column types");
            BTreeMap::new()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn scan_node(table: Option<&str>, rows: u64, projections: Value) -> Value {
        let mut extra = serde_json::Map::new();
        if let Some(table) = table {
            extra.insert("Table".into(), json!(table));
        } else {
            extra.insert("Function".into(), json!("READ_PARQUET"));
        }
        extra.insert("Projections".into(), projections);
        json!({
            "operator_type": "TABLE_SCAN",
            "operator_rows_scanned": rows,
            "extra_info": extra,
            "children": [],
        })
    }

    fn types(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
            .collect()
    }

    #[test]
    fn projections_accept_array_string_and_empty() {
        let node = |p: Value| scan_node(Some("m.main.t"), 1, p);
        assert_eq!(projections(&node(json!(["a", "b"]))), ["a", "b"]);
        assert_eq!(projections(&node(json!("a"))), ["a"]);
        assert!(projections(&node(json!(""))).is_empty());
    }

    #[test]
    fn a_scan_without_a_table_is_unattributed() {
        let root = json!({
            "operator_type": "EXPLAIN",
            "children": [
                scan_node(None, 10, json!("a")),
                scan_node(Some("m.main.t"), 5, json!("a")),
            ],
        });
        let raw = parse_profile(&root).unwrap();
        assert_eq!(raw.unattributed, 1);
        assert_eq!(raw.scans.len(), 1);
    }

    #[test]
    fn a_json_without_operators_is_not_a_profile() {
        assert_eq!(parse_profile(&json!({"x": 1})), None);
    }

    #[test]
    fn widths_cover_fixed_types_only() {
        assert_eq!(fixed_width("BIGINT"), Some(8));
        assert_eq!(fixed_width("integer"), Some(4));
        assert_eq!(fixed_width("DECIMAL(18,3)"), Some(8));
        assert_eq!(fixed_width("DECIMAL(38,0)"), Some(16));
        assert_eq!(fixed_width("VARCHAR"), None);
        assert_eq!(fixed_width("BIGINT[]"), None);
        assert_eq!(fixed_width("STRUCT(a INTEGER)"), None);
    }

    #[test]
    fn the_estimate_is_rows_times_projected_width() {
        let t = types(&[("a", "BIGINT"), ("b", "INTEGER"), ("s", "VARCHAR")]);
        let cols = |c: &[&str]| c.iter().map(|s| (*s).to_string()).collect::<Vec<_>>();
        assert_eq!(estimate_bytes(100, &cols(&["a", "b"]), &t), Some(1200));
        assert_eq!(estimate_bytes(100, &cols(&["a", "s"]), &t), None);
        assert_eq!(estimate_bytes(100, &cols(&["missing"]), &t), None);
        assert_eq!(estimate_bytes(100, &[], &t), None);
    }

    #[test]
    fn scans_of_one_table_sum() {
        let raw = RawScans {
            scans: vec![
                RawScan {
                    table: "m.main.t".into(),
                    rows_scanned: 10,
                    projections: vec!["a".into()],
                },
                RawScan {
                    table: "m.main.t".into(),
                    rows_scanned: 10,
                    projections: vec!["a".into(), "b".into()],
                },
            ],
            unattributed: 0,
        };
        let reads = summarize(raw, |_| types(&[("a", "BIGINT"), ("b", "BIGINT")]));
        assert_eq!(reads.reads.len(), 1);
        assert_eq!(reads.reads[0].rows_scanned, 20);
        assert_eq!(reads.reads[0].projected_columns, ["a", "b"]);
        assert_eq!(reads.reads[0].est_bytes, Some(10 * 8 + 10 * 16));
    }

    #[test]
    fn a_batch_is_not_profiled() {
        assert!(is_single_statement("CREATE TABLE t AS SELECT 1;"));
        assert!(!is_single_statement("SELECT 1; SELECT 2"));
    }
}
