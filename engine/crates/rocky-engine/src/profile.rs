//! Statistical profiling for models.
//!
//! Generates column-level statistics (null rates, cardinality, min/max, etc.)
//! and diffs profiles between runs to detect data quality regressions.

use serde::{Deserialize, Serialize};

/// Statistical profile for a single column.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnProfile {
    pub column_name: String,
    pub data_type: String,
    pub row_count: u64,
    pub null_count: u64,
    pub null_rate: f64,
    pub distinct_count: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mean: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stddev: Option<f64>,
}

/// Statistical profile for an entire model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelProfile {
    pub model_name: String,
    pub row_count: u64,
    pub columns: Vec<ColumnProfile>,
    pub profiled_at: chrono::DateTime<chrono::Utc>,
}

/// Severity of a profile diff finding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffSeverity {
    /// Informational change, no action needed.
    Info,
    /// Potential issue worth investigating.
    Warning,
    /// Likely data quality regression requiring attention.
    Critical,
}

impl std::fmt::Display for DiffSeverity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiffSeverity::Info => write!(f, "INFO"),
            DiffSeverity::Warning => write!(f, "WARNING"),
            DiffSeverity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// A single difference found between two profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileDiffEntry {
    pub column: String,
    pub metric: String,
    pub old_value: String,
    pub new_value: String,
    pub severity: DiffSeverity,
    pub message: String,
}

/// Full diff between two model profiles.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProfileDiff {
    pub model_name: String,
    pub entries: Vec<ProfileDiffEntry>,
    pub has_critical: bool,
    pub has_warning: bool,
}

/// Compares two model profiles and returns differences with severity levels.
///
/// Severity rules:
/// - **Critical**: null rate increased by 10+ percentage points, row count dropped 50%+
/// - **Warning**: null rate increased by 1+ percentage points, cardinality changed 20%+
/// - **Info**: any other notable change (new column, removed column)
pub fn diff_profiles(old: &ModelProfile, new: &ModelProfile) -> ProfileDiff {
    let mut entries = Vec::new();

    // Row count change
    if old.row_count > 0 {
        let pct_change =
            (new.row_count as f64 - old.row_count as f64) / old.row_count as f64 * 100.0;
        if pct_change <= -50.0 {
            entries.push(ProfileDiffEntry {
                column: "_table_".to_string(),
                metric: "row_count".to_string(),
                old_value: old.row_count.to_string(),
                new_value: new.row_count.to_string(),
                severity: DiffSeverity::Critical,
                message: format!("row count dropped {:.1}%", pct_change.abs()),
            });
        } else if pct_change.abs() > 10.0 {
            entries.push(ProfileDiffEntry {
                column: "_table_".to_string(),
                metric: "row_count".to_string(),
                old_value: old.row_count.to_string(),
                new_value: new.row_count.to_string(),
                severity: DiffSeverity::Warning,
                message: format!("row count changed {pct_change:+.1}%"),
            });
        }
    }

    // Build lookup for old columns
    let old_cols: std::collections::HashMap<&str, &ColumnProfile> = old
        .columns
        .iter()
        .map(|c| (c.column_name.as_str(), c))
        .collect();
    let new_cols: std::collections::HashMap<&str, &ColumnProfile> = new
        .columns
        .iter()
        .map(|c| (c.column_name.as_str(), c))
        .collect();

    // Check for removed columns
    for name in old_cols.keys() {
        if !new_cols.contains_key(name) {
            entries.push(ProfileDiffEntry {
                column: name.to_string(),
                metric: "existence".to_string(),
                old_value: "present".to_string(),
                new_value: "removed".to_string(),
                severity: DiffSeverity::Warning,
                message: format!("column `{name}` was removed"),
            });
        }
    }

    // Check for new columns
    for name in new_cols.keys() {
        if !old_cols.contains_key(name) {
            entries.push(ProfileDiffEntry {
                column: name.to_string(),
                metric: "existence".to_string(),
                old_value: "absent".to_string(),
                new_value: "present".to_string(),
                severity: DiffSeverity::Info,
                message: format!("column `{name}` was added"),
            });
        }
    }

    // Compare matching columns
    for (name, new_col) in &new_cols {
        let Some(old_col) = old_cols.get(name) else {
            continue;
        };

        // Null rate change
        let null_rate_delta = new_col.null_rate - old_col.null_rate;
        if null_rate_delta >= 0.10 {
            entries.push(ProfileDiffEntry {
                column: name.to_string(),
                metric: "null_rate".to_string(),
                old_value: format!("{:.4}", old_col.null_rate),
                new_value: format!("{:.4}", new_col.null_rate),
                severity: DiffSeverity::Critical,
                message: format!(
                    "null rate increased by {:.1} percentage points",
                    null_rate_delta * 100.0
                ),
            });
        } else if null_rate_delta >= 0.01 {
            entries.push(ProfileDiffEntry {
                column: name.to_string(),
                metric: "null_rate".to_string(),
                old_value: format!("{:.4}", old_col.null_rate),
                new_value: format!("{:.4}", new_col.null_rate),
                severity: DiffSeverity::Warning,
                message: format!(
                    "null rate increased by {:.1} percentage points",
                    null_rate_delta * 100.0
                ),
            });
        }

        // Cardinality change
        if old_col.distinct_count > 0 {
            let card_pct = ((new_col.distinct_count as f64 - old_col.distinct_count as f64)
                / old_col.distinct_count as f64
                * 100.0)
                .abs();
            if card_pct > 20.0 {
                entries.push(ProfileDiffEntry {
                    column: name.to_string(),
                    metric: "distinct_count".to_string(),
                    old_value: old_col.distinct_count.to_string(),
                    new_value: new_col.distinct_count.to_string(),
                    severity: DiffSeverity::Warning,
                    message: format!("cardinality changed by {card_pct:.1}%"),
                });
            }
        }
    }

    let has_critical = entries.iter().any(|e| e.severity == DiffSeverity::Critical);
    let has_warning = entries.iter().any(|e| e.severity == DiffSeverity::Warning);

    ProfileDiff {
        model_name: new.model_name.clone(),
        entries,
        has_critical,
        has_warning,
    }
}

/// Generates profiling SQL for DuckDB.
///
/// Produces a single query that computes column-level statistics for each
/// column in the given table.
pub fn generate_profile_sql(table_name: &str, columns: &[(String, String)]) -> String {
    if columns.is_empty() {
        return format!("SELECT COUNT(*) AS row_count FROM {table_name}");
    }

    let mut parts = Vec::new();
    parts.push("SELECT COUNT(*) AS _row_count".to_string());

    for (col_name, col_type) in columns {
        let is_numeric = matches!(
            col_type.to_uppercase().as_str(),
            "INT"
                | "INTEGER"
                | "BIGINT"
                | "SMALLINT"
                | "TINYINT"
                | "FLOAT"
                | "DOUBLE"
                | "DECIMAL"
                | "NUMERIC"
                | "REAL"
                | "HUGEINT"
        );

        parts.push(format!(
            ", COUNT(DISTINCT \"{col_name}\") AS \"{col_name}__distinct\""
        ));
        parts.push(format!(
            ", SUM(CASE WHEN \"{col_name}\" IS NULL THEN 1 ELSE 0 END) AS \"{col_name}__nulls\""
        ));
        parts.push(format!(
            ", MIN(CAST(\"{col_name}\" AS VARCHAR)) AS \"{col_name}__min\""
        ));
        parts.push(format!(
            ", MAX(CAST(\"{col_name}\" AS VARCHAR)) AS \"{col_name}__max\""
        ));

        if is_numeric {
            parts.push(format!(
                ", AVG(CAST(\"{col_name}\" AS DOUBLE)) AS \"{col_name}__mean\""
            ));
            parts.push(format!(
                ", STDDEV(CAST(\"{col_name}\" AS DOUBLE)) AS \"{col_name}__stddev\""
            ));
        }
    }

    format!("{}\nFROM {table_name}", parts.join(""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn make_profile(name: &str, row_count: u64, columns: Vec<ColumnProfile>) -> ModelProfile {
        ModelProfile {
            model_name: name.to_string(),
            row_count,
            columns,
            profiled_at: Utc::now(),
        }
    }

    fn make_col(name: &str, null_rate: f64, distinct_count: u64, row_count: u64) -> ColumnProfile {
        ColumnProfile {
            column_name: name.to_string(),
            data_type: "VARCHAR".to_string(),
            row_count,
            null_count: (row_count as f64 * null_rate) as u64,
            null_rate,
            distinct_count,
            min_value: None,
            max_value: None,
            mean: None,
            stddev: None,
        }
    }

    #[test]
    fn test_critical_null_rate_increase() {
        let old = make_profile("orders", 1000, vec![make_col("email", 0.05, 900, 1000)]);
        let new = make_profile("orders", 1000, vec![make_col("email", 0.20, 900, 1000)]);

        let diff = diff_profiles(&old, &new);
        assert!(diff.has_critical);
        let entry = diff
            .entries
            .iter()
            .find(|e| e.column == "email" && e.metric == "null_rate")
            .unwrap();
        assert_eq!(entry.severity, DiffSeverity::Critical);
    }

    #[test]
    fn test_warning_null_rate_increase() {
        let old = make_profile("orders", 1000, vec![make_col("email", 0.05, 900, 1000)]);
        let new = make_profile("orders", 1000, vec![make_col("email", 0.08, 900, 1000)]);

        let diff = diff_profiles(&old, &new);
        assert!(diff.has_warning);
        assert!(!diff.has_critical);
        let entry = diff
            .entries
            .iter()
            .find(|e| e.column == "email" && e.metric == "null_rate")
            .unwrap();
        assert_eq!(entry.severity, DiffSeverity::Warning);
    }

    #[test]
    fn test_critical_row_count_drop() {
        let old = make_profile("orders", 10000, vec![]);
        let new = make_profile("orders", 3000, vec![]);

        let diff = diff_profiles(&old, &new);
        assert!(diff.has_critical);
        let entry = diff
            .entries
            .iter()
            .find(|e| e.metric == "row_count")
            .unwrap();
        assert_eq!(entry.severity, DiffSeverity::Critical);
        assert!(entry.message.contains("dropped"));
    }

    #[test]
    fn test_warning_cardinality_change() {
        let old = make_profile("orders", 1000, vec![make_col("status", 0.0, 100, 1000)]);
        let new = make_profile("orders", 1000, vec![make_col("status", 0.0, 130, 1000)]);

        let diff = diff_profiles(&old, &new);
        assert!(diff.has_warning);
        let entry = diff
            .entries
            .iter()
            .find(|e| e.metric == "distinct_count")
            .unwrap();
        assert_eq!(entry.severity, DiffSeverity::Warning);
        assert!(entry.message.contains("cardinality"));
    }

    #[test]
    fn test_column_added() {
        let old = make_profile("orders", 1000, vec![make_col("id", 0.0, 1000, 1000)]);
        let new = make_profile(
            "orders",
            1000,
            vec![
                make_col("id", 0.0, 1000, 1000),
                make_col("new_col", 0.0, 500, 1000),
            ],
        );

        let diff = diff_profiles(&old, &new);
        let entry = diff.entries.iter().find(|e| e.column == "new_col").unwrap();
        assert_eq!(entry.severity, DiffSeverity::Info);
        assert!(entry.message.contains("added"));
    }

    #[test]
    fn test_column_removed() {
        let old = make_profile(
            "orders",
            1000,
            vec![
                make_col("id", 0.0, 1000, 1000),
                make_col("removed_col", 0.0, 500, 1000),
            ],
        );
        let new = make_profile("orders", 1000, vec![make_col("id", 0.0, 1000, 1000)]);

        let diff = diff_profiles(&old, &new);
        assert!(diff.has_warning);
        let entry = diff
            .entries
            .iter()
            .find(|e| e.column == "removed_col")
            .unwrap();
        assert_eq!(entry.severity, DiffSeverity::Warning);
    }

    #[test]
    fn test_no_changes() {
        let col = make_col("id", 0.01, 1000, 1000);
        let old = make_profile("orders", 1000, vec![col.clone()]);
        let new = make_profile("orders", 1000, vec![col]);

        let diff = diff_profiles(&old, &new);
        assert!(!diff.has_critical);
        assert!(!diff.has_warning);
        assert!(diff.entries.is_empty());
    }

    #[test]
    fn test_generate_profile_sql_basic() {
        let cols = vec![
            ("id".to_string(), "INTEGER".to_string()),
            ("name".to_string(), "VARCHAR".to_string()),
        ];
        let sql = generate_profile_sql("orders", &cols);
        assert!(sql.contains("COUNT(*)"));
        assert!(sql.contains("COUNT(DISTINCT \"id\")"));
        assert!(sql.contains("\"id__distinct\""));
        assert!(sql.contains("\"id__nulls\""));
        // integer should get mean/stddev
        assert!(sql.contains("\"id__mean\""));
        assert!(sql.contains("\"id__stddev\""));
        // varchar should NOT get mean/stddev
        assert!(!sql.contains("\"name__mean\""));
        assert!(sql.contains("FROM orders"));
    }

    #[test]
    fn test_generate_profile_sql_empty_columns() {
        let sql = generate_profile_sql("empty_table", &[]);
        assert_eq!(sql, "SELECT COUNT(*) AS row_count FROM empty_table");
    }
}
