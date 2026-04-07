//! Partition-level incremental processing.
//!
//! Beyond watermark-based incremental (append-only), this module supports:
//! - **Partition checksums** — detect changes in existing rows
//! - **Column-level change propagation** — skip downstream models
//!   that don't depend on changed columns (using the semantic graph)
//! - **Schema version hashes** — detect upstream schema changes

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::ir::PartitionWindow;
use crate::models::TimeGrain;

/// Checksum for a single partition of a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionChecksum {
    /// Partition key value (e.g., "2026-03-28" for date partitions).
    pub partition_key: String,
    /// Hash of the partition data.
    pub checksum: u64,
    /// Number of rows in this partition.
    pub row_count: u64,
    /// When the checksum was computed.
    pub computed_at: DateTime<Utc>,
}

/// Lifecycle status of a partition recorded in the state store.
///
/// Used by the state-store `PARTITIONS` table (added in Phase 2) and by
/// `--missing` discovery to decide which partitions need recomputation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionStatus {
    /// Computed successfully and committed to the warehouse.
    Computed,
    /// Compute attempt failed; partition is undefined in the warehouse.
    Failed,
    /// Compute is in flight (set at the start of a partition run, cleared at
    /// the end). A long-lived `InProgress` row indicates a crashed runner.
    InProgress,
}

/// State-store record for a single partition of a `time_interval` model.
///
/// One row per `(model_name, partition_key)` in the `PARTITIONS` redb table
/// (added in Phase 2). Authoritative source for "did partition X compute"; the
/// `--missing` selection consults this table only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionRecord {
    pub model_name: String,
    pub partition_key: String,
    pub status: PartitionStatus,
    pub computed_at: DateTime<Utc>,
    pub row_count: u64,
    pub duration_ms: u64,
    /// Cross-reference into `RUN_HISTORY` for forensics.
    pub run_id: String,
    /// Optional content checksum for change detection (reuses
    /// `PartitionChecksum::checksum`).
    pub checksum: Option<u64>,
}

/// Errors from parsing partition keys or computing partition windows.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PartitionError {
    #[error("partition key '{key}' does not match the canonical {grain:?} format '{expected}'")]
    BadFormat {
        key: String,
        grain: TimeGrain,
        expected: &'static str,
    },

    #[error("range start '{start}' is after range end '{end}'")]
    InvertedRange { start: String, end: String },
}

/// Parse a canonical partition key (e.g., `"2026-04-07"` for `Day`) into the
/// `[start, end)` window the runtime substitutes for `@start_date` /
/// `@end_date`.
///
/// The `end` is computed via [`TimeGrain::next`] so month/year boundaries
/// honor the calendar (not 30/365-day approximations).
pub fn partition_key_to_window(
    grain: TimeGrain,
    key: &str,
) -> Result<PartitionWindow, PartitionError> {
    let start = parse_partition_key(grain, key)?;
    let end = grain.next(start);
    Ok(PartitionWindow {
        key: key.to_string(),
        start,
        end,
    })
}

/// Validate that a string is a well-formed canonical partition key for the
/// given grain. Used by the compiler to check `first_partition` and by the CLI
/// to validate `--partition` / `--from` / `--to` arguments.
pub fn validate_partition_key(grain: TimeGrain, key: &str) -> Result<(), PartitionError> {
    parse_partition_key(grain, key).map(|_| ())
}

/// Compute the inclusive list of canonical partition keys spanning
/// `[first, last]` for the given grain. Returns the keys in chronological
/// order (earliest first), aligned to grain boundaries.
///
/// `first` and `last` are truncated to the start of their containing partition
/// before walking, so callers can pass arbitrary timestamps without risking an
/// off-by-one at the boundary.
pub fn expected_partitions(
    grain: TimeGrain,
    first: DateTime<Utc>,
    last: DateTime<Utc>,
) -> Result<Vec<String>, PartitionError> {
    let start = grain.truncate(first);
    let end = grain.truncate(last);
    if start > end {
        return Err(PartitionError::InvertedRange {
            start: grain.format_key(first),
            end: grain.format_key(last),
        });
    }
    let mut keys = Vec::new();
    let mut cursor = start;
    while cursor <= end {
        keys.push(grain.format_key(cursor));
        cursor = grain.next(cursor);
    }
    Ok(keys)
}

/// Parse a canonical partition key string into the start of its window.
fn parse_partition_key(grain: TimeGrain, key: &str) -> Result<DateTime<Utc>, PartitionError> {
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    let bad = |expected: &'static str| PartitionError::BadFormat {
        key: key.to_string(),
        grain,
        expected,
    };
    let dt = match grain {
        TimeGrain::Hour => {
            // Parse "YYYY-MM-DDTHH" by appending ":00:00"
            NaiveDateTime::parse_from_str(&format!("{key}:00:00"), "%Y-%m-%dT%H:%M:%S")
                .map_err(|_| bad("YYYY-MM-DDTHH"))?
        }
        TimeGrain::Day => {
            let d = NaiveDate::parse_from_str(key, "%Y-%m-%d").map_err(|_| bad("YYYY-MM-DD"))?;
            NaiveDateTime::new(d, NaiveTime::MIN)
        }
        TimeGrain::Month => {
            // Parse "YYYY-MM" by appending "-01"
            let d = NaiveDate::parse_from_str(&format!("{key}-01"), "%Y-%m-%d")
                .map_err(|_| bad("YYYY-MM"))?;
            NaiveDateTime::new(d, NaiveTime::MIN)
        }
        TimeGrain::Year => {
            // Parse "YYYY" by appending "-01-01"
            let d = NaiveDate::parse_from_str(&format!("{key}-01-01"), "%Y-%m-%d")
                .map_err(|_| bad("YYYY"))?;
            NaiveDateTime::new(d, NaiveTime::MIN)
        }
    };
    // Round-trip the parsed value back to canonical format and require
    // string equality. This rejects keys like "2026-04-7" (no zero-padding)
    // or "2026-13" that chrono would happily parse but aren't canonical.
    let utc = DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc);
    if grain.format_key(utc) != key {
        return Err(PartitionError::BadFormat {
            key: key.to_string(),
            grain,
            expected: grain.format_str(),
        });
    }
    Ok(utc)
}

/// Incremental strategy for a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IncrementalStrategy {
    /// No prior state — full refresh needed.
    FullRefresh,
    /// Append-only: only process rows after the watermark.
    WatermarkBased { column: String, last_value: String },
    /// Checksum-based: reprocess only changed partitions.
    ChecksumBased {
        changed_partitions: Vec<String>,
        unchanged_partitions: Vec<String>,
    },
    /// Column propagation: skip if no dependencies on changed columns.
    Skip { reason: String },
}

/// An incremental plan for a model.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalPlan {
    pub model_name: String,
    pub strategy: IncrementalStrategy,
    pub estimated_rows_to_process: Option<u64>,
}

/// Decision for a downstream model based on column-level change propagation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropagationDecision {
    /// Model must be recomputed.
    Recompute,
    /// Model can be skipped.
    Skip { reason: String },
}

/// Compare partition checksums between runs to find changed partitions.
pub fn diff_checksums(
    previous: &[PartitionChecksum],
    current: &[PartitionChecksum],
) -> (Vec<String>, Vec<String>) {
    let prev_map: HashMap<&str, u64> = previous
        .iter()
        .map(|p| (p.partition_key.as_str(), p.checksum))
        .collect();
    let curr_map: HashMap<&str, u64> = current
        .iter()
        .map(|c| (c.partition_key.as_str(), c.checksum))
        .collect();

    let mut changed = Vec::new();
    let mut unchanged = Vec::new();

    for (key, checksum) in &curr_map {
        match prev_map.get(key) {
            Some(prev_checksum) if prev_checksum == checksum => {
                unchanged.push(key.to_string());
            }
            _ => {
                // New partition or checksum changed
                changed.push(key.to_string());
            }
        }
    }

    // Partitions that were removed (in previous but not in current)
    for key in prev_map.keys() {
        if !curr_map.contains_key(key) {
            changed.push(key.to_string());
        }
    }

    changed.sort();
    unchanged.sort();
    (changed, unchanged)
}

/// Compute column-level change propagation decisions.
///
/// Given a model that changed and the specific columns that changed,
/// determines which downstream models need recomputation vs can be skipped.
///
/// Uses the semantic graph's lineage edges to check if a downstream model
/// has any dependency on the changed columns.
pub fn compute_propagation(
    changed_model: &str,
    changed_columns: &[String],
    downstream_models: &[String],
    column_dependencies: &HashMap<String, Vec<(String, String)>>, // model → [(source_model, source_col)]
) -> HashMap<String, PropagationDecision> {
    let mut decisions = HashMap::new();

    for downstream in downstream_models {
        let deps = column_dependencies
            .get(downstream)
            .cloned()
            .unwrap_or_default();

        let affected = deps
            .iter()
            .any(|(source, col)| source == changed_model && changed_columns.contains(col));

        if affected {
            decisions.insert(downstream.clone(), PropagationDecision::Recompute);
        } else {
            decisions.insert(
                downstream.clone(),
                PropagationDecision::Skip {
                    reason: format!("no dependency on changed columns in '{changed_model}'"),
                },
            );
        }
    }

    decisions
}

/// Compute a deterministic hash of a model's schema (column names + types).
///
/// Changes when columns are added, removed, or change type.
pub fn compute_schema_hash(columns: &[(String, String)]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for (name, data_type) in columns {
        name.hash(&mut hasher);
        data_type.hash(&mut hasher);
    }
    hasher.finish()
}

/// Generate SQL for computing partition checksums.
pub fn generate_checksum_sql(
    table_ref: &str,
    partition_column: &str,
    value_columns: &[String],
) -> String {
    let concat_cols = if value_columns.is_empty() {
        "*".to_string()
    } else {
        value_columns.join(", ")
    };

    format!(
        "SELECT {partition_column}, \
         SUM(HASH({concat_cols})) AS checksum, \
         COUNT(*) AS row_count \
         FROM {table_ref} \
         GROUP BY {partition_column} \
         ORDER BY {partition_column}"
    )
}

/// Generate SQL for a whole-table checksum (no partition column).
///
/// Used by the Layer 0 cross-table dedup experiment for tables that
/// have no declared partition column: they participate in the
/// comparison as a single logical "partition" keyed by the constant
/// `'__whole_table__'`. The output shape mirrors [`generate_checksum_sql`]
/// — same `checksum` + `row_count` columns — so both results flow into
/// the same downstream analysis without a branch.
///
/// The absent `GROUP BY` is deliberate: the partition key is a literal,
/// not a column, and grouping by a literal would be a no-op at best.
pub fn generate_whole_table_checksum_sql(table_ref: &str, value_columns: &[String]) -> String {
    let concat_cols = if value_columns.is_empty() {
        "*".to_string()
    } else {
        value_columns.join(", ")
    };

    format!(
        "SELECT '__whole_table__' AS partition_key, \
         SUM(HASH({concat_cols})) AS checksum, \
         COUNT(*) AS row_count \
         FROM {table_ref}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_diff_checksums_no_change() {
        let prev = vec![
            PartitionChecksum {
                partition_key: "2026-01".into(),
                checksum: 100,
                row_count: 50,
                computed_at: Utc::now(),
            },
            PartitionChecksum {
                partition_key: "2026-02".into(),
                checksum: 200,
                row_count: 60,
                computed_at: Utc::now(),
            },
        ];
        let curr = prev.clone();
        let (changed, unchanged) = diff_checksums(&prev, &curr);
        assert!(changed.is_empty());
        assert_eq!(unchanged.len(), 2);
    }

    #[test]
    fn test_diff_checksums_one_changed() {
        let prev = vec![
            PartitionChecksum {
                partition_key: "2026-01".into(),
                checksum: 100,
                row_count: 50,
                computed_at: Utc::now(),
            },
            PartitionChecksum {
                partition_key: "2026-02".into(),
                checksum: 200,
                row_count: 60,
                computed_at: Utc::now(),
            },
        ];
        let curr = vec![
            PartitionChecksum {
                partition_key: "2026-01".into(),
                checksum: 100,
                row_count: 50,
                computed_at: Utc::now(),
            },
            PartitionChecksum {
                partition_key: "2026-02".into(),
                checksum: 999,
                row_count: 65,
                computed_at: Utc::now(),
            }, // changed
        ];
        let (changed, unchanged) = diff_checksums(&prev, &curr);
        assert_eq!(changed, vec!["2026-02"]);
        assert_eq!(unchanged, vec!["2026-01"]);
    }

    #[test]
    fn test_diff_checksums_new_partition() {
        let prev = vec![PartitionChecksum {
            partition_key: "2026-01".into(),
            checksum: 100,
            row_count: 50,
            computed_at: Utc::now(),
        }];
        let curr = vec![
            PartitionChecksum {
                partition_key: "2026-01".into(),
                checksum: 100,
                row_count: 50,
                computed_at: Utc::now(),
            },
            PartitionChecksum {
                partition_key: "2026-02".into(),
                checksum: 200,
                row_count: 60,
                computed_at: Utc::now(),
            },
        ];
        let (changed, unchanged) = diff_checksums(&prev, &curr);
        assert_eq!(changed, vec!["2026-02"]);
        assert_eq!(unchanged, vec!["2026-01"]);
    }

    #[test]
    fn test_diff_checksums_removed_partition() {
        let prev = vec![
            PartitionChecksum {
                partition_key: "2026-01".into(),
                checksum: 100,
                row_count: 50,
                computed_at: Utc::now(),
            },
            PartitionChecksum {
                partition_key: "2026-02".into(),
                checksum: 200,
                row_count: 60,
                computed_at: Utc::now(),
            },
        ];
        let curr = vec![PartitionChecksum {
            partition_key: "2026-01".into(),
            checksum: 100,
            row_count: 50,
            computed_at: Utc::now(),
        }];
        let (changed, unchanged) = diff_checksums(&prev, &curr);
        assert_eq!(changed, vec!["2026-02"]); // removed partition counts as changed
        assert_eq!(unchanged, vec!["2026-01"]);
    }

    #[test]
    fn test_propagation_recompute() {
        let deps: HashMap<String, Vec<(String, String)>> = [(
            "model_b".to_string(),
            vec![("model_a".to_string(), "status".to_string())],
        )]
        .into();

        let decisions = compute_propagation(
            "model_a",
            &["status".to_string()],
            &["model_b".to_string()],
            &deps,
        );
        assert_eq!(decisions["model_b"], PropagationDecision::Recompute);
    }

    #[test]
    fn test_propagation_skip() {
        let deps: HashMap<String, Vec<(String, String)>> = [(
            "model_c".to_string(),
            vec![("model_a".to_string(), "amount".to_string())], // depends on amount, not status
        )]
        .into();

        let decisions = compute_propagation(
            "model_a",
            &["status".to_string()], // only status changed
            &["model_c".to_string()],
            &deps,
        );
        assert!(matches!(
            decisions["model_c"],
            PropagationDecision::Skip { .. }
        ));
    }

    #[test]
    fn test_propagation_multiple_downstream() {
        let deps: HashMap<String, Vec<(String, String)>> = [
            (
                "b".to_string(),
                vec![("a".to_string(), "status".to_string())],
            ),
            (
                "c".to_string(),
                vec![("a".to_string(), "amount".to_string())],
            ),
        ]
        .into();

        let decisions = compute_propagation(
            "a",
            &["status".to_string()],
            &["b".to_string(), "c".to_string()],
            &deps,
        );
        assert_eq!(decisions["b"], PropagationDecision::Recompute);
        assert!(matches!(decisions["c"], PropagationDecision::Skip { .. }));
    }

    #[test]
    fn test_schema_hash_deterministic() {
        let cols = vec![
            ("id".to_string(), "INT64".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];
        let h1 = compute_schema_hash(&cols);
        let h2 = compute_schema_hash(&cols);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_schema_hash_changes_on_add() {
        let cols1 = vec![("id".to_string(), "INT64".to_string())];
        let cols2 = vec![
            ("id".to_string(), "INT64".to_string()),
            ("name".to_string(), "STRING".to_string()),
        ];
        assert_ne!(compute_schema_hash(&cols1), compute_schema_hash(&cols2));
    }

    #[test]
    fn test_schema_hash_changes_on_type_change() {
        let cols1 = vec![("id".to_string(), "INT64".to_string())];
        let cols2 = vec![("id".to_string(), "STRING".to_string())];
        assert_ne!(compute_schema_hash(&cols1), compute_schema_hash(&cols2));
    }

    #[test]
    fn test_generate_checksum_sql() {
        let sql = generate_checksum_sql(
            "db.schema.orders",
            "order_date",
            &["id".into(), "amount".into()],
        );
        assert!(sql.contains("GROUP BY order_date"));
        assert!(sql.contains("HASH(id, amount)"));
        assert!(sql.contains("FROM db.schema.orders"));
    }

    #[test]
    fn test_generate_whole_table_checksum_sql_with_columns() {
        let sql =
            generate_whole_table_checksum_sql("db.schema.orders", &["id".into(), "amount".into()]);
        assert!(sql.contains("'__whole_table__' AS partition_key"));
        assert!(sql.contains("SUM(HASH(id, amount))"));
        assert!(sql.contains("COUNT(*) AS row_count"));
        assert!(sql.contains("FROM db.schema.orders"));
        // No GROUP BY — the partition key is a literal.
        assert!(!sql.contains("GROUP BY"));
    }

    #[test]
    fn test_generate_whole_table_checksum_sql_no_columns() {
        let sql = generate_whole_table_checksum_sql("t", &[]);
        assert!(sql.contains("HASH(*)"));
        assert!(sql.contains("FROM t"));
    }

    // ----- partition arithmetic tests (Phase 1.3) -----

    use chrono::TimeZone;

    #[test]
    fn test_partition_key_to_window_day() {
        let win = partition_key_to_window(TimeGrain::Day, "2026-04-07").unwrap();
        assert_eq!(win.key, "2026-04-07");
        assert_eq!(
            win.start,
            Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap()
        );
        assert_eq!(win.end, Utc.with_ymd_and_hms(2026, 4, 8, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_partition_key_to_window_hour() {
        let win = partition_key_to_window(TimeGrain::Hour, "2026-04-07T13").unwrap();
        assert_eq!(
            win.start,
            Utc.with_ymd_and_hms(2026, 4, 7, 13, 0, 0).unwrap()
        );
        assert_eq!(win.end, Utc.with_ymd_and_hms(2026, 4, 7, 14, 0, 0).unwrap());
    }

    #[test]
    fn test_partition_key_to_window_month_leap_february() {
        // 2024 is a leap year. Window for "2024-02" must be 29 days, not 30.
        let win = partition_key_to_window(TimeGrain::Month, "2024-02").unwrap();
        assert_eq!(
            win.start,
            Utc.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(win.end, Utc.with_ymd_and_hms(2024, 3, 1, 0, 0, 0).unwrap());
        let span_days = (win.end - win.start).num_days();
        assert_eq!(span_days, 29, "leap February must have 29-day window");
    }

    #[test]
    fn test_partition_key_to_window_month_non_leap_february() {
        let win = partition_key_to_window(TimeGrain::Month, "2025-02").unwrap();
        let span_days = (win.end - win.start).num_days();
        assert_eq!(span_days, 28, "non-leap February must have 28-day window");
    }

    #[test]
    fn test_partition_key_to_window_year() {
        let win = partition_key_to_window(TimeGrain::Year, "2024").unwrap();
        assert_eq!(
            win.start,
            Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap()
        );
        assert_eq!(win.end, Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap());
        // 2024 is leap → 366 days
        assert_eq!((win.end - win.start).num_days(), 366);
    }

    #[test]
    fn test_partition_key_rejects_unpadded_day() {
        // chrono parses "2026-04-7" but it's not the canonical format.
        // We require zero-padding via the round-trip check.
        assert!(matches!(
            partition_key_to_window(TimeGrain::Day, "2026-04-7"),
            Err(PartitionError::BadFormat { .. })
        ));
    }

    #[test]
    fn test_partition_key_rejects_invalid_month() {
        assert!(matches!(
            partition_key_to_window(TimeGrain::Month, "2026-13"),
            Err(PartitionError::BadFormat { .. })
        ));
    }

    #[test]
    fn test_partition_key_rejects_wrong_grain_format() {
        // YYYY-MM-DD passed as Hour should fail.
        assert!(matches!(
            partition_key_to_window(TimeGrain::Hour, "2026-04-07"),
            Err(PartitionError::BadFormat { .. })
        ));
    }

    #[test]
    fn test_validate_partition_key() {
        assert!(validate_partition_key(TimeGrain::Day, "2026-04-07").is_ok());
        assert!(validate_partition_key(TimeGrain::Year, "2026").is_ok());
        assert!(validate_partition_key(TimeGrain::Day, "not-a-date").is_err());
    }

    #[test]
    fn test_expected_partitions_day_range() {
        let first = Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap();
        let last = Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap();
        let keys = expected_partitions(TimeGrain::Day, first, last).unwrap();
        assert_eq!(
            keys,
            vec![
                "2026-04-01",
                "2026-04-02",
                "2026-04-03",
                "2026-04-04",
                "2026-04-05",
                "2026-04-06",
                "2026-04-07",
            ]
        );
    }

    #[test]
    fn test_expected_partitions_month_range_crosses_year() {
        let first = Utc.with_ymd_and_hms(2024, 11, 1, 0, 0, 0).unwrap();
        let last = Utc.with_ymd_and_hms(2025, 2, 15, 0, 0, 0).unwrap(); // mid-month → truncated
        let keys = expected_partitions(TimeGrain::Month, first, last).unwrap();
        assert_eq!(keys, vec!["2024-11", "2024-12", "2025-01", "2025-02"]);
    }

    #[test]
    fn test_expected_partitions_single_partition() {
        let t = Utc.with_ymd_and_hms(2026, 4, 7, 13, 30, 45).unwrap();
        let keys = expected_partitions(TimeGrain::Day, t, t).unwrap();
        assert_eq!(keys, vec!["2026-04-07"]);
    }

    #[test]
    fn test_expected_partitions_inverted_range_errors() {
        let first = Utc.with_ymd_and_hms(2026, 4, 7, 0, 0, 0).unwrap();
        let last = Utc.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap();
        assert!(matches!(
            expected_partitions(TimeGrain::Day, first, last),
            Err(PartitionError::InvertedRange { .. })
        ));
    }

    #[test]
    fn test_expected_partitions_truncates_input() {
        // Mid-day inputs should snap to day boundaries before walking.
        let first = Utc.with_ymd_and_hms(2026, 4, 1, 14, 30, 0).unwrap();
        let last = Utc.with_ymd_and_hms(2026, 4, 3, 9, 15, 0).unwrap();
        let keys = expected_partitions(TimeGrain::Day, first, last).unwrap();
        assert_eq!(keys, vec!["2026-04-01", "2026-04-02", "2026-04-03"]);
    }
}
