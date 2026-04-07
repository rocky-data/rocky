//! Shadow mode comparison engine.
//!
//! Compares shadow table output against production tables to detect
//! discrepancies in row counts, schemas, and sampled data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::ir::ColumnInfo;

// ---------------------------------------------------------------------------
// Comparison result
// ---------------------------------------------------------------------------

/// Full comparison result for a single table pair (shadow vs. production).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonResult {
    /// Fully-qualified table name (production target).
    pub table: String,
    /// Whether row counts match exactly.
    pub row_count_match: bool,
    /// Row count from the shadow table.
    pub shadow_count: u64,
    /// Row count from the production table.
    pub production_count: u64,
    /// Absolute difference (shadow - production). Negative means shadow has fewer rows.
    pub row_count_diff: i64,
    /// Percentage difference relative to production count. 0.0 if both are zero.
    pub row_count_diff_pct: f64,
    /// Whether schemas match (ignoring order if allowed by thresholds).
    pub schema_match: bool,
    /// Individual schema differences detected.
    pub schema_diffs: Vec<SchemaDiff>,
    /// Whether sampled data matches. `None` if sample comparison was skipped.
    pub sample_match: Option<bool>,
    /// Individual sample row mismatches.
    pub sample_mismatches: Vec<SampleMismatch>,
}

// ---------------------------------------------------------------------------
// Schema diff
// ---------------------------------------------------------------------------

/// A single difference between shadow and production table schemas.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum SchemaDiff {
    /// Column exists in shadow but not in production.
    ColumnAdded { name: String },
    /// Column exists in production but not in shadow.
    ColumnRemoved { name: String },
    /// Column exists in both but with different types.
    ColumnTypeDiff {
        name: String,
        shadow_type: String,
        prod_type: String,
    },
    /// Column exists in both but at different ordinal positions.
    ColumnOrderDiff {
        name: String,
        shadow_pos: usize,
        prod_pos: usize,
    },
}

// ---------------------------------------------------------------------------
// Sample mismatch
// ---------------------------------------------------------------------------

/// A single row-level mismatch found during sample data comparison.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SampleMismatch {
    /// Key column values identifying the mismatched row.
    pub key_values: HashMap<String, String>,
    /// The column where the values differ.
    pub column: String,
    /// Value from the shadow table.
    pub shadow_value: String,
    /// Value from the production table.
    pub production_value: String,
}

// ---------------------------------------------------------------------------
// Thresholds
// ---------------------------------------------------------------------------

/// Configurable thresholds for evaluating comparison results.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonThresholds {
    /// Row count percentage difference that triggers a warning (default: 0.01 = 1%).
    #[serde(default = "default_warn_pct")]
    pub row_count_diff_pct_warn: f64,
    /// Row count percentage difference that triggers a failure (default: 0.05 = 5%).
    #[serde(default = "default_fail_pct")]
    pub row_count_diff_pct_fail: f64,
    /// Whether to allow column order differences without failing (default: true).
    #[serde(default = "default_allow_order_diff")]
    pub allow_column_order_diff: bool,
}

fn default_warn_pct() -> f64 {
    0.01
}

fn default_fail_pct() -> f64 {
    0.05
}

fn default_allow_order_diff() -> bool {
    true
}

impl Default for ComparisonThresholds {
    fn default() -> Self {
        Self {
            row_count_diff_pct_warn: default_warn_pct(),
            row_count_diff_pct_fail: default_fail_pct(),
            allow_column_order_diff: true,
        }
    }
}

// ---------------------------------------------------------------------------
// Verdict
// ---------------------------------------------------------------------------

/// Overall verdict from evaluating a comparison result against thresholds.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "status", content = "messages")]
pub enum ComparisonVerdict {
    /// All checks passed within acceptable thresholds.
    Pass,
    /// Some checks produced warnings but nothing failed.
    Warn(Vec<String>),
    /// One or more checks exceeded failure thresholds.
    Fail(Vec<String>),
}

// ---------------------------------------------------------------------------
// Pure comparison functions
// ---------------------------------------------------------------------------

/// Compare row counts and return (exact_match, signed_diff, pct_diff).
///
/// The percentage is relative to the production count. If both counts are zero,
/// the percentage is 0.0 and the match is exact.
pub fn compare_row_counts(shadow_count: u64, prod_count: u64) -> (bool, i64, f64) {
    let diff = shadow_count as i64 - prod_count as i64;
    let exact = diff == 0;

    let pct = if prod_count == 0 {
        if shadow_count == 0 {
            0.0
        } else {
            // Shadow has rows, production has none — 100% difference.
            1.0
        }
    } else {
        (diff as f64).abs() / prod_count as f64
    };

    (exact, diff, pct)
}

/// Compare two ordered column lists and return all detected schema differences.
///
/// Detection includes:
/// - Columns present in shadow but not production (`ColumnAdded`)
/// - Columns present in production but not shadow (`ColumnRemoved`)
/// - Columns in both with different data types (`ColumnTypeDiff`)
/// - Columns in both but at different ordinal positions (`ColumnOrderDiff`)
pub fn compare_schemas(shadow_cols: &[ColumnInfo], prod_cols: &[ColumnInfo]) -> Vec<SchemaDiff> {
    let mut diffs = Vec::new();

    // Build lookup maps: name -> (position, data_type)
    let shadow_map: HashMap<&str, (usize, &str)> = shadow_cols
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.as_str(), (i, c.data_type.as_str())))
        .collect();

    let prod_map: HashMap<&str, (usize, &str)> = prod_cols
        .iter()
        .enumerate()
        .map(|(i, c)| (c.name.as_str(), (i, c.data_type.as_str())))
        .collect();

    // Check for columns added in shadow (not in production).
    for col in shadow_cols {
        if !prod_map.contains_key(col.name.as_str()) {
            diffs.push(SchemaDiff::ColumnAdded {
                name: col.name.clone(),
            });
        }
    }

    // Check for columns removed from shadow (in production but not shadow).
    for col in prod_cols {
        if !shadow_map.contains_key(col.name.as_str()) {
            diffs.push(SchemaDiff::ColumnRemoved {
                name: col.name.clone(),
            });
        }
    }

    // Check for type and order differences on columns present in both.
    for col in shadow_cols {
        if let Some(&(prod_pos, prod_type)) = prod_map.get(col.name.as_str()) {
            let &(shadow_pos, shadow_type) = shadow_map.get(col.name.as_str()).unwrap();

            if shadow_type != prod_type {
                diffs.push(SchemaDiff::ColumnTypeDiff {
                    name: col.name.clone(),
                    shadow_type: shadow_type.to_string(),
                    prod_type: prod_type.to_string(),
                });
            }

            if shadow_pos != prod_pos {
                diffs.push(SchemaDiff::ColumnOrderDiff {
                    name: col.name.clone(),
                    shadow_pos,
                    prod_pos,
                });
            }
        }
    }

    diffs
}

/// Evaluate a comparison result against thresholds and produce a verdict.
pub fn evaluate_comparison(
    result: &ComparisonResult,
    thresholds: &ComparisonThresholds,
) -> ComparisonVerdict {
    let mut warnings: Vec<String> = Vec::new();
    let mut failures: Vec<String> = Vec::new();

    // Row count evaluation.
    if !result.row_count_match {
        let pct = result.row_count_diff_pct;
        if pct >= thresholds.row_count_diff_pct_fail {
            failures.push(format!(
                "row count diff {:.2}% exceeds failure threshold {:.2}% \
                 (shadow={}, production={}, diff={})",
                pct * 100.0,
                thresholds.row_count_diff_pct_fail * 100.0,
                result.shadow_count,
                result.production_count,
                result.row_count_diff,
            ));
        } else if pct >= thresholds.row_count_diff_pct_warn {
            warnings.push(format!(
                "row count diff {:.2}% exceeds warning threshold {:.2}% \
                 (shadow={}, production={}, diff={})",
                pct * 100.0,
                thresholds.row_count_diff_pct_warn * 100.0,
                result.shadow_count,
                result.production_count,
                result.row_count_diff,
            ));
        }
    }

    // Schema evaluation.
    if !result.schema_match {
        for diff in &result.schema_diffs {
            match diff {
                SchemaDiff::ColumnAdded { name } => {
                    failures.push(format!(
                        "column `{name}` exists in shadow but not production"
                    ));
                }
                SchemaDiff::ColumnRemoved { name } => {
                    failures.push(format!(
                        "column `{name}` exists in production but not shadow"
                    ));
                }
                SchemaDiff::ColumnTypeDiff {
                    name,
                    shadow_type,
                    prod_type,
                } => {
                    failures.push(format!(
                        "column `{name}` type mismatch: shadow={shadow_type}, production={prod_type}"
                    ));
                }
                SchemaDiff::ColumnOrderDiff {
                    name,
                    shadow_pos,
                    prod_pos,
                } => {
                    if thresholds.allow_column_order_diff {
                        warnings.push(format!(
                            "column `{name}` order differs: shadow pos={shadow_pos}, production pos={prod_pos}"
                        ));
                    } else {
                        failures.push(format!(
                            "column `{name}` order differs: shadow pos={shadow_pos}, production pos={prod_pos}"
                        ));
                    }
                }
            }
        }
    }

    // Sample evaluation.
    if let Some(false) = result.sample_match {
        if result.sample_mismatches.is_empty() {
            warnings.push("sample comparison reported mismatches but no details".into());
        } else {
            failures.push(format!(
                "{} sample row(s) differ between shadow and production",
                result.sample_mismatches.len()
            ));
        }
    }

    if !failures.is_empty() {
        ComparisonVerdict::Fail(failures)
    } else if !warnings.is_empty() {
        ComparisonVerdict::Warn(warnings)
    } else {
        ComparisonVerdict::Pass
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Row count comparison
    // -----------------------------------------------------------------------

    #[test]
    fn test_row_count_exact_match() {
        let (exact, diff, pct) = compare_row_counts(1000, 1000);
        assert!(exact);
        assert_eq!(diff, 0);
        assert_eq!(pct, 0.0);
    }

    #[test]
    fn test_row_count_both_zero() {
        let (exact, diff, pct) = compare_row_counts(0, 0);
        assert!(exact);
        assert_eq!(diff, 0);
        assert_eq!(pct, 0.0);
    }

    #[test]
    fn test_row_count_shadow_has_more() {
        let (exact, diff, pct) = compare_row_counts(1010, 1000);
        assert!(!exact);
        assert_eq!(diff, 10);
        assert!((pct - 0.01).abs() < 1e-9);
    }

    #[test]
    fn test_row_count_shadow_has_fewer() {
        let (exact, diff, pct) = compare_row_counts(990, 1000);
        assert!(!exact);
        assert_eq!(diff, -10);
        assert!((pct - 0.01).abs() < 1e-9);
    }

    #[test]
    fn test_row_count_large_diff() {
        let (exact, diff, pct) = compare_row_counts(1100, 1000);
        assert!(!exact);
        assert_eq!(diff, 100);
        assert!((pct - 0.10).abs() < 1e-9);
    }

    #[test]
    fn test_row_count_prod_zero_shadow_nonzero() {
        let (exact, diff, pct) = compare_row_counts(100, 0);
        assert!(!exact);
        assert_eq!(diff, 100);
        assert_eq!(pct, 1.0);
    }

    // -----------------------------------------------------------------------
    // Schema comparison
    // -----------------------------------------------------------------------

    fn col(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
        }
    }

    #[test]
    fn test_schema_identical() {
        let shadow = vec![col("id", "INT"), col("name", "STRING")];
        let prod = vec![col("id", "INT"), col("name", "STRING")];
        let diffs = compare_schemas(&shadow, &prod);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_schema_column_added_in_shadow() {
        let shadow = vec![
            col("id", "INT"),
            col("name", "STRING"),
            col("loyalty_tier", "STRING"),
        ];
        let prod = vec![col("id", "INT"), col("name", "STRING")];
        let diffs = compare_schemas(&shadow, &prod);

        assert_eq!(diffs.len(), 1);
        assert_eq!(
            diffs[0],
            SchemaDiff::ColumnAdded {
                name: "loyalty_tier".into()
            }
        );
    }

    #[test]
    fn test_schema_column_removed_from_shadow() {
        let shadow = vec![col("id", "INT")];
        let prod = vec![col("id", "INT"), col("name", "STRING")];
        let diffs = compare_schemas(&shadow, &prod);

        assert_eq!(diffs.len(), 1);
        assert_eq!(
            diffs[0],
            SchemaDiff::ColumnRemoved {
                name: "name".into()
            }
        );
    }

    #[test]
    fn test_schema_type_diff() {
        let shadow = vec![col("id", "BIGINT"), col("name", "STRING")];
        let prod = vec![col("id", "INT"), col("name", "STRING")];
        let diffs = compare_schemas(&shadow, &prod);

        assert_eq!(diffs.len(), 1);
        assert_eq!(
            diffs[0],
            SchemaDiff::ColumnTypeDiff {
                name: "id".into(),
                shadow_type: "BIGINT".into(),
                prod_type: "INT".into(),
            }
        );
    }

    #[test]
    fn test_schema_order_diff() {
        let shadow = vec![col("name", "STRING"), col("id", "INT")];
        let prod = vec![col("id", "INT"), col("name", "STRING")];
        let diffs = compare_schemas(&shadow, &prod);

        // Both columns have order diffs.
        let order_diffs: Vec<_> = diffs
            .iter()
            .filter(|d| matches!(d, SchemaDiff::ColumnOrderDiff { .. }))
            .collect();
        assert_eq!(order_diffs.len(), 2);
    }

    #[test]
    fn test_schema_multiple_diffs() {
        let shadow = vec![
            col("id", "BIGINT"),
            col("email", "STRING"),
            col("tier", "STRING"),
        ];
        let prod = vec![
            col("id", "INT"),
            col("name", "STRING"),
            col("email", "VARCHAR"),
        ];
        let diffs = compare_schemas(&shadow, &prod);

        // id: type diff (BIGINT vs INT)
        // email: type diff (STRING vs VARCHAR) + order diff (1 vs 2)
        // tier: added in shadow
        // name: removed from shadow
        assert!(
            diffs
                .iter()
                .any(|d| matches!(d, SchemaDiff::ColumnAdded { name } if name == "tier"))
        );
        assert!(
            diffs
                .iter()
                .any(|d| matches!(d, SchemaDiff::ColumnRemoved { name } if name == "name"))
        );
        assert!(
            diffs
                .iter()
                .any(|d| matches!(d, SchemaDiff::ColumnTypeDiff { name, .. } if name == "id"))
        );
        assert!(
            diffs
                .iter()
                .any(|d| matches!(d, SchemaDiff::ColumnTypeDiff { name, .. } if name == "email"))
        );
    }

    // -----------------------------------------------------------------------
    // Verdict evaluation
    // -----------------------------------------------------------------------

    fn make_result(
        shadow_count: u64,
        prod_count: u64,
        schema_diffs: Vec<SchemaDiff>,
        sample_match: Option<bool>,
        sample_mismatches: Vec<SampleMismatch>,
    ) -> ComparisonResult {
        let (row_count_match, row_count_diff, row_count_diff_pct) =
            compare_row_counts(shadow_count, prod_count);
        let schema_match = schema_diffs.is_empty();
        ComparisonResult {
            table: "test_table".into(),
            row_count_match,
            shadow_count,
            production_count: prod_count,
            row_count_diff,
            row_count_diff_pct,
            schema_match,
            schema_diffs,
            sample_match,
            sample_mismatches,
        }
    }

    #[test]
    fn test_verdict_pass_exact_match() {
        let result = make_result(1000, 1000, vec![], Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert_eq!(verdict, ComparisonVerdict::Pass);
    }

    #[test]
    fn test_verdict_pass_no_sample() {
        let result = make_result(1000, 1000, vec![], None, vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert_eq!(verdict, ComparisonVerdict::Pass);
    }

    #[test]
    fn test_verdict_warn_row_count_within_warn_threshold() {
        // 0.5% diff — above default warn (1%) is false, let's do 1.5%
        let result = make_result(1015, 1000, vec![], Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Warn(_)));
    }

    #[test]
    fn test_verdict_fail_row_count_exceeds_fail_threshold() {
        // 10% diff — well above default fail threshold of 5%.
        let result = make_result(1100, 1000, vec![], Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_fail_schema_column_added() {
        let diffs = vec![SchemaDiff::ColumnAdded {
            name: "loyalty_tier".into(),
        }];
        let result = make_result(1000, 1000, diffs, Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_fail_schema_column_removed() {
        let diffs = vec![SchemaDiff::ColumnRemoved {
            name: "old_col".into(),
        }];
        let result = make_result(1000, 1000, diffs, Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_fail_schema_type_diff() {
        let diffs = vec![SchemaDiff::ColumnTypeDiff {
            name: "id".into(),
            shadow_type: "BIGINT".into(),
            prod_type: "INT".into(),
        }];
        let result = make_result(1000, 1000, diffs, Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_warn_column_order_diff_allowed() {
        let diffs = vec![SchemaDiff::ColumnOrderDiff {
            name: "name".into(),
            shadow_pos: 1,
            prod_pos: 0,
        }];
        let result = make_result(1000, 1000, diffs, Some(true), vec![]);
        let thresholds = ComparisonThresholds {
            allow_column_order_diff: true,
            ..Default::default()
        };
        let verdict = evaluate_comparison(&result, &thresholds);
        // Order diff with allow=true should produce a warning, not failure.
        assert!(matches!(verdict, ComparisonVerdict::Warn(_)));
    }

    #[test]
    fn test_verdict_fail_column_order_diff_disallowed() {
        let diffs = vec![SchemaDiff::ColumnOrderDiff {
            name: "name".into(),
            shadow_pos: 1,
            prod_pos: 0,
        }];
        let result = make_result(1000, 1000, diffs, Some(true), vec![]);
        let thresholds = ComparisonThresholds {
            allow_column_order_diff: false,
            ..Default::default()
        };
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_fail_sample_mismatches() {
        let mismatches = vec![SampleMismatch {
            key_values: HashMap::from([("order_id".into(), "12345".into())]),
            column: "revenue".into(),
            shadow_value: "99.95".into(),
            production_value: "99.94".into(),
        }];
        let result = make_result(1000, 1000, vec![], Some(false), mismatches);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert!(matches!(verdict, ComparisonVerdict::Fail(_)));
    }

    #[test]
    fn test_verdict_pass_below_warn_threshold() {
        // 0.5% diff — below default warn threshold of 1%.
        let result = make_result(1005, 1000, vec![], Some(true), vec![]);
        let thresholds = ComparisonThresholds::default();
        let verdict = evaluate_comparison(&result, &thresholds);
        assert_eq!(verdict, ComparisonVerdict::Pass);
    }

    // -----------------------------------------------------------------------
    // Serialization
    // -----------------------------------------------------------------------

    #[test]
    fn test_comparison_result_serialization() {
        let result = make_result(1000, 1000, vec![], Some(true), vec![]);
        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ComparisonResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.table, "test_table");
        assert!(deserialized.row_count_match);
        assert_eq!(deserialized.shadow_count, 1000);
    }

    #[test]
    fn test_thresholds_serialization() {
        let thresholds = ComparisonThresholds::default();
        let json = serde_json::to_string(&thresholds).unwrap();
        let deserialized: ComparisonThresholds = serde_json::from_str(&json).unwrap();
        assert!((deserialized.row_count_diff_pct_warn - 0.01).abs() < 1e-9);
        assert!((deserialized.row_count_diff_pct_fail - 0.05).abs() < 1e-9);
        assert!(deserialized.allow_column_order_diff);
    }

    #[test]
    fn test_schema_diff_serialization() {
        let diff = SchemaDiff::ColumnTypeDiff {
            name: "id".into(),
            shadow_type: "BIGINT".into(),
            prod_type: "INT".into(),
        };
        let json = serde_json::to_string(&diff).unwrap();
        let deserialized: SchemaDiff = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, diff);
    }

    #[test]
    fn test_verdict_serialization() {
        let verdict = ComparisonVerdict::Warn(vec!["minor diff".into()]);
        let json = serde_json::to_string(&verdict).unwrap();
        let deserialized: ComparisonVerdict = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, verdict);
    }
}
