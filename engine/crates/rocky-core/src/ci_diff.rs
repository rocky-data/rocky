//! CI data diff types and formatting for PR comments.
//!
//! Provides typed structs representing the diff between model outputs across
//! two branches or pipeline states, plus formatting functions that produce
//! markdown (for GitHub PR comments) and terminal-friendly tables.
//!
//! This module is **foundation only** — it defines the result types and
//! formatters. The actual data comparison logic and CLI command will be
//! added in a follow-up.

use std::fmt;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Model-level diff status
// ---------------------------------------------------------------------------

/// Status of a single model in the diff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelDiffStatus {
    /// Model output is identical across both sides.
    Unchanged,
    /// Model exists on both sides but output differs.
    Modified,
    /// Model exists only on the incoming (PR) side.
    Added,
    /// Model exists only on the base (target branch) side.
    Removed,
}

impl fmt::Display for ModelDiffStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unchanged => write!(f, "unchanged"),
            Self::Modified => write!(f, "modified"),
            Self::Added => write!(f, "added"),
            Self::Removed => write!(f, "removed"),
        }
    }
}

// ---------------------------------------------------------------------------
// Column-level change
// ---------------------------------------------------------------------------

/// The kind of change observed for a single column.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnChangeType {
    /// Column was added in the incoming side.
    Added,
    /// Column was removed (present on base, absent on incoming).
    Removed,
    /// Column data type changed between base and incoming.
    TypeChanged,
}

impl fmt::Display for ColumnChangeType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Added => write!(f, "added"),
            Self::Removed => write!(f, "removed"),
            Self::TypeChanged => write!(f, "type changed"),
        }
    }
}

/// A single column-level difference within a model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDiff {
    /// Column name.
    pub column_name: String,
    /// Kind of change.
    pub change_type: ColumnChangeType,
    /// Previous data type (empty for added columns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_type: Option<String>,
    /// New data type (empty for removed columns).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_type: Option<String>,
}

// ---------------------------------------------------------------------------
// Per-model diff result
// ---------------------------------------------------------------------------

/// Diff result for a single model between two pipeline states.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiffResult {
    /// Fully-qualified model name (e.g. `catalog.schema.table`).
    pub model_name: String,
    /// High-level status.
    pub status: ModelDiffStatus,
    /// Row count on the base (target branch) side. `None` for added models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count_before: Option<u64>,
    /// Row count on the incoming (PR) side. `None` for removed models.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_count_after: Option<u64>,
    /// Column-level changes (empty for unchanged models).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_changes: Vec<ColumnDiff>,
    /// Optional sample of changed rows for human review.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_changed_rows: Option<Vec<Vec<String>>>,
}

// ---------------------------------------------------------------------------
// Aggregate summary
// ---------------------------------------------------------------------------

/// High-level summary across all models in a diff run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffSummary {
    pub total_models: usize,
    pub unchanged: usize,
    pub modified: usize,
    pub added: usize,
    pub removed: usize,
}

impl DiffSummary {
    /// Build a summary from a slice of diff results.
    pub fn from_results(results: &[DiffResult]) -> Self {
        let mut summary = Self {
            total_models: results.len(),
            unchanged: 0,
            modified: 0,
            added: 0,
            removed: 0,
        };
        for r in results {
            match r.status {
                ModelDiffStatus::Unchanged => summary.unchanged += 1,
                ModelDiffStatus::Modified => summary.modified += 1,
                ModelDiffStatus::Added => summary.added += 1,
                ModelDiffStatus::Removed => summary.removed += 1,
            }
        }
        summary
    }

    /// Returns `true` when there are no modifications, additions, or removals.
    pub fn is_clean(&self) -> bool {
        self.modified == 0 && self.added == 0 && self.removed == 0
    }
}

// ---------------------------------------------------------------------------
// Markdown formatter (GitHub PR comment)
// ---------------------------------------------------------------------------

/// Produce a markdown summary suitable for a GitHub PR comment.
///
/// The output includes a one-line summary, a table of changed models, and
/// per-model details (column changes, row count deltas) for non-unchanged
/// models. Unchanged models are mentioned only in the summary count.
pub fn format_diff_markdown(results: &[DiffResult]) -> String {
    let summary = DiffSummary::from_results(results);
    let mut out = String::new();

    // Header
    out.push_str("### Rocky CI Diff\n\n");

    // One-line verdict
    if summary.is_clean() {
        out.push_str("No data changes detected.\n");
        return out;
    }

    out.push_str(&format!(
        "**{} model(s) changed** ({} modified, {} added, {} removed, {} unchanged)\n\n",
        summary.modified + summary.added + summary.removed,
        summary.modified,
        summary.added,
        summary.removed,
        summary.unchanged,
    ));

    // Summary table — only non-unchanged models
    out.push_str("| Model | Status | Rows (before) | Rows (after) | Delta |\n");
    out.push_str("|-------|--------|--------------|-------------|-------|\n");

    for r in results {
        if r.status == ModelDiffStatus::Unchanged {
            continue;
        }
        let before = r
            .row_count_before
            .map_or(String::from("-"), |n| n.to_string());
        let after = r
            .row_count_after
            .map_or(String::from("-"), |n| n.to_string());
        let delta = format_row_delta(r.row_count_before, r.row_count_after);
        let status_badge = match r.status {
            ModelDiffStatus::Modified => "modified",
            ModelDiffStatus::Added => "added",
            ModelDiffStatus::Removed => "removed",
            ModelDiffStatus::Unchanged => unreachable!(),
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} | {} |\n",
            r.model_name, status_badge, before, after, delta
        ));
    }
    out.push('\n');

    // Per-model column details
    for r in results {
        if r.column_changes.is_empty() {
            continue;
        }
        out.push_str(&format!(
            "<details>\n<summary><b>{}</b> — column changes</summary>\n\n",
            r.model_name
        ));
        out.push_str("| Column | Change | Old Type | New Type |\n");
        out.push_str("|--------|--------|----------|----------|\n");
        for c in &r.column_changes {
            let old = c.old_type.as_deref().unwrap_or("-");
            let new = c.new_type.as_deref().unwrap_or("-");
            out.push_str(&format!(
                "| `{}` | {} | {} | {} |\n",
                c.column_name, c.change_type, old, new
            ));
        }
        out.push_str("\n</details>\n\n");
    }

    out
}

// ---------------------------------------------------------------------------
// Terminal table formatter
// ---------------------------------------------------------------------------

/// Produce a terminal-friendly table of diff results.
///
/// Uses fixed-width columns aligned with spaces. Suitable for CLI `--output table`.
pub fn format_diff_table(results: &[DiffResult]) -> String {
    let summary = DiffSummary::from_results(results);
    let mut out = String::new();

    // Header line
    out.push_str(&format!(
        "Diff: {} model(s) total — {} modified, {} added, {} removed, {} unchanged\n\n",
        summary.total_models, summary.modified, summary.added, summary.removed, summary.unchanged,
    ));

    if summary.is_clean() {
        out.push_str("No data changes detected.\n");
        return out;
    }

    // Compute column widths for alignment
    let changed: Vec<&DiffResult> = results
        .iter()
        .filter(|r| r.status != ModelDiffStatus::Unchanged)
        .collect();

    let model_width = changed
        .iter()
        .map(|r| r.model_name.len())
        .max()
        .unwrap_or(5)
        .max(5);
    let status_width = 8; // "modified" is the longest status string
    let num_width = 12;

    // Table header
    out.push_str(&format!(
        "  {:<model_width$}  {:<status_width$}  {:>num_width$}  {:>num_width$}  {:>num_width$}\n",
        "MODEL", "STATUS", "BEFORE", "AFTER", "DELTA",
    ));
    out.push_str(&format!(
        "  {:<model_width$}  {:<status_width$}  {:>num_width$}  {:>num_width$}  {:>num_width$}\n",
        "-".repeat(model_width),
        "-".repeat(status_width),
        "-".repeat(num_width),
        "-".repeat(num_width),
        "-".repeat(num_width),
    ));

    for r in &changed {
        let before = r
            .row_count_before
            .map_or(String::from("-"), |n| n.to_string());
        let after = r
            .row_count_after
            .map_or(String::from("-"), |n| n.to_string());
        let delta = format_row_delta(r.row_count_before, r.row_count_after);

        out.push_str(&format!(
            "  {:<model_width$}  {:<status_width$}  {:>num_width$}  {:>num_width$}  {:>num_width$}\n",
            r.model_name,
            r.status.to_string(),
            before,
            after,
            delta,
        ));

        // Inline column changes
        for c in &r.column_changes {
            let detail = match c.change_type {
                ColumnChangeType::Added => {
                    format!(
                        "  + column `{}` ({})",
                        c.column_name,
                        c.new_type.as_deref().unwrap_or("?")
                    )
                }
                ColumnChangeType::Removed => {
                    format!(
                        "  - column `{}` (was {})",
                        c.column_name,
                        c.old_type.as_deref().unwrap_or("?")
                    )
                }
                ColumnChangeType::TypeChanged => {
                    format!(
                        "  ~ column `{}`: {} -> {}",
                        c.column_name,
                        c.old_type.as_deref().unwrap_or("?"),
                        c.new_type.as_deref().unwrap_or("?")
                    )
                }
            };
            out.push_str(&format!("  {:<model_width$}  {detail}\n", ""));
        }
    }

    out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Format a row count delta as a signed string (e.g. "+100", "-50", "N/A").
fn format_row_delta(before: Option<u64>, after: Option<u64>) -> String {
    match (before, after) {
        (Some(b), Some(a)) => {
            let diff = a as i64 - b as i64;
            if diff >= 0 {
                format!("+{diff}")
            } else {
                format!("{diff}")
            }
        }
        (None, Some(a)) => format!("+{a}"),
        (Some(b), None) => format!("-{b}"),
        (None, None) => "N/A".to_string(),
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn unchanged_model(name: &str, rows: u64) -> DiffResult {
        DiffResult {
            model_name: name.to_string(),
            status: ModelDiffStatus::Unchanged,
            row_count_before: Some(rows),
            row_count_after: Some(rows),
            column_changes: vec![],
            sample_changed_rows: None,
        }
    }

    fn modified_model(
        name: &str,
        before: u64,
        after: u64,
        column_changes: Vec<ColumnDiff>,
    ) -> DiffResult {
        DiffResult {
            model_name: name.to_string(),
            status: ModelDiffStatus::Modified,
            row_count_before: Some(before),
            row_count_after: Some(after),
            column_changes,
            sample_changed_rows: None,
        }
    }

    fn added_model(name: &str, rows: u64) -> DiffResult {
        DiffResult {
            model_name: name.to_string(),
            status: ModelDiffStatus::Added,
            row_count_before: None,
            row_count_after: Some(rows),
            column_changes: vec![],
            sample_changed_rows: None,
        }
    }

    fn removed_model(name: &str, rows: u64) -> DiffResult {
        DiffResult {
            model_name: name.to_string(),
            status: ModelDiffStatus::Removed,
            row_count_before: Some(rows),
            row_count_after: None,
            column_changes: vec![],
            sample_changed_rows: None,
        }
    }

    // -----------------------------------------------------------------------
    // DiffSummary
    // -----------------------------------------------------------------------

    #[test]
    fn summary_from_empty_results() {
        let summary = DiffSummary::from_results(&[]);
        assert_eq!(summary.total_models, 0);
        assert!(summary.is_clean());
    }

    #[test]
    fn summary_counts_statuses_correctly() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            unchanged_model("acme.raw.customers", 5_000),
            modified_model("acme.raw.products", 1_000, 1_200, vec![]),
            added_model("acme.raw.reviews", 500),
            removed_model("acme.raw.legacy_users", 2_000),
        ];
        let summary = DiffSummary::from_results(&results);
        assert_eq!(summary.total_models, 5);
        assert_eq!(summary.unchanged, 2);
        assert_eq!(summary.modified, 1);
        assert_eq!(summary.added, 1);
        assert_eq!(summary.removed, 1);
        assert!(!summary.is_clean());
    }

    #[test]
    fn summary_is_clean_when_all_unchanged() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            unchanged_model("acme.raw.customers", 5_000),
        ];
        let summary = DiffSummary::from_results(&results);
        assert!(summary.is_clean());
    }

    // -----------------------------------------------------------------------
    // format_row_delta
    // -----------------------------------------------------------------------

    #[test]
    fn delta_positive() {
        assert_eq!(format_row_delta(Some(100), Some(150)), "+50");
    }

    #[test]
    fn delta_negative() {
        assert_eq!(format_row_delta(Some(200), Some(150)), "-50");
    }

    #[test]
    fn delta_zero() {
        assert_eq!(format_row_delta(Some(100), Some(100)), "+0");
    }

    #[test]
    fn delta_added_model() {
        assert_eq!(format_row_delta(None, Some(500)), "+500");
    }

    #[test]
    fn delta_removed_model() {
        assert_eq!(format_row_delta(Some(500), None), "-500");
    }

    #[test]
    fn delta_both_none() {
        assert_eq!(format_row_delta(None, None), "N/A");
    }

    // -----------------------------------------------------------------------
    // format_diff_markdown
    // -----------------------------------------------------------------------

    #[test]
    fn markdown_no_changes() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            unchanged_model("acme.raw.customers", 5_000),
        ];
        let md = format_diff_markdown(&results);
        assert!(md.contains("### Rocky CI Diff"));
        assert!(md.contains("No data changes detected."));
        // Should not contain a table
        assert!(!md.contains("| Model"));
    }

    #[test]
    fn markdown_empty_results() {
        let md = format_diff_markdown(&[]);
        assert!(md.contains("No data changes detected."));
    }

    #[test]
    fn markdown_mixed_changes() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            modified_model(
                "acme.raw.products",
                1_000,
                1_200,
                vec![
                    ColumnDiff {
                        column_name: "price".to_string(),
                        change_type: ColumnChangeType::TypeChanged,
                        old_type: Some("FLOAT".to_string()),
                        new_type: Some("DOUBLE".to_string()),
                    },
                    ColumnDiff {
                        column_name: "discount_pct".to_string(),
                        change_type: ColumnChangeType::Added,
                        old_type: None,
                        new_type: Some("DECIMAL(5,2)".to_string()),
                    },
                ],
            ),
            added_model("acme.raw.reviews", 500),
            removed_model("acme.raw.legacy_users", 2_000),
        ];

        let md = format_diff_markdown(&results);

        // Summary line
        assert!(md.contains("**3 model(s) changed**"));
        assert!(md.contains("1 modified"));
        assert!(md.contains("1 added"));
        assert!(md.contains("1 removed"));
        assert!(md.contains("1 unchanged"));

        // Model table rows
        assert!(md.contains("| `acme.raw.products` | modified | 1000 | 1200 | +200 |"));
        assert!(md.contains("| `acme.raw.reviews` | added | - | 500 | +500 |"));
        assert!(md.contains("| `acme.raw.legacy_users` | removed | 2000 | - | -2000 |"));

        // Unchanged model should NOT appear in the table
        assert!(!md.contains("| `acme.raw.orders`"));

        // Column details in collapsible section
        assert!(md.contains("<details>"));
        assert!(md.contains("acme.raw.products"));
        assert!(md.contains("| `price` | type changed | FLOAT | DOUBLE |"));
        assert!(md.contains("| `discount_pct` | added | - | DECIMAL(5,2) |"));
    }

    #[test]
    fn markdown_removed_column_detail() {
        let results = vec![modified_model(
            "acme.stg.users",
            5_000,
            5_000,
            vec![ColumnDiff {
                column_name: "legacy_flag".to_string(),
                change_type: ColumnChangeType::Removed,
                old_type: Some("BOOLEAN".to_string()),
                new_type: None,
            }],
        )];

        let md = format_diff_markdown(&results);
        assert!(md.contains("| `legacy_flag` | removed | BOOLEAN | - |"));
    }

    // -----------------------------------------------------------------------
    // format_diff_table
    // -----------------------------------------------------------------------

    #[test]
    fn table_no_changes() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            unchanged_model("acme.raw.customers", 5_000),
        ];
        let tbl = format_diff_table(&results);
        assert!(tbl.contains("0 modified"));
        assert!(tbl.contains("No data changes detected."));
    }

    #[test]
    fn table_mixed_changes() {
        let results = vec![
            unchanged_model("acme.raw.orders", 10_000),
            modified_model(
                "acme.raw.products",
                1_000,
                1_200,
                vec![ColumnDiff {
                    column_name: "price".to_string(),
                    change_type: ColumnChangeType::TypeChanged,
                    old_type: Some("FLOAT".to_string()),
                    new_type: Some("DOUBLE".to_string()),
                }],
            ),
            added_model("acme.raw.reviews", 500),
        ];

        let tbl = format_diff_table(&results);

        // Header
        assert!(tbl.contains("MODEL"));
        assert!(tbl.contains("STATUS"));
        assert!(tbl.contains("BEFORE"));
        assert!(tbl.contains("AFTER"));
        assert!(tbl.contains("DELTA"));

        // Data rows
        assert!(tbl.contains("acme.raw.products"));
        assert!(tbl.contains("modified"));
        assert!(tbl.contains("acme.raw.reviews"));
        assert!(tbl.contains("added"));

        // Unchanged should not appear in table body
        assert!(!tbl.contains("acme.raw.orders"));

        // Inline column change
        assert!(tbl.contains("~ column `price`: FLOAT -> DOUBLE"));
    }

    #[test]
    fn table_column_added_and_removed() {
        let results = vec![modified_model(
            "acme.stg.events",
            8_000,
            8_200,
            vec![
                ColumnDiff {
                    column_name: "event_type".to_string(),
                    change_type: ColumnChangeType::Added,
                    old_type: None,
                    new_type: Some("STRING".to_string()),
                },
                ColumnDiff {
                    column_name: "old_category".to_string(),
                    change_type: ColumnChangeType::Removed,
                    old_type: Some("VARCHAR".to_string()),
                    new_type: None,
                },
            ],
        )];

        let tbl = format_diff_table(&results);
        assert!(tbl.contains("+ column `event_type` (STRING)"));
        assert!(tbl.contains("- column `old_category` (was VARCHAR)"));
    }

    // -----------------------------------------------------------------------
    // Serialization round-trip
    // -----------------------------------------------------------------------

    #[test]
    fn diff_result_serialization_roundtrip() {
        let result = modified_model(
            "acme.raw.products",
            1_000,
            1_200,
            vec![ColumnDiff {
                column_name: "price".to_string(),
                change_type: ColumnChangeType::TypeChanged,
                old_type: Some("FLOAT".to_string()),
                new_type: Some("DOUBLE".to_string()),
            }],
        );

        let json = serde_json::to_string_pretty(&result).unwrap();
        let roundtrip: DiffResult = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip.model_name, "acme.raw.products");
        assert_eq!(roundtrip.status, ModelDiffStatus::Modified);
        assert_eq!(roundtrip.row_count_before, Some(1_000));
        assert_eq!(roundtrip.row_count_after, Some(1_200));
        assert_eq!(roundtrip.column_changes.len(), 1);
        assert_eq!(roundtrip.column_changes[0].column_name, "price");
        assert_eq!(
            roundtrip.column_changes[0].change_type,
            ColumnChangeType::TypeChanged
        );
    }

    #[test]
    fn diff_summary_serialization_roundtrip() {
        let summary = DiffSummary {
            total_models: 5,
            unchanged: 2,
            modified: 1,
            added: 1,
            removed: 1,
        };
        let json = serde_json::to_string(&summary).unwrap();
        let roundtrip: DiffSummary = serde_json::from_str(&json).unwrap();
        assert_eq!(roundtrip, summary);
    }

    #[test]
    fn column_diff_skip_serializing_none_types() {
        let added = ColumnDiff {
            column_name: "new_col".to_string(),
            change_type: ColumnChangeType::Added,
            old_type: None,
            new_type: Some("STRING".to_string()),
        };
        let json = serde_json::to_string(&added).unwrap();
        // old_type should be absent from the JSON
        assert!(!json.contains("old_type"));
        assert!(json.contains("new_type"));
    }

    // -----------------------------------------------------------------------
    // ModelDiffStatus Display
    // -----------------------------------------------------------------------

    #[test]
    fn status_display() {
        assert_eq!(ModelDiffStatus::Unchanged.to_string(), "unchanged");
        assert_eq!(ModelDiffStatus::Modified.to_string(), "modified");
        assert_eq!(ModelDiffStatus::Added.to_string(), "added");
        assert_eq!(ModelDiffStatus::Removed.to_string(), "removed");
    }

    // -----------------------------------------------------------------------
    // ColumnChangeType Display
    // -----------------------------------------------------------------------

    #[test]
    fn change_type_display() {
        assert_eq!(ColumnChangeType::Added.to_string(), "added");
        assert_eq!(ColumnChangeType::Removed.to_string(), "removed");
        assert_eq!(ColumnChangeType::TypeChanged.to_string(), "type changed");
    }
}
