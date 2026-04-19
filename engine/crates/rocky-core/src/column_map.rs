//! Shared helpers for building case-insensitive column lookups.
//!
//! Used by drift detection, contract validation, and column-match checks
//! to avoid duplicating the lowercase-key map/set construction.

use std::collections::{HashMap, HashSet};

use crate::ir::ColumnInfo;

/// Build a case-insensitive `name → &ColumnInfo` map.
pub fn build_column_map(columns: &[ColumnInfo]) -> HashMap<String, &ColumnInfo> {
    columns.iter().map(|c| (c.name.to_lowercase(), c)).collect()
}

/// Build a case-insensitive set of column names, excluding any in `exclude`.
///
/// `exclude` is a pre-built lowercase `HashSet` — callers that invoke this
/// for both source and target columns (e.g. `check_column_match`) should
/// hoist the set construction so the lowercase pass happens once per
/// exclude list, not once per call (§P4.1). Use
/// [`build_exclude_set`] to construct one from a `&[String]`.
pub fn build_column_name_set(columns: &[ColumnInfo], exclude: &HashSet<String>) -> HashSet<String> {
    columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .filter(|n| !exclude.contains(n))
        .collect()
}

/// Build the lowercase-set representation of an exclude list.
///
/// Helper so callers that pass the same exclude list into multiple
/// [`build_column_name_set`] invocations pay the lowercase pass once.
pub fn build_exclude_set(exclude: &[String]) -> HashSet<String> {
    exclude.iter().map(|s| s.to_lowercase()).collect()
}
