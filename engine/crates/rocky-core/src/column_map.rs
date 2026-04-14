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
pub fn build_column_name_set(columns: &[ColumnInfo], exclude: &[String]) -> HashSet<String> {
    let exclude_set: HashSet<String> = exclude.iter().map(|s| s.to_lowercase()).collect();
    columns
        .iter()
        .map(|c| c.name.to_lowercase())
        .filter(|n| !exclude_set.contains(n))
        .collect()
}
