//! `rocky branch` — virtual environments.
//!
//! Creates isolated branches for testing model changes without
//! affecting production data.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A virtual branch for testing model changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    /// Branch name.
    pub name: String,
    /// Shadow catalog/schema prefix for this branch.
    pub schema_prefix: String,
    /// Models modified in this branch.
    pub modified_models: Vec<String>,
}

/// Result of comparing branch output against main.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchDiff {
    /// Per-model diffs.
    pub model_diffs: Vec<ModelDiff>,
}

/// Diff for a single model between branch and main.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelDiff {
    pub model: String,
    /// Row count in main vs branch.
    pub main_rows: Option<u64>,
    pub branch_rows: Option<u64>,
    /// Schema changes.
    pub added_columns: Vec<String>,
    pub removed_columns: Vec<String>,
}

/// Create a new branch.
pub fn create_branch(name: &str, user: &str) -> Branch {
    Branch {
        name: name.to_string(),
        schema_prefix: format!("dev__{user}__{name}"),
        modified_models: Vec::new(),
    }
}

/// Compute diff between branch and main.
///
/// Currently compares model schemas (column additions/removals).
/// Full row-level diff requires warehouse execution.
pub fn diff_branch(
    branch: &Branch,
    main_schemas: &HashMap<String, Vec<String>>,
    branch_schemas: &HashMap<String, Vec<String>>,
) -> BranchDiff {
    let mut diffs = Vec::new();

    for model in &branch.modified_models {
        let main_cols: Vec<String> = main_schemas.get(model).cloned().unwrap_or_default();
        let branch_cols: Vec<String> = branch_schemas.get(model).cloned().unwrap_or_default();

        let added: Vec<String> = branch_cols
            .iter()
            .filter(|c| !main_cols.contains(c))
            .cloned()
            .collect();
        let removed: Vec<String> = main_cols
            .iter()
            .filter(|c| !branch_cols.contains(c))
            .cloned()
            .collect();

        diffs.push(ModelDiff {
            model: model.clone(),
            main_rows: None,
            branch_rows: None,
            added_columns: added,
            removed_columns: removed,
        });
    }

    BranchDiff { model_diffs: diffs }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_branch() {
        let branch = create_branch("feature/new-model", "hugo");
        assert_eq!(branch.schema_prefix, "dev__hugo__feature/new-model");
    }

    #[test]
    fn test_diff_added_column() {
        let branch = Branch {
            name: "test".to_string(),
            schema_prefix: "dev__test".to_string(),
            modified_models: vec!["orders".to_string()],
        };

        let mut main_schemas = HashMap::new();
        main_schemas.insert(
            "orders".to_string(),
            vec!["id".to_string(), "amount".to_string()],
        );

        let mut branch_schemas = HashMap::new();
        branch_schemas.insert(
            "orders".to_string(),
            vec!["id".to_string(), "amount".to_string(), "tax".to_string()],
        );

        let diff = diff_branch(&branch, &main_schemas, &branch_schemas);
        assert_eq!(diff.model_diffs.len(), 1);
        assert_eq!(diff.model_diffs[0].added_columns, vec!["tax"]);
        assert!(diff.model_diffs[0].removed_columns.is_empty());
    }

    #[test]
    fn test_diff_removed_column() {
        let branch = Branch {
            name: "test".to_string(),
            schema_prefix: "dev__test".to_string(),
            modified_models: vec!["orders".to_string()],
        };

        let mut main_schemas = HashMap::new();
        main_schemas.insert(
            "orders".to_string(),
            vec!["id".to_string(), "amount".to_string(), "legacy".to_string()],
        );

        let mut branch_schemas = HashMap::new();
        branch_schemas.insert(
            "orders".to_string(),
            vec!["id".to_string(), "amount".to_string()],
        );

        let diff = diff_branch(&branch, &main_schemas, &branch_schemas);
        assert_eq!(diff.model_diffs[0].removed_columns, vec!["legacy"]);
    }
}
