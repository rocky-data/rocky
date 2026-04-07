use serde::{Deserialize, Serialize};

use crate::column_map;
use crate::ir::ColumnInfo;

/// Data contract configuration — enforced at copy time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContractConfig {
    /// Columns that must exist with specific types.
    #[serde(default)]
    pub required_columns: Vec<RequiredColumn>,

    /// Column names that must never be removed from the target.
    #[serde(default)]
    pub protected_columns: Vec<String>,

    /// Type changes that are allowed (widening only).
    #[serde(default)]
    pub allowed_type_changes: Vec<AllowedTypeChange>,
}

/// A column that must exist in the source with a specific type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequiredColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

/// A permitted type widening (e.g., INT to BIGINT) that won't trigger a violation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowedTypeChange {
    pub from: String,
    pub to: String,
}

/// Result of contract validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractResult {
    pub passed: bool,
    pub violations: Vec<ContractViolation>,
}

/// A single contract rule violation with the rule name, affected column, and message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractViolation {
    pub rule: String,
    pub column: String,
    pub message: String,
}

/// Validates source columns against a data contract.
///
/// Returns violations for:
/// - Required columns missing from source
/// - Required columns with wrong type
/// - Protected columns missing from source (compared against previous target)
/// - Type changes that aren't in the allowed list
pub fn validate_contract(
    contract: &ContractConfig,
    source_columns: &[ColumnInfo],
    target_columns: &[ColumnInfo],
) -> ContractResult {
    let mut violations = Vec::new();

    let source_map = column_map::build_column_map(source_columns);
    let target_map = column_map::build_column_map(target_columns);

    // Check required columns
    for req in &contract.required_columns {
        let name_lower = req.name.to_lowercase();
        match source_map.get(&name_lower) {
            None => {
                violations.push(ContractViolation {
                    rule: "required_column".to_string(),
                    column: req.name.clone(),
                    message: format!("required column '{}' not found in source", req.name),
                });
            }
            Some(col) => {
                if col.data_type.to_lowercase() != req.data_type.to_lowercase() {
                    violations.push(ContractViolation {
                        rule: "required_column_type".to_string(),
                        column: req.name.clone(),
                        message: format!(
                            "column '{}' has type '{}', expected '{}'",
                            req.name, col.data_type, req.data_type
                        ),
                    });
                }
            }
        }
    }

    // Check protected columns (must exist in source if they existed in target)
    for protected in &contract.protected_columns {
        let name_lower = protected.to_lowercase();
        if target_map.contains_key(&name_lower) && !source_map.contains_key(&name_lower) {
            violations.push(ContractViolation {
                rule: "protected_column".to_string(),
                column: protected.clone(),
                message: format!(
                    "protected column '{}' exists in target but missing from source",
                    protected
                ),
            });
        }
    }

    // Check type changes against allowed list
    for (name, source_col) in &source_map {
        if let Some(target_col) = target_map.get(name) {
            let src_type = source_col.data_type.to_lowercase();
            let tgt_type = target_col.data_type.to_lowercase();

            if src_type != tgt_type {
                let is_allowed = contract.allowed_type_changes.iter().any(|atc| {
                    atc.from.to_lowercase() == tgt_type && atc.to.to_lowercase() == src_type
                });

                if !is_allowed {
                    violations.push(ContractViolation {
                        rule: "disallowed_type_change".to_string(),
                        column: source_col.name.clone(),
                        message: format!(
                            "type changed from '{}' to '{}' (not in allowed_type_changes)",
                            target_col.data_type, source_col.data_type
                        ),
                    });
                }
            }
        }
    }

    ContractResult {
        passed: violations.is_empty(),
        violations,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, data_type: &str) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable: true,
        }
    }

    #[test]
    fn test_empty_contract_passes() {
        let contract = ContractConfig::default();
        let result = validate_contract(&contract, &[col("id", "INT")], &[col("id", "INT")]);
        assert!(result.passed);
        assert!(result.violations.is_empty());
    }

    #[test]
    fn test_required_column_present() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            ..Default::default()
        };
        let result = validate_contract(&contract, &[col("id", "BIGINT")], &[]);
        assert!(result.passed);
    }

    #[test]
    fn test_required_column_missing() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            ..Default::default()
        };
        let result = validate_contract(&contract, &[col("name", "STRING")], &[]);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column");
    }

    #[test]
    fn test_required_column_wrong_type() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            ..Default::default()
        };
        let result = validate_contract(&contract, &[col("id", "STRING")], &[]);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column_type");
    }

    #[test]
    fn test_protected_column_removed() {
        let contract = ContractConfig {
            protected_columns: vec!["email".into()],
            ..Default::default()
        };
        // email exists in target but not in source → violation
        let result = validate_contract(
            &contract,
            &[col("id", "INT")],
            &[col("id", "INT"), col("email", "STRING")],
        );
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "protected_column");
    }

    #[test]
    fn test_protected_column_still_present() {
        let contract = ContractConfig {
            protected_columns: vec!["email".into()],
            ..Default::default()
        };
        let result = validate_contract(
            &contract,
            &[col("id", "INT"), col("email", "STRING")],
            &[col("id", "INT"), col("email", "STRING")],
        );
        assert!(result.passed);
    }

    #[test]
    fn test_protected_column_not_in_target_ok() {
        let contract = ContractConfig {
            protected_columns: vec!["email".into()],
            ..Default::default()
        };
        // email not in target = new table, no protection needed
        let result = validate_contract(&contract, &[col("id", "INT")], &[]);
        assert!(result.passed);
    }

    #[test]
    fn test_disallowed_type_change() {
        let contract = ContractConfig::default();
        // STRING → INT is not in allowed_type_changes
        let result = validate_contract(
            &contract,
            &[col("status", "INT")],
            &[col("status", "STRING")],
        );
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "disallowed_type_change");
    }

    #[test]
    fn test_allowed_type_change() {
        let contract = ContractConfig {
            allowed_type_changes: vec![AllowedTypeChange {
                from: "INT".into(),
                to: "BIGINT".into(),
            }],
            ..Default::default()
        };
        // INT → BIGINT is allowed (widening)
        let result = validate_contract(&contract, &[col("id", "BIGINT")], &[col("id", "INT")]);
        assert!(result.passed);
    }

    #[test]
    fn test_reverse_type_change_not_allowed() {
        let contract = ContractConfig {
            allowed_type_changes: vec![AllowedTypeChange {
                from: "INT".into(),
                to: "BIGINT".into(),
            }],
            ..Default::default()
        };
        // BIGINT → INT is NOT allowed (narrowing)
        let result = validate_contract(&contract, &[col("id", "INT")], &[col("id", "BIGINT")]);
        assert!(!result.passed);
    }

    #[test]
    fn test_case_insensitive() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "ID".into(),
                data_type: "bigint".into(),
                nullable: false,
            }],
            ..Default::default()
        };
        let result = validate_contract(&contract, &[col("id", "BIGINT")], &[]);
        assert!(result.passed);
    }

    #[test]
    fn test_multiple_violations() {
        let contract = ContractConfig {
            required_columns: vec![
                RequiredColumn {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: false,
                },
                RequiredColumn {
                    name: "email".into(),
                    data_type: "STRING".into(),
                    nullable: false,
                },
            ],
            ..Default::default()
        };
        // Both required columns missing
        let result = validate_contract(&contract, &[col("name", "STRING")], &[]);
        assert!(!result.passed);
        assert_eq!(result.violations.len(), 2);
    }

    #[test]
    fn test_contract_serialization() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            protected_columns: vec!["email".into()],
            allowed_type_changes: vec![AllowedTypeChange {
                from: "INT".into(),
                to: "BIGINT".into(),
            }],
        };
        let json = serde_json::to_string(&contract).unwrap();
        let deserialized: ContractConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.required_columns.len(), 1);
        assert_eq!(deserialized.protected_columns.len(), 1);
        assert_eq!(deserialized.allowed_type_changes.len(), 1);
    }
}
