//! Compile-time contract validation.
//!
//! Validates inferred model schemas against `.contract.toml` files at compile time,
//! catching issues like missing columns, type mismatches, and nullability violations
//! before warehouse execution.
//!
//! This complements the runtime contract validation in `rocky_core::contracts`.

use std::collections::HashMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::diagnostic::{Diagnostic, E010, E011, E012, E013, W010};
use crate::types::{RockyType, TypedColumn};

/// A compile-time contract for a model's output schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompilerContract {
    /// Column constraints.
    #[serde(default)]
    pub columns: Vec<ContractColumn>,
    /// Schema-level rules.
    #[serde(default)]
    pub rules: ContractRules,
}

/// A column constraint in a contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractColumn {
    /// Column name (required).
    pub name: String,
    /// Expected Rocky type name (e.g., "Int64", "String"). Optional.
    #[serde(rename = "type")]
    pub type_name: Option<String>,
    /// Whether the column must be non-nullable. Optional.
    pub nullable: Option<bool>,
    /// Human-readable description. Not validated, for documentation.
    pub description: Option<String>,
}

/// Schema-level rules in a contract.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ContractRules {
    /// Columns that must always exist in the output.
    #[serde(default)]
    pub required: Vec<String>,
    /// Columns that must never be removed.
    #[serde(default)]
    pub protected: Vec<String>,
    /// If true, no new nullable columns may be added.
    #[serde(default)]
    pub no_new_nullable: bool,
}

/// Load contracts from a directory.
///
/// Each file named `{model_name}.contract.toml` defines a contract for that model.
pub fn load_contracts(dir: &Path) -> Result<HashMap<String, CompilerContract>, String> {
    let mut contracts = HashMap::new();

    if !dir.exists() {
        return Ok(contracts);
    }

    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
            if let Some(model_name) = name.strip_suffix(".contract.toml") {
                let content = std::fs::read_to_string(&path)
                    .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
                let contract: CompilerContract = toml::from_str(&content)
                    .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
                contracts.insert(model_name.to_string(), contract);
            }
        }
    }

    Ok(contracts)
}

/// Discover contracts from models that have a `contract_path` set
/// (auto-discovered `<stem>.contract.toml` next to the `.sql` file).
///
/// Returns contracts keyed by model name. Use alongside [`load_contracts`]
/// to merge explicit and auto-discovered contracts.
pub fn discover_contracts_from_models(
    models: &[rocky_core::models::Model],
) -> Result<HashMap<String, CompilerContract>, String> {
    let mut contracts = HashMap::new();

    for model in models {
        if let Some(ref contract_path) = model.contract_path {
            let content = std::fs::read_to_string(contract_path)
                .map_err(|e| format!("failed to read {}: {e}", contract_path.display()))?;
            let contract: CompilerContract = toml::from_str(&content)
                .map_err(|e| format!("failed to parse {}: {e}", contract_path.display()))?;
            contracts.insert(model.config.name.clone(), contract);
        }
    }

    Ok(contracts)
}

/// Validate a model's inferred schema against its contract.
pub fn validate_contract(
    model_name: &str,
    inferred_schema: &[TypedColumn],
    contract: &CompilerContract,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let col_names: Vec<&str> = inferred_schema.iter().map(|c| c.name.as_str()).collect();

    // Check required columns exist
    for required in &contract.rules.required {
        if !col_names.contains(&required.as_str()) {
            diagnostics.push(Diagnostic::error(
                E010,
                model_name,
                format!("required column '{required}' missing from model output"),
            ));
        }
    }

    // Check column constraints
    for contract_col in &contract.columns {
        let inferred = inferred_schema.iter().find(|c| c.name == contract_col.name);

        match inferred {
            Some(col) => {
                // Type check
                if let Some(ref expected_type) = contract_col.type_name {
                    let matches = type_name_matches(&col.data_type, expected_type);
                    if !matches && col.data_type != RockyType::Unknown {
                        diagnostics.push(Diagnostic::error(
                            E011,
                            model_name,
                            format!(
                                "column '{}' type mismatch: contract expects {}, got {:?}",
                                contract_col.name, expected_type, col.data_type
                            ),
                        ));
                    }
                }

                // Nullability check
                if let Some(nullable) = contract_col.nullable {
                    if !nullable && col.nullable {
                        diagnostics.push(Diagnostic::error(
                            E012,
                            model_name,
                            format!(
                                "column '{}' must be non-nullable per contract, but is nullable",
                                contract_col.name
                            ),
                        ));
                    }
                }
            }
            None => {
                // Column defined in contract but missing from model
                if contract.rules.required.contains(&contract_col.name) {
                    // Already reported as E010
                } else {
                    diagnostics.push(Diagnostic::warning(
                        W010,
                        model_name,
                        format!(
                            "contract column '{}' not found in model output",
                            contract_col.name
                        ),
                    ));
                }
            }
        }
    }

    // Check protected columns
    for protected in &contract.rules.protected {
        if !col_names.contains(&protected.as_str()) {
            diagnostics.push(Diagnostic::error(
                E013,
                model_name,
                format!("protected column '{protected}' has been removed"),
            ));
        }
    }

    diagnostics
}

/// Check if a RockyType matches a type name string from a contract.
fn type_name_matches(rocky_type: &RockyType, type_name: &str) -> bool {
    match rocky_type {
        RockyType::Boolean => type_name == "Boolean",
        RockyType::Int32 => type_name == "Int32",
        RockyType::Int64 => type_name == "Int64",
        RockyType::Float32 => type_name == "Float32",
        RockyType::Float64 => type_name == "Float64",
        RockyType::Decimal { .. } => type_name == "Decimal" || type_name.starts_with("Decimal("),
        RockyType::String => type_name == "String",
        RockyType::Binary => type_name == "Binary",
        RockyType::Date => type_name == "Date",
        RockyType::Timestamp => type_name == "Timestamp",
        RockyType::TimestampNtz => type_name == "TimestampNtz",
        RockyType::Array(_) => type_name == "Array" || type_name.starts_with("Array<"),
        RockyType::Map(_, _) => type_name == "Map" || type_name.starts_with("Map<"),
        RockyType::Struct(_) => type_name == "Struct",
        RockyType::Variant => type_name == "Variant",
        RockyType::Unknown => true, // Unknown matches anything
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed_col(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    #[test]
    fn test_valid_contract() {
        let schema = vec![
            typed_col("id", RockyType::Int64, false),
            typed_col("name", RockyType::String, true),
        ];

        let contract = CompilerContract {
            columns: vec![
                ContractColumn {
                    name: "id".to_string(),
                    type_name: Some("Int64".to_string()),
                    nullable: Some(false),
                    description: None,
                },
                ContractColumn {
                    name: "name".to_string(),
                    type_name: Some("String".to_string()),
                    nullable: None,
                    description: None,
                },
            ],
            rules: ContractRules {
                required: vec!["id".to_string()],
                ..Default::default()
            },
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(diags.is_empty(), "expected no diagnostics: {diags:?}");
    }

    #[test]
    fn test_missing_required_column() {
        let schema = vec![typed_col("name", RockyType::String, true)];

        let contract = CompilerContract {
            columns: vec![],
            rules: ContractRules {
                required: vec!["id".to_string()],
                ..Default::default()
            },
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(diags.iter().any(|d| &*d.code == "E010"));
    }

    #[test]
    fn test_type_mismatch() {
        let schema = vec![typed_col("id", RockyType::String, false)];

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some("Int64".to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(diags.iter().any(|d| &*d.code == "E011"));
    }

    #[test]
    fn test_nullability_violation() {
        let schema = vec![typed_col("id", RockyType::Int64, true)]; // nullable

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: None,
                nullable: Some(false), // must be non-nullable
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(diags.iter().any(|d| &*d.code == "E012"));
    }

    #[test]
    fn test_protected_column_removed() {
        let schema = vec![typed_col("name", RockyType::String, true)];

        let contract = CompilerContract {
            columns: vec![],
            rules: ContractRules {
                protected: vec!["id".to_string()],
                ..Default::default()
            },
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(diags.iter().any(|d| &*d.code == "E013"));
    }

    #[test]
    fn test_unknown_type_passes() {
        let schema = vec![typed_col("id", RockyType::Unknown, false)];

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some("Int64".to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let diags = validate_contract("test_model", &schema, &contract);
        // Unknown type should not produce an error (we can't check)
        assert!(diags.iter().all(|d| &*d.code != "E011"));
    }

    #[test]
    fn test_contract_toml_parsing() {
        let toml_str = r#"
[[columns]]
name = "customer_id"
type = "Int64"
nullable = false
description = "Unique customer identifier"

[[columns]]
name = "total_revenue"
type = "Decimal"
nullable = false

[rules]
required = ["customer_id", "total_revenue"]
protected = ["customer_id"]
no_new_nullable = true
"#;

        let contract: CompilerContract = toml::from_str(toml_str).unwrap();
        assert_eq!(contract.columns.len(), 2);
        assert_eq!(contract.rules.required.len(), 2);
        assert_eq!(contract.rules.protected.len(), 1);
        assert!(contract.rules.no_new_nullable);
    }
}
