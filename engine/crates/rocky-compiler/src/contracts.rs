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

use crate::diagnostic::{Diagnostic, E010, E011, E012, E013, E037, W010, W032};
use crate::types::{RockyType, TypedColumn, UNVERIFIABLE_POLICY, UnverifiablePolicy};

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

        if let Some(name) = path.file_name().and_then(|n| n.to_str())
            && let Some(model_name) = name.strip_suffix(".contract.toml")
        {
            let content = std::fs::read_to_string(&path)
                .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
            let contract: CompilerContract = toml::from_str(&content)
                .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;
            contracts.insert(model_name.to_string(), contract);
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
    validate_contract_with_policy(model_name, inferred_schema, contract, UNVERIFIABLE_POLICY)
}

/// [`validate_contract`] with the unverifiable-type policy as an argument.
///
/// Private on purpose — see the same arrangement in
/// `rocky_core::contracts::validate_contract_typed_with_policy`. The argument
/// exists so the `Reject` arm is exercised by tests before the flip, not so a
/// caller can pick how strict a governance gate is.
fn validate_contract_with_policy(
    model_name: &str,
    inferred_schema: &[TypedColumn],
    contract: &CompilerContract,
    policy: UnverifiablePolicy,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    let col_names: Vec<&str> = inferred_schema.iter().map(|c| c.name.as_str()).collect();

    // Check required columns exist
    for required in &contract.rules.required {
        if !col_names.contains(&required.as_str()) {
            diagnostics.push(
                Diagnostic::error(
                    E010,
                    model_name,
                    format!("required column '{required}' missing from model output"),
                )
                .with_suggestion(format!(
                    "add `{required}` to the SELECT, or remove it from `[rules] required`"
                )),
            );
        }
    }

    // Check column constraints
    for contract_col in &contract.columns {
        let inferred = inferred_schema.iter().find(|c| c.name == contract_col.name);

        match inferred {
            Some(col) => {
                // Type check.
                //
                // Three outcomes, not two. The contract's expected type is a
                // *string* here, not a `RockyType`, so `contract_verdict` does
                // not slot in — but the same rule applies: an unresolved type
                // is unresolved evidence, and "could not check" must not be
                // recorded the same way as "checked and found nothing wrong".
                // It used to be recorded by emitting nothing at all.
                if let Some(ref expected_type) = contract_col.type_name {
                    if col.data_type == RockyType::Unknown {
                        // The `Unverifiable` verdict, expressed against a
                        // string-declared type. Scoped to declared decimals for
                        // this release — see `declares_rocky_decimal`.
                        if declares_rocky_decimal(expected_type) {
                            let message = format!(
                                "column '{}' has no inferred type, so the contract's expected \
                                 type '{expected_type}' could not be verified — unresolved \
                                 evidence does not satisfy a contract",
                                contract_col.name
                            );
                            let suggestion = format!(
                                "give `{}` a concrete type — an explicit CAST with declared \
                                 precision and scale, e.g. CAST({0} AS DECIMAL(38,2)) — or \
                                 compile with source schemas available so the type resolves",
                                contract_col.name
                            );
                            diagnostics.push(match policy {
                                UnverifiablePolicy::Warn => Diagnostic::warning(
                                    W032,
                                    model_name,
                                    format!(
                                        "{message}. Warning in this release; error in the next"
                                    ),
                                )
                                .with_suggestion(suggestion),
                                UnverifiablePolicy::Reject => {
                                    Diagnostic::error(E037, model_name, message)
                                        .with_suggestion(suggestion)
                                }
                            });
                        }
                    } else if !type_name_matches(&col.data_type, expected_type) {
                        diagnostics.push(
                            Diagnostic::error(
                                E011,
                                model_name,
                                format!(
                                    "column '{}' type mismatch: contract expects {}, got {:?}",
                                    contract_col.name, expected_type, col.data_type
                                ),
                            )
                            .with_suggestion(format!(
                                "CAST `{}` to {} in the SELECT, or update the contract's expected type",
                                contract_col.name, expected_type
                            )),
                        );
                    }
                }

                // Nullability check
                if let Some(nullable) = contract_col.nullable
                    && !nullable
                    && col.nullable
                {
                    diagnostics.push(
                        Diagnostic::error(
                            E012,
                            model_name,
                            format!(
                                "column '{}' must be non-nullable per contract, but is nullable",
                                contract_col.name
                            ),
                        )
                        .with_suggestion(format!(
                            "filter out NULLs (e.g. `WHERE {0} IS NOT NULL`) or COALESCE `{0}` to a default, \
                             or relax `nullable = true` in the contract",
                            contract_col.name
                        )),
                    );
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
            diagnostics.push(
                Diagnostic::error(
                    E013,
                    model_name,
                    format!("protected column '{protected}' has been removed"),
                )
                .with_suggestion(format!(
                    "restore `{protected}` in the SELECT, or remove it from `[rules] protected`"
                )),
            );
        }
    }

    diagnostics
}

/// Whether a contract's declared type *name* is in the fixed-point decimal
/// family, in Rocky's vocabulary (`Decimal`, `Decimal(38,2)`).
///
/// The load gate's counterpart reads *warehouse* vocabulary (`DECIMAL`,
/// `NUMBER`, …) and is `rocky_core::contracts::looks_like_decimal`; the two look
/// alike and are not interchangeable. This one must agree with the `Decimal` arm
/// of [`type_name_matches`], or a declaration that arm accepts could fall
/// outside the unverifiable report.
fn declares_rocky_decimal(type_name: &str) -> bool {
    type_name == "Decimal" || type_name.starts_with("Decimal(")
}

/// Check if a RockyType matches a type name string from a contract.
///
/// **`Unknown` matches nothing.** Callers must decide the unresolved case before
/// asking this question — `validate_contract_with_policy` does, which makes the
/// `Unknown` arm below unreachable. That is exactly why it must not be left
/// returning `true`: an unreachable fail-open is still a fail-open, waiting for
/// the next caller.
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
        // Not "matches anything" — nothing is known about it. Unreachable from
        // `validate_contract_with_policy`, which is total on `Unknown`.
        RockyType::Unknown => false,
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
        let e010 = diags.iter().find(|d| &*d.code == "E010").expect("E010");
        assert!(
            e010.suggestion.as_deref().is_some_and(|s| s.contains("id")),
            "E010 must carry an actionable suggestion: {e010:?}"
        );
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
        let e011 = diags.iter().find(|d| &*d.code == "E011").expect("E011");
        assert!(
            e011.suggestion
                .as_deref()
                .is_some_and(|s| s.contains("CAST")),
            "E011 must suggest a CAST: {e011:?}"
        );
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
        let e012 = diags.iter().find(|d| &*d.code == "E012").expect("E012");
        assert!(
            e012.suggestion.is_some(),
            "E012 must carry a nullability hint: {e012:?}"
        );
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
        let e013 = diags.iter().find(|d| &*d.code == "E013").expect("E013");
        assert!(
            e013.suggestion
                .as_deref()
                .is_some_and(|s| s.contains("restore") || s.contains("protected")),
            "E013 must suggest restoring the column or relaxing the rule: {e013:?}"
        );
    }

    /// G4 — **the inversion.** This test was named `test_unknown_type_passes`
    /// and commented *"Unknown type should not produce an error (we can't
    /// check)"*. The first half is still true and the second half is the
    /// fail-open: not being able to check is not the same as checking and
    /// finding nothing wrong, and the gate used to record the two identically —
    /// by emitting nothing at all.
    ///
    /// The column still produces no `E011` (that code means *mismatch*, which
    /// is not what happened). It now produces `W032` under the shipping policy
    /// and `E037` under `Reject`.
    #[test]
    fn test_unknown_type_is_reported_not_permitted() {
        let schema = vec![typed_col("total_revenue", RockyType::Unknown, false)];

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "total_revenue".to_string(),
                type_name: Some("Decimal".to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let warned = validate_contract_with_policy(
            "test_model",
            &schema,
            &contract,
            UnverifiablePolicy::Warn,
        );
        assert!(
            warned.iter().all(|d| &*d.code != E011),
            "an unresolved type is not a mismatch: {warned:?}"
        );
        let w032 = warned
            .iter()
            .find(|d| &*d.code == W032)
            .expect("an unverifiable contract type must be reported");
        assert!(
            w032.suggestion.is_some(),
            "W032 must carry an actionable suggestion: {w032:?}"
        );

        let rejected = validate_contract_with_policy(
            "test_model",
            &schema,
            &contract,
            UnverifiablePolicy::Reject,
        );
        assert!(
            rejected.iter().any(|d| &*d.code == E037),
            "the flip must reject, got {rejected:?}"
        );
        assert!(rejected.iter().all(|d| &*d.code != W032));

        // The shipping default is the warning.
        let shipped = validate_contract("test_model", &schema, &contract);
        assert!(shipped.iter().any(|d| &*d.code == W032));
        assert!(shipped.iter().all(|d| &*d.code != E037));
    }

    /// The scope of the compiler gate's unverifiable report: only a contract
    /// that declares a *decimal* type. A cold schema cache makes every
    /// contracted column `Unknown`, so a general rule would emit one diagnostic
    /// per contracted column on every cold compile — and the flip would then
    /// break every such build. The hole this leaves is real and asserted here
    /// rather than only described.
    #[test]
    fn test_unverifiable_report_is_scoped_to_declared_decimals() {
        let schema = vec![typed_col("id", RockyType::Unknown, false)];
        let contract = |type_name: &str| CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some(type_name.to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        for declared in ["Decimal", "Decimal(38,2)"] {
            let diags = validate_contract("m", &schema, &contract(declared));
            assert!(
                diags.iter().any(|d| &*d.code == W032),
                "{declared} must be reported"
            );
        }

        // Out of scope for this release — still permitted, and still silent,
        // under both policies.
        for declared in ["Int64", "String", "Timestamp"] {
            let warned = validate_contract("m", &schema, &contract(declared));
            let rejected = validate_contract_with_policy(
                "m",
                &schema,
                &contract(declared),
                UnverifiablePolicy::Reject,
            );
            assert!(
                warned.is_empty() && rejected.is_empty(),
                "{declared}: scoped out of this release, but got {warned:?} / {rejected:?}"
            );
        }
    }

    /// G6 — `Unknown` must not match a declared type name either. Once the
    /// caller decides `Unknown` first this arm is unreachable, which is exactly
    /// why it must not be left returning `true`: it is a fail-open sitting in
    /// wait for the next caller.
    #[test]
    fn test_type_name_matches_rejects_unknown() {
        assert!(!type_name_matches(&RockyType::Unknown, "Int64"));
        assert!(!type_name_matches(&RockyType::Unknown, "Decimal"));
        assert!(!type_name_matches(&RockyType::Unknown, "Unknown"));
        // Known types are unaffected.
        assert!(type_name_matches(&RockyType::Int64, "Int64"));
        assert!(type_name_matches(
            &RockyType::Decimal {
                precision: 10,
                scale: 2
            },
            "Decimal(10,2)"
        ));
        assert!(!type_name_matches(&RockyType::Int64, "String"));
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
