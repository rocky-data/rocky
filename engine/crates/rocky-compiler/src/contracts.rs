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

use crate::diagnostic::{Diagnostic, E010, E011, E012, E013, E014, W010};
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
                // Type check
                if let Some(ref expected_type) = contract_col.type_name {
                    let matches = type_name_matches(&col.data_type, expected_type);
                    if !matches && col.data_type != RockyType::Unknown {
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

    // `[rules] no_new_nullable` — parsed since it was introduced, enforced
    // nowhere until now. The product lowering layer knew: it refuses to emit
    // this key precisely because "the engine parses that rule and enforces it
    // nowhere, so emitting it would promise a guard that does not run"
    // (`rocky-core/src/product/lowering.rs`). A declared control that never
    // runs is worse than no control, because the operator believes it is on
    // (#1467).
    //
    // The reading: the contract's `[[columns]]` are the declared baseline, so
    // a NEW nullable column is a nullable output column the contract does not
    // declare. Enforcement is opt-in (`no_new_nullable` defaults to false), so
    // this can only fail a project that explicitly asked for the guard.
    if contract.rules.no_new_nullable {
        if contract.columns.is_empty() {
            // No baseline, so "new" has no meaning. Refusing beats the two
            // silent readings: treating every nullable column as new (a
            // surprise mass-failure) or treating none as new (inert again).
            diagnostics.push(
                Diagnostic::error(
                    E014,
                    model_name,
                    "`[rules] no_new_nullable` is set but the contract declares no `[[columns]]`, \
                     so there is no baseline for what counts as new"
                        .to_string(),
                )
                .with_suggestion(
                    "declare the expected columns in `[[columns]]`, or remove `no_new_nullable`"
                        .to_string(),
                ),
            );
        } else {
            let declared: std::collections::HashSet<&str> =
                contract.columns.iter().map(|c| c.name.as_str()).collect();
            for col in inferred_schema {
                if col.nullable && !declared.contains(col.name.as_str()) {
                    diagnostics.push(
                        Diagnostic::error(
                            E014,
                            model_name,
                            format!(
                                "nullable column '{}' is not declared in the contract, and \
                                 `[rules] no_new_nullable` forbids adding one",
                                col.name
                            ),
                        )
                        .with_suggestion(format!(
                            "declare `{}` in `[[columns]]`, make it NOT NULL in the SELECT, or \
                             remove `no_new_nullable`",
                            col.name
                        )),
                    );
                }
            }
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
        RockyType::Decimal { precision, scale } => {
            decimal_type_matches(*precision, *scale, type_name)
        }
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

/// Check if a contract's `Decimal` spelling matches an inferred decimal type.
///
/// A bare `Decimal` matches any precision and scale, so contracts written
/// before the digits were checked keep passing. `Decimal(p,s)` must match the
/// inferred precision and scale exactly. `Decimal(p)` means scale 0 — the same
/// reading the type checker gives SQL's `DECIMAL(p)`. A parameter block that
/// does not parse as digits never matches, so an unreadable contract does not
/// pass on the prefix alone.
///
/// The match is exact, not "the inferred type fits inside the declared one".
/// A contract states the model's declared output type, and this matcher
/// already rejects an inferred `Int32` against a contract saying `Int64` even
/// though that widening is safe. `drift.rs::is_safe_type_widening` answers a
/// different question — whether a live warehouse column can be altered in
/// place. It is a `SqlDialect` method, and each dialect scopes its own
/// allowlist (the default, Databricks and Trino all differ), so a compile-time
/// diagnostic cannot inherit it without becoming dialect-dependent. Every one
/// of those decimal rules requires the scale to be equal, so the case this
/// function was written for — a `Decimal(18,2)` contract over an inferred
/// `Decimal(10,0)` — is a mismatch under them too.
fn decimal_type_matches(precision: u8, scale: u8, type_name: &str) -> bool {
    if type_name == "Decimal" {
        return true;
    }

    let Some(args) = type_name
        .strip_prefix("Decimal(")
        .and_then(|rest| rest.strip_suffix(')'))
    else {
        return false;
    };

    let (declared_precision, declared_scale) = match args.split_once(',') {
        Some((declared_precision, declared_scale)) => {
            (declared_precision.trim(), declared_scale.trim())
        }
        None => (args.trim(), "0"),
    };

    declared_precision
        .parse::<u8>()
        .is_ok_and(|declared| declared == precision)
        && declared_scale
            .parse::<u8>()
            .is_ok_and(|declared| declared == scale)
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

    /// Helper: a contract declaring `id`, with the rules the test wants.
    fn contract_declaring_id(rules: ContractRules) -> CompilerContract {
        CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: None,
                nullable: None,
                description: None,
            }],
            rules,
        }
    }

    /// The rule was parsed and enforced nowhere (#1467). An undeclared
    /// nullable column is exactly what it forbids.
    #[test]
    fn no_new_nullable_rejects_an_undeclared_nullable_column() {
        let schema = vec![
            typed_col("id", RockyType::Int64, false),
            typed_col("surprise", RockyType::String, true),
        ];
        let contract = contract_declaring_id(ContractRules {
            no_new_nullable: true,
            ..Default::default()
        });

        let diags = validate_contract("m", &schema, &contract);
        let e014: Vec<_> = diags.iter().filter(|d| &*d.code == "E014").collect();
        assert_eq!(e014.len(), 1, "expected one E014, got: {diags:?}");
        assert!(
            e014[0].message.contains("surprise"),
            "the diagnostic must name the column: {:?}",
            e014[0].message
        );
    }

    /// A NON-nullable undeclared column is not what this rule is about.
    #[test]
    fn no_new_nullable_allows_an_undeclared_non_nullable_column() {
        let schema = vec![
            typed_col("id", RockyType::Int64, false),
            typed_col("added", RockyType::String, false),
        ];
        let contract = contract_declaring_id(ContractRules {
            no_new_nullable: true,
            ..Default::default()
        });

        let diags = validate_contract("m", &schema, &contract);
        assert!(
            diags.iter().all(|d| &*d.code != "E014"),
            "a non-nullable column must not trip no_new_nullable: {diags:?}"
        );
    }

    /// The rule is OPT-IN. Enforcing it must not change any project that
    /// never set it — the same schema with the flag off is clean.
    #[test]
    fn no_new_nullable_is_opt_in() {
        let schema = vec![
            typed_col("id", RockyType::Int64, false),
            typed_col("surprise", RockyType::String, true),
        ];
        let contract = contract_declaring_id(ContractRules::default());

        let diags = validate_contract("m", &schema, &contract);
        assert!(
            diags.iter().all(|d| &*d.code != "E014"),
            "no_new_nullable defaults to false and must stay inert then: {diags:?}"
        );
    }

    /// With no `[[columns]]` the rule has no baseline, so "new" is undefined.
    /// Refusing beats guessing: treating every nullable column as new is a
    /// surprise mass-failure, treating none as new is inert again.
    #[test]
    fn no_new_nullable_without_a_baseline_is_refused() {
        let schema = vec![typed_col("anything", RockyType::String, true)];
        let contract = CompilerContract {
            columns: vec![],
            rules: ContractRules {
                no_new_nullable: true,
                ..Default::default()
            },
        };

        let diags = validate_contract("m", &schema, &contract);
        let e014: Vec<_> = diags.iter().filter(|d| &*d.code == "E014").collect();
        assert_eq!(e014.len(), 1, "expected exactly one E014: {diags:?}");
        assert!(
            e014[0].message.contains("no baseline"),
            "the refusal must explain why: {:?}",
            e014[0].message
        );
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

    /// Validate one decimal column against one contract type string.
    fn decimal_diagnostics(precision: u8, scale: u8, contract_type: &str) -> Vec<Diagnostic> {
        let schema = vec![typed_col(
            "amount",
            RockyType::Decimal { precision, scale },
            false,
        )];

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "amount".to_string(),
                type_name: Some(contract_type.to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        validate_contract("test_model", &schema, &contract)
    }

    #[test]
    fn test_decimal_scale_mismatch_is_e011() {
        // The reported case: the contract pins Decimal(18,2), the model
        // produces Decimal(10,0). Neither digit matches.
        let diags = decimal_diagnostics(10, 0, "Decimal(18,2)");
        assert!(
            diags.iter().any(|d| &*d.code == "E011"),
            "Decimal(10,0) must not satisfy a Decimal(18,2) contract: {diags:?}"
        );
    }

    #[test]
    fn test_decimal_precision_widening_is_e011() {
        // Same scale, narrower precision. A "fits inside" rule would pass this;
        // a contract states the declared type, so it is a mismatch.
        let diags = decimal_diagnostics(10, 2, "Decimal(18,2)");
        assert!(
            diags.iter().any(|d| &*d.code == "E011"),
            "Decimal(10,2) must not satisfy a Decimal(18,2) contract: {diags:?}"
        );
    }

    #[test]
    fn test_decimal_exact_match_passes() {
        let diags = decimal_diagnostics(18, 2, "Decimal(18,2)");
        assert!(
            diags.iter().all(|d| &*d.code != "E011"),
            "Decimal(18,2) must satisfy a Decimal(18,2) contract: {diags:?}"
        );
    }

    #[test]
    fn test_bare_decimal_contract_matches_any_precision() {
        let diags = decimal_diagnostics(10, 0, "Decimal");
        assert!(
            diags.iter().all(|d| &*d.code != "E011"),
            "a bare `Decimal` contract must keep matching any precision: {diags:?}"
        );
    }

    #[test]
    fn test_decimal_type_spellings() {
        // `Decimal(p)` means scale 0, as the type checker reads `DECIMAL(p)`.
        assert!(decimal_type_matches(18, 0, "Decimal(18)"));
        assert!(!decimal_type_matches(18, 2, "Decimal(18)"));
        // Whitespace around the digits is accepted.
        assert!(decimal_type_matches(18, 2, "Decimal( 18 , 2 )"));
        // A parameter block that is not digits never matches.
        assert!(!decimal_type_matches(18, 2, "Decimal(18,2"));
        assert!(!decimal_type_matches(18, 2, "Decimal()"));
        assert!(!decimal_type_matches(18, 2, "Decimal(p,s)"));
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
