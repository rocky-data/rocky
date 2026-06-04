use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::column_map;
use rocky_ir::{ColumnInfo, RockyType, is_assignable};

/// Data contract configuration — enforced at copy/load time.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct RequiredColumn {
    pub name: String,
    /// Expected type, written in warehouse vocabulary (e.g. `BIGINT`,
    /// `VARCHAR`, `NUMBER(38,0)`). It is normalized to a portable Rocky type
    /// before comparison, so the same contract ports across warehouses
    /// (DuckDB `VARCHAR` and Snowflake `STRING` both match). A type the
    /// normalizer doesn't recognize is treated as unknown and never fails the
    /// type check — presence and nullability still apply.
    #[serde(rename = "type")]
    pub data_type: String,
    #[serde(default = "default_true")]
    pub nullable: bool,
}

fn default_true() -> bool {
    true
}

/// A permitted type widening (e.g., INT to BIGINT) that won't trigger a violation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AllowedTypeChange {
    pub from: String,
    pub to: String,
}

/// Result of contract validation.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct ContractResult {
    pub passed: bool,
    pub violations: Vec<ContractViolation>,
    /// Non-fatal warnings — e.g. a contract clause that can't be
    /// meaningfully enforced in this context. Does not affect `passed`.
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// A single contract rule violation with the rule name, affected column, and message.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
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

    // Check required columns (§P1.9: CiStr lookups avoid per-column alloc).
    for req in &contract.required_columns {
        let key = column_map::CiStr::new(&req.name);
        match source_map.get(key) {
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
        let key = column_map::CiStr::new(protected);
        if target_map.contains_key(key) && !source_map.contains_key(key) {
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

    // Check type changes against allowed list (§P1.9: cross-map lookup via
    // CiStr so the source map's key lifetime doesn't have to match the
    // target map's).
    for (name, source_col) in &source_map {
        if let Some(target_col) = target_map.get(column_map::CiStr::new(name.as_str())) {
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
        warnings: Vec::new(),
    }
}

/// Validate freshly-landed warehouse columns against a data contract,
/// comparing types in Rocky's portable type vocabulary.
///
/// This is the contract gate for `rocky load`: `landed_columns` come from a
/// live `DESCRIBE` of the staging table (raw warehouse type strings such as
/// DuckDB `VARCHAR` or Snowflake `NUMBER(38,0)`), so type comparison can't
/// be raw string equality if contracts are to port across warehouses.
///
/// Checks performed:
/// - **Presence** (fully portable): every `required_columns` entry must
///   exist in the landed data.
/// - **Nullability** (fully portable): a required column declared
///   `nullable = false` whose landed column is nullable is a violation.
/// - **Type match** (best-effort, widening-aware): both the landed raw type
///   and the contract's expected type string are normalized to [`RockyType`];
///   a violation is emitted only when both normalize to a *known* type and the
///   landed type is not assignable to (does not fit within) the expected type.
///   So a narrower landed type (`INT32`) satisfies a wider contract (`BIGINT`),
///   but not the reverse. If either side normalizes to [`RockyType::Unknown`],
///   the match is skipped (never a false failure).
///
/// `protected_columns` and `allowed_type_changes` describe source-vs-target
/// *evolution*, which a single-table load can't meaningfully evaluate (there
/// is no prior target snapshot in scope). When declared, they are surfaced
/// as warnings rather than silently ignored.
pub fn validate_contract_typed(
    contract: &ContractConfig,
    landed_columns: &[ColumnInfo],
) -> ContractResult {
    let mut violations = Vec::new();
    let mut warnings = Vec::new();

    let landed_map = column_map::build_column_map(landed_columns);

    for req in &contract.required_columns {
        let key = column_map::CiStr::new(&req.name);
        let Some(col) = landed_map.get(key) else {
            violations.push(ContractViolation {
                rule: "required_column".to_string(),
                column: req.name.clone(),
                message: format!("required column '{}' not found in loaded data", req.name),
            });
            continue;
        };

        // Nullability — fully portable.
        if !req.nullable && col.nullable {
            violations.push(ContractViolation {
                rule: "required_column_nullability".to_string(),
                column: req.name.clone(),
                message: format!(
                    "column '{}' is nullable but the contract requires it to be non-nullable",
                    req.name
                ),
            });
        }

        // Type match — best-effort in Rocky's type vocabulary.
        let landed_ty = warehouse_type_to_rocky(&col.data_type);
        let expected_ty = warehouse_type_to_rocky(&req.data_type);
        if !landed_type_conforms(&landed_ty, &expected_ty) {
            violations.push(ContractViolation {
                rule: "required_column_type".to_string(),
                column: req.name.clone(),
                message: format!(
                    "column '{}' has type '{}' ({landed_ty}), expected '{}' ({expected_ty})",
                    req.name, col.data_type, req.data_type
                ),
            });
        }
    }

    if !contract.protected_columns.is_empty() {
        warnings.push(
            "`protected_columns` is declared but cannot be enforced on a load contract: \
             it compares source columns against a prior target snapshot, which is not \
             available when loading files. Ignored for this load."
                .to_string(),
        );
    }
    if !contract.allowed_type_changes.is_empty() {
        warnings.push(
            "`allowed_type_changes` is declared but cannot be enforced on a load contract: \
             it gates type evolution against a prior target snapshot, which is not available \
             when loading files. Ignored for this load."
                .to_string(),
        );
    }

    ContractResult {
        passed: violations.is_empty(),
        violations,
        warnings,
    }
}

/// Normalize a raw warehouse type string into a [`RockyType`].
///
/// Mirrors the compiler's `default_type_mapper` so the load gate compares
/// types in the same vocabulary the rest of Rocky uses. Lives in rocky-core
/// (not rocky-compiler) so the runtime load path can reach it without a
/// dependency cycle; [`RockyType`] is shared via rocky-ir.
///
/// Unrecognized types map to [`RockyType::Unknown`] (which matches anything),
/// so a type the map doesn't cover never produces a false failure.
pub fn warehouse_type_to_rocky(warehouse_type: &str) -> RockyType {
    let upper = warehouse_type.trim().to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => RockyType::Boolean,
        "TINYINT" | "BYTE" | "SMALLINT" | "SHORT" | "INT" | "INTEGER" => RockyType::Int32,
        "BIGINT" | "LONG" => RockyType::Int64,
        "FLOAT" | "REAL" => RockyType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" => RockyType::Float64,
        "STRING" | "VARCHAR" | "TEXT" => RockyType::String,
        "BINARY" => RockyType::Binary,
        "DATE" => RockyType::Date,
        "TIMESTAMP" => RockyType::Timestamp,
        "TIMESTAMP_NTZ" => RockyType::TimestampNtz,
        "VARIANT" => RockyType::Variant,
        // DECIMAL / NUMERIC (ANSI, Databricks, BigQuery) and NUMBER
        // (Snowflake's fixed-point name). Snowflake's `DESCRIBE` returns
        // `NUMBER(38,0)`, so it must normalize to the same RockyType as a
        // contract written `DECIMAL(38,0)` for the contract to port.
        _ if upper.starts_with("DECIMAL")
            || upper.starts_with("NUMERIC")
            || upper.starts_with("NUMBER") =>
        {
            if let Some(params) = upper
                .strip_prefix("DECIMAL(")
                .or_else(|| upper.strip_prefix("NUMERIC("))
                .or_else(|| upper.strip_prefix("NUMBER("))
                .and_then(|s| s.strip_suffix(')'))
            {
                let parts: Vec<&str> = params.split(',').collect();
                if parts.len() == 2
                    && let (Ok(p), Ok(s)) = (parts[0].trim().parse(), parts[1].trim().parse())
                {
                    return RockyType::Decimal {
                        precision: p,
                        scale: s,
                    };
                }
            }
            RockyType::Decimal {
                precision: 38,
                scale: 0,
            }
        }
        _ => RockyType::Unknown,
    }
}

/// Compare two normalized [`RockyType`]s for contract purposes.
///
/// Returns `true` (a match, so no violation) when either side is
/// [`RockyType::Unknown`] — an un-normalizable type is never a false
/// failure. Otherwise compares by equality.
/// Whether a landed column type conforms to a contract's expected type: the
/// landed type must be *assignable to* (fit within) the expected type, so a
/// narrower landed type (e.g. `INT32`) satisfies a wider contract (`BIGINT`),
/// but a wider landed type does not satisfy a narrower contract. `Unknown` on
/// either side passes (never a false failure) — `is_assignable` handles both.
fn landed_type_conforms(landed: &RockyType, expected: &RockyType) -> bool {
    is_assignable(landed, expected)
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

    // --- Typed (load-gate) validation path ---

    fn col_n(name: &str, data_type: &str, nullable: bool) -> ColumnInfo {
        ColumnInfo {
            name: name.to_string(),
            data_type: data_type.to_string(),
            nullable,
        }
    }

    #[test]
    fn test_warehouse_type_to_rocky() {
        assert_eq!(warehouse_type_to_rocky("VARCHAR"), RockyType::String);
        assert_eq!(warehouse_type_to_rocky("varchar"), RockyType::String);
        assert_eq!(warehouse_type_to_rocky("BIGINT"), RockyType::Int64);
        assert_eq!(warehouse_type_to_rocky("BOOLEAN"), RockyType::Boolean);
        assert_eq!(
            warehouse_type_to_rocky("NUMBER(38,0)"),
            RockyType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(
            warehouse_type_to_rocky("SOMETHING_WEIRD"),
            RockyType::Unknown
        );
    }

    #[test]
    fn test_typed_required_present_and_type_matches() {
        // Landed types mirror DuckDB DESCRIBE output (BIGINT, VARCHAR).
        let contract = ContractConfig {
            required_columns: vec![
                RequiredColumn {
                    name: "id".into(),
                    data_type: "BIGINT".into(),
                    nullable: true,
                },
                RequiredColumn {
                    name: "name".into(),
                    data_type: "VARCHAR".into(),
                    nullable: true,
                },
            ],
            ..Default::default()
        };
        let landed = vec![col_n("id", "BIGINT", true), col_n("name", "VARCHAR", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
    }

    #[test]
    fn test_typed_required_missing() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("name", "VARCHAR", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column");
    }

    #[test]
    fn test_typed_wrong_type_both_known() {
        // Landed BIGINT (Int64) vs contract VARCHAR (String) — both known, differ.
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "VARCHAR".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("id", "BIGINT", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column_type");
    }

    #[test]
    fn test_typed_portable_aliases_match() {
        // Contract written as Rocky-ish "STRING" must match DuckDB's "VARCHAR"
        // because both normalize to RockyType::String — the portability point.
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "name".into(),
                data_type: "STRING".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("name", "VARCHAR", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
    }

    #[test]
    fn test_typed_widening_narrower_landed_satisfies_wider_contract() {
        // Landed INT (Int32) satisfies a BIGINT (Int64) contract — INT widens
        // to BIGINT. (Databricks `read_files` infers small CSV ints as INT, so
        // a natural BIGINT contract must accept the inferred INT.)
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("id", "INT", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
    }

    #[test]
    fn test_typed_narrowing_wider_landed_fails() {
        // Landed BIGINT (Int64) does NOT satisfy an INT (Int32) contract —
        // narrowing could overflow, so it stays a violation.
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "INT".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("id", "BIGINT", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column_type");
    }

    #[test]
    fn test_typed_unknown_type_never_fails() {
        // An un-normalizable landed type must not produce a type violation.
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "geo".into(),
                data_type: "BIGINT".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("geo", "GEOMETRY", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
    }

    #[test]
    fn test_typed_nullability_violation() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("id", "BIGINT", true)]; // landed nullable
        let result = validate_contract_typed(&contract, &landed);
        assert!(!result.passed);
        assert_eq!(result.violations[0].rule, "required_column_nullability");
    }

    #[test]
    fn test_typed_protected_and_type_changes_warn() {
        let contract = ContractConfig {
            required_columns: vec![],
            protected_columns: vec!["id".into()],
            allowed_type_changes: vec![AllowedTypeChange {
                from: "INT".into(),
                to: "BIGINT".into(),
            }],
        };
        let landed = vec![col_n("id", "BIGINT", true)];
        let result = validate_contract_typed(&contract, &landed);
        // Unenforceable clauses must surface as warnings, not silently no-op,
        // and must not fail the contract.
        assert!(result.passed);
        assert_eq!(result.warnings.len(), 2);
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("protected_columns"))
        );
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("allowed_type_changes"))
        );
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
