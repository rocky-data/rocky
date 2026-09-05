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
    /// normalizer does not recognise is never compared: the load gate reports
    /// the column in `ContractResult::warnings` instead, and the type check
    /// neither passes nor fails it — presence and nullability still apply.
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
    /// meaningfully enforced in this context, or a required column whose
    /// type Rocky could not compare because the landed or the declared type
    /// string is outside its type map. Does not affect `passed`.
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
///   but not the reverse. If either side normalizes to [`RockyType::Unknown`]
///   the column cannot be compared: it is reported in `warnings`, naming the
///   column and both raw type strings, and is neither a violation nor a
///   silent pass (#1614).
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
        //
        // `Unknown` is decided here, before the matcher runs. Until #1614 a
        // type string outside `warehouse_type_to_rocky`'s map — on either
        // side — became `Unknown`, and `is_assignable` treats `Unknown` as
        // assignable to and from anything, so the column satisfied whatever
        // the contract declared and staging was promoted with nothing said.
        // The same shape was closed on the compile-time gate in #1240 (I003).
        // This gate has no info channel, so the report goes to `warnings`:
        // it names the column and both raw type strings, and does not touch
        // `passed`, because "could not compare" is not "does not conform".
        let landed_ty = warehouse_type_to_rocky(&col.data_type);
        let expected_ty = warehouse_type_to_rocky(&req.data_type);
        let landed_unknown = landed_ty == RockyType::Unknown;
        let expected_unknown = expected_ty == RockyType::Unknown;
        if landed_unknown || expected_unknown {
            warnings.push(unchecked_type_warning(
                &req.name,
                &col.data_type,
                &req.data_type,
                landed_unknown,
                expected_unknown,
            ));
        } else if !landed_type_conforms(&landed_ty, &expected_ty) {
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
/// Shares the compiler's `default_type_mapper` vocabulary so the load gate
/// compares types the way the rest of Rocky does, and reads the decimal
/// family by the same grammar. It is not a full mirror: this function also
/// knows BigQuery's scalar names (`INT64`, `FLOAT64`, `BYTES`, `DATETIME`),
/// `BIGNUMERIC` and Snowflake's `NUMBER`, which `default_type_mapper` maps
/// to [`RockyType::Unknown`]. Bringing those names to the compiler would
/// give a source column a concrete type where it has none today, which can
/// turn an `I003` into an `E011`, so it is deliberately not done here
/// (#1646). Lives in rocky-core (not rocky-compiler) so the runtime load
/// path can reach it without a dependency cycle; [`RockyType`] is shared
/// via rocky-ir.
///
/// Unrecognized types map to [`RockyType::Unknown`]. [`validate_contract_typed`]
/// reports such a column in its warnings instead of comparing it, so a type
/// the map doesn't cover never produces a false failure — and, since #1614,
/// never passes silently either.
pub fn warehouse_type_to_rocky(warehouse_type: &str) -> RockyType {
    let upper = warehouse_type.trim().to_uppercase();
    match upper.as_str() {
        "BOOLEAN" | "BOOL" => RockyType::Boolean,
        "TINYINT" | "BYTE" | "SMALLINT" | "SHORT" | "INT" | "INTEGER" => RockyType::Int32,
        // `INT64` / `FLOAT64` / `BYTES` / `DATETIME` are BigQuery's scalar names.
        "BIGINT" | "LONG" | "INT64" => RockyType::Int64,
        "FLOAT" | "REAL" => RockyType::Float32,
        "DOUBLE" | "DOUBLE PRECISION" | "FLOAT64" => RockyType::Float64,
        "STRING" | "VARCHAR" | "TEXT" => RockyType::String,
        "BINARY" | "BYTES" => RockyType::Binary,
        "DATE" => RockyType::Date,
        "TIMESTAMP" => RockyType::Timestamp,
        // BigQuery `DATETIME` is a timezone-naive timestamp.
        "TIMESTAMP_NTZ" | "DATETIME" => RockyType::TimestampNtz,
        "VARIANT" => RockyType::Variant,
        // DECIMAL / NUMERIC (ANSI, Databricks, BigQuery) and NUMBER
        // (Snowflake's fixed-point name). Snowflake's `DESCRIBE` returns
        // `NUMBER(38,0)`, so it must normalize to the same RockyType as a
        // contract written `DECIMAL(38,0)` for the contract to port.
        _ => decimal_family_type(&upper),
    }
}

/// The decimal family: `DECIMAL`, `NUMERIC`, `BIGNUMERIC` (ANSI, Databricks,
/// BigQuery) and `NUMBER` (Snowflake).
///
/// Two spellings carry their own digits, so they mean the same thing on
/// every warehouse: `NAME(p,s)`, and `NAME(p)`, which means scale 0 exactly
/// as the compile-time gate reads `Decimal(p)`.
///
/// A *bare* name carries no digits. It only means something if the name
/// itself names one warehouse. `NUMBER` does: no other adapter Rocky ships
/// has that type, and on Snowflake it is `NUMBER(38, 0)` — see
/// `rocky-snowflake/src/adapter.rs`, where `INTEGER` is documented as an
/// "alias for `NUMBER(38, 0)`", and `rocky-snowflake/src/loader.rs`, which
/// writes `NUMBER(38,0)` for an inferred `BIGINT`.
///
/// `DECIMAL`, `NUMERIC` and `BIGNUMERIC` do not. This function takes no
/// dialect, and [`validate_contract_typed`] hands it both the landed type
/// (a live `DESCRIBE`) and the type a person wrote in a `.contract.toml`,
/// so one reading has to serve every warehouse. `DECIMAL` is `(18,3)` on
/// DuckDB and `(10,0)` on Databricks; `NUMERIC` is a legal spelling on
/// Snowflake too — `rocky-snowflake/src/types.rs` groups `NUMERIC` with
/// `NUMBER` and `DECIMAL` in one family — and it does not mean there what
/// it means on BigQuery. Choosing one warehouse's digits would invent a
/// type from a name, which is the thing this function stopped doing. So
/// they are [`RockyType::Unknown`] and the load gate reports the column
/// (#1646).
///
/// Anything else that starts with one of the names — `NUMBER(nope)`,
/// `DECIMAL(10,2,3)`, `NUMBERWANG` — is `Unknown` for the same reason.
/// Until #1614 every such string was read as `(38, 0)` and compared as if
/// that were true.
fn decimal_family_type(upper: &str) -> RockyType {
    const NAMES: [&str; 4] = ["DECIMAL", "BIGNUMERIC", "NUMERIC", "NUMBER"];
    for name in NAMES {
        if upper == name {
            // Snowflake's `NUMBER` is the one bare name a single warehouse
            // pins; the others disagree across warehouses, so they are not
            // read at all rather than read as one warehouse's answer.
            return if name == "NUMBER" {
                RockyType::Decimal {
                    precision: 38,
                    scale: 0,
                }
            } else {
                RockyType::Unknown
            };
        }
        // `DECIMAL (10,2)` is valid SQL, and a `.contract.toml` is written by
        // hand, so the space between the name and the parameters is trimmed
        // rather than making the type unreadable.
        let Some(params) = upper
            .strip_prefix(name)
            .map(str::trim_start)
            .and_then(|rest| rest.strip_prefix('('))
            .and_then(|rest| rest.strip_suffix(')'))
        else {
            continue;
        };
        // `NAME(p)` means scale 0, the same reading the compile-time gate
        // gives `Decimal(p)` (`rocky-compiler`'s `decimal_type_matches`).
        // The two gates must read one spelling the same way.
        let (precision, scale) = match params.split_once(',') {
            Some((precision, scale)) => (precision.trim(), scale.trim()),
            None => (params.trim(), "0"),
        };
        if let (Ok(precision), Ok(scale)) = (precision.parse(), scale.parse()) {
            return RockyType::Decimal { precision, scale };
        }
        return RockyType::Unknown;
    }
    RockyType::Unknown
}

/// Whether a landed column type conforms to a contract's expected type: the
/// landed type must be *assignable to* (fit within) the expected type, so a
/// narrower landed type (e.g. `INT32`) satisfies a wider contract (`BIGINT`),
/// but a wider landed type does not satisfy a narrower contract. Decimal
/// assignments must preserve fractional scale and integer-digit capacity.
///
/// `Unknown` on either side is *not* a match. [`validate_contract_typed`]
/// branches on `Unknown` before calling this, so that arm is unreachable
/// from there; it answers "no" so a future caller that forgets the branch
/// gets a violation, not a silent pass. (`is_assignable` itself treats
/// `Unknown` as assignable both ways — correct for the compiler's
/// best-effort inference, wrong for a gate.)
fn landed_type_conforms(landed: &RockyType, expected: &RockyType) -> bool {
    if *landed == RockyType::Unknown || *expected == RockyType::Unknown {
        return false;
    }
    is_assignable(landed, expected)
}

/// The warning for a required column whose type could not be compared.
/// Names the column and both raw type strings, and says which side Rocky
/// did not recognise, so the reader can fix the contract or extend the map.
fn unchecked_type_warning(
    column: &str,
    landed_type: &str,
    declared_type: &str,
    landed_unknown: bool,
    declared_unknown: bool,
) -> String {
    let which = match (landed_unknown, declared_unknown) {
        (true, true) => "Rocky recognises neither type",
        (true, false) => "Rocky does not recognise the landed type",
        (false, true) => "Rocky does not recognise the declared type",
        (false, false) => unreachable!("only called when at least one side is Unknown"),
    };
    format!(
        "column '{column}' landed as '{landed_type}' and the contract declares '{declared_type}'; \
         {which}, so the declared type was not checked. Presence and nullability were checked. \
         The load was not refused for this."
    )
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
        // BigQuery scalar names.
        assert_eq!(warehouse_type_to_rocky("INT64"), RockyType::Int64);
        assert_eq!(warehouse_type_to_rocky("FLOAT64"), RockyType::Float64);
        assert_eq!(warehouse_type_to_rocky("BYTES"), RockyType::Binary);
        assert_eq!(warehouse_type_to_rocky("DATETIME"), RockyType::TimestampNtz);
        assert!(matches!(
            warehouse_type_to_rocky("BIGNUMERIC(76,38)"),
            RockyType::Decimal { .. }
        ));
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
        assert!(
            result.warnings.is_empty(),
            "known, matching types must not warn: {:?}",
            result.warnings
        );
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
    fn test_typed_decimal_contract_checks_full_capacity() {
        for (landed_type, expected_type, should_pass) in [
            ("DECIMAL(10,2)", "DECIMAL(10,4)", false),
            ("DECIMAL(10,2)", "DECIMAL(12,4)", true),
            ("INTEGER", "DECIMAL(10,2)", false),
            ("INTEGER", "DECIMAL(12,2)", true),
            ("BIGINT", "DECIMAL(19,10)", false),
            ("BIGINT", "DECIMAL(29,10)", true),
        ] {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "amount".into(),
                    data_type: expected_type.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            let result = validate_contract_typed(&contract, &[col_n("amount", landed_type, true)]);

            assert_eq!(
                result.passed, should_pass,
                "landed {landed_type}, expected {expected_type}: {:?}",
                result.violations
            );
            if !should_pass {
                assert_eq!(result.violations[0].rule, "required_column_type");
            }
        }
    }

    /// A landed type outside Rocky's map cannot be compared, so it must not
    /// produce a type violation — and it must not pass silently either. Until
    /// #1614 this test pinned the fail-open: it asserted only `passed`.
    #[test]
    fn test_typed_unknown_landed_type_is_reported_not_passed_silently() {
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
        assert!(result.violations.is_empty());
        assert_eq!(
            result.warnings.len(),
            1,
            "exactly one warning for the unchecked column: {:?}",
            result.warnings
        );
        let w = &result.warnings[0];
        assert!(
            w.contains("'geo'") && w.contains("'GEOMETRY'") && w.contains("'BIGINT'"),
            "the warning must name the column, the landed type and the declared type: {w}"
        );
        assert!(
            w.contains("does not recognise the landed type"),
            "the warning must say which side was not recognised: {w}"
        );
    }

    /// The declared side has the same hole: a contract type string the
    /// normalizer does not know must be reported, not matched to anything.
    #[test]
    fn test_typed_unknown_declared_type_is_reported_not_passed_silently() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "id".into(),
                data_type: "SOMETHING_WEIRD".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("id", "BIGINT", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
        assert_eq!(result.warnings.len(), 1, "warnings: {:?}", result.warnings);
        let w = &result.warnings[0];
        assert!(
            w.contains("'id'") && w.contains("'SOMETHING_WEIRD'") && w.contains("'BIGINT'"),
            "the warning must name the column, the declared type and the landed type: {w}"
        );
        assert!(
            w.contains("does not recognise the declared type"),
            "the warning must say which side was not recognised: {w}"
        );
    }

    /// Both sides unrecognised is one unchecked column, so one warning.
    #[test]
    fn test_typed_both_types_unknown_reported_once() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "geo".into(),
                data_type: "GEOM".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("geo", "GEOMETRY", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(result.passed, "violations: {:?}", result.violations);
        assert_eq!(result.warnings.len(), 1, "warnings: {:?}", result.warnings);
        let w = &result.warnings[0];
        assert!(
            w.contains("'geo'") && w.contains("'GEOMETRY'") && w.contains("'GEOM'"),
            "the warning must name the column and both type strings: {w}"
        );
        assert!(
            w.contains("recognises neither type"),
            "the warning must say that neither side was recognised: {w}"
        );
    }

    /// The decimal family has a grammar. A string that starts with one of its
    /// names but is neither a bare name nor `NAME(p,s)` is not a type Rocky
    /// understands; until #1614 it was read as `DECIMAL(38,0)` and compared
    /// as if that were true. A bare name is only a type when one warehouse
    /// fixes its digits, which is `NUMBER` and nothing else (#1646).
    #[test]
    fn test_warehouse_type_to_rocky_decimal_family_grammar() {
        let d38 = RockyType::Decimal {
            precision: 38,
            scale: 0,
        };
        // Snowflake's `NUMBER` is the one bare name a single warehouse pins.
        for bare in ["NUMBER", "number", "  NUMBER  "] {
            assert_eq!(warehouse_type_to_rocky(bare), d38, "{bare}");
        }
        // The rest carry no digits and no single warehouse (#1646).
        for bare in ["DECIMAL", "NUMERIC", "BIGNUMERIC", "numeric", "decimal"] {
            assert_eq!(
                warehouse_type_to_rocky(bare),
                RockyType::Unknown,
                "bare {bare} names no digits and no one warehouse"
            );
        }
        for spelled in ["NUMERIC(10, 2)", "NUMERIC (10,2)", "  numeric(10,2)  "] {
            assert_eq!(
                warehouse_type_to_rocky(spelled),
                RockyType::Decimal {
                    precision: 10,
                    scale: 2
                },
                "{spelled}"
            );
        }
        // `NAME(p)` is scale 0, the reading the compile-time gate gives
        // `Decimal(p)`. The two gates must not disagree about one spelling.
        assert_eq!(
            warehouse_type_to_rocky("DECIMAL(10)"),
            RockyType::Decimal {
                precision: 10,
                scale: 0
            }
        );
        for malformed in [
            "NUMBER(nope)",
            "DECIMAL(10,2,3)",
            "NUMBERWANG",
            "DECIMALX",
            "NUMERIC()",
            "NUMBER(300,0)",
        ] {
            assert_eq!(
                warehouse_type_to_rocky(malformed),
                RockyType::Unknown,
                "{malformed} must not become a made-up decimal"
            );
        }
    }

    /// The bare BigQuery case (#1646). `INFORMATION_SCHEMA.COLUMNS.data_type`
    /// reports a default-precision column as a bare `NUMERIC` — the live
    /// sweep in `rocky-bigquery/tests/dialect_sweep_live.rs` asserts exactly
    /// that string after an `ALTER ... SET DATA TYPE NUMERIC`. Read as
    /// `DECIMAL(38,0)` it refused a correct load, because
    /// `is_assignable(Decimal(38,0), Decimal(38,9))` fails on integer digits.
    /// It is now unread, so the column is reported and the load promotes.
    #[test]
    fn test_typed_bare_numeric_is_reported_not_refused() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "NUMERIC(38,9)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let result = validate_contract_typed(&contract, &[col_n("amount", "NUMERIC", true)]);

        assert!(
            result.passed,
            "a bare NUMERIC must not refuse a NUMERIC(38,9) contract: {:?}",
            result.violations
        );
        assert!(result.violations.is_empty());
        assert_eq!(result.warnings.len(), 1, "{:?}", result.warnings);
        let w = &result.warnings[0];
        assert!(
            w.contains("'amount'") && w.contains("'NUMERIC'") && w.contains("'NUMERIC(38,9)'"),
            "the warning must name the column and both type strings: {w}"
        );
    }

    /// The other half of #1646: a contract written `NUMERIC(38,0)` used to
    /// accept a landed bare `NUMERIC`, which on BigQuery holds nine decimal
    /// places. It is still not refused — `passed` is computed from
    /// violations only — but it is no longer silent.
    #[test]
    fn test_typed_bare_numeric_against_narrow_contract_is_reported() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "NUMERIC(38,0)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let result = validate_contract_typed(&contract, &[col_n("amount", "NUMERIC", true)]);

        assert!(result.violations.is_empty());
        assert_eq!(
            result.warnings.len(),
            1,
            "the landed bare NUMERIC must be reported, not accepted silently: {:?}",
            result.warnings
        );
    }

    /// Honest-failure control for #1646: the exact strings the DuckDB,
    /// Databricks and Snowflake adapters emit from a live `DESCRIBE` today
    /// must still read as concrete decimals, so the new `Unknown` cannot
    /// fire on a healthy run. Sources for each string:
    /// `rocky-duckdb/src/adapter.rs` (`DECIMAL(10,2)`),
    /// `rocky-databricks/src/adapter.rs` (lowercase `decimal(10,2)`),
    /// `rocky-snowflake/src/loader.rs` and `src/dialect.rs` (`NUMBER(38,0)`),
    /// and BigQuery's parameterized spellings.
    #[test]
    fn test_healthy_describe_decimals_stay_concrete() {
        for (describe_output, precision, scale) in [
            ("DECIMAL(10,2)", 10, 2),
            ("decimal(10,2)", 10, 2),
            ("DECIMAL(18,3)", 18, 3),
            ("NUMBER(38,0)", 38, 0),
            ("NUMBER(19, 0)", 19, 0),
            ("NUMERIC(38,9)", 38, 9),
            ("BIGNUMERIC(76,38)", 76, 38),
        ] {
            assert_eq!(
                warehouse_type_to_rocky(describe_output),
                RockyType::Decimal { precision, scale },
                "{describe_output} is a live DESCRIBE string and must stay concrete"
            );
        }
    }

    /// A malformed decimal on the declared side is reported, not read as
    /// `DECIMAL(38,0)` and matched against the landed `NUMBER(38,0)`.
    #[test]
    fn test_typed_malformed_declared_decimal_is_reported() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "NUMBER(nope)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let result = validate_contract_typed(&contract, &[col_n("amount", "NUMBER(38,0)", true)]);
        assert!(result.passed, "violations: {:?}", result.violations);
        assert_eq!(result.warnings.len(), 1, "warnings: {:?}", result.warnings);
        let w = &result.warnings[0];
        assert!(
            w.contains("'NUMBER(nope)'") && w.contains("does not recognise the declared type"),
            "{w}"
        );
    }

    /// A malformed decimal on the landed side is reported too. Before #1614 it
    /// was read as `DECIMAL(38,0)` and failed a narrower contract for a made-up
    /// reason.
    #[test]
    fn test_typed_malformed_landed_decimal_is_reported() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "DECIMAL(12,4)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let result =
            validate_contract_typed(&contract, &[col_n("amount", "DECIMAL(10,2,3)", true)]);
        assert!(result.passed, "violations: {:?}", result.violations);
        assert_eq!(result.warnings.len(), 1, "warnings: {:?}", result.warnings);
        let w = &result.warnings[0];
        assert!(
            w.contains("'DECIMAL(10,2,3)'") && w.contains("does not recognise the landed type"),
            "{w}"
        );
    }

    /// The matcher must answer "not a match" for `Unknown` on either side.
    /// `validate_contract_typed` branches on `Unknown` before calling it, so
    /// this arm is unreachable from there today; it exists so a future caller
    /// that forgets the branch gets a violation, not a silent pass.
    #[test]
    fn test_landed_type_conforms_rejects_unknown_on_either_side() {
        assert!(!landed_type_conforms(
            &RockyType::Unknown,
            &RockyType::Int64
        ));
        assert!(!landed_type_conforms(
            &RockyType::Int64,
            &RockyType::Unknown
        ));
        assert!(!landed_type_conforms(
            &RockyType::Unknown,
            &RockyType::Unknown
        ));
        // Sanity: known widening still conforms.
        assert!(landed_type_conforms(&RockyType::Int32, &RockyType::Int64));
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
