//! Compile-time contract validation.
//!
//! Validates inferred model schemas against `.contract.toml` files at compile time,
//! catching issues like missing columns, type mismatches, and nullability violations
//! before warehouse execution.
//!
//! This complements the runtime contract validation in `rocky_core::contracts`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::diagnostic::{Diagnostic, E010, E011, E012, E013, E014, E059, I003, W010};
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

/// The project contracts directory that belongs to a models directory.
///
/// Same convention as `functions/` and `macros/`: a `contracts/` directory
/// beside the models directory, which in the usual layout is the directory
/// that holds `rocky.toml`. Every compile that is not given an explicit
/// contracts directory reads this one, so `rocky compile`, `rocky ci`,
/// `rocky test`, `rocky run` (every shape, `--dag` included) and the language
/// server check the same contracts.
///
/// Returns `None` when the directory does not exist, and for an empty
/// `models_dir` (a compile over preloaded models), so a caller never
/// resolves `../contracts` against the process working directory.
#[must_use]
pub fn project_contracts_dir_for(models_dir: &Path) -> Option<PathBuf> {
    if models_dir.as_os_str().is_empty() {
        return None;
    }
    let dir = models_dir.join("../contracts");
    dir.is_dir().then_some(dir)
}

/// Load contracts from a directory.
///
/// Each file named `{model_name}.contract.toml` defines a contract for that model.
pub fn load_contracts(dir: &Path) -> Result<HashMap<String, CompilerContract>, String> {
    let mut contracts = HashMap::new();

    let entries = std::fs::read_dir(dir).map_err(|e| {
        let why = if e.kind() == std::io::ErrorKind::NotFound {
            match rocky_core::path_presence::classify_not_found(dir) {
                rocky_core::path_presence::PathPresence::Present { detail } => detail,
                rocky_core::path_presence::PathPresence::Absent => e.to_string(),
            }
        } else {
            e.to_string()
        };
        format!(
            "failed to read explicit contracts directory {}: {why}",
            dir.display()
        )
    })?;

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

/// The path of `model_name`'s contract in a contracts directory.
#[must_use]
pub fn contract_file_in(dir: &Path, model_name: &str) -> PathBuf {
    dir.join(format!("{model_name}.contract.toml"))
}

/// Load the project `contracts/` directory for one compile.
///
/// Returns each in-scope model's contract with the file it was read from.
/// `in_scope` names the models the compile includes. One directory serves
/// every pipeline, so a file for a model outside the compile is skipped. If
/// that file does not parse, Rocky logs a warning and goes on: the compile
/// that includes the model reports it. A file for an in-scope model that
/// cannot be read or parsed is an error.
pub fn load_project_contracts(
    dir: &Path,
    in_scope: impl Fn(&str) -> bool,
) -> Result<HashMap<String, (PathBuf, CompilerContract)>, String> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| format!("failed to read contracts directory {}: {e}", dir.display()))?;
    let mut contracts = HashMap::new();
    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();
        let Some(model_name) = path
            .file_name()
            .and_then(|n| n.to_str())
            .and_then(|n| n.strip_suffix(".contract.toml"))
            .map(str::to_string)
        else {
            continue;
        };
        let parsed = std::fs::read_to_string(&path)
            .map_err(|e| format!("failed to read {}: {e}", path.display()))
            .and_then(|content| {
                toml::from_str::<CompilerContract>(&content)
                    .map_err(|e| format!("failed to parse {}: {e}", path.display()))
            });
        if !in_scope(&model_name) {
            if let Err(error) = parsed {
                tracing::warn!(
                    model = %model_name,
                    "skipping a contract for a model outside this compile: {error}"
                );
            }
            continue;
        }
        contracts.insert(model_name, (path, parsed?));
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
            // The loader hands on a contract that IS there (#1817), so a
            // `NotFound` here is a dangling link, not a missing file — say
            // so, rather than "No such file or directory" about a path the
            // operator can see in a listing.
            let content = std::fs::read_to_string(contract_path).map_err(|e| {
                let why = if e.kind() == std::io::ErrorKind::NotFound {
                    match rocky_core::path_presence::classify_not_found(contract_path) {
                        rocky_core::path_presence::PathPresence::Present { detail } => detail,
                        rocky_core::path_presence::PathPresence::Absent => e.to_string(),
                    }
                } else {
                    e.to_string()
                };
                format!("failed to read {}: {why}", contract_path.display())
            })?;
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
    validate_contract_with(model_name, inferred_schema, contract, None)
}

/// Why a contract column's type is unknown, given the column name. Passed to
/// [`validate_contract_with`] to turn an unchecked declared type into the
/// [`E059`] error (`--strict-contracts`).
pub type UnknownTypeReason<'a> = &'a dyn Fn(&str) -> String;

/// [`validate_contract`] with an optional strict mode.
///
/// With `strict_reason` set, a column whose contract declares a type but
/// whose inferred type is `Unknown` is an [`E059`] error instead of the
/// [`I003`] info note. The callback explains why the type is unknown.
pub fn validate_contract_with(
    model_name: &str,
    inferred_schema: &[TypedColumn],
    contract: &CompilerContract,
    strict_reason: Option<UnknownTypeReason<'_>>,
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
                // `Unknown` is handled here, before the matcher runs, and not
                // by the matcher. Until #1240 the skip was spread across two
                // places that each fail open on their own: this condition
                // (`&& col.data_type != RockyType::Unknown`) and
                // `type_name_matches`, whose `Unknown` arm returned `true`.
                // Closing one alone would have left the other as the live
                // path. The branch below is now the only place `Unknown`
                // decides anything, and `type_name_matches` no longer treats
                // it as a match.
                if let Some(ref expected_type) = contract_col.type_name {
                    if col.data_type == RockyType::Unknown
                        && let Some(reason) = strict_reason
                    {
                        diagnostics.push(
                            Diagnostic::error(
                                E059,
                                model_name,
                                format!(
                                    "column '{col}' of model '{model_name}': the contract declares type \
                                     {expected_type}, but Rocky cannot work out the column's type, so the \
                                     declared type cannot be checked ({why})",
                                    col = contract_col.name,
                                    why = reason(&contract_col.name),
                                ),
                            )
                            .with_suggestion(format!(
                                "cast `{0}` to a fully specified type in the SELECT (not a bare DECIMAL; the \
                                 cast is then checked against the contract's {1}), or give the compiler source \
                                 schemas so the type resolves (`data/seed.sql`, or \
                                 `rocky discover --with-schemas`), or drop the type from the \
                                 contract. `--strict-contracts` refuses a declared type Rocky \
                                 cannot check",
                                contract_col.name, expected_type
                            )),
                        );
                    } else if col.data_type == RockyType::Unknown {
                        // Report, don't fail: see `I003` for why this is info
                        // severity and not a warning.
                        diagnostics.push(
                            Diagnostic::info(
                                I003,
                                model_name,
                                format!(
                                    "column '{}' declares type {} in the contract, but Rocky could not \
                                     work out the column's type, so it did not check the declared type",
                                    contract_col.name, expected_type
                                ),
                            )
                            // A CAST to a fully specified type does clear
                            // this: the cast's target is the column's type
                            // whatever the input is. A bare DECIMAL names no
                            // digits and stays Unknown, so the advice below
                            // says "fully specified" (#1721).
                            .with_suggestion(format!(
                                "give the compiler source schemas so `{0}`'s type resolves — \
                                 `rocky compile`, `rocky test` and `rocky ci` read them from \
                                 `data/seed.sql` when the project has one; for a replication \
                                 pipeline, `rocky discover --with-schemas` fills the schema \
                                 cache (it refuses transformation-only pipelines). Or cast \
                                 `{0}` to a fully specified type in the SELECT (not a bare \
                                 DECIMAL): the cast's target is then checked against {1}",
                                contract_col.name, expected_type
                            )),
                        );
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
///
/// Callers must handle [`RockyType::Unknown`] before calling: an inferred type
/// Rocky withheld is neither a match nor a mismatch, and this function answers
/// "not a match" so a caller that forgets cannot pass a contract by accident.
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
        // Not a match. `Unknown` means "Rocky withheld a type", which is not
        // evidence that the declared type is right. `validate_contract` never
        // reaches this arm — it branches on `Unknown` first and reports `I003`
        // — so this value only decides what a future caller sees, and the safe
        // answer there is "unverified", not "fine" (#1240).
        RockyType::Unknown => false,
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
    use crate::diagnostic::Severity;

    #[test]
    fn project_contracts_dir_for_is_the_models_sibling_when_present() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).expect("models dir");
        assert_eq!(project_contracts_dir_for(&models), None, "absent directory");
        assert_eq!(
            project_contracts_dir_for(Path::new("")),
            None,
            "no models dir"
        );
        std::fs::write(tmp.path().join("contracts"), "not a directory").expect("file");
        assert_eq!(
            project_contracts_dir_for(&models),
            None,
            "a file is not a directory"
        );
        std::fs::remove_file(tmp.path().join("contracts")).expect("remove file");
        std::fs::create_dir(tmp.path().join("contracts")).expect("contracts dir");
        assert_eq!(
            project_contracts_dir_for(&models),
            Some(models.join("../contracts"))
        );
    }

    fn typed_col(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    /// The one compile this change can newly fail (#1646), asserted rather
    /// than described. A source column the warehouse reports as `DECIMAL(10)`
    /// used to reach the compiler as `Decimal(38,0)`, so a contract declaring
    /// `Decimal(38,0)` matched it. `DECIMAL(10)` means precision 10 and scale
    /// 0 — how SQL reads it, and how this file's `decimal_type_matches`
    /// already read a contract written `Decimal(10)` — so it is now an
    /// `E011`. The contract was never enforcing the 38 digits it named.
    #[test]
    fn precision_only_source_decimal_no_longer_satisfies_a_38_digit_contract() {
        use crate::compile::default_type_mapper;

        let inferred = default_type_mapper("DECIMAL(10)");
        assert_eq!(
            inferred,
            RockyType::Decimal {
                precision: 10,
                scale: 0
            },
            "the mapper must read the digits the string names"
        );

        let schema = vec![typed_col("amount", inferred.clone(), true)];
        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "amount".to_string(),
                type_name: Some("Decimal(38,0)".to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let diags = validate_contract("m", &schema, &contract);
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert_eq!(&*diags[0].code, E011);
        assert_eq!(diags[0].severity, Severity::Error);

        // The same contract over a source column that really is 38 digits
        // still passes, so this is a narrower reading, not a broken one.
        let wide = vec![typed_col(
            "amount",
            default_type_mapper("DECIMAL(38,0)"),
            true,
        )];
        assert!(validate_contract("m", &wide, &contract).is_empty());
    }

    /// A bare `DECIMAL` or `NUMERIC` source column is now unread, so the
    /// contract gate says "not checked" (`I003`) instead of comparing it to a
    /// fabricated `Decimal(38,0)`. This direction only ever softens a
    /// diagnostic: an `E011` becomes an `I003` (#1646).
    #[test]
    fn bare_source_decimal_reports_i003_not_a_fabricated_match() {
        use crate::compile::default_type_mapper;

        for bare in ["DECIMAL", "NUMERIC"] {
            let schema = vec![typed_col("amount", default_type_mapper(bare), true)];
            let contract = CompilerContract {
                columns: vec![ContractColumn {
                    name: "amount".to_string(),
                    type_name: Some("Decimal(38,0)".to_string()),
                    nullable: None,
                    description: None,
                }],
                rules: ContractRules::default(),
            };

            let diags = validate_contract("m", &schema, &contract);
            assert_eq!(diags.len(), 1, "{bare}: {diags:?}");
            assert_eq!(&*diags[0].code, I003, "{bare}");
            assert_ne!(
                diags[0].severity,
                Severity::Error,
                "{bare}: an unread type must not fail the compile"
            );
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

    /// An inferred `Unknown` still does not fail the build — but it is no
    /// longer silent. Before #1240 this test was named
    /// `test_unknown_type_passes` and asserted only the absence of `E011`,
    /// which pinned the fail-open: any declared type passed and the user was
    /// never told the check had not run.
    #[test]
    fn test_unknown_type_reports_i003_instead_of_passing_silently() {
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

        // Still not an error: an unresolvable type must not fail a build.
        assert!(
            diags.iter().all(|d| &*d.code != "E011"),
            "an unresolved type must not produce E011: {diags:?}"
        );

        let i003 = diags
            .iter()
            .find(|d| &*d.code == "I003")
            .expect("I003 must report the unchecked contract type");
        assert_eq!(i003.severity, Severity::Info);
        assert!(
            i003.message.contains("'id'") && i003.message.contains("Int64"),
            "I003 must name the column and the declared type: {i003:?}"
        );
        assert!(
            i003.suggestion.is_some(),
            "I003 must say how to make the check run: {i003:?}"
        );
    }

    /// Strict contracts: the same unchecked type is the `E059` error, with the
    /// model, the column, the reason and the fix in it. A type that resolved
    /// stays a plain `E011` or a pass, and a column with no declared type is
    /// not a type claim at all.
    #[test]
    fn test_strict_turns_i003_into_e059() {
        let contract = |type_name: Option<&str>| CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: type_name.map(str::to_string),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let why =
            |column: &str| format!("it reads `raw.t.{column}` and Rocky has no schema for `raw.t`");
        let unknown = vec![typed_col("id", RockyType::Unknown, true)];

        let diags = validate_contract_with("m", &unknown, &contract(Some("Int64")), Some(&why));
        assert!(diags.iter().all(|d| &*d.code != "I003"), "{diags:?}");
        let e059 = diags
            .iter()
            .find(|d| &*d.code == "E059")
            .unwrap_or_else(|| panic!("expected E059, got {diags:?}"));
        assert_eq!(e059.severity, Severity::Error);
        assert_eq!(e059.model, "m");
        for needle in ["column 'id'", "model 'm'", "Int64", "raw.t.id"] {
            assert!(e059.message.contains(needle), "{needle}: {}", e059.message);
        }
        let fix = e059.suggestion.as_deref().unwrap();
        assert!(
            fix.contains("cast") && fix.contains("source schemas"),
            "{fix}"
        );

        // Not strict: unchanged.
        let diags = validate_contract("m", &unknown, &contract(Some("Int64")));
        assert!(diags.iter().any(|d| &*d.code == "I003"), "{diags:?}");
        assert!(diags.iter().all(|d| &*d.code != "E059"), "{diags:?}");

        // Strict, type resolved: a pass or an E011, never E059.
        let known = vec![typed_col("id", RockyType::Int64, false)];
        let diags = validate_contract_with("m", &known, &contract(Some("Int64")), Some(&why));
        assert!(diags.is_empty(), "{diags:?}");
        let diags = validate_contract_with("m", &known, &contract(Some("String")), Some(&why));
        assert!(diags.iter().any(|d| &*d.code == "E011"), "{diags:?}");

        // Strict, no declared type: nothing to check.
        let diags = validate_contract_with("m", &unknown, &contract(None), Some(&why));
        assert!(diags.is_empty(), "{diags:?}");
    }

    /// The second half of the #1240 fail-open. The gate branches on `Unknown`
    /// before calling the matcher, so this arm is unreachable from
    /// `validate_contract` today — it is pinned so a future caller that skips
    /// the branch gets "not a match" rather than a silent pass.
    #[test]
    fn test_type_name_matches_does_not_accept_unknown() {
        assert!(!type_name_matches(&RockyType::Unknown, "Int64"));
        assert!(!type_name_matches(&RockyType::Unknown, "Boolean"));
        assert!(!type_name_matches(&RockyType::Unknown, "Decimal"));
    }

    /// A contract column with no `type` declared is not a type claim, so it
    /// must stay silent — `I003` reports an *unchecked declaration*, not an
    /// unresolved column.
    #[test]
    fn test_unknown_type_without_a_declared_type_is_silent() {
        let schema = vec![typed_col("id", RockyType::Unknown, false)];

        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: None,
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        let diags = validate_contract("test_model", &schema, &contract);
        assert!(
            diags.is_empty(),
            "no type declared, nothing to report: {diags:?}"
        );
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
