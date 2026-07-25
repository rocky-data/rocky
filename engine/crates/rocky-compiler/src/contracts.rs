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

    // Whether *nothing* in this model resolved. Measured, not assumed: with
    // `[cache.schemas]` disabled or a cold cache the compiler is handed an empty
    // `source_schemas`, every leaf column types as `Unknown`, and so does
    // everything downstream of it — both of this repository's fixture POCs are
    // in exactly that state, every column, and so is any project compiled
    // without a live warehouse schema.
    //
    // That is a *project-configuration* fact, not a fact about any one column,
    // and it is the only thing this flag is used for: the per-column reports
    // below collapse into one model-level report naming the same columns. It is
    // **not** a suppression — see `collapsed_unverifiable`.
    let every_column_unresolved = inferred_schema
        .iter()
        .all(|c| c.data_type == RockyType::Unknown);
    // Columns whose declared type could not be checked, when the cause is
    // project-wide. Collected here and reported once, after the loop.
    let mut collapsed_unverifiable: Vec<&str> = Vec::new();

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
                        // string-declared type. It fires for **every** declared
                        // type, not just a declared decimal.
                        //
                        // Scoping it to declared decimals is what round 2
                        // shipped, and it was a fail-open: `default_type_mapper`
                        // now resolves a bare `decimal` (what Databricks
                        // `information_schema` returns for every decimal source
                        // column) to `Unknown`, so a contract declaring `Int64`
                        // / `String` / `Timestamp` for such a column produced
                        // `E011` before this branch existed and produced
                        // *nothing at all* after it. A check that used to run
                        // must not stop running.
                        if every_column_unresolved {
                            // Deferred, not dropped — reported once per model
                            // below.
                            collapsed_unverifiable.push(&contract_col.name);
                        } else {
                            let message = format!(
                                "column '{}' has no inferred type, so the contract's expected \
                                 type '{expected_type}' could not be verified — unresolved \
                                 evidence does not satisfy a contract",
                                contract_col.name
                            );
                            let suggestion = unresolved_remedy(&contract_col.name);
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

    // The collapsed form of the same verdict: one report per model instead of
    // one per contracted column, emitted when *no* column in the model
    // resolved.
    //
    // # Why collapse rather than suppress
    //
    // Suppressing here was measured and rejected: both of this repository's
    // fixture POCs, and every project compiled without source schemas, have no
    // resolved column at all, so suppression would make this gate report
    // nothing in any of them — trading one silence for another. The verdict is
    // the same verdict; only its granularity changes, and the columns it stands
    // in for are named so it stays actionable.
    //
    // # What this becomes at the flip
    //
    // Under [`UnverifiablePolicy::Reject`] this is one **error** per contracted
    // model, and every project that compiles without source schemas — every POC
    // in this repository included — fails its contracts. That is the correct
    // fail-closed reading (a contract cannot be checked against evidence that
    // was never produced), and it is a far larger migration than a per-column
    // warning suggests. It is named on [`crate::types::UNVERIFIABLE_POLICY`] as
    // flip-time work.
    if !collapsed_unverifiable.is_empty() {
        let names = collapsed_unverifiable
            .iter()
            .map(|n| format!("'{n}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let count = collapsed_unverifiable.len();
        let message = format!(
            "{count} contracted column(s) ({names}) could not be type-checked: no column in \
             this model has an inferred type, so no expected type could be verified — \
             unresolved evidence does not satisfy a contract"
        );
        let suggestion = unresolved_remedy_project();
        diagnostics.push(match policy {
            UnverifiablePolicy::Warn => Diagnostic::warning(
                W032,
                model_name,
                format!("{message}. Warning in this release; error in the next"),
            )
            .with_suggestion(suggestion),
            UnverifiablePolicy::Reject => {
                Diagnostic::error(E037, model_name, message).with_suggestion(suggestion)
            }
        });
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

/// The remedy for one unresolved contracted column, **in the order that
/// actually works**.
///
/// The order is measured, not stylistic. `enhanced_inference` applies a cast's
/// target type only when the value being cast already has a known type (the
/// `input_is_known` gate in `typecheck.rs`), so a `CAST` does *not* repair a
/// column whose source type never resolved. Probed on this branch, with the
/// model `SELECT CAST(amount AS DECIMAL(10,2)) AS amount, id FROM src.raw.t`:
///
/// ```text
/// source `amount` described as "decimal"       -> model `amount` = Unknown
/// source `amount` described as "DECIMAL(38,0)" -> model `amount` = Decimal(10,2)
/// SELECT CAST(1 AS DECIMAL(10,2)) AS lit       -> `lit` = Unknown
/// ```
///
/// The previous wording led with the cast, which is false for exactly the
/// population that receives this diagnostic.
fn unresolved_remedy(column: &str) -> String {
    format!(
        "resolve the source type first — run `rocky discover --with-schemas` (or any `rocky \
         run`) to warm the schema cache this compile reads. A common cause is a source that \
         reports a bare `DECIMAL` with no precision or scale, which names a different type on \
         every warehouse and so resolves to none. An explicit `CAST({column} AS …)` only takes \
         effect once the value being cast already has a known type, so on its own it does not \
         repair a column whose source type never resolved."
    )
}

/// [`unresolved_remedy`] for the collapsed, model-level report — same ordering,
/// no single column to name.
fn unresolved_remedy_project() -> String {
    "no column in this model resolved, which is what compiling with no source schemas looks \
     like: run `rocky discover --with-schemas` (or any `rocky run`) to warm the schema cache \
     this compile reads, and check `[cache.schemas] enabled` is not `false`. An explicit `CAST` \
     only takes effect once the value being cast already has a known type, so on its own it \
     does not repair this."
        .to_string()
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

/// Warehouse type strings a source adapter can really hand the compiler, plus
/// the malformed spellings the decimal rewrite re-routed.
///
/// Shared by the two halves of the transition oracle — the mapper-level half in
/// [`crate::compile`] and the gate-level half below — so neither can drift onto
/// a narrower grid than the other. `decimal` in lower case is in the grid
/// because that is literally what Databricks `information_schema.columns
/// .data_type` returns for every decimal source column.
#[cfg(test)]
pub(crate) const TRANSITION_TYPE_STRINGS: &[&str] = &[
    "BIGINT",
    "INT",
    "SMALLINT",
    "STRING",
    "VARCHAR",
    "DOUBLE",
    "FLOAT",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TIMESTAMP_NTZ",
    "BINARY",
    "VARIANT",
    "DECIMAL",
    "decimal",
    "NUMERIC",
    "DECIMAL(10,2)",
    "DECIMAL (10,2)",
    "DECIMAL(38,0)",
    "DECIMAL(10)",
    "DECIMAL(300,2)",
    "DECIMAL(10,-2)",
    "DECIMAL(5,10)",
    "DECIMAL(x)",
    "DECIMAL(1,2,3)",
    "NUMERICAL",
    "DECIMALISH",
    "HUGEINT",
    "GEOMETRY",
];

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
        // A model with *something* resolved: the per-column report. The
        // all-unresolved model is the collapsed report and has its own test.
        let schema = vec![
            typed_col("total_revenue", RockyType::Unknown, false),
            typed_col("order_id", RockyType::Int64, false),
        ];

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

    /// **The inversion of the round-2 scoping**, and the regression test for the
    /// silence it caused.
    ///
    /// This test previously asserted that a contract declaring `Int64` /
    /// `String` / `Timestamp` over an unresolved column produced *nothing* under
    /// both policies, and called that "out of scope for this release". It was a
    /// fail-open, and it pinned one: `default_type_mapper` resolves the bare
    /// `decimal` that Databricks `information_schema` returns to `Unknown`, so
    /// those three declarations produced `E011` before the unverifiable branch
    /// existed and produced nothing after it. Measured on this branch, with the
    /// column typed from a bare `"decimal"`:
    ///
    /// ```text
    ///   contract declares Int64      pre-PR=[E011]  round-2=[]  <- the silence
    ///   contract declares String     pre-PR=[E011]  round-2=[]
    ///   contract declares Timestamp  pre-PR=[E011]  round-2=[]
    /// ```
    ///
    /// The declared type name is now irrelevant to *whether* the verdict is
    /// reported.
    #[test]
    fn test_unverifiable_report_covers_every_declared_type() {
        // One resolved column, so this is a per-column gap rather than a
        // project-wide one.
        let schema = vec![
            typed_col("id", RockyType::Unknown, false),
            typed_col("resolved", RockyType::Int64, false),
        ];
        let contract = |type_name: &str| CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: Some(type_name.to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };

        for declared in [
            "Decimal",
            "Decimal(38,2)",
            "Int64",
            "String",
            "Timestamp",
            "Boolean",
            "Array<String>",
        ] {
            let warned = validate_contract("m", &schema, &contract(declared));
            let w032 = warned
                .iter()
                .find(|d| &*d.code == W032)
                .unwrap_or_else(|| panic!("{declared} must be reported, got {warned:?}"));
            assert!(
                w032.message.contains(declared),
                "the report must name the declared type: {w032:?}"
            );
            // The remedy must lead with resolving the *source* type, because a
            // `CAST` provably does not repair an unresolved one — see
            // `unresolved_remedy`.
            let suggestion = w032.suggestion.as_deref().unwrap_or_default();
            assert!(
                suggestion.contains("rocky discover --with-schemas"),
                "the remedy must name the action that works: {w032:?}"
            );
            assert!(
                suggestion.find("resolve the source type") < suggestion.find("An explicit `CAST"),
                "the remedy must put the source type before the CAST: {w032:?}"
            );

            let rejected = validate_contract_with_policy(
                "m",
                &schema,
                &contract(declared),
                UnverifiablePolicy::Reject,
            );
            assert!(
                rejected.iter().any(|d| &*d.code == E037),
                "{declared} must reject at the flip, got {rejected:?}"
            );
        }
    }

    /// The collapsed form: when *no* column in the model resolved, the same
    /// verdict is reported once for the model rather than once per column.
    ///
    /// Not a suppression — suppressing here was measured and rejected. Both of
    /// this repository's fixture POCs have no resolved column at all (every
    /// column of every model), so a suppression would have made this gate report
    /// nothing in any in-repo project, trading one silence for another.
    #[test]
    fn test_unverifiable_report_collapses_when_nothing_resolved() {
        let schema = vec![
            typed_col("id", RockyType::Unknown, false),
            typed_col("amount", RockyType::Unknown, true),
        ];
        let contract = CompilerContract {
            columns: vec![
                ContractColumn {
                    name: "id".to_string(),
                    type_name: Some("Int64".to_string()),
                    nullable: None,
                    description: None,
                },
                ContractColumn {
                    name: "amount".to_string(),
                    type_name: Some("Decimal".to_string()),
                    nullable: None,
                    description: None,
                },
            ],
            rules: ContractRules::default(),
        };

        let warned = validate_contract("m", &schema, &contract);
        let reports: Vec<_> = warned.iter().filter(|d| &*d.code == W032).collect();
        assert_eq!(
            reports.len(),
            1,
            "one report for the model, not one per column: {warned:?}"
        );
        // It stands in for both columns, so it must name both.
        assert!(reports[0].message.contains("'id'"), "{:?}", reports[0]);
        assert!(reports[0].message.contains("'amount'"), "{:?}", reports[0]);
        assert!(reports[0].message.contains('2'), "{:?}", reports[0]);
        assert!(reports[0].suggestion.is_some());

        // The flip turns the collapsed report into a single error per model.
        let rejected =
            validate_contract_with_policy("m", &schema, &contract, UnverifiablePolicy::Reject);
        let errors: Vec<_> = rejected.iter().filter(|d| &*d.code == E037).collect();
        assert_eq!(errors.len(), 1, "{rejected:?}");
    }

    /// A contract column with no declared type is not a type check, so it is
    /// neither reported nor collapsed. Guards the collapse from counting
    /// columns that were never going to be checked.
    #[test]
    fn test_untyped_contract_column_is_not_collapsed() {
        let schema = vec![typed_col("id", RockyType::Unknown, false)];
        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "id".to_string(),
                type_name: None,
                nullable: Some(true),
                description: None,
            }],
            rules: ContractRules::default(),
        };
        let diags = validate_contract("m", &schema, &contract);
        assert!(diags.is_empty(), "{diags:?}");
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

    // ======================================================================
    // Transition oracle, gate-level half.
    //
    // The mapper-level half lives in `compile.rs` and pins *which type* each
    // string resolves to, old vs new. This half pins what that does to a
    // contract verdict, which is the thing a user actually sees.
    // ======================================================================

    /// `type_name_matches` exactly as it stood before the split, frozen.
    ///
    /// The only difference is the `Unknown` arm, which returned `true` —
    /// "Unknown matches anything". That arm is the entire pre-rewrite reason a
    /// column with no inferred type passed every contract silently.
    fn type_name_matches_frozen_old(rocky_type: &RockyType, type_name: &str) -> bool {
        match rocky_type {
            RockyType::Unknown => true,
            other => type_name_matches(other, type_name),
        }
    }

    /// The pre-rewrite compile-gate verdict for one (source type string,
    /// declared type name) pair: `Some(E011)` or `None`.
    fn frozen_old_gate_code(source_type: &str, declared: &str) -> Option<&'static str> {
        let ty = crate::compile::default_type_mapper_frozen_old(source_type);
        (!type_name_matches_frozen_old(&ty, declared)).then_some(E011)
    }

    /// Contract type names a user can write in a `.contract.toml`.
    const DECLARED_NAMES: &[&str] = &[
        "Int64",
        "Int32",
        "String",
        "Boolean",
        "Float64",
        "Date",
        "Timestamp",
        "Binary",
        "Decimal",
        "Decimal(10,2)",
        "Decimal(38,0)",
    ];

    fn new_gate_codes(source_type: &str, declared: &str) -> Vec<String> {
        // A resolved sibling column, so this exercises the per-column report
        // rather than the collapsed one. The collapsed path has its own test.
        let schema = vec![
            typed_col("c", crate::compile::default_type_mapper(source_type), true),
            typed_col("_resolved", RockyType::Int64, false),
        ];
        let contract = CompilerContract {
            columns: vec![ContractColumn {
                name: "c".to_string(),
                type_name: Some(declared.to_string()),
                nullable: None,
                description: None,
            }],
            rules: ContractRules::default(),
        };
        validate_contract("m", &schema, &contract)
            .iter()
            .map(|d| d.code.to_string())
            .collect()
    }

    /// **The invariant F1 restored, as a property over the whole grid:** a
    /// contract type check that used to run must not stop running.
    ///
    /// Stated executably: there is no (source type, declared type) pair for
    /// which the pre-rewrite gate produced `E011` and this gate produces
    /// nothing. Round 2 had 3 such pairs for *every* decimal source string —
    /// measured, with the column typed from Databricks' bare `decimal`:
    ///
    /// ```text
    ///   declares Int64      pre-rewrite=[E011]  round-2=[]   <- silent
    ///   declares String     pre-rewrite=[E011]  round-2=[]
    ///   declares Timestamp  pre-rewrite=[E011]  round-2=[]
    /// ```
    ///
    /// A property, not a list, because the list would have to be re-derived
    /// every time the grid grows — and because the failure mode being guarded
    /// is precisely "a case nobody thought to enumerate went quiet".
    #[test]
    fn test_compiler_gate_never_goes_silent_where_it_used_to_speak() {
        let mut regressions = Vec::new();
        for source_type in TRANSITION_TYPE_STRINGS {
            for declared in DECLARED_NAMES {
                let old = frozen_old_gate_code(source_type, declared);
                let new = new_gate_codes(source_type, declared);
                if old.is_some() && new.is_empty() {
                    regressions.push((*source_type, *declared));
                }
            }
        }
        assert!(
            regressions.is_empty(),
            "the pre-rewrite gate reported these and this one is silent: {regressions:?}"
        );
    }

    /// The gate-level transition table: every pair whose diagnostic changed,
    /// classified, with the whole grid asserted against the classification.
    ///
    /// Three transition classes are legitimate and each is checked by a rule
    /// rather than by an enumerated pair list, because the grid is 29 × 11:
    ///
    /// - `E011` → `W032`: the type stopped resolving, so a *mismatch* verdict
    ///   became an *unverifiable* one. The check still runs; it reports at
    ///   warning severity this release and rejects at the flip.
    /// - `∅` → `W032`: same cause, for a declaration the old concrete type
    ///   happened to satisfy. A new report, never a new permit.
    /// - `∅` → `E011` / `E011` → `∅` at *resolved* types: only `DECIMAL(10)`
    ///   and `DECIMAL (10,2)`, which now resolve to what they name.
    ///
    /// What must never appear is a fourth class: a pair that reported before and
    /// reports nothing now. That is asserted separately, as a property, by
    /// `test_compiler_gate_never_goes_silent_where_it_used_to_speak`.
    #[test]
    fn test_compiler_gate_transition_oracle() {
        let mut changed = Vec::new();
        for source_type in TRANSITION_TYPE_STRINGS {
            for declared in DECLARED_NAMES {
                let old = frozen_old_gate_code(source_type, declared);
                let new = new_gate_codes(source_type, declared);
                let old_v: Vec<String> = old.iter().map(|c| (*c).to_string()).collect();
                if old_v == new {
                    continue;
                }
                changed.push((*source_type, *declared, old_v.clone(), new.clone()));

                let resolves_now =
                    crate::compile::default_type_mapper(source_type) != RockyType::Unknown;
                if new == vec![W032.to_string()] {
                    assert!(
                        !resolves_now,
                        "{source_type}/{declared}: W032 is only for a type that does not resolve"
                    );
                } else {
                    assert!(
                        resolves_now,
                        "{source_type}/{declared}: an unresolved type can only produce W032, \
                         got {new:?}"
                    );
                    assert!(
                        matches!(source_type, &"DECIMAL(10)" | &"DECIMAL (10,2)"),
                        "{source_type}/{declared}: the only strings that changed *resolved* \
                         verdict are the two whose parameters are now read correctly, got \
                         {old_v:?} -> {new:?}"
                    );
                }
            }
        }
        assert!(
            !changed.is_empty(),
            "the oracle must observe the change it exists to bound"
        );
    }
}
