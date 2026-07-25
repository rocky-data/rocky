use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::column_map;
use rocky_ir::{
    ColumnInfo, ContractVerdict, RockyType, UNVERIFIABLE_POLICY, UnverifiablePolicy,
    contract_verdict, validate_decimal_params,
};

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
///   and the contract's expected type string are normalized to [`RockyType`],
///   and the pair is decided by [`contract_verdict`]. A narrower landed type
///   (`INT32`) satisfies a wider contract (`BIGINT`), but not the reverse.
/// - **Unverifiable types** (the gate's third outcome): when either side
///   normalizes to [`RockyType::Unknown`] the containment question usually
///   cannot be answered. That is **not** a permit —
///   [`is_assignable`][rocky_ir::is_assignable] would say it is, which is the
///   fail-open this gate no longer has. Two things can happen:
///
///   1. The *family* the unresolved side names decides it anyway — no
///      `DECIMAL`, whatever its parameters, holds a `VARCHAR`. That is a
///      violation under both policies, now, and restores a rejection the
///      decimal rewrite dropped. See [`family_rules_out`].
///   2. Otherwise it is genuinely undecidable and [`UNVERIFIABLE_POLICY`]
///      applies: `Warn` reports `W032` and permits (the migration window),
///      `Reject` reports a `required_column_type_unverifiable` violation.
///
///   The *report* in (2) is scoped to the decimal family in this release — see
///   [`unverifiable_is_reportable`] for the conditions and for the residual
///   fail-open that scoping deliberately leaves open.
///
/// `protected_columns` and `allowed_type_changes` describe source-vs-target
/// *evolution*, which a single-table load can't meaningfully evaluate (there
/// is no prior target snapshot in scope). When declared, they are surfaced
/// as warnings rather than silently ignored.
pub fn validate_contract_typed(
    contract: &ContractConfig,
    landed_columns: &[ColumnInfo],
) -> ContractResult {
    validate_contract_typed_with_policy(contract, landed_columns, UNVERIFIABLE_POLICY)
}

/// [`validate_contract_typed`] with the unverifiable-type policy as an
/// argument.
///
/// Private on purpose: the shipping policy is the [`UNVERIFIABLE_POLICY`]
/// constant, and a per-call knob that disables a governance gate is the
/// false-permit-by-configuration this whole change exists to remove. The
/// argument exists so the *rejecting* arm is exercised by tests before it
/// ships, rather than being a code path nobody has ever run.
fn validate_contract_typed_with_policy(
    contract: &ContractConfig,
    landed_columns: &[ColumnInfo],
    policy: UnverifiablePolicy,
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

        match contract_verdict(&landed_ty, &expected_ty) {
            ContractVerdict::Satisfied => {}
            ContractVerdict::Violated => {
                // Distinguish a precision/scale containment failure from an
                // unrelated type mismatch, so the class of contracts affected by
                // the decimal containment rule is greppable. `contract_verdict`
                // returns a verdict by design and must not grow a reason
                // channel, so the discrimination happens here.
                let precision_failure = is_exact_numeric(&landed_ty)
                    && is_exact_numeric(&expected_ty)
                    && (landed_ty.is_decimal() || expected_ty.is_decimal());
                let rule = if precision_failure {
                    "required_column_type_precision"
                } else {
                    "required_column_type"
                };
                violations.push(ContractViolation {
                    rule: rule.to_string(),
                    column: req.name.clone(),
                    message: format!(
                        "column '{}' has type '{}' ({landed_ty}), expected '{}' ({expected_ty})",
                        req.name, col.data_type, req.data_type
                    ),
                });
            }
            // Unverifiable, but the *family* decides it anyway. Checked before
            // the reporting arm below, because this is a violation under both
            // policies — it is not waiting for the flip.
            ContractVerdict::Unverifiable
                if family_rules_out(&col.data_type, &landed_ty, &req.data_type, &expected_ty) =>
            {
                violations.push(ContractViolation {
                    // Deliberately the same rule the pre-split gate used for
                    // these pairs, because that is what this arm restores.
                    rule: "required_column_type".to_string(),
                    column: req.name.clone(),
                    message: format!(
                        "column '{}' has type '{}' ({landed_ty}), expected '{}' ({expected_ty}) — \
                         the unparameterised side names the fixed-point decimal family, and no \
                         decimal, whatever its precision and scale, satisfies this pair",
                        req.name, col.data_type, req.data_type
                    ),
                });
            }
            ContractVerdict::Unverifiable
                if unverifiable_is_reportable(&col.data_type, &req.data_type) =>
            {
                let detail = format!(
                    "column '{}' — contract type check not verified: landed type '{}' resolved \
                     to {landed_ty}, contract type '{}' resolved to {expected_ty}. Unresolved \
                     evidence does not satisfy a contract.",
                    req.name, col.data_type, req.data_type
                );
                let remedy = "Declare the decimal precision and scale on both sides \
                              (e.g. 'NUMBER(38,0)').";
                match policy {
                    UnverifiablePolicy::Warn => warnings.push(format!(
                        "W032: {detail} This is a warning in this release and becomes a \
                         violation in the next. {remedy}"
                    )),
                    UnverifiablePolicy::Reject => violations.push(ContractViolation {
                        rule: "required_column_type_unverifiable".to_string(),
                        column: req.name.clone(),
                        message: format!("{detail} {remedy}"),
                    }),
                }
            }
            // Unverifiable and out of the reported scope: permitted silently.
            // This is the residual fail-open, and it is not closed by this
            // change — see `unverifiable_is_reportable`.
            ContractVerdict::Unverifiable => {}
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
        // The type-name head, exactly — not a prefix of the whole string, which
        // would route a custom `NUMBERED` type through the decimal branch.
        _ if normalizes_as_decimal(&upper) => {
            // Split on the first `(` rather than stripping a fixed prefix:
            // `NUMBER (38,0)` is the same type as `NUMBER(38,0)`, and reading
            // the spaced form as unparameterised made an identical pair
            // unverifiable — and, after the flip, rejected.
            if let Some(params) = upper
                .split_once('(')
                .and_then(|(_, rest)| rest.trim_end().strip_suffix(')'))
            {
                // Parse into the widest form first, then validate — never
                // narrow an unvalidated value into the `u8` precision/scale
                // fields, where `DECIMAL(300,2)` would wrap to `DECIMAL(44,2)`
                // and change what the contract means.
                let parts: Vec<&str> = params.split(',').collect();
                match parts.len() {
                    // `DECIMAL(p)` is `DECIMAL(p, 0)` — ANSI, and DuckDB
                    // executed: `CAST(1 AS DECIMAL(10))` -> `DECIMAL(10,0)`.
                    // Treating it as unparameterised is what let a contract
                    // written `DECIMAL(10)` admit a `NUMBER(38,0)` column with
                    // 28 more integer digits than declared.
                    1 => {
                        return parts[0]
                            .trim()
                            .parse::<u64>()
                            .ok()
                            .and_then(|precision| validate_decimal_params(precision, 0))
                            .map_or(RockyType::Unknown, |(precision, scale)| {
                                RockyType::Decimal { precision, scale }
                            });
                    }
                    2 => {
                        return parts[0]
                            .trim()
                            .parse::<u64>()
                            .ok()
                            .zip(parts[1].trim().parse::<i64>().ok())
                            .and_then(|(p, s)| validate_decimal_params(p, s))
                            .map_or(RockyType::Unknown, |(precision, scale)| {
                                RockyType::Decimal { precision, scale }
                            });
                    }
                    _ => return RockyType::Unknown,
                }
            }
            // SPLIT-FEASIBILITY REVERT (site 2/3): unparameterised (`NUMBER`,
            // `DECIMAL`) resolves to the known-wrong-but-decisive
            // `Decimal(38,0)` that origin/main fabricates. Restored verbatim to
            // measure whether the type algebra is separable from the
            // Unknown-mapper change.
            RockyType::Decimal {
                precision: 38,
                scale: 0,
            }
        }
        _ => RockyType::Unknown,
    }
}

/// Whether an [`ContractVerdict::Unverifiable`] pair is nonetheless **decided**
/// — `Violated` — by the type *family* the unresolved side names.
///
/// # Why "cannot decide" is sometimes false
///
/// `Unverifiable` says the containment question cannot be answered. For an
/// unresolved *decimal* that is only true of its parameters. The family is still
/// named, and the family alone answers the question whenever the answer is the
/// same for **every** member of it:
///
/// | pair | decided? |
/// |---|---|
/// | landed `VARCHAR`, declared `DECIMAL` | yes — no `Decimal(p,s)` admits a `String` |
/// | landed `DOUBLE`, declared `DECIMAL` | yes — no `Decimal(p,s)` admits a `Float64` |
/// | landed `DECIMAL`, declared `BIGINT` | yes — no `Decimal(p,s)` is assignable to an integer |
/// | landed `DECIMAL`, declared `DECIMAL(10,2)` | **no** — `Decimal(10,2)` fits, `Decimal(38,0)` does not |
///
/// Row 4 is the residual and it is genuinely undecidable: it stays
/// `Unverifiable`, is reported, and permits until the flip. It is live on
/// Databricks, whose Unity Catalog table-info responses fall back to a bare
/// `"DECIMAL"` when they omit `type_text`.
///
/// So the rule is an existential over the family, in whichever direction the
/// unresolved side sits:
/// - unresolved **declared** side: violated unless the landed type is
///   exact-numeric (the only types any decimal can hold).
/// - unresolved **landed** side: violated unless the declared type is a decimal
///   (the only target any decimal is assignable to — [`rocky_ir::is_assignable`]
///   has no `Decimal → Int` arm, so `Decimal(5,0) → BIGINT` is not a permit
///   there either, and this must not invent one).
///
/// # Why the narrow name set, when [`looks_like_decimal`] is wider
///
/// Two different jobs. *Reporting* an unverifiable pair is safe to over-include
/// — it only adds a warning — so it uses the wide set (`DEC`, `BIGDECIMAL` too).
/// *Deciding* one is a rejection under the shipping policy, so it is restricted
/// to the four spellings [`warehouse_type_to_rocky`] actually normalizes. That
/// restriction buys a property worth more than the extra coverage:
///
/// > **every pair this arm rejects was already rejected before the decimal
/// > rewrite.** Each of those four names resolved to a concrete `Decimal(38,0)`
/// > pre-rewrite, and `is_assignable` said false for exactly the pairs above.
///
/// So this arm cannot invent a rejection; it restores ones the rewrite dropped.
/// `test_family_refinement_only_restores_pre_rewrite_rejections` proves it over
/// the transition grid rather than asserting it here. The cost is a residual:
/// a landed `DEC` against a `VARCHAR` contract is reported but still permitted,
/// because `DEC` resolved to `Unknown` before the rewrite too — that permit is
/// pre-existing, not opened here, and the flip closes it.
fn family_rules_out(
    landed_raw: &str,
    landed_ty: &RockyType,
    declared_raw: &str,
    declared_ty: &RockyType,
) -> bool {
    // Both sides unresolved: nothing to compare a family against.
    if *landed_ty == RockyType::Unknown && *declared_ty == RockyType::Unknown {
        return false;
    }
    if *declared_ty == RockyType::Unknown
        && normalizes_as_decimal(&declared_raw.trim().to_uppercase())
    {
        return !(landed_ty.is_integer() || landed_ty.is_decimal());
    }
    if *landed_ty == RockyType::Unknown && normalizes_as_decimal(&landed_raw.trim().to_uppercase())
    {
        return !declared_ty.is_decimal();
    }
    false
}

/// Whether a [`ContractVerdict::Unverifiable`] verdict is reported at this gate.
///
/// Two conditions, and both are load-bearing.
///
/// **1. The raw type strings must differ, and the shortcut applies only to
/// COMPLETE type names.** Identical warehouse type strings usually means the
/// landed type *is* the declared type, and every type contains itself — so the
/// check is genuinely *verified*, not skipped. Without it, a `GEOMETRY` column
/// against a `GEOMETRY` contract would be reported, and under `Reject` would
/// fail a load that is obviously fine.
///
/// That reasoning fails for a type name that *elides its parameters*. Two
/// columns both described as `DECIMAL` can be `DECIMAL(10,0)` and
/// `DECIMAL(38,10)`, and one does not contain the other — the string is a lossy
/// projection, not a type. (Reachable: Databricks Unity falls back to
/// `type_name`, a bare `"DECIMAL"`, when a table-info response omits
/// `type_text`.) So the shortcut is withheld from exactly those names, via
/// [`elides_decimal_parameters`] — **not** from every decimal-family name.
/// `DECIMAL(10,2)` against `DECIMAL(10,2)` names one complete type twice, and
/// withholding the shortcut there would be a false reject at the flip.
///
/// **2. Either side must name a decimal.** The rule is exactly that, stated
/// plainly: *this release reports an unverifiable comparison whenever the
/// decimal family is named on either side.* It does not try to decide which
/// side "caused" the failure.
///
/// The narrower alternative — report only when the *unresolved* side names a
/// decimal — was written and then reverted, because it makes a landed
/// `DECIMAL(10,2)` against a contract declaring a type this mapper does not know
/// pass silently under **both** policies. `HUGEINT` is such a type and it is an
/// integer, so `0.01` does not fit it: that is a false permit, and the WP-03
/// rule is explicit that a false permit is far worse than a false `Unknown`.
/// Under the wide rule the same pair is reported now and rejected at the flip,
/// and that rejection is not a false one — Rocky genuinely cannot verify a
/// decimal against a type it does not model, and "cannot verify" is precisely
/// what the verdict says.
///
/// This is still a *narrow* scope, and it leaves a fail-open open: a contracted
/// `String` or `Timestamp` column whose landed type does not normalize passes
/// this gate silently, because no decimal is named anywhere. Note that
/// [`family_rules_out`] does not reach it either — that arm needs a decimal name
/// on the unresolved side, by construction.
///
/// The scoping is not timidity, and the reason is **local to this gate** — the
/// cold-schema-cache argument that scopes the *compiler* gate does not apply
/// here, because neither of this gate's inputs comes from the schema cache:
/// `landed_columns` is a live `DESCRIBE` and the declared type is written in
/// `rocky.toml`. The reason here is that [`warehouse_type_to_rocky`] matches
/// non-decimal names by exact string, so every *parameterised* non-decimal name
/// a warehouse emits already resolves to `Unknown` — Snowflake alone returns
/// `VARCHAR(16777216)` and `TIMESTAMP_NTZ(9)` from `DESCRIBE`. A general "report
/// every unverifiable pair" rule would fire on those today and, at the flip,
/// fail loads that work now. Closing the general case means widening the
/// normalizer to parse parameterised non-decimal names, which is a verdict
/// change with its own migration, not a reporting change.
fn unverifiable_is_reportable(landed_raw: &str, declared_raw: &str) -> bool {
    if landed_raw.trim().eq_ignore_ascii_case(declared_raw.trim())
        && !elides_decimal_parameters(landed_raw)
    {
        return false;
    }
    looks_like_decimal(landed_raw) || looks_like_decimal(declared_raw)
}

/// Whether a raw warehouse type string names a fixed-point decimal family.
///
/// Matches the type-name **head** — everything before the first `(` — against an
/// exact set, rather than testing a prefix of the whole string. A prefix test
/// classifies `NUMBERED` and `NUMERICAL` as decimals, and since the identity
/// shortcut in [`unverifiable_is_reportable`] is withheld from decimal-family
/// names, a custom `NUMBERED` column against a `NUMBERED` contract would be
/// reported now and *rejected* at the flip — a false reject on two identical,
/// complete type names.
///
/// The set is deliberately the same four spellings [`warehouse_type_to_rocky`]
/// recognizes, and that mapper matches the same head, so a name it claims to
/// handle can never fall outside the report and vice versa.
fn looks_like_decimal(warehouse_type: &str) -> bool {
    decimal_family_head(warehouse_type).is_some()
}

/// Whether a raw type string names the decimal family **without naming its
/// parameters** — `DECIMAL`, `NUMBER`, `DEC`, but not `DECIMAL(10,2)`.
///
/// This is the property that makes raw-string equality unsafe: an
/// unparameterised name is a projection of a type, not a type, so two columns
/// that print the same string need not be the same type. A *parameterised* name
/// is a complete type and equality means what it looks like it means.
fn elides_decimal_parameters(warehouse_type: &str) -> bool {
    decimal_family_head(warehouse_type).is_some() && !warehouse_type.contains('(')
}

/// The head of a warehouse type name (everything before a parameter list),
/// matched against the fixed-point decimal spellings — returned uppercased when
/// it is one, `None` when it is not.
///
/// # The reported set is deliberately wider than the normalized set
///
/// [`warehouse_type_to_rocky`] normalizes four spellings (`DECIMAL`,
/// `NUMERIC`, `NUMBER`, `BIGNUMERIC`); this matcher also admits `DEC` and
/// `BIGDECIMAL`. The asymmetry is intentional and is the safe direction: a name
/// Rocky does *not* normalize resolves to `Unknown`, and if it is a decimal
/// spelling that `Unknown` is exactly what must be reported rather than waved
/// through by raw-string equality. Widening what `warehouse_type_to_rocky`
/// *normalizes* is a separate change — it would make previously-unverifiable
/// contracts resolve, which is a verdict change, not a reporting change.
///
/// `sql_type_to_rocky` (the compiler's AST-driven parser) does type all six,
/// because sqlparser gives each its own `DataType` variant;
/// `test_three_decimal_parsers_agree` pins each parser's claimed set so the
/// asymmetry cannot widen unnoticed.
fn decimal_family_head(warehouse_type: &str) -> Option<String> {
    let upper = warehouse_type.trim().to_uppercase();
    let head = upper
        .split('(')
        .next()
        .unwrap_or(&upper)
        .trim_end()
        .to_string();
    matches!(
        head.as_str(),
        "DECIMAL" | "BIGNUMERIC" | "NUMERIC" | "NUMBER" | "DEC" | "BIGDECIMAL"
    )
    .then_some(head)
}

/// Whether the head names one of the four spellings [`warehouse_type_to_rocky`]
/// normalizes. Narrower than [`decimal_family_head`] on purpose — see there.
fn normalizes_as_decimal(upper_type: &str) -> bool {
    let head = upper_type
        .split('(')
        .next()
        .unwrap_or(upper_type)
        .trim_end();
    matches!(head, "DECIMAL" | "BIGNUMERIC" | "NUMERIC" | "NUMBER")
}

/// Whether a normalized type participates in the exact-numeric containment rule
/// (integers and decimals — the types with a well-defined digit count).
fn is_exact_numeric(ty: &RockyType) -> bool {
    ty.is_integer() || ty.is_decimal()
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

    /// An un-normalizable landed type that names **no** family Rocky can reason
    /// about does not produce a type violation.
    ///
    /// The old name for this test was `test_typed_unknown_type_never_fails`, and
    /// that claim is no longer true in general: an unresolved type whose *name*
    /// is in the decimal family now fails against a contract no decimal can
    /// satisfy (see `test_unverifiable_outcomes_violated_reported_silent`).
    /// `GEOMETRY` carries no such name, so nothing about it is decidable.
    #[test]
    fn test_typed_unnamed_unknown_type_does_not_fail() {
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

    /// T5 for the load-gate parser — the third of Rocky's three decimal
    /// parsers. Out-of-range parameters used to wrap silently through `as u8`
    /// (`NUMBER(300,2)` became `DECIMAL(44,2)`), which changed what the
    /// contract meant. They degrade to `Unknown`, which is now a reported
    /// non-permit (`W032` under the migration policy) rather than a silent pass.
    ///
    /// The unparameterised and one-parameter rows below are *inverted* from what
    /// this test asserted a round ago; the rationale for the inversion is on
    /// [`warehouse_type_to_rocky`] itself, at the site the fabricated default
    /// used to live.
    #[test]
    fn test_warehouse_type_to_rocky_decimal_parameters() {
        let dec = |precision: u8, scale: u8| RockyType::Decimal { precision, scale };

        assert_eq!(warehouse_type_to_rocky("DECIMAL(10,2)"), dec(10, 2));
        assert_eq!(warehouse_type_to_rocky("NUMBER(38,0)"), dec(38, 0));
        assert_eq!(warehouse_type_to_rocky("BIGNUMERIC(76,38)"), dec(76, 38));

        for invalid in [
            "DECIMAL(256,2)",
            "NUMBER(300,2)",
            "DECIMAL(10,1000)",
            "DECIMAL(10,-2)",
            "NUMERIC(5,10)",
            "DECIMAL(0,0)",
            "BIGNUMERIC(77,0)",
        ] {
            assert_eq!(
                warehouse_type_to_rocky(invalid),
                RockyType::Unknown,
                "{invalid} should not normalize to a concrete type"
            );
        }

        // Unparameterised names a different type on Databricks (10,0), DuckDB
        // (18,3) and Snowflake (38,0), so there is no concrete answer to give.
        // `Decimal(38,0)` is not even an upper bound on `Decimal(18,3)`.
        assert_eq!(warehouse_type_to_rocky("NUMBER"), RockyType::Unknown);
        assert_eq!(warehouse_type_to_rocky("DECIMAL"), RockyType::Unknown);
        // `DECIMAL(p)` is `DECIMAL(p, 0)` — ANSI, and DuckDB executed:
        // `CAST(1 AS DECIMAL(10))` -> `DECIMAL(10,0)`. This now agrees with the
        // compiler's two decimal parsers instead of contradicting them; the
        // disagreement admitted a `NUMBER(38,0)` column into a `DECIMAL(10)`
        // contract.
        assert_eq!(warehouse_type_to_rocky("DECIMAL(10)"), dec(10, 0));
        assert_eq!(warehouse_type_to_rocky("NUMBER(1)"), dec(1, 0));
        assert_eq!(warehouse_type_to_rocky("DECIMAL(77)"), RockyType::Unknown);
        assert_eq!(warehouse_type_to_rocky("DECIMAL(0)"), RockyType::Unknown);
        assert_eq!(warehouse_type_to_rocky("DECIMAL(x)"), RockyType::Unknown);
        assert_eq!(
            warehouse_type_to_rocky("DECIMAL(1,2,3)"),
            RockyType::Unknown
        );
    }

    /// T3.5 — the load-gate flip class. A Snowflake integer column
    /// (`DESCRIBE` returns `NUMBER(38,0)` for every one of them) does NOT
    /// satisfy a contract declaring a nonzero decimal scale: 38 integer digits
    /// cannot fit into a `DECIMAL(38,2)`'s 36. This passed before the
    /// containment fix, and there is no contract writable on Snowflake that
    /// makes it pass (`DECIMAL(40,2)` exceeds Snowflake's own max precision) —
    /// the escape is an explicit CAST in the model or a `DECIMAL(38,0)`
    /// contract. The rule string names precision so the class is greppable.
    #[test]
    fn test_typed_decimal_precision_flip_is_a_violation() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "DECIMAL(38,2)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("amount", "NUMBER(38,0)", true)];
        let result = validate_contract_typed(&contract, &landed);
        assert!(!result.passed);
        assert_eq!(
            result.violations[0].rule, "required_column_type_precision",
            "the precision failure class must be distinguishable from an \
             unrelated type mismatch"
        );
        assert!(result.violations[0].message.contains("DECIMAL(38,0)"));
    }

    /// The rest of the containment matrix at the gate, in both directions.
    #[test]
    fn test_typed_decimal_containment_matrix() {
        let check = |landed: &str, expected: &str| {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "c".into(),
                    data_type: expected.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            validate_contract_typed(&contract, &[col_n("c", landed, true)]).passed
        };

        // Flipped from pass to violation — each is a case where the old
        // max-precision/max-scale rule fabricated a type that cannot hold the
        // landed values.
        assert!(!check("INT", "DECIMAL(10,2)"), "10 int digits into 8");
        assert!(!check("BIGINT", "DECIMAL(19,2)"), "19 int digits into 17");
        assert!(
            !check("DECIMAL(10,0)", "DECIMAL(10,10)"),
            "10 int digits to 0"
        );
        assert!(
            !check("DECIMAL(12,2)", "DECIMAL(12,4)"),
            "10 int digits into 8"
        );

        // Still passing — genuine widening.
        assert!(check("DECIMAL(10,2)", "DECIMAL(12,4)"));
        assert!(check("DECIMAL(10,2)", "DECIMAL(10,2)"));
        assert!(check("BIGINT", "DECIMAL(21,2)"));
        assert!(check("INT", "DECIMAL(12,2)"));
        // Newly passing — the old rule rejected this while permitting the
        // strictly wider BIGINT -> DECIMAL(19,2) above.
        assert!(check("INT", "DECIMAL(10,0)"));
    }

    /// G3 — **the inversion.** This test previously asserted
    /// `result.passed` with the comment *"an un-normalizable expected type must
    /// not fail the gate"*, which states the fail-open as the intended
    /// behaviour. It is not: `Unknown` at a validation gate is unresolved
    /// evidence, and unresolved evidence must not read as safe.
    ///
    /// Under the shipping [`UNVERIFIABLE_POLICY`] (`Warn`) the gate still
    /// permits, and that is the migration concession, not a verification — the
    /// verdict for the same pair is [`ContractVerdict::Unverifiable`], asserted
    /// here so the two cannot be confused. Under `Reject` — one line away, and
    /// exercised now rather than after the flip — it is a violation.
    #[test]
    fn test_typed_unverifiable_decimal_contract_is_not_a_permit() {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "DECIMAL(300,2)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("amount", "DECIMAL(10,2)", true)];

        // The verdict itself — this is what the gate now decides on, and it is
        // not "compatible".
        assert_eq!(
            contract_verdict(
                &warehouse_type_to_rocky("DECIMAL(10,2)"),
                &warehouse_type_to_rocky("DECIMAL(300,2)")
            ),
            ContractVerdict::Unverifiable
        );

        let warned =
            validate_contract_typed_with_policy(&contract, &landed, UnverifiablePolicy::Warn);
        assert!(
            warned.passed,
            "the migration window permits — see UNVERIFIABLE_POLICY: {:?}",
            warned.violations
        );
        assert!(
            warned.warnings.iter().any(|w| w.starts_with("W032:")),
            "the permit must never be silent, got {:?}",
            warned.warnings
        );

        let rejected =
            validate_contract_typed_with_policy(&contract, &landed, UnverifiablePolicy::Reject);
        assert!(
            !rejected.passed,
            "unresolved evidence must not satisfy a contract"
        );
        assert_eq!(
            rejected.violations[0].rule,
            "required_column_type_unverifiable"
        );

        // The shipping default is the `Warn` arm.
        assert_eq!(UNVERIFIABLE_POLICY, UnverifiablePolicy::Warn);
        let shipped = validate_contract_typed(&contract, &landed);
        assert_eq!(shipped.passed, warned.passed);

        // A well-formed decimal contract produces no W032 — both sides resolve,
        // so the check is genuinely performed.
        let ok = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "amount".into(),
                data_type: "DECIMAL(38,2)".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let result = validate_contract_typed(&ok, &[col_n("amount", "DECIMAL(10,2)", true)]);
        assert!(result.warnings.iter().all(|w| !w.starts_with("W032:")));
    }

    /// G5 — identical raw type strings are *verified*, not skipped, **when the
    /// type name is complete**: the landed type is the declared type, and every
    /// type contains itself. Without this guard the scoping predicate would
    /// report every unrecognized-but-matching type pair, and under `Reject`
    /// would fail a load that is obviously fine.
    ///
    /// The decimal family is excluded from the shortcut and is asserted in
    /// `test_identical_bare_decimal_strings_are_not_a_verification` — `DECIMAL`
    /// elides its parameters, so two columns that print the same string are not
    /// necessarily the same type.
    ///
    /// This is the discriminator against the naive "report every Unverifiable"
    /// implementation.
    #[test]
    fn test_identical_raw_type_strings_are_verified_not_warned() {
        let check = |landed: &str, declared: &str| {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "c".into(),
                    data_type: declared.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            let landed_cols = vec![col_n("c", landed, true)];
            (
                validate_contract_typed(&contract, &landed_cols),
                validate_contract_typed_with_policy(
                    &contract,
                    &landed_cols,
                    UnverifiablePolicy::Reject,
                ),
            )
        };

        for (landed, declared) in [
            ("GEOMETRY", "GEOMETRY"),
            ("geometry", "GEOMETRY"),
            ("VARIANT_X", "variant_x"),
        ] {
            let (warned, rejected) = check(landed, declared);
            assert!(warned.passed, "{landed} vs {declared}: must pass");
            assert!(
                warned.warnings.iter().all(|w| !w.starts_with("W032:")),
                "{landed} vs {declared}: must not warn, got {:?}",
                warned.warnings
            );
            // And the flip must not turn it into a failure either — the
            // rejecting arm is gated on the same predicate as the warning.
            assert!(
                rejected.passed,
                "{landed} vs {declared}: the flip must not reject a verified pair, got {:?}",
                rejected.violations
            );
        }

        // A non-decimal type that does *not* match is the residual fail-open:
        // still permitted, still silent, under both policies. Named here so the
        // hole is visible in the test suite rather than only in prose.
        let (warned, rejected) = check("GEOMETRY", "BIGINT");
        assert!(warned.passed);
        assert!(warned.warnings.iter().all(|w| !w.starts_with("W032:")));
        assert!(
            rejected.passed,
            "scoped out of this release: non-decimal unverifiable types still pass"
        );
    }

    /// Two columns both described as `DECIMAL` are **not** thereby the same
    /// type: the string elides the parameters, so it is a lossy projection, and
    /// `DECIMAL(10,0)` versus `DECIMAL(38,10)` prints identically. Asserting
    /// "identical strings, therefore verified" here would be the round-1 failure
    /// mode again — a test certifying a fail-open.
    ///
    /// Reachable in production: Databricks Unity falls back to `type_name`
    /// (`"DECIMAL"`) when a table-info response omits `type_text`, and Databricks
    /// `information_schema.columns.data_type` is the simple type name, so a
    /// landed side can genuinely arrive bare.
    #[test]
    fn test_identical_bare_decimal_strings_are_not_a_verification() {
        let check = |landed: &str, declared: &str| {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "c".into(),
                    data_type: declared.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            let landed_cols = vec![col_n("c", landed, true)];
            let warned = validate_contract_typed(&contract, &landed_cols);
            let rejected = validate_contract_typed_with_policy(
                &contract,
                &landed_cols,
                UnverifiablePolicy::Reject,
            );
            (
                warned.warnings.iter().any(|w| w.starts_with("W032:")),
                rejected.passed,
            )
        };

        for (landed, declared) in [
            ("DECIMAL", "DECIMAL"),
            ("NUMBER", "number"),
            ("decimal", "DECIMAL"),
            // `DEC` is a decimal spelling this mapper does not normalize; that
            // makes the pair MORE unresolved, not less, so it must not take the
            // identity shortcut either.
            ("DEC", "DEC"),
            ("BIGDECIMAL", "bigdecimal"),
        ] {
            let (warned, reject_passes) = check(landed, declared);
            assert!(
                warned,
                "{landed} vs {declared}: identical bare decimal strings are not a verification"
            );
            assert!(
                !reject_passes,
                "{landed} vs {declared}: the flip must reject an unverified decimal pair"
            );
        }

        // A fully parameterised decimal on both sides IS verified — it resolves,
        // so it never reaches the unverifiable arm at all.
        let (warned, reject_passes) = check("DECIMAL(10,2)", "DECIMAL(10,2)");
        assert!(!warned);
        assert!(reject_passes);
        // And a parameterised spelling Rocky does NOT normalize still names one
        // complete type on both sides, so raw-string identity is a genuine
        // verification there and must survive the flip.
        let (warned, reject_passes) = check("DEC(10,2)", "DEC(10,2)");
        assert!(!warned, "identical complete type names are verified");
        assert!(reject_passes);
    }

    /// What the gate does with an unverifiable pair, as a table. Three
    /// outcomes, and which one you get depends on how much the *family* of the
    /// unresolved side settles.
    ///
    /// - **Violated** — the family decides it: no member of it satisfies the
    ///   pair. Rejected under both policies. See [`family_rules_out`].
    /// - **Reported** — genuinely undecidable, and the decimal family is named
    ///   on either side, so `W032` now / reject at the flip.
    /// - **Silent** — genuinely undecidable and no decimal is named anywhere:
    ///   the documented residual hole.
    ///
    /// The `Reported` scope's narrower "only when the *unresolved* side names a
    /// decimal" variant was written and reverted; the row that killed it is
    /// `DECIMAL(10,2)` vs `HUGEINT`, which the narrow rule passed silently under
    /// **both** policies. `HUGEINT` is DuckDB's 128-bit *integer*, so `0.01`
    /// does not fit it — a false permit, and the WP-03 rule ranks a false permit
    /// far worse than a false `Unknown`.
    #[test]
    fn test_unverifiable_outcomes_violated_reported_silent() {
        #[derive(Debug, PartialEq, Eq, Clone, Copy)]
        enum Outcome {
            Violated,
            Reported,
            Silent,
        }

        let check = |landed: &str, declared: &str| {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "c".into(),
                    data_type: declared.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            let landed_cols = vec![col_n("c", landed, true)];
            let warned = validate_contract_typed(&contract, &landed_cols);
            let rejected = validate_contract_typed_with_policy(
                &contract,
                &landed_cols,
                UnverifiablePolicy::Reject,
            );
            let outcome = if !warned.passed {
                Outcome::Violated
            } else if warned.warnings.iter().any(|w| w.starts_with("W032:")) {
                Outcome::Reported
            } else {
                Outcome::Silent
            };
            (outcome, rejected.passed)
        };

        let cases: &[(&str, &str, Outcome)] = &[
            // The unresolved side names a decimal and the other side is outside
            // what any decimal satisfies. Decided, not deferred.
            ("VARCHAR", "DECIMAL", Outcome::Violated),
            ("DOUBLE", "DECIMAL", Outcome::Violated),
            ("NUMBER", "BIGINT", Outcome::Violated),
            ("NUMBER", "VARCHAR", Outcome::Violated),
            // Same family on both sides: the parameters are what is unknown, and
            // they genuinely decide it either way. This is the residual, and it
            // is live on Databricks.
            ("DECIMAL", "DECIMAL(10,2)", Outcome::Reported),
            ("DECIMAL(10,2)", "DECIMAL", Outcome::Reported),
            // Resolved decimal against a type Rocky does not model. The family
            // rule cannot reach it — `HUGEINT` is not a decimal name — so it
            // stays deferred. This is the row that reverted the narrow rule.
            ("DECIMAL(10,2)", "HUGEINT", Outcome::Reported),
            ("DECIMAL(10,2)", "SUPER", Outcome::Reported),
            // Declared decimal that cannot be verified against an unresolved
            // landed type: both sides unresolved, nothing to compare.
            ("GEOMETRY", "DECIMAL(38,2)", Outcome::Reported),
            // No decimal named anywhere: the documented residual hole.
            ("VARCHAR", "HUGEINT", Outcome::Silent),
            ("GEOMETRY", "BIGINT", Outcome::Silent),
            // `DEC` is in the *reported* set but not the *decided* set, on
            // purpose — it resolved to `Unknown` before the rewrite too, so
            // deciding it here would invent a rejection rather than restore one.
            ("DEC", "VARCHAR", Outcome::Reported),
        ];

        for (landed, declared, expected) in cases {
            let (outcome, reject_passes) = check(landed, declared);
            assert_eq!(outcome, *expected, "{landed} vs {declared}");
            let expected_reject_passes = *expected == Outcome::Silent;
            assert_eq!(
                reject_passes, expected_reject_passes,
                "{landed} vs {declared}: the flip must reject everything that is \
                 violated or reported"
            );
        }
    }

    /// A *complete* type name whose text merely starts with a decimal spelling
    /// is not in the family. `NUMBERED` vs `NUMBERED` must stay silent and must
    /// keep passing at the flip: two identical complete type names are the same
    /// type, and a prefix test would have made that a false reject once the
    /// identity shortcut started excluding decimal-family names.
    #[test]
    fn test_decimal_family_is_matched_on_the_name_head() {
        assert!(!looks_like_decimal("NUMBERED"));
        assert!(!looks_like_decimal("NUMERICAL"));
        assert!(!looks_like_decimal("DECIMALISH"));
        assert!(looks_like_decimal("DECIMAL"));
        assert!(looks_like_decimal("decimal(10,2)"));
        assert!(looks_like_decimal(" NUMBER (38,0) "));
        assert!(looks_like_decimal("BIGNUMERIC(76,38)"));
        // Reported but not normalized — the deliberate asymmetry.
        assert!(looks_like_decimal("DEC"));
        assert!(looks_like_decimal("BIGDECIMAL(10,2)"));

        // The identity shortcut keys on *parameter elision*, not on family
        // membership: a parameterised name is a complete type, so two equal
        // ones are the same type and must keep passing at the flip.
        assert!(elides_decimal_parameters("DECIMAL"));
        assert!(elides_decimal_parameters("dec"));
        assert!(!elides_decimal_parameters("DECIMAL(10,2)"));
        assert!(!elides_decimal_parameters("NUMBER (38,0)"));
        assert!(!elides_decimal_parameters("GEOMETRY"));

        // And the mapper agrees: a near-miss name is simply unrecognized, not
        // routed through the decimal branch.
        assert_eq!(warehouse_type_to_rocky("NUMBERED"), RockyType::Unknown);
        assert_eq!(warehouse_type_to_rocky("NUMERICAL"), RockyType::Unknown);
        // A spaced parameter list is the same type as an unspaced one; reading
        // it as unparameterised made an identical pair unverifiable.
        assert_eq!(
            warehouse_type_to_rocky("NUMBER (38,0)"),
            RockyType::Decimal {
                precision: 38,
                scale: 0
            }
        );
        assert_eq!(
            crate::contracts::warehouse_type_to_rocky("DECIMAL ( 10 , 2 )"),
            RockyType::Decimal {
                precision: 10,
                scale: 2
            }
        );

        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "c".into(),
                data_type: "NUMBERED".into(),
                nullable: true,
            }],
            ..Default::default()
        };
        let landed = vec![col_n("c", "NUMBERED", true)];
        let warned = validate_contract_typed(&contract, &landed);
        assert!(warned.warnings.iter().all(|w| !w.starts_with("W032:")));
        let rejected =
            validate_contract_typed_with_policy(&contract, &landed, UnverifiablePolicy::Reject);
        assert!(
            rejected.passed,
            "identical complete type names must not be rejected at the flip"
        );
    }

    /// T5.14 — the six landed/declared pairs measured against the pre-fix gate,
    /// each asserting the post-fix verdict. Five of the eight rows here return
    /// the opposite result on the previous implementation.
    #[test]
    fn test_load_gate_decimal_pairs() {
        let gate = |landed: &str, declared: &str, policy: UnverifiablePolicy| {
            let contract = ContractConfig {
                required_columns: vec![RequiredColumn {
                    name: "c".into(),
                    data_type: declared.into(),
                    nullable: true,
                }],
                ..Default::default()
            };
            validate_contract_typed_with_policy(&contract, &[col_n("c", landed, true)], policy)
        };

        // (landed, declared, passes under Warn, warns W032 under Warn, passes
        // under Reject)
        let cases: &[(&str, &str, bool, bool, bool)] = &[
            // Migration window: a violation today, a reported permit for one
            // release, a violation again at the flip.
            ("NUMBER", "DECIMAL(38,2)", true, true, false),
            // Was a false reject (landed DECIMAL(10,2) genuinely fits nothing
            // the bare contract names, because the bare contract names nothing).
            ("DECIMAL(10,2)", "DECIMAL", true, true, false),
            // Passed before only by mutual fabrication — both sides invented
            // `Decimal(38,0)`. Now reported.
            ("NUMBER(38,0)", "DECIMAL", true, true, false),
            // B2, landed side: a false REJECT removed immediately, not deferred
            // to the flip. DuckDB executes `CAST(1 AS DECIMAL(10))` to exactly
            // `DECIMAL(10,0)`.
            ("DECIMAL(10)", "DECIMAL(10,0)", true, false, true),
            // B2, declared side: a false PERMIT removed immediately. 38 integer
            // digits do not fit a contract declaring 10.
            ("NUMBER(38,0)", "DECIMAL(10)", false, false, false),
            // Unchanged, both directions — the containment rule is untouched.
            ("DECIMAL(10,2)", "DECIMAL(12,4)", true, false, true),
            ("NUMBER(38,0)", "DECIMAL(38,2)", false, false, false),
            ("INT", "DECIMAL(12,2)", true, false, true),
        ];

        for (landed, declared, warn_passes, warns, reject_passes) in cases {
            let warned = gate(landed, declared, UnverifiablePolicy::Warn);
            assert_eq!(
                warned.passed, *warn_passes,
                "Warn: landed={landed} declared={declared} violations={:?}",
                warned.violations
            );
            assert_eq!(
                warned.warnings.iter().any(|w| w.starts_with("W032:")),
                *warns,
                "Warn: landed={landed} declared={declared} warnings={:?}",
                warned.warnings
            );
            let rejected = gate(landed, declared, UnverifiablePolicy::Reject);
            assert_eq!(
                rejected.passed, *reject_passes,
                "Reject: landed={landed} declared={declared} violations={:?}",
                rejected.violations
            );
        }
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

    // ======================================================================
    // Transition oracle: the pre-rewrite gate vs this one, over a type grid.
    //
    // The decimal rewrite's verdict change does **not** live in
    // `is_assignable` — old and new agree that `Decimal(38,0)` does not fit
    // `Decimal(10,2)`. It lives in *which type each side resolves to*. Nothing
    // in the change compared the mappers' old and new output, so a verdict
    // could move without any test noticing. These frozen copies plus the grid
    // below are that comparison.
    // ======================================================================

    /// `warehouse_type_to_rocky` exactly as it stood before the decimal
    /// rewrite, frozen as ground truth. Sibling of
    /// `rocky_ir::types::tests::is_assignable_frozen_old`.
    ///
    /// Two behaviours matter and both are reproduced verbatim: the
    /// `starts_with` family test (so `NUMBERED` was a decimal), and the
    /// `Decimal(38,0)` fallthrough for anything it could not parse into exactly
    /// two `u8` parameters.
    fn warehouse_type_to_rocky_frozen_old(warehouse_type: &str) -> RockyType {
        let upper = warehouse_type.trim().to_uppercase();
        match upper.as_str() {
            "BOOLEAN" | "BOOL" => RockyType::Boolean,
            "TINYINT" | "BYTE" | "SMALLINT" | "SHORT" | "INT" | "INTEGER" => RockyType::Int32,
            "BIGINT" | "LONG" | "INT64" => RockyType::Int64,
            "FLOAT" | "REAL" => RockyType::Float32,
            "DOUBLE" | "DOUBLE PRECISION" | "FLOAT64" => RockyType::Float64,
            "STRING" | "VARCHAR" | "TEXT" => RockyType::String,
            "BINARY" | "BYTES" => RockyType::Binary,
            "DATE" => RockyType::Date,
            "TIMESTAMP" => RockyType::Timestamp,
            "TIMESTAMP_NTZ" | "DATETIME" => RockyType::TimestampNtz,
            "VARIANT" => RockyType::Variant,
            _ if upper.starts_with("DECIMAL")
                || upper.starts_with("BIGNUMERIC")
                || upper.starts_with("NUMERIC")
                || upper.starts_with("NUMBER") =>
            {
                if let Some(params) = upper
                    .strip_prefix("DECIMAL(")
                    .or_else(|| upper.strip_prefix("BIGNUMERIC("))
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

    /// The pre-rewrite `is_assignable`, frozen. Copied from
    /// `rocky_ir::types::tests::is_assignable_frozen_old`, which cannot be
    /// imported across the crate boundary.
    fn is_assignable_frozen_old(from: &RockyType, to: &RockyType) -> bool {
        fn common_supertype_frozen_old(a: &RockyType, b: &RockyType) -> Option<RockyType> {
            if a == b {
                return Some(a.clone());
            }
            if *a == RockyType::Unknown {
                return Some(b.clone());
            }
            if *b == RockyType::Unknown {
                return Some(a.clone());
            }
            match (a, b) {
                (RockyType::Int32, RockyType::Int64) | (RockyType::Int64, RockyType::Int32) => {
                    Some(RockyType::Int64)
                }
                (RockyType::Float32, RockyType::Float64)
                | (RockyType::Float64, RockyType::Float32) => Some(RockyType::Float64),
                (i, RockyType::Float64) | (RockyType::Float64, i) if i.is_integer() => {
                    Some(RockyType::Float64)
                }
                (i, RockyType::Float32) | (RockyType::Float32, i) if i.is_integer() => {
                    Some(RockyType::Float64)
                }
                (RockyType::Int32, RockyType::Decimal { precision, scale })
                | (RockyType::Decimal { precision, scale }, RockyType::Int32) => {
                    Some(RockyType::Decimal {
                        precision: (*precision).max(10),
                        scale: *scale,
                    })
                }
                (RockyType::Int64, RockyType::Decimal { precision, scale })
                | (RockyType::Decimal { precision, scale }, RockyType::Int64) => {
                    Some(RockyType::Decimal {
                        precision: (*precision).max(19),
                        scale: *scale,
                    })
                }
                (
                    RockyType::Decimal {
                        precision: p1,
                        scale: s1,
                    },
                    RockyType::Decimal {
                        precision: p2,
                        scale: s2,
                    },
                ) => Some(RockyType::Decimal {
                    precision: (*p1).max(*p2),
                    scale: (*s1).max(*s2),
                }),
                (RockyType::Timestamp, RockyType::TimestampNtz)
                | (RockyType::TimestampNtz, RockyType::Timestamp) => Some(RockyType::Timestamp),
                _ => None,
            }
        }

        if from == to || *from == RockyType::Unknown || *to == RockyType::Unknown {
            return true;
        }
        common_supertype_frozen_old(from, to).is_some_and(|sup| sup == *to)
    }

    /// The pre-rewrite gate verdict for one (landed, declared) pair: two
    /// outcomes, decided by the frozen mapper feeding the frozen predicate.
    fn frozen_old_gate_passes(landed: &str, declared: &str) -> bool {
        is_assignable_frozen_old(
            &warehouse_type_to_rocky_frozen_old(landed),
            &warehouse_type_to_rocky_frozen_old(declared),
        )
    }

    /// Raw warehouse type strings a supported warehouse can actually emit from
    /// `DESCRIBE`, or a user can actually write in `rocky.toml`, plus the
    /// malformed spellings the rewrite re-routed.
    ///
    /// Snowflake's `VARCHAR(16777216)` and `TIMESTAMP_NTZ(9)` are in the grid
    /// because they are what `DESCRIBE` really returns there, and they resolve
    /// to `Unknown` on **both** sides of the rewrite — they are the population
    /// the residual silent hole is about.
    const TRANSITION_GRID: &[&str] = &[
        "VARCHAR",
        "VARCHAR(16777216)",
        "STRING",
        "TEXT",
        "BIGINT",
        "INT",
        "SMALLINT",
        "DOUBLE",
        "FLOAT",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_NTZ(9)",
        "BINARY",
        "VARIANT",
        "DECIMAL",
        "NUMBER",
        "NUMERIC",
        "DECIMAL(10,2)",
        "DECIMAL(38,0)",
        "NUMBER(38,0)",
        "NUMBER (38,0)",
        "DECIMAL(10)",
        "DECIMAL(300,2)",
        "DECIMAL(10,-2)",
        "DECIMAL(x)",
        "DECIMAL(1,2,3)",
        "BIGNUMERIC(76,38)",
        "DEC",
        "HUGEINT",
        "GEOMETRY",
    ];

    fn new_gate_passes(landed: &str, declared: &str, policy: UnverifiablePolicy) -> bool {
        let contract = ContractConfig {
            required_columns: vec![RequiredColumn {
                name: "c".into(),
                data_type: declared.into(),
                nullable: true,
            }],
            ..Default::default()
        };
        validate_contract_typed_with_policy(&contract, &[col_n("c", landed, true)], policy).passed
    }

    /// **The oracle.** Every ordered pair over [`TRANSITION_GRID`], old verdict
    /// against new verdict, with both transition directions enumerated
    /// explicitly. An unintended permit fails here rather than shipping.
    ///
    /// Read the two lists as the whole behavioural delta of the decimal rewrite
    /// at this gate.
    #[test]
    fn test_load_gate_transition_oracle() {
        // reject -> pass. Every entry is a *deliberate* new permit.
        //
        // All of them have a decimal-family head on both sides, and all of them
        // are the same shape: a pair the pre-rewrite mapper decided using a
        // `Decimal(38,0)` it invented, where the truth is that the parameters
        // are unknown. They are reported (`W032`) and reject at the flip; the
        // property assertion below states the "decimal on both sides" invariant
        // independently of this list, so a future widening on a *non*-decimal
        // name fails even if someone extends the list.
        let intended_new_permits: &[(&str, &str)] = &[
            // Unparameterised or malformed declared type: the landed decimal
            // might or might not fit, and the pre-rewrite answer was decided by
            // a fabricated `Decimal(38,0)`.
            ("DECIMAL(10,2)", "DECIMAL"),
            ("DECIMAL(10,2)", "NUMBER"),
            ("DECIMAL(10,2)", "NUMERIC"),
            ("DECIMAL(10,2)", "DECIMAL(300,2)"),
            ("DECIMAL(10,2)", "DECIMAL(10,-2)"),
            ("DECIMAL(10,2)", "DECIMAL(x)"),
            ("DECIMAL(10,2)", "DECIMAL(1,2,3)"),
            ("BIGNUMERIC(76,38)", "DECIMAL"),
            ("BIGNUMERIC(76,38)", "NUMBER"),
            ("BIGNUMERIC(76,38)", "NUMERIC"),
            ("BIGNUMERIC(76,38)", "DECIMAL(300,2)"),
            ("BIGNUMERIC(76,38)", "DECIMAL(10,-2)"),
            ("BIGNUMERIC(76,38)", "DECIMAL(x)"),
            ("BIGNUMERIC(76,38)", "DECIMAL(1,2,3)"),
            ("NUMERIC", "DECIMAL(10,2)"),
            ("DECIMAL", "DECIMAL(10,2)"),
            ("NUMBER", "DECIMAL(10,2)"),
            ("DECIMAL(300,2)", "DECIMAL(10,2)"),
            ("DECIMAL(10,-2)", "DECIMAL(10,2)"),
            ("DECIMAL(x)", "DECIMAL(10,2)"),
            ("DECIMAL(1,2,3)", "DECIMAL(10,2)"),
        ];

        // pass -> reject. Every entry is a *deliberate* closed false permit.
        let intended_new_rejections: &[(&str, &str)] = &[
            // `DECIMAL(p)` is `DECIMAL(p,0)`, not unparameterised. A contract
            // written `DECIMAL(10)` used to admit 28 more integer digits than
            // it declared.
            ("BIGINT", "DECIMAL(10)"),
            ("DECIMAL(38,0)", "DECIMAL(10)"),
            ("NUMBER(38,0)", "DECIMAL(10)"),
            ("NUMBER (38,0)", "DECIMAL(10)"),
            // `is_assignable` no longer proxies through `common_supertype`, so
            // an integer's digits must fit the declared *integer* digits.
            ("INT", "DECIMAL(10,2)"),
            ("SMALLINT", "DECIMAL(10,2)"),
        ];

        let mut unexpected_permits = Vec::new();
        let mut unexpected_rejections = Vec::new();
        for landed in TRANSITION_GRID {
            for declared in TRANSITION_GRID {
                let old = frozen_old_gate_passes(landed, declared);
                let new = new_gate_passes(landed, declared, UnverifiablePolicy::Warn);
                if !old && new && !intended_new_permits.contains(&(landed, declared)) {
                    unexpected_permits.push((*landed, *declared));
                }
                if old && !new && !intended_new_rejections.contains(&(landed, declared)) {
                    unexpected_rejections.push((*landed, *declared));
                }
            }
        }
        assert!(
            unexpected_permits.is_empty(),
            "unintended reject -> pass transitions: {unexpected_permits:?}"
        );
        assert!(
            unexpected_rejections.is_empty(),
            "unintended pass -> reject transitions: {unexpected_rejections:?}"
        );

        // Both lists must stay live: an entry that no longer transitions is
        // stale documentation, and stale documentation of a false permit is
        // exactly what this oracle exists to prevent.
        for (landed, declared) in intended_new_permits {
            assert!(
                !frozen_old_gate_passes(landed, declared)
                    && new_gate_passes(landed, declared, UnverifiablePolicy::Warn),
                "{landed} vs {declared} is listed as a new permit but no longer transitions"
            );
            // The property, stated independently of the list.
            assert!(
                looks_like_decimal(landed) && looks_like_decimal(declared),
                "{landed} vs {declared}: a new permit must have a decimal head on \
                 both sides — a non-decimal name transitioning is out of scope for \
                 this change"
            );
            // Every one of them is reported, and closes at the flip.
            assert!(
                !new_gate_passes(landed, declared, UnverifiablePolicy::Reject),
                "{landed} vs {declared}: a migration-window permit must close at the flip"
            );
        }
        for (landed, declared) in intended_new_rejections {
            assert!(
                frozen_old_gate_passes(landed, declared)
                    && !new_gate_passes(landed, declared, UnverifiablePolicy::Warn),
                "{landed} vs {declared} is listed as a closed false permit but no \
                 longer transitions"
            );
        }

        // The split the CHANGELOG quotes, pinned so the numbers stay true. Of
        // the pairs the pre-rewrite gate rejected and the new *mappers* alone
        // would have let through, the family arm decides most and defers the
        // rest.
        let mut would_open = 0usize;
        let mut restored_by_family = 0usize;
        for landed in TRANSITION_GRID {
            for declared in TRANSITION_GRID {
                if frozen_old_gate_passes(landed, declared) {
                    continue;
                }
                let decided = family_rules_out(
                    landed,
                    &warehouse_type_to_rocky(landed),
                    declared,
                    &warehouse_type_to_rocky(declared),
                );
                if decided {
                    would_open += 1;
                    restored_by_family += 1;
                } else if new_gate_passes(landed, declared, UnverifiablePolicy::Warn) {
                    would_open += 1;
                }
            }
        }
        // These two literals are the guard, not the subtraction below them: a
        // grid string that opens a genuinely new permit moves `would_open` even
        // if someone dutifully adds the pair to `intended_new_permits`.
        assert_eq!(would_open, 196, "grid size or mapper coverage changed");
        assert_eq!(restored_by_family, 175);
        // Weaker, and says only what it pins: the enumerated list is complete,
        // with no listed pair that the family arm actually decides.
        assert_eq!(
            would_open - restored_by_family,
            intended_new_permits.len(),
            "every deferred permit must be enumerated, and every enumerated one \
             must still be deferred rather than decided"
        );
    }

    /// The containment property [`family_rules_out`] is scoped for: **it never
    /// invents a rejection.**
    ///
    /// Restricting the family decision to the four names
    /// [`warehouse_type_to_rocky`] normalizes is what buys this. Every pair the
    /// arm rejects was rejected before the rewrite too, so the arm restores
    /// verdicts rather than adding them — which is what makes it safe to ship in
    /// a release whose new verdict is otherwise warning-only.
    #[test]
    fn test_family_refinement_only_restores_pre_rewrite_rejections() {
        let mut decided = 0usize;
        for landed in TRANSITION_GRID {
            for declared in TRANSITION_GRID {
                let landed_ty = warehouse_type_to_rocky(landed);
                let declared_ty = warehouse_type_to_rocky(declared);
                if !family_rules_out(landed, &landed_ty, declared, &declared_ty) {
                    continue;
                }
                decided += 1;
                assert!(
                    !frozen_old_gate_passes(landed, declared),
                    "{landed} vs {declared}: the family arm invented a rejection the \
                     pre-rewrite gate did not make"
                );
                assert!(
                    !new_gate_passes(landed, declared, UnverifiablePolicy::Warn),
                    "{landed} vs {declared}: decided by family, so it must not pass"
                );
            }
        }
        assert!(
            decided > 0,
            "the family arm must actually fire on this grid, or the property is vacuous"
        );
    }

    /// The residual F2 leaves open, asserted rather than only described.
    ///
    /// Same-family pairs are genuinely undecidable without the parameters, so
    /// they stay reported-and-permitted until the flip. The first row is live on
    /// Databricks: Unity Catalog table-info responses fall back to the bare
    /// `type_name` `"DECIMAL"` when they omit `type_text`.
    #[test]
    fn test_same_family_decimal_pairs_remain_permitted_until_the_flip() {
        for (landed, declared) in [("DECIMAL", "DECIMAL(10,2)"), ("DECIMAL(10,2)", "DECIMAL")] {
            assert!(
                !frozen_old_gate_passes(landed, declared),
                "premise: the pre-rewrite gate rejected {landed} vs {declared}"
            );
            assert!(
                new_gate_passes(landed, declared, UnverifiablePolicy::Warn),
                "{landed} vs {declared}: still permitted in the migration window"
            );
            assert!(
                !new_gate_passes(landed, declared, UnverifiablePolicy::Reject),
                "{landed} vs {declared}: and closed by the flip"
            );
        }

        // The same undecidability in the other direction: the pre-rewrite gate
        // *passed* `NUMBER` against `DECIMAL(38,0)`, but only because it
        // fabricated `Decimal(38,0)` for the bare name — the landed column could
        // as easily have been `NUMBER(38,10)`, which does not fit. So this pair
        // is permitted now for an honest reason where it was permitted then for
        // a dishonest one, and the flip will newly reject it.
        assert!(frozen_old_gate_passes("NUMBER", "DECIMAL(38,0)"));
        assert!(new_gate_passes(
            "NUMBER",
            "DECIMAL(38,0)",
            UnverifiablePolicy::Warn
        ));
        assert!(!new_gate_passes(
            "NUMBER",
            "DECIMAL(38,0)",
            UnverifiablePolicy::Reject
        ));
    }
}
