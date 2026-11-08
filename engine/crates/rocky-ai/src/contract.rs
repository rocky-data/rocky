//! AI-drafted data contracts from observed data.
//!
//! Given a per-column profile of a model's target table (null rate, distinct
//! count, low-cardinality domain, min/max, type), this module asks the LLM to
//! propose a `.contract.toml` and runs a compile-verify loop: the proposed
//! contract must pass `validate_contract` against the model's inferred schema
//! before it's accepted. On failure the diagnostics are fed back as the next
//! retry's error context, mirroring the model-generation loop in `generate.rs`.
//!
//! The data supplies the evidence; the LLM supplies judgment on which
//! constraints matter (`required`/`protected` columns, types, nullability).

use rocky_compiler::contracts::{CompilerContract, validate_contract};
use rocky_compiler::types::TypedColumn;

use crate::client::{AiError, LlmClient};

/// Profile of a single column, observed from the warehouse.
///
/// All fields are evidence rendered into the LLM prompt. `observed_values` is
/// the low-cardinality domain (only populated when the column is below the
/// cardinality cap); it grounds the LLM's judgment but is not written into the
/// contract file — the `.contract.toml` format carries no `accepted_values`
/// constraint today.
#[derive(Debug, Clone)]
pub struct ColumnProfile {
    /// Column name.
    pub name: String,
    /// Inferred Rocky type name (the contract's `type` vocabulary, e.g.
    /// `Int64`, `String`).
    pub type_name: String,
    /// Total rows scanned.
    pub rows: u64,
    /// Null count.
    pub nulls: u64,
    /// Fraction of nulls in `[0.0, 1.0]`.
    pub null_rate: f64,
    /// Distinct non-null values.
    pub distinct: u64,
    /// Observed domain for low-cardinality columns. Empty when the column
    /// exceeds the cardinality cap.
    pub observed_values: Vec<String>,
    /// Minimum value (as a display string), when available.
    pub min: Option<String>,
    /// Maximum value (as a display string), when available.
    pub max: Option<String>,
}

/// A full table profile: the model name plus a per-column profile.
#[derive(Debug, Clone)]
pub struct TableProfile {
    /// The model whose target table was profiled.
    pub model: String,
    /// Per-column profiles, in schema order.
    pub columns: Vec<ColumnProfile>,
}

/// Result of a successful contract draft.
#[derive(Debug)]
pub struct DraftedContract {
    /// The proposed contract, parsed and compile-verified.
    pub contract: CompilerContract,
    /// The contract serialized as `.contract.toml`.
    pub toml: String,
    /// Number of LLM attempts taken.
    pub attempts: usize,
    /// Declared column types the compiler could NOT check, one message each.
    ///
    /// The verify loop only fails on error-severity diagnostics. A column
    /// whose type Rocky could not infer produces `I003` instead, and the
    /// declared type is then accepted without ever being compared. With a
    /// cold schema cache that is every leaf column, so an empty error list
    /// is not evidence the types are right (#1618). Empty when every
    /// declared type was actually checked.
    pub unverified_types: Vec<String>,
    /// Optional columns the draft declares that the model does not produce.
    ///
    /// `W010`, dropped by the same error-only filter. A required column that
    /// is missing fails the draft as `E010`; an optional one only warns, so
    /// an LLM that invented a column name was never contradicted.
    pub unmatched_columns: Vec<String>,
    /// `nullable = false` claims the draft made and the loop withheld.
    ///
    /// Each column had no NULLs in the profiled table, but Rocky cannot
    /// prove it non-null from the model SQL and the source schemas it has.
    /// `E012` refuses an unproven `nullable = false`, so the loop drops the
    /// `nullable` key for that column (which promises nothing) instead of
    /// sending the `E012` to the LLM, whose retry rewrote it to
    /// `nullable = true`. One message per column. Empty when every
    /// `nullable = false` in the draft was proven.
    pub unproven_non_null: Vec<String>,
}

/// Map a Rocky `Display` type rendering (e.g. `INT64`, `DECIMAL(10,2)`) to the
/// contract's type-name vocabulary (e.g. `Int64`, `Decimal`).
///
/// The contract file uses Rocky enum-variant names, not the `Display` form;
/// this keeps the prompt evidence aligned with what `validate_contract`
/// expects so the LLM emits matching type strings.
pub fn contract_type_name(typed_col: &TypedColumn) -> String {
    use rocky_compiler::types::RockyType;
    match &typed_col.data_type {
        RockyType::Boolean => "Boolean".to_string(),
        RockyType::Int32 => "Int32".to_string(),
        RockyType::Int64 => "Int64".to_string(),
        RockyType::Float32 => "Float32".to_string(),
        RockyType::Float64 => "Float64".to_string(),
        RockyType::Decimal { .. } | RockyType::DecimalUnsized { .. } => "Decimal".to_string(),
        RockyType::String => "String".to_string(),
        RockyType::Binary => "Binary".to_string(),
        RockyType::Date => "Date".to_string(),
        RockyType::Timestamp => "Timestamp".to_string(),
        RockyType::TimestampNtz => "TimestampNtz".to_string(),
        RockyType::Array(_) => "Array".to_string(),
        RockyType::Map(_, _) => "Map".to_string(),
        RockyType::Struct(_) => "Struct".to_string(),
        RockyType::Variant => "Variant".to_string(),
        RockyType::Unknown => "Unknown".to_string(),
    }
}

/// Render the per-column profile as prompt evidence.
///
/// Pure function: no LLM, no I/O. The output is the body of the user message
/// the LLM sees, so the contract it proposes is grounded in observed data.
pub fn render_profile_evidence(profile: &TableProfile) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "Model `{}` — observed profile of {} column(s):\n\n",
        profile.model,
        profile.columns.len()
    ));

    for col in &profile.columns {
        out.push_str(&format!("- {} ({})\n", col.name, col.type_name));
        out.push_str(&format!(
            "    rows={}, nulls={}, null_rate={:.4}, distinct={}\n",
            col.rows, col.nulls, col.null_rate, col.distinct
        ));
        if let Some(min) = &col.min {
            out.push_str(&format!("    min={min}\n"));
        }
        if let Some(max) = &col.max {
            out.push_str(&format!("    max={max}\n"));
        }
        if !col.observed_values.is_empty() {
            out.push_str(&format!(
                "    observed_domain=[{}]\n",
                col.observed_values.join(", ")
            ));
        }
    }

    out
}

/// Build the system prompt for contract drafting.
///
/// Pinned to the `CompilerContract` TOML shape (`[[columns]]` with
/// `name`/`type`/`nullable`, and a `[rules]` table with `required`/`protected`)
/// so the LLM emits a file that round-trips through `toml::from_str`. The
/// observed domain is offered as evidence but the format carries no
/// `accepted_values` constraint, so the LLM is told not to invent fields.
pub fn build_system_prompt() -> String {
    let mut p = String::new();
    p.push_str(
        "You are a data contract author. Given an observed column profile of a table, \
         propose a Rocky data contract that locks in the invariants worth enforcing on \
         every future run. The data is the evidence; you supply judgment on which \
         constraints matter.\n\n",
    );
    p.push_str("# Output format\n\n");
    p.push_str(
        "Emit ONLY a `.contract.toml` file body. No explanation, no markdown fencing. \
         The exact schema is:\n\n",
    );
    p.push_str(
        "```\n\
         [[columns]]\n\
         name = \"<column>\"          # required\n\
         type = \"<RockyType>\"        # optional: Boolean, Int32, Int64, Float32, Float64, Decimal, String, Binary, Date, Timestamp, TimestampNtz, Array, Map, Struct, Variant\n\
         nullable = <true|false>     # optional\n\
         description = \"...\"         # optional, documentation only\n\n\
         [rules]\n\
         required = [\"<column>\", ...]   # columns that must always exist\n\
         protected = [\"<column>\", ...]  # columns that must never be removed\n\
         ```\n\n",
    );
    p.push_str("# Rules\n\n");
    // The `nullable = false` rule below follows the data alone. A claim Rocky
    // cannot prove is withheld by `withhold_unproven_non_null`, not retried.
    p.push_str(
        "- Use ONLY the `type` names listed above; they must match the observed type for each column.\n\
         - Set `nullable = false` for any column with an observed null_rate of exactly 0; otherwise leave it nullable.\n\
         - Mark stable identity/key columns and never-null business-critical columns as `required` and `protected`.\n\
         - Do NOT invent fields the schema does not list (there is no `accepted_values` or `min`/`max` field).\n\
         - Do NOT wrap the output in any block or add a header.\n",
    );
    p
}

/// Extract a `.contract.toml` body from an LLM response, stripping markdown
/// fences if present. Mirrors `generate::extract_code`.
pub fn extract_toml(content: &str) -> String {
    let trimmed = content.trim();
    if let Some(rest) = trimmed.strip_prefix("```") {
        let rest = rest
            .strip_prefix("toml")
            .or_else(|| rest.strip_prefix("ini"))
            .unwrap_or(rest);
        if let Some(body) = rest.strip_suffix("```") {
            return body.trim().to_string();
        }
    }
    trimmed.to_string()
}

/// Validate a candidate contract TOML against the model's inferred schema.
///
/// Returns the parsed contract on success, or a human-readable error string
/// (TOML parse failure or contract diagnostics) suitable for feeding back into
/// the next retry's error context.
/// A contract that passed the verify loop, plus what the loop could not check.
#[derive(Debug)]
struct VerifiedContract {
    contract: CompilerContract,
    /// `I003` messages: declared types the compiler did not compare because it
    /// could not infer the column's own type.
    unverified_types: Vec<String>,
    /// `W010` messages: optional columns the draft declares that the model
    /// does not produce.
    unmatched_columns: Vec<String>,
    /// `nullable = false` claims withheld because Rocky could not prove them.
    /// See [`withhold_unproven_non_null`].
    unproven_non_null: Vec<String>,
}

fn verify_contract_toml(
    toml_body: &str,
    profile: &TableProfile,
    inferred_schema: &[TypedColumn],
) -> Result<VerifiedContract, String> {
    if toml_body.trim().is_empty() {
        return Err("generated contract is empty".to_string());
    }
    let mut contract: CompilerContract =
        toml::from_str(toml_body).map_err(|e| format!("contract TOML parse error: {e}"))?;

    let unproven_non_null = withhold_unproven_non_null(&mut contract, profile, inferred_schema);

    let diagnostics = validate_contract(&profile.model, inferred_schema, &contract);
    let errors: Vec<String> = diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(std::string::ToString::to_string)
        .collect();

    if !errors.is_empty() {
        return Err(errors.join("\n"));
    }

    // Carried, not dropped. `validate_contract` emits exactly two diagnostics
    // that do not fail this loop, and both mean the draft was accepted with
    // something unconfirmed:
    //
    //   I003  the declared type was never compared, because Rocky could not
    //         infer the column's own type
    //   W010  the draft declares an OPTIONAL column the model does not
    //         produce (a required one fails as E010)
    //
    // Every other code it emits is error severity and has already returned
    // above. Without these two, a caller cannot tell a verified draft from
    // one nothing checked (#1618).
    let collect_code = |code: &str| -> Vec<String> {
        diagnostics
            .iter()
            .filter(|d| &*d.code == code)
            .map(|d| d.message.to_string())
            .collect()
    };

    Ok(VerifiedContract {
        contract,
        unverified_types: collect_code(rocky_compiler::diagnostic::I003),
        unmatched_columns: collect_code(rocky_compiler::diagnostic::W010),
        unproven_non_null,
    })
}

/// Drop each `nullable = false` the data supports but Rocky cannot prove.
///
/// A contract's `nullable = false` is a promise Rocky must prove from the
/// model SQL and the source schemas (ADR-CONTRACTS §4). A source column with
/// no `NOT NULL` constraint, or one whose schema Rocky does not have, is not
/// proven non-null, so `E012` refuses the claim. That refusal is right. But
/// the drafter used to hand it to the LLM as retry context, and the only
/// change the LLM can make to a contract is to write `nullable = true`. The
/// retry then recorded "may be NULL" for a column the profile showed holds
/// no NULL at all.
///
/// The drafter can tell the cases apart, because it has the profile:
///
/// | Rocky's inference       | profiled NULLs  | the draft's `nullable = false` |
/// |-------------------------|-----------------|--------------------------------|
/// | proven non-null         | any             | kept; `E012` cannot fire       |
/// | not proven (or unknown) | 0, in rows > 0  | withheld here and reported     |
/// | not proven (or unknown) | > 0, or no rows | kept; `E012` goes to the retry |
///
/// "Not proven" covers both a column Rocky knows may be NULL and one whose
/// nullability it does not know: `TypedColumn::nullable` is `true` for both.
/// Zero profiled NULLs is evidence about today's table, not proof about
/// future runs, which is why the claim is dropped rather than kept.
/// The withheld column keeps every other key, so its type and its place in
/// `required` / `protected` still go through validation. Withholding only
/// weakens the draft; it never makes it claim non-null. Returns one message
/// per withheld column.
fn withhold_unproven_non_null(
    contract: &mut CompilerContract,
    profile: &TableProfile,
    inferred_schema: &[TypedColumn],
) -> Vec<String> {
    let mut withheld = Vec::new();
    for column in &mut contract.columns {
        if column.nullable != Some(false) {
            continue;
        }
        // A column the model does not produce is left to E010 / W010.
        let Some(inferred) = inferred_schema.iter().find(|c| c.name == column.name) else {
            continue;
        };
        if !inferred.nullable {
            continue;
        }
        let Some(observed) = profile.columns.iter().find(|c| c.name == column.name) else {
            continue;
        };
        if observed.rows == 0 || observed.nulls != 0 {
            continue;
        }
        column.nullable = None;
        withheld.push(format!(
            "column '{}' had no NULLs in {} profiled row(s), but Rocky cannot prove it is \
             non-null from the model SQL and the source schemas it has, so the draft leaves \
             `nullable` unset. To make `nullable = false` checkable, COALESCE it in the model \
             to a value that is never NULL: a literal default or a NOT NULL column. A NOT NULL source column also works, but only once Rocky's \
             schema cache holds that table: a replication pipeline's `rocky run` or `rocky \
             discover --with-schemas` can fill it; a transformation-only pipeline does not",
            column.name, observed.rows
        ));
    }
    withheld
}

/// Draft a data contract for a model from its observed profile.
///
/// Renders the profile evidence into the user message, asks the LLM for a
/// `.contract.toml`, then runs a compile-verify loop: parse the TOML and run
/// `validate_contract` against `inferred_schema`. On failure the diagnostics
/// become the next retry's error context. The loop is bounded both by
/// `max_attempts` and by the client's cumulative output-token budget, matching
/// the model-generation loop.
pub async fn draft_contract(
    profile: &TableProfile,
    inferred_schema: &[TypedColumn],
    client: &LlmClient,
    max_attempts: usize,
) -> Result<DraftedContract, AiError> {
    let system = build_system_prompt();
    let evidence = render_profile_evidence(profile);
    let mut error_context: Option<String> = None;
    let token_budget: u64 = u64::from(client.max_tokens());
    let mut consumed_output_tokens: u64 = 0;

    for attempt in 1..=max_attempts {
        let response = client
            .generate(&system, &evidence, error_context.as_deref())
            .await?;

        consumed_output_tokens =
            consumed_output_tokens.saturating_add(response.output_tokens.unwrap_or(0));

        let toml_body = extract_toml(&response.content);
        match verify_contract_toml(&toml_body, profile, inferred_schema) {
            Ok(verified) => {
                // Re-serialize from the parsed struct so the on-disk file is
                // canonical TOML regardless of the LLM's formatting.
                let toml =
                    toml::to_string_pretty(&verified.contract).map_err(|e| AiError::Api {
                        message: format!("contract re-serialize failed: {e}"),
                    })?;
                return Ok(DraftedContract {
                    contract: verified.contract,
                    toml,
                    attempts: attempt,
                    unverified_types: verified.unverified_types,
                    unmatched_columns: verified.unmatched_columns,
                    unproven_non_null: verified.unproven_non_null,
                });
            }
            Err(errors) => {
                if consumed_output_tokens > token_budget {
                    tracing::warn!(
                        attempt,
                        consumed_output_tokens,
                        budget = token_budget,
                        "AI token budget exceeded; aborting contract compile-verify loop"
                    );
                    return Err(AiError::TokenBudgetExceeded {
                        attempts: attempt,
                        consumed_output_tokens,
                        budget: client.max_tokens(),
                    });
                }
                if attempt < max_attempts {
                    tracing::warn!(
                        attempt,
                        errors = errors.as_str(),
                        "drafted contract failed validation, retrying"
                    );
                    error_context = Some(errors);
                } else {
                    return Err(AiError::CompileFailed {
                        attempts: max_attempts,
                    });
                }
            }
        }
    }

    Err(AiError::CompileFailed {
        attempts: max_attempts,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_compiler::types::RockyType;

    fn tc(name: &str, ty: RockyType, nullable: bool) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable,
        }
    }

    fn sample_profile() -> TableProfile {
        TableProfile {
            model: "orders".to_string(),
            columns: vec![
                ColumnProfile {
                    name: "id".to_string(),
                    type_name: "Int64".to_string(),
                    rows: 1000,
                    nulls: 0,
                    null_rate: 0.0,
                    distinct: 1000,
                    observed_values: vec![],
                    min: Some("1".to_string()),
                    max: Some("1000".to_string()),
                },
                ColumnProfile {
                    name: "status".to_string(),
                    type_name: "String".to_string(),
                    rows: 1000,
                    nulls: 0,
                    null_rate: 0.0,
                    distinct: 3,
                    observed_values: vec![
                        "completed".to_string(),
                        "pending".to_string(),
                        "cancelled".to_string(),
                    ],
                    min: None,
                    max: None,
                },
            ],
        }
    }

    #[test]
    fn contract_type_name_maps_variants() {
        assert_eq!(
            contract_type_name(&tc("a", RockyType::Int64, false)),
            "Int64"
        );
        assert_eq!(
            contract_type_name(&tc("b", RockyType::String, true)),
            "String"
        );
        assert_eq!(
            contract_type_name(&tc(
                "c",
                RockyType::Decimal {
                    precision: 10,
                    scale: 2
                },
                false
            )),
            "Decimal"
        );
        assert_eq!(contract_type_name(&tc("d", RockyType::Date, false)), "Date");
    }

    #[test]
    fn evidence_includes_profile_facts() {
        let evidence = render_profile_evidence(&sample_profile());
        assert!(evidence.contains("Model `orders`"));
        assert!(evidence.contains("id (Int64)"));
        assert!(evidence.contains("null_rate=0.0000"));
        assert!(evidence.contains("distinct=3"));
        // The low-cardinality domain is surfaced as evidence.
        assert!(evidence.contains("observed_domain=[completed, pending, cancelled]"));
        // min/max render when present, are omitted when absent.
        assert!(evidence.contains("min=1"));
        assert!(evidence.contains("max=1000"));
    }

    #[test]
    fn system_prompt_pins_the_contract_shape() {
        let p = build_system_prompt();
        assert!(p.contains("[[columns]]"));
        assert!(p.contains("[rules]"));
        assert!(p.contains("required"));
        assert!(p.contains("protected"));
        // The format has no accepted_values field; the prompt must say so.
        assert!(p.contains("accepted_values"));
    }

    #[test]
    fn extract_toml_strips_fences() {
        let fenced = "```toml\n[[columns]]\nname = \"id\"\n```";
        assert_eq!(extract_toml(fenced), "[[columns]]\nname = \"id\"");
        let plain = "[[columns]]\nname = \"id\"";
        assert_eq!(extract_toml(plain), plain);
    }

    #[test]
    fn verify_accepts_a_well_formed_contract() {
        let schema = vec![
            tc("id", RockyType::Int64, false),
            tc("status", RockyType::String, false),
        ];
        let toml_body = r#"
[[columns]]
name = "id"
type = "Int64"
nullable = false

[[columns]]
name = "status"
type = "String"
nullable = false

[rules]
required = ["id"]
protected = ["id"]
"#;
        let verified =
            verify_contract_toml(toml_body, &sample_profile(), &schema).expect("should verify");
        assert_eq!(verified.contract.columns.len(), 2);
        assert_eq!(verified.contract.rules.required, vec!["id".to_string()]);
        assert!(
            verified.unverified_types.is_empty(),
            "every declared type was checked against a known column type: {:?}",
            verified.unverified_types
        );
        assert!(
            verified.unmatched_columns.is_empty(),
            "every declared column exists in the model: {:?}",
            verified.unmatched_columns
        );
    }

    /// The same error-only filter drops `W010`: an OPTIONAL column the draft
    /// declares that the model does not produce. A required one fails as
    /// `E010`, so only this arm was silent — and an LLM that invented a
    /// column name was never contradicted.
    #[test]
    fn verify_reports_a_declared_column_the_model_does_not_produce() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let toml_body = r#"
[[columns]]
name = "id"
type = "Int64"

[[columns]]
name = "invented"
type = "String"
"#;
        let verified =
            verify_contract_toml(toml_body, &sample_profile(), &schema).expect("should verify");
        assert_eq!(
            verified.unmatched_columns.len(),
            1,
            "expected one unmatched column: {:?}",
            verified.unmatched_columns
        );
        assert!(
            verified.unmatched_columns[0].contains("invented"),
            "the message must name the column: {}",
            verified.unmatched_columns[0]
        );
    }

    /// #1618: a column whose type Rocky could not infer produces `I003`, not
    /// an error, so the declared type is accepted without ever being
    /// compared. The verify loop passes — and must say what it did not check.
    /// With a cold schema cache this is every leaf column.
    #[test]
    fn verify_reports_a_declared_type_it_could_not_check() {
        let schema = vec![
            tc("id", RockyType::Int64, false),
            tc("amount", RockyType::Unknown, true),
        ];
        let toml_body = r#"
[[columns]]
name = "id"
type = "Int64"

[[columns]]
name = "amount"
type = "Decimal"
"#;
        let verified =
            verify_contract_toml(toml_body, &sample_profile(), &schema).expect("should verify");
        assert_eq!(
            verified.unverified_types.len(),
            1,
            "expected one unchecked declared type: {:?}",
            verified.unverified_types
        );
        let only = &verified.unverified_types[0];
        assert!(
            only.contains("amount") && only.contains("could not work out"),
            "the message must name the column and say the type was not checked: {only}"
        );
    }

    #[test]
    fn verify_rejects_required_column_absent_from_schema() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        // `email` is required by the contract but missing from the schema —
        // validate_contract emits E010 (an error).
        let toml_body = r#"
[rules]
required = ["email"]
"#;
        let err = verify_contract_toml(toml_body, &sample_profile(), &schema).unwrap_err();
        assert!(err.contains("E010"), "expected E010 in: {err}");
    }

    #[test]
    fn verify_rejects_type_mismatch() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let toml_body = r#"
[[columns]]
name = "id"
type = "String"
"#;
        let err = verify_contract_toml(toml_body, &sample_profile(), &schema).unwrap_err();
        assert!(err.contains("E011"), "expected E011 in: {err}");
    }

    #[test]
    fn verify_rejects_unparseable_toml() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let err =
            verify_contract_toml("this is = = not toml", &sample_profile(), &schema).unwrap_err();
        assert!(err.contains("parse error"), "got: {err}");
    }

    #[test]
    fn verify_rejects_empty_body() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let err = verify_contract_toml("   ", &sample_profile(), &schema).unwrap_err();
        assert!(err.contains("empty"), "got: {err}");
    }

    /// A one-table profile: `rows` rows, `nulls` of them NULL in each column.
    fn profile_of(model: &str, columns: &[&str], rows: u64, nulls: u64) -> TableProfile {
        TableProfile {
            model: model.to_string(),
            columns: columns
                .iter()
                .map(|name| ColumnProfile {
                    name: (*name).to_string(),
                    type_name: "Unknown".to_string(),
                    rows,
                    nulls,
                    null_rate: if rows == 0 {
                        0.0
                    } else {
                        nulls as f64 / rows as f64
                    },
                    distinct: rows,
                    observed_values: vec![],
                    min: None,
                    max: None,
                })
                .collect(),
        }
    }

    fn non_null_draft(columns: &[&str]) -> String {
        columns
            .iter()
            .map(|c| format!("[[columns]]\nname = \"{c}\"\nnullable = false\n\n"))
            .collect::<String>()
            + &format!(
                "[rules]\nrequired = [{}]\n",
                columns
                    .iter()
                    .map(|c| format!("\"{c}\""))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
    }

    /// The reproduction. A passthrough model over a source whose schema Rocky
    /// does not have (a cold schema cache) infers every column as
    /// `(Unknown, nullable)`. The profile shows no NULL in any of 500 rows,
    /// so the LLM drafts `nullable = false` for each. That used to fail as
    /// six `E012`s, and the retry rewrote all six to `nullable = true`. The
    /// draft must now pass on the first attempt, with `nullable` unset (not
    /// `true`) and each withheld claim reported.
    #[test]
    fn unproven_non_null_on_a_passthrough_is_withheld_not_flipped() {
        let cols = [
            "order_id",
            "customer_id",
            "product_id",
            "amount",
            "status",
            "order_date",
        ];
        let schema: Vec<TypedColumn> = cols
            .iter()
            .map(|c| tc(c, RockyType::Unknown, true))
            .collect();
        let profile = profile_of("raw_orders", &cols, 500, 0);

        let verified = verify_contract_toml(&non_null_draft(&cols), &profile, &schema)
            .expect("an unproven but observed non-null column must not fail the draft");

        for column in &verified.contract.columns {
            assert_eq!(
                column.nullable, None,
                "'{}' must be left unset, never `nullable = true` or an unproven `false`",
                column.name
            );
        }
        assert_eq!(verified.unproven_non_null.len(), cols.len());
        for (message, col) in verified.unproven_non_null.iter().zip(cols) {
            assert!(
                message.contains(&format!("'{col}'")) && message.contains("500"),
                "the message names the column and the evidence: {message}"
            );
        }
        // The rest of the draft still went through validation.
        assert_eq!(verified.contract.rules.required.len(), cols.len());
    }

    /// Proven non-null: Rocky's inference says the column cannot hold NULL
    /// (for example a `NOT NULL` source column passed through), so the draft's
    /// `nullable = false` is kept and nothing is reported.
    #[test]
    fn proven_non_null_keeps_the_claim() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let profile = profile_of("orders", &["id"], 10, 0);
        let verified = verify_contract_toml(&non_null_draft(&["id"]), &profile, &schema)
            .expect("a proven non-null column verifies");
        assert_eq!(verified.contract.columns[0].nullable, Some(false));
        assert!(verified.unproven_non_null.is_empty());
    }

    /// A column Rocky KNOWS may be NULL (a typed source column with no
    /// `NOT NULL`, or a LEFT JOIN's null-extended side) that held no NULL in
    /// the profile is withheld too, while a proven column in the same draft
    /// keeps its `nullable = false`.
    #[test]
    fn a_known_nullable_column_is_withheld_beside_a_proven_one() {
        let schema = vec![
            tc("id", RockyType::Int64, false),
            tc("customer_id", RockyType::Int64, true),
        ];
        let profile = profile_of("orders", &["id", "customer_id"], 10, 0);
        let verified =
            verify_contract_toml(&non_null_draft(&["id", "customer_id"]), &profile, &schema)
                .expect("the draft verifies");
        assert_eq!(verified.contract.columns[0].nullable, Some(false));
        assert_eq!(verified.contract.columns[1].nullable, None);
        assert_eq!(verified.unproven_non_null.len(), 1);
        assert!(verified.unproven_non_null[0].contains("'customer_id'"));
    }

    /// Proven nullable, and the data agrees: the profile counted NULLs, so the
    /// draft's `nullable = false` contradicts its own evidence. `E012` still
    /// fails the attempt and goes back to the LLM.
    #[test]
    fn a_column_with_observed_nulls_still_fails_e012() {
        for ty in [RockyType::Int64, RockyType::Unknown] {
            let schema = vec![tc("id", ty.clone(), true)];
            let profile = profile_of("orders", &["id"], 10, 3);
            let err = verify_contract_toml(&non_null_draft(&["id"]), &profile, &schema)
                .expect_err("observed NULLs must not be withheld");
            assert!(err.contains("E012"), "{ty:?}: expected E012 in: {err}");
        }
    }

    /// Unknown with no evidence: an empty table proves nothing either way, so
    /// the unproven claim is not withheld and `E012` still fires.
    #[test]
    fn an_empty_profile_is_not_evidence_of_non_null() {
        let schema = vec![tc("id", RockyType::Unknown, true)];
        let profile = profile_of("orders", &["id"], 0, 0);
        let err = verify_contract_toml(&non_null_draft(&["id"]), &profile, &schema)
            .expect_err("zero rows is not evidence");
        assert!(err.contains("E012"), "expected E012 in: {err}");
    }

    /// A column the profile does not cover is not withheld: there is no
    /// evidence for it, so `E012` still fires.
    #[test]
    fn a_column_missing_from_the_profile_is_not_withheld() {
        let schema = vec![tc("id", RockyType::Unknown, true)];
        let profile = profile_of("orders", &["other"], 10, 0);
        let err = verify_contract_toml(&non_null_draft(&["id"]), &profile, &schema)
            .expect_err("no profile, no evidence");
        assert!(err.contains("E012"), "expected E012 in: {err}");
    }
}
