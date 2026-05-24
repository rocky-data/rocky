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
        RockyType::Decimal { .. } => "Decimal".to_string(),
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
fn verify_contract_toml(
    toml_body: &str,
    model_name: &str,
    inferred_schema: &[TypedColumn],
) -> Result<CompilerContract, String> {
    if toml_body.trim().is_empty() {
        return Err("generated contract is empty".to_string());
    }
    let contract: CompilerContract =
        toml::from_str(toml_body).map_err(|e| format!("contract TOML parse error: {e}"))?;

    let diagnostics = validate_contract(model_name, inferred_schema, &contract);
    let errors: Vec<String> = diagnostics
        .iter()
        .filter(|d| d.is_error())
        .map(std::string::ToString::to_string)
        .collect();

    if errors.is_empty() {
        Ok(contract)
    } else {
        Err(errors.join("\n"))
    }
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
        match verify_contract_toml(&toml_body, &profile.model, inferred_schema) {
            Ok(contract) => {
                // Re-serialize from the parsed struct so the on-disk file is
                // canonical TOML regardless of the LLM's formatting.
                let toml = toml::to_string_pretty(&contract).map_err(|e| AiError::Api {
                    message: format!("contract re-serialize failed: {e}"),
                })?;
                return Ok(DraftedContract {
                    contract,
                    toml,
                    attempts: attempt,
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
        let contract = verify_contract_toml(toml_body, "orders", &schema).expect("should verify");
        assert_eq!(contract.columns.len(), 2);
        assert_eq!(contract.rules.required, vec!["id".to_string()]);
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
        let err = verify_contract_toml(toml_body, "orders", &schema).unwrap_err();
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
        let err = verify_contract_toml(toml_body, "orders", &schema).unwrap_err();
        assert!(err.contains("E011"), "expected E011 in: {err}");
    }

    #[test]
    fn verify_rejects_unparseable_toml() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let err = verify_contract_toml("this is = = not toml", "orders", &schema).unwrap_err();
        assert!(err.contains("parse error"), "got: {err}");
    }

    #[test]
    fn verify_rejects_empty_body() {
        let schema = vec![tc("id", RockyType::Int64, false)];
        let err = verify_contract_toml("   ", "orders", &schema).unwrap_err();
        assert!(err.contains("empty"), "got: {err}");
    }
}
