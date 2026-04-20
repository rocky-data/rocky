//! System prompt builder for AI model generation.
//!
//! Constructs the LLM system prompt from project context:
//! DSL spec, existing typed models, source schemas, target dialect.

use rocky_compiler::types::TypedColumn;

/// Build the system prompt for model generation.
///
/// Renders each upstream (sources + existing models) with its column names
/// and `RockyType`s so the LLM picks real columns with correct types. This is
/// the "schema-grounded" input that drives first-attempt compile success;
/// the compile-verify loop in `generate.rs` remains the safety net for
/// whatever slips through.
pub fn build_system_prompt(
    model_schemas: &[(String, Vec<TypedColumn>)],
    source_tables: &[(String, Vec<TypedColumn>)],
    format: &str,
) -> String {
    let mut prompt = String::new();

    prompt.push_str("You are a data transformation expert. Generate Rocky model files.\n\n");

    if format == "rocky" {
        prompt.push_str("# Rocky DSL Syntax\n\n");
        prompt.push_str("Rocky DSL is a pipeline-oriented language. Key constructs:\n");
        prompt.push_str("- `from <model>` — start pipeline from a model or source\n");
        prompt.push_str(
            "- `where <predicate>` — filter rows (use == for equality, != for NULL-safe inequality)\n",
        );
        prompt.push_str("- `group <keys> { name: agg(col) }` — aggregate\n");
        prompt.push_str("- `derive { name: expr }` — add computed columns\n");
        prompt.push_str("- `select { col1, col2 }` — choose columns\n");
        prompt.push_str("- `join <model> as <alias> on <key> { keep col1, col2 }` — join\n");
        prompt.push_str("- `sort <col> [asc|desc]` — order results\n");
        prompt.push_str("- `take <n>` — limit rows\n");
        prompt.push_str("- `distinct` — deduplicate\n");
        prompt.push_str("- `@2025-01-01` — date literal\n");
        prompt.push_str("- `!=` compiles to IS DISTINCT FROM (NULL-safe)\n\n");
    } else {
        prompt.push_str("# SQL Model Format\n\n");
        prompt.push_str("Generate a standard SQL SELECT statement.\n");
        prompt.push_str("Reference other models by bare name (e.g., FROM orders).\n\n");
    }

    if !model_schemas.is_empty() {
        prompt.push_str("# Existing Models\n\n");
        prompt.push_str(
            "These models are available as dependencies. Columns and types are shown — reference only these.\n",
        );
        for (name, cols) in model_schemas {
            prompt.push_str(&format!("- {name}: {}\n", render_columns(cols)));
        }
        prompt.push('\n');
    }

    if !source_tables.is_empty() {
        prompt.push_str("# Available Source Tables\n\n");
        prompt.push_str(
            "Each source lists columns with their Rocky types. Do NOT reference columns that are not listed.\n",
        );
        for (table, cols) in source_tables {
            prompt.push_str(&format!("- {table}: {}\n", render_columns(cols)));
        }
        prompt.push('\n');
    }

    prompt.push_str("# Instructions\n\n");
    prompt.push_str("Generate ONLY the model source code. No explanation, no markdown fencing.\n");
    prompt.push_str("Use existing models as dependencies where appropriate.\n");
    prompt.push_str(
        "Only reference columns that appear in the schemas above. Columns that don't exist will fail at warehouse execution time.\n",
    );

    prompt
}

/// Render a column list as a compact one-liner: `id: INT64, amount: DECIMAL(10,2)`.
fn render_columns(cols: &[TypedColumn]) -> String {
    cols.iter()
        .map(|c| format!("{}: {}", c.name, c.data_type))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_compiler::types::RockyType;

    fn tc(name: &str, ty: RockyType) -> TypedColumn {
        TypedColumn {
            name: name.to_string(),
            data_type: ty,
            nullable: true,
        }
    }

    #[test]
    fn test_build_prompt_rocky_with_types() {
        let prompt = build_system_prompt(
            &[(
                "orders".to_string(),
                vec![
                    tc("id", RockyType::Int64),
                    tc(
                        "amount",
                        RockyType::Decimal {
                            precision: 10,
                            scale: 2,
                        },
                    ),
                ],
            )],
            &[(
                "source.raw.customers".to_string(),
                vec![tc("id", RockyType::Int64), tc("email", RockyType::String)],
            )],
            "rocky",
        );
        assert!(prompt.contains("Rocky DSL Syntax"));
        assert!(prompt.contains("orders: id: INT64, amount: DECIMAL(10,2)"));
        assert!(prompt.contains("source.raw.customers: id: INT64, email: STRING"));
        assert!(prompt.contains("warehouse execution"));
    }

    #[test]
    fn test_build_prompt_sql() {
        let prompt = build_system_prompt(&[], &[], "sql");
        assert!(prompt.contains("SQL Model Format"));
        assert!(!prompt.contains("Rocky DSL"));
    }

    #[test]
    fn test_build_prompt_empty_schemas_no_header() {
        let prompt = build_system_prompt(&[], &[], "rocky");
        assert!(!prompt.contains("# Existing Models"));
        assert!(!prompt.contains("# Available Source Tables"));
    }
}
