use std::collections::HashMap;

use thiserror::Error;

use super::HookContext;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum TemplateError {
    #[error("failed to serialize hook context: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("unclosed conditional block: {{{{#if {field}}}}}")]
    UnclosedConditional { field: String },
}

// ---------------------------------------------------------------------------
// Template rendering
// ---------------------------------------------------------------------------

/// Renders a Mustache-style template using the given HookContext.
///
/// Supported syntax:
/// - `{{field}}` — top-level fields: event, run_id, pipeline, model, table, error,
///   timestamp, duration_ms
/// - `{{metadata.key}}` — nested metadata lookup
/// - `{{#if field}}...{{/if}}` — conditional: renders inner content only when the
///   field is present and non-empty
///
/// Unknown fields resolve to empty string (not an error).
pub fn render_template(template: &str, context: &HookContext) -> Result<String, TemplateError> {
    // Build a flat lookup map from the context
    let lookup = build_lookup(context)?;

    // First pass: resolve conditionals
    let after_conditionals = resolve_conditionals(template, &lookup)?;

    // Second pass: substitute {{field}} placeholders
    let result = substitute_fields(&after_conditionals, &lookup);

    Ok(result)
}

/// If no template is provided, serialize the full HookContext as JSON.
pub fn render_or_serialize(
    template: Option<&str>,
    context: &HookContext,
) -> Result<String, TemplateError> {
    match template {
        Some(t) => render_template(t, context),
        None => Ok(serde_json::to_string(context)?),
    }
}

// ---------------------------------------------------------------------------
// Internal: build flat lookup map
// ---------------------------------------------------------------------------

fn build_lookup(ctx: &HookContext) -> Result<HashMap<String, String>, TemplateError> {
    let mut map = HashMap::new();

    // Top-level fields
    map.insert("event".to_string(), ctx.event.config_key().to_string());
    map.insert("run_id".to_string(), ctx.run_id.clone());
    map.insert("pipeline".to_string(), ctx.pipeline.clone());
    map.insert("timestamp".to_string(), ctx.timestamp.to_rfc3339());

    if let Some(ref model) = ctx.model {
        map.insert("model".to_string(), model.clone());
    }
    if let Some(ref table) = ctx.table {
        map.insert("table".to_string(), table.clone());
    }
    if let Some(ref error) = ctx.error {
        map.insert("error".to_string(), error.clone());
    }
    if let Some(duration) = ctx.duration_ms {
        map.insert("duration_ms".to_string(), duration.to_string());
    }

    // Metadata: flatten to "metadata.key" entries
    for (key, value) in &ctx.metadata {
        let string_value = match value {
            serde_json::Value::String(s) => s.clone(),
            serde_json::Value::Number(n) => n.to_string(),
            serde_json::Value::Bool(b) => b.to_string(),
            serde_json::Value::Null => String::new(),
            other => other.to_string(),
        };
        map.insert(format!("metadata.{key}"), string_value);
    }

    Ok(map)
}

// ---------------------------------------------------------------------------
// Internal: resolve {{#if field}}...{{/if}} conditionals
// ---------------------------------------------------------------------------

fn resolve_conditionals(
    input: &str,
    lookup: &HashMap<String, String>,
) -> Result<String, TemplateError> {
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    loop {
        // Find next {{#if ...}}
        let Some(if_start) = remaining.find("{{#if ") else {
            result.push_str(remaining);
            break;
        };

        // Copy everything before the conditional
        result.push_str(&remaining[..if_start]);

        // Parse the field name
        let after_if = &remaining[if_start + 6..]; // skip "{{#if "
        let Some(close_brace) = after_if.find("}}") else {
            // Malformed — treat as literal
            result.push_str(&remaining[if_start..if_start + 6]);
            remaining = after_if;
            continue;
        };

        let field = after_if[..close_brace].trim();
        let after_open_tag = &after_if[close_brace + 2..];

        // Find the matching {{/if}}
        let Some(endif_pos) = after_open_tag.find("{{/if}}") else {
            return Err(TemplateError::UnclosedConditional {
                field: field.to_string(),
            });
        };

        let inner = &after_open_tag[..endif_pos];
        let after_endif = &after_open_tag[endif_pos + 7..]; // skip "{{/if}}"

        // Include inner content only if field is present and non-empty
        let field_present = lookup.get(field).is_some_and(|v| !v.is_empty());

        if field_present {
            result.push_str(inner);
        }

        remaining = after_endif;
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Internal: substitute {{field}} placeholders
// ---------------------------------------------------------------------------

fn substitute_fields(input: &str, lookup: &HashMap<String, String>) -> String {
    let mut result = String::with_capacity(input.len());
    let mut remaining = input;

    loop {
        let Some(open) = remaining.find("{{") else {
            result.push_str(remaining);
            break;
        };

        result.push_str(&remaining[..open]);

        let after_open = &remaining[open + 2..];
        let Some(close) = after_open.find("}}") else {
            // Unclosed — treat as literal
            result.push_str("{{");
            remaining = after_open;
            continue;
        };

        let field = after_open[..close].trim();

        // Look up value; unknown fields resolve to empty string
        if let Some(value) = lookup.get(field) {
            result.push_str(value);
        }
        // else: empty string (intentionally no output for unknown fields)

        remaining = &after_open[close + 2..];
    }

    result
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::HookContext;

    fn test_context() -> HookContext {
        let mut ctx = HookContext::materialize_error(
            "run-42",
            "raw_replication",
            "cat.sch.tbl",
            "timeout after 30s",
        );
        ctx.model = Some("my_model".to_string());
        ctx.metadata.insert(
            "tables_processed".to_string(),
            serde_json::Value::Number(42.into()),
        );
        ctx
    }

    #[test]
    fn test_simple_substitution() {
        let ctx = test_context();
        let result = render_template("Model: {{model}}", &ctx).unwrap();
        assert_eq!(result, "Model: my_model");
    }

    #[test]
    fn test_multiple_fields() {
        let ctx = test_context();
        let result = render_template("{{model}} in {{pipeline}}", &ctx).unwrap();
        assert_eq!(result, "my_model in raw_replication");
    }

    #[test]
    fn test_nested_metadata() {
        let ctx = test_context();
        let result =
            render_template("Processed: {{metadata.tables_processed}} tables", &ctx).unwrap();
        assert_eq!(result, "Processed: 42 tables");
    }

    #[test]
    fn test_conditional_renders_when_present() {
        let ctx = test_context();
        let result = render_template("{{#if error}}Error: {{error}}{{/if}}", &ctx).unwrap();
        assert_eq!(result, "Error: timeout after 30s");
    }

    #[test]
    fn test_conditional_omitted_when_absent() {
        let ctx = HookContext::pipeline_start("run-1", "pipe");
        let result =
            render_template("Start{{#if error}} Error: {{error}}{{/if}} done", &ctx).unwrap();
        assert_eq!(result, "Start done");
    }

    #[test]
    fn test_unknown_field_resolves_to_empty() {
        let ctx = test_context();
        let result = render_template("Value: [{{nonexistent}}]", &ctx).unwrap();
        assert_eq!(result, "Value: []");
    }

    #[test]
    fn test_no_template_serializes_full_json() {
        let ctx = HookContext::pipeline_start("run-1", "pipe");
        let result = render_or_serialize(None, &ctx).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&result).unwrap();
        assert_eq!(parsed["event"], "pipeline_start");
        assert_eq!(parsed["run_id"], "run-1");
    }

    #[test]
    fn test_with_template_renders() {
        let ctx = HookContext::pipeline_start("run-1", "pipe");
        let result = render_or_serialize(Some("event={{event}}"), &ctx).unwrap();
        assert_eq!(result, "event=on_pipeline_start");
    }

    #[test]
    fn test_event_field_uses_config_key() {
        let ctx = HookContext::pipeline_start("run-1", "pipe");
        let result = render_template("{{event}}", &ctx).unwrap();
        assert_eq!(result, "on_pipeline_start");
    }

    #[test]
    fn test_conditional_with_metadata() {
        let ctx = test_context();
        let result = render_template(
            "{{#if metadata.tables_processed}}count={{metadata.tables_processed}}{{/if}}",
            &ctx,
        )
        .unwrap();
        assert_eq!(result, "count=42");
    }

    #[test]
    fn test_unclosed_conditional_error() {
        let ctx = test_context();
        let result = render_template("{{#if error}}no closing", &ctx);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            TemplateError::UnclosedConditional { .. }
        ));
    }

    #[test]
    fn test_empty_template() {
        let ctx = test_context();
        let result = render_template("", &ctx).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn test_no_placeholders() {
        let ctx = test_context();
        let result = render_template("plain text", &ctx).unwrap();
        assert_eq!(result, "plain text");
    }

    #[test]
    fn test_json_body_template() {
        let ctx = test_context();
        let template = r#"{"text": "{{model}} failed: {{error}}"}"#;
        let result = render_template(template, &ctx).unwrap();
        assert_eq!(result, r#"{"text": "my_model failed: timeout after 30s"}"#);
    }
}
