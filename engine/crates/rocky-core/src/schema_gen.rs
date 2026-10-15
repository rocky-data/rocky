//! One place that fixes the JSON Schema dialect Rocky exports.
//!
//! schemars 1.x defaults to JSON Schema 2020-12 (`$defs`). The committed
//! `schemas/*.schema.json` files, the Pydantic and TypeScript generators and
//! the OpenAPI bridge all read Draft-07 (`definitions`, `type: [T, "null"]`
//! for an `Option`). Every schema Rocky exports goes through this helper so
//! the dialect cannot differ between callers.
//!
//! The helper also keeps four spellings that schemars 0.8 used and that the
//! generated bindings were built on: doc comments wrapped on one line per
//! paragraph, `required` lists in sorted order, and a one-value enum written
//! as `enum: [v]` instead of `const: v`. Integer bounds are written as floats
//! (`minimum: 0.0`).

use schemars::JsonSchema;
use schemars::generate::SchemaSettings;
use serde_json::Value;

/// The Draft-07 root schema of `T`, as a JSON value.
///
/// Shared types sit under `definitions`, referenced as `#/definitions/Name`.
pub fn draft07_schema_for<T: JsonSchema + ?Sized>() -> Value {
    let schema = SchemaSettings::draft07()
        .into_generator()
        .into_root_schema_for::<T>();
    let mut value = serde_json::to_value(schema).expect("a root schema serializes to JSON");
    normalize(&mut value);
    value
}

const DATA_KEYWORDS: [&str; 4] = ["default", "examples", "enum", "const"];

fn normalize(value: &mut Value) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(text)) = map.get_mut("description") {
                *text = unwrap_paragraphs(text);
            }
            if let Some(Value::Array(items)) = map.get_mut("required")
                && items.iter().all(Value::is_string)
            {
                items.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            }
            if matches!(map.get("const"), Some(v) if !v.is_object())
                && let Some(constant) = map.remove("const")
            {
                map.insert("enum".to_owned(), Value::Array(vec![constant]));
            }
            for key in ["minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"] {
                if let Some(Value::Number(n)) = map.get(key)
                    && (n.is_i64() || n.is_u64())
                    && let Some(float) = n.as_f64().and_then(serde_json::Number::from_f64)
                {
                    map.insert(key.to_owned(), Value::Number(float));
                }
            }
            // These keys hold data, not schemas: a default value with a
            // `const` or `required` key must come out as written.
            for (key, child) in map.iter_mut() {
                if !DATA_KEYWORDS.contains(&key.as_str()) {
                    normalize(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(normalize),
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

/// Join the lines of each paragraph with a space. A blank line still ends a
/// paragraph, so the `\n\n` between paragraphs stays.
fn unwrap_paragraphs(text: &str) -> String {
    text.split("\n\n")
        .map(|paragraph| {
            paragraph
                .lines()
                .map(str::trim)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::normalize;

    #[test]
    fn data_under_default_and_examples_is_left_as_written() {
        let mut schema = json!({
            "type": "object",
            "const": "x",
            "default": {"const": "y", "required": ["b", "a"], "minimum": 1},
            "examples": [{"description": "line one\nline two"}],
            "properties": {"n": {"type": "integer", "minimum": 0, "required": ["z", "y"]}}
        });
        normalize(&mut schema);
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "enum": ["x"],
                "default": {"const": "y", "required": ["b", "a"], "minimum": 1},
                "examples": [{"description": "line one\nline two"}],
                "properties": {"n": {"type": "integer", "minimum": 0.0, "required": ["y", "z"]}}
            })
        );
    }
}
