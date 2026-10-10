//! Every value this process substituted into config from the environment.
//!
//! The registry itself, and the text helpers over it, live in the
//! dependency-free `rocky-secret-registry` crate so that crates below
//! `rocky-core` can render through it too. They are re-exported here
//! unchanged; see that crate for the design and its limits (#1897, #1919).
//!
//! This module adds the JSON helpers, which need `serde_json`.

pub use rocky_secret_registry::{
    SECRET_LENGTH_FLOOR, fmt_rendered_debug, is_empty, register_substitution, render_placeholders,
    substitutions,
};

/// [`render_placeholders`] over every string in a JSON document: string
/// values, object keys and numbers, at any depth. A number that holds a
/// resolved value prints as a string. Booleans and structure are left as
/// they are.
///
/// For an output that is built as a `serde_json::Value` from a config type
/// rather than from typed fields, such as a plan's config snapshot (#1919).
pub fn render_json_placeholders(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    if is_empty() {
        return value;
    }
    match value {
        Value::String(s) => Value::String(render_placeholders(&s)),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(render_json_placeholders).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (render_placeholders(&k), render_json_placeholders(v)))
                .collect(),
        ),
        // A number whose digits hold a resolved value prints as a string.
        Value::Number(n) => {
            let text = n.to_string();
            let rendered = render_placeholders(&text);
            if rendered == text {
                Value::Number(n)
            } else {
                Value::String(rendered)
            }
        }
        other => other,
    }
}

/// A copy of `value` with every registered value in it written as `${NAME}`.
///
/// Serializes to JSON, renders every string VALUE with
/// [`render_placeholders`] and reads the result back as the same type. For an
/// output struct that embeds a config type wholesale, so its JSON schema stays
/// the config type's own.
///
/// Two things are never rewritten, because the type reads them back as
/// structure rather than as text:
///
/// - **Object keys.** A key is a field name or a map key. Rewriting
///   `timestamp_column` because a value is `timestamp` would make the copy
///   unreadable (#1919 follow-up).
/// - **A string the type will not accept in the `${NAME}` form.** An enum tag
///   such as `#[serde(tag = "type")]`'s `"incremental"`, or a unit variant,
///   comes from the type's own fixed vocabulary. If rendering every string at
///   once produces a document the type rejects, each string is rendered on its
///   own and kept only where the type still accepts it. A string left as it was
///   is one the schema spells out, so printing it discloses nothing the schema
///   does not.
///
/// The returned copy is for printing only. Its strings are placeholders, not
/// the values the code runs with.
///
/// # Errors
///
/// Only when `T` does not round-trip through JSON on its own, before any
/// rendering. A rendered form the type rejects is never an error.
pub fn render_placeholders_in<T>(value: &T) -> Result<T, serde_json::Error>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    let resolved = serde_json::to_value(value)?;
    if is_empty() {
        return serde_json::from_value(resolved);
    }
    if let Ok(rendered) = serde_json::from_value(render_json_string_values(resolved.clone())) {
        return Ok(rendered);
    }
    // One leaf at a time, keeping each rendering the type still accepts.
    let mut current = resolved;
    let mut paths = Vec::new();
    collect_string_leaf_paths(&current, &mut Vec::new(), &mut paths);
    for path in paths {
        let mut candidate = current.clone();
        let Some(serde_json::Value::String(leaf)) = json_at_path_mut(&mut candidate, &path) else {
            continue;
        };
        let rendered = render_placeholders(leaf);
        if rendered == *leaf {
            continue;
        }
        *leaf = rendered;
        if serde_json::from_value::<T>(candidate.clone()).is_ok() {
            current = candidate;
        }
    }
    serde_json::from_value(current)
}

/// [`render_placeholders`] over every string value in a JSON document, at any
/// depth. Object keys are left as they are (see [`render_placeholders_in`]).
pub(crate) fn render_json_string_values(value: serde_json::Value) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::String(s) => Value::String(render_placeholders(&s)),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(render_json_string_values).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (k, render_json_string_values(v)))
                .collect(),
        ),
        other => other,
    }
}

/// One step into a JSON document: an object key or an array index.
#[derive(Clone)]
enum JsonStep {
    Key(String),
    Index(usize),
}

/// The path of every string leaf in `value`, in document order.
fn collect_string_leaf_paths(
    value: &serde_json::Value,
    prefix: &mut Vec<JsonStep>,
    out: &mut Vec<Vec<JsonStep>>,
) {
    use serde_json::Value;
    match value {
        Value::String(_) => out.push(prefix.clone()),
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                prefix.push(JsonStep::Index(i));
                collect_string_leaf_paths(item, prefix, out);
                prefix.pop();
            }
        }
        Value::Object(map) => {
            for (k, v) in map {
                prefix.push(JsonStep::Key(k.clone()));
                collect_string_leaf_paths(v, prefix, out);
                prefix.pop();
            }
        }
        _ => {}
    }
}

fn json_at_path_mut<'a>(
    value: &'a mut serde_json::Value,
    path: &[JsonStep],
) -> Option<&'a mut serde_json::Value> {
    path.iter().try_fold(value, |node, step| match step {
        JsonStep::Key(k) => node.get_mut(k.as_str()),
        JsonStep::Index(i) => node.get_mut(*i),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_render_covers_numbers_and_fmt_rendered_debug_renders_display() {
        register_substitution("RV_REG_NUM", "4815162343");
        let v = render_json_placeholders(
            serde_json::json!({"n": 4815162343u64, "k": [4815162343u64], "f": 1.5}),
        );
        assert_eq!(
            v,
            serde_json::json!({"n": "${RV_REG_NUM}", "k": ["${RV_REG_NUM}"], "f": 1.5})
        );

        struct Plain(&'static str);
        impl std::fmt::Display for Plain {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.0)
            }
        }
        impl std::fmt::Debug for Plain {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                fmt_rendered_debug(f, "Plain", self)
            }
        }
        // A Display that never renders is still rendered by the Debug helper.
        assert_eq!(
            format!("{:?}", Plain("x 4815162343 y")),
            "Plain(x ${RV_REG_NUM} y)"
        );
    }

    #[test]
    fn render_json_placeholders_rewrites_values_and_keys_at_any_depth() {
        let secret = "ROCKY-RENDER-JSON-5e0c71aa";
        register_substitution("ROCKY_RENDER_JSON", secret);
        let doc = serde_json::json!({
            "adapter": { secret: { "path": format!("/data/{secret}.db"), "port": 5432 } },
            "list": [secret, true, null],
        });
        let out = render_json_placeholders(doc).to_string();
        assert!(!out.contains(secret), "{out}");
        assert!(out.contains("/data/${ROCKY_RENDER_JSON}.db"), "{out}");
        assert!(
            out.contains("\"${ROCKY_RENDER_JSON}\":{"),
            "keys too: {out}"
        );
        assert!(out.contains("5432"), "{out}");
    }

    /// The shape of `StrategyConfig` the round-trip has to survive: an
    /// internally tagged enum with a field named like a value. The registry is
    /// process-global, so the tag and the field name are made unique here
    /// rather than reusing `incremental` / `timestamp_column`, which would
    /// rewrite other tests' text.
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(tag = "type")]
    enum Tagged {
        #[serde(rename = "rockytagmodeqq")]
        Incremental { rockykeyzzqq_column: String },
        #[serde(rename = "rockyunitmodeqq")]
        FullRefresh,
    }

    /// #1919 follow-up: a registered value equal to an enum tag (`type =
    /// "${MODE}"` resolving to `incremental`) cannot be rendered, because the
    /// type would no longer read back. The copy keeps the tag and still
    /// renders every other string.
    #[test]
    fn render_placeholders_in_keeps_an_enum_tag_the_type_needs() {
        register_substitution("ROCKY_T1919_TAG_MODE", "rockytagmodeqq");
        register_substitution("ROCKY_T1919_UNIT_MODE", "rockyunitmodeqq");
        register_substitution("ROCKY_T1919_TAG_COL", "ROCKY-T1919-TAG-COLUMN-3f9a");
        let value = Tagged::Incremental {
            rockykeyzzqq_column: "ROCKY-T1919-TAG-COLUMN-3f9a".to_string(),
        };
        let out = render_placeholders_in(&value).expect("renders without failing");
        assert_eq!(
            out,
            Tagged::Incremental {
                rockykeyzzqq_column: "${ROCKY_T1919_TAG_COL}".to_string(),
            }
        );
        assert_eq!(
            render_placeholders_in(&Tagged::FullRefresh).expect("unit variant"),
            Tagged::FullRefresh
        );
    }

    /// #1919 follow-up: a registered value inside a field NAME (`timestamp`
    /// inside `timestamp_column`) is never rewritten. Keys are structure.
    #[test]
    fn render_placeholders_in_never_rewrites_a_key() {
        register_substitution("ROCKY_T1919_KEY_TS", "rockykeyzzqq");
        let value = Tagged::Incremental {
            rockykeyzzqq_column: "rockykeyzzqq".to_string(),
        };
        let out = render_placeholders_in(&value).expect("renders without failing");
        assert_eq!(
            out,
            Tagged::Incremental {
                rockykeyzzqq_column: "${ROCKY_T1919_KEY_TS}".to_string(),
            }
        );
    }
}
