//! A config string that prints a resolved `${VAR}` value only as `${NAME}`.
//!
//! `${VAR}` is expanded before a config is parsed (#1897). From then on a
//! resolved value is ordinary bytes in an ordinary `String`, and every error,
//! output struct or log line that copies the field can print a live secret.
//! The `rocky serve` response filter closes that for HTTP responses only.
//! CLI terminal output and `--output json` have no such filter (#1878).
//!
//! [`EnvString`] closes it at the type (#1919). It holds two forms:
//!
//! ```text
//!   value      what the code uses       "dapi_abc123_secret"
//!   rendered   what every printer sees  "${DATABRICKS_TOKEN}"
//! ```
//!
//! `Display`, `Debug` and `Serialize` all write `rendered`. The plaintext is
//! reachable only through [`EnvString::expose`], which is greppable, or by
//! serializing inside [`crate::redacted::with_unredacted_scope`], the same
//! guard that [`crate::redacted::RedactedString`] uses.
//!
//! ## Where `rendered` comes from
//!
//! A field built from the substitution itself knows its variable by name, so
//! [`EnvString::substituted`] renders `${NAME}` exactly.
//!
//! A field read from parsed config does not. The parse sees only the expanded
//! text. So [`EnvString::from_resolved`] rewrites every value in the
//! [`crate::secret_registry`] that occurs in the field. That registry holds
//! every value this process expanded from the environment, and a value is
//! registered before the text that holds it is parsed. So a field parsed from
//! expanded config is always rendered against a registry that already holds
//! its values.
//!
//! The registry keeps the owner's 8-byte floor (#1897): a value shorter than
//! [`crate::secret_registry::SECRET_LENGTH_FLOOR`] is not treated as a secret
//! and prints as itself. The literal in `${VAR:-default}` is not registered,
//! because it is already in the config file in cleartext.
//!
//! ## Equality uses the value
//!
//! `PartialEq`, `Eq`, `Ord` and `Hash` compare `value`. Two fields that hold
//! the same bytes are the same field, whatever they print as. That keeps
//! policy matching and map keys exactly as they were with `String`.

use std::fmt;

use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use std::cell::Cell;

use crate::redacted::unredacted_scope_active;

thread_local! {
    /// Depth of [`with_env_values_scope`]. A counter so nested scopes compose.
    static ENV_VALUES_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Run `f` with [`EnvString`] serializing its value instead of `${NAME}`.
///
/// Narrower than [`crate::redacted::with_unredacted_scope`]: a
/// [`crate::redacted::RedactedString`] credential still writes `"***"`. The
/// one use is a keyed digest of the resolved config, which must change when
/// an environment value changes but must not hold or depend on credentials
/// (#1919). Never print what this produces.
pub fn with_env_values_scope<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    ENV_VALUES_DEPTH.with(|d| d.set(d.get().saturating_add(1)));
    // Decrement on panic too, so the thread never stays in this mode.
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            ENV_VALUES_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
        }
    }
    let _guard = Guard;
    f()
}

fn env_values_scope_active() -> bool {
    ENV_VALUES_DEPTH.with(|d| d.get() > 0)
}

/// A config string whose printed form never holds a resolved `${VAR}` value.
///
/// See the [module docs](self).
///
/// ```
/// use rocky_core::env_string::EnvString;
///
/// let token = EnvString::substituted("DEPLOY_TOKEN", "dapi_abc123_secret");
/// assert_eq!(token.to_string(), "${DEPLOY_TOKEN}");
/// // `Debug` quotes the placeholder, as it quotes any string.
/// assert_eq!(format!("{token:?}"), "\"${DEPLOY_TOKEN}\"");
/// assert_eq!(serde_json::to_string(&token).unwrap(), "\"${DEPLOY_TOKEN}\"");
/// assert_eq!(token.expose(), "dapi_abc123_secret");
/// ```
#[derive(Clone)]
pub struct EnvString(Box<Forms>);

/// Boxed so an `EnvString` is one pointer wide. Error enums carry several of
/// them per variant, and two inline `String`s would make every `Result` that
/// holds such an error large.
#[derive(Clone)]
struct Forms {
    value: String,
    rendered: String,
}

impl EnvString {
    /// A value that came from the environment variable `name`.
    ///
    /// It always prints as `${name}`, whatever its length. The caller knows
    /// the provenance exactly, so the registry floor does not apply.
    pub fn substituted(name: &str, value: impl Into<String>) -> Self {
        Self(Box::new(Forms {
            value: value.into(),
            rendered: format!("${{{name}}}"),
        }))
    }

    /// A string that may hold resolved `${VAR}` values anywhere inside it.
    ///
    /// Every registered value in it prints as its `${NAME}`. A string that
    /// holds none prints as itself.
    pub fn from_resolved(value: impl Into<String>) -> Self {
        let value = value.into();
        let rendered = crate::secret_registry::render_placeholders(&value);
        Self(Box::new(Forms { value, rendered }))
    }

    /// The plaintext value. The only direct accessor, so every use is
    /// greppable. Do not pass the result to a printer.
    pub fn expose(&self) -> &str {
        &self.0.value
    }

    /// The printable form: the value with each resolved `${VAR}` value
    /// replaced by `${NAME}`. Same text as `Display`.
    pub fn rendered(&self) -> &str {
        &self.0.rendered
    }

    /// Whether the value is empty.
    pub fn is_empty(&self) -> bool {
        self.0.value.is_empty()
    }
}

/// [`EnvString::expose`] for an optional field: the `Option<&str>` twin of
/// `Option<String>::as_deref`, which `Option<EnvString>` deliberately does
/// not support. Named `expose_opt` so the use site stays greppable.
pub trait ExposeOpt {
    /// The plaintext value, if set. Do not pass the result to a printer.
    fn expose_opt(&self) -> Option<&str>;
}

impl ExposeOpt for Option<EnvString> {
    fn expose_opt(&self) -> Option<&str> {
        self.as_ref().map(EnvString::expose)
    }
}

/// Join the printable forms with `sep`. The `[EnvString]` twin of
/// `[String]::join`, which the type deliberately does not support.
pub fn join_rendered(items: &[EnvString], sep: &str) -> String {
    items
        .iter()
        .map(EnvString::rendered)
        .collect::<Vec<_>>()
        .join(sep)
}

impl fmt::Display for EnvString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // `pad`, not `write_str`, so width and alignment flags still work in
        // table output.
        f.pad(&self.0.rendered)
    }
}

impl fmt::Debug for EnvString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.0.rendered.as_str(), f)
    }
}

impl Serialize for EnvString {
    /// Writes the rendered form, unless the caller is inside
    /// [`crate::redacted::with_unredacted_scope`]. Same boundary as
    /// [`crate::redacted::RedactedString`].
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if unredacted_scope_active() || env_values_scope_active() {
            self.0.value.serialize(serializer)
        } else {
            self.0.rendered.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for EnvString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self::from_resolved)
    }
}

/// Delegates to [`String`] under the name `String`, and is never a `$ref`.
/// The exported schemas, and the Pydantic and TypeScript bindings generated
/// from them, are therefore byte-identical to a plain `String` field.
impl JsonSchema for EnvString {
    fn is_referenceable() -> bool {
        false
    }

    fn schema_name() -> String {
        String::schema_name()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

impl From<String> for EnvString {
    fn from(value: String) -> Self {
        Self::from_resolved(value)
    }
}

impl From<&String> for EnvString {
    fn from(value: &String) -> Self {
        Self::from_resolved(value.clone())
    }
}

impl From<&str> for EnvString {
    fn from(value: &str) -> Self {
        Self::from_resolved(value)
    }
}

impl PartialEq for EnvString {
    fn eq(&self, other: &Self) -> bool {
        self.0.value == other.0.value
    }
}

impl Eq for EnvString {}

impl PartialOrd for EnvString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EnvString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.value.cmp(&other.0.value)
    }
}

impl std::hash::Hash for EnvString {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.value.hash(state);
    }
}

impl PartialEq<str> for EnvString {
    fn eq(&self, other: &str) -> bool {
        self.0.value == other
    }
}

impl PartialEq<&str> for EnvString {
    fn eq(&self, other: &&str) -> bool {
        self.0.value == *other
    }
}

impl PartialEq<String> for EnvString {
    fn eq(&self, other: &String) -> bool {
        &self.0.value == other
    }
}

impl PartialEq<EnvString> for str {
    fn eq(&self, other: &EnvString) -> bool {
        self == other.0.value
    }
}

impl PartialEq<EnvString> for &str {
    fn eq(&self, other: &EnvString) -> bool {
        *self == other.0.value
    }
}

impl PartialEq<EnvString> for String {
    fn eq(&self, other: &EnvString) -> bool {
        *self == other.0.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacted::with_unredacted_scope;
    use crate::secret_registry::register_substitution;

    /// Distinctive, so no other test in this binary registers it.
    const SECRET: &str = "ROCKY-ENVSTRING-TEST-c41e7a09";

    #[test]
    fn a_substituted_value_prints_only_as_its_name() {
        let s = EnvString::substituted("ROCKY_ENVSTRING_NAME", SECRET);
        for printed in [
            s.to_string(),
            format!("{s:?}"),
            format!("{s:>40}"),
            serde_json::to_string(&s).unwrap(),
        ] {
            assert!(!printed.contains(SECRET), "leaked in {printed:?}");
            assert!(printed.contains("${ROCKY_ENVSTRING_NAME}"), "{printed:?}");
        }
        assert_eq!(s.expose(), SECRET);
    }

    #[test]
    fn a_substituted_value_under_the_floor_still_prints_as_its_name() {
        // Known provenance needs no length heuristic.
        let s = EnvString::substituted("PORT", "5432");
        assert_eq!(s.to_string(), "${PORT}");
    }

    #[test]
    fn a_resolved_value_inside_a_longer_string_is_replaced_in_place() {
        register_substitution("ROCKY_ENVSTRING_MID", SECRET);
        let s = EnvString::from_resolved(format!("orders_{SECRET}_*"));
        assert_eq!(s.to_string(), "orders_${ROCKY_ENVSTRING_MID}_*");
        assert_eq!(s.expose(), format!("orders_{SECRET}_*"));
    }

    #[test]
    fn deserialize_renders_against_the_registry() {
        let value = "ROCKY-ENVSTRING-DESER-5b0d93f1";
        register_substitution("ROCKY_ENVSTRING_DESER", value);
        let s: EnvString = serde_json::from_str(&format!("\"{value}\"")).unwrap();
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            "\"${ROCKY_ENVSTRING_DESER}\""
        );
        assert_eq!(s, value, "equality compares the value, not the print");
    }

    #[test]
    fn the_unredacted_scope_writes_the_value() {
        let s = EnvString::substituted("ROCKY_ENVSTRING_SCOPE", SECRET);
        let json = with_unredacted_scope(|| serde_json::to_string(&s).unwrap());
        assert_eq!(json, format!("\"{SECRET}\""));
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            "\"${ROCKY_ENVSTRING_SCOPE}\"",
            "the scope ends with the closure"
        );
    }

    #[test]
    fn the_env_values_scope_writes_the_value_but_not_a_credential() {
        let s = EnvString::substituted("ROCKY_ENVSTRING_ENVSCOPE", SECRET);
        let cred = crate::redacted::RedactedString::new("ROCKY-CRED-9d1f22aa".to_string());
        let json = with_env_values_scope(|| serde_json::to_string(&(&s, &cred)).unwrap());
        assert_eq!(json, format!("[\"{SECRET}\",\"***\"]"));
        assert_eq!(
            serde_json::to_string(&s).unwrap(),
            "\"${ROCKY_ENVSTRING_ENVSCOPE}\"",
            "the scope ends with the closure"
        );
    }

    #[test]
    fn an_unregistered_string_prints_as_itself() {
        let s = EnvString::from_resolved("fct_orders");
        assert_eq!(s.to_string(), "fct_orders");
        assert_eq!(serde_json::to_string(&s).unwrap(), "\"fct_orders\"");
    }

    #[test]
    fn the_schema_is_a_plain_inline_string() {
        let ours = schemars::schema_for!(EnvString);
        let plain = schemars::schema_for!(String);
        assert_eq!(
            serde_json::to_value(&ours.schema).unwrap(),
            serde_json::to_value(&plain.schema).unwrap(),
            "a schema change would drift the generated SDK and VS Code bindings"
        );
    }
}
