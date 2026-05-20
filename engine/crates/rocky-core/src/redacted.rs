//! Credential-safe string wrapper.
//!
//! [`RedactedString`] implements `Debug` and `Display` as `***`, preventing
//! credential leakage via logs, error messages, and `{:?}` format strings.
//! Used for auth tokens, API secrets, and other sensitive values across
//! all adapter crates.
//!
//! ## Serialization model
//!
//! By default `Serialize` writes the redacted token `"***"` — not the
//! cleartext. Any orchestrator-visible JSON, persisted plan payload, or
//! ad-hoc `serde_json::to_value(rocky_cfg)` therefore strips credentials
//! by construction. This closes a leak path where `RockyConfig.adapter.*`
//! gets snapshotted into [`crate`]-external surfaces (e.g. the
//! `ReplicationPlan.config_snapshot` field consumed by `rocky apply` and
//! re-surfaced through the Dagster bridge).
//!
//! When a code path genuinely needs the raw value — config-file writers,
//! pass-through to a downstream credential header — it must opt in via
//! the [`with_unredacted_scope`] guard, which flips a thread-local
//! counter for the duration of the closure. Outside that scope the
//! cleartext is unreachable through `Serialize`. The only other way to
//! observe the raw value is the explicit [`RedactedString::expose`]
//! accessor, which is greppable and audited.

use std::cell::Cell;
use std::fmt;

use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

thread_local! {
    /// Counter (not a bool) so nested scopes compose. `Serialize` writes
    /// cleartext only while the counter is non-zero on the current
    /// thread. `Cell<u32>` is sufficient — the value is never borrowed
    /// across yields and `RedactedString` is `!Send` safe by virtue of
    /// being thread-local-gated.
    static UNREDACTED_DEPTH: Cell<u32> = const { Cell::new(0) };
}

/// Returns `true` when the current thread is inside an
/// [`with_unredacted_scope`] guard. Test-only helper; production code
/// should never branch on this — the [`Serialize`] impl is the single
/// consumer.
#[doc(hidden)]
pub fn unredacted_scope_active() -> bool {
    UNREDACTED_DEPTH.with(|d| d.get() > 0)
}

/// Run `f` with [`RedactedString::serialize`] writing the raw cleartext
/// value instead of `"***"`.
///
/// Use this exclusively for code paths that must persist or transmit the
/// credential — config-file writers, adapter handshake payloads. The
/// scope is per-thread and re-entrant; nested calls compose correctly.
///
/// ```
/// use rocky_core::redacted::{RedactedString, with_unredacted_scope};
///
/// let secret = RedactedString::new("dapi_abc123".into());
///
/// // Default: redacted on the wire.
/// assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
///
/// // Inside a scope: cleartext on the wire.
/// let json = with_unredacted_scope(|| serde_json::to_string(&secret).unwrap());
/// assert_eq!(json, "\"dapi_abc123\"");
///
/// // After the scope: redacted again.
/// assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
/// ```
pub fn with_unredacted_scope<F, R>(f: F) -> R
where
    F: FnOnce() -> R,
{
    UNREDACTED_DEPTH.with(|d| d.set(d.get().saturating_add(1)));
    // Decrement on panic too, so a panicking serializer doesn't leave
    // the thread permanently in unredacted mode.
    struct Guard;
    impl Drop for Guard {
        fn drop(&mut self) {
            UNREDACTED_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
        }
    }
    let _g = Guard;
    f()
}

/// A string whose `Debug` and `Display` impls print `***` instead of the
/// raw value. Prevents credential leakage in logs, error messages, and —
/// crucially — `serde_json::to_value` snapshots that get embedded in
/// plan output, state files, or orchestrator metadata.
///
/// The inner value is accessible only via [`expose()`](RedactedString::expose)
/// (greppable, audited) or by serializing inside a
/// [`with_unredacted_scope`] guard.
///
/// ## Serialization
///
/// `Serialize` writes `"***"` by default and the cleartext value only
/// inside [`with_unredacted_scope`]. `Deserialize` is unconditional —
/// it always reads the raw value from the wire — because config files
/// and incoming requests must continue to round-trip.
///
/// ```
/// use rocky_core::redacted::RedactedString;
///
/// let secret = RedactedString::new("dapi_abc123".into());
/// assert_eq!(format!("{:?}", secret), "***");
/// assert_eq!(format!("{}", secret), "***");
/// assert_eq!(secret.expose(), "dapi_abc123");
/// // Default serialization redacts:
/// assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
/// ```
#[derive(Clone)]
pub struct RedactedString(String);

impl RedactedString {
    /// Wrap a raw string value.
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Returns the raw value. This is the only direct accessor for the
    /// inner string — every call site is greppable and audited.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for RedactedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

impl fmt::Display for RedactedString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

impl Serialize for RedactedString {
    /// Writes `"***"` unless the caller is inside a
    /// [`with_unredacted_scope`] guard, in which case the cleartext is
    /// written. This is the security boundary — any `serde_json::to_*`
    /// /`toml::to_string*` call against a struct containing
    /// `RedactedString` redacts by construction. See the module-level
    /// docs for the design rationale.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if unredacted_scope_active() {
            self.0.serialize(serializer)
        } else {
            "***".serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for RedactedString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(RedactedString)
    }
}

/// `JsonSchema` is implemented by delegating to [`String`], so
/// `RedactedString` appears in generated schemas as a plain `string`.
/// The `***` redaction is a runtime concern; the schema only describes
/// the wire shape.
impl JsonSchema for RedactedString {
    fn schema_name() -> String {
        "RedactedString".to_owned()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_does_not_leak_secret() {
        let secret = RedactedString::new("super_secret_token".into());
        let debug_str = format!("{:?}", secret);
        assert_eq!(debug_str, "***");
        assert!(!debug_str.contains("super_secret_token"));
    }

    #[test]
    fn display_does_not_leak_secret() {
        let secret = RedactedString::new("api_key_xyz".into());
        assert_eq!(format!("{}", secret), "***");
    }

    #[test]
    fn expose_returns_raw_value() {
        let secret = RedactedString::new("my_token".into());
        assert_eq!(secret.expose(), "my_token");
    }

    #[test]
    fn clone_preserves_value() {
        let secret = RedactedString::new("cloned_secret".into());
        let cloned = secret.clone();
        assert_eq!(cloned.expose(), "cloned_secret");
    }

    /// Default serialization MUST redact. This is the contract that the
    /// `ReplicationPlan.config_snapshot` path relies on — without it,
    /// `serde_json::to_value(rocky_cfg)` in `rocky-cli/src/commands/plan.rs`
    /// would embed cleartext credentials into orchestrator-visible JSON.
    #[test]
    fn default_serialize_redacts() {
        let secret = RedactedString::new("super_secret_token".into());
        let json = serde_json::to_string(&secret).unwrap();
        assert_eq!(json, "\"***\"");
        assert!(!json.contains("super_secret_token"));

        // Deserialize still reads cleartext from the wire (configs and
        // incoming envelopes must round-trip).
        let deserialized: RedactedString = serde_json::from_str("\"raw_from_wire\"").unwrap();
        assert_eq!(deserialized.expose(), "raw_from_wire");
        assert_eq!(format!("{:?}", deserialized), "***");
    }

    /// Cleartext is reachable only inside an explicit unredacted scope.
    /// This is the opt-in seam for config-file writers and adapter
    /// handshake payloads.
    #[test]
    fn unredacted_scope_writes_cleartext() {
        let secret = RedactedString::new("dapi_abc123".into());

        // Outside: redacted.
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");

        // Inside: cleartext.
        let inside = with_unredacted_scope(|| serde_json::to_string(&secret).unwrap());
        assert_eq!(inside, "\"dapi_abc123\"");

        // After: redacted again — guard restored on scope exit.
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
        assert!(!unredacted_scope_active());
    }

    /// Nested scopes compose — the inner exit doesn't end the outer
    /// guard's lifetime.
    #[test]
    fn nested_unredacted_scopes_compose() {
        let secret = RedactedString::new("nested".into());
        let outer = with_unredacted_scope(|| {
            let inner = with_unredacted_scope(|| serde_json::to_string(&secret).unwrap());
            assert_eq!(inner, "\"nested\"");
            // Still unredacted at the outer level.
            serde_json::to_string(&secret).unwrap()
        });
        assert_eq!(outer, "\"nested\"");
        // Counter fully unwound.
        assert!(!unredacted_scope_active());
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
    }

    /// A panic inside a scope must still decrement the counter, otherwise
    /// a panicking serializer would leave the thread permanently in
    /// unredacted mode.
    #[test]
    fn panic_in_scope_restores_redaction() {
        let secret = RedactedString::new("panic_safe".into());
        let caught = std::panic::catch_unwind(|| {
            with_unredacted_scope(|| {
                panic!("boom");
            })
        });
        assert!(caught.is_err());
        assert!(!unredacted_scope_active());
        assert_eq!(serde_json::to_string(&secret).unwrap(), "\"***\"");
    }
}
