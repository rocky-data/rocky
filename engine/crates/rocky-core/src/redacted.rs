//! Credential-safe string wrapper.
//!
//! [`RedactedString`] implements `Debug` and `Display` as `***`, preventing
//! credential leakage via logs, error messages, and `{:?}` format strings.
//! Used for auth tokens, API secrets, and other sensitive values across
//! all adapter crates.
//!
//! Supports `serde` round-tripping: `Deserialize` reads the raw value,
//! `Serialize` writes the raw value (use only for config persistence, never
//! for logging).

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A string whose `Debug` and `Display` impls print `***` instead of the
/// raw value. Prevents credential leakage in logs and error messages.
///
/// The inner value is accessible only via [`expose()`](RedactedString::expose).
///
/// `Serialize` / `Deserialize` round-trip the raw value so the type can
/// be embedded in config structs that are parsed from TOML / JSON. The
/// serialization path is intentional — it is used for writing config files,
/// not for logging.
///
/// ```
/// use rocky_core::redacted::RedactedString;
///
/// let secret = RedactedString::new("dapi_abc123".into());
/// assert_eq!(format!("{:?}", secret), "***");
/// assert_eq!(format!("{}", secret), "***");
/// assert_eq!(secret.expose(), "dapi_abc123");
/// ```
#[derive(Clone)]
pub struct RedactedString(String);

impl RedactedString {
    /// Wrap a raw string value.
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Returns the raw value. This is the only way to access the inner string.
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
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for RedactedString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(RedactedString)
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

    #[test]
    fn serde_round_trip() {
        let secret = RedactedString::new("round_trip_secret".into());
        let json = serde_json::to_string(&secret).unwrap();
        assert_eq!(json, "\"round_trip_secret\"");

        let deserialized: RedactedString = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.expose(), "round_trip_secret");
        // Debug still redacts after round-trip.
        assert_eq!(format!("{:?}", deserialized), "***");
    }
}
