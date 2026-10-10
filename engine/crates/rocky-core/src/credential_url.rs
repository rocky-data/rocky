//! A connection URL that can embed a credential (#2339).
//!
//! A Valkey or object-store URL names a server, and it can also carry the
//! password that opens it:
//!
//! ```text
//!   redis://default:s3cret@valkey:6379/0      userinfo holds the password
//!   redis+unix:///run/valkey.sock?pass=s3cret query holds the password
//! ```
//!
//! [`CredentialUrl`] keeps the full URL for the client that connects, and
//! shows everything else a URL without its credential parts:
//!
//! ```text
//!   expose()                      redis://default:s3cret@valkey:6379/0
//!   Debug, Display, Serialize     redis://***@valkey:6379/0
//! ```
//!
//! The userinfo becomes `***@` and a query or fragment becomes `?***` or
//! `#***`. The scheme, host, port and path stay, because they say *where*
//! the data lives. So the plan config identity
//! (`config_policy_identity` in `rocky-cli`) still changes when the URL
//! points at another server, but a rotated password no longer changes it.
//! That matches every other credential, which hashes as `"***"`.
//!
//! ## Serialization
//!
//! - Default: the credential-free form, with each resolved `${VAR}` value in
//!   it written as `${NAME}`, as [`crate::env_string::EnvString`] does.
//! - Inside [`crate::env_string::with_env_values_scope`]: the credential-free
//!   form with the resolved values. That is what the keyed config digests
//!   hash.
//! - Inside [`crate::redacted::with_unredacted_scope`]: the full URL.
//!
//! `Deserialize` reads a plain string, so `rocky.toml` and `${VAR}` work as
//! before. The JSON schema is the one of `String`.

use std::fmt;

use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::env_string::env_values_scope_active;
use crate::redacted::unredacted_scope_active;

/// A connection URL whose printed and serialized forms drop its credential
/// parts. See the [module docs](self).
///
/// ```
/// use rocky_core::credential_url::CredentialUrl;
///
/// let url = CredentialUrl::new("rediss://default:s3cret-pw@valkey:6379/0".into());
/// assert_eq!(url.to_string(), "rediss://***@valkey:6379/0");
/// assert_eq!(format!("{url:?}"), "\"rediss://***@valkey:6379/0\"");
/// assert_eq!(serde_json::to_string(&url).unwrap(), "\"rediss://***@valkey:6379/0\"");
/// assert_eq!(url.expose(), "rediss://default:s3cret-pw@valkey:6379/0");
/// ```
#[derive(Clone)]
pub struct CredentialUrl(String);

impl CredentialUrl {
    /// Wrap a raw URL.
    pub fn new(url: String) -> Self {
        Self(url)
    }

    /// The full URL, credential included. The only accessor, so every use is
    /// greppable. Pass it to the client that connects, never to a printer.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether the URL is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The URL without its userinfo, query or fragment. Holds no credential
    /// but may hold a resolved `${VAR}` value, so print [`Self::rendered`].
    fn without_credentials(&self) -> String {
        without_credentials(&self.0)
    }

    /// The printable form: [`Self::without_credentials`] with each resolved
    /// `${VAR}` value written as `${NAME}`.
    fn rendered(&self) -> String {
        crate::secret_registry::render_placeholders(&self.without_credentials())
    }
}

/// Removes the parts of a URL that can carry a credential.
///
/// ```text
///   scheme://user:pass@host:port/path?query#fragment
///   scheme://***@host:port/path?***
/// ```
///
/// A scheme ends at the first `://`, and only when that `://` comes before
/// any `/`, `?` or `#` (the rule of the endpoint identity's host split). The
/// userinfo ends at the LAST `@`, because a password may itself hold `@`, `/`
/// or `?`. An `@` in a path or query is therefore read as the end of a
/// userinfo too. That hides more than it must, never less.
fn without_credentials(url: &str) -> String {
    let route_start = url.find(['/', '?', '#']);
    let (scheme, rest) = match url.find("://") {
        Some(index) if route_start == Some(index + 1) => url.split_at(index + 3),
        _ => ("", url),
    };
    let (userinfo, rest) = match rest.rfind('@') {
        Some(index) => ("***@", &rest[index + 1..]),
        None => ("", rest),
    };
    let (location, extra) = rest.split_at(rest.find(['?', '#']).unwrap_or(rest.len()));
    let extra = match extra.chars().next() {
        Some('?') => "?***",
        Some('#') => "#***",
        _ => "",
    };
    format!("{scheme}{userinfo}{location}{extra}")
}

impl fmt::Display for CredentialUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.pad(&self.rendered())
    }
}

impl fmt::Debug for CredentialUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.rendered().as_str(), f)
    }
}

impl Serialize for CredentialUrl {
    /// The full URL only inside [`crate::redacted::with_unredacted_scope`].
    /// See the [module docs](self).
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if unredacted_scope_active() {
            self.0.serialize(serializer)
        } else if env_values_scope_active() {
            self.without_credentials().serialize(serializer)
        } else {
            self.rendered().serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for CredentialUrl {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer).map(Self)
    }
}

/// Delegates to [`String`] under the name `String`, and is never a `$ref`,
/// so the exported schemas and generated bindings match a plain `String`.
impl JsonSchema for CredentialUrl {
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

impl From<&str> for CredentialUrl {
    fn from(url: &str) -> Self {
        Self(url.to_string())
    }
}

impl From<String> for CredentialUrl {
    fn from(url: String) -> Self {
        Self(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env_string::with_env_values_scope;
    use crate::redacted::with_unredacted_scope;

    #[test]
    fn credential_parts_are_removed() {
        let cases = [
            ("redis://localhost:6379/", "redis://localhost:6379/"),
            ("redis://:pw-SECRET@host:6379", "redis://***@host:6379"),
            (
                "rediss://default:pw-SECRET@host:6379/2",
                "rediss://***@host:6379/2",
            ),
            // A password that holds `@`, `/` and `?` unencoded.
            (
                "redis://u:p@s/s?w-SECRET@host:6379",
                "redis://***@host:6379",
            ),
            (
                "redis+unix:///run/v.sock?pass=pw-SECRET",
                "redis+unix:///run/v.sock?***",
            ),
            ("redis://host:6379#pw-SECRET", "redis://host:6379#***"),
            ("s3://bucket/prefix/", "s3://bucket/prefix/"),
            ("user:pw-SECRET@host:6379", "***@host:6379"),
            ("", ""),
        ];
        for (raw, shown) in cases {
            let url = CredentialUrl::from(raw);
            assert_eq!(url.to_string(), shown, "for {raw}");
            assert!(!format!("{url:?}").contains("SECRET"), "for {raw}");
            assert_eq!(url.expose(), raw);
        }
    }

    #[test]
    fn serialize_writes_the_full_url_only_in_the_unredacted_scope() {
        let url = CredentialUrl::from("redis://:pw-SECRET@host:6379/0");
        let shown = "\"redis://***@host:6379/0\"";
        assert_eq!(serde_json::to_string(&url).unwrap(), shown);
        assert_eq!(
            with_env_values_scope(|| serde_json::to_string(&url).unwrap()),
            shown
        );
        assert_eq!(
            with_unredacted_scope(|| serde_json::to_string(&url).unwrap()),
            "\"redis://:pw-SECRET@host:6379/0\""
        );
    }

    #[test]
    fn a_resolved_host_prints_as_its_name_but_digests_by_value() {
        let host = "valkey-ROCKY-CREDURL-3f9a1c.internal";
        crate::secret_registry::register_substitution("ROCKY_CREDURL_HOST", host);
        let url = CredentialUrl::from(format!("redis://:pw-SECRET@{host}:6379").as_str());
        assert_eq!(url.to_string(), "redis://***@${ROCKY_CREDURL_HOST}:6379");
        assert_eq!(
            with_env_values_scope(|| serde_json::to_string(&url).unwrap()),
            format!("\"redis://***@{host}:6379\"")
        );
    }

    #[test]
    fn deserializes_from_a_plain_string() {
        let url: CredentialUrl = serde_json::from_str("\"redis://:pw@h:1\"").unwrap();
        assert_eq!(url.expose(), "redis://:pw@h:1");
    }
}
