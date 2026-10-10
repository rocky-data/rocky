//! A connection URL that can embed a credential (#2339).
//!
//! A Valkey or object-store URL names a server, and it can also carry the
//! secret that opens it:
//!
//! ```text
//!   redis://default:s3cret@valkey:6379/0         the password in the userinfo
//!   redis+unix:///run/valkey.sock?pass=s3cret    the password in the query
//!   az://container/prefix/?sv=...&sig=...        an Azure SAS token in the query
//! ```
//!
//! [`CredentialUrl`] keeps the full URL for the client that connects, and
//! builds two forms without the secret:
//!
//! ```text
//!   identity   redis://default:***@valkey:6379/0?db=1#insecure
//!              secret query values masked, every other part kept
//!   shown      redis://default:***@valkey:6379/0?***#***
//!              the whole query and fragment hidden as well
//! ```
//!
//! The **identity** is what the plan config identity (`config_policy_identity`
//! in `rocky-cli`) hashes. It keeps the scheme, user name, host, port, path,
//! the non-secret query (`?db=`, a unix-socket setting) and the fragment
//! (`#insecure` turns TLS verification off), because each of them changes
//! which server is used or how. Only the password and the values of secret
//! query keys are `***`, so a rotated password does not refuse a plan, as
//! for every other credential.
//!
//! ## Fail closed
//!
//! The URL is parsed with the `url` crate, so a userinfo is read from the
//! authority only. When the URL does not parse, or an `@` is not the one
//! userinfo delimiter (two of them, or one in a path or query), the split is
//! a guess. The identity is then a digest of the whole URL: a password change
//! refuses the plan, which is the safe direction. The shown form then hides
//! everything before the last `@` as well.
//!
//! ## Serialization
//!
//! - Default: the shown form, with each resolved `${VAR}` value in it written
//!   as `${NAME}`, as [`crate::env_string::EnvString`] does.
//! - Inside [`crate::env_string::with_env_values_scope`]: the identity form,
//!   with the resolved values. Only ever hashed, never printed.
//! - Inside [`crate::redacted::with_unredacted_scope`]: the full URL.
//!
//! `Deserialize` reads a plain string, so `rocky.toml` and `${VAR}` work as
//! before. The JSON schema is the one of `String`.

use std::fmt;

use schemars::JsonSchema;
use schemars::r#gen::SchemaGenerator;
use schemars::schema::Schema;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url::Url;

use crate::env_string::env_values_scope_active;
use crate::redacted::unredacted_scope_active;

/// A connection URL whose printed and serialized forms drop its secret
/// parts. See the [module docs](self).
///
/// ```
/// use rocky_core::credential_url::CredentialUrl;
///
/// let url = CredentialUrl::new("rediss://default:s3cret-pw@valkey:6379/0".into());
/// assert_eq!(url.to_string(), "rediss://default:***@valkey:6379/0");
/// assert_eq!(format!("{url:?}"), "\"rediss://default:***@valkey:6379/0\"");
/// assert_eq!(
///     serde_json::to_string(&url).unwrap(),
///     "\"rediss://default:***@valkey:6379/0\""
/// );
/// assert_eq!(url.expose(), "rediss://default:s3cret-pw@valkey:6379/0");
/// ```
#[derive(Clone)]
pub struct CredentialUrl(String);

impl CredentialUrl {
    /// Wrap a raw URL.
    pub fn new(url: String) -> Self {
        Self(url)
    }

    /// The full URL, secret included. The only accessor, so every use is
    /// greppable. Pass it to the client that connects, never to a printer.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Whether the URL is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The form the config identity hashes. Holds no secret, but holds every
    /// part that selects the server or how to reach it. Never print it: it may
    /// hold a resolved `${VAR}` value, and for a URL that does not parse it is
    /// a digest of the whole URL.
    pub fn identity(&self) -> String {
        match parse_unambiguous(&self.0) {
            Some(url) => identity_form(url),
            None => format!(
                "unparsed-url:blake3:{}",
                blake3::hash(format!("rocky.credential_url.v1\0{}", self.0).as_bytes()).to_hex()
            ),
        }
    }

    /// The URL without any part that can hold a secret: the password, the
    /// query and the fragment. May hold a resolved `${VAR}` value, so print
    /// [`Self::rendered`].
    fn shown(&self) -> String {
        match parse_unambiguous(&self.0) {
            Some(url) => shown_form(url),
            None => shown_textual(&self.0),
        }
    }

    /// The printable form: [`Self::shown`] with each resolved `${VAR}` value
    /// written as `${NAME}`.
    fn rendered(&self) -> String {
        crate::secret_registry::render_placeholders(&self.shown())
    }

    /// `text` with every secret of this URL replaced, for an error message
    /// from a library that echoes the URL it was given.
    ///
    /// The raw URL and its parsed spelling become the shown form. Then the
    /// password and the whole query and fragment, raw and decoded, become
    /// `***`, which also covers a library that echoes only a part of the URL.
    pub fn scrub(&self, text: &str) -> String {
        let shown = self.rendered();
        let mut out = text.replace(&self.0, &shown);
        let mut secrets: Vec<String> = Vec::new();
        match Url::parse(&self.0) {
            Ok(url) => {
                out = out.replace(url.as_str(), &shown);
                if let Some(password) = url.password() {
                    secrets.push(password.to_string());
                    secrets.push(percent_decode(password));
                }
                for part in [url.query(), url.fragment()].into_iter().flatten() {
                    secrets.push(part.to_string());
                    secrets.push(percent_decode(part));
                    for (_, value) in url::form_urlencoded::parse(part.as_bytes()) {
                        secrets.push(value.into_owned());
                    }
                }
            }
            Err(_) => {
                // No reliable split: hide everything before the last `@` and
                // after the first `?` or `#`.
                if let Some(index) = self.0.rfind('@') {
                    secrets.push(self.0[..index].to_string());
                }
                if let Some(index) = self.0.find(['?', '#']) {
                    secrets.push(self.0[index..].to_string());
                }
            }
        }
        // Longest first, so a secret that contains another is replaced whole.
        secrets.retain(|s| !s.is_empty());
        secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
        for secret in secrets {
            out = out.replace(&secret, "***");
        }
        out
    }
}

/// The URL parsed, when its userinfo split is certain.
///
/// Certain means: it parses, and every `@` in it is the one userinfo
/// delimiter. With no `@` there is no userinfo. With one `@`, the parse must
/// have read a user name or a password before it; if not, the `@` sits in a
/// path, query or fragment, where another reader could split it differently.
/// Two or more `@` are never certain (an unencoded `@` in a password, or one
/// in a query such as `?client_name=a@b`).
fn parse_unambiguous(raw: &str) -> Option<Url> {
    let url = Url::parse(raw).ok()?;
    match raw.matches('@').count() {
        0 => Some(url),
        1 if !url.username().is_empty() || url.password().is_some() => Some(url),
        _ => None,
    }
}

/// [`CredentialUrl::identity`] for a URL whose split is certain.
fn identity_form(mut url: Url) -> String {
    if url.password().is_some() {
        // Cannot fail: a URL with a password has a host.
        let _ = url.set_password(Some("***"));
    }
    if let Some(query) = url.query().map(str::to_string) {
        let mut masked = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if is_secret_query_key(&key) {
                masked.append_pair(&key, "***");
            } else {
                masked.append_pair(&key, &value);
            }
        }
        url.set_query(Some(&masked.finish()));
    }
    url.to_string()
}

/// [`CredentialUrl::shown`] for a URL whose split is certain.
fn shown_form(mut url: Url) -> String {
    if url.password().is_some() {
        let _ = url.set_password(Some("***"));
    }
    if url.query().is_some() {
        url.set_query(Some("***"));
    }
    if url.fragment().is_some() {
        url.set_fragment(Some("***"));
    }
    url.to_string()
}

/// [`CredentialUrl::shown`] for a URL whose split is a guess. Hides
/// everything that could be a userinfo (up to the LAST `@`) and everything
/// from the first `?` or `#`. It hides more than it must, never less.
fn shown_textual(raw: &str) -> String {
    let route_start = raw.find(['/', '?', '#']);
    let (scheme, rest) = match raw.find("://") {
        Some(index) if route_start == Some(index + 1) => raw.split_at(index + 3),
        _ => ("", raw),
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

/// Whether a query key can hold a secret. Errs toward yes: a masked
/// non-secret only means two values of it hash the same.
///
/// Covers passwords (`password`, `pass`, `pwd`), tokens and keys (`auth`,
/// `token`, `secret`, `key`, `credential`, `session`), signatures (`sig`) and
/// every Azure SAS field, since a SAS token is one credential spread over
/// several keys.
fn is_secret_query_key(key: &str) -> bool {
    const FRAGMENTS: &[&str] = &[
        "pass", "pwd", "auth", "token", "secret", "sig", "key", "cred", "session",
    ];
    const SAS_KEYS: &[&str] = &[
        "sv", "ss", "srt", "sp", "se", "st", "spr", "sr", "si", "sdd", "skoid", "sktid", "skt",
        "ske", "sks", "skv", "saoid", "suoid", "scid",
    ];
    let key = key.to_ascii_lowercase();
    FRAGMENTS.iter().any(|f| key.contains(f)) || SAS_KEYS.contains(&key.as_str())
}

fn percent_decode(text: &str) -> String {
    url::form_urlencoded::parse(format!("x={text}").as_bytes())
        .next()
        .map_or_else(|| text.to_string(), |(_, value)| value.into_owned())
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
            self.identity().serialize(serializer)
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

    fn id(raw: &str) -> String {
        CredentialUrl::from(raw).identity()
    }

    #[test]
    fn the_shown_form_holds_no_secret() {
        let cases = [
            ("redis://localhost:6379/", "redis://localhost:6379/"),
            ("redis://:pw-SECRET@host:6379", "redis://:***@host:6379"),
            (
                "rediss://default:pw-SECRET@host:6379/2",
                "rediss://default:***@host:6379/2",
            ),
            // Ambiguous: an unencoded `@` and `/` in the password.
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
            (
                "az://container@account.dfs.core.windows.net/path",
                "az://container@account.dfs.core.windows.net/path",
            ),
            // Not a URL the parser accepts as one: the textual fallback.
            ("user:pw-SECRET@host:6379", "***@host:6379"),
            ("", ""),
        ];
        for (raw, shown) in cases {
            let url = CredentialUrl::from(raw);
            assert_eq!(url.to_string(), shown, "for {raw}");
            assert!(!format!("{url:?}").contains("SECRET"), "for {raw}");
            // The identity keeps the fragment whole (`#insecure` changes the
            // TLS mode). A secret put there is hashed by value, so rotating it
            // refuses a plan: the safe direction. It is never printed.
            if !raw.contains('#') {
                assert!(!url.identity().contains("SECRET"), "for {raw}");
            }
            assert_eq!(url.expose(), raw);
        }
    }

    /// Each pair names a different server, database or TLS mode, so the two
    /// identities must differ.
    #[test]
    fn the_identity_keeps_every_part_that_selects_the_server() {
        let pairs = [
            ("redis+unix:///s.sock?db=1", "redis+unix:///s.sock?db=2"),
            ("rediss://h:6379/", "rediss://h:6379/#insecure"),
            (
                "redis://hostA/0?client_name=a@b",
                "redis://hostB/0?client_name=a@b",
            ),
            (
                "az://container-a@account.dfs.core.windows.net/path",
                "az://container-b@account.dfs.core.windows.net/path",
            ),
            ("redis://:pw@valkey-a:6379/0", "redis://:pw@valkey-b:6379/0"),
            ("redis://:pw@h:6379/0", "redis://:pw@h:6379/1"),
        ];
        for (a, b) in pairs {
            assert_ne!(id(a), id(b), "{a} and {b} must differ");
        }
    }

    /// A password change, wherever the password sits, must not move the
    /// identity when the split is certain.
    #[test]
    fn the_identity_ignores_a_rotated_secret() {
        let pairs = [
            (
                "rediss://default:pw-A@h:6379/0",
                "rediss://default:pw-B@h:6379/0",
            ),
            (
                "redis+unix:///s.sock?db=1&pass=pw-A",
                "redis+unix:///s.sock?db=1&pass=pw-B",
            ),
            (
                "az://c/p/?sv=2024&se=2026-01-01&sig=SIG-A",
                "az://c/p/?sv=2025&se=2027-01-01&sig=SIG-B",
            ),
        ];
        for (a, b) in pairs {
            assert_eq!(id(a), id(b), "{a} and {b} must agree");
        }
    }

    /// When the split is a guess, a password change refuses (the identity
    /// is a digest of the whole URL), and nothing of the URL is readable.
    #[test]
    fn an_ambiguous_url_fails_closed() {
        let a = id("redis://u:p@ss-A@h:6379");
        let b = id("redis://u:p@ss-B@h:6379");
        assert_ne!(a, b);
        assert!(a.starts_with("unparsed-url:blake3:"), "{a}");
        assert!(!a.contains("ss-A"));
    }

    #[test]
    fn serialize_writes_the_full_url_only_in_the_unredacted_scope() {
        let url = CredentialUrl::from("redis://:pw-SECRET@host:6379/0?db=1");
        assert_eq!(
            serde_json::to_string(&url).unwrap(),
            "\"redis://:***@host:6379/0?***\""
        );
        assert_eq!(
            with_env_values_scope(|| serde_json::to_string(&url).unwrap()),
            "\"redis://:***@host:6379/0?db=1\""
        );
        assert_eq!(
            with_unredacted_scope(|| serde_json::to_string(&url).unwrap()),
            "\"redis://:pw-SECRET@host:6379/0?db=1\""
        );
    }

    #[test]
    fn a_resolved_host_prints_as_its_name_but_digests_by_value() {
        let host = "valkey-rocky-credurl-3f9a1c.internal";
        crate::secret_registry::register_substitution("ROCKY_CREDURL_HOST", host);
        let url = CredentialUrl::from(format!("redis://:pw-SECRET@{host}:6379").as_str());
        assert_eq!(url.to_string(), "redis://:***@${ROCKY_CREDURL_HOST}:6379");
        assert_eq!(
            with_env_values_scope(|| serde_json::to_string(&url).unwrap()),
            format!("\"redis://:***@{host}:6379\"")
        );
    }

    #[test]
    fn scrub_removes_every_secret_an_error_can_echo() {
        let raw = "foo://user:PW%2FSECRET@h/p?sig=SIG-SECRET#FRAG-SECRET";
        let url = CredentialUrl::from(raw);
        let parsed = Url::parse(raw).unwrap();
        let text = format!(
            "raw {raw} | parsed \"{parsed}\" | prefix {} | decoded PW/SECRET | value SIG-SECRET",
            &parsed[..url::Position::BeforePath]
        );
        let scrubbed = url.scrub(&text);
        assert!(!scrubbed.contains("SECRET"), "{scrubbed}");
        assert!(
            scrubbed.contains("foo://user:***@h/p?***#***"),
            "{scrubbed}"
        );
    }

    #[test]
    fn deserializes_from_a_plain_string() {
        let url: CredentialUrl = serde_json::from_str("\"redis://:pw@h:1\"").unwrap();
        assert_eq!(url.expose(), "redis://:pw@h:1");
    }
}
