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
//!   identity   redis://default:***@valkey:6379/0?db=1&pass=***#insecure
//!              only what a client reads, and no secret
//!   shown      redis://default:***@valkey:6379/0?***#***
//!              the whole query and fragment hidden as well
//! ```
//!
//! The **identity** is what the plan config identity (`config_policy_identity`
//! in `rocky-cli`) hashes and what a plan's config snapshot stores. It keeps
//! the scheme, user name, host, port and path, because each of them can
//! change which server is used. The query and fragment are masked by an
//! allow-list of what the clients read:
//!
//! - query: on a Redis scheme the values of `db`, `user` and `protocol` stay.
//!   Every other value is `***` (the key name stays). The Redis client reads
//!   only those keys and `pass`; `object_store` reads no query.
//! - fragment: kept only when it is `insecure`, which turns TLS verification
//!   off. The Redis client reads no other fragment.
//!
//! It also masks the password, so a rotated password does not refuse a plan,
//! and a user name with no password, except for the Azure schemes, where it
//! names the container. Without a password the Redis client never sends a
//! user name, so it can only be a misplaced token.
//!
//! The kept values are stored as written, like an
//! [`crate::env_string::EnvString`] field: a resolved `${VAR}` value prints
//! as `${NAME}` in the stored snapshot.
//!
//! The URL is parsed with the `url` crate, the parser the Redis client and
//! `object_store` use, so the userinfo split is the one the client sees. A
//! URL it cannot parse is one no client can connect with. Its identity is
//! the shown form, marked `unparseable-url`, and holds no secret.
//!
//! ## Serialization
//!
//! - Default: the shown form, with each resolved `${VAR}` value in it written
//!   as `${NAME}`, as [`crate::env_string::EnvString`] does.
//! - Inside [`crate::env_string::with_env_values_scope`]: the identity form,
//!   with the resolved values. Hashed, and stored in a plan's config snapshot
//!   after `${NAME}` rendering, so it must never hold a password.
//! - Inside [`crate::redacted::with_unredacted_scope`]: the full URL.
//!
//! `Deserialize` reads a plain string, so `rocky.toml` and `${VAR}` work as
//! before. The JSON schema is the one of `String`.

use std::fmt;

use schemars::JsonSchema;
use schemars::{Schema, SchemaGenerator};
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

    /// The form the config identity hashes and a plan stores. Holds no
    /// password and no value of a secret query key, but every other part
    /// that can select the server. Print [`Self::rendered`] instead: it may
    /// hold a resolved `${VAR}` value.
    pub fn identity(&self) -> String {
        match parse(&self.0) {
            Some(url) => identity_form(url),
            None => format!("unparseable-url {}", shown_textual(&self.0)),
        }
    }

    /// The URL without any part that can hold a secret: the password, the
    /// query and the fragment. May hold a resolved `${VAR}` value, so print
    /// [`Self::rendered`].
    fn shown(&self) -> String {
        match parse(&self.0) {
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
        match parse(&self.0) {
            Some(url) => {
                out = out.replace(url.as_str(), &shown);
                if let Some(password) = password(&url) {
                    secrets.push(password.to_string());
                    secrets.push(percent_decode(password));
                }
                if masks_user_name(&url) {
                    secrets.push(url.username().to_string());
                    secrets.push(percent_decode(url.username()));
                }
                for part in [url.query(), url.fragment()].into_iter().flatten() {
                    secrets.push(part.to_string());
                    secrets.push(percent_decode(part));
                    for pair in part.split('&') {
                        // The value as written (`abc%2Bxyz`) and decoded.
                        let raw_value = pair.split_once('=').map_or(pair, |(_, v)| v);
                        secrets.push(raw_value.to_string());
                        secrets.push(percent_decode(raw_value));
                    }
                }
            }
            None => {
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

/// The URL as the Redis client and `object_store` parse it, or `None` when
/// it has no authority (`user:pw@host:6379` parses as the scheme `user`
/// and an opaque path). Neither client connects with such a URL, and its
/// userinfo split is a guess, so it takes the textual fallback.
fn parse(raw: &str) -> Option<Url> {
    Url::parse(raw).ok().filter(|url| !url.cannot_be_a_base())
}

/// The Azure schemes `object_store` accepts. Their user name is the
/// container (`az://container@account.dfs.core.windows.net/path`).
const AZURE_SCHEMES: &[&str] = &["az", "adl", "azure", "abfs", "abfss"];

/// The schemes the Redis client accepts (redis-1.7.1 `parse_redis_url`).
const REDIS_SCHEMES: &[&str] = &[
    "redis",
    "rediss",
    "valkey",
    "valkeys",
    "redis+unix",
    "valkey+unix",
    "unix",
];

/// The query keys whose values the identity keeps: the non-secret ones the
/// Redis client reads (`db`, `user` and `protocol`; it also reads `pass`).
/// `object_store` reads no query at all.
const KEPT_REDIS_QUERY_KEYS: &[&str] = &["db", "user", "protocol"];

/// The password, or `None` when it is absent or empty (`redis://TOKEN:@h`).
fn password(url: &Url) -> Option<&str> {
    url.password().filter(|p| !p.is_empty())
}

/// Whether the user name is masked: it is set, there is no password, and the
/// scheme is not an Azure one. Without a password the Redis client never
/// sends a user name (`AUTH` and `HELLO ... AUTH` need the password), so a
/// lone user name selects nothing and can only be a misplaced token.
fn masks_user_name(url: &Url) -> bool {
    !url.username().is_empty() && password(url).is_none() && !AZURE_SCHEMES.contains(&url.scheme())
}

/// The password and a lone user name become `***`.
fn mask_userinfo(url: &mut Url) {
    // Neither setter can fail: a URL with a userinfo has a host.
    if password(url).is_some() {
        let _ = url.set_password(Some("***"));
    } else if masks_user_name(url) {
        let _ = url.set_password(None);
        let _ = url.set_username("***");
    }
}

/// [`CredentialUrl::identity`] for a URL that parses.
///
/// The query and fragment are masked by an allow-list of what the clients
/// read, so a value no client reads, secret or not, never reaches a plan:
///
/// - query: `db`, `user` and `protocol` on a Redis scheme are kept. Every
///   other pair is `***=***`, its key name too: a bare `?TOKEN` is a key with
///   no value, and the key is the secret.
/// - fragment: kept only when it is `insecure`, the one fragment the Redis
///   client reads (it turns TLS verification off). Any other is `***`.
fn identity_form(mut url: Url) -> String {
    mask_userinfo(&mut url);
    let redis = REDIS_SCHEMES.contains(&url.scheme());
    if let Some(query) = url.query().map(str::to_string) {
        let mut masked = url::form_urlencoded::Serializer::new(String::new());
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            if redis && KEPT_REDIS_QUERY_KEYS.contains(&key.as_ref()) {
                masked.append_pair(&key, &value);
            } else {
                masked.append_pair("***", "***");
            }
        }
        url.set_query(Some(&masked.finish()));
    }
    if url.fragment().is_some_and(|f| f != "insecure") {
        url.set_fragment(Some("***"));
    }
    url.to_string()
}

/// [`CredentialUrl::shown`] for a URL that parses.
fn shown_form(mut url: Url) -> String {
    mask_userinfo(&mut url);
    if url.query().is_some() {
        url.set_query(Some("***"));
    }
    if url.fragment().is_some() {
        url.set_fragment(Some("***"));
    }
    url.to_string()
}

/// [`CredentialUrl::shown`] for a URL that does not parse. Hides everything
/// that could be a userinfo (up to the LAST `@`) and everything from the
/// first `?` or `#`. It hides more than it must, never less.
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
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> std::borrow::Cow<'static, str> {
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
            // The split the client sees: the authority ends at the first `/`.
            (
                "redis://u:p@s/s?w-SECRET@host:6379",
                "redis://u:***@s/s?***",
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
            // A token put where the user name goes.
            ("redis://TOKEN-SECRET@host:6379", "redis://***@host:6379"),
            // Not a URL the parser accepts: the textual fallback.
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

    /// What a plan stores: never a password, a secret query value or a lone
    /// user name, and never a digest of one.
    #[test]
    fn the_identity_holds_no_secret() {
        for raw in [
            "redis://:pw-SECRET@host:6379",
            "rediss://default:pw-SECRET@host:6379/2",
            "redis://TOKEN-SECRET@host:6379",
            "redis+unix:///run/v.sock?db=1&pass=pw-SECRET",
            "az://c/p/?sv=2024&sig=SIG-SECRET",
            "https://h/p?X-Amz-Signature=SIG-SECRET&X-Amz-Credential=CRED-SECRET",
            "redis://u:p@ss-SECRET@h:6379",
            "user:pw-SECRET@host:6379",
            // A fragment other than `insecure`, and query keys no client reads.
            "redis://h:6379/0#TOKEN-SECRET",
            "redis://h/?auth=A-SECRET&key=K-SECRET&x-api-key=X-SECRET&sas_token=S-SECRET",
            "redis://TOKEN-SECRET:@h:6379",
            // A bare key: the key itself is the secret.
            "redis://h:6379/0?TOKEN-SECRET",
            "az://c/p/?TOKEN-SECRET",
        ] {
            let identity = id(raw);
            assert!(!identity.contains("SECRET"), "{raw} -> {identity}");
            assert!(!identity.contains("blake3"), "{raw} -> {identity}");
        }
    }

    /// Each pair names a different server, database, TLS mode, container or
    /// client setting, so the two identities must differ.
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
            ("redis://user-a:pw@h:6379", "redis://user-b:pw@h:6379"),
            // The query keys the Redis client reads.
            ("redis://h:6379/?protocol=2", "redis://h:6379/?protocol=3"),
            (
                "redis+unix:///s.sock?user=a&pass=p",
                "redis+unix:///s.sock?user=b&pass=p",
            ),
            // A key that exists, against none.
            ("redis://h/", "redis://h/?keyspace=a"),
        ];
        for (a, b) in pairs {
            assert_ne!(id(a), id(b), "{a} and {b} must differ");
        }
    }

    /// A password change, wherever the password sits, must not move the
    /// identity.
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
            // An unencoded `@` in the password: the client reads it as part of
            // the password too.
            ("redis://u:p@ss-A@h:6379", "redis://u:p@ss-B@h:6379"),
            ("redis://TOKEN-A@h:6379", "redis://TOKEN-B@h:6379"),
            ("redis://TOKEN-A:@h:6379", "redis://TOKEN-B:@h:6379"),
            // Values no client reads: a change selects nothing.
            ("redis://h/?keyspace=a", "redis://h/?keyspace=b"),
            (
                "redis://h/?ssl_keyfile=/a.key",
                "redis://h/?ssl_keyfile=/b.key",
            ),
            ("redis://h:6379/0#TOKEN-A", "redis://h:6379/0#TOKEN-B"),
        ];
        for (a, b) in pairs {
            assert_eq!(id(a), id(b), "{a} and {b} must agree");
        }
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
        let raw = "foo://user:PW%2FSECRET@h/p?sig=SIG%2BSECRET#FRAG-SECRET";
        let url = CredentialUrl::from(raw);
        let parsed = Url::parse(raw).unwrap();
        let text = format!(
            "raw {raw} | parsed \"{parsed}\" | prefix {} | decoded PW/SECRET \
             | value SIG+SECRET | encoded SIG%2BSECRET",
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
    fn scrub_removes_a_token_used_as_the_user_name() {
        let url = CredentialUrl::from("redis://TOKEN-SECRET@h:6379");
        let scrubbed = url.scrub("auth failed for TOKEN-SECRET at redis://TOKEN-SECRET@h:6379");
        assert!(!scrubbed.contains("SECRET"), "{scrubbed}");
    }

    #[test]
    fn deserializes_from_a_plain_string() {
        let url: CredentialUrl = serde_json::from_str("\"redis://:pw@h:1\"").unwrap();
        assert_eq!(url.expose(), "redis://:pw@h:1");
    }
}
