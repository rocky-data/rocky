//! The browser UI's session cookie.
//!
//! `rocky serve --ui` prints `http://<host>:<port>/login?t=<token>`. Opening
//! it exchanges the bearer token for a cookie, and the page then talks to the
//! API with that cookie instead of holding the token in script.
//!
//! ```text
//!   GET /login?t=<token>  --(constant-time match)-->  303 /ui/
//!                                                     Set-Cookie: rocky_ui=<keyed hash>
//!   /api/v1/* with the cookie --> authenticated with the token's scope
//! ```
//!
//! The cookie value is NOT the token. It is a keyed blake3 hash of the token
//! and its scope, under a key made at every start and never written down. So
//! a cookie read out of a browser profile cannot be replayed as a bearer
//! token, and a restart ends every session. One server has one token, so a
//! valid cookie means exactly that token, and the token's scope applies.
//!
//! The cookie is `HttpOnly` (no script reads it), `SameSite=Strict` (no other
//! site's request carries it) and a session cookie (no `Max-Age`). Because a
//! browser adds a cookie by itself, a cookie-authenticated request with a
//! non-safe method must also carry an allowed `Origin` and `X-Rocky-UI: 1`;
//! see [`crate::auth::require_bearer_token`].

use crate::auth::{ServeToken, TokenScope};

/// The cookie's name.
pub const UI_SESSION_COOKIE: &str = "rocky_ui";

/// The header a cookie-authenticated write must carry. A cross-site form
/// cannot set a custom header, and a cross-site `fetch` that sets one needs
/// a CORS preflight this server does not grant.
pub const UI_WRITE_HEADER: &str = "x-rocky-ui";

/// A per-process key for [`session_cookie_value`]. 244 random bits from two
/// v4 UUIDs (the thread RNG, seeded from the OS), hashed to 32 bytes.
#[derive(Clone)]
pub struct UiSessionKey([u8; 32]);

impl std::fmt::Debug for UiSessionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UiSessionKey(..)")
    }
}

impl UiSessionKey {
    /// A fresh key. Called once per [`crate::state::ServerState`].
    pub fn generate() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        hasher.update(uuid::Uuid::new_v4().as_bytes());
        Self(*hasher.finalize().as_bytes())
    }
}

/// The cookie value for `token`: hex of a keyed blake3 over a domain tag, the
/// scope, and the secret.
pub fn session_cookie_value(key: &UiSessionKey, token: &ServeToken) -> String {
    let scope: &[u8] = match token.scope {
        TokenScope::Full => b"full",
        TokenScope::ReadOnly => b"read-only",
    };
    let mut hasher = blake3::Hasher::new_keyed(&key.0);
    hasher.update(b"rocky-ui-session-v1\0");
    hasher.update(scope);
    hasher.update(b"\0");
    hasher.update(token.secret.as_bytes());
    hasher.finalize().to_hex().to_string()
}

/// Whether any `Cookie` header carries a `rocky_ui` value that matches
/// `token` under `key`. Every candidate is compared in constant time.
pub fn cookie_authenticates(
    key: &UiSessionKey,
    token: &ServeToken,
    cookie_headers: &[&str],
) -> bool {
    let expected = session_cookie_value(key, token);
    let mut matched = false;
    for header in cookie_headers {
        for pair in header.split(';') {
            let Some((name, value)) = pair.trim().split_once('=') else {
                continue;
            };
            if name.trim() == UI_SESSION_COOKIE
                && crate::auth::constant_time_eq(value.trim().as_bytes(), expected.as_bytes())
            {
                matched = true;
            }
        }
    }
    matched
}

/// Whether a presented login token is the configured one (constant time).
pub fn login_token_matches(token: &ServeToken, presented: &str) -> bool {
    crate::auth::constant_time_eq(presented.as_bytes(), token.secret.as_bytes())
}

/// The `Set-Cookie` value for a fresh session. `secure` adds `Secure`, for a
/// request that arrived over https (a TLS proxy in front).
pub fn set_cookie_header(key: &UiSessionKey, token: &ServeToken, secure: bool) -> String {
    let value = session_cookie_value(key, token);
    let secure = if secure { "; Secure" } else { "" };
    format!("{UI_SESSION_COOKIE}={value}; HttpOnly; SameSite=Strict; Path=/{secure}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(scope: TokenScope) -> ServeToken {
        ServeToken {
            secret: "s3cret-token".to_string(),
            scope,
        }
    }

    #[test]
    fn the_cookie_is_not_the_token_and_binds_key_and_scope() {
        let key = UiSessionKey::generate();
        let full = session_cookie_value(&key, &token(TokenScope::Full));
        assert!(!full.contains("s3cret-token"));
        assert_eq!(full.len(), 64);
        assert_ne!(
            full,
            session_cookie_value(&key, &token(TokenScope::ReadOnly)),
            "the scope is bound into the value"
        );
        assert_ne!(
            full,
            session_cookie_value(&UiSessionKey::generate(), &token(TokenScope::Full)),
            "a restart (new key) ends the session"
        );
    }

    #[test]
    fn a_cookie_header_authenticates_only_with_the_right_value() {
        let key = UiSessionKey::generate();
        let t = token(TokenScope::Full);
        let good = format!(
            "other=1; {UI_SESSION_COOKIE}={}",
            session_cookie_value(&key, &t)
        );
        assert!(cookie_authenticates(&key, &t, &[good.as_str()]));
        assert!(!cookie_authenticates(&key, &t, &["rocky_ui=s3cret-token"]));
        assert!(!cookie_authenticates(&key, &t, &["rocky_ui="]));
        assert!(!cookie_authenticates(&key, &t, &[]));
    }

    #[test]
    fn the_set_cookie_header_has_the_required_attributes() {
        let key = UiSessionKey::generate();
        let t = token(TokenScope::ReadOnly);
        let plain = set_cookie_header(&key, &t, false);
        for attr in ["HttpOnly", "SameSite=Strict", "Path=/"] {
            assert!(plain.contains(attr), "{plain}");
        }
        assert!(
            !plain.contains("Max-Age") && !plain.contains("Expires"),
            "{plain}"
        );
        assert!(!plain.contains("Secure"), "{plain}");
        assert!(set_cookie_header(&key, &t, true).ends_with("; Secure"));
    }
}
