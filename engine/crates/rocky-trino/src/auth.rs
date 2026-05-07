//! Trino authentication.
//!
//! v0 supports two methods, in real-world frequency order for self-hosted
//! Trino clusters:
//!
//! 1. **HTTP Basic** — `Authorization: Basic base64(<user>:<password>)`,
//!    used with the `password-authenticator.properties` configured on the
//!    coordinator. The 90% case for self-hosted clusters.
//! 2. **JWT bearer** — `Authorization: Bearer <token>`, validated by the
//!    coordinator against a JWKS URL or pre-shared key. Common in managed
//!    offerings (Starburst Galaxy issues JWTs via PAT).
//!
//! Kerberos / SPNEGO and OAuth 2.0 are deferred to follow-up PRs.
//!
//! Credentials are wrapped in [`RedactedString`] so `Debug` output never
//! leaks raw secrets.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use rocky_core::redacted::RedactedString;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("Trino auth requires either basic (user + password) or jwt (token); none provided")]
    NoAuth,

    #[error("Trino basic auth requires both user and password")]
    BasicMissingField,

    #[error("Trino jwt auth requires a non-empty token")]
    JwtEmptyToken,
}

/// Trino authentication provider.
///
/// Wrapped credentials use [`RedactedString`]; the custom `Debug` impl
/// prints `***` so logs never leak raw passwords or tokens.
#[derive(Clone)]
pub enum TrinoAuth {
    /// HTTP Basic — `Authorization: Basic base64(<user>:<password>)`.
    Basic {
        user: String,
        password: RedactedString,
    },
    /// JWT bearer — `Authorization: Bearer <token>`.
    Jwt(RedactedString),
}

impl std::fmt::Debug for TrinoAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TrinoAuth::Basic { user, .. } => f
                .debug_struct("TrinoAuth::Basic")
                .field("user", user)
                .field("password", &"***")
                .finish(),
            TrinoAuth::Jwt(_) => f
                .debug_struct("TrinoAuth::Jwt")
                .field("token", &"***")
                .finish(),
        }
    }
}

impl TrinoAuth {
    /// Construct a Basic-auth provider.
    ///
    /// # Errors
    /// Returns [`AuthError::BasicMissingField`] when either field is empty.
    pub fn basic(user: impl Into<String>, password: impl Into<String>) -> Result<Self, AuthError> {
        let user = user.into();
        let password = password.into();
        if user.is_empty() || password.is_empty() {
            return Err(AuthError::BasicMissingField);
        }
        Ok(TrinoAuth::Basic {
            user,
            password: RedactedString::new(password),
        })
    }

    /// Construct a JWT-bearer provider.
    ///
    /// # Errors
    /// Returns [`AuthError::JwtEmptyToken`] if `token` is empty.
    pub fn jwt(token: impl Into<String>) -> Result<Self, AuthError> {
        let token = token.into();
        if token.is_empty() {
            return Err(AuthError::JwtEmptyToken);
        }
        Ok(TrinoAuth::Jwt(RedactedString::new(token)))
    }

    /// Return the Trino username advertised on the `X-Trino-User` header.
    ///
    /// Trino requires `X-Trino-User` on every request; for Basic auth it
    /// matches the credentials user. For JWT auth the token's `sub` claim
    /// is the canonical user, but Trino accepts a separate header value
    /// — callers pass an explicit `runtime_user` when they need to
    /// override the JWT-derived default.
    pub fn trino_user_default(&self) -> Option<&str> {
        match self {
            TrinoAuth::Basic { user, .. } => Some(user.as_str()),
            TrinoAuth::Jwt(_) => None,
        }
    }

    /// Render the `Authorization` header value.
    ///
    /// - Basic: `"Basic <base64(user:password)>"`.
    /// - JWT:   `"Bearer <token>"`.
    pub fn authorization_header(&self) -> String {
        match self {
            TrinoAuth::Basic { user, password } => {
                let raw = format!("{}:{}", user, password.expose());
                let encoded = BASE64_STANDARD.encode(raw.as_bytes());
                format!("Basic {encoded}")
            }
            TrinoAuth::Jwt(token) => format!("Bearer {}", token.expose()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_rejects_empty_fields() {
        assert!(TrinoAuth::basic("", "pw").is_err());
        assert!(TrinoAuth::basic("u", "").is_err());
    }

    #[test]
    fn basic_auth_encodes_user_password() {
        // Decode-and-compare instead of asserting against a base64 literal:
        // a literal `Basic <base64-of-user:pass>` in source trips GitHub's
        // secret scanner (it pattern-matches the encoded form regardless
        // of whether the credential is real). The decoded round-trip below
        // verifies the same property — `Basic <prefix>` + `base64(user:pass)`
        // payload — without storing the encoded literal.
        let (user, pass, auth) = crate::test_helpers::test_basic_auth_inputs();
        let header = auth.authorization_header();
        let encoded = header
            .strip_prefix("Basic ")
            .expect("header should start with 'Basic '");
        let decoded = BASE64_STANDARD
            .decode(encoded)
            .expect("payload should be valid base64");
        assert_eq!(decoded, format!("{user}:{pass}").as_bytes());
    }

    #[test]
    fn jwt_auth_rejects_empty_token() {
        assert!(TrinoAuth::jwt("").is_err());
    }

    #[test]
    fn jwt_auth_emits_bearer_header() {
        let auth = TrinoAuth::jwt("eyJ.signed.token").unwrap();
        assert_eq!(auth.authorization_header(), "Bearer eyJ.signed.token");
    }

    #[test]
    fn debug_basic_redacts_password() {
        let (user, pass, auth) = crate::test_helpers::test_basic_auth_inputs();
        let debug = format!("{auth:?}");
        assert!(!debug.contains(&pass));
        assert!(debug.contains("***"));
        assert!(debug.contains(&user));
    }

    #[test]
    fn debug_jwt_redacts_token() {
        let auth = TrinoAuth::jwt("very_secret_jwt_value").unwrap();
        let debug = format!("{auth:?}");
        assert!(!debug.contains("very_secret_jwt_value"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn trino_user_default_returns_user_for_basic() {
        let (user, _, auth) = crate::test_helpers::test_basic_auth_inputs();
        assert_eq!(auth.trino_user_default(), Some(user.as_str()));
    }

    #[test]
    fn trino_user_default_is_none_for_jwt() {
        let auth = TrinoAuth::jwt("x").unwrap();
        assert!(auth.trino_user_default().is_none());
    }
}
