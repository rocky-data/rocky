//! Snowflake authentication providers.
//!
//! Supports three authentication methods:
//! - **Password** — Username/password exchanged for a session token
//! - **OAuth** — External OAuth token exchange (e.g., from an IdP)
//! - **Key-pair** — RS256 JWT signed with a private RSA key (PKCS#8 PEM)
//!
//! Auth auto-detection priority: OAuth > key-pair > password.

use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use rocky_core::redacted::RedactedString;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::RwLock;

/// Snowflake's documented JWT lifetime cap is 60 minutes. Minting for
/// 59 min lets a long-running pipeline cover almost the full hour while
/// leaving headroom for the refresh-slack check below.
const JWT_TTL_SECS: u64 = 59 * 60;

/// Serve a cached token only if it still has at least this much time
/// left. A request in flight when the token crosses the server-side
/// expiry boundary would otherwise 401; the connector retry loop
/// re-mints on 401, but burning a retry on every refresh boundary is
/// wasteful.
const REFRESH_SLACK: Duration = Duration::from_secs(60);

/// Errors from Snowflake authentication.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("no auth configured: set password, OAuth, or key-pair credentials")]
    NoAuthConfigured,

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("auth response missing session token")]
    MissingToken,

    #[error("key-pair I/O error: {0}")]
    KeyPairIo(String),

    #[error("key-pair invalid: {0}")]
    KeyPairInvalid(String),

    #[error("auth API error {status}: {body}")]
    ApiError { status: u16, body: String },
}

/// Configuration for Snowflake authentication.
pub struct AuthConfig {
    /// Snowflake account identifier (e.g., "xy12345.us-east-1").
    pub account: String,
    /// Username for password auth.
    pub username: Option<String>,
    /// Password for password auth.
    pub password: Option<String>,
    /// OAuth access token (pre-obtained from an IdP).
    pub oauth_token: Option<String>,
    /// Path to RSA private key file (PEM) for key-pair auth.
    pub private_key_path: Option<String>,
}

/// Opaque auth handle. Use [`Auth::from_config`] to create.
#[derive(Clone)]
pub struct Auth {
    inner: AuthInner,
}

#[derive(Clone)]
enum AuthInner {
    /// Pre-supplied OAuth token (no refresh — caller must supply a valid token).
    OAuth { token: RedactedString },
    /// Username/password, exchanged for a Snowflake session token.
    Password {
        account: String,
        username: String,
        password: RedactedString,
        http_client: Client,
        cached_token: Arc<RwLock<Option<CachedToken>>>,
    },
    /// Key-pair JWT auth: RS256 JWT signed with a private RSA key.
    KeyPair {
        account: String,
        username: String,
        private_key_path: String,
        cached_token: Arc<RwLock<Option<CachedToken>>>,
    },
}

#[derive(Clone)]
struct CachedToken {
    token: RedactedString,
    expires_at: Instant,
}

impl CachedToken {
    /// Returns true if the token is valid and not within `REFRESH_SLACK`
    /// of server-side expiry.
    fn is_fresh(&self) -> bool {
        self.expires_at > Instant::now() + REFRESH_SLACK
    }
}

/// Fast-path cache read. Returns the cached token if one exists and is
/// still within the refresh-slack window; otherwise returns `None` and
/// the caller is expected to acquire the write lock and mint a new one.
async fn read_fresh_token(cache: &RwLock<Option<CachedToken>>) -> Option<String> {
    cache
        .read()
        .await
        .as_ref()
        .filter(|ct| ct.is_fresh())
        .map(|ct| ct.token.expose().to_string())
}

/// Snowflake login response (simplified).
#[derive(Debug, Deserialize)]
struct LoginResponse {
    data: Option<LoginData>,
    message: Option<String>,
    success: bool,
}

#[derive(Debug, Deserialize)]
struct LoginData {
    token: Option<String>,
    #[serde(rename = "validityInSeconds")]
    validity_in_seconds: Option<u64>,
}

impl Auth {
    /// Creates an Auth provider using auto-detection:
    /// 1. If `oauth_token` is set -> OAuth
    /// 2. If `private_key_path` is set -> Key-pair (RS256 JWT)
    /// 3. If `username` + `password` are set -> Password
    /// 4. Otherwise -> error
    pub fn from_config(config: AuthConfig) -> Result<Self, AuthError> {
        // OAuth takes highest priority
        if let Some(token) = config.oauth_token.filter(|t| !t.is_empty()) {
            return Ok(Auth {
                inner: AuthInner::OAuth {
                    token: RedactedString::new(token),
                },
            });
        }

        // Key-pair auth (stub — will error when get_token is called)
        if let Some(key_path) = config.private_key_path.filter(|p| !p.is_empty()) {
            if let Some(ref username) = config.username {
                if !username.is_empty() {
                    return Ok(Auth {
                        inner: AuthInner::KeyPair {
                            account: config.account,
                            username: username.clone(),
                            private_key_path: key_path,
                            cached_token: Arc::new(RwLock::new(None)),
                        },
                    });
                }
            }
        }

        // Password auth
        match (config.username, config.password) {
            (Some(user), Some(pass)) if !user.is_empty() && !pass.is_empty() => Ok(Auth {
                inner: AuthInner::Password {
                    account: config.account,
                    username: user,
                    password: RedactedString::new(pass),
                    http_client: Client::new(),
                    cached_token: Arc::new(RwLock::new(None)),
                },
            }),
            _ => Err(AuthError::NoAuthConfigured),
        }
    }

    /// Returns a valid Bearer token, refreshing if necessary.
    pub async fn get_token(&self) -> Result<String, AuthError> {
        match &self.inner {
            AuthInner::OAuth { token } => Ok(token.expose().to_string()),

            AuthInner::KeyPair {
                account,
                username,
                private_key_path,
                cached_token,
            } => {
                if let Some(token) = read_fresh_token(cached_token).await {
                    return Ok(token);
                }

                let mut cache = cached_token.write().await;
                if let Some(ct) = cache.as_ref().filter(|ct| ct.is_fresh()) {
                    return Ok(ct.token.expose().to_string());
                }

                use base64::Engine;
                use base64::engine::general_purpose::STANDARD as BASE64;
                use rsa::RsaPrivateKey;
                use rsa::pkcs8::{DecodePrivateKey, EncodePublicKey};
                use sha2::{Digest, Sha256};

                // Read PEM private key
                let pem_str = std::fs::read_to_string(private_key_path)
                    .map_err(|e| AuthError::KeyPairIo(e.to_string()))?;

                // Parse RSA private key from PKCS#8 PEM
                let private_key = RsaPrivateKey::from_pkcs8_pem(&pem_str)
                    .map_err(|e| AuthError::KeyPairInvalid(format!("invalid PEM: {e}")))?;

                // Derive public key and encode as DER for fingerprint
                let public_key = private_key.to_public_key();
                let pub_der = public_key
                    .to_public_key_der()
                    .map_err(|e| AuthError::KeyPairInvalid(format!("DER encode failed: {e}")))?;

                // SHA-256 fingerprint of the DER-encoded public key (SPKI)
                let fingerprint = Sha256::digest(pub_der.as_bytes());
                let fp_b64 = BASE64.encode(fingerprint);

                // Snowflake issuer format: ACCOUNT.USER.SHA256:<base64 fingerprint>
                let account_upper = account.to_uppercase();
                let user_upper = username.to_uppercase();
                let issuer = format!("{account_upper}.{user_upper}.SHA256:{fp_b64}");

                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                #[derive(serde::Serialize)]
                struct Claims {
                    iss: String,
                    sub: String,
                    iat: u64,
                    exp: u64,
                }

                let claims = Claims {
                    iss: issuer,
                    sub: format!("{account_upper}.{user_upper}"),
                    iat: now,
                    exp: now + JWT_TTL_SECS,
                };

                let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(pem_str.as_bytes())
                    .map_err(|e| AuthError::KeyPairInvalid(format!("JWT key error: {e}")))?;

                let header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
                let token = jsonwebtoken::encode(&header, &claims, &encoding_key)
                    .map_err(|e| AuthError::KeyPairInvalid(format!("JWT encode failed: {e}")))?;

                *cache = Some(CachedToken {
                    token: RedactedString::new(token.clone()),
                    expires_at: Instant::now() + Duration::from_secs(JWT_TTL_SECS),
                });

                Ok(token)
            }

            AuthInner::Password {
                account,
                username,
                password,
                http_client,
                cached_token,
            } => {
                if let Some(token) = read_fresh_token(cached_token).await {
                    return Ok(token);
                }

                let mut cache = cached_token.write().await;
                if let Some(ct) = cache.as_ref().filter(|ct| ct.is_fresh()) {
                    return Ok(ct.token.expose().to_string());
                }

                let url =
                    format!("https://{account}.snowflakecomputing.com/session/v1/login-request");

                let body = serde_json::json!({
                    "data": {
                        "ACCOUNT_NAME": account,
                        "LOGIN_NAME": username,
                        "PASSWORD": password.expose(),
                        "CLIENT_APP_ID": "Rocky",
                        "CLIENT_APP_VERSION": env!("CARGO_PKG_VERSION"),
                    }
                });

                let resp = http_client
                    .post(&url)
                    .header("Content-Type", "application/json")
                    .header("Accept", "application/json")
                    .json(&body)
                    .send()
                    .await?;

                if !resp.status().is_success() {
                    let status = resp.status().as_u16();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(AuthError::ApiError { status, body: text });
                }

                let login_resp: LoginResponse = resp.json().await?;

                if !login_resp.success {
                    return Err(AuthError::ApiError {
                        status: 401,
                        body: login_resp
                            .message
                            .unwrap_or_else(|| "login failed".to_string()),
                    });
                }

                let data = login_resp.data.ok_or(AuthError::MissingToken)?;
                let token = data.token.ok_or(AuthError::MissingToken)?;
                let validity = data.validity_in_seconds.unwrap_or(3600);

                let new_cached = CachedToken {
                    token: RedactedString::new(token.clone()),
                    expires_at: Instant::now() + Duration::from_secs(validity),
                };
                *cache = Some(new_cached);

                Ok(token)
            }
        }
    }

    /// Invalidates any cached session/JWT token so the next `get_token`
    /// call forces a fresh mint. Called when the server returns 401 —
    /// a long-running pipeline may otherwise keep replaying an expired
    /// token from the cache.
    pub async fn invalidate_cache(&self) {
        match &self.inner {
            AuthInner::OAuth { .. } => {
                // OAuth here is caller-supplied and not refreshable by us.
            }
            AuthInner::Password { cached_token, .. } | AuthInner::KeyPair { cached_token, .. } => {
                let mut cache = cached_token.write().await;
                *cache = None;
            }
        }
    }

    #[cfg(test)]
    fn is_oauth(&self) -> bool {
        matches!(self.inner, AuthInner::OAuth { .. })
    }

    #[cfg(test)]
    fn is_password(&self) -> bool {
        matches!(self.inner, AuthInner::Password { .. })
    }

    #[cfg(test)]
    fn is_key_pair(&self) -> bool {
        matches!(self.inner, AuthInner::KeyPair { .. })
    }

    #[cfg(test)]
    async fn has_cached_token(&self) -> bool {
        match &self.inner {
            AuthInner::OAuth { .. } => false,
            AuthInner::Password { cached_token, .. } | AuthInner::KeyPair { cached_token, .. } => {
                cached_token.read().await.is_some()
            }
        }
    }

    #[cfg(test)]
    async fn prime_cache_with(&self, token: &str, ttl: Duration) {
        let new_cached = CachedToken {
            token: RedactedString::new(token.to_string()),
            expires_at: Instant::now() + ttl,
        };
        match &self.inner {
            AuthInner::OAuth { .. } => {}
            AuthInner::Password { cached_token, .. } | AuthInner::KeyPair { cached_token, .. } => {
                *cached_token.write().await = Some(new_cached);
            }
        }
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            AuthInner::OAuth { .. } => f
                .debug_struct("Auth::OAuth")
                .field("token", &"***")
                .finish(),
            AuthInner::Password {
                account, username, ..
            } => f
                .debug_struct("Auth::Password")
                .field("account", account)
                .field("username", username)
                .finish_non_exhaustive(),
            AuthInner::KeyPair {
                account, username, ..
            } => f
                .debug_struct("Auth::KeyPair")
                .field("account", account)
                .field("username", username)
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oauth_auth() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: None,
            password: None,
            oauth_token: Some("oauth_token_abc".into()),
            private_key_path: None,
        })
        .unwrap();
        assert!(auth.is_oauth());
    }

    #[test]
    fn test_password_auth() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: Some("pass".into()),
            oauth_token: None,
            private_key_path: None,
        })
        .unwrap();
        assert!(auth.is_password());
    }

    #[test]
    fn test_key_pair_auth() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: None,
            oauth_token: None,
            private_key_path: Some("/path/to/key.pem".into()),
        })
        .unwrap();
        assert!(auth.is_key_pair());
    }

    #[test]
    fn test_oauth_takes_precedence() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: Some("pass".into()),
            oauth_token: Some("token".into()),
            private_key_path: Some("/path/to/key.pem".into()),
        })
        .unwrap();
        assert!(auth.is_oauth());
    }

    #[test]
    fn test_key_pair_before_password() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: Some("pass".into()),
            oauth_token: None,
            private_key_path: Some("/path/to/key.pem".into()),
        })
        .unwrap();
        assert!(auth.is_key_pair());
    }

    #[test]
    fn test_no_auth_configured() {
        let result = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: None,
            password: None,
            oauth_token: None,
            private_key_path: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_password_fails() {
        let result = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: None,
            oauth_token: None,
            private_key_path: None,
        });
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_oauth_get_token() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: None,
            password: None,
            oauth_token: Some("my_oauth_token".into()),
            private_key_path: None,
        })
        .unwrap();
        assert_eq!(auth.get_token().await.unwrap(), "my_oauth_token");
    }

    #[tokio::test]
    async fn test_invalidate_cache_clears_keypair_cache() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: None,
            oauth_token: None,
            private_key_path: Some("/path/to/key.pem".into()),
        })
        .unwrap();
        assert!(!auth.has_cached_token().await);
        auth.prime_cache_with("jwt_token_abc", Duration::from_secs(3540))
            .await;
        assert!(auth.has_cached_token().await);
        auth.invalidate_cache().await;
        assert!(!auth.has_cached_token().await);
    }

    #[tokio::test]
    async fn test_invalidate_cache_clears_password_cache() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: Some("pass".into()),
            oauth_token: None,
            private_key_path: None,
        })
        .unwrap();
        assert!(!auth.has_cached_token().await);
        auth.prime_cache_with("session_token_xyz", Duration::from_secs(3600))
            .await;
        assert!(auth.has_cached_token().await);
        auth.invalidate_cache().await;
        assert!(!auth.has_cached_token().await);
    }

    #[tokio::test]
    async fn test_invalidate_cache_noop_on_oauth() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: None,
            password: None,
            oauth_token: Some("pre_supplied_token".into()),
            private_key_path: None,
        })
        .unwrap();
        auth.invalidate_cache().await; // should not panic
        assert_eq!(auth.get_token().await.unwrap(), "pre_supplied_token");
    }

    #[tokio::test]
    async fn test_keypair_missing_file_returns_error() {
        let auth = Auth::from_config(AuthConfig {
            account: "test_account".into(),
            username: Some("test_user".into()),
            password: None,
            oauth_token: None,
            private_key_path: Some("/nonexistent/key.pem".into()),
        })
        .unwrap();
        let result = auth.get_token().await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, AuthError::KeyPairIo(..)),
            "expected KeyPairIo, got: {err}"
        );
    }

    #[test]
    fn test_debug_hides_secrets() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: None,
            password: None,
            oauth_token: Some("super_secret_token".into()),
            private_key_path: None,
        })
        .unwrap();
        let debug = format!("{auth:?}");
        assert!(!debug.contains("super_secret_token"));
        assert!(debug.contains("***"));
    }

    #[test]
    fn test_empty_oauth_falls_through() {
        let auth = Auth::from_config(AuthConfig {
            account: "xy12345".into(),
            username: Some("user".into()),
            password: Some("pass".into()),
            oauth_token: Some(String::new()),
            private_key_path: None,
        })
        .unwrap();
        assert!(auth.is_password());
    }
}
