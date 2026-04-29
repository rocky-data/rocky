//! BigQuery authentication — Service Account JSON key, ADC, or Bearer token.
//!
//! Credential fields (`private_key`, bearer tokens) are wrapped in
//! [`RedactedString`] so that `Debug` output never leaks secrets.

use std::sync::Arc;
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

/// Serve a cached OAuth access token only if it still has at least this
/// much time left. Mirrors the Databricks adapter so a request in flight
/// when the token crosses the server-side expiry doesn't 401 right
/// before the call finishes. Google access tokens are typically valid
/// for 3600s, so a 60s slack is well below the TTL.
const REFRESH_SLACK: Duration = Duration::from_secs(60);

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("failed to read service account key: {0}")]
    ReadKey(#[from] std::io::Error),

    #[error("failed to parse service account key: {0}")]
    ParseKey(#[source] serde_json::Error),

    #[error("failed to create JWT: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),

    #[error("failed to exchange token: {0}")]
    TokenExchange(String),

    #[error(
        "no authentication method available — set GOOGLE_APPLICATION_CREDENTIALS or provide a bearer token"
    )]
    NoAuth,
}

/// Google Service Account key file format.
///
/// The `private_key` field is wrapped in [`RedactedString`] so that
/// `Debug` never leaks the RSA private key.
#[derive(Clone, Deserialize)]
pub struct ServiceAccountKey {
    #[serde(rename = "type")]
    pub key_type: String,
    pub project_id: String,
    pub private_key_id: String,
    pub private_key: RedactedString,
    pub client_email: String,
    pub token_uri: String,
}

impl std::fmt::Debug for ServiceAccountKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceAccountKey")
            .field("key_type", &self.key_type)
            .field("project_id", &self.project_id)
            .field("private_key_id", &self.private_key_id)
            .field("private_key", &"***")
            .field("client_email", &self.client_email)
            .field("token_uri", &self.token_uri)
            .finish()
    }
}

/// JWT claims for Google OAuth2.
#[derive(Debug, Serialize)]
struct Claims {
    iss: String,
    scope: String,
    aud: String,
    exp: u64,
    iat: u64,
}

/// Cached exchanged OAuth access token. Mirrors the Databricks pattern
/// (`rocky-databricks/src/auth.rs`). `pub` only because it appears in
/// the public `BigQueryAuth::ServiceAccount` variant; consumers should
/// treat it as opaque.
#[derive(Clone)]
pub struct CachedToken {
    access_token: RedactedString,
    expires_at: Instant,
}

impl CachedToken {
    fn is_fresh(&self) -> bool {
        self.expires_at > Instant::now() + REFRESH_SLACK
    }
}

/// BigQuery authentication provider.
///
/// Credential fields are wrapped in [`RedactedString`]; the custom `Debug`
/// impl prints `***` instead of raw secrets.
///
/// The `ServiceAccount` variant carries an in-memory access-token cache
/// so each `get_token` call doesn't mint + exchange a fresh JWT (~100ms
/// of latency per call). The cache is keyed by the auth instance — clones
/// of the same `BigQueryAuth` share the same cache via `Arc`.
#[derive(Clone)]
pub enum BigQueryAuth {
    /// Service Account JSON key with cached exchanged access token.
    ServiceAccount {
        key: ServiceAccountKey,
        cached_token: Arc<RwLock<Option<CachedToken>>>,
    },
    /// Pre-supplied Bearer token (e.g., from ADC or gcloud).
    Bearer(RedactedString),
}

impl std::fmt::Debug for BigQueryAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BigQueryAuth::ServiceAccount { key, .. } => f
                .debug_tuple("BigQueryAuth::ServiceAccount")
                .field(key)
                .finish(),
            BigQueryAuth::Bearer(_) => f
                .debug_struct("BigQueryAuth::Bearer")
                .field("token", &"***")
                .finish(),
        }
    }
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// Lifetime of the access token in seconds. Used to populate the
    /// local cache so subsequent `get_token` calls within the window
    /// skip the JWT mint + exchange roundtrip.
    expires_in: u64,
}

impl BigQueryAuth {
    /// Construct a `ServiceAccount` variant from a parsed key. Initializes
    /// an empty access-token cache.
    pub fn service_account(key: ServiceAccountKey) -> Self {
        BigQueryAuth::ServiceAccount {
            key,
            cached_token: Arc::new(RwLock::new(None)),
        }
    }

    /// Auto-detect authentication from environment.
    pub fn from_env() -> Result<Self, AuthError> {
        // 1. Check for explicit bearer token
        if let Ok(token) = std::env::var("BIGQUERY_TOKEN") {
            return Ok(BigQueryAuth::Bearer(RedactedString::new(token)));
        }

        // 2. Check for GOOGLE_APPLICATION_CREDENTIALS
        if let Ok(path) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
            let content = std::fs::read_to_string(&path)?;
            let key: ServiceAccountKey =
                serde_json::from_str(&content).map_err(AuthError::ParseKey)?;
            return Ok(BigQueryAuth::service_account(key));
        }

        Err(AuthError::NoAuth)
    }

    /// Get a bearer token for API calls.
    ///
    /// For `ServiceAccount`, returns the cached exchanged access token if
    /// it still has at least [`REFRESH_SLACK`] left; otherwise mints a
    /// fresh JWT, exchanges it at Google's token endpoint, caches the
    /// result with the server-supplied `expires_in`, and returns it.
    pub async fn get_token(&self, client: &reqwest::Client) -> Result<String, AuthError> {
        match self {
            BigQueryAuth::Bearer(token) => Ok(token.expose().to_string()),
            BigQueryAuth::ServiceAccount { key, cached_token } => {
                // Fast path: cache hit under a read lock.
                if let Some(token) = read_fresh_token(cached_token).await {
                    return Ok(token);
                }

                // Slow path: take the write lock, double-check (someone
                // else may have refreshed while we were waiting), then
                // mint + exchange.
                let mut cache = cached_token.write().await;
                if let Some(ct) = cache.as_ref().filter(|ct| ct.is_fresh()) {
                    return Ok(ct.access_token.expose().to_string());
                }

                let now = chrono::Utc::now().timestamp() as u64;
                let claims = Claims {
                    iss: key.client_email.clone(),
                    scope: "https://www.googleapis.com/auth/bigquery".to_string(),
                    aud: key.token_uri.clone(),
                    iat: now,
                    exp: now + 3600,
                };

                let header = Header::new(Algorithm::RS256);
                let encoding_key = EncodingKey::from_rsa_pem(key.private_key.expose().as_bytes())?;
                let jwt = encode(&header, &claims, &encoding_key)?;

                // Exchange JWT for access token.
                let resp = client
                    .post(&key.token_uri)
                    .form(&[
                        ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
                        ("assertion", &jwt),
                    ])
                    .send()
                    .await
                    .map_err(|e| AuthError::TokenExchange(e.to_string()))?;

                if !resp.status().is_success() {
                    let body = resp
                        .text()
                        .await
                        .unwrap_or_else(|_| "unknown error".to_string());
                    return Err(AuthError::TokenExchange(body));
                }

                let token: TokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| AuthError::TokenExchange(e.to_string()))?;

                *cache = Some(CachedToken {
                    access_token: RedactedString::new(token.access_token.clone()),
                    expires_at: Instant::now() + Duration::from_secs(token.expires_in),
                });

                Ok(token.access_token)
            }
        }
    }

    /// Invalidate any cached access token so the next `get_token` call
    /// forces a fresh JWT exchange. Bearer auth has no cache and is a
    /// no-op. Useful after a server 401 — long pipelines can otherwise
    /// keep replaying a server-expired token from the local cache.
    pub async fn invalidate_cache(&self) {
        if let BigQueryAuth::ServiceAccount { cached_token, .. } = self {
            *cached_token.write().await = None;
        }
    }
}

/// Fast-path cache read: returns the cached access token if it still has
/// at least [`REFRESH_SLACK`] left, otherwise `None`.
async fn read_fresh_token(cache: &RwLock<Option<CachedToken>>) -> Option<String> {
    cache
        .read()
        .await
        .as_ref()
        .filter(|ct| ct.is_fresh())
        .map(|ct| ct.access_token.expose().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_key() -> ServiceAccountKey {
        ServiceAccountKey {
            key_type: "service_account".into(),
            project_id: "my-project".into(),
            private_key_id: "key-id-123".into(),
            private_key: RedactedString::new(
                "-----BEGIN RSA PRIVATE KEY-----\nSECRET\n-----END RSA PRIVATE KEY-----".into(),
            ),
            client_email: "test@project.iam.gserviceaccount.com".into(),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        }
    }

    #[test]
    fn debug_bearer_hides_token() {
        let auth = BigQueryAuth::Bearer(RedactedString::new("super_secret_bearer".into()));
        let debug = format!("{auth:?}");
        assert!(
            !debug.contains("super_secret_bearer"),
            "bearer token leaked in Debug: {debug}"
        );
        assert!(debug.contains("***"));
    }

    #[test]
    fn debug_service_account_hides_private_key() {
        let auth = BigQueryAuth::service_account(fake_key());
        let debug = format!("{auth:?}");
        assert!(
            !debug.contains("SECRET"),
            "private key leaked in Debug: {debug}"
        );
        assert!(
            !debug.contains("BEGIN RSA"),
            "private key leaked in Debug: {debug}"
        );
        assert!(debug.contains("***"));
        // Non-secret fields should still be visible.
        assert!(debug.contains("my-project"));
        assert!(debug.contains("test@project.iam.gserviceaccount.com"));
    }

    #[test]
    fn service_account_key_debug_hides_private_key() {
        let key = ServiceAccountKey {
            key_type: "service_account".into(),
            project_id: "proj".into(),
            private_key_id: "kid".into(),
            private_key: RedactedString::new("raw_secret_key_material".into()),
            client_email: "sa@proj.iam.gserviceaccount.com".into(),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        };
        let debug = format!("{key:?}");
        assert!(
            !debug.contains("raw_secret_key_material"),
            "private key leaked: {debug}"
        );
        assert!(debug.contains("***"));
    }

    #[tokio::test]
    async fn cache_starts_empty() {
        let auth = BigQueryAuth::service_account(fake_key());
        if let BigQueryAuth::ServiceAccount { cached_token, .. } = &auth {
            assert!(cached_token.read().await.is_none());
        } else {
            panic!("expected ServiceAccount variant");
        }
    }

    #[tokio::test]
    async fn invalidate_cache_clears_token() {
        let auth = BigQueryAuth::service_account(fake_key());
        if let BigQueryAuth::ServiceAccount { cached_token, .. } = &auth {
            *cached_token.write().await = Some(CachedToken {
                access_token: RedactedString::new("primed_token".into()),
                expires_at: Instant::now() + Duration::from_secs(3600),
            });
            assert!(cached_token.read().await.is_some());
        }
        auth.invalidate_cache().await;
        if let BigQueryAuth::ServiceAccount { cached_token, .. } = &auth {
            assert!(cached_token.read().await.is_none());
        }
    }

    #[tokio::test]
    async fn invalidate_cache_noop_on_bearer() {
        let auth = BigQueryAuth::Bearer(RedactedString::new("bearer_tok".into()));
        // Should not panic.
        auth.invalidate_cache().await;
        let client = reqwest::Client::new();
        assert_eq!(auth.get_token(&client).await.unwrap(), "bearer_tok");
    }

    #[tokio::test]
    async fn fresh_cached_token_is_returned() {
        let auth = BigQueryAuth::service_account(fake_key());
        if let BigQueryAuth::ServiceAccount { cached_token, .. } = &auth {
            *cached_token.write().await = Some(CachedToken {
                access_token: RedactedString::new("cached_access_token".into()),
                expires_at: Instant::now() + Duration::from_secs(3600),
            });
        }
        let client = reqwest::Client::new();
        // No network call should happen — the cache is still fresh.
        let token = auth.get_token(&client).await.unwrap();
        assert_eq!(token, "cached_access_token");
    }

    #[tokio::test]
    async fn expired_cached_token_is_not_used() {
        // We can't easily mock the token endpoint here, but we can prove
        // the cache freshness check rejects an expired entry by inspecting
        // `read_fresh_token` directly.
        let cache: RwLock<Option<CachedToken>> = RwLock::new(Some(CachedToken {
            access_token: RedactedString::new("expired".into()),
            // Already past the slack window.
            expires_at: Instant::now() + Duration::from_secs(1),
        }));
        assert!(read_fresh_token(&cache).await.is_none());

        let cache_fresh: RwLock<Option<CachedToken>> = RwLock::new(Some(CachedToken {
            access_token: RedactedString::new("ok".into()),
            expires_at: Instant::now() + Duration::from_secs(3600),
        }));
        assert_eq!(read_fresh_token(&cache_fresh).await.as_deref(), Some("ok"));
    }
}
