use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::Client;
use rocky_core::redacted::RedactedString;
use serde::Deserialize;
use thiserror::Error;
use tokio::sync::RwLock;

/// Errors from Databricks authentication (PAT or OAuth M2M).
#[derive(Debug, Error)]
pub enum AuthError {
    #[error(
        "no auth configured: set DATABRICKS_TOKEN or (DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET)"
    )]
    NoAuthConfigured,

    #[error("OAuth token request failed: {0}")]
    TokenRequest(#[from] reqwest::Error),

    #[error("OAuth token response missing access_token")]
    MissingToken,
}

/// Databricks authentication provider.
///
/// Auto-detects PAT vs OAuth M2M based on available configuration.
/// For OAuth, handles token caching and refresh.
#[derive(Clone)]
pub(crate) enum AuthInner {
    Pat {
        token: RedactedString,
    },
    OAuthM2M {
        host: String,
        client_id: String,
        client_secret: RedactedString,
        http_client: Client,
        cached_token: Arc<RwLock<Option<CachedToken>>>,
    },
}

/// Opaque auth handle. Use [`Auth::from_config`] to create.
#[derive(Clone)]
pub struct Auth {
    inner: AuthInner,
}

#[derive(Clone)]
pub(crate) struct CachedToken {
    access_token: RedactedString,
    expires_at: Instant,
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[allow(dead_code)]
    token_type: String,
    expires_in: u64,
}

/// Configuration for Databricks auth, typically from env vars or config.
pub struct AuthConfig {
    pub host: String,
    pub token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
}

impl Auth {
    /// Creates an Auth provider using auto-detection:
    /// 1. If `token` is set → PAT
    /// 2. If `client_id` + `client_secret` are set → OAuth M2M
    /// 3. Otherwise → error
    pub fn from_config(config: AuthConfig) -> Result<Self, AuthError> {
        if let Some(token) = config.token.filter(|t| !t.is_empty()) {
            return Ok(Auth {
                inner: AuthInner::Pat {
                    token: RedactedString::new(token),
                },
            });
        }

        match (config.client_id, config.client_secret) {
            (Some(id), Some(secret)) if !id.is_empty() && !secret.is_empty() => Ok(Auth {
                inner: AuthInner::OAuthM2M {
                    host: config.host,
                    client_id: id,
                    client_secret: RedactedString::new(secret),
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
            AuthInner::Pat { token } => Ok(token.expose().to_string()),
            AuthInner::OAuthM2M {
                host,
                client_id,
                client_secret,
                http_client,
                cached_token,
            } => {
                // Check cache (with read lock)
                {
                    let cache = cached_token.read().await;
                    if let Some(ct) = cache.as_ref() {
                        if ct.expires_at > Instant::now() + Duration::from_secs(60) {
                            return Ok(ct.access_token.expose().to_string());
                        }
                    }
                }

                // Refresh (with write lock)
                let mut cache = cached_token.write().await;

                // Double-check after acquiring write lock
                if let Some(ct) = cache.as_ref() {
                    if ct.expires_at > Instant::now() + Duration::from_secs(60) {
                        return Ok(ct.access_token.expose().to_string());
                    }
                }

                let token_url = format!("https://{host}/oidc/v1/token");
                let resp = http_client
                    .post(&token_url)
                    .form(&[
                        ("grant_type", "client_credentials"),
                        ("client_id", client_id),
                        ("client_secret", client_secret.expose()),
                        ("scope", "all-apis"),
                    ])
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<OAuthTokenResponse>()
                    .await?;

                let new_token = CachedToken {
                    access_token: RedactedString::new(resp.access_token.clone()),
                    expires_at: Instant::now() + Duration::from_secs(resp.expires_in),
                };
                *cache = Some(new_token);

                Ok(resp.access_token)
            }
        }
    }

    #[cfg(test)]
    fn is_pat(&self) -> bool {
        matches!(self.inner, AuthInner::Pat { .. })
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.inner {
            AuthInner::Pat { .. } => f.debug_struct("Auth::Pat").field("token", &"***").finish(),
            AuthInner::OAuthM2M {
                host, client_id, ..
            } => f
                .debug_struct("Auth::OAuthM2M")
                .field("host", host)
                .field("client_id", client_id)
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pat_auth() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: Some("dapi_test_token".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        assert!(auth.is_pat());
    }

    #[test]
    fn test_oauth_auth() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: None,
            client_id: Some("client_123".into()),
            client_secret: Some("secret_456".into()),
        })
        .unwrap();
        assert!(!auth.is_pat());
    }

    #[test]
    fn test_pat_takes_precedence() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: Some("dapi_test".into()),
            client_id: Some("client_123".into()),
            client_secret: Some("secret_456".into()),
        })
        .unwrap();
        assert!(auth.is_pat());
    }

    #[test]
    fn test_empty_token_falls_through_to_oauth() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: Some(String::new()),
            client_id: Some("client_123".into()),
            client_secret: Some("secret_456".into()),
        })
        .unwrap();
        assert!(!auth.is_pat());
    }

    #[test]
    fn test_no_auth_configured() {
        let result = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: None,
            client_id: None,
            client_secret: None,
        });
        assert!(result.is_err());
    }

    #[test]
    fn test_partial_oauth_fails() {
        let result = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: None,
            client_id: Some("client_123".into()),
            client_secret: None,
        });
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pat_get_token() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: Some("dapi_test".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        assert_eq!(auth.get_token().await.unwrap(), "dapi_test");
    }

    #[test]
    fn test_debug_hides_secrets() {
        let auth = Auth::from_config(AuthConfig {
            host: "host.databricks.com".into(),
            token: Some("secret_token".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let debug = format!("{auth:?}");
        assert!(!debug.contains("secret_token"));
        assert!(debug.contains("***"));
    }
}
