//! BigQuery authentication — Service Account JSON key, ADC, or Bearer token.
//!
//! Credential fields (`private_key`, bearer tokens) are wrapped in
//! [`RedactedString`] so that `Debug` output never leaks secrets.

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

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

/// BigQuery authentication provider.
///
/// Credential fields are wrapped in [`RedactedString`]; the custom `Debug`
/// impl prints `***` instead of raw secrets.
#[derive(Clone)]
pub enum BigQueryAuth {
    /// Service Account JSON key.
    ServiceAccount(ServiceAccountKey),
    /// Pre-supplied Bearer token (e.g., from ADC or gcloud).
    Bearer(RedactedString),
}

impl std::fmt::Debug for BigQueryAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BigQueryAuth::ServiceAccount(key) => f
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

impl BigQueryAuth {
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
            return Ok(BigQueryAuth::ServiceAccount(key));
        }

        Err(AuthError::NoAuth)
    }

    /// Get a bearer token for API calls.
    pub async fn get_token(&self, client: &reqwest::Client) -> Result<String, AuthError> {
        match self {
            BigQueryAuth::Bearer(token) => Ok(token.expose().to_string()),
            BigQueryAuth::ServiceAccount(key) => {
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

                // Exchange JWT for access token
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

                #[derive(Deserialize)]
                struct TokenResponse {
                    access_token: String,
                }

                let token: TokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| AuthError::TokenExchange(e.to_string()))?;
                Ok(token.access_token)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let key = ServiceAccountKey {
            key_type: "service_account".into(),
            project_id: "my-project".into(),
            private_key_id: "key-id-123".into(),
            private_key: RedactedString::new(
                "-----BEGIN RSA PRIVATE KEY-----\nSECRET\n-----END RSA PRIVATE KEY-----".into(),
            ),
            client_email: "test@project.iam.gserviceaccount.com".into(),
            token_uri: "https://oauth2.googleapis.com/token".into(),
        };
        let auth = BigQueryAuth::ServiceAccount(key);
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
}
