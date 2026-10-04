//! Microsoft Entra ID (Azure AD) access tokens for the TDS login.
//!
//! Two sources:
//!
//! - A pre-acquired token (`oauth_token`), passed through unchanged.
//! - A service principal (`client_id` + `client_secret` + `tenant_id`),
//!   exchanged with the OAuth 2.0 client-credentials grant at
//!   `{authority_host}/{tenant_id}/oauth2/v2.0/token` for the scope
//!   `https://database.windows.net/.default` — the resource Azure SQL,
//!   Managed Instance and Fabric Warehouse all accept
//!   (learn.microsoft.com/entra/identity-platform/v2-oauth2-client-creds-grant-flow).
//!
//! A fetched token is cached and reused until five minutes before it
//! expires. The token is only presented at login, so a long statement on an
//! open connection is not cut off when the token expires.

use std::time::{Duration, Instant};

use tokio::sync::Mutex;

use crate::config::{Auth, SqlServerConfig};
use crate::connector::SqlServerError;

/// The Entra ID scope for Azure SQL / Fabric data-plane access.
pub const DATABASE_SCOPE: &str = "https://database.windows.net/.default";

/// Refresh this long before the token's stated expiry.
const REFRESH_MARGIN: Duration = Duration::from_secs(300);

struct Cached {
    token: String,
    expires_at: Instant,
}

/// Hands out the access token a new connection logs in with.
pub struct TokenProvider {
    http: reqwest::Client,
    cache: Mutex<Option<Cached>>,
}

impl TokenProvider {
    /// A provider with its own HTTP client.
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Auth`] when the HTTP client cannot be built.
    pub fn new(timeout: Duration) -> Result<Self, SqlServerError> {
        let http = reqwest::Client::builder()
            .timeout(timeout.min(Duration::from_secs(60)))
            .build()
            .map_err(|e| SqlServerError::Auth(format!("cannot build the HTTP client: {e}")))?;
        Ok(Self {
            http,
            cache: Mutex::new(None),
        })
    }

    /// The token for `config`'s auth method, or `None` for SQL
    /// authentication.
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Auth`] when the token endpoint refuses the service
    /// principal or answers with something that is not a token.
    pub async fn token(&self, config: &SqlServerConfig) -> Result<Option<String>, SqlServerError> {
        match &config.auth {
            Auth::SqlPassword { .. } => Ok(None),
            Auth::AccessToken(token) => Ok(Some(token.clone())),
            Auth::ServicePrincipal {
                tenant_id,
                client_id,
                client_secret,
            } => {
                let mut cache = self.cache.lock().await;
                if let Some(c) = cache.as_ref()
                    && Instant::now() + REFRESH_MARGIN < c.expires_at
                {
                    return Ok(Some(c.token.clone()));
                }
                let fetched = self
                    .fetch(&config.authority_host, tenant_id, client_id, client_secret)
                    .await?;
                let token = fetched.token.clone();
                *cache = Some(fetched);
                Ok(Some(token))
            }
        }
    }

    async fn fetch(
        &self,
        authority_host: &str,
        tenant_id: &str,
        client_id: &str,
        client_secret: &str,
    ) -> Result<Cached, SqlServerError> {
        let url = format!("{authority_host}/{tenant_id}/oauth2/v2.0/token");
        let response = self
            .http
            .post(&url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", client_id),
                ("client_secret", client_secret),
                ("scope", DATABASE_SCOPE),
            ])
            .send()
            .await
            .map_err(|e| SqlServerError::Auth(format!("token request to {url} failed: {e}")))?;
        let status = response.status();
        let body: serde_json::Value = response.json().await.map_err(|e| {
            SqlServerError::Auth(format!(
                "token endpoint answered HTTP {status} with a non-JSON body: {e}"
            ))
        })?;
        parse_token_response(status.as_u16(), &body)
    }
}

/// Read a token-endpoint response. On failure, report Entra's
/// `error` / `error_description` (never the secret that was sent).
fn parse_token_response(status: u16, body: &serde_json::Value) -> Result<Cached, SqlServerError> {
    if !(200..300).contains(&status) {
        let code = body
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown_error");
        let description = body
            .get("error_description")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("")
            .lines()
            .next()
            .unwrap_or("");
        return Err(SqlServerError::Auth(format!(
            "Entra ID refused the service principal (HTTP {status}, {code}): {description}"
        )));
    }
    let token = body
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| SqlServerError::Auth("token response has no access_token".into()))?;
    // `expires_in` is seconds; some endpoints send it as a string.
    let expires_in = body
        .get("expires_in")
        .and_then(|v| v.as_u64().or_else(|| v.as_str()?.parse().ok()))
        .unwrap_or(3600);
    Ok(Cached {
        token: token.to_string(),
        expires_at: Instant::now() + Duration::from_secs(expires_in),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use crate::config::Credentials;

    #[test]
    fn token_response_parsing() {
        let ok = parse_token_response(
            200,
            &serde_json::json!({"access_token": "abc", "expires_in": "3599", "token_type": "Bearer"}),
        )
        .unwrap();
        assert_eq!(ok.token, "abc");
        assert!(ok.expires_at > Instant::now() + Duration::from_secs(3500));

        let err = parse_token_response(
            401,
            &serde_json::json!({
                "error": "invalid_client",
                "error_description": "AADSTS7000215: Invalid client secret provided.\r\nTrace ID: x"
            }),
        )
        .map(|_| ())
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("invalid_client"), "{msg}");
        assert!(msg.contains("AADSTS7000215"), "{msg}");
        assert!(!msg.contains("Trace ID"), "{msg}");

        assert!(parse_token_response(200, &serde_json::json!({})).is_err());
    }

    /// The client-credentials request against a local stand-in for the
    /// token endpoint: form fields, scope, and caching (one request for two
    /// tokens).
    #[tokio::test]
    async fn service_principal_flow_requests_once_and_caches() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            // Serve exactly one request; a second would hang the test, which
            // is the assertion that the cache was used.
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 8192];
            let mut read = 0;
            loop {
                let n = sock.read(&mut buf[read..]).await.unwrap();
                read += n;
                let text = String::from_utf8_lossy(&buf[..read]);
                if let Some(end) = text.find("\r\n\r\n") {
                    let len = text
                        .lines()
                        .find_map(|l| {
                            l.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if read >= end + 4 + len || n == 0 {
                        break;
                    }
                }
            }
            requests.push(String::from_utf8_lossy(&buf[..read]).to_string());
            let body = r#"{"token_type":"Bearer","expires_in":3599,"access_token":"tok-1"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                body.len()
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            requests
        });

        let creds = Credentials {
            client_id: Some("app-id"),
            client_secret: Some("s3cret"),
            ..Credentials::default()
        };
        let mut extra = BTreeMap::new();
        extra.insert("tenant_id".into(), serde_json::json!("contoso"));
        extra.insert(
            "authority_host".into(),
            serde_json::json!(format!("http://127.0.0.1:{}", addr.port())),
        );
        let cfg = SqlServerConfig::new(
            Some("db"),
            Some("d"),
            &creds,
            Duration::from_secs(10),
            &extra,
        )
        .unwrap();
        let provider = TokenProvider::new(Duration::from_secs(10)).unwrap();
        assert_eq!(
            provider.token(&cfg).await.unwrap().as_deref(),
            Some("tok-1")
        );
        assert_eq!(
            provider.token(&cfg).await.unwrap().as_deref(),
            Some("tok-1")
        );

        let requests = server.await.unwrap();
        let req = &requests[0];
        assert!(req.starts_with("POST /contoso/oauth2/v2.0/token "), "{req}");
        assert!(req.contains("grant_type=client_credentials"), "{req}");
        assert!(req.contains("client_id=app-id"), "{req}");
        assert!(
            req.contains("scope=https%3A%2F%2Fdatabase.windows.net%2F.default"),
            "{req}"
        );
    }
}
