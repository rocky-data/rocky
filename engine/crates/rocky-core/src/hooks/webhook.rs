use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tracing::{debug, info, warn};

use super::template::{self, TemplateError};
use super::{FailureAction, HookContext, HookResult};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("webhook request failed: {url} — {message}")]
    RequestFailed { url: String, message: String },

    #[error("webhook timed out: {url} after {timeout_ms}ms")]
    Timeout { url: String, timeout_ms: u64 },

    #[error("webhook HTTP error: {url} — status {status}")]
    HttpError {
        url: String,
        status: u16,
        body: String,
    },

    #[error("webhook template error: {0}")]
    Template(#[from] TemplateError),

    #[error("webhook HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
}

// ---------------------------------------------------------------------------
// WebhookConfig
// ---------------------------------------------------------------------------

fn default_method() -> String {
    "POST".to_string()
}

fn default_webhook_timeout_ms() -> u64 {
    10_000
}

fn default_retry_delay_ms() -> u64 {
    1_000
}

/// Configuration for a single webhook endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    /// Target URL for the webhook request.
    pub url: String,

    /// HTTP method (default: POST).
    #[serde(default = "default_method")]
    pub method: String,

    /// Additional HTTP headers.
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Mustache-style body template. If None, the full HookContext is serialized as JSON.
    pub body_template: Option<String>,

    /// HMAC-SHA256 signing secret. When set, adds `X-Rocky-Signature: sha256=<hex>` header.
    pub secret: Option<String>,

    /// Request timeout in milliseconds (default: 10000).
    #[serde(default = "default_webhook_timeout_ms")]
    pub timeout_ms: u64,

    /// If true, fire-and-forget (spawn task, don't await). Default: false.
    #[serde(default, rename = "async")]
    pub async_mode: bool,

    /// What to do when the webhook fails.
    #[serde(default)]
    pub on_failure: FailureAction,

    /// Number of retry attempts on failure (default: 0).
    #[serde(default)]
    pub retry_count: u32,

    /// Delay between retries in milliseconds (default: 1000).
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,

    /// Optional preset name (e.g., "slack", "pagerduty").
    /// When set, preset defaults are merged before execution.
    pub preset: Option<String>,
}

impl fmt::Display for WebhookConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.method, self.url)
    }
}

// ---------------------------------------------------------------------------
// HMAC-SHA256 signature
// ---------------------------------------------------------------------------

type HmacSha256 = Hmac<Sha256>;

/// Computes HMAC-SHA256 of `body` using `secret` and returns hex-encoded digest.
pub fn compute_signature(secret: &str, body: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(body.as_bytes());
    let result = mac.finalize();
    let bytes = result.into_bytes();
    hex::encode(bytes.as_slice())
}

// Inline hex encoder to avoid an extra dependency
mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        let mut s = String::with_capacity(bytes.len() * 2);
        for b in bytes {
            s.push_str(&format!("{b:02x}"));
        }
        s
    }
}

// ---------------------------------------------------------------------------
// Webhook execution
// ---------------------------------------------------------------------------

/// Fires a webhook request. For async_mode webhooks, spawns a background task
/// and returns `HookResult::Continue` immediately.
pub async fn fire_webhook(
    config: &WebhookConfig,
    ctx: &HookContext,
    http: &reqwest::Client,
) -> Result<HookResult, WebhookError> {
    // Render the body
    let body = template::render_or_serialize(config.body_template.as_deref(), ctx)?;

    if config.async_mode {
        let url_for_log = config.url.clone();
        let config = config.clone();
        let http = http.clone();
        let body = body.clone();
        tokio::spawn(async move {
            if let Err(e) = fire_webhook_inner(&config, &body, &http).await {
                warn!(webhook = %config.url, error = %e, "async webhook failed");
            }
        });
        debug!(webhook = %url_for_log, "async webhook spawned");
        return Ok(HookResult::Continue);
    }

    // Synchronous: execute with retries
    match fire_webhook_inner(config, &body, http).await {
        Ok(()) => {
            info!(webhook = %config.url, method = %config.method, "webhook succeeded");
            Ok(HookResult::Continue)
        }
        Err(e) => {
            warn!(webhook = %config.url, error = %e, "webhook failed");
            Err(e)
        }
    }
}

/// Internal execution: send the request with retry logic.
async fn fire_webhook_inner(
    config: &WebhookConfig,
    body: &str,
    http: &reqwest::Client,
) -> Result<(), WebhookError> {
    let max_attempts = 1 + config.retry_count;

    for attempt in 1..=max_attempts {
        match send_request(config, body, http).await {
            Ok(()) => return Ok(()),
            Err(e) => {
                if attempt < max_attempts {
                    // Exponential backoff capped at 30s to prevent overflow at high attempt counts
                    let delay = (config.retry_delay_ms
                        * (1u64 << (attempt - 1).min(10)))
                    .min(30_000);
                    warn!(
                        webhook = %config.url,
                        attempt = attempt,
                        max_attempts = max_attempts,
                        retry_delay_ms = delay,
                        error = %e,
                        "webhook failed, retrying"
                    );
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                } else {
                    return Err(e);
                }
            }
        }
    }

    unreachable!("loop always returns")
}

/// Sends a single HTTP request (no retries).
async fn send_request(
    config: &WebhookConfig,
    body: &str,
    http: &reqwest::Client,
) -> Result<(), WebhookError> {
    let method: reqwest::Method = config.method.parse().unwrap_or(reqwest::Method::POST);

    let mut builder = http
        .request(method, &config.url)
        .timeout(Duration::from_millis(config.timeout_ms))
        .header("Content-Type", "application/json")
        .body(body.to_string());

    // Add custom headers
    for (key, value) in &config.headers {
        builder = builder.header(key.as_str(), value.as_str());
    }

    // Add HMAC signature if secret is configured
    if let Some(ref secret) = config.secret {
        let sig = compute_signature(secret, body);
        builder = builder.header("X-Rocky-Signature", format!("sha256={sig}"));
    }

    let response = builder.send().await.map_err(|e| {
        if e.is_timeout() {
            WebhookError::Timeout {
                url: config.url.clone(),
                timeout_ms: config.timeout_ms,
            }
        } else {
            WebhookError::RequestFailed {
                url: config.url.clone(),
                message: e.to_string(),
            }
        }
    })?;

    let status = response.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = response.text().await.unwrap_or_default();
        Err(WebhookError::HttpError {
            url: config.url.clone(),
            status: status.as_u16(),
            body,
        })
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hooks::HookContext;

    #[test]
    fn test_compute_signature() {
        let sig = compute_signature("my_secret", r#"{"event":"test"}"#);
        // Verify it's a 64-char hex string (SHA256 = 32 bytes = 64 hex chars)
        assert_eq!(sig.len(), 64);
        assert!(sig.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_signature_deterministic() {
        let sig1 = compute_signature("secret", "body");
        let sig2 = compute_signature("secret", "body");
        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_signature_differs_with_different_secret() {
        let sig1 = compute_signature("secret1", "body");
        let sig2 = compute_signature("secret2", "body");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_signature_differs_with_different_body() {
        let sig1 = compute_signature("secret", "body1");
        let sig2 = compute_signature("secret", "body2");
        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_webhook_config_defaults() {
        let toml_str = r#"
url = "https://example.com/hook"
"#;
        let config: WebhookConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.url, "https://example.com/hook");
        assert_eq!(config.method, "POST");
        assert_eq!(config.timeout_ms, 10_000);
        assert!(!config.async_mode);
        assert_eq!(config.on_failure, FailureAction::Warn);
        assert_eq!(config.retry_count, 0);
        assert_eq!(config.retry_delay_ms, 1_000);
        assert!(config.secret.is_none());
        assert!(config.body_template.is_none());
        assert!(config.headers.is_empty());
    }

    #[test]
    fn test_webhook_config_full() {
        let toml_str = r#"
url = "https://hooks.slack.com/services/T/B/x"
method = "POST"
body_template = '{"text": "{{model}} failed"}'
secret = "my_secret"
timeout_ms = 5000
async = true
on_failure = "abort"
retry_count = 3
retry_delay_ms = 2000

[headers]
"X-Custom" = "value"
"#;
        let config: WebhookConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.url, "https://hooks.slack.com/services/T/B/x");
        assert_eq!(config.method, "POST");
        assert_eq!(
            config.body_template.as_deref(),
            Some(r#"{"text": "{{model}} failed"}"#)
        );
        assert_eq!(config.secret.as_deref(), Some("my_secret"));
        assert_eq!(config.timeout_ms, 5000);
        assert!(config.async_mode);
        assert_eq!(config.on_failure, FailureAction::Abort);
        assert_eq!(config.retry_count, 3);
        assert_eq!(config.retry_delay_ms, 2000);
        assert_eq!(config.headers.get("X-Custom").unwrap(), "value");
    }

    #[test]
    fn test_webhook_display() {
        let config = WebhookConfig {
            url: "https://example.com/hook".to_string(),
            method: "POST".to_string(),
            headers: HashMap::new(),
            body_template: None,
            secret: None,
            timeout_ms: 10_000,
            async_mode: false,
            on_failure: FailureAction::Warn,
            retry_count: 0,
            retry_delay_ms: 1000,
            preset: None,
        };
        assert_eq!(format!("{config}"), "POST https://example.com/hook");
    }

    #[test]
    fn test_known_hmac_value() {
        // Verify against a known HMAC-SHA256 computation
        // echo -n "hello" | openssl dgst -sha256 -hmac "key"
        // => 9307b3b915efb5171ff14d8cb55fbcc798c6c0ef1456d66ded1a6aa723a58b7b
        let sig = compute_signature("key", "hello");
        assert_eq!(
            sig,
            "9307b3b915efb5171ff14d8cb55fbcc798c6c0ef1456d66ded1a6aa723a58b7b"
        );
    }

    #[tokio::test]
    async fn test_fire_webhook_async_returns_immediately() {
        // Async mode should return Continue immediately without making a real request
        let config = WebhookConfig {
            url: "https://httpbin.org/status/500".to_string(), // would fail if actually awaited
            method: "POST".to_string(),
            headers: HashMap::new(),
            body_template: Some(r#"{"test": true}"#.to_string()),
            secret: None,
            timeout_ms: 100,
            async_mode: true,
            on_failure: FailureAction::Abort,
            retry_count: 0,
            retry_delay_ms: 100,
            preset: None,
        };

        let ctx = HookContext::pipeline_start("run-1", "test");
        let http = reqwest::Client::new();
        let result = fire_webhook(&config, &ctx, &http).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }
}
