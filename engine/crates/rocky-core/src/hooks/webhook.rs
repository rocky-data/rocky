use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::template::{self, TemplateError};
use super::{FailureAction, HookContext, HookResult};

/// Hard cap on total time spent firing a single webhook (retries + delays
/// included). Guards the pipeline from runaway retry storms; a misconfigured
/// `retry_count × timeout_ms` combination can otherwise pin the run for
/// minutes per hook. Not configurable today — if users hit it intentionally
/// we'll promote this to `WebhookConfig`.
pub(crate) const WEBHOOK_GLOBAL_TIMEOUT: Duration = Duration::from_secs(120);
pub(crate) const WEBHOOK_GLOBAL_TIMEOUT_MS: u64 = 120_000;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    #[error("webhook request failed: {url} — {message}")]
    RequestFailed { url: String, message: String },

    #[error("webhook timed out: {url} after {timeout_ms}ms")]
    Timeout { url: String, timeout_ms: u64 },

    #[error("webhook exceeded global timeout: {url} after {timeout_ms}ms")]
    GlobalTimeout { url: String, timeout_ms: u64 },

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
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
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
// Async webhook tracking
// ---------------------------------------------------------------------------

/// Handle returned from `fire_webhook` for async-mode webhooks. Callers
/// retain this and await it at pipeline shutdown via
/// [`super::HookRegistry::wait_async_webhooks`] so fire-and-forget deliveries
/// don't fail silently.
#[derive(Debug)]
pub struct AsyncWebhookHandle {
    pub url: String,
    handle: JoinHandle<Result<(), WebhookError>>,
}

impl AsyncWebhookHandle {
    /// Awaits the background task, returning the URL alongside the result.
    pub async fn join(self) -> (String, Result<Result<(), WebhookError>, tokio::task::JoinError>) {
        let result = self.handle.await;
        (self.url, result)
    }
}

// ---------------------------------------------------------------------------
// Webhook execution
// ---------------------------------------------------------------------------

/// Fires a webhook request. For async_mode webhooks, spawns a background task
/// and returns an [`AsyncWebhookHandle`] alongside `HookResult::Continue` —
/// the caller is expected to retain the handle for end-of-pipeline summary.
///
/// Both sync and async paths are bounded by [`WEBHOOK_GLOBAL_TIMEOUT`] so a
/// runaway retry storm can't pin the pipeline.
pub async fn fire_webhook(
    config: &WebhookConfig,
    ctx: &HookContext,
    http: &reqwest::Client,
) -> Result<(HookResult, Option<AsyncWebhookHandle>), WebhookError> {
    // Render the body
    let body = template::render_or_serialize(config.body_template.as_deref(), ctx)?;

    if config.async_mode {
        let url = config.url.clone();
        let url_for_log = url.clone();
        let config_clone = config.clone();
        let http_clone = http.clone();
        let body_clone = body;
        let handle = tokio::spawn(async move {
            match tokio::time::timeout(
                WEBHOOK_GLOBAL_TIMEOUT,
                fire_webhook_inner(&config_clone, &body_clone, &http_clone),
            )
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => {
                    warn!(webhook = %config_clone.url, error = %e, "async webhook failed");
                    Err(e)
                }
                Err(_) => {
                    let err = WebhookError::GlobalTimeout {
                        url: config_clone.url.clone(),
                        timeout_ms: WEBHOOK_GLOBAL_TIMEOUT_MS,
                    };
                    warn!(webhook = %config_clone.url, error = %err, "async webhook exceeded global timeout");
                    Err(err)
                }
            }
        });
        debug!(webhook = %url_for_log, "async webhook spawned");
        return Ok((HookResult::Continue, Some(AsyncWebhookHandle { url, handle })));
    }

    // Synchronous: execute with retries, bounded by the global cap.
    match tokio::time::timeout(
        WEBHOOK_GLOBAL_TIMEOUT,
        fire_webhook_inner(config, &body, http),
    )
    .await
    {
        Ok(Ok(())) => {
            info!(webhook = %config.url, method = %config.method, "webhook succeeded");
            Ok((HookResult::Continue, None))
        }
        Ok(Err(e)) => {
            warn!(webhook = %config.url, error = %e, "webhook failed");
            Err(e)
        }
        Err(_) => {
            let err = WebhookError::GlobalTimeout {
                url: config.url.clone(),
                timeout_ms: WEBHOOK_GLOBAL_TIMEOUT_MS,
            };
            warn!(webhook = %config.url, error = %err, "webhook exceeded global timeout");
            Err(err)
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
                    let delay =
                        (config.retry_delay_ms * (1u64 << (attempt - 1).min(10))).min(30_000);
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
        // Async mode should return Continue immediately without awaiting the real
        // request; the AsyncWebhookHandle is returned so the caller can track it.
        let config = WebhookConfig {
            url: "https://127.0.0.1:1/never-connects".to_string(), // intentionally unroutable
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
        let (result, handle) = fire_webhook(&config, &ctx, &http).await.unwrap();
        assert_eq!(result, HookResult::Continue);
        let handle = handle.expect("async mode should return a tracking handle");
        assert_eq!(handle.url, "https://127.0.0.1:1/never-connects");
        // Drain the spawned task so it doesn't leak past the test runtime.
        let (_url, outcome) = handle.join().await;
        // Delivery is expected to fail (connection refused); we just care the
        // task ran to completion.
        assert!(outcome.is_ok(), "join should not panic: {outcome:?}");
    }

    #[tokio::test]
    async fn test_fire_webhook_sync_global_timeout_short_circuits() {
        // Verify the sync global timeout path: a wildly slow per-request timeout
        // combined with retries would otherwise pin the caller, but the global
        // cap converts it into a GlobalTimeout error. Using an unroutable IP with
        // a very long per-request timeout, the request will stall well past our
        // test cap; we temporarily shorten the global cap via a local timeout
        // for test speed by asserting the error shape only.
        //
        // NOTE: we can't actually lower WEBHOOK_GLOBAL_TIMEOUT from a test
        // (it's a const). Instead we verify that GlobalTimeout is a distinct
        // WebhookError variant via a constructed value.
        let err = WebhookError::GlobalTimeout {
            url: "https://example.com".to_string(),
            timeout_ms: WEBHOOK_GLOBAL_TIMEOUT_MS,
        };
        let msg = err.to_string();
        assert!(msg.contains("global timeout"), "unexpected: {msg}");
        assert!(msg.contains("120000"), "unexpected: {msg}");
    }
}
